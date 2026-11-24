from datetime import datetime
import hashlib
import json
import logging
import os
import re
import tempfile
import threading
import time
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from flask import Flask, request, abort

from linebot.v3 import WebhookHandler
from linebot.v3.exceptions import InvalidSignatureError
from linebot.v3.messaging import (
    Configuration,
    ApiClient,
    MessagingApi,
    PushMessageRequest,
    ReplyMessageRequest,
    TextMessage,
)
from linebot.v3.webhooks import (
    MessageEvent,
    TextMessageContent,
    MemberJoinedEvent,
    MemberLeftEvent,
)

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("abyss_main_bot")

CHANNEL_ACCESS_TOKEN = (os.getenv("LINE_CHANNEL_ACCESS_TOKEN") or "").strip()
CHANNEL_SECRET = (os.getenv("LINE_CHANNEL_SECRET") or "").strip()
# 건의·신고를 수신할 방장 user_id(U...) 또는 운영진 전용방 group_id(C...)
ADMIN_TARGET_ID = (os.getenv("ADMIN_TARGET_ID") or "").strip()

if not CHANNEL_ACCESS_TOKEN or not CHANNEL_SECRET:
    raise ValueError("LINE_CHANNEL_ACCESS_TOKEN과 LINE_CHANNEL_SECRET 환경 변수를 설정해주세요.")

app = Flask(__name__)
configuration = Configuration(access_token=CHANNEL_ACCESS_TOKEN)
handler = WebhookHandler(CHANNEL_SECRET)

KST = ZoneInfo("Asia/Seoul")
RANKING_STORE_PATH = "/tmp/abyss_rankings.json"
TARGETS_STORE_PATH = "/tmp/abyss_targets.json"
MEMBERS_STORE_PATH = "/tmp/abyss_members.json"
REPORTS_STORE_PATH = "/tmp/abyss_reports.json"

# 같은 유저의 연타 도배 방지 최소 간격(초). 0으로 두면 모든 유효 메시지 카운트
MIN_SPEECH_INTERVAL_SEC = 2.0
# 일일 활동부족 경고 기준 마디 수 (30마디 미만 시 경고 표시)
MIN_ACTIVITY_MADIT = 30
# 1개 메시지에서 인정되는 유효 단어(3글자 이상)의 최소 길이
MIN_WORD_LEN = 3

# 건의·신고 도배 방지 쿨타임(초) 및 최소 내용 길이
REPORT_COOLDOWN_SEC = 300
MIN_REPORT_LEN = 5

state_lock = threading.Lock()
last_counted_at: dict[str, float] = {}
last_reported_at: dict[str, float] = {}

WELCOME_MSG = (
    "🌊 ━━━ 🐬 𝓐𝓑𝓨𝓢𝓢 🐬 ━━━ 🌊\n"
    " 새로운 분들의 입장을 환영합니다!\n"
    "𝓐𝓑𝓨𝓢𝓢에 오신 걸 환영해요. 입장 후 간단한 자기소개를 부탁드립니다.\n\n"
    " 기존 멤버분들은 잠시 대화를 멈추고 새로 오신 분의 자기소개를 확인해 주세요. 👀\n"
    " 입장 후 10분 이내 작성 필수 ⚠️ 미완료 시 퇴장될 수 있습니다.\n\n"
    "01. 우측 상단 ≡ 클릭\n"
    "02.  노트 메뉴 입장 \n"
    "03.  공지사항 필독! 끝까지 읽기  댓글에 “확인” 작성 \n"
    "04.  자기소개서 게시글 댓글 작성\n"
    "05.  족보 게시글 댓글 작성\n\n"
    "🦭 모든 노트 작성 완료 후 “노트 작성 완료했습니다!” 라고 말씀해 주세요.\n\n"
    "다시 한번 𝓐𝓑𝓨𝓢𝓢에 오신 것을 환영합니다!"
)

HWAN_MSG = (
    "📜환츄/환찌✉️\n"
    "1.    신입분의 고추나 가슴 사진을 올리기!\n"
    "2.    올렸던 사진을 ‘보내기 취소’\n"
    "3.    그 후 보고싶은 사람을 선택!\n"
    "4.    지목당한 사람은 똑같이 사진을 오픈!\n"
    "5.    (매너 안하면 지목 못해용 )\n\n"
    "📢릴레이 아닙니다.\n"
    "📢신입분들에 한정됩니다."
)

DISCOMFORT_REPORT_GUIDE_MSG = (
    "🚨 불편신고 안내\n"
    "[오른쪽 상단 메뉴 -> 멤버 -> 봇 친구추가]\n\n"
    "친구 추가 후 봇과의 1:1 채팅에서\n"
    "/신고 [내용] 또는 /건의 [내용]으로 보내주시면\n"
    "운영진에게 전달됩니다."
)

COMMAND_LIST_MSG = (
    "📌 사용 가능한 명령어 안내\n\n"
    "[일반 멤버 명령어]\n"
    "• /내랭킹 — 오늘 내 마디 수, 순위, 활동 상태 확인\n"
    "• /환찌 (/환츄) — 환츄/환찌 안내 보기\n"
    "• /불편신고 — 1:1 신고·건의 방법 안내\n"
    "• /명령어 — 사용 가능한 명령어 목록 보기\n\n"
    "[관리자 전용 사용 가능 명령어]\n"
    "• /전체랭킹 — 오늘 방 전체 마디 수 랭킹 확인\n"
    "• /활동부족 — 오늘 30마디 미만 인원 목록 확인"
)


def today_key() -> str:
    return datetime.now(KST).strftime("%Y-%m-%d")


def load_json_dict(path: str) -> dict:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, dict) else {}
    except Exception as e:
        logger.warning("[STORE] failed to load %s: %s", path, e)
        return {}


def save_json_dict(path: str, data: dict) -> None:
    """임시 파일 기록 후 os.replace()로 원자적 저장"""
    directory = os.path.dirname(path) or "/tmp"
    os.makedirs(directory, exist_ok=True)
    tmp_path = None
    try:
        fd, tmp_path = tempfile.mkstemp(prefix=".tmp_", suffix=".json", dir=directory)
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, path)
    except Exception as e:
        logger.warning("[STORE] failed to save %s: %s", path, e)
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except OSError:
                pass


ranking_data = load_json_dict(RANKING_STORE_PATH)
targets_data = load_json_dict(TARGETS_STORE_PATH)
members_data = load_json_dict(MEMBERS_STORE_PATH)
reports_data = load_json_dict(REPORTS_STORE_PATH)


def normalize_command(text: str) -> str:
    """앞뒤 공백 및 @멘션 토큰 제거 후 첫 명령어 토큰 반환"""
    tokens = [tok for tok in text.strip().split() if not tok.startswith("@")]
    return tokens[0] if tokens else ""


def extract_command_payload(text: str, cmd: str) -> str:
    """명령어 뒤의 본문 내용 추출"""
    stripped = text.strip()
    idx = stripped.find(cmd)
    if idx == -1:
        return ""
    return stripped[idx + len(cmd):].strip()


def make_anon_code(user_id: str) -> str:
    """참고용 4자리 식별 코드"""
    raw = f"{CHANNEL_SECRET}:{user_id}".encode("utf-8")
    return "#" + hashlib.sha256(raw).hexdigest()[:4].upper()


def calculate_message_madit(text: str) -> int:
    """
    '/'로 시작하지 않는 메시지에서 한글·영문·숫자 기준 3글자 이상인
    유효 단어의 개수를 마디 수로 계산 (없으면 0 반환)
    """
    stripped = text.strip()
    if not stripped or stripped.startswith("/"):
        return 0

    count = 0
    for token in stripped.split():
        if token.startswith("@"):
            continue
        cleaned = re.sub(r"[^가-힣A-Za-z0-9]", "", token)
        if len(cleaned) >= MIN_WORD_LEN:
            count += 1

    return count


def register_chat_target(event) -> None:
    """
    채팅방 대상 등록 — 1:1 유저가 아닌 그룹(group_id) 또는 다인 채팅방(room_id)만 등록
    """
    source = getattr(event, "source", None)
    if not source:
        return

    target_id = getattr(source, "group_id", None) or getattr(source, "room_id", None)
    if not target_id:
        return

    if ADMIN_TARGET_ID and target_id == ADMIN_TARGET_ID:
        return

    with state_lock:
        latest = load_json_dict(TARGETS_STORE_PATH)
        targets_data.update(latest)
        if target_id not in targets_data:
            targets_data[target_id] = True
            save_json_dict(TARGETS_STORE_PATH, targets_data)
            logger.info("[TARGET] registered chat target=%s", target_id)


def get_chat_targets() -> list[str]:
    with state_lock:
        latest = load_json_dict(TARGETS_STORE_PATH)
        targets_data.update(latest)
        return [
            tid for tid in targets_data.keys()
            if not (ADMIN_TARGET_ID and tid == ADMIN_TARGET_ID)
        ]


def get_display_name_by_ids(
    user_id: str | None,
    group_id: str | None = None,
    room_id: str | None = None,
) -> str:
    """외부 LINE Profile API 호출 — 반드시 state_lock 밖에서 호출"""
    uid = user_id or "unknown"
    try:
        with ApiClient(configuration) as api_client:
            bot_api = MessagingApi(api_client)
            if group_id and uid != "unknown":
                profile = bot_api.get_group_member_profile(group_id, uid)
                display_name = getattr(profile, "display_name", None)
                if display_name:
                    return str(display_name)
            elif room_id and uid != "unknown":
                profile = bot_api.get_room_member_profile(room_id, uid)
                display_name = getattr(profile, "display_name", None)
                if display_name:
                    return str(display_name)
            elif uid != "unknown":
                profile = bot_api.get_profile(uid)
                display_name = getattr(profile, "display_name", None)
                if display_name:
                    return str(display_name)
    except Exception as e:
        logger.debug("[PROFILE] fallback for user_id=%s: %s", uid, e)

    return uid[-6:] if uid != "unknown" else "익명"


def get_display_name(event) -> str:
    """이벤트로부터 발신자 닉네임 조회 — 반드시 state_lock 밖에서 호출"""
    source = getattr(event, "source", None)
    user_id = getattr(source, "user_id", None)
    group_id = getattr(source, "group_id", None)
    room_id = getattr(source, "room_id", None)
    return get_display_name_by_ids(user_id=user_id, group_id=group_id, room_id=room_id)


def upsert_member(user_id: str | None, name: str | None = None) -> None:
    """방에서 인지된 멤버를 상시 멤버 목록에 등록/갱신"""
    if not user_id:
        return
    with state_lock:
        latest = load_json_dict(MEMBERS_STORE_PATH)
        members_data.update(latest)
        existing = members_data.get(user_id)
        existing_name = existing.get("name") if isinstance(existing, dict) else None
        resolved_name = name or existing_name or user_id[-6:]
        members_data[user_id] = {
            "name": resolved_name,
            "updated_at": datetime.now(KST).isoformat(),
        }
        save_json_dict(MEMBERS_STORE_PATH, members_data)


def remove_member(user_id: str | None, remove_today_ranking: bool = True) -> None:
    """퇴장한 멤버를 상시 멤버 목록 및 오늘 랭킹에서 자동 제외"""
    if not user_id:
        return
    day = today_key()
    with state_lock:
        latest_members = load_json_dict(MEMBERS_STORE_PATH)
        members_data.clear()
        members_data.update(latest_members)
        removed_entry = members_data.pop(user_id, None)
        removed_name = removed_entry.get("name") if isinstance(removed_entry, dict) else None
        members_changed = removed_name is not None or user_id in latest_members
        if members_changed:
            save_json_dict(MEMBERS_STORE_PATH, members_data)

        ranking_changed = False
        if remove_today_ranking:
            latest_rank = load_json_dict(RANKING_STORE_PATH)
            ranking_data.clear()
            ranking_data.update(latest_rank)
            day_board = ranking_data.get(day)
            if isinstance(day_board, dict) and user_id in day_board:
                rank_info = day_board.pop(user_id, None)
                if not removed_name and isinstance(rank_info, dict):
                    removed_name = rank_info.get("name")
                ranking_changed = True
                save_json_dict(RANKING_STORE_PATH, ranking_data)

        if members_changed or ranking_changed:
            logger.info(
                "[MEMBER_LEFT] removed user_id=%s name=%s (members=%s, today_ranking=%s)",
                user_id,
                removed_name or user_id[-6:],
                members_changed,
                ranking_changed,
            )


def sync_group_members_from_api() -> None:
    """가능한 경우 그룹 멤버 ID 목록을 조회해 미등록 멤버(0마디)도 보충"""
    targets = get_chat_targets()
    group_ids = [tid for tid in targets if isinstance(tid, str) and tid.startswith("C")]
    if not group_ids:
        return

    with state_lock:
        latest = load_json_dict(MEMBERS_STORE_PATH)
        members_data.update(latest)
        known_ids = set(members_data.keys())

    for gid in group_ids:
        start_token = None
        while True:
            try:
                with ApiClient(configuration) as api_client:
                    bot_api = MessagingApi(api_client)
                    res = bot_api.get_group_members_ids(gid, start=start_token)
            except Exception as e:
                logger.debug("[MEMBERS_SYNC] skipped get_group_members_ids for %s: %s", gid, e)
                break

            member_ids = getattr(res, "member_ids", None) or []
            for uid in member_ids:
                if uid and uid not in known_ids:
                    display_name = get_display_name_by_ids(uid, group_id=gid)
                    upsert_member(uid, display_name)
                    known_ids.add(uid)

            start_token = getattr(res, "next", None)
            if not start_token:
                break


def ensure_known_member(user_id: str | None, name: str) -> None:
    """오늘 기록이 없더라도 알려진 멤버로 등록해 0마디 미활동자도 랭킹에 포함"""
    if not user_id:
        return
    upsert_member(user_id, name)
    day = today_key()
    with state_lock:
        latest = load_json_dict(RANKING_STORE_PATH)
        ranking_data.update(latest)
        day_board = ranking_data.setdefault(day, {})
        if not isinstance(day_board, dict):
            day_board = {}
            ranking_data[day] = day_board
        if user_id not in day_board or not isinstance(day_board.get(user_id), dict):
            day_board[user_id] = {"name": name or user_id[-6:], "madit": 0}
            save_json_dict(RANKING_STORE_PATH, ranking_data)
        elif name:
            day_board[user_id]["name"] = name
            save_json_dict(RANKING_STORE_PATH, ranking_data)


def add_speech_count(user_id: str, name: str, amount: int = 1) -> int:
    """이미 조회된 name을 받아 state_lock 안에서만 상태 갱신 및 파일 저장"""
    upsert_member(user_id, name)
    day = today_key()
    with state_lock:
        latest = load_json_dict(RANKING_STORE_PATH)
        ranking_data.update(latest)
        day_board = ranking_data.setdefault(day, {})
        if not isinstance(day_board, dict):
            day_board = {}
            ranking_data[day] = day_board
        entry = day_board.get(user_id)
        if not isinstance(entry, dict):
            entry = {"name": name or user_id[-6:], "madit": 0}
            day_board[user_id] = entry
        if name:
            entry["name"] = name
        new_total = int(entry.get("madit", 0)) + int(amount)
        entry["madit"] = new_total
        save_json_dict(RANKING_STORE_PATH, ranking_data)
        return new_total


def get_sorted_today_entries() -> list[tuple[str, dict]]:
    """
    현재 방에 남아 있는 멤버(오늘 0마디 포함) 기준으로 오늘 발언 수 내림차순 정렬
    """
    day = today_key()
    with state_lock:
        latest_rank = load_json_dict(RANKING_STORE_PATH)
        ranking_data.update(latest_rank)

        latest_members = load_json_dict(MEMBERS_STORE_PATH)
        members_data.clear()
        members_data.update(latest_members)

        merged_today: dict[str, dict] = {}

        for uid, m_info in members_data.items():
            if not uid or not isinstance(m_info, dict):
                continue
            merged_today[uid] = {
                "name": m_info.get("name") or uid[-6:],
                "madit": 0,
            }

        day_board = ranking_data.get(day, {})
        if isinstance(day_board, dict):
            for uid, info in day_board.items():
                if not uid or not isinstance(info, dict) or uid not in merged_today:
                    continue
                prev_name = merged_today[uid].get("name") or uid[-6:]
                resolved_name = info.get("name") or prev_name
                merged_today[uid] = {
                    "name": resolved_name,
                    "madit": int(info.get("madit", 0)),
                }

        items = list(merged_today.items())

    items.sort(key=lambda kv: (-int(kv[1].get("madit", 0)), str(kv[1].get("name", ""))))
    return items


def activity_warning_suffix(madit_count: int) -> str:
    """30마디 미만인 경우 활동부족 경고 문구 반환"""
    if madit_count < MIN_ACTIVITY_MADIT:
        return f" ⚠️ 활동부족({MIN_ACTIVITY_MADIT}마디 미만)"
    return ""


def format_my_ranking(user_id: str | None, fallback_name: str) -> str:
    day = today_key()
    sorted_items = get_sorted_today_entries()

    if not user_id or not sorted_items:
        return (
            f"📊 [{day}] 내 랭킹\n"
            f"• 닉네임: {fallback_name}\n"
            f"• 오늘 발언 수: 0마디\n"
            f"• 오늘 현재 순위: 기록 없음\n"
            f"• 상태: ⚠️ 활동부족({MIN_ACTIVITY_MADIT}마디 미만)"
        )

    rank = None
    my_count = 0
    my_name = fallback_name

    for idx, (uid, info) in enumerate(sorted_items, start=1):
        if uid == user_id:
            rank = idx
            my_count = int(info.get("madit", 0))
            my_name = info.get("name") or fallback_name
            break

    if rank is None:
        return (
            f"📊 [{day}] 내 랭킹\n"
            f"• 닉네임: {my_name}\n"
            f"• 오늘 발언 수: 0마디\n"
            f"• 오늘 현재 순위: 기록 없음\n"
            f"• 상태: ⚠️ 활동부족({MIN_ACTIVITY_MADIT}마디 미만)"
        )

    status_line = (
        f"⚠️ 활동부족({MIN_ACTIVITY_MADIT}마디 미만)"
        if my_count < MIN_ACTIVITY_MADIT
        else "활발"
    )

    return (
        f"📊 [{day}] 내 랭킹\n"
        f"• 닉네임: {my_name}\n"
        f"• 오늘 발언 수: {my_count}마디\n"
        f"• 오늘 현재 순위: {rank}위 / {len(sorted_items)}명\n"
        f"• 상태: {status_line}"
    )


def format_all_ranking(header_prefix: str = "🏆") -> str:
    day = today_key()
    sync_group_members_from_api()
    sorted_items = get_sorted_today_entries()

    if not sorted_items:
        return (
            f"{header_prefix} [{day}] 전체 랭킹\n"
            f"아직 등록된 멤버가 없습니다."
        )

    lines = [
        f"{header_prefix} [{day}] 전체 랭킹 (총 {len(sorted_items)}명 · 기준: {MIN_ACTIVITY_MADIT}마디 이상)"
    ]
    for idx, (_, info) in enumerate(sorted_items, start=1):
        name = info.get("name") or "멤버"
        count = int(info.get("madit", 0))
        medal = "🥇" if idx == 1 else ("🥈" if idx == 2 else ("🥉" if idx == 3 else f"{idx}."))
        warning = activity_warning_suffix(count)
        lines.append(f"{medal} {name} — {count}마디{warning}")

    return "\n".join(lines)


def format_inactive_members() -> str:
    """오늘 발언 수가 MIN_ACTIVITY_MADIT(30마디) 미만인 인원만 모아 반환"""
    day = today_key()
    sync_group_members_from_api()
    sorted_items = get_sorted_today_entries()

    if not sorted_items:
        return (
            f"⚠️ [{day}] 활동부족 인원 ({MIN_ACTIVITY_MADIT}마디 미만)\n"
            f"아직 등록된 멤버가 없습니다."
        )

    inactive_items = [
        (uid, info)
        for uid, info in sorted_items
        if int(info.get("madit", 0)) < MIN_ACTIVITY_MADIT
    ]

    if not inactive_items:
        return (
            f"✅ [{day}] 활동부족 인원 ({MIN_ACTIVITY_MADIT}마디 미만)\n"
            f"전원 기준({MIN_ACTIVITY_MADIT}마디 이상)을 충족했습니다!"
        )

    inactive_items.sort(
        key=lambda kv: (int(kv[1].get("madit", 0)), str(kv[1].get("name", "")))
    )

    lines = [
        f"⚠️ [{day}] 활동부족 인원 ({len(inactive_items)}명 / 전체 {len(sorted_items)}명 · 기준: {MIN_ACTIVITY_MADIT}마디 미만)"
    ]
    for idx, (_, info) in enumerate(inactive_items, start=1):
        name = info.get("name") or "멤버"
        count = int(info.get("madit", 0))
        shortage = MIN_ACTIVITY_MADIT - count
        lines.append(f"{idx}. {name} — {count}마디 (기준까지 {shortage}마디 부족)")

    return "\n".join(lines)


def save_and_forward_report(
    user_id: str,
    reporter_name: str,
    kind: str,
    content: str,
) -> tuple[bool, str]:
    """
    1:1 개인 채팅으로 접수된 건의·신고를 작성자 닉네임과 함께 저장하고
    ADMIN_TARGET_ID로 푸시 전달
    """
    if not ADMIN_TARGET_ID:
        return (
            False,
            "⚠️ 현재 운영진 수신처가 아직 연결되지 않아 접수할 수 없습니다. 잠시 후 다시 시도해 주세요.",
        )

    now_ts = time.monotonic()
    with state_lock:
        prev_ts = last_reported_at.get(user_id, 0.0)
        elapsed = now_ts - prev_ts
        if prev_ts > 0 and elapsed < REPORT_COOLDOWN_SEC:
            remain_sec = int(REPORT_COOLDOWN_SEC - elapsed) + 1
            return (
                False,
                f"⏳ 연속 접수 방지를 위해 {remain_sec}초 후에 다시 보낼 수 있습니다.",
            )

        now_kst_str = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
        anon_code = make_anon_code(user_id)

        latest = load_json_dict(REPORTS_STORE_PATH)
        reports_data.update(latest)
        items = reports_data.get("items")
        if not isinstance(items, list):
            items = []
        reports_data["items"] = items

        report_no = len(items) + 1
        entry = {
            "no": report_no,
            "kind": kind,
            "reporter_name": reporter_name,
            "anon_code": anon_code,
            "content": content,
            "created_at": now_kst_str,
        }
        items.append(entry)
        if len(items) > 100:
            reports_data["items"] = items[-100:]
        save_json_dict(REPORTS_STORE_PATH, reports_data)
        last_reported_at[user_id] = now_ts

    icon = "🚨" if kind == "신고" else "📩"
    admin_text = (
        f"{icon} [{kind} 접수 #{report_no}]\n"
        f"• 접수 시각: {now_kst_str}\n"
        f"• 작성자: {reporter_name}\n"
        f"• 코드: {anon_code}\n"
        f"• 내용:\n{content}"
    )

    try:
        with ApiClient(configuration) as api_client:
            line_bot_api = MessagingApi(api_client)
            line_bot_api.push_message(
                PushMessageRequest(
                    to=ADMIN_TARGET_ID,
                    messages=[TextMessage(text=admin_text)],
                )
            )
        logger.info(
            "[REPORT] forwarded no=%d kind=%s reporter=%s",
            report_no,
            kind,
            reporter_name,
        )
    except Exception:
        logger.exception("[REPORT] failed to push report no=%d to ADMIN_TARGET_ID", report_no)
        return (
            False,
            "⚠️ 운영진 알림 전송 중 일시적인 오류가 발생했습니다. 잠시 후 다시 시도해 주세요.",
        )

    return (
        True,
        f"✅ {kind}가 정상 접수되었습니다. (접수번호 #{report_no})\n운영진에게 전달되었습니다.",
    )


def format_recent_reports(limit: int = 10) -> str:
    with state_lock:
        latest = load_json_dict(REPORTS_STORE_PATH)
        reports_data.update(latest)
        items = reports_data.get("items", [])
        if not isinstance(items, list) or not items:
            return "📋 접수된 건의·신고 내역이 없습니다."
        recent = list(reversed(items[-limit:]))

    lines = [f"📋 최근 건의·신고 내역 (최근 {len(recent)}건)"]
    for r in recent:
        if not isinstance(r, dict):
            continue
        no = r.get("no", "?")
        kind = r.get("kind", "건의")
        reporter_name = r.get("reporter_name") or "알 수 없음"
        created_at = r.get("created_at", "")
        content = r.get("content", "")
        lines.append(
            f"\n[#{no} · {kind}] 작성자: {reporter_name} ({created_at})\n{content}"
        )

    return "\n".join(lines)


def reply_messages(reply_token: str, texts: list[str]) -> None:
    if not reply_token:
        return
    messages = [TextMessage(text=t) for t in texts[:5]]
    try:
        with ApiClient(configuration) as api_client:
            line_bot_api = MessagingApi(api_client)
            line_bot_api.reply_message_with_http_info(
                ReplyMessageRequest(
                    reply_token=reply_token,
                    messages=messages,
                )
            )
    except Exception:
        logger.exception("[REPLY] failed to send reply_message (token=%s...)", reply_token[:8])
        raise


@app.route("/", methods=["GET"])
def health_check():
    return "ABYSS Main Room Bot is running.", 200


@app.route("/callback", methods=["POST"])
def callback():
    signature = request.headers.get("X-Line-Signature", "")
    body = request.get_data(as_text=True)

    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        logger.warning("[WEBHOOK] InvalidSignatureError on /callback")
        abort(400)
    except Exception:
        logger.exception("[WEBHOOK] unexpected error handling /callback")
        abort(500)

    return "OK", 200


@handler.add(MemberJoinedEvent)
def handle_member_joined(event):
    register_chat_target(event)
    joined_members = getattr(event.joined, "members", None) or []
    group_id = getattr(event.source, "group_id", None)
    room_id = getattr(event.source, "room_id", None)
    for m in joined_members:
        uid = getattr(m, "user_id", None)
        if uid:
            display_name = get_display_name_by_ids(uid, group_id=group_id, room_id=room_id)
            ensure_known_member(uid, display_name)
    reply_messages(event.reply_token, [WELCOME_MSG])


@handler.add(MemberLeftEvent)
def handle_member_left(event):
    register_chat_target(event)
    left_members = getattr(event.left, "members", None) or []
    for m in left_members:
        uid = getattr(m, "user_id", None)
        remove_member(uid, remove_today_ranking=True)


@handler.add(MessageEvent, message=TextMessageContent)
def handle_message(event):
    source = getattr(event, "source", None)
    user_id = getattr(source, "user_id", None)
    group_id = getattr(source, "group_id", None)
    room_id = getattr(source, "room_id", None)

    is_group_or_room = bool(group_id or room_id)
    is_private_chat = (not is_group_or_room) and bool(user_id)
    chat_id = group_id or room_id or user_id or ""

    if is_group_or_room:
        register_chat_target(event)

    user_text = (getattr(event.message, "text", None) or "").strip()
    cmd = normalize_command(user_text)

    if cmd == "/명령어":
        reply_messages(event.reply_token, [COMMAND_LIST_MSG])
        return

    if cmd == "/불편신고":
        reply_messages(event.reply_token, [DISCOMFORT_REPORT_GUIDE_MSG])
        return

    # 1) 초기 세팅용 ID 확인 명령어 (설정 완료 후 본방에서는 완전 숨김)
    if cmd == "/방아이디":
        if is_private_chat or (not ADMIN_TARGET_ID) or (ADMIN_TARGET_ID and chat_id == ADMIN_TARGET_ID):
            reply_messages(event.reply_token, [f"🆔 현재 채팅 ID:\n{chat_id}"])
        return

    # 2) 운영진 전용 건의·신고 내역 조회 (본방에서는 완전 무응답)
    if cmd == "/건의목록":
        if ADMIN_TARGET_ID and chat_id == ADMIN_TARGET_ID:
            reply_messages(event.reply_token, [format_recent_reports()])
        return

    # 3) 1:1 건의·신고 접수 — 오직 1:1 개인 채팅에서만 동작, 본방에서는 완전 무응답
    if cmd in ("/건의", "/신고"):
        if not is_private_chat or not user_id:
            return

        kind = "신고" if cmd == "/신고" else "건의"
        payload = extract_command_payload(user_text, cmd)
        if len(payload) < MIN_REPORT_LEN:
            reply_messages(
                event.reply_token,
                [
                    f"📝 {kind} 내용을 {MIN_REPORT_LEN}글자 이상 함께 입력해 주세요.\n"
                    f"예시) {cmd} 방 공지 관련 건의사항이 있습니다."
                ],
            )
            return

        reporter_name = get_display_name(event)
        _, result_msg = save_and_forward_report(
            user_id=user_id,
            reporter_name=reporter_name,
            kind=kind,
            content=payload,
        )
        reply_messages(event.reply_token, [result_msg])
        return

    # 이하 일반 본방 기능 (1:1 채팅 유저는 본방 멤버 목록에 섞지 않음)
    if is_group_or_room and user_id:
        name = get_display_name(event)
        upsert_member(user_id, name)
    elif user_id:
        name = get_display_name(event)
    else:
        name = "익명"

    if cmd in ("/환찌", "/환츄"):
        reply_messages(event.reply_token, [HWAN_MSG])
        return

    if cmd == "/내랭킹":
        if is_group_or_room:
            ensure_known_member(user_id, name)
        reply_messages(event.reply_token, [format_my_ranking(user_id, name)])
        return

    if cmd == "/전체랭킹":
        reply_messages(event.reply_token, [format_all_ranking()])
        return

    if cmd == "/활동부족":
        reply_messages(event.reply_token, [format_inactive_members()])
        return

    if is_group_or_room and user_id:
        gained_madit = calculate_message_madit(user_text)
        if gained_madit > 0:
            now_ts = time.monotonic()
            with state_lock:
                prev_ts = last_counted_at.get(user_id, 0.0)
                if MIN_SPEECH_INTERVAL_SEC > 0 and (now_ts - prev_ts) < MIN_SPEECH_INTERVAL_SEC:
                    logger.info("[SPEECH] rate-limited user_id=%s text=%r", user_id, user_text)
                    return
                last_counted_at[user_id] = now_ts

            total = add_speech_count(user_id, name, gained_madit)
            logger.info(
                "[SPEECH] counted user_id=%s name=%s +%d (total=%d) text=%r",
                user_id,
                name,
                gained_madit,
                total,
                user_text,
            )


if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    app.run(host="0.0.0.0", port=port, debug=False)
