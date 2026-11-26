from datetime import datetime, timedelta
import json
import logging
import os
import random
import re
import threading
import time
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from flask import Flask, request, abort
from upstash_redis import Redis

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
UPSTASH_REDIS_REST_URL = (os.getenv("UPSTASH_REDIS_REST_URL") or "").strip()
UPSTASH_REDIS_REST_TOKEN = (os.getenv("UPSTASH_REDIS_REST_TOKEN") or "").strip()


def parse_id_list(*env_keys: str) -> list[str]:
    """쉼표로 구분된 다중 LINE ID 환경 변수 파싱 및 중복 제거"""
    combined = ",".join((os.getenv(k) or "") for k in env_keys)
    candidates = [item.strip() for item in combined.split(",") if item.strip()]
    seen: set[str] = set()
    ordered: list[str] = []
    for tid in candidates:
        if tid not in seen:
            seen.add(tid)
            ordered.append(tid)
    return ordered


# 건의·신고·관리자호출 알림을 수신할 관리자 user_id(U...) 또는 운영진 전용방 group_id(C...) 목록
ADMIN_TARGET_IDS: list[str] = parse_id_list("ADMIN_TARGET_IDS", "ADMIN_TARGET_ID")
ADMIN_TARGET_SET: set[str] = set(ADMIN_TARGET_IDS)

# 고민상담을 전담 수신할 상담 담당 운영진·상담사 user_id 목록 (미설정 시 ADMIN_TARGET_IDS 사용)
_raw_counseling_targets = parse_id_list("COUNSELING_TARGET_IDS", "COUNSELING_TARGET_ID")
COUNSELING_TARGET_IDS: list[str] = _raw_counseling_targets if _raw_counseling_targets else list(ADMIN_TARGET_IDS)
COUNSELING_TARGET_SET: set[str] = set(COUNSELING_TARGET_IDS)

# 관리자 전용 명령어를 실행할 수 있는 user_id 목록 (쉼표 구분: U111,U222)
ADMIN_USER_IDS: set[str] = {
    uid.strip()
    for uid in (os.getenv("ADMIN_USER_IDS") or "").split(",")
    if uid.strip()
}

if not CHANNEL_ACCESS_TOKEN or not CHANNEL_SECRET:
    raise ValueError("LINE_CHANNEL_ACCESS_TOKEN과 LINE_CHANNEL_SECRET 환경 변수를 설정해주세요.")

if not UPSTASH_REDIS_REST_URL or not UPSTASH_REDIS_REST_TOKEN:
    raise ValueError("UPSTASH_REDIS_REST_URL과 UPSTASH_REDIS_REST_TOKEN 환경 변수를 설정해주세요.")

app = Flask(__name__)
configuration = Configuration(access_token=CHANNEL_ACCESS_TOKEN)
handler = WebhookHandler(CHANNEL_SECRET)
redis_client = Redis(url=UPSTASH_REDIS_REST_URL, token=UPSTASH_REDIS_REST_TOKEN)

KST = ZoneInfo("Asia/Seoul")
RANKING_STORE_KEY = "abyss:rankings"
TARGETS_STORE_KEY = "abyss:targets"
MEMBERS_STORE_KEY = "abyss:members"
REPORTS_STORE_KEY = "abyss:reports"
COUNSELINGS_STORE_KEY = "abyss:counselings"
SONGS_STORE_KEY = "abyss:songs"
ADULT_LINKS_STORE_KEY = "abyss:adult_links"

# Upstash 무료 티어 명령어 절약용 메모리 캐시 재조회 주기(초)
REDIS_REFRESH_INTERVAL_SEC = 15.0

# 같은 유저의 연타 도배 방지 최소 간격(초). 0으로 두면 모든 유효 메시지 카운트
MIN_SPEECH_INTERVAL_SEC = 2.0
# 일일 활동부족 경고 기준 마디 수 (30마디 미만 시 경고 표시)
MIN_ACTIVITY_MADIT = 30
# 1개 메시지에서 인정되는 유효 단어(3글자 이상)의 최소 길이
MIN_WORD_LEN = 3

# 건의·신고·고민상담 도배 방지 쿨타임(초) 및 최소 내용 길이
REPORT_COOLDOWN_SEC = 300
MIN_REPORT_LEN = 5
MIN_COUNSELING_LEN = 5

# /관리자호출 도배 방지 쿨타임(초)
ADMIN_CALL_COOLDOWN_SEC = 60

# 오노추 시작 최소 등록 개수(5개) / 오야추 시작 최소 등록 개수(1개 이상)
MIN_SONG_RECOMMEND_POOL_SIZE = 5
MIN_ADULT_RECOMMEND_POOL_SIZE = 1

state_lock = threading.Lock()
last_counted_at: dict[str, float] = {}
last_reported_at: dict[str, float] = {}
last_counseled_at: dict[str, float] = {}
last_admin_called_at: dict[str, float] = {}
store_cache: dict[str, dict] = {}
store_loaded_at: dict[str, float] = {}

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

COUNSELING_GUIDE_MSG = (
    "🔒 비밀 고민상담 및 불편신고 안내\n"
    "[오른쪽 상단 메뉴 -> 멤버 -> 봇 친구추가]\n\n"
    "친구 추가 후 봇과의 1:1 채팅에서\n"
    "• /고민상담 [내용] (🔒 100% 비밀보장 · 상담 담당자 전용 전달)\n"
    "• /신고 [내용] 또는 /건의 [내용]\n"
    "으로 보내주시면 본방에 전혀 공개되지 않고 안전하게 전달됩니다."
)

# /명령어 (임시 1:1 전용)
PRIVATE_COMMAND_LIST_MSG = (
    "📌 봇 1:1 채팅 일반 멤버 명령어 안내 (임시 1:1 전용 운영 중)\n\n"
    "• /명령어 — 1:1 일반 멤버 명령어 보기\n"
    "• /관리자호출 [사유(선택)] — 지정된 관리자에게 호출 알림 전송\n"
    "• /고민상담 [내용] — 상담 담당 운영진·상담사에게만 비밀 전달\n"
    "  🔒 모든 상담 내용은 본방에 절대 공개되지 않으며,\n"
    "     지정된 담당 운영진·상담사 외에는 열람할 수 없습니다.\n"
    "• /신고 [내용] — 운영진에게 비공개 신고 접수\n"
    "• /건의 [내용] — 운영진에게 건의사항 전달\n"
    "• /노래신청 (가수)-(노래제목) — 통합 노래 리스트에 한 곡/여러 곡 등록\n"
    "  예) /노래신청 아이유-밤편지, 성시경-거리에서\n"
    "• /야동신청 (링크) — 오야추 링크 등록\n"
    "  예) /야동신청 https://example.com\n"
    "• /내랭킹 — 오늘 내 마디 수, 순위, 활동 상태 확인\n"
    "• /오노추 — 오늘의 통합 노래 추천 확인(전원 동일)\n"
    "• /오야추 — 오늘의 추천 링크 확인\n"
    "• /상담 — 비밀 고민상담·신고·건의 안내 다시 보기\n\n"
    "※ 현재 임시 운영 중으로 본방에서는 /환찌(/환츄), /상담만 동작합니다."
)

# /관리명령어 (1:1 채팅 전용)
ALL_COMMAND_LIST_MSG = (
    "🛠️ 전체 명령어 안내 (임시 1:1 전용 운영 중)\n\n"
    "[본방/1:1 공용 허용 명령어]\n"
    "• /환찌 (/환츄) — (본방/1:1 공용) 환츄/환찌 안내 보기\n"
    "• /상담 — (본방/1:1 공용) 1:1 비밀 고민상담·신고·건의 안내\n\n"
    "[일반 멤버 명령어 (임시 1:1 전용)]\n"
    "• /명령어 — 일반 멤버 명령어 보기\n"
    "• /관리자호출 [사유(선택)] — 지정된 관리자에게 호출 알림 전송\n"
    "• /내랭킹 — 오늘 내 마디 수, 순위, 활동 상태 확인\n"
    "• /오노추 — 오늘의 통합 노래 추천 확인(전원 동일)\n"
    "• /오야추 — 오늘의 추천 링크 확인\n"
    "• /고민상담 [내용] — 상담 담당자에게 비밀 고민상담 전달\n"
    "• /신고 [내용] — 운영진에게 비공개 신고 접수\n"
    "• /건의 [내용] — 운영진에게 건의사항 전달\n"
    "• /노래신청 (가수)-(노래제목) — 통합 노래 리스트 등록\n"
    "• /야동신청 (링크) — 오야추 링크 등록\n\n"
    "[관리명령어 (임시 1:1 전용)]\n"
    "• /전체랭킹 — 오늘 방 전체 마디 수 랭킹 확인\n"
    "• /활동부족 — 오늘 30마디 미만 인원 목록 확인\n"
    "• /관리명령어 — 전체 명령어(일반/관리 구분) 보기\n"
    "• /노래목록 — 전체 통합 노래 및 다음 랜덤 추천 후보 확인\n"
    "• /노래삭제 [번호(복수 가능) / 가수-제목] — 통합 노래 삭제\n"
    "• /노래큐초기화 — 노래 추천 큐 초기화\n"
    "• /링크목록 — 전체 추천 링크 목록 확인\n"
    "• /링크삭제 [번호(복수 가능) / URL] — 추천 링크 삭제\n"
    "• /링크큐초기화 — 링크 추천 큐 초기화\n"
    "• /방아이디 — 현재 채팅 ID 확인\n"
    "• /건의목록 — (수신처 전용) 최근 건의·신고 내역 조회\n"
    "• /상담목록 — (상담 담당 전용) 최근 비밀 고민상담 내역 조회"
)


def is_admin_user(user_id: str | None, chat_id: str = "") -> bool:
    """관리자 권한 체크 해제 — 모든 사용자가 관리 명령어 실행 가능"""
    return True


def is_counseling_staff(user_id: str | None, chat_id: str = "") -> bool:
    """상담 담당 운영진·상담사 권한 판별"""
    if COUNSELING_TARGET_SET:
        return bool((user_id and user_id in COUNSELING_TARGET_SET) or (chat_id and chat_id in COUNSELING_TARGET_SET))
    return bool(user_id or chat_id)


def today_key() -> str:
    return datetime.now(KST).strftime("%Y-%m-%d")


def yesterday_key() -> str:
    return (datetime.now(KST) - timedelta(days=1)).strftime("%Y-%m-%d")


def _parse_redis_json(raw_val) -> dict:
    if raw_val is None:
        return {}
    if isinstance(raw_val, dict):
        return raw_val
    if isinstance(raw_val, (bytes, bytearray)):
        raw_val = raw_val.decode("utf-8", errors="ignore")
    if isinstance(raw_val, str):
        if not raw_val.strip():
            return {}
        parsed = json.loads(raw_val)
        return parsed if isinstance(parsed, dict) else {}
    return {}


def load_json_dict_strict(key: str) -> dict:
    """
    Upstash Redis에서 반드시 최신 JSON 딕셔너리를 조회.
    네트워크/Redis 오류 시 빈 딕셔너리로 폴백하지 않고 예외를 발생시켜
    기존 저장 데이터의 0마디 덮어쓰기를 방지한다.
    """
    raw_val = redis_client.get(key)
    data = _parse_redis_json(raw_val)
    store_cache[key] = data
    store_loaded_at[key] = time.monotonic()
    return data


def load_json_dict(key: str, force_refresh: bool = False) -> dict:
    """Upstash Redis에서 JSON 딕셔너리 조회(단기 캐시 및 안전한 폴백 적용)"""
    now_ts = time.monotonic()
    cached = store_cache.get(key)
    last_ts = store_loaded_at.get(key, 0.0)
    if (
        not force_refresh
        and cached is not None
        and (now_ts - last_ts) < REDIS_REFRESH_INTERVAL_SEC
    ):
        return cached

    try:
        return load_json_dict_strict(key)
    except Exception as e:
        logger.warning("[REDIS] failed to load key=%s: %s", key, e)
        if cached is not None:
            return cached
        fallback: dict = {}
        store_cache.setdefault(key, fallback)
        return fallback


def save_json_dict(key: str, data: dict) -> None:
    """Upstash Redis에 JSON 문자열로 저장 및 메모리 캐시 동기화"""
    safe_dict = data if isinstance(data, dict) else {}
    store_cache[key] = safe_dict
    store_loaded_at[key] = time.monotonic()
    try:
        payload = json.dumps(safe_dict, ensure_ascii=False)
        redis_client.set(key, payload)
    except Exception as e:
        logger.warning("[REDIS] failed to save key=%s: %s", key, e)


ranking_data = load_json_dict(RANKING_STORE_KEY, force_refresh=True)
targets_data = load_json_dict(TARGETS_STORE_KEY, force_refresh=True)
members_data = load_json_dict(MEMBERS_STORE_KEY, force_refresh=True)
reports_data = load_json_dict(REPORTS_STORE_KEY, force_refresh=True)
counselings_data = load_json_dict(COUNSELINGS_STORE_KEY, force_refresh=True)
songs_data = load_json_dict(SONGS_STORE_KEY, force_refresh=True)
adult_links_data = load_json_dict(ADULT_LINKS_STORE_KEY, force_refresh=True)


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


def parse_comma_numbers(cleaned: str) -> tuple[bool, list[int], list[str]]:
    """
    쉼표로 구분된 번호 목록 파싱:
    - 모든 쉼표 분리 조각이 '#?숫자' 형식일 때만 다중 번호 모드로 인식
    - 반환: (is_multi_number_mode, 중복 제거된 양의 정수 리스트, 잘못된 토큰 리스트)
    """
    if "," not in cleaned:
        return False, [], []

    parts = [p.strip() for p in cleaned.split(",")]
    if any(not p for p in parts):
        return False, [], []

    parsed_nums: list[int] = []
    invalid_tokens: list[str] = []
    any_number = False

    for p in parts:
        m = re.fullmatch(r"#?(\d+)", p)
        if m:
            any_number = True
            val = int(m.group(1))
            if val >= 1:
                if val not in parsed_nums:
                    parsed_nums.append(val)
            else:
                invalid_tokens.append(p)
        else:
            invalid_tokens.append(p)

    if any_number and not invalid_tokens:
        return True, parsed_nums, []
    if any_number and invalid_tokens:
        return True, parsed_nums, invalid_tokens
    return False, [], []


def normalize_song_key(artist: str, title: str) -> str:
    """가수와 노래제목 일치 여부를 판별하기 위한 정규화 키"""
    norm_artist = re.sub(r"\s+", "", artist).lower()
    norm_title = re.sub(r"\s+", "", title).lower()
    return f"{norm_artist}:::{norm_title}"


def parse_single_song_chunk(chunk: str) -> tuple[str, str] | None:
    """단일 '(가수)-(노래제목)' 항목 파싱 — 제목에 하이픈이 포함된 경우도 첫 '-' 기준으로 분리"""
    cleaned = chunk.strip()
    if not cleaned:
        return None

    cleaned = re.sub(r"^[\d]+[\.)]\s*", "", cleaned).strip()
    cleaned = re.sub(r"^[•·\-*]\s*", "", cleaned).strip()

    if cleaned.startswith("(") and cleaned.endswith(")") and cleaned.count("(") == 1 and cleaned.count(")") == 1:
        cleaned = cleaned[1:-1].strip()

    if "-" not in cleaned:
        return None

    artist_part, title_part = cleaned.split("-", 1)
    artist = artist_part.strip()
    title = title_part.strip()
    if not artist or not title:
        return None
    return artist, title


def parse_songs_payload(payload: str) -> tuple[list[tuple[str, str]], list[str]]:
    """
    여러 곡 신청 페이로드 파싱:
    - 줄바꿈(\n), 쉼표(,), 세미콜론(;), 공백 포함 슬래시( / ) 등으로 곡 항목 분리
    - 반환: (유효한 [(가수, 제목), ...], 형식이 맞지 않은 원문 항목 [...])
    """
    raw = payload.strip()
    if not raw:
        return [], []

    line_chunks = re.split(r"[\r\n,;]+", raw)
    candidates: list[str] = []

    for line in line_chunks:
        part = line.strip()
        if not part:
            continue
        if " / " in part:
            slash_parts = [p.strip() for p in part.split(" / ") if p.strip()]
            valid_slash_pairs = sum(1 for p in slash_parts if parse_single_song_chunk(p) is not None)
            if valid_slash_pairs >= 2:
                candidates.extend(slash_parts)
                continue
        candidates.append(part)

    parsed_songs: list[tuple[str, str]] = []
    invalid_chunks: list[str] = []

    for chunk in candidates:
        parsed = parse_single_song_chunk(chunk)
        if parsed:
            parsed_songs.append(parsed)
        else:
            invalid_chunks.append(chunk)

    return parsed_songs, invalid_chunks


def normalize_link_key(raw_link: str) -> str:
    """링크 중복 비교용 정규화 키"""
    cleaned = raw_link.strip()
    if cleaned.startswith("<") and cleaned.endswith(">") and len(cleaned) > 2:
        cleaned = cleaned[1:-1].strip()
    cleaned = re.sub(r"\s+", "", cleaned)
    return cleaned.rstrip("/").lower()


def extract_link_payload(payload: str) -> str | None:
    """'/야동신청 (링크)' 또는 '/야동신청 링크'에서 링크 추출"""
    cleaned = payload.strip()
    if not cleaned:
        return None
    if cleaned.startswith("(") and cleaned.endswith(")") and cleaned.count("(") == 1 and cleaned.count(")") == 1:
        cleaned = cleaned[1:-1].strip()
    if not cleaned or any(ch.isspace() for ch in cleaned):
        return None
    return cleaned


def ensure_pool_schema(store: dict, items_key: str) -> list[dict]:
    """추천 풀 저장소 기본 구조 보정"""
    items = store.get(items_key)
    if not isinstance(items, list):
        items = []
        store[items_key] = items
    used_ids = store.get("used_ids")
    if not isinstance(used_ids, list):
        store["used_ids"] = []
    daily_picks = store.get("daily_picks")
    if not isinstance(daily_picks, dict):
        store["daily_picks"] = {}
    upcoming_ids = store.get("upcoming_ids")
    if not isinstance(upcoming_ids, list):
        store["upcoming_ids"] = []
    return items


def sync_upcoming_song_ids(store: dict, items: list[dict], day: str | None = None, prev_day: str | None = None) -> list[str]:
    """
    통합 노래 풀 기준 앞으로 추천될 일이 남은 노래 ID 목록('upcoming_ids') 동기화:
    - 이미 추천 완료된 used_ids 제외
    - 오늘/어제 추천된 곡 제외
    - 전체 곡을 한 바퀴 다 돌았을 때는 어제 추천 곡만 제외하고 남은 후보를 다시 구성
    """
    current_day = day or today_key()
    yesterday = prev_day or yesterday_key()

    item_map = {
        str(it.get("id")): it for it in items if isinstance(it, dict) and it.get("id")
    }
    all_ids = [str(it.get("id")) for it in items if isinstance(it, dict) and it.get("id")]

    used_ids: list[str] = []
    for uid in store.get("used_ids", []):
        sid = str(uid)
        if sid in item_map and sid not in used_ids:
            used_ids.append(sid)
    store["used_ids"] = used_ids

    daily_picks: dict = store.get("daily_picks") if isinstance(store.get("daily_picks"), dict) else {}
    today_pick_id = str(daily_picks.get(current_day) or "")
    yesterday_pick_id = str(daily_picks.get(yesterday) or "")

    stored_upcoming: list[str] = []
    for uid in store.get("upcoming_ids", []):
        sid = str(uid)
        if sid in item_map and sid not in used_ids and sid != today_pick_id and sid not in stored_upcoming:
            stored_upcoming.append(sid)

    for sid in all_ids:
        if sid not in used_ids and sid != today_pick_id and sid not in stored_upcoming:
            stored_upcoming.append(sid)

    if not stored_upcoming and all_ids:
        used_ids = []
        store["used_ids"] = []
        rebuilt = [sid for sid in all_ids if sid != yesterday_pick_id and sid != today_pick_id]
        if not rebuilt:
            rebuilt = [sid for sid in all_ids if sid != today_pick_id] or list(all_ids)
        stored_upcoming = rebuilt

    store["upcoming_ids"] = stored_upcoming
    return stored_upcoming


def _cleanup_deleted_pool_refs(store: dict, items: list[dict], deleted_ids: set[str]) -> None:
    """항목 삭제 시 used_ids, upcoming_ids, daily_picks 내 삭제된 ID 참조 정리"""
    if not deleted_ids:
        return
    valid_ids = {str(it.get("id")) for it in items if isinstance(it, dict) and it.get("id")}
    store["used_ids"] = [
        str(uid) for uid in (store.get("used_ids") or [])
        if str(uid) in valid_ids and str(uid) not in deleted_ids
    ]
    store["upcoming_ids"] = [
        str(uid) for uid in (store.get("upcoming_ids") or [])
        if str(uid) in valid_ids and str(uid) not in deleted_ids
    ]
    daily_picks = store.get("daily_picks")
    if isinstance(daily_picks, dict):
        cleaned_picks = {
            k: v for k, v in daily_picks.items()
            if str(v) in valid_ids and str(v) not in deleted_ids
        }
        store["daily_picks"] = cleaned_picks


def register_song(payload: str, requester_name: str) -> str:
    """통합 노래 리스트에 한 곡 또는 여러 곡 등록 처리 (1:1 채팅 전용)"""
    parsed_songs, invalid_chunks = parse_songs_payload(payload)
    if not parsed_songs:
        return (
            "🎵 올바른 형식으로 입력해 주세요. (여러 곡은 줄바꿈 또는 쉼표로 구분)\n"
            "예시) /노래신청 아이유-밤편지, 성시경-거리에서"
        )

    added_songs: list[tuple[str, str]] = []
    duplicate_songs: list[tuple[str, str]] = []
    batch_seen_keys: set[str] = set()
    now_str = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
    base_ts_ms = int(time.time() * 1000)

    with state_lock:
        latest = load_json_dict(SONGS_STORE_KEY, force_refresh=True)
        songs_data.clear()
        songs_data.update(latest)
        items = ensure_pool_schema(songs_data, "songs")

        existing_keys: set[str] = set()
        for item in items:
            if not isinstance(item, dict):
                continue
            ek = item.get("dedup_key") or normalize_song_key(
                str(item.get("artist", "")), str(item.get("title", ""))
            )
            if ek:
                existing_keys.add(ek)

        for idx, (artist, title) in enumerate(parsed_songs, start=1):
            song_key = normalize_song_key(artist, title)
            if song_key in existing_keys or song_key in batch_seen_keys:
                duplicate_songs.append((artist, title))
                continue

            batch_seen_keys.add(song_key)
            existing_keys.add(song_key)
            new_id = f"song_{base_ts_ms}_{len(items) + idx}"
            items.append(
                {
                    "id": new_id,
                    "artist": artist,
                    "title": title,
                    "dedup_key": song_key,
                    "requested_by": requester_name,
                    "created_at": now_str,
                }
            )
            added_songs.append((artist, title))

        if added_songs:
            sync_upcoming_song_ids(songs_data, items)
            save_json_dict(SONGS_STORE_KEY, songs_data)

        total_count = len(items)

    status_note = (
        "오노추가 시작되었습니다! /오노추로 확인해 보세요."
        if total_count >= MIN_SONG_RECOMMEND_POOL_SIZE
        else f"오노추 시작까지 {MIN_SONG_RECOMMEND_POOL_SIZE - total_count}곡 남았습니다."
    )

    if len(parsed_songs) == 1 and not invalid_chunks:
        if added_songs:
            artist, title = added_songs[0]
            return (
                f"✅ 통합 리스트에 노래가 등록되었습니다: {artist} - {title}\n"
                f"(현재 {total_count}/{MIN_SONG_RECOMMEND_POOL_SIZE}곡 · {status_note})"
            )
        artist, title = duplicate_songs[0]
        return f"🎶 이미 통합 리스트에 등록된 노래입니다: {artist} - {title}"

    lines: list[str] = []
    if added_songs:
        lines.append(f"✅ 통합 리스트에 노래 {len(added_songs)}곡이 등록되었습니다:")
        for artist, title in added_songs:
            lines.append(f"• {artist} - {title}")
    else:
        lines.append("🎵 새로 등록된 노래가 없습니다.")

    if duplicate_songs:
        lines.append(f"\n🎶 이미 등록된 중복 곡 ({len(duplicate_songs)}곡):")
        for artist, title in duplicate_songs:
            lines.append(f"• {artist} - {title}")

    if invalid_chunks:
        lines.append(f"\n⚠️ 형식 오류로 제외된 항목 ({len(invalid_chunks)}건 · '가수-노래제목' 형식 필요):")
        for bad in invalid_chunks[:5]:
            lines.append(f"• {bad}")
        if len(invalid_chunks) > 5:
            lines.append(f"• 외 {len(invalid_chunks) - 5}건")

    lines.append(f"\n(현재 통합 {total_count}/{MIN_SONG_RECOMMEND_POOL_SIZE}곡 · {status_note})")
    return "\n".join(lines)


def delete_song_item(query_text: str) -> str:
    """단일/복수 번호 또는 '가수-제목'/키워드로 통합 노래 리스트에서 곡 삭제 (1:1 채팅 전용)"""
    cleaned = query_text.strip()
    if not cleaned:
        return (
            "🗑️ 삭제할 곡 번호 또는 '가수-노래제목'을 입력해 주세요.\n"
            "• 단일 번호 삭제: /노래삭제 3\n"
            "• 여러 번호 삭제: /노래삭제 1, 3, 5\n"
            "• 곡명 삭제: /노래삭제 아이유-밤편지"
        )

    is_multi_num, num_list, invalid_tokens = parse_comma_numbers(cleaned)

    with state_lock:
        latest = load_json_dict(SONGS_STORE_KEY, force_refresh=True)
        songs_data.clear()
        songs_data.update(latest)
        items = ensure_pool_schema(songs_data, "songs")

        if not items:
            return "🎵 현재 통합 리스트에 등록된 노래가 없습니다."

        if is_multi_num:
            out_of_range = [n for n in num_list if not (1 <= n <= len(items))]
            valid_nums = [n for n in num_list if 1 <= n <= len(items)]

            removed_entries: list[tuple[int, str, str, str]] = []
            for idx_1based in sorted(valid_nums, reverse=True):
                removed = items.pop(idx_1based - 1)
                removed_entries.append(
                    (
                        idx_1based,
                        str(removed.get("id") or ""),
                        str(removed.get("artist") or ""),
                        str(removed.get("title") or ""),
                    )
                )
            removed_entries.sort(key=lambda x: x[0])

            deleted_ids = {rid for _, rid, _, _ in removed_entries if rid}
            if deleted_ids:
                _cleanup_deleted_pool_refs(songs_data, items, deleted_ids)
                sync_upcoming_song_ids(songs_data, items)
                save_json_dict(SONGS_STORE_KEY, songs_data)

            remain_count = len(items)
            lines: list[str] = []
            if removed_entries:
                lines.append(f"🗑️ 통합 리스트에서 노래 {len(removed_entries)}곡이 삭제되었습니다:")
                for num, _, artist, title in removed_entries:
                    lines.append(f"• {num}번: {artist} - {title}")
            else:
                lines.append("⚠️ 삭제된 노래가 없습니다.")

            if out_of_range:
                lines.append(
                    f"\n⚠️ 범위 밖 번호 ({', '.join(f'{n}번' for n in out_of_range)}) — "
                    f"삭제 전 기준 1번~{len(items) + len(removed_entries)}번 범위만 유효합니다."
                )
            if invalid_tokens:
                lines.append(f"\n⚠️ 잘못된 번호 형식: {', '.join(invalid_tokens)}")

            lines.append(f"\n(남은 통합 노래: {remain_count}곡)")
            return "\n".join(lines)

        target_index: int | None = None
        num_match = re.fullmatch(r"#?(\d+)", cleaned)
        if num_match:
            idx_1based = int(num_match.group(1))
            if 1 <= idx_1based <= len(items):
                target_index = idx_1based - 1
            else:
                return f"⚠️ 잘못된 번호입니다. 현재 1번부터 {len(items)}번까지 있습니다."
        else:
            parsed_pair = parse_single_song_chunk(cleaned)
            if parsed_pair:
                target_key = normalize_song_key(parsed_pair[0], parsed_pair[1])
                for i, it in enumerate(items):
                    if not isinstance(it, dict):
                        continue
                    ek = it.get("dedup_key") or normalize_song_key(
                        str(it.get("artist", "")), str(it.get("title", ""))
                    )
                    if ek == target_key:
                        target_index = i
                        break

            if target_index is None:
                norm_query = re.sub(r"\s+", "", cleaned).lower()
                matches: list[int] = []
                for i, it in enumerate(items):
                    if not isinstance(it, dict):
                        continue
                    combined = re.sub(
                        r"\s+", "", f"{it.get('artist', '')}-{it.get('title', '')}"
                    ).lower()
                    if norm_query and norm_query in combined:
                        matches.append(i)
                if len(matches) == 1:
                    target_index = matches[0]
                elif len(matches) > 1:
                    sample_lines = [
                        f"• {m + 1}. {items[m].get('artist', '')} - {items[m].get('title', '')}"
                        for m in matches[:5]
                    ]
                    return (
                        f"⚠️ '{cleaned}'와 일치하는 곡이 {len(matches)}건 있습니다. 번호로 지정해 주세요:\n"
                        + "\n".join(sample_lines)
                    )

        if target_index is None:
            return f"⚠️ '{cleaned}'에 해당하는 노래를 찾지 못했습니다. /노래목록으로 확인해 주세요."

        removed = items.pop(target_index)
        removed_id = str(removed.get("id") or "")
        removed_artist = str(removed.get("artist") or "")
        removed_title = str(removed.get("title") or "")

        _cleanup_deleted_pool_refs(songs_data, items, {removed_id} if removed_id else set())
        sync_upcoming_song_ids(songs_data, items)
        save_json_dict(SONGS_STORE_KEY, songs_data)
        remain_count = len(items)

    return (
        f"🗑️ 통합 리스트에서 노래가 삭제되었습니다: {removed_artist} - {removed_title}\n"
        f"(남은 통합 노래: {remain_count}곡)"
    )


def reset_song_queue() -> str:
    """노래 추천 이력(used_ids), 남은 후보(upcoming_ids), 오늘 추천 기록 초기화 (1:1 채팅 전용)"""
    day = today_key()
    with state_lock:
        latest = load_json_dict(SONGS_STORE_KEY, force_refresh=True)
        songs_data.clear()
        songs_data.update(latest)
        items = ensure_pool_schema(songs_data, "songs")
        total_count = len(items)

        songs_data["used_ids"] = []
        songs_data["upcoming_ids"] = []
        daily_picks = songs_data.get("daily_picks")
        if isinstance(daily_picks, dict):
            daily_picks.pop(day, None)
        else:
            songs_data["daily_picks"] = {}

        if total_count > 0:
            sync_upcoming_song_ids(songs_data, items, day=day)
        save_json_dict(SONGS_STORE_KEY, songs_data)

    return (
        f"🔄 노래 추천 큐가 초기화되었습니다.\n"
        f"• 등록 유지 곡: {total_count}곡\n"
        f"• 추천 이력 및 오늘({day}) 추천 기록이 초기화되어 새 순환으로 진행됩니다."
    )


def format_song_recommendation_msg(day: str, artist: str, title: str) -> str:
    return (
        f"🎧 [{day}] 오늘의 통합 노래 추천 [오노추]\n"
        f"• 가수: {artist}\n"
        f"• 노래: {title}\n\n"
        f"※ 노래 신청은 봇과의 1:1 채팅에서\n"
        f"/노래신청 (가수)-(노래제목)으로 해주세요."
    )


def get_today_song_recommendation() -> str:
    """
    오늘의 통합 노래 추천(오노추):
    - 통합 노래 풀 5개 이상부터 시작
    - 당일(today_key) 이미 결정된 곡이 있으면 어떤 멤버가 입력하든 동일한 오늘 추천 곡 반환
    - 앞으로 추천될 일이 남은 목록(upcoming_ids)에서 랜덤 선정
    - 전체 곡 소진 전까지 중복 미출력
    - 어제 추천된 곡이 내일(오늘) 첫 추천에서 다시 나오지 않도록 제외
    """
    day = today_key()
    prev_day = yesterday_key()

    with state_lock:
        latest = load_json_dict(SONGS_STORE_KEY, force_refresh=True)
        songs_data.clear()
        songs_data.update(latest)
        items = ensure_pool_schema(songs_data, "songs")
        total_count = len(items)

        if total_count < MIN_SONG_RECOMMEND_POOL_SIZE:
            return (
                f"🎵 오노추는 통합 노래가 {MIN_SONG_RECOMMEND_POOL_SIZE}곡 이상 모여야 시작됩니다.\n"
                f"현재 등록된 노래: {total_count}/{MIN_SONG_RECOMMEND_POOL_SIZE}곡 "
                f"({MIN_SONG_RECOMMEND_POOL_SIZE - total_count}곡 더 필요해요!)\n\n"
                f"※ 노래 신청은 봇과의 1:1 채팅에서\n"
                f"/노래신청 (가수)-(노래제목)으로 해주세요."
            )

        daily_picks: dict = songs_data["daily_picks"]
        today_pick_id = daily_picks.get(day)
        item_map = {
            str(it.get("id")): it for it in items if isinstance(it, dict) and it.get("id")
        }

        if today_pick_id and str(today_pick_id) in item_map:
            prev_upcoming = list(songs_data.get("upcoming_ids") or [])
            sync_upcoming_song_ids(songs_data, items, day=day, prev_day=prev_day)
            if songs_data.get("upcoming_ids") != prev_upcoming:
                save_json_dict(SONGS_STORE_KEY, songs_data)
            picked = item_map[str(today_pick_id)]
            return format_song_recommendation_msg(
                day=day,
                artist=str(picked.get("artist", "")),
                title=str(picked.get("title", "")),
            )

        upcoming_ids = sync_upcoming_song_ids(songs_data, items, day=day, prev_day=prev_day)
        yesterday_pick_id = str(daily_picks.get(prev_day) or "")

        candidates = [sid for sid in upcoming_ids if sid != yesterday_pick_id]
        if not candidates:
            candidates = list(upcoming_ids) or list(item_map.keys())

        chosen_id = random.choice(candidates)
        used_ids: list[str] = [
            str(uid) for uid in songs_data.get("used_ids", []) if str(uid) in item_map
        ]
        if chosen_id not in used_ids:
            used_ids.append(chosen_id)
        songs_data["used_ids"] = used_ids
        daily_picks[day] = chosen_id

        remaining_after_pick = [sid for sid in upcoming_ids if sid != chosen_id]
        songs_data["upcoming_ids"] = remaining_after_pick
        sync_upcoming_song_ids(songs_data, items, day=day, prev_day=prev_day)

        save_json_dict(SONGS_STORE_KEY, songs_data)

        picked = item_map[chosen_id]
        return format_song_recommendation_msg(
            day=day,
            artist=str(picked.get("artist", "")),
            title=str(picked.get("title", "")),
        )


def format_song_lists_msg() -> str:
    """통합 노래 목록과 앞으로 추천될 일이 남은 다음 랜덤 추천 후보 목록을 함께 출력 (1:1 채팅 전용)"""
    day = today_key()
    prev_day = yesterday_key()

    with state_lock:
        latest = load_json_dict(SONGS_STORE_KEY, force_refresh=True)
        songs_data.clear()
        songs_data.update(latest)
        items = ensure_pool_schema(songs_data, "songs")
        prev_upcoming = list(songs_data.get("upcoming_ids") or [])
        upcoming_ids = sync_upcoming_song_ids(songs_data, items, day=day, prev_day=prev_day)
        if songs_data.get("upcoming_ids") != prev_upcoming:
            save_json_dict(SONGS_STORE_KEY, songs_data)

    if not items:
        return (
            "🎵 현재 통합 리스트에 등록된 노래가 없습니다.\n\n"
            "※ 노래 신청은 봇과의 1:1 채팅에서\n"
            "/노래신청 (가수)-(노래제목)으로 해주세요."
        )

    item_map = {
        str(it.get("id")): it for it in items if isinstance(it, dict) and it.get("id")
    }
    used_set = {str(uid) for uid in songs_data.get("used_ids", [])}
    today_pick_id = str((songs_data.get("daily_picks") or {}).get(day) or "")
    yesterday_pick_id = str((songs_data.get("daily_picks") or {}).get(prev_day) or "")

    all_lines = [f"🎶 전체 통합 노래 목록 (총 {len(items)}곡 · 기준: {MIN_SONG_RECOMMEND_POOL_SIZE}곡)"]
    for idx, it in enumerate(items, start=1):
        if not isinstance(it, dict):
            continue
        sid = str(it.get("id") or "")
        artist = str(it.get("artist") or "")
        title = str(it.get("title") or "")
        if sid == today_pick_id:
            badge = " [오늘 추천]"
        elif sid in used_set:
            badge = " [추천 완료]"
        else:
            badge = " [추천 대기]"
        all_lines.append(f"{idx}. {artist} - {title}{badge}")

    next_random_candidates = [sid for sid in upcoming_ids if sid != yesterday_pick_id] or list(upcoming_ids)
    next_lines = [f"\n🎲 앞으로 추천될 남은 노래 (다음 랜덤 후보 {len(next_random_candidates)}곡)"]
    if next_random_candidates:
        for idx, sid in enumerate(next_random_candidates, start=1):
            it = item_map.get(sid)
            if isinstance(it, dict):
                next_lines.append(f"{idx}. {it.get('artist', '')} - {it.get('title', '')}")
    else:
        next_lines.append("• 대기 중인 노래가 없습니다.")

    return "\n".join(all_lines + next_lines)


def register_adult_link(payload: str, requester_name: str) -> str:
    """1:1 채팅 야동 링크 신청 처리 — 중복 링크 자동 차단 (개수 제한 없음)"""
    link = extract_link_payload(payload)
    if not link:
        return (
            "🔞 올바른 형식으로 입력해 주세요.\n"
            "예시) /야동신청 https://example.com/video"
        )

    link_key = normalize_link_key(link)

    with state_lock:
        latest = load_json_dict(ADULT_LINKS_STORE_KEY, force_refresh=True)
        adult_links_data.clear()
        adult_links_data.update(latest)
        items = ensure_pool_schema(adult_links_data, "links")

        for item in items:
            if not isinstance(item, dict):
                continue
            existing_key = item.get("dedup_key") or normalize_link_key(str(item.get("url", "")))
            if existing_key == link_key:
                return "🔞 이미 등록된 링크입니다."

        new_id = f"link_{int(time.time() * 1000)}_{len(items) + 1}"
        items.append(
            {
                "id": new_id,
                "url": link,
                "dedup_key": link_key,
                "requested_by": requester_name,
                "created_at": datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S"),
            }
        )
        save_json_dict(ADULT_LINKS_STORE_KEY, adult_links_data)
        total_count = len(items)

    return (
        f"✅ 링크가 등록되었습니다.\n"
        f"(현재 등록된 링크: {total_count}개 · 바로 /오야추로 확인할 수 있습니다.)"
    )


def format_adult_link_list_msg() -> str:
    """등록된 전체 추천 링크 목록 조회 (1:1 채팅 전용)"""
    day = today_key()
    with state_lock:
        latest = load_json_dict(ADULT_LINKS_STORE_KEY, force_refresh=True)
        adult_links_data.clear()
        adult_links_data.update(latest)
        items = ensure_pool_schema(adult_links_data, "links")

    if not items:
        return "🔞 현재 등록된 추천 링크가 없습니다."

    used_set = {str(uid) for uid in (adult_links_data.get("used_ids") or [])}
    today_pick_id = str((adult_links_data.get("daily_picks") or {}).get(day) or "")

    lines = [f"🔗 전체 추천 링크 목록 (총 {len(items)}개)"]
    for idx, it in enumerate(items, start=1):
        if not isinstance(it, dict):
            continue
        lid = str(it.get("id") or "")
        url = str(it.get("url") or "")
        if lid == today_pick_id:
            badge = " [오늘 추천]"
        elif lid in used_set:
            badge = " [추천 완료]"
        else:
            badge = " [추천 대기]"
        lines.append(f"{idx}. {url}{badge}")

    return "\n".join(lines)


def delete_adult_link_item(query_text: str) -> str:
    """단일/복수 번호 또는 URL 일부로 추천 링크 삭제 (1:1 채팅 전용)"""
    cleaned = query_text.strip()
    if not cleaned:
        return (
            "🗑️ 삭제할 링크 번호 또는 URL 일부를 입력해 주세요.\n"
            "• 단일 번호 삭제: /링크삭제 2\n"
            "• 여러 번호 삭제: /링크삭제 2, 4, 6\n"
            "• URL 삭제: /링크삭제 example.com"
        )

    is_multi_num, num_list, invalid_tokens = parse_comma_numbers(cleaned)

    with state_lock:
        latest = load_json_dict(ADULT_LINKS_STORE_KEY, force_refresh=True)
        adult_links_data.clear()
        adult_links_data.update(latest)
        items = ensure_pool_schema(adult_links_data, "links")

        if not items:
            return "🔞 현재 등록된 추천 링크가 없습니다."

        if is_multi_num:
            out_of_range = [n for n in num_list if not (1 <= n <= len(items))]
            valid_nums = [n for n in num_list if 1 <= n <= len(items)]

            removed_entries: list[tuple[int, str, str]] = []
            for idx_1based in sorted(valid_nums, reverse=True):
                removed = items.pop(idx_1based - 1)
                removed_entries.append(
                    (
                        idx_1based,
                        str(removed.get("id") or ""),
                        str(removed.get("url") or ""),
                    )
                )
            removed_entries.sort(key=lambda x: x[0])

            deleted_ids = {rid for _, rid, _ in removed_entries if rid}
            if deleted_ids:
                _cleanup_deleted_pool_refs(adult_links_data, items, deleted_ids)
                save_json_dict(ADULT_LINKS_STORE_KEY, adult_links_data)

            remain_count = len(items)
            lines: list[str] = []
            if removed_entries:
                lines.append(f"🗑️ 추천 링크 {len(removed_entries)}개가 삭제되었습니다:")
                for num, _, url in removed_entries:
                    lines.append(f"• {num}번: {url}")
            else:
                lines.append("⚠️ 삭제된 링크가 없습니다.")

            if out_of_range:
                lines.append(
                    f"\n⚠️ 범위 밖 번호 ({', '.join(f'{n}번' for n in out_of_range)}) — "
                    f"삭제 전 기준 1번~{len(items) + len(removed_entries)}번 범위만 유효합니다."
                )
            if invalid_tokens:
                lines.append(f"\n⚠️ 잘못된 번호 형식: {', '.join(invalid_tokens)}")

            lines.append(f"\n(남은 등록 링크: {remain_count}개)")
            return "\n".join(lines)

        target_index: int | None = None
        num_match = re.fullmatch(r"#?(\d+)", cleaned)
        if num_match:
            idx_1based = int(num_match.group(1))
            if 1 <= idx_1based <= len(items):
                target_index = idx_1based - 1
            else:
                return f"⚠️ 잘못된 번호입니다. 현재 1번부터 {len(items)}번까지 있습니다."
        else:
            norm_key = normalize_link_key(cleaned)
            matches: list[int] = []
            for i, it in enumerate(items):
                if not isinstance(it, dict):
                    continue
                url_str = str(it.get("url") or "")
                ek = str(it.get("dedup_key") or normalize_link_key(url_str))
                if (norm_key and norm_key in ek) or (cleaned.lower() in url_str.lower()):
                    matches.append(i)
            if len(matches) == 1:
                target_index = matches[0]
            elif len(matches) > 1:
                sample_lines = [
                    f"• {m + 1}. {items[m].get('url', '')}"
                    for m in matches[:5]
                ]
                return (
                    f"⚠️ '{cleaned}'와 일치하는 링크가 {len(matches)}건 있습니다. 번호로 지정해 주세요:\n"
                    + "\n".join(sample_lines)
                )

        if target_index is None:
            return f"⚠️ '{cleaned}'에 해당하는 링크를 찾지 못했습니다. /링크목록으로 확인해 주세요."

        removed = items.pop(target_index)
        removed_id = str(removed.get("id") or "")
        removed_url = str(removed.get("url") or "")

        _cleanup_deleted_pool_refs(adult_links_data, items, {removed_id} if removed_id else set())
        save_json_dict(ADULT_LINKS_STORE_KEY, adult_links_data)
        remain_count = len(items)

    return (
        f"🗑️ 추천 링크가 삭제되었습니다: {removed_url}\n"
        f"(남은 등록 링크: {remain_count}개)"
    )


def reset_adult_link_queue() -> str:
    """링크 추천 이력(used_ids) 및 오늘 추천 기록 초기화 (1:1 채팅 전용)"""
    day = today_key()
    with state_lock:
        latest = load_json_dict(ADULT_LINKS_STORE_KEY, force_refresh=True)
        adult_links_data.clear()
        adult_links_data.update(latest)
        items = ensure_pool_schema(adult_links_data, "links")
        total_count = len(items)

        adult_links_data["used_ids"] = []
        adult_links_data["upcoming_ids"] = []
        daily_picks = adult_links_data.get("daily_picks")
        if isinstance(daily_picks, dict):
            daily_picks.pop(day, None)
        else:
            adult_links_data["daily_picks"] = {}

        save_json_dict(ADULT_LINKS_STORE_KEY, adult_links_data)

    return (
        f"🔄 링크 추천 큐가 초기화되었습니다.\n"
        f"• 등록 유지 링크: {total_count}개\n"
        f"• 추천 이력 및 오늘({day}) 추천 기록이 초기화되어 새 순환으로 진행됩니다."
    )


def format_adult_link_recommendation_msg(day: str, url: str) -> str:
    return (
        f"🔞 [{day}] 오늘의 추천 [오야추]\n"
        f"{url}\n\n"
        f"※ 링크 신청은 봇과의 1:1 채팅에서\n"
        f"/야동신청 (링크)로 해주세요."
    )


def get_today_adult_link_recommendation() -> str:
    """
    오늘의 야동 추천(오야추):
    - 개수 제한 없이 1개 이상이면 바로 시작
    - 오늘 이미 뽑힌 링크가 있으면 오늘 값 반환
    - 전체 링크 소진 전까지 중복 미출력
    - 어제 추천된 링크가 내일(오늘) 첫 추천에서 다시 나오지 않도록 제외
    """
    day = today_key()
    prev_day = yesterday_key()

    with state_lock:
        latest = load_json_dict(ADULT_LINKS_STORE_KEY, force_refresh=True)
        adult_links_data.clear()
        adult_links_data.update(latest)
        items = ensure_pool_schema(adult_links_data, "links")
        total_count = len(items)

        if total_count < MIN_ADULT_RECOMMEND_POOL_SIZE:
            return (
                f"🔞 아직 등록된 추천 링크가 없습니다.\n\n"
                f"※ 링크 신청은 봇과의 1:1 채팅에서\n"
                f"/야동신청 (링크)로 해주세요."
            )

        daily_picks: dict = adult_links_data["daily_picks"]
        today_pick_id = daily_picks.get(day)
        item_map = {
            str(it.get("id")): it for it in items if isinstance(it, dict) and it.get("id")
        }

        if today_pick_id and str(today_pick_id) in item_map:
            picked = item_map[str(today_pick_id)]
            return format_adult_link_recommendation_msg(
                day=day,
                url=str(picked.get("url", "")),
            )

        used_ids: list[str] = [
            str(uid) for uid in adult_links_data.get("used_ids", []) if str(uid) in item_map
        ]
        yesterday_pick_id = daily_picks.get(prev_day)

        candidates = [
            item_id for item_id in item_map.keys()
            if item_id not in used_ids and item_id != yesterday_pick_id
        ]

        if not candidates:
            used_ids = []
            candidates = [
                item_id for item_id in item_map.keys()
                if item_id != yesterday_pick_id
            ] or list(item_map.keys())

        chosen_id = random.choice(candidates)
        used_ids.append(chosen_id)
        adult_links_data["used_ids"] = used_ids
        daily_picks[day] = chosen_id
        save_json_dict(ADULT_LINKS_STORE_KEY, adult_links_data)

        picked = item_map[chosen_id]
        return format_adult_link_recommendation_msg(
            day=day,
            url=str(picked.get("url", "")),
        )


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

    if target_id in ADMIN_TARGET_SET or target_id in COUNSELING_TARGET_SET:
        return

    with state_lock:
        try:
            latest = load_json_dict_strict(TARGETS_STORE_KEY)
            targets_data.update(latest)
            if target_id not in targets_data:
                targets_data[target_id] = True
                save_json_dict(TARGETS_STORE_KEY, targets_data)
                logger.info("[TARGET] registered chat target=%s", target_id)
        except Exception as e:
            logger.warning("[TARGET] skipped registering target=%s due to redis error: %s", target_id, e)


def get_chat_targets() -> list[str]:
    with state_lock:
        latest = load_json_dict(TARGETS_STORE_KEY)
        targets_data.update(latest)
        return [
            tid for tid in targets_data.keys()
            if tid not in ADMIN_TARGET_SET and tid not in COUNSELING_TARGET_SET
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

    return uid[-6:] if uid != "unknown" else "미확인"


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
        try:
            latest = load_json_dict_strict(MEMBERS_STORE_KEY)
            members_data.update(latest)
            existing = members_data.get(user_id)
            existing_name = existing.get("name") if isinstance(existing, dict) else None
            resolved_name = name or existing_name or user_id[-6:]
            if not isinstance(existing, dict) or existing.get("name") != resolved_name:
                members_data[user_id] = {
                    "name": resolved_name,
                    "updated_at": datetime.now(KST).isoformat(),
                }
                save_json_dict(MEMBERS_STORE_KEY, members_data)
        except Exception as e:
            logger.warning("[MEMBER] skipped upsert due to redis error: %s", e)


def remove_member(user_id: str | None, remove_today_ranking: bool = True) -> None:
    """퇴장한 멤버를 상시 멤버 목록 및 오늘 랭킹에서 자동 제외"""
    if not user_id:
        return
    day = today_key()
    with state_lock:
        try:
            latest_members = load_json_dict_strict(MEMBERS_STORE_KEY)
            members_data.clear()
            members_data.update(latest_members)
            removed_entry = members_data.pop(user_id, None)
            removed_name = removed_entry.get("name") if isinstance(removed_entry, dict) else None
            members_changed = removed_name is not None or user_id in latest_members
            if members_changed:
                save_json_dict(MEMBERS_STORE_KEY, members_data)
        except Exception as e:
            logger.warning("[MEMBER_LEFT] failed to update members store: %s", e)
            members_changed = False
            removed_name = None

        ranking_changed = False
        if remove_today_ranking:
            try:
                latest_rank = load_json_dict_strict(RANKING_STORE_KEY)
                ranking_data.clear()
                ranking_data.update(latest_rank)
                day_board = ranking_data.get(day)
                if isinstance(day_board, dict) and user_id in day_board:
                    rank_info = day_board.pop(user_id, None)
                    if not removed_name and isinstance(rank_info, dict):
                        removed_name = rank_info.get("name")
                    ranking_changed = True
                    save_json_dict(RANKING_STORE_KEY, ranking_data)
            except Exception as e:
                logger.warning("[MEMBER_LEFT] failed to update ranking store: %s", e)

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
        latest = load_json_dict(MEMBERS_STORE_KEY)
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
        try:
            latest = load_json_dict_strict(RANKING_STORE_KEY)
            ranking_data.update(latest)
            day_board = ranking_data.setdefault(day, {})
            if not isinstance(day_board, dict):
                day_board = {}
                ranking_data[day] = day_board
            if user_id not in day_board or not isinstance(day_board.get(user_id), dict):
                day_board[user_id] = {"name": name or user_id[-6:], "madit": 0}
                save_json_dict(RANKING_STORE_KEY, ranking_data)
            elif name and day_board[user_id].get("name") != name:
                day_board[user_id]["name"] = name
                save_json_dict(RANKING_STORE_KEY, ranking_data)
        except Exception as e:
            logger.warning("[RANKING] skipped ensure_known_member due to redis error: %s", e)


def add_speech_count(user_id: str, name: str, amount: int = 1) -> int:
    """이미 조회된 name을 받아 state_lock 안에서만 안전하게 상태 갱신 및 저장"""
    upsert_member(user_id, name)
    day = today_key()
    with state_lock:
        try:
            latest = load_json_dict_strict(RANKING_STORE_KEY)
            ranking_data.update(latest)
            redis_ok = True
        except Exception as e:
            logger.warning("[SPEECH] redis load failed, updating in-memory state only: %s", e)
            redis_ok = False

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
        if redis_ok:
            save_json_dict(RANKING_STORE_KEY, ranking_data)
        return new_total


def get_sorted_today_entries() -> list[tuple[str, dict]]:
    """
    현재 방에 남아 있는 멤버(오늘 0마디 포함) 기준으로 오늘 발언 수 내림차순 정렬.
    Redis 일시 장애 시에도 메모리에 있는 오늘 기록이 0으로 초기화되지 않도록 보호한다.
    """
    day = today_key()
    with state_lock:
        try:
            latest_rank = load_json_dict_strict(RANKING_STORE_KEY)
            if latest_rank:
                ranking_data.update(latest_rank)
        except Exception as e:
            logger.warning("[RANKING] using cached ranking_data due to redis error: %s", e)

        try:
            latest_members = load_json_dict_strict(MEMBERS_STORE_KEY)
            if latest_members:
                members_data.clear()
                members_data.update(latest_members)
        except Exception as e:
            logger.warning("[RANKING] using cached members_data due to redis error: %s", e)

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
                if not uid or not isinstance(info, dict):
                    continue
                if uid not in merged_today and int(info.get("madit", 0)) <= 0:
                    continue
                prev_name = (merged_today.get(uid) or {}).get("name") or uid[-6:]
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


def send_admin_call(
    caller_id: str,
    caller_name: str,
    chat_label: str,
    reason: str = "",
) -> tuple[bool, str]:
    """
    /관리자호출 실행 시 ADMIN_TARGET_IDS에 등록된 관리자들에게 호출 알림 푸시 전송
    """
    if not ADMIN_TARGET_IDS:
        return (
            False,
            "⚠️ 현재 호출 알림을 받을 관리자 수신처가 설정되지 않았습니다.",
        )

    resolved_caller_name = (
        caller_name.strip()
        if caller_name and caller_name.strip()
        else (caller_id[-6:] if caller_id else "미확인")
    )

    now_ts = time.monotonic()
    with state_lock:
        prev_ts = last_admin_called_at.get(caller_id, 0.0)
        elapsed = now_ts - prev_ts
        if prev_ts > 0 and elapsed < ADMIN_CALL_COOLDOWN_SEC:
            remain_sec = int(ADMIN_CALL_COOLDOWN_SEC - elapsed) + 1
            return (
                False,
                f"⏳ 연속 호출 방지를 위해 {remain_sec}초 후에 다시 호출할 수 있습니다.",
            )
        last_admin_called_at[caller_id] = now_ts

    now_kst_str = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
    reason_block = f"\n• 호출 사유: {reason}" if reason else ""
    alert_text = (
        f"🚨 [관리자 호출 알림]\n"
        f"• 호출 시각: {now_kst_str}\n"
        f"• 호출자: {resolved_caller_name}\n"
        f"• 위치: {chat_label}{reason_block}"
    )

    delivered_count = 0
    try:
        with ApiClient(configuration) as api_client:
            line_bot_api = MessagingApi(api_client)
            for target_id in ADMIN_TARGET_IDS:
                try:
                    line_bot_api.push_message(
                        PushMessageRequest(
                            to=target_id,
                            messages=[TextMessage(text=alert_text)],
                        )
                    )
                    delivered_count += 1
                except Exception:
                    logger.exception(
                        "[ADMIN_CALL] failed to push alert to target=%s",
                        target_id,
                    )
        logger.info(
            "[ADMIN_CALL] caller=%s (%s) delivered=%d/%d",
            resolved_caller_name,
            caller_id,
            delivered_count,
            len(ADMIN_TARGET_IDS),
        )
    except Exception:
        logger.exception("[ADMIN_CALL] failed to initialize API client")
        return (
            False,
            "⚠️ 관리자 호출 알림 전송 중 일시적인 오류가 발생했습니다. 잠시 후 다시 시도해 주세요.",
        )

    if delivered_count == 0:
        return (
            False,
            "⚠️ 관리자 호출 알림 전송 중 일시적인 오류가 발생했습니다. 잠시 후 다시 시도해 주세요.",
        )

    return (
        True,
        "📣 관리자에게 호출 알림을 전송했습니다.",
    )


def save_and_forward_report(
    cooldown_key: str,
    reporter_name: str,
    kind: str,
    content: str,
) -> tuple[bool, str]:
    """
    1:1 개인 채팅으로 접수된 건의·신고를 작성자 닉네임과 함께 저장하고
    ADMIN_TARGET_IDS에 등록된 관리자에게 푸시 전달
    """
    if not ADMIN_TARGET_IDS:
        return (
            False,
            "⚠️ 현재 운영진 수신처가 아직 연결되지 않아 접수할 수 없습니다. 잠시 후 다시 시도해 주세요.",
        )

    resolved_reporter_name = (
        reporter_name.strip()
        if reporter_name and reporter_name.strip()
        else (cooldown_key[-6:] if cooldown_key else "미확인")
    )

    now_ts = time.monotonic()
    with state_lock:
        prev_ts = last_reported_at.get(cooldown_key, 0.0)
        elapsed = now_ts - prev_ts
        if prev_ts > 0 and elapsed < REPORT_COOLDOWN_SEC:
            remain_sec = int(REPORT_COOLDOWN_SEC - elapsed) + 1
            return (
                False,
                f"⏳ 연속 접수 방지를 위해 {remain_sec}초 후에 다시 보낼 수 있습니다.",
            )

        now_kst_str = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")

        latest = load_json_dict(REPORTS_STORE_KEY)
        reports_data.update(latest)
        items = reports_data.get("items")
        if not isinstance(items, list):
            items = []
        reports_data["items"] = items

        report_no = len(items) + 1
        entry = {
            "no": report_no,
            "kind": kind,
            "reporter_name": resolved_reporter_name,
            "content": content,
            "created_at": now_kst_str,
        }
        items.append(entry)
        if len(items) > 100:
            reports_data["items"] = items[-100:]
        save_json_dict(REPORTS_STORE_KEY, reports_data)
        last_reported_at[cooldown_key] = now_ts

    icon = "🚨" if kind == "신고" else "📩"
    admin_text = (
        f"{icon} [{kind} 접수 #{report_no}]\n"
        f"• 접수 시각: {now_kst_str}\n"
        f"• 작성자: {resolved_reporter_name}\n"
        f"• 내용:\n{content}"
    )

    delivered_count = 0
    try:
        with ApiClient(configuration) as api_client:
            line_bot_api = MessagingApi(api_client)
            for target_id in ADMIN_TARGET_IDS:
                try:
                    line_bot_api.push_message(
                        PushMessageRequest(
                            to=target_id,
                            messages=[TextMessage(text=admin_text)],
                        )
                    )
                    delivered_count += 1
                except Exception:
                    logger.exception(
                        "[REPORT] failed to push report no=%d to target=%s",
                        report_no,
                        target_id,
                    )
        logger.info(
            "[REPORT] forwarded no=%d kind=%s reporter=%s delivered=%d/%d",
            report_no,
            kind,
            resolved_reporter_name,
            delivered_count,
            len(ADMIN_TARGET_IDS),
        )
    except Exception:
        logger.exception("[REPORT] failed to initialize API client for report no=%d", report_no)
        return (
            False,
            "⚠️ 운영진 알림 전송 중 일시적인 오류가 발생했습니다. 잠시 후 다시 시도해 주세요.",
        )

    if delivered_count == 0:
        return (
            False,
            "⚠️ 운영진 알림 전송 중 일시적인 오류가 발생했습니다. 잠시 후 다시 시도해 주세요.",
        )

    return (
        True,
        f"✅ {kind}가 정상 접수되었습니다. (접수번호 #{report_no})\n운영진에게 전달되었습니다.",
    )


def save_and_forward_counseling(
    cooldown_key: str,
    requester_name: str,
    content: str,
) -> tuple[bool, str]:
    """
    1:1 개인 채팅으로 접수된 고민상담을 상담 담당 운영진·상담사(COUNSELING_TARGET_IDS)에게만
    비밀리에 전달하고 별도 저장소에 보관
    """
    if not COUNSELING_TARGET_IDS:
        return (
            False,
            "⚠️ 현재 상담 담당 운영진·상담사 수신처가 연결되지 않았습니다. 잠시 후 다시 시도해 주세요.",
        )

    resolved_name = (
        requester_name.strip()
        if requester_name and requester_name.strip()
        else (cooldown_key[-6:] if cooldown_key else "미확인")
    )

    now_ts = time.monotonic()
    with state_lock:
        prev_ts = last_counseled_at.get(cooldown_key, 0.0)
        elapsed = now_ts - prev_ts
        if prev_ts > 0 and elapsed < REPORT_COOLDOWN_SEC:
            remain_sec = int(REPORT_COOLDOWN_SEC - elapsed) + 1
            return (
                False,
                f"⏳ 연속 접수 방지를 위해 {remain_sec}초 후에 다시 보낼 수 있습니다.",
            )

        now_kst_str = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")

        latest = load_json_dict(COUNSELINGS_STORE_KEY, force_refresh=True)
        counselings_data.clear()
        counselings_data.update(latest)
        items = counselings_data.get("items")
        if not isinstance(items, list):
            items = []
        counselings_data["items"] = items

        counseling_no = len(items) + 1
        entry = {
            "no": counseling_no,
            "requester_name": resolved_name,
            "content": content,
            "created_at": now_kst_str,
        }
        items.append(entry)
        if len(items) > 100:
            counselings_data["items"] = items[-100:]
        save_json_dict(COUNSELINGS_STORE_KEY, counselings_data)
        last_counseled_at[cooldown_key] = now_ts

    counselor_text = (
        f"🔒 [비밀 고민상담 접수 #{counseling_no}]\n"
        f"• 접수 시각: {now_kst_str}\n"
        f"• 신청자: {resolved_name}\n"
        f"• 상담 내용:\n{content}\n\n"
        f"⚠️ [비밀보장 엄수] 본 상담 내용은 상담 담당 운영진·상담사 전용으로만 전달되었으며 외부·본방 유출을 엄격히 금지합니다."
    )

    delivered_count = 0
    try:
        with ApiClient(configuration) as api_client:
            line_bot_api = MessagingApi(api_client)
            for target_id in COUNSELING_TARGET_IDS:
                try:
                    line_bot_api.push_message(
                        PushMessageRequest(
                            to=target_id,
                            messages=[TextMessage(text=counselor_text)],
                        )
                    )
                    delivered_count += 1
                except Exception:
                    logger.exception(
                        "[COUNSELING] failed to push counseling no=%d to target=%s",
                        counseling_no,
                        target_id,
                    )
        logger.info(
            "[COUNSELING] forwarded no=%d requester=%s delivered=%d/%d",
            counseling_no,
            resolved_name,
            delivered_count,
            len(COUNSELING_TARGET_IDS),
        )
    except Exception:
        logger.exception("[COUNSELING] failed to initialize API client for counseling no=%d", counseling_no)
        return (
            False,
            "⚠️ 상담 담당자 전송 중 일시적인 오류가 발생했습니다. 잠시 후 다시 시도해 주세요.",
        )

    if delivered_count == 0:
        return (
            False,
            "⚠️ 상담 담당자 전송 중 일시적인 오류가 발생했습니다. 잠시 후 다시 시도해 주세요.",
        )

    return (
        True,
        f"🔒 고민상담이 비밀리에 접수되었습니다. (상담번호 #{counseling_no})\n\n"
        f"• 모든 상담 내용은 100% 비밀이 보장되며 본방에는 절대 공개되지 않습니다.\n"
        f"• 지정된 상담 담당 운영진·상담사에게만 직접 전달되었습니다.",
    )


def format_recent_reports(limit: int = 10) -> str:
    with state_lock:
        latest = load_json_dict(REPORTS_STORE_KEY)
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
        reporter_name = r.get("reporter_name") or "미확인"
        created_at = r.get("created_at", "")
        content = r.get("content", "")
        lines.append(
            f"\n[#{no} · {kind}] 작성자: {reporter_name} ({created_at})\n{content}"
        )

    return "\n".join(lines)


def format_recent_counselings(limit: int = 10) -> str:
    """상담 담당 운영진·상담사 전용 최근 고민상담 내역 조회"""
    with state_lock:
        latest = load_json_dict(COUNSELINGS_STORE_KEY, force_refresh=True)
        counselings_data.clear()
        counselings_data.update(latest)
        items = counselings_data.get("items", [])
        if not isinstance(items, list) or not items:
            return "🔒 접수된 비밀 고민상담 내역이 없습니다."
        recent = list(reversed(items[-limit:]))

    lines = [f"🔒 최근 비밀 고민상담 내역 (최근 {len(recent)}건 · 외부 유출 금지)"]
    for c in recent:
        if not isinstance(c, dict):
            continue
        no = c.get("no", "?")
        requester_name = c.get("requester_name") or "미확인"
        created_at = c.get("created_at", "")
        content = c.get("content", "")
        lines.append(
            f"\n[#{no} · 비밀상담] 신청자: {requester_name} ({created_at})\n{content}"
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

    # ------------------------------------------------------------------
    # [임시 운영 모드]
    # 본방 허용 명령어는 /환찌, /환츄, /상담 3가지만 유지하고
    # 나머지 모든 명령어는 1:1 개인 채팅(is_private_chat)에서만 동작
    # ------------------------------------------------------------------
    if cmd in ("/환찌", "/환츄"):
        reply_messages(event.reply_token, [HWAN_MSG])
        return

    if cmd == "/상담":
        reply_messages(event.reply_token, [COUNSELING_GUIDE_MSG])
        return

    if cmd == "/관리자호출":
        if not is_private_chat or not user_id:
            return
        caller_name = get_display_name(event)
        reason = extract_command_payload(user_text, cmd)
        chat_label = "1:1 개인 채팅"
        _, call_msg = send_admin_call(
            caller_id=user_id,
            caller_name=caller_name,
            chat_label=chat_label,
            reason=reason,
        )
        reply_messages(event.reply_token, [call_msg])
        return

    # 1:1 채팅 전용 명령어 영역
    if cmd == "/명령어":
        if is_private_chat:
            reply_messages(event.reply_token, [PRIVATE_COMMAND_LIST_MSG])
        return

    if cmd == "/관리명령어":
        if is_private_chat and user_id and is_admin_user(user_id, chat_id):
            reply_messages(event.reply_token, [ALL_COMMAND_LIST_MSG])
        return

    if cmd == "/방아이디":
        if is_private_chat and user_id:
            reply_messages(event.reply_token, [f"🆔 현재 채팅 ID:\n{chat_id}"])
        return

    if cmd == "/건의목록":
        if is_private_chat and user_id and ADMIN_TARGET_SET and chat_id in ADMIN_TARGET_SET and is_admin_user(user_id, chat_id):
            reply_messages(event.reply_token, [format_recent_reports()])
        return

    if cmd == "/상담목록":
        if is_private_chat and user_id and COUNSELING_TARGET_SET and chat_id in COUNSELING_TARGET_SET and is_counseling_staff(user_id, chat_id):
            reply_messages(event.reply_token, [format_recent_counselings()])
        return

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
            cooldown_key=user_id,
            reporter_name=reporter_name,
            kind=kind,
            content=payload,
        )
        reply_messages(event.reply_token, [result_msg])
        return

    if cmd == "/고민상담":
        if not is_private_chat or not user_id:
            return

        payload = extract_command_payload(user_text, cmd)
        if len(payload) < MIN_COUNSELING_LEN:
            reply_messages(
                event.reply_token,
                [
                    f"🔒 [비밀 고민상담 안내]\n"
                    f"상담 내용은 100% 비밀이 보장되며, 본방에는 절대 공개되지 않고 지정된 상담 담당 운영진·상담사에게만 전달됩니다.\n\n"
                    f"고민 내용을 {MIN_COUNSELING_LEN}글자 이상 함께 입력해 주세요.\n"
                    f"예시) /고민상담 요즘 진로와 인간관계 때문에 고민이 많습니다."
                ],
            )
            return

        requester_name = get_display_name(event)
        _, result_msg = save_and_forward_counseling(
            cooldown_key=user_id,
            requester_name=requester_name,
            content=payload,
        )
        reply_messages(event.reply_token, [result_msg])
        return

    if cmd == "/노래신청":
        if not is_private_chat or not user_id:
            return
        requester_name = get_display_name(event)
        payload = extract_command_payload(user_text, cmd)
        reply_messages(event.reply_token, [register_song(payload, requester_name=requester_name)])
        return

    if cmd == "/야동신청":
        if not is_private_chat or not user_id:
            return
        requester_name = get_display_name(event)
        payload = extract_command_payload(user_text, cmd)
        reply_messages(event.reply_token, [register_adult_link(payload, requester_name=requester_name)])
        return

    if cmd == "/노래목록":
        if is_private_chat and user_id and is_admin_user(user_id, chat_id):
            reply_messages(event.reply_token, [format_song_lists_msg()])
        return

    if cmd == "/노래삭제":
        if not is_private_chat or not user_id:
            return
        payload = extract_command_payload(user_text, cmd)
        reply_messages(event.reply_token, [delete_song_item(payload)])
        return

    if cmd == "/노래큐초기화":
        if not is_private_chat or not user_id:
            return
        reply_messages(event.reply_token, [reset_song_queue()])
        return

    if cmd == "/링크목록":
        if not is_private_chat or not user_id:
            return
        reply_messages(event.reply_token, [format_adult_link_list_msg()])
        return

    if cmd == "/링크삭제":
        if not is_private_chat or not user_id:
            return
        payload = extract_command_payload(user_text, cmd)
        reply_messages(event.reply_token, [delete_adult_link_item(payload)])
        return

    if cmd == "/링크큐초기화":
        if not is_private_chat or not user_id:
            return
        reply_messages(event.reply_token, [reset_adult_link_queue()])
        return

    if cmd == "/오노추":
        if is_private_chat:
            reply_messages(event.reply_token, [get_today_song_recommendation()])
        return

    if cmd == "/오야추":
        if is_private_chat:
            reply_messages(event.reply_token, [get_today_adult_link_recommendation()])
        return

    if cmd == "/내랭킹":
        if is_private_chat and user_id:
            name = get_display_name(event)
            reply_messages(event.reply_token, [format_my_ranking(user_id, name)])
        return

    if cmd == "/전체랭킹":
        if is_private_chat:
            reply_messages(event.reply_token, [format_all_ranking()])
        return

    if cmd == "/활동부족":
        if is_private_chat:
            reply_messages(event.reply_token, [format_inactive_members()])
        return

    # 본방 일반 채팅 마디 수 집계는 정상 유지
    if is_group_or_room and user_id:
        name = get_display_name(event)
        upsert_member(user_id, name)
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
