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


def parse_admin_target_ids() -> list[str]:
    """ADMIN_TARGET_IDS(쉼표 구분 다중 ID) 및 하위호환 ADMIN_TARGET_ID 파싱"""
    raw_multi = os.getenv("ADMIN_TARGET_IDS") or ""
    raw_single = os.getenv("ADMIN_TARGET_ID") or ""
    candidates = [item.strip() for item in f"{raw_multi},{raw_single}".split(",") if item.strip()]
    seen: set[str] = set()
    ordered: list[str] = []
    for tid in candidates:
        if tid not in seen:
            seen.add(tid)
            ordered.append(tid)
    return ordered


# 건의·신고를 수신할 관리자 user_id(U...) 또는 운영진 전용방 group_id(C...) 목록
ADMIN_TARGET_IDS: list[str] = parse_admin_target_ids()
ADMIN_TARGET_SET: set[str] = set(ADMIN_TARGET_IDS)
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

# 건의·신고 도배 방지 쿨타임(초) 및 최소 내용 길이
REPORT_COOLDOWN_SEC = 300
MIN_REPORT_LEN = 5

# 오노추 시작 최소 등록 개수(5개) / 오야추 시작 최소 등록 개수(1개 이상)
MIN_SONG_RECOMMEND_POOL_SIZE = 5
MIN_ADULT_RECOMMEND_POOL_SIZE = 1

state_lock = threading.Lock()
last_counted_at: dict[str, float] = {}
last_reported_at: dict[str, float] = {}
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

DISCOMFORT_REPORT_GUIDE_MSG = (
    "🚨 불편신고 안내\n"
    "[오른쪽 상단 메뉴 -> 멤버 -> 봇 친구추가]\n\n"
    "친구 추가 후 봇과의 1:1 채팅에서\n"
    "/신고 [내용] 또는 /건의 [내용]으로 보내주시면\n"
    "운영진에게 전달됩니다."
)

COMMAND_LIST_MSG = (
    "📌 본방 사용 가능한 명령어 안내\n\n"
    "[일반 멤버 명령어]\n"
    "• /내랭킹 — 오늘 내 마디 수, 순위, 활동 상태 확인\n"
    "• /오노추 — 오늘의 노래 추천 확인\n"
    "• /노래목록 — 전체 노래 및 다음 랜덤 추천 후보 확인\n"
    "• /오야추 — 오늘의 추천 링크 확인\n"
    "• /불편신고 — 1:1 신고·건의 방법 안내\n"
    "• /명령어 — 본방 명령어 목록 보기\n\n"
    "[관리자 전용 사용 가능 명령어]\n"
    "• /환찌 (/환츄) — 환츄/환찌 안내 보기\n"
    "• /전체랭킹 — 오늘 방 전체 마디 수 랭킹 확인\n"
    "• /활동부족 — 오늘 30마디 미만 인원 목록 확인\n\n"
    "💡 노래·링크 신청(/노래신청, /야동신청) 및 신고·건의는\n"
    "봇과의 1:1 채팅에서 /명령어를 입력해 확인해 주세요."
)

PRIVATE_COMMAND_LIST_MSG = (
    "📌 봇 1:1 개인 채팅 전용 명령어 안내\n\n"
    "[신청 및 접수 명령어 (1:1 전용)]\n"
    "• /노래신청 (가수)-(노래제목) — 한 곡 또는 여러 곡 등록\n"
    "  예) /노래신청 아이유-밤편지, 성시경-거리에서\n"
    "• /야동신청 (링크) — 오야추 링크 등록\n"
    "  예) /야동신청 https://example.com\n"
    "• /신고 [내용] — 운영진에게 익명/비공개 신고 접수\n"
    "• /건의 [내용] — 운영진에게 건의사항 전달\n\n"
    "[조회 및 안내 명령어]\n"
    "• /오노추 — 오늘의 노래 추천 확인\n"
    "• /노래목록 — 전체 노래 및 다음 랜덤 추천 후보 확인\n"
    "• /오야추 — 오늘의 추천 링크 확인\n"
    "• /명령어 — 1:1 채팅 명령어 목록 다시 보기"
)


def is_admin_user(user_id: str | None, chat_id: str = "") -> bool:
    """관리자 권한 판별: ADMIN_USER_IDS 지정 시 해당 user_id만 허용"""
    if ADMIN_USER_IDS:
        return bool(user_id and user_id in ADMIN_USER_IDS)
    if ADMIN_TARGET_SET:
        return bool((user_id and user_id in ADMIN_TARGET_SET) or (chat_id and chat_id in ADMIN_TARGET_SET))
    return False


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


def load_json_dict(key: str, force_refresh: bool = False) -> dict:
    """Upstash Redis에서 JSON 딕셔너리 조회(단기 캐시 적용)"""
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
        raw_val = redis_client.get(key)
        data = _parse_redis_json(raw_val)
        store_cache[key] = data
        store_loaded_at[key] = now_ts
        return data
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

    # 줄바꿈 및 쉼표/세미콜론으로 먼저 1차 분리
    line_chunks = re.split(r"[\r\n,;]+", raw)
    candidates: list[str] = []

    for line in line_chunks:
        part = line.strip()
        if not part:
            continue
        # ' / ' 형태로 여러 곡을 이어 적은 경우 분리하되, '가수-제목' 쌍이 2개 이상일 때만 분할
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
    앞으로 추천될 일이 남은 노래 ID 목록('upcoming_ids')을 별도 리스트로 동기화:
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


def register_song(payload: str, requester_name: str) -> str:
    """1:1 채팅 노래 신청 처리 — 한 번에 여러 곡 등록 지원 및 중복 거부"""
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
        latest = load_json_dict(SONGS_STORE_KEY)
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
        "오노추가 시작되었습니다! 본방에서 /오노추로 확인해 보세요."
        if total_count >= MIN_SONG_RECOMMEND_POOL_SIZE
        else f"오노추 시작까지 {MIN_SONG_RECOMMEND_POOL_SIZE - total_count}곡 남았습니다."
    )

    # 한 곡만 입력한 경우 기존과 동일한 간결한 메시지 유지
    if len(parsed_songs) == 1 and not invalid_chunks:
        if added_songs:
            artist, title = added_songs[0]
            return (
                f"✅ 노래가 등록되었습니다: {artist} - {title}\n"
                f"(현재 {total_count}/{MIN_SONG_RECOMMEND_POOL_SIZE}곡 · {status_note})"
            )
        artist, title = duplicate_songs[0]
        return f"🎶 이미 등록된 노래입니다: {artist} - {title}"

    lines: list[str] = []
    if added_songs:
        lines.append(f"✅ 노래 {len(added_songs)}곡이 등록되었습니다:")
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

    lines.append(f"\n(현재 {total_count}/{MIN_SONG_RECOMMEND_POOL_SIZE}곡 · {status_note})")
    return "\n".join(lines)


def format_song_recommendation_msg(day: str, artist: str, title: str) -> str:
    return (
        f"🎧 [{day}] 오늘의 노래 추천 [오노추]\n"
        f"• 가수: {artist}\n"
        f"• 노래: {title}\n\n"
        f"※ 노래 신청은 봇과의 1:1 채팅에서\n"
        f"/노래신청 (가수)-(노래제목)으로 해주세요."
    )


def get_today_song_recommendation() -> str:
    """
    오늘의 노래 추천(오노추):
    - 5개 이상부터 시작
    - 오늘 이미 뽑힌 곡이 있으면 오늘 값 반환
    - 앞으로 추천될 일이 남은 목록(upcoming_ids)에서 랜덤 선정
    - 전체 곡 소진 전까지 중복 미출력
    - 어제 추천된 곡이 내일(오늘) 첫 추천에서 다시 나오지 않도록 제외
    """
    day = today_key()
    prev_day = yesterday_key()

    with state_lock:
        latest = load_json_dict(SONGS_STORE_KEY)
        songs_data.clear()
        songs_data.update(latest)
        items = ensure_pool_schema(songs_data, "songs")
        total_count = len(items)

        if total_count < MIN_SONG_RECOMMEND_POOL_SIZE:
            return (
                f"🎵 오노추는 노래가 {MIN_SONG_RECOMMEND_POOL_SIZE}곡 이상 모여야 시작됩니다.\n"
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
    """전체 등록 노래 목록과 앞으로 추천될 일이 남은 다음 랜덤 추천 후보 목록을 함께 출력"""
    day = today_key()
    prev_day = yesterday_key()

    with state_lock:
        latest = load_json_dict(SONGS_STORE_KEY)
        songs_data.clear()
        songs_data.update(latest)
        items = ensure_pool_schema(songs_data, "songs")
        prev_upcoming = list(songs_data.get("upcoming_ids") or [])
        upcoming_ids = sync_upcoming_song_ids(songs_data, items, day=day, prev_day=prev_day)
        if songs_data.get("upcoming_ids") != prev_upcoming:
            save_json_dict(SONGS_STORE_KEY, songs_data)

    if not items:
        return (
            "🎵 현재 등록된 노래가 없습니다.\n\n"
            "※ 노래 신청은 봇과의 1:1 채팅에서\n"
            "/노래신청 (가수)-(노래제목)으로 해주세요."
        )

    item_map = {
        str(it.get("id")): it for it in items if isinstance(it, dict) and it.get("id")
    }
    used_set = {str(uid) for uid in songs_data.get("used_ids", [])}
    today_pick_id = str((songs_data.get("daily_picks") or {}).get(day) or "")
    yesterday_pick_id = str((songs_data.get("daily_picks") or {}).get(prev_day) or "")

    all_lines = [f"🎶 전체 등록 노래 목록 (총 {len(items)}곡 · 기준: {MIN_SONG_RECOMMEND_POOL_SIZE}곡)"]
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
        latest = load_json_dict(ADULT_LINKS_STORE_KEY)
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
        latest = load_json_dict(ADULT_LINKS_STORE_KEY)
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

    if target_id in ADMIN_TARGET_SET:
        return

    with state_lock:
        latest = load_json_dict(TARGETS_STORE_KEY)
        targets_data.update(latest)
        if target_id not in targets_data:
            targets_data[target_id] = True
            save_json_dict(TARGETS_STORE_KEY, targets_data)
            logger.info("[TARGET] registered chat target=%s", target_id)


def get_chat_targets() -> list[str]:
    with state_lock:
        latest = load_json_dict(TARGETS_STORE_KEY)
        targets_data.update(latest)
        return [
            tid for tid in targets_data.keys()
            if tid not in ADMIN_TARGET_SET
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
        latest = load_json_dict(MEMBERS_STORE_KEY)
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


def remove_member(user_id: str | None, remove_today_ranking: bool = True) -> None:
    """퇴장한 멤버를 상시 멤버 목록 및 오늘 랭킹에서 자동 제외"""
    if not user_id:
        return
    day = today_key()
    with state_lock:
        latest_members = load_json_dict(MEMBERS_STORE_KEY)
        members_data.clear()
        members_data.update(latest_members)
        removed_entry = members_data.pop(user_id, None)
        removed_name = removed_entry.get("name") if isinstance(removed_entry, dict) else None
        members_changed = removed_name is not None or user_id in latest_members
        if members_changed:
            save_json_dict(MEMBERS_STORE_KEY, members_data)

        ranking_changed = False
        if remove_today_ranking:
            latest_rank = load_json_dict(RANKING_STORE_KEY)
            ranking_data.clear()
            ranking_data.update(latest_rank)
            day_board = ranking_data.get(day)
            if isinstance(day_board, dict) and user_id in day_board:
                rank_info = day_board.pop(user_id, None)
                if not removed_name and isinstance(rank_info, dict):
                    removed_name = rank_info.get("name")
                ranking_changed = True
                save_json_dict(RANKING_STORE_KEY, ranking_data)

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
        latest = load_json_dict(RANKING_STORE_KEY)
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


def add_speech_count(user_id: str, name: str, amount: int = 1) -> int:
    """이미 조회된 name을 받아 state_lock 안에서만 상태 갱신 및 저장"""
    upsert_member(user_id, name)
    day = today_key()
    with state_lock:
        latest = load_json_dict(RANKING_STORE_KEY)
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
        save_json_dict(RANKING_STORE_KEY, ranking_data)
        return new_total


def get_sorted_today_entries() -> list[tuple[str, dict]]:
    """
    현재 방에 남아 있는 멤버(오늘 0마디 포함) 기준으로 오늘 발언 수 내림차순 정렬
    """
    day = today_key()
    with state_lock:
        latest_rank = load_json_dict(RANKING_STORE_KEY)
        ranking_data.update(latest_rank)

        latest_members = load_json_dict(MEMBERS_STORE_KEY)
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

    if cmd == "/명령어":
        if is_private_chat:
            reply_messages(event.reply_token, [PRIVATE_COMMAND_LIST_MSG])
        else:
            reply_messages(event.reply_token, [COMMAND_LIST_MSG])
        return

    if cmd == "/불편신고":
        reply_messages(event.reply_token, [DISCOMFORT_REPORT_GUIDE_MSG])
        return

    # 1) 초기 세팅용 ID 확인 명령어 (설정 완료 후 본방에서는 완전 숨김)
    if cmd == "/방아이디":
        if is_private_chat or (not ADMIN_TARGET_SET) or (chat_id in ADMIN_TARGET_SET):
            reply_messages(event.reply_token, [f"🆔 현재 채팅 ID:\n{chat_id}"])
        return

    # 2) 운영진 전용 건의·신고 내역 조회 (본방에서는 완전 무응답)
    if cmd == "/건의목록":
        if ADMIN_TARGET_SET and chat_id in ADMIN_TARGET_SET and is_admin_user(user_id, chat_id):
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
            cooldown_key=user_id,
            reporter_name=reporter_name,
            kind=kind,
            content=payload,
        )
        reply_messages(event.reply_token, [result_msg])
        return

    # 4) 1:1 오노추·오야추 신청 접수 — 오직 1:1 개인 채팅에서만 동작, 본방에서는 완전 무응답
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

    # 이하 일반 본방 기능 (1:1 채팅 유저는 본방 멤버 목록에 섞지 않음)
    if is_group_or_room and user_id:
        name = get_display_name(event)
        upsert_member(user_id, name)
    elif user_id:
        name = get_display_name(event)
    else:
        name = "미확인"

    if cmd == "/오노추":
        reply_messages(event.reply_token, [get_today_song_recommendation()])
        return

    if cmd == "/노래목록":
        reply_messages(event.reply_token, [format_song_lists_msg()])
        return

    if cmd == "/오야추":
        reply_messages(event.reply_token, [get_today_adult_link_recommendation()])
        return

    if cmd in ("/환찌", "/환츄"):
        reply_messages(event.reply_token, [HWAN_MSG])
        return

    if cmd == "/내랭킹":
        if is_group_or_room:
            ensure_known_member(user_id, name)
        reply_messages(event.reply_token, [format_my_ranking(user_id, name)])
        return

    if cmd == "/전체랭킹":
        if is_admin_user(user_id, chat_id):
            reply_messages(event.reply_token, [format_all_ranking()])
        return

    if cmd == "/활동부족":
        if is_admin_user(user_id, chat_id):
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
