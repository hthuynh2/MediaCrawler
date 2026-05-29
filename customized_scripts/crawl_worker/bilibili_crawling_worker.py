"""
Bilibili crawling worker.

Polls the server for the next Bilibili task and runs it with the project's
custom, cookie-less Bilibili crawler
(``customized_scripts/customized_crawler/bilibili_crawler.py``), then reports
the crawled posts and the task outcome back to the server.

Unlike the xhs/douyin workers, this one does NOT shell out to ``main.py``: the
custom crawler is a self-contained, importable *sync* class, so we call it
in-process. Success is "no exception"; we no longer grep stdout for a marker.

Reporting mirrors the previous Bilibili flow:
  - per-post data  -> report_bilibili_post_data_to_server (View-shaped dicts)
  - task outcome   -> report_crawler_task_outcome

Config (all env-overridable, sensible defaults):
  BILI_PROXY              proxy URL (e.g. http://user:pass@host:port); default direct
  BILI_HEADLESS           run browser headless (default: false, matches the fleet)
  BILI_FETCH_FULL_DETAIL  account mode: fetch each video's full detail to fill in
                          like/favorite counts (default: true). Flip to false to
                          report partial stats (fewer requests, lower block risk).
  BILI_SEARCH_MAX_VIDEOS  max videos to fetch per search keyword (default: 30)
"""

import html
import os
import re
import time
import traceback

from dotenv import load_dotenv

from customized_scripts.customized_crawler.bilibili_crawler import BilibiliCrawler
from customized_scripts.server_utils.server_report_utils import (
    get_next_crawling_task,
    report_bilibili_post_data_to_server,
    report_crawler_task_outcome,
)

load_dotenv()


def _env_bool(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in ("1", "true", "yes", "y", "on")


# ----- worker config -----
BILI_PROXY = os.environ.get("BILI_PROXY") or None
BILI_HEADLESS = _env_bool("BILI_HEADLESS", False)

# Account/creator mode: Bilibili's space list API returns play + comment but NOT
# like/favorite. When True we fetch each video's full detail to populate complete
# stats (matching the old crawler). Set BILI_FETCH_FULL_DETAIL=false to skip the
# extra requests and report partial stats (like/favorite = 0) instead.
FETCH_FULL_DETAIL = _env_bool("BILI_FETCH_FULL_DETAIL", True)

SEARCH_MAX_VIDEOS = int(os.environ.get("BILI_SEARCH_MAX_VIDEOS") or 30)
DEFAULT_MAX_NUM_POSTS = int(os.environ.get("BILI_MAX_NUM_POSTS") or 30)

ACCOUNT_PAGE_SIZE = 30
ACCOUNT_PAGE_SLEEP_SEC = 1.5     # between space-list pages
SEARCH_KEYWORD_SLEEP_SEC = 2.0   # between search keywords
DETAIL_FETCH_SLEEP_MS = int(os.environ.get("BILI_DETAIL_SLEEP_MS") or 1000)  # between view fetches


# ----- normalization helpers -----
# The server reporter (report_bilibili_post_data_to_server) reads a nested,
# video-detail "View" shape: bvid, aid, ctime, int duration, title,
# owner.{mid,name}, stat.{favorite,like,reply,view}. The custom crawler emits
# flatter rows with different field names, so we normalize before reporting.

_TAG_RE = re.compile(r"<[^>]+>")


def _clean_text(text) -> str:
    """Strip search-result highlight markup (<em>...</em>) and unescape entities."""
    return html.unescape(_TAG_RE.sub("", text or "")).strip()


def _parse_duration(value) -> int:
    """'8:38' -> 518, '102:38' -> 6158, '1:02:03' -> 3723, 518 -> 518, '' -> 0."""
    if isinstance(value, (int, float)):
        return int(value)
    text = str(value or "").strip()
    if not text:
        return 0
    if text.isdigit():
        return int(text)
    seconds = 0
    for part in text.split(":"):
        part = part.strip()
        if not part.isdigit():
            return 0
        seconds = seconds * 60 + int(part)
    return seconds


def _to_mid(creator_id) -> int:
    """Coerce a creator id (int, numeric string, or space URL) to an mid int."""
    text = str(creator_id).strip()
    if text.isdigit():
        return int(text)
    digits = re.findall(r"\d+", text)
    if digits:
        return int(digits[-1])
    raise ValueError(f"cannot parse creator mid from {creator_id!r}")


def _is_real_video(item: dict) -> bool:
    """Filter out injected search ads / placeholder rows."""
    if not item.get("bvid") or not item.get("title"):
        return False
    if str(item.get("type", "")).startswith("video_ad"):
        return False
    return True


def _view_from_search(item: dict) -> dict:
    """Search-result row -> View-shaped dict. Search rows already carry stats."""
    return {
        "bvid": item.get("bvid", ""),
        "aid": item.get("aid", 0),
        "ctime": int(item.get("pubdate", 0) or 0),
        "duration": _parse_duration(item.get("duration")),
        "title": _clean_text(item.get("title", "")),
        "owner": {"mid": item.get("mid", 0), "name": item.get("author", "")},
        "stat": {
            "favorite": int(item.get("favorites", 0) or 0),
            "like": int(item.get("like", 0) or 0),
            "reply": int(item.get("review", 0) or 0),
            "view": int(item.get("play", 0) or 0),
        },
    }


def _view_from_vlist(item: dict) -> dict:
    """Space-list (account) row -> View-shaped dict, partial stats.

    The space list API does not expose like/favorite, so they are 0 here. When
    FETCH_FULL_DETAIL is on we replace this with the real View object instead.
    """
    return {
        "bvid": item.get("bvid", ""),
        "aid": item.get("aid", 0),
        "ctime": int(item.get("created", 0) or 0),
        "duration": _parse_duration(item.get("length")),
        "title": _clean_text(item.get("title", "")),
        "owner": {"mid": item.get("mid", 0), "name": item.get("author", "")},
        "stat": {
            "favorite": 0,  # not provided by the space list API
            "like": 0,      # not provided by the space list API
            "reply": int(item.get("comment", 0) or 0),
            "view": int(item.get("play", 0) or 0),
        },
    }


def _report_account_items(crawler: BilibiliCrawler, items: list, task_id) -> int:
    """Normalize a batch of space-list rows and report them. Enriches with full
    per-video detail when FETCH_FULL_DETAIL is enabled."""
    if not items:
        return 0
    if FETCH_FULL_DETAIL:
        bvids = [it.get("bvid") for it in items if it.get("bvid")]
        views = crawler.crawl_video_views(bvids, sleep_ms=DETAIL_FETCH_SLEEP_MS)
        posts = []
        for it in items:
            detail = views.get(it.get("bvid"))
            # Fall back to partial stats if a video's detail failed to resolve.
            posts.append(detail if isinstance(detail, dict) else _view_from_vlist(it))
    else:
        posts = [_view_from_vlist(it) for it in items]
    report_bilibili_post_data_to_server(posts, task_id)
    return len(posts)


# ----- task handlers -----
def execute_crawl_account(crawler: BilibiliCrawler, params: dict) -> int:
    """Crawl a creator's uploaded videos, newest first, honoring max_num_posts
    and min_create_time, reporting each page as it is fetched."""
    task_id = params["task_id"]
    mid = _to_mid(params["creator_id"])
    max_num_posts = int(params.get("max_num_posts") or DEFAULT_MAX_NUM_POSTS)
    min_create_time = int(params.get("min_create_time") or 0)

    ps = ACCOUNT_PAGE_SIZE
    pn = 1
    reported = 0
    while True:
        resp = crawler.crawl_account({
            "mid": mid, "pn": pn, "ps": ps, "tid": 0, "special_type": "",
            "order": "pubdate", "index": 0, "keyword": "",
            "order_avoided": "true", "platform": "web",
        })
        envelope = (resp or {}).get("data", {}) or {}
        if envelope.get("code") != 0:
            raise RuntimeError(
                f"account api error: code={envelope.get('code')} "
                f"message={envelope.get('message')!r} (mid={mid}, pn={pn})"
            )
        inner = envelope.get("data", {}) or {}
        vlist = (inner.get("list", {}) or {}).get("vlist", []) or []
        total = int((inner.get("page", {}) or {}).get("count", 0) or 0)
        if not vlist:
            break

        in_range = [v for v in vlist if int(v.get("created", 0) or 0) > min_create_time]
        remaining = max_num_posts - reported
        batch = in_range[:remaining] if remaining > 0 else []
        reported += _report_account_items(crawler, batch, task_id)

        if reported >= max_num_posts:
            break
        if not in_range:          # whole page older than cutoff -> done
            break
        if pn * ps >= total:      # no more pages
            break
        pn += 1
        time.sleep(ACCOUNT_PAGE_SLEEP_SEC)

    print(f"[bilibili_worker] account mid={mid}: reported {reported} posts")
    return reported


def execute_search(crawler: BilibiliCrawler, params: dict) -> int:
    """Search each keyword and report the matching videos."""
    task_id = params["task_id"]
    keywords = params.get("keywords") or []
    if isinstance(keywords, str):
        keywords = [keywords]

    order = params.get("search_sort_type") or ""   # "", click, pubdate, dm, stow
    start = int(params.get("search_start_ts") or 0)  # unix seconds, 0 = no filter
    end = int(params.get("search_end_ts") or 0)
    date_range = (start, end) if (start and end) else None

    reported = 0
    for keyword in keywords:
        keyword = (keyword or "").strip()
        if not keyword:
            continue
        resp = crawler.search_keywords(
            keyword,
            num_videos=SEARCH_MAX_VIDEOS,
            date_range=date_range,
            order=order,
        )
        videos = (resp or {}).get("videos", []) or []
        posts = [_view_from_search(v) for v in videos if _is_real_video(v)]
        if posts:
            report_bilibili_post_data_to_server(posts, task_id)
            reported += len(posts)
        print(f"[bilibili_worker] search '{keyword}': reported {len(posts)} posts")
        time.sleep(SEARCH_KEYWORD_SLEEP_SEC)

    return reported


def execute_task(params: dict) -> bool:
    task_id = params.get("task_id")
    task_type = params.get("task_type")

    is_success = False
    error = ""
    reported = 0
    try:
        # One browser per task; reused across pages/keywords/detail fetches.
        with BilibiliCrawler(proxy=BILI_PROXY, headless=BILI_HEADLESS, verbose=True) as crawler:
            if task_type == "crawl_account_posts":
                reported = execute_crawl_account(crawler, params)
            elif task_type == "search":
                reported = execute_search(crawler, params)
            else:
                raise ValueError(f"unrecognized task type {task_type!r}")
        is_success = True
        # Marker kept for log-grep parity with the other workers.
        print(f"[bilibili_worker] Bilibili Crawler finished — task {task_id}, reported {reported} posts")
    except Exception as e:
        error = f"{type(e).__name__}: {e}\n{traceback.format_exc()}"
        print(f"[bilibili_worker] task {task_id} FAILED: {error}")

    # report the task to the server
    report_crawler_task_outcome(is_success, error, "bilibili", task_id)

    # =================================
    print("At {}: Done for task {}; is_success: {};".format(int(time.time()), task_id, is_success))
    return is_success


if __name__ == '__main__':
    while 1:
        task_info = get_next_crawling_task("bilibili")
        print("task_info: {}".format(task_info))
        if task_info is None or len(task_info) == 0:
            print("{}: No task from the server, wait for 60s....".format(int(time.time())))
            # wait 1 min before get next task
            time.sleep(60)
            continue

        is_success = execute_task(task_info)
        # wait 10s before each tasks
        print("{}: Completed task with status {is_success}, wait for 10s before start next ask".format(int(time.time()), is_success=is_success))
        time.sleep(10)
