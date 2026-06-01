"""
BilibiliCrawler — a single-class Bilibili crawler.

Two operations:
    - crawl_account(params)   : list videos uploaded by a UP主 (mid)
    - search_keywords(...)    : search videos by keyword, optional date filter

Usage:
    # One-off calls (browser auto-managed per call):
    b = BilibiliCrawler(proxy="http://user:pass@host:8080")
    videos = b.search_keywords("知能行考研数学", num_videos=50)

    # Or reuse the browser across several calls:
    with BilibiliCrawler(proxy=None) as b:
        v1 = b.search_keywords("topic A", num_videos=30)
        v2 = b.search_keywords("topic B", num_videos=30)
        a  = b.crawl_account({"mid": 402630951, "pn": 1, "ps": 40})

Proxy toggle:
    - Pass `proxy="http://user:pass@host:port"` (or socks5://...) to the class.
    - Or set env var BILI_PROXY and leave the arg as None.
    - Pass nothing / unset env var → direct connection.
"""

import hashlib
import json
import os
import time
import urllib.parse
from typing import Any, Optional, Tuple

from playwright.sync_api import sync_playwright, Browser


class BilibiliCrawler:
    # ----- Bilibili endpoints -----
    HOME_URL = "https://www.bilibili.com/"
    NAV_URL = "https://api.bilibili.com/x/web-interface/nav"
    ACCOUNT_API = "https://api.bilibili.com/x/space/wbi/arc/search"
    SEARCH_URL = "https://search.bilibili.com/all"
    # Full per-video detail; returns the nested "View" object (owner{}, stat{}).
    VIEW_API = "https://api.bilibili.com/x/web-interface/view"

    # Per-page size of the "video" tab (searchTypeResponse.pagesize). Bilibili
    # uses 42-per-page for the video tab; the "all" tab's curated video group
    # would be 20, but we read from searchTypeResponse since that's what the
    # UI renders from and what honors the date filter strictly.
    PAGE_SIZE = 42

    # Stop search after this many consecutive pages with no new in-range hits.
    EMPTY_STREAK_LIMIT = 3

    DEFAULT_UA = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    )
    DEFAULT_LOCALE = "zh-CN"

    # WBI mixin permutation (stable for years).
    _MIXIN_TAB = [
        46, 47, 18, 2, 53, 8, 23, 32, 15, 50, 10, 31, 58, 3, 45, 35,
        27, 43, 5, 49, 33, 9, 42, 19, 29, 28, 14, 39, 12, 38, 41, 13,
        37, 48, 7, 16, 24, 55, 40, 61, 26, 17, 0, 1, 60, 51, 30, 4,
        22, 25, 54, 21, 56, 59, 6, 63, 57, 62, 11, 36, 20, 34, 44, 52,
    ]

    # Browser-fingerprint params expected by the account API.
    _DM_PARAMS = {
        "dm_img_list": "[]",
        "dm_img_str": "V2ViR0wgMS4wIChPcGVuR0wgRVMgMi4wIENocm9taXVtKQ",
        "dm_cover_img_str": (
            "QU5HTEUgKEFwcGxlLCBBTkdMRSBNZXRhbCBSZW5kZXJlcjogQXBwbGUgTTQs"
            "IFVuc3BlY2lmaWVkIFZlcnNpb24pR29vZ2xlIEluYy4gKEFwcGxlKQ"
        ),
        "dm_img_inter": '{"ds":[],"wh":[0,0,0],"of":[0,0,0]}',
        "web_location": "333.1387",
    }

    # Different referer-ish hints for page 1 vs subsequent search pages.
    _SEARCH_PAGE_1_QS = {
        "from_source": "web_search",
        "spm_id_from": "333.1387",
        "search_source": "5",
    }
    _SEARCH_PAGE_N_QS = {
        "from_source": "web_search",
        "spm_id_from": "333.1007",
        "search_source": "4",
    }

    # JS that runs inside the page (same origin + cookies = like DevTools console).
    _FETCH_IN_PAGE_JS = """
    async (url) => {
        const res = await fetch(url, {
            method: 'GET',
            credentials: 'include',
            headers: { 'Accept': 'application/json, text/plain, */*' },
        });
        const text = await res.text();
        try { return { status: res.status, data: JSON.parse(text) }; }
        catch (e) { return { status: res.status, raw: text }; }
    }
    """

    _EXTRACT_SEARCH_JS = r"""
    () => {
        const str = window.__pinia
            && window.__pinia.searchTypeResponse
            && window.__pinia.searchTypeResponse.searchTypeResponse;
        if (!str) return { error: 'searchTypeResponse missing' };
        return {
            page: str.page, pagesize: str.pagesize,
            numResults: str.numResults, numPages: str.numPages,
            seid: str.seid,
            videos: Array.isArray(str.result) ? str.result : [],
        };
    }
    """

    # ----- init / lifecycle -----

    def __init__(
        self,
        proxy: Optional[str] = None,
        headless: bool = False,
        user_agent: Optional[str] = None,
        locale: Optional[str] = None,
        verbose: bool = True,
    ):
        self.proxy_cfg = self._resolve_proxy(proxy)
        self.headless = headless
        self.user_agent = user_agent or self.DEFAULT_UA
        self.locale = locale or self.DEFAULT_LOCALE
        self.verbose = verbose
        self._pw = None
        self._browser: Optional[Browser] = None

    def __enter__(self) -> "BilibiliCrawler":
        self._pw = sync_playwright().start()
        self._browser = self._pw.chromium.launch(
            headless=self.headless, proxy=self.proxy_cfg
        )
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._browser:
            self._browser.close()
        if self._pw:
            self._pw.stop()
        self._browser = None
        self._pw = None

    # ----- public API -----

    def crawl_account(
        self,
        user_params: dict,
        save_to: Optional[str] = None,
    ) -> dict:
        """
        Fetch a UP主's video list via /x/space/wbi/arc/search.

        user_params: logical query params, e.g.
            {"mid": 402630951, "pn": 1, "ps": 40, "tid": 0, "order": "pubdate",
             "platform": "web", "order_avoided": "true", "keyword": "",
             "special_type": "", "index": 0}

        Returns the parsed JSON. If save_to is set, also writes to that path.
        """
        self._log_proxy()
        return self._with_browser(
            lambda br: self._do_crawl_account(br, user_params, save_to)
        )

    def search_keywords(
        self,
        keyword: str,
        num_videos: int = 40,
        date_range: Optional[Tuple[int, int]] = None,
        save_to: Optional[str] = None,
        order: str = "",
    ) -> dict:
        """
        Search videos by keyword, paginating until num_videos is reached.

        date_range: optional (pubtime_begin_s, pubtime_end_s) unix-second tuple.
                    Strict client-side pubdate filter is applied on top of the
                    server-side filter (Bilibili's filter is loose).
        order: optional Bilibili web-search sort. "" = default (totalrank);
               "click" (most played), "pubdate" (newest), "dm" (most danmu),
               "stow" (most favorited), "scores" (most commented).

        Returns {keyword, date_range, requested, returned, numResults,
                 numPages, pages_crawled, videos}.
        """
        self._log_proxy()
        return self._with_browser(
            lambda br: self._do_search_keywords(br, keyword, num_videos, date_range, save_to, order)
        )

    def crawl_video_views(
        self,
        bvids: list,
        sleep_ms: int = 1000,
        save_to: Optional[str] = None,
    ) -> dict:
        """
        Fetch full per-video detail (the "View" object) for each bvid via
        /x/web-interface/view, reusing a single page.

        Returns {bvid: view_data, ...}, where each view_data already carries the
        nested owner{} / stat{} / ctime / int-`duration` shape. Used to enrich
        account/creator listings, whose space-API rows lack like/favorite counts.
        Bvids that fail to resolve are simply omitted from the result.
        """
        self._log_proxy()
        if not bvids:
            return {}
        return self._with_browser(
            lambda br: self._do_crawl_video_views(br, bvids, sleep_ms, save_to)
        )

    # ----- internal: browser lifecycle helper -----

    def _with_browser(self, fn):
        if self._browser is not None:
            return fn(self._browser)
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=self.headless, proxy=self.proxy_cfg)
            try:
                return fn(browser)
            finally:
                browser.close()

    def _new_context(self, browser: Browser):
        return browser.new_context(user_agent=self.user_agent, locale=self.locale)

    def _log_proxy(self):
        if self.verbose and self.proxy_cfg:
            print(f"proxy: {self.proxy_cfg['server']}")

    def _log(self, *a):
        if self.verbose:
            print(*a)

    # ----- internal: account crawl -----

    def _do_crawl_account(
        self, browser: Browser, user_params: dict, save_to: Optional[str]
    ) -> dict:
        ctx = self._new_context(browser)
        page = ctx.new_page()
        try:
            page.goto(self.HOME_URL, wait_until="domcontentloaded")
            page.wait_for_timeout(2500)

            nav = page.evaluate(self._FETCH_IN_PAGE_JS, self.NAV_URL)
            wbi_img = nav["data"]["data"]["wbi_img"]
            img_key, sub_key = self._extract_wbi_keys(
                wbi_img["img_url"], wbi_img["sub_url"]
            )
            self._log(f"wbi keys: img={img_key[:8]}... sub={sub_key[:8]}...")

            params = {**user_params, **self._DM_PARAMS}
            signed = self._sign_wbi(params, img_key, sub_key)
            url = self.ACCOUNT_API + "?" + urllib.parse.urlencode(signed, safe="")
            self._log(f"wts={signed['wts']}  w_rid={signed['w_rid']}")

            result = page.evaluate(self._FETCH_IN_PAGE_JS, url)
        finally:
            ctx.close()

        if save_to:
            with open(save_to, "w", encoding="utf-8") as f:
                json.dump(result, f, indent=2, ensure_ascii=False)
            self._log(f"status={result.get('status')} -> saved to {save_to}")
        else:
            self._log(f"status={result.get('status')}")
        return result

    # ----- internal: per-video detail -----

    def _do_crawl_video_views(
        self,
        browser: Browser,
        bvids: list,
        sleep_ms: int,
        save_to: Optional[str],
    ) -> dict:
        ctx = self._new_context(browser)
        page = ctx.new_page()
        out: dict = {}
        try:
            # On-origin once so the in-page fetch carries bilibili cookies/referer.
            page.goto(self.HOME_URL, wait_until="domcontentloaded")
            page.wait_for_timeout(1500)
            for bvid in bvids:
                if not bvid:
                    continue
                url = self.VIEW_API + "?" + urllib.parse.urlencode({"bvid": bvid})
                res = page.evaluate(self._FETCH_IN_PAGE_JS, url)
                # _FETCH_IN_PAGE_JS -> {status, data: <bili envelope>}; the View
                # object is the envelope's inner `data`.
                envelope = res.get("data") if isinstance(res, dict) else None
                if (
                    isinstance(envelope, dict)
                    and envelope.get("code") == 0
                    and isinstance(envelope.get("data"), dict)
                ):
                    out[bvid] = envelope["data"]
                    self._log(f"view {bvid}: ok")
                else:
                    code = envelope.get("code") if isinstance(envelope, dict) else None
                    status = res.get("status") if isinstance(res, dict) else None
                    self._log(f"view {bvid}: failed (status={status}, code={code})")
                page.wait_for_timeout(sleep_ms)
        finally:
            ctx.close()

        if save_to:
            with open(save_to, "w", encoding="utf-8") as f:
                json.dump(out, f, indent=2, ensure_ascii=False)
            self._log(f"saved {len(out)} views -> {save_to}")
        return out

    # ----- internal: search crawl -----

    def _do_search_keywords(
        self,
        browser: Browser,
        keyword: str,
        num_videos: int,
        date_range: Optional[Tuple[int, int]],
        save_to: Optional[str],
        order: str = "",
    ) -> dict:
        all_videos: list = []
        seen: set = set()
        current = 1
        num_pages = None
        num_results = None
        empty_streak = 0

        while True:
            url = self._build_search_url(keyword, current, date_range, order)
            # Fresh CONTEXT per iteration: Bilibili keys pagination off the
            # session, so reusing cookies makes it resend page 1.
            ctx = self._new_context(browser)
            page = ctx.new_page()
            try:
                page.goto(url, wait_until="domcontentloaded")
                page.wait_for_function(
                    "() => window.__pinia"
                    "?.searchTypeResponse?.searchTypeResponse?.result",
                    timeout=20000,
                )
                data = page.evaluate(self._EXTRACT_SEARCH_JS)
            finally:
                ctx.close()

            if "error" in data:
                self._log(f"page {current}: {data['error']} — stopping")
                break

            if num_pages is None:
                num_pages = data["numPages"] or 0
                num_results = data["numResults"] or 0
                if num_results == 0:
                    self._log("0 results for this query — stopping")
                    break

            new_count = 0
            filtered_out = 0
            for v in data["videos"]:
                key = v.get("bvid") or v.get("aid") or v.get("id")
                if not key or key in seen:
                    continue
                seen.add(key)
                if date_range:
                    pub = v.get("pubdate") or 0
                    if not (date_range[0] <= pub <= date_range[1]):
                        filtered_out += 1
                        continue
                all_videos.append(v)
                new_count += 1
                if len(all_videos) >= num_videos:
                    break

            tail = f" ({filtered_out} out-of-range)" if date_range else ""
            self._log(
                f"page {current}/{num_pages}: +{new_count} new{tail} "
                f"(total {len(all_videos)}/{num_videos})"
            )

            if len(all_videos) >= num_videos:
                break
            if current >= num_pages:
                break

            empty_streak = empty_streak + 1 if new_count == 0 else 0
            if empty_streak >= self.EMPTY_STREAK_LIMIT:
                self._log(
                    f"  ({self.EMPTY_STREAK_LIMIT} pages in a row with no new "
                    "in-range videos — stopping)"
                )
                break

            current += 1
            time.sleep(1.2)

        all_videos = all_videos[:num_videos]
        result = {
            "keyword": keyword,
            "date_range": list(date_range) if date_range else None,
            "requested": num_videos,
            "returned": len(all_videos),
            "numResults": num_results,
            "numPages": num_pages,
            "pages_crawled": current,
            "videos": all_videos,
        }

        if save_to:
            with open(save_to, "w", encoding="utf-8") as f:
                json.dump(result, f, indent=2, ensure_ascii=False)
            self._log(f"saved {len(all_videos)} videos -> {save_to}")
        return result

    # ----- internal: WBI signing -----

    @classmethod
    def _get_mixin_key(cls, orig: str) -> str:
        return "".join(orig[i] for i in cls._MIXIN_TAB)[:32]

    @classmethod
    def _sign_wbi(cls, params: dict, img_key: str, sub_key: str) -> dict:
        mixin_key = cls._get_mixin_key(img_key + sub_key)
        signed = dict(params)
        signed["wts"] = int(time.time())
        cleaned = {
            k: "".join(c for c in str(v) if c not in "!'()*")
            for k, v in sorted(signed.items())
        }
        query = urllib.parse.urlencode(cleaned, safe="")
        signed["w_rid"] = hashlib.md5((query + mixin_key).encode()).hexdigest()
        return signed

    @staticmethod
    def _extract_wbi_keys(img_url: str, sub_url: str) -> Tuple[str, str]:
        img_key = img_url.rsplit("/", 1)[-1].split(".")[0]
        sub_key = sub_url.rsplit("/", 1)[-1].split(".")[0]
        return img_key, sub_key

    # ----- internal: search URL builder -----

    @classmethod
    def _build_search_url(
        cls,
        keyword: str,
        page_num: int,
        date_range: Optional[Tuple[int, int]],
        order: str = "",
    ) -> str:
        base = cls._SEARCH_PAGE_1_QS if page_num == 1 else cls._SEARCH_PAGE_N_QS
        params: dict[str, Any] = {**base, "keyword": keyword, "page": page_num}
        if page_num > 1:
            params["o"] = (page_num - 1) * cls.PAGE_SIZE
        if date_range:
            params["pubtime_begin_s"], params["pubtime_end_s"] = date_range
        if order:
            params["order"] = order
        return cls.SEARCH_URL + "?" + urllib.parse.urlencode(params)

    # ----- internal: proxy resolution -----

    @staticmethod
    def _resolve_proxy(proxy: Optional[str]) -> Optional[dict]:
        if proxy is None:
            proxy = os.environ.get("BILI_PROXY") or None
        if not proxy:
            return None
        p = urllib.parse.urlparse(proxy)
        server = f"{p.scheme}://{p.hostname}"
        if p.port:
            server += f":{p.port}"
        cfg = {"server": server}
        if p.username:
            cfg["username"] = urllib.parse.unquote(p.username)
        if p.password:
            cfg["password"] = urllib.parse.unquote(p.password)
        return cfg


if __name__ == "__main__":
    # Demo. Drop into your own project as:
    #     from bilibili_crawler import BilibiliCrawler
    with BilibiliCrawler(proxy=None) as b:
        b.search_keywords(
            keyword="知能行考研数学",
            num_videos=30,
            date_range=(1779382800, 1779987599),
            save_to="search_response.json",
        )
        b.crawl_account(
            {
                "pn": 1, "ps": 40, "tid": 0, "special_type": "",
                "order": "pubdate", "mid": 402630951, "index": 0,
                "keyword": "", "order_avoided": "true", "platform": "web",
            },
            save_to="account_response.json",
        )
