import html
import json
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import quote, unquote, urljoin, urlparse

from fastapi.concurrency import run_in_threadpool

from app.core.config import settings
from app.core.context import TorrentInfo
from app.helper.sites import SitesHelper
from app.log import logger
from app.plugins import _PluginBase
from app.schemas.types import MediaType
from app.utils.http import RequestUtils
from app.utils.string import StringUtils


class GyingIndexer(_PluginBase):
    plugin_name = "观影（GYing）"
    plugin_desc = "为 GYing 提供磁力搜索与清晰度过滤支持。"
    plugin_icon = "gying.png"
    plugin_version = "1.3.1"
    plugin_author = "yang124541"
    author_url = "https://github.com/yang124541/moviepilot-plugin"
    plugin_config_prefix = "gyingindexer_"
    plugin_order = 30
    auth_level = 2

    _enabled = False
    _enable_1080 = False
    _enable_zh1080 = True
    _enable_4k = False
    _enable_zh4k = True
    _include_original = True
    _extra_hosts = ""

    _default_hosts: Set[str] = {
        "gying.si",
        "gying.org",
        "gying.net",
        "gyg.la",
        "gyg.si",
    }
    _max_search_pages: int = 8
    _resolved_original_codes: Set[str] = set()
    _quality_label_by_code: Dict[str, str] = {}
    _subtitle_tokens: Tuple[str, ...] = (
        "\u4e2d\u5b57",
        "\u4e2d\u6587\u5b57\u5e55",
        "\u4e2d\u82f1\u5b57\u5e55",
        "\u7b80\u4e2d",
        "\u7e41\u4e2d",
        "\u7b80\u7e41",
        "chs",
        "cht",
        "chi",
    )
    _original_strong_tokens: Tuple[str, ...] = (
        "原盘",
        "原盘源",
        "remux",
        "bdremux",
        "uhd",
        "bdmv",
        "bd25",
        "bd50",
        "bd66",
        "bd100",
        "iso",
        "m2ts",
    )
    _original_weak_tokens: Tuple[str, ...] = (
        "blu-ray",
        "bluray",
        "fullblu",
        "full bluray",
    )
    _non_original_tokens: Tuple[str, ...] = (
        "web-dl",
        "webrip",
        "hdtv",
        "bdrip",
        "hdrip",
        "dvdrip",
        "x264",
        "x265",
    )

    def init_plugin(self, config: dict = None):
        if config:
            self._enabled = bool(config.get("enabled"))
            self._include_original = bool(config.get("include_original", True))
            self._extra_hosts = (config.get("extra_hosts") or "").strip()

            # 新版 5 开关
            if any(k in config for k in ("enable_1080", "enable_zh1080", "enable_4k", "enable_zh4k")):
                self._enable_1080 = bool(config.get("enable_1080", False))
                self._enable_zh1080 = bool(config.get("enable_zh1080", True))
                self._enable_4k = bool(config.get("enable_4k", False))
                self._enable_zh4k = bool(config.get("enable_zh4k", True))
            else:
                # 兼容旧配置：strict_quality + include_original
                strict_quality = bool(config.get("strict_quality", True))
                if strict_quality:
                    self._enable_1080 = False
                    self._enable_zh1080 = True
                    self._enable_4k = False
                    self._enable_zh4k = True
                else:
                    self._enable_1080 = True
                    self._enable_zh1080 = True
                    self._enable_4k = True
                    self._enable_zh4k = True
        if self._enabled:
            self._register_builtin_indexer()
        self._resolved_original_codes = set()
        self._quality_label_by_code = {}

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return []

    def get_api(self) -> List[Dict[str, Any]]:
        return []

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        return [
            {
                "component": "VForm",
                "content": [
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "sm": 6, "md": 2},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "enabled",
                                            "label": "启用插件",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "sm": 6, "md": 2},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "enable_1080",
                                            "label": "1080P",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "sm": 6, "md": 2},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "enable_zh1080",
                                            "label": "中字1080P",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "sm": 6, "md": 2},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "enable_4k",
                                            "label": "4K",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "sm": 6, "md": 2},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "enable_zh4k",
                                            "label": "中字4K",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "sm": 6, "md": 2},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "include_original",
                                            "label": "原盘",
                                        },
                                    }
                                ],
                            }
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VTextarea",
                                        "props": {
                                            "model": "extra_hosts",
                                            "rows": 3,
                                            "label": "额外域名（每行一个）",
                                            "placeholder": "www.example.com",
                                        },
                                    }
                                ],
                            }
                        ],
                    },
                ],
            }
        ], {
            "enabled": False,
            "enable_1080": False,
            "enable_zh1080": True,
            "enable_4k": False,
            "enable_zh4k": True,
            "include_original": True,
            "extra_hosts": "",
        }

    def get_page(self) -> List[dict]:
        pass

    def get_module(self) -> Dict[str, Any]:
        return {
            "search_torrents": self.search_torrents,
            "async_search_torrents": self.async_search_torrents,
        }

    def stop_service(self):
        pass

    async def async_search_torrents(self, site: dict,
                                    keyword: str = None,
                                    mtype: MediaType = None,
                                    page: Optional[int] = 0) -> Optional[List[TorrentInfo]]:
        return await run_in_threadpool(self.search_torrents, site, keyword, mtype, page)

    def search_torrents(self, site: dict,
                        keyword: str = None,
                        mtype: MediaType = None,
                        page: Optional[int] = 0) -> Optional[List[TorrentInfo]]:
        if not self._enabled:
            return None
        if not site or not keyword:
            return []
        if not self._match_target_site(site):
            return None

        start_at = datetime.now()
        base_url = self._resolve_base_url(site)
        if not base_url:
            return []

        timeout = int(site.get("timeout") or 20)
        ua = site.get("ua") or settings.USER_AGENT
        cookie = site.get("cookie")
        proxies = settings.PROXY if site.get("proxy") else None
        referer = base_url

        logger.info(
            f"GYing search start: {keyword} "
            f"[1080={self._enable_1080}, zh1080={self._enable_zh1080}, "
            f"4k={self._enable_4k}, zh4k={self._enable_zh4k}, original={self._include_original}]"
        )

        try:
            client = RequestUtils(
                ua=ua,
                cookies=cookie,
                proxies=proxies,
                timeout=timeout,
                referer=referer
            )
            search_entries = self._collect_search_entries(
                client=client,
                base_url=base_url,
                keyword=keyword
            )
            if not search_entries:
                logger.warn("GYing search empty after paging")
                return []

            results: List[TorrentInfo] = []
            parent_meta_cache: Dict[str, Dict[str, Any]] = {}
            parent_default_dir: Dict[str, str] = {}
            parent_down_entries_cache: Dict[str, List[Dict[str, Any]]] = {}
            parent_down_entry_map_cache: Dict[str, Dict[str, Dict[str, Any]]] = {}
            bt_parent_cache: Dict[str, str] = {}
            result_ids: Set[str] = set()
            for entry in search_entries:
                res_id = str(entry.get("id") or "").strip()
                if not res_id:
                    continue
                res_dir = str(entry.get("dir") or "bt").strip().lower()
                if not res_dir:
                    res_dir = "bt"

                title = str(entry.get("title") or "").strip()
                if not title:
                    continue
                search_quality_code = str(entry.get("quality") or "").strip().lower()
                search_tag_label = str(entry.get("tag") or "").strip()

                detail_url = urljoin(base_url, f"{res_dir}/{res_id}")
                detail_data: Dict[str, Any] = {}

                tag_code = ""
                tag_label = ""
                parent_meta: Dict[str, Any] = {}
                down_item: Dict[str, Any] = {}
                cache_key = str(bt_parent_cache.get(res_id) or "").strip()
                if cache_key:
                    parent_meta = parent_meta_cache.get(cache_key) or {}
                    down_item = parent_down_entry_map_cache.get(cache_key, {}).get(res_id) or {}
                    if down_item:
                        res_dir = str(down_item.get("dir") or res_dir).strip().lower() or res_dir
                        title = str(down_item.get("title") or title).strip() or title
                        detail_url = urljoin(base_url, f"{res_dir}/{res_id}")
                        tag_code = str(down_item.get("quality") or "").strip().lower()
                        tag_label = str(down_item.get("quality_label") or "").strip()
                else:
                    detail_html = client.get(detail_url)
                    if detail_html:
                        _detail_data = self._extract_js_object(detail_html, "_obj.d")
                        if isinstance(_detail_data, dict):
                            detail_data = _detail_data

                    parent_dir, parent_id = self._parse_parent_route(detail_data.get("du"))
                    if parent_dir and parent_id:
                        cache_key = f"{parent_dir}/{parent_id}"
                        parent_default_dir.setdefault(cache_key, res_dir)

                        if cache_key not in parent_meta_cache:
                            parent_meta_cache[cache_key] = self._fetch_parent_meta(
                                client=client,
                                base_url=base_url,
                                parent_dir=parent_dir,
                                parent_id=parent_id
                            )
                        parent_meta = parent_meta_cache.get(cache_key) or {}

                        if cache_key not in parent_down_entries_cache:
                            _down_entries = self._fetch_parent_down_entries(
                                client=client,
                                base_url=base_url,
                                parent_dir=parent_dir,
                                parent_id=parent_id
                            )
                            parent_down_entries_cache[cache_key] = _down_entries
                            _id_map: Dict[str, Dict[str, Any]] = {}
                            for _item in _down_entries:
                                _cid = str(_item.get("id") or "").strip()
                                if not _cid:
                                    continue
                                _id_map[_cid] = _item
                                bt_parent_cache[_cid] = cache_key
                            parent_down_entry_map_cache[cache_key] = _id_map

                        down_item = parent_down_entry_map_cache.get(cache_key, {}).get(res_id) or {}
                        if down_item:
                            res_dir = str(down_item.get("dir") or res_dir).strip().lower() or res_dir
                            title = str(down_item.get("title") or title).strip() or title
                            detail_url = urljoin(base_url, f"{res_dir}/{res_id}")
                            tag_code = str(down_item.get("quality") or "").strip().lower()
                            tag_label = str(down_item.get("quality_label") or "").strip()

                filter_title = str(
                    detail_data.get("title")
                    or down_item.get("title")
                    or title
                    or ""
                ).strip()
                quality_code_for_filter = tag_code or search_quality_code
                quality_label_for_filter = (
                    tag_label
                    or str(self._quality_label_by_code.get(quality_code_for_filter) or "").strip()
                    or search_tag_label
                )
                if not self._should_keep_entry(
                    title=filter_title,
                    quality_code=quality_code_for_filter,
                    quality_label=quality_label_for_filter
                ):
                    continue

                entry_hash = str(down_item.get("hash") or "").strip()
                enclosure = self._build_magnet_from_hash(info_hash=entry_hash, title=title)
                if not enclosure:
                    if not detail_data:
                        detail_html = client.get(detail_url)
                        if detail_html:
                            _detail_data = self._extract_js_object(detail_html, "_obj.d")
                            if isinstance(_detail_data, dict):
                                detail_data = _detail_data

                    download_candidates = self._extract_download_candidates_from_node(
                        node=detail_data,
                        base_url=base_url
                    )
                    if not download_candidates:
                        download_candidates = self._fetch_download_candidates_from_downurl(
                            client=client,
                            base_url=base_url,
                            resource_dir=res_dir,
                            resource_id=res_id
                        )
                    enclosure = self._pick_preferred_enclosure(download_candidates)
                if not enclosure:
                    continue

                search_size_text = str(down_item.get("size") or entry.get("size") or "").strip()
                detail_size_text = str(detail_data.get("s") or detail_data.get("size") or "").strip()
                size_bytes = self._parse_size_bytes(detail_size_text, search_size_text)
                seeds_text = down_item.get("seeds") or entry.get("seeds")
                elapsed_text = str(down_item.get("time") or entry.get("time") or "").strip()
                tag_text = tag_label or search_tag_label
                detail_title = str(detail_data.get("title") or title).strip()
                parent_year = str(parent_meta.get("year") or "").strip()
                title_for_match = self._build_match_title(
                    title=title,
                    parent_title=str(parent_meta.get("title") or "").strip(),
                    parent_year=parent_year
                )
                desc_parts = [x for x in [tag_text, detail_title, str(parent_meta.get("title") or "").strip()] if x]
                description = " | ".join(desc_parts[:3])
                if parent_year and not re.search(r"(19|20)\d{2}", description):
                    description = f"{description} {parent_year}".strip()
                description = self._append_unique_marker(
                    description=description or detail_title or title,
                    resource_id=res_id,
                    enclosure=enclosure
                )

                results.append(TorrentInfo(
                    site=site.get("id"),
                    site_name=site.get("name"),
                    site_cookie=site.get("cookie"),
                    site_ua=site.get("ua"),
                    site_proxy=site.get("proxy"),
                    site_order=site.get("pri"),
                    site_downloader=site.get("downloader"),
                    title=title_for_match or title,
                    description=description,
                    enclosure=enclosure,
                    page_url=detail_url,
                    size=size_bytes,
                    seeders=self._to_int(seeds_text),
                    peers=0,
                    grabs=0,
                    pubdate=None,
                    date_elapsed=elapsed_text,
                    downloadvolumefactor=0,
                    uploadvolumefactor=1,
                ))
                result_ids.add(res_id)

            # 站点搜索页可能漏掉同父级下的部分条目，补充抓取父级 downlist 全量条目。
            for cache_key, parent_meta in parent_meta_cache.items():
                try:
                    parent_dir, parent_id = cache_key.split("/", 1)
                except Exception:
                    continue
                default_dir = str(parent_default_dir.get(cache_key) or "bt").strip().lower() or "bt"
                down_entries = parent_down_entries_cache.get(cache_key)
                if down_entries is None:
                    down_entries = self._fetch_parent_down_entries(
                        client=client,
                        base_url=base_url,
                        parent_dir=parent_dir,
                        parent_id=parent_id
                    )
                    parent_down_entries_cache[cache_key] = down_entries
                if not down_entries:
                    continue

                for down_item in down_entries:
                    child_id = str(down_item.get("id") or "").strip()
                    if not child_id or child_id in result_ids:
                        continue

                    child_title = str(down_item.get("title") or "").strip()
                    if not child_title:
                        continue
                    child_quality_code = str(down_item.get("quality") or "").strip().lower()
                    child_quality_label = str(
                        down_item.get("quality_label")
                        or self._quality_label_by_code.get(child_quality_code)
                        or ""
                    ).strip()
                    if not self._should_keep_entry(
                        title=child_title,
                        quality_code=child_quality_code,
                        quality_label=child_quality_label
                    ):
                        continue

                    child_dir = str(down_item.get("dir") or default_dir).strip().lower() or "bt"
                    child_detail_url = urljoin(base_url, f"{child_dir}/{child_id}")
                    child_hash = str(down_item.get("hash") or "").strip()
                    enclosure = self._build_magnet_from_hash(info_hash=child_hash, title=child_title)
                    child_detail_data: Dict[str, Any] = {}
                    if not enclosure:
                        child_detail_html = client.get(child_detail_url)
                        if child_detail_html:
                            _child_detail_data = self._extract_js_object(child_detail_html, "_obj.d")
                            if isinstance(_child_detail_data, dict):
                                child_detail_data = _child_detail_data

                        download_candidates = self._extract_download_candidates_from_node(
                            node=child_detail_data,
                            base_url=base_url
                        )
                        if not download_candidates:
                            download_candidates = self._fetch_download_candidates_from_downurl(
                                client=client,
                                base_url=base_url,
                                resource_dir=child_dir,
                                resource_id=child_id
                            )
                        enclosure = self._pick_preferred_enclosure(download_candidates)
                    if not enclosure:
                        continue

                    search_size_text = str(down_item.get("size") or "").strip()
                    detail_size_text = str(child_detail_data.get("s") or child_detail_data.get("size") or "").strip()
                    size_bytes = self._parse_size_bytes(detail_size_text, search_size_text)
                    seeds_text = down_item.get("seeds")
                    elapsed_text = str(down_item.get("time") or "").strip()
                    tag_text = child_quality_label
                    detail_title = str(child_detail_data.get("title") or child_title).strip()
                    parent_year = str(parent_meta.get("year") or "").strip()
                    title_for_match = self._build_match_title(
                        title=child_title,
                        parent_title=str(parent_meta.get("title") or "").strip(),
                        parent_year=parent_year
                    )
                    desc_parts = [x for x in [tag_text, detail_title, str(parent_meta.get("title") or "").strip()] if x]
                    description = " | ".join(desc_parts[:3])
                    if parent_year and not re.search(r"(19|20)\d{2}", description):
                        description = f"{description} {parent_year}".strip()
                    description = self._append_unique_marker(
                        description=description or detail_title or child_title,
                        resource_id=child_id,
                        enclosure=enclosure
                    )

                    results.append(TorrentInfo(
                        site=site.get("id"),
                        site_name=site.get("name"),
                        site_cookie=site.get("cookie"),
                        site_ua=site.get("ua"),
                        site_proxy=site.get("proxy"),
                        site_order=site.get("pri"),
                        site_downloader=site.get("downloader"),
                        title=title_for_match or child_title,
                        description=description,
                        enclosure=enclosure,
                        page_url=child_detail_url,
                        size=size_bytes,
                        seeders=self._to_int(seeds_text),
                        peers=0,
                        grabs=0,
                        pubdate=None,
                        date_elapsed=elapsed_text,
                        downloadvolumefactor=0,
                        uploadvolumefactor=1,
                    ))
                    result_ids.add(child_id)

            cost = (datetime.now() - start_at).seconds
            logger.info(f"GYing search done: {len(results)} result(s), cost={cost}s")
            return results
        except Exception as err:
            logger.error(f"GYing search error: {err}")
            return []

    def _match_target_site(self, site: dict) -> bool:
        site_id = str(site.get("id") or "").strip().lower()
        if site_id == "gying":
            return True

        host_candidates = [
            site.get("domain"),
            site.get("url"),
        ]
        hosts = self._all_hosts()
        for candidate in host_candidates:
            host = self._extract_host(candidate)
            if host and self._is_host_match(host, hosts):
                return True
        return False

    def _resolve_base_url(self, site: dict) -> str:
        raw = str(site.get("url") or site.get("domain") or "").strip()
        if not raw:
            raw = "https://www.gying.si/"
        if "://" not in raw:
            raw = f"https://{raw}"
        parsed = urlparse(raw)
        if not parsed.netloc:
            return "https://www.gying.si/"
        return f"{parsed.scheme}://{parsed.netloc}/"

    @staticmethod
    def _build_search_url(base_url: str, keyword: str,
                          page_no: int = 1, quality_code: Optional[str] = None) -> str:
        # 站点新版搜索路由不再支持 i5/i9 分类码，固定使用 s/{page}-4--1
        return urljoin(base_url, f"s/{page_no}-4--1/{quote(keyword)}")

    def _collect_search_entries(self, client: RequestUtils, base_url: str, keyword: str) -> List[Dict[str, Any]]:
        entry_map: Dict[str, Dict[str, Any]] = {}
        keyword_plan = self._expand_search_keywords(client=client, base_url=base_url, keyword=keyword)
        logger.info(f"GYing keyword plan: {keyword_plan}")

        for query_keyword in keyword_plan:
            for page_no in range(1, self._max_search_pages + 1):
                search_url = self._build_search_url(
                    base_url=base_url,
                    keyword=query_keyword,
                    page_no=page_no,
                    quality_code=None
                )
                html = client.get(search_url)
                if not html:
                    break
                search_data = self._extract_js_object(html, "_obj.search")
                if not isinstance(search_data, dict):
                    break
                page_entries = self._extract_entries_from_search(
                    search_data=search_data,
                    forced_quality=None
                )
                if not page_entries:
                    break

                new_count = 0
                for item in page_entries:
                    key = str(item.get("id") or "").strip()
                    if key and key not in entry_map:
                        entry_map[key] = item
                        new_count += 1

                if page_no > 1 and new_count == 0:
                    break

        logger.info(f"GYing entries collected: {len(entry_map)}")
        return list(entry_map.values())

    def _expand_search_keywords(self, client: RequestUtils, base_url: str, keyword: str) -> List[str]:
        primary = str(keyword or "").strip()
        if not primary:
            return []

        keywords: List[str] = [primary]
        seen: Set[str] = {self._normalize_text(primary)}

        suggest_url = urljoin(base_url, f"res/s/{quote(primary)}")
        text = client.get(suggest_url)
        if not text:
            return keywords

        try:
            suggest_list = json.loads(text)
        except Exception:
            return keywords

        if not isinstance(suggest_list, list):
            return keywords

        for item in suggest_list[:3]:
            if not isinstance(item, dict):
                continue
            title = str(item.get("title") or "").strip()
            if not title:
                continue
            norm = self._normalize_text(title)
            if not norm or norm in seen:
                continue
            seen.add(norm)
            keywords.append(title)
            if len(keywords) >= 3:
                break

        return keywords

    def _extract_entries_from_search(self, search_data: Dict[str, Any],
                                     forced_quality: Optional[str]) -> List[Dict[str, Any]]:
        list_obj = search_data.get("l") or {}
        if not isinstance(list_obj, dict):
            return []

        ids = self._as_list(list_obj.get("i"))
        dirs = self._as_list(list_obj.get("d"))
        titles = self._as_list(list_obj.get("title"))
        sizes = self._as_list(list_obj.get("size"))
        seeds = self._as_list(list_obj.get("seeds"))
        times = self._as_list(list_obj.get("time"))
        tags = self._as_list(list_obj.get("k"))
        qualities = self._as_list(list_obj.get("p"))

        entries: List[Dict[str, Any]] = []
        for idx, btid in enumerate(ids):
            row_dir = str(self._safe_at(dirs, idx) or "").strip().lower()
            if not row_dir:
                row_dir = "bt"
            title = str(self._safe_at(titles, idx) or "").strip()
            if not title:
                continue

            row_quality = str(self._safe_at(qualities, idx) or forced_quality or "").strip().lower()

            entries.append({
                "id": btid,
                "dir": row_dir,
                "title": title,
                "size": self._safe_at(sizes, idx),
                "seeds": self._safe_at(seeds, idx),
                "time": self._safe_at(times, idx),
                "tag": self._safe_at(tags, idx),
                "quality": row_quality
            })
        return entries

    def _all_hosts(self) -> Set[str]:
        hosts = set(self._default_hosts)
        for line in self._extra_hosts.splitlines():
            host = self._extract_host(line)
            if host:
                hosts.add(host)
        return hosts

    def _register_builtin_indexer(self) -> None:
        hosts = sorted(self._all_hosts())
        if not hosts:
            return

        primary = "gying.si" if "gying.si" in hosts else hosts[0]
        indexer = self._build_indexer_schema(primary_host=primary, all_hosts=hosts)

        for host in hosts:
            try:
                SitesHelper().add_indexer(domain=host, indexer=indexer)
            except Exception as err:
                logger.warn(f"GYing indexer register failed for {host}: {err}")
        logger.info(f"GYing indexer registered for hosts: {', '.join(hosts)}")

    @staticmethod
    def _build_indexer_schema(primary_host: str, all_hosts: List[str]) -> Dict[str, Any]:
        ext_domains = [f"https://{host}/" for host in all_hosts if host != primary_host]
        return {
            "id": "gying",
            "name": "GYing",
            "domain": f"https://{primary_host}/",
            "ext_domains": ext_domains,
            "encoding": "UTF-8",
            "public": True,
            "proxy": True,
            "result_num": 100,
            "timeout": 30,
            "search": {
                "paths": [
                    {
                        "path": "s/1-4--1/{keyword}",
                        "method": "get"
                    }
                ]
            },
            "torrents": {
                "list": {
                    "selector": "table.__never_match__ > tr"
                },
                "fields": {
                    "id": {"selector": "a"},
                    "title": {"selector": "a"},
                    "details": {
                        "selector": "a",
                        "attribute": "href"
                    },
                    "download": {
                        "selector": "a",
                        "attribute": "href"
                    },
                    "downloadvolumefactor": {
                        "case": {
                            "*": 0
                        }
                    },
                    "uploadvolumefactor": {
                        "case": {
                            "*": 1
                        }
                    }
                }
            }
        }

    @staticmethod
    def _extract_host(raw: Any) -> str:
        if raw is None:
            return ""
        text = str(raw).strip().lower()
        if not text:
            return ""
        if "://" not in text:
            text = f"https://{text}"
        try:
            host = (urlparse(text).hostname or "").lower()
        except Exception:
            return ""
        if host.startswith("www."):
            host = host[4:]
        return host

    @staticmethod
    def _is_host_match(host: str, allowed_hosts: Set[str]) -> bool:
        pure_host = host.lower().lstrip(".")
        if pure_host.startswith("www."):
            pure_host = pure_host[4:]
        for allowed in allowed_hosts:
            if pure_host == allowed or pure_host.endswith(f".{allowed}"):
                return True
        return False

    @staticmethod
    def _is_4k(title_norm: str) -> bool:
        return (
            bool(re.search(r"(?<!\d)2160p(?!\d)", title_norm)) or
            bool(re.search(r"(?<!\d)4k(?!\d)", title_norm))
        )

    @staticmethod
    def _is_1080(title_norm: str) -> bool:
        if not bool(re.search(r"(?<!\d)1080p(?!\d)", title_norm)):
            return False
        # 同时出现 2160/4k 时优先认为是 4k
        return not GyingIndexer._is_4k(title_norm)

    @staticmethod
    def _normalize_text(text: Any) -> str:
        return re.sub(r"\s+", "", str(text or "")).lower()

    def _has_chinese_subtitle(self, quality_label: str = "", title: str = "") -> bool:
        label_norm = self._normalize_text(quality_label)
        title_norm = self._normalize_text(title)
        if ("中字" in label_norm) or ("中文" in label_norm):
            return True
        if ("中字" in title_norm) or ("中文" in title_norm):
            return True

        for token in self._subtitle_tokens:
            token_norm = self._normalize_text(token)
            if not token_norm:
                continue
            if token_norm in label_norm or token_norm in title_norm:
                return True
        return False

    def _should_keep_entry(self, title: str, quality_code: str = "", quality_label: str = "") -> bool:
        label_norm = self._normalize_text(quality_label)
        title_norm = self._normalize_text(title)

        is_original = "原盘" in label_norm
        is_4k = ("4k" in label_norm) or ("2160" in label_norm)
        is_1080 = ("1080" in label_norm) and not is_4k
        has_zh_sub = self._has_chinese_subtitle(quality_label=quality_label, title=title)

        # 标签缺失/异常时，回退到标题判断，减少误过滤。
        if not (is_original or is_4k or is_1080):
            is_original = self._match_original(title)
            is_4k = self._is_4k(title_norm)
            is_1080 = self._is_1080(title_norm)

        if not (is_original or is_4k or is_1080):
            return False

        keep = False
        if self._include_original and is_original:
            keep = True
        if self._enable_zh4k and is_4k and has_zh_sub:
            keep = True
        if self._enable_4k and is_4k and not has_zh_sub:
            keep = True
        if self._enable_zh1080 and is_1080 and has_zh_sub:
            keep = True
        if self._enable_1080 and is_1080 and not has_zh_sub:
            keep = True
        return keep

    @staticmethod
    def _parse_parent_route(raw: Any) -> Tuple[str, str]:
        text = str(raw or "").strip()
        if not text:
            return "", ""
        parsed = urlparse(text)
        path = parsed.path or text
        m = re.match(r"^/?([a-zA-Z0-9_]+)/([a-zA-Z0-9]+)$", path)
        if not m:
            return "", ""
        return m.group(1).lower(), m.group(2)

    def _fetch_parent_tag_index(self, client: RequestUtils, base_url: str,
                                parent_dir: str, parent_id: str) -> Tuple[Dict[str, str], Dict[str, str]]:
        url = urljoin(base_url, f"res/downurl/{parent_dir}/{parent_id}")
        text = client.get(url)
        if not text:
            return {}, {}

        try:
            obj = json.loads(text)
        except Exception:
            return {}, {}

        downlist = obj.get("downlist") if isinstance(obj, dict) else None
        if not isinstance(downlist, dict):
            return {}, {}

        label_by_code: Dict[str, str] = {}
        type_obj = downlist.get("type") or {}
        if isinstance(type_obj, dict):
            names = self._as_list(type_obj.get("a"))
            codes = self._as_list(type_obj.get("b"))
            for idx, code in enumerate(codes):
                code_key = str(code or "").strip().lower()
                if not code_key:
                    continue
                label_by_code[code_key] = str(self._safe_at(names, idx) or "").strip()

        tag_by_bt: Dict[str, str] = {}
        list_obj = downlist.get("list") or {}
        if isinstance(list_obj, dict):
            bt_ids = self._as_list(list_obj.get("u"))
            tag_codes = self._as_list(list_obj.get("p"))
            for idx, bt_id in enumerate(bt_ids):
                bt_key = str(bt_id or "").strip()
                if not bt_key:
                    continue
                tag_by_bt[bt_key] = str(self._safe_at(tag_codes, idx) or "").strip().lower()

        return tag_by_bt, label_by_code

    def _fetch_parent_meta(self, client: RequestUtils, base_url: str,
                           parent_dir: str, parent_id: str) -> Dict[str, Any]:
        detail_url = urljoin(base_url, f"{parent_dir}/{parent_id}")
        html = client.get(detail_url)
        if not html:
            return {}
        detail_data = self._extract_js_object(html, "_obj.d")
        if not isinstance(detail_data, dict):
            return {}
        return {
            "title": str(detail_data.get("title") or "").strip(),
            "year": str(detail_data.get("year") or "").strip(),
        }

    def _fetch_parent_down_entries(self, client: RequestUtils, base_url: str,
                                   parent_dir: str, parent_id: str) -> List[Dict[str, Any]]:
        url = urljoin(base_url, f"res/downurl/{parent_dir}/{parent_id}")
        text = client.get(url)
        if not text:
            return []

        try:
            obj = json.loads(text)
        except Exception:
            return []

        downlist = obj.get("downlist") if isinstance(obj, dict) else None
        if not isinstance(downlist, dict):
            return []

        label_by_code: Dict[str, str] = {}
        type_obj = downlist.get("type") or {}
        if isinstance(type_obj, dict):
            names = self._as_list(type_obj.get("a"))
            codes = self._as_list(type_obj.get("b"))
            for idx, code in enumerate(codes):
                code_key = str(code or "").strip().lower()
                if not code_key:
                    continue
                label_text = str(self._safe_at(names, idx) or "").strip()
                if label_text:
                    label_by_code[code_key] = label_text
                    self._quality_label_by_code[code_key] = label_text

        list_obj = downlist.get("list") or {}
        if not isinstance(list_obj, dict):
            return []

        ids = self._as_list(list_obj.get("u"))
        titles = self._as_list(list_obj.get("t"))
        sizes = self._as_list(list_obj.get("s"))
        seeds = self._as_list(list_obj.get("e"))
        times = self._as_list(list_obj.get("n"))
        qualities = self._as_list(list_obj.get("p"))
        hashes = self._as_list(list_obj.get("m"))
        dirs = self._as_list(list_obj.get("d"))

        entries: List[Dict[str, Any]] = []
        for idx, btid in enumerate(ids):
            rid = str(btid or "").strip()
            if not rid:
                continue
            quality_code = str(self._safe_at(qualities, idx) or "").strip().lower()
            quality_label = str(label_by_code.get(quality_code) or "").strip()
            entries.append({
                "id": rid,
                "dir": str(self._safe_at(dirs, idx) or "bt").strip().lower() or "bt",
                "title": str(self._safe_at(titles, idx) or "").strip(),
                "size": str(self._safe_at(sizes, idx) or "").strip(),
                "seeds": self._safe_at(seeds, idx),
                "time": str(self._safe_at(times, idx) or "").strip(),
                "quality": quality_code,
                "quality_label": quality_label,
                "hash": str(self._safe_at(hashes, idx) or "").strip(),
            })
        return entries

    @staticmethod
    def _build_match_title(title: str, parent_title: str = "", parent_year: str = "") -> str:
        base = str(title or "").strip()
        if not base:
            return ""
        parent = str(parent_title or "").strip()
        year = str(parent_year or "").strip()
        if re.match(r"^(19|20)\d{2}$", year):
            base = GyingIndexer._strip_release_group_year_noise(base=base, parent_year=year)
        has_year_in_base = bool(re.search(r"(19|20)\d{2}", base))

        # 优先补父级片名，提升中文标题匹配命中（如：云图 + Cloud.Atlas...）。
        if parent and parent not in base:
            if re.match(r"^(19|20)\d{2}$", year) and year not in base:
                return f"{parent}.{year}.{base}"
            return f"{parent}.{base}"

        if has_year_in_base:
            return base
        if re.match(r"^(19|20)\d{2}$", year):
            return f"{base}.{year}"
        return base

    @staticmethod
    def _strip_release_group_year_noise(base: str, parent_year: str) -> str:
        """
        去除发布组名中与片子年份冲突的尾缀年份（如 EDGE2020）。
        这类年份会干扰 MoviePilot 匹配年份判断，导致资源被误判不匹配。
        """
        text = str(base or "").strip()
        if not text:
            return text

        def repl(match: re.Match) -> str:
            prefix = str(match.group(1) or "")
            token = str(match.group(2) or "")
            y = str(match.group(3) or "")
            if y and y != parent_year:
                return f"{prefix}{token}"
            return match.group(0)

        # 命中示例：-EDGE2020 / .GROUP2019
        return re.sub(r"([\-\.])([A-Za-z]{2,})(19|20)\d{2}\b", repl, text)

    @staticmethod
    def _append_unique_marker(description: str, resource_id: str, enclosure: str = "") -> str:
        """
        MoviePilot 在搜索链路中按 site_name + title + description 去重。
        追加资源唯一标识，避免同标题同描述条目被误合并。
        """
        base = str(description or "").strip()
        rid = str(resource_id or "").strip()
        if not rid:
            return base

        ext = ""
        lower_enclosure = str(enclosure or "").strip().lower()
        ext_match = re.search(r"\.(mkv|mp4|torrent)(?:$|[?#])", lower_enclosure)
        if ext_match:
            ext = ext_match.group(1).lower()

        marker = f"GY[{rid}{('/' + ext) if ext else ''}]"
        if marker in base:
            return base
        if not base:
            return marker
        return f"{base} | {marker}"

    @staticmethod
    def _build_magnet_from_hash(info_hash: str, title: str = "") -> str:
        token = str(info_hash or "").strip()
        if not token:
            return ""
        if not re.match(r"^(?:[0-9a-fA-F]{40}|[A-Za-z2-7]{32})$", token):
            return ""
        if title:
            return f"magnet:?xt=urn:btih:{token}&dn={quote(str(title))}"
        return f"magnet:?xt=urn:btih:{token}"

    def _match_original(self, title: str) -> bool:
        title_norm = re.sub(r"\s+", "", title).lower()
        has_negative = any(token.replace(" ", "") in title_norm for token in self._non_original_tokens)
        has_strong = any(token.replace(" ", "") in title_norm for token in self._original_strong_tokens)
        has_weak = any(token.replace(" ", "") in title_norm for token in self._original_weak_tokens)

        if has_strong:
            # remux/bdmv/iso 等强关键词优先保留；若明显是 web-dl/rip 编码则剔除
            if has_negative and ("remux" not in title_norm and "原盘" not in title_norm):
                return False
            return True
        # 弱关键词（bluray）必须同时具备 remux/uhd 才判为原盘
        if has_weak and ("remux" in title_norm or "uhd" in title_norm):
            return True
        return False

    @staticmethod
    def _extract_original_codes(search_data: Dict[str, Any]) -> Set[str]:
        """
        递归提取搜索数据中与“原盘”关联的分类码。
        常见结构示例：
        - {"id":"i10","cat":"原盘"}
        - {"p":"i10","name":"原盘"}
        """
        result: Set[str] = set()

        def walk(node: Any):
            if isinstance(node, dict):
                text_values = [str(v) for v in node.values() if isinstance(v, str)]
                if any("原盘" in tv for tv in text_values):
                    for key in ("id", "p", "code", "catid", "value"):
                        val = node.get(key)
                        if isinstance(val, str) and re.match(r"^i\d+$", val.strip().lower()):
                            result.add(val.strip().lower())
                for v in node.values():
                    walk(v)
            elif isinstance(node, list):
                for item in node:
                    walk(item)

        walk(search_data)
        return result

    def _fetch_download_candidates_from_downurl(self, client: RequestUtils, base_url: str,
                                                resource_dir: str, resource_id: str) -> List[str]:
        """
        回退接口：部分条目详情页不直接包含 magnet，需要从 downurl 接口读取。
        返回磁力与可下载链接候选（优先磁力，其次 torrent/媒体直链）。
        """
        url = urljoin(base_url, f"res/downurl/{resource_dir}/{resource_id}")
        text = client.get(url)
        if not text:
            return []
        try:
            obj = json.loads(text)
        except Exception:
            return []

        return self._extract_download_candidates_from_node(node=obj, base_url=base_url)

    def _extract_download_candidates_from_node(self, node: Any, base_url: str) -> List[str]:
        ordered: List[str] = []
        seen: Set[str] = set()

        def add_value(value: str):
            item = str(value or "").strip()
            if not item:
                return
            key = item.lower()
            if key in seen:
                return
            seen.add(key)
            ordered.append(item)

        def walk(data: Any):
            if isinstance(data, dict):
                for v in data.values():
                    walk(v)
            elif isinstance(data, list):
                for item in data:
                    walk(item)
            elif isinstance(data, str):
                for item in self._extract_download_candidates_from_text(data=data, base_url=base_url):
                    add_value(item)

        walk(node)
        return ordered

    def _extract_download_candidates_from_text(self, data: str, base_url: str) -> List[str]:
        text = html.unescape(str(data or "").strip())
        if not text:
            return []

        variants: List[str] = [text]
        cursor = text
        for _ in range(2):
            decoded = unquote(cursor)
            if decoded == cursor:
                break
            variants.append(decoded)
            cursor = decoded

        ordered: List[str] = []
        seen: Set[str] = set()

        def add_value(value: str):
            item = str(value or "").strip().strip("\"'")
            if not item:
                return
            key = item.lower()
            if key in seen:
                return
            seen.add(key)
            ordered.append(item)

        for variant in variants:
            for magnet in re.findall(r"magnet:\?[^\s\"'<>]+", variant, flags=re.IGNORECASE):
                add_value(magnet)

            url_text = variant.strip().strip("\"'")
            if not url_text:
                continue
            if url_text.startswith("//"):
                url_text = "https:" + url_text
            elif url_text.startswith("/"):
                url_text = urljoin(base_url, url_text)
            elif not re.match(r"^https?://", url_text, flags=re.IGNORECASE):
                continue

            lower_url = url_text.lower()
            if re.search(r"\.(torrent|mkv|mp4)(?:$|[?#])", lower_url):
                add_value(url_text)
            elif any(token in lower_url for token in ("/down/", "res/downurl/", "download")):
                add_value(url_text)

        return ordered

    @staticmethod
    def _pick_preferred_enclosure(candidates: List[str]) -> str:
        if not candidates:
            return ""
        for item in candidates:
            value = str(item or "").strip()
            if value.lower().startswith("magnet:?"):
                return value
        for item in candidates:
            value = str(item or "").strip()
            if re.search(r"\.torrent(?:$|[?#])", value, flags=re.IGNORECASE):
                return value
        for item in candidates:
            value = str(item or "").strip()
            if re.search(r"\.(mkv|mp4)(?:$|[?#])", value, flags=re.IGNORECASE):
                return value
        return str(candidates[0] or "").strip()

    @staticmethod
    def _extract_js_object(html: str, marker: str) -> Optional[Dict[str, Any]]:
        if not html or marker not in html:
            return None
        marker_pos = html.find(marker)
        if marker_pos < 0:
            return None
        equal_pos = html.find("=", marker_pos)
        if equal_pos < 0:
            return None

        start = equal_pos + 1
        while start < len(html) and html[start] in (" ", "\t", "\r", "\n"):
            start += 1
        if start >= len(html) or html[start] not in ("{", "["):
            return None

        payload = GyingIndexer._extract_balanced_json(html, start)
        if not payload:
            return None
        try:
            obj = json.loads(payload)
            if isinstance(obj, dict):
                return obj
        except Exception as err:
            logger.debug(f"GYing json parse failed for {marker}: {err}")
        return None

    @staticmethod
    def _extract_balanced_json(text: str, start: int) -> Optional[str]:
        pair = {"{": "}", "[": "]"}
        open_char = text[start]
        close_char = pair.get(open_char)
        if not close_char:
            return None

        stack = [close_char]
        in_string = False
        escaped = False
        idx = start + 1
        while idx < len(text):
            char = text[idx]
            if in_string:
                if escaped:
                    escaped = False
                elif char == "\\":
                    escaped = True
                elif char == "\"":
                    in_string = False
            else:
                if char == "\"":
                    in_string = True
                elif char in ("{", "["):
                    stack.append(pair[char])
                elif char in ("}", "]"):
                    if not stack or char != stack[-1]:
                        return None
                    stack.pop()
                    if not stack:
                        return text[start:idx + 1]
            idx += 1
        return None

    @staticmethod
    def _as_list(data: Any) -> List[Any]:
        if data is None:
            return []
        if isinstance(data, list):
            return data
        return [data]

    @staticmethod
    def _safe_at(data: List[Any], index: int) -> Any:
        if index < 0:
            return None
        if index >= len(data):
            return None
        return data[index]

    @staticmethod
    def _to_int(value: Any) -> int:
        if value is None:
            return 0
        if isinstance(value, int):
            return value
        text = str(value).strip().replace(",", "")
        if not text:
            return 0
        if text.isdigit():
            return int(text)
        match = re.search(r"-?\d+", text)
        if not match:
            return 0
        try:
            return int(match.group(0))
        except Exception:
            return 0

    @staticmethod
    def _parse_size_bytes(*size_texts: str) -> int:
        """
        解析体积字符串。
        优先使用带单位的大小（如 7.84GB）。
        若仅有纯数字（如 8），按 GB 处理，避免被识别为字节。
        """
        numeric_value: Optional[float] = None
        unit_map = {
            "K": 1024,
            "M": 1024 ** 2,
            "G": 1024 ** 3,
            "T": 1024 ** 4,
            "P": 1024 ** 5,
        }
        for raw in size_texts:
            text = str(raw or "").strip()
            if not text:
                continue

            # 优先兼容站点常见简写：12.81G / 774.46M / 1.2T
            short_match = re.match(r"^\s*(\d+(?:\.\d+)?)\s*([kKmMgGtTpP])\s*$", text)
            if short_match:
                try:
                    value = float(short_match.group(1))
                    unit = short_match.group(2).upper()
                    factor = unit_map.get(unit)
                    if factor and value > 0:
                        return int(value * factor)
                except Exception:
                    pass

            has_unit = bool(re.search(r"[a-zA-Z]", text))
            if has_unit:
                normalized = re.sub(r"\s+", "", text).upper()
                normalized = normalized.replace("IB", "B")
                unit_tail = re.search(r"(K|M|G|T|P)$", normalized)
                if unit_tail:
                    normalized = f"{normalized}B"
                size = StringUtils.num_filesize(normalized)
                if size > 0:
                    return size

            if re.match(r"^\d+(?:\.\d+)?$", text):
                try:
                    numeric_value = float(text)
                except Exception:
                    continue

        if numeric_value and numeric_value > 0:
            return int(numeric_value * 1024 ** 3)
        return 0
