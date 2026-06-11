import hashlib
import html
import importlib
import json
import re
import threading
from collections import deque
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import datetime
from time import perf_counter
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from urllib.parse import quote, unquote, urljoin, urlparse

import requests
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
    plugin_icon = "https://raw.githubusercontent.com/yang124541/moviepilot-plugin/main/gying.png"
    plugin_version = "2.0.10"
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
    _login_username = ""
    _login_password = ""
    _detail_concurrency = 6
    _runtime_site_cookies: Dict[str, str] = {}
    _runtime_site_base_urls: Dict[str, str] = {}

    _default_hosts: Set[str] = {
        "xn--kivn76b41nnhi.com",
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
            self._login_username = str(
                config.get("login_username")
                or config.get("username")
                or ""
            ).strip()
            self._login_password = str(
                config.get("login_password")
                or config.get("password")
                or ""
            ).strip()
            try:
                self._detail_concurrency = max(1, min(100, int(config.get("detail_concurrency") or 6)))
            except Exception:
                self._detail_concurrency = 6

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
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "login_username",
                                            "label": "观影账号",
                                            "placeholder": "请输入 gying.si 账号",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "login_password",
                                            "label": "观影密码",
                                            "type": "password",
                                            "placeholder": "请输入 gying.si 密码",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "detail_concurrency",
                                            "label": "详情并发数",
                                            "type": "number",
                                            "min": 1,
                                            "max": 100,
                                            "placeholder": "1-100",
                                        },
                                    }
                                ],
                            },
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
            "login_username": "",
            "login_password": "",
            "detail_concurrency": 6,
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

        timeout = int(site.get("timeout") or 20)
        ua = site.get("ua") or settings.USER_AGENT
        proxies = settings.PROXY if site.get("proxy") else None
        logger.info(
            f"观影(GYing)开始搜索：关键词='{keyword}'，"
            f"过滤开关[1080={self._enable_1080}, 中字1080={self._enable_zh1080}, "
            f"4K={self._enable_4k}, 中字4K={self._enable_zh4k}, 原盘={self._include_original}]"
        )

        start_at = datetime.now()
        total_started_at = perf_counter()
        preflight_started_at = perf_counter()
        base_url = self._resolve_base_url(site=site, ua=ua, proxies=proxies, timeout=timeout)
        if not base_url:
            return []
        referer = base_url
        cookie, warm_cache = self._resolve_site_cookie(
            site=site,
            base_url=base_url,
            ua=ua,
            proxies=proxies,
            timeout=timeout,
            keyword=keyword,
        )
        preflight_elapsed_ms = (perf_counter() - preflight_started_at) * 1000
        logger.debug(
            f"观影(GYing)前置预检耗时：关键词='{keyword}'，"
            f"base_url={base_url}，耗时={preflight_elapsed_ms:.1f}ms"
        )

        try:
            client = RequestUtils(
                ua=ua,
                cookies=cookie,
                proxies=proxies,
                timeout=timeout,
                referer=referer
            )
            request_state: Dict[str, Any] = {"http": 0, "cache_hit": 0, "http_time_ms": 0.0}
            url_cache: Dict[str, str] = dict(warm_cache or {})
            guarded_get = self._make_cached_get(
                client=client,
                url_cache=url_cache,
                request_state=request_state,
                base_url=base_url,
                ua=ua,
                proxies=proxies,
                timeout=timeout,
                cookie=cookie,
                site=site,
            )
            collect_started_at = perf_counter()
            search_entries = self._collect_search_entries(
                client=client,
                base_url=base_url,
                keyword=keyword,
                fetcher=guarded_get
            )
            collect_elapsed_ms = (perf_counter() - collect_started_at) * 1000
            logger.debug(
                f"观影(GYing)搜索页采集耗时：关键词='{keyword}'，条目数={len(search_entries)}，"
                f"耗时={collect_elapsed_ms:.1f}ms"
            )
            search_video_count = len(search_entries)
            if not search_entries:
                cost = (datetime.now() - start_at).seconds
                logger.info(
                    f"观影(GYing)搜索完成：关键词='{keyword}'，找到视频=0，返回磁力=0，耗时={cost}s"
                )
                return []

            results: List[TorrentInfo] = []
            parent_title_cache: Dict[str, str] = {}
            parent_year_cache: Dict[str, str] = {}
            parent_default_dir: Dict[str, str] = {}
            parent_down_entries_cache: Dict[str, List[Dict[str, Any]]] = {}
            parent_down_entry_map_cache: Dict[str, Dict[str, Dict[str, Any]]] = {}
            bt_parent_cache: Dict[str, str] = {}
            skip_keyword_parent_keys: Set[str] = set()
            result_ids: Set[str] = set()
            shared_lock = threading.RLock()
            parent_down_waiters: Dict[str, threading.Event] = {}
            worker_count = min(len(search_entries), max(1, int(self._detail_concurrency or 1)))
            ordered_results: Dict[int, Optional[Tuple[str, TorrentInfo]]] = {}
            pending_child_ids: Set[str] = set()
            resolved_result_ids: Set[str] = set()
            outstanding_search_ids: Set[str] = {
                str(entry.get("id") or "").strip()
                for entry in search_entries
                if str(entry.get("id") or "").strip()
            }
            task_queue = deque(
                {
                    "kind": "search",
                    "order": index,
                    "entry": entry,
                    "resource_id": str(entry.get("id") or "").strip(),
                }
                for index, entry in enumerate(search_entries)
            )
            next_order = len(search_entries)

            def _task_title(task: Dict[str, Any]) -> str:
                task_kind = str(task.get("kind") or "search").strip().lower()
                if task_kind == "child":
                    return str((task.get("down_item") or {}).get("title") or "").strip()
                return str((task.get("entry") or {}).get("title") or "").strip()

            def _submit_task(executor: ThreadPoolExecutor, task: Dict[str, Any]):
                task["started_at"] = perf_counter()
                task_kind = str(task.get("kind") or "search").strip().lower()
                if task_kind == "child":
                    return executor.submit(
                        self._build_child_result_entry,
                        task["cache_key"],
                        task["parent_dir"],
                        task["parent_id"],
                        task["down_item"],
                        site,
                        keyword,
                        client,
                        base_url,
                        guarded_get,
                        parent_title_cache,
                        parent_year_cache,
                        skip_keyword_parent_keys,
                        shared_lock,
                        task["default_dir"],
                    )
                return executor.submit(
                    self._build_search_result_entry,
                    task["entry"],
                    site,
                    keyword,
                    client,
                    base_url,
                    guarded_get,
                    parent_title_cache,
                    parent_year_cache,
                    parent_default_dir,
                    parent_down_entries_cache,
                    parent_down_entry_map_cache,
                    bt_parent_cache,
                    skip_keyword_parent_keys,
                    parent_down_waiters,
                    shared_lock,
                )

            concurrent_started_at = perf_counter()
            with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="gying") as executor:
                running_tasks: Dict[Any, Dict[str, Any]] = {}
                while task_queue or running_tasks:
                    while task_queue and len(running_tasks) < worker_count:
                        task = task_queue.popleft()
                        running_tasks[_submit_task(executor, task)] = task

                    if not running_tasks:
                        break

                    done, _ = wait(tuple(running_tasks.keys()), return_when=FIRST_COMPLETED)
                    for future in done:
                        task = running_tasks.pop(future)
                        task_kind = str(task.get("kind") or "search").strip().lower()
                        task_order = int(task.get("order") or 0)
                        task_resource_id = str(task.get("resource_id") or "").strip()
                        task_elapsed_ms = (perf_counter() - float(task.get("started_at") or perf_counter())) * 1000

                        try:
                            item = future.result()
                        except Exception as err:
                            if task_kind == "child":
                                logger.debug(f"观影(GYing)并发处理父级子资源异常：{err}")
                            else:
                                logger.debug(f"观影(GYing)并发处理搜索条目异常：{err}")
                            item = None

                        ordered_results[task_order] = item

                        if task_kind == "search" and task_resource_id:
                            outstanding_search_ids.discard(task_resource_id)

                        if item:
                            resolved_result_ids.add(item[0])

                        if task_kind != "search":
                            continue

                        pending_children = self._collect_pending_child_tasks(
                            parent_default_dir=parent_default_dir,
                            parent_down_entries_cache=parent_down_entries_cache,
                            outstanding_search_ids=outstanding_search_ids,
                            pending_child_ids=pending_child_ids,
                            resolved_result_ids=resolved_result_ids,
                            shared_lock=shared_lock,
                        )
                        for cache_key, parent_dir, parent_id, down_item, default_dir in pending_children:
                            child_id = str(down_item.get("id") or "").strip()
                            task_queue.append(
                                {
                                    "kind": "child",
                                    "order": next_order,
                                    "resource_id": child_id,
                                    "cache_key": cache_key,
                                    "parent_dir": parent_dir,
                                    "parent_id": parent_id,
                                    "down_item": down_item,
                                    "default_dir": default_dir,
                                }
                            )
                            next_order += 1

            concurrent_elapsed_ms = (perf_counter() - concurrent_started_at) * 1000
            for index in sorted(ordered_results.keys()):
                item = ordered_results.get(index)
                if not item:
                    continue
                res_id, torrent = item
                if res_id in result_ids:
                    continue
                results.append(torrent)
                result_ids.add(res_id)

            total_elapsed_ms = (perf_counter() - total_started_at) * 1000
            logger.debug(
                f"观影(GYing)耗时汇总：关键词='{keyword}'，前置预检={preflight_elapsed_ms:.1f}ms，搜索页采集={collect_elapsed_ms:.1f}ms，"
                f"并发阶段={concurrent_elapsed_ms:.1f}ms，总耗时={total_elapsed_ms:.1f}ms，"
                f"HTTP累计={float(request_state.get('http_time_ms') or 0.0):.1f}ms"
            )
            cost = (datetime.now() - start_at).seconds
            logger.info(
                f"观影(GYing)搜索完成：关键词='{keyword}'，找到视频={search_video_count}，"
                f"返回磁力={len(results)}，耗时={cost}s"
            )
            return results
        except Exception as err:
            logger.error(f"观影(GYing)搜索异常：关键词='{keyword}'，错误={err}")
            return []

    def _build_search_result_entry(self, entry: Dict[str, Any], site: dict, keyword: str,
                                   client: RequestUtils, base_url: str,
                                   fetcher: Callable[[str], str],
                                   parent_title_cache: Dict[str, str],
                                   parent_year_cache: Dict[str, str],
                                   parent_default_dir: Dict[str, str],
                                   parent_down_entries_cache: Dict[str, List[Dict[str, Any]]],
                                   parent_down_entry_map_cache: Dict[str, Dict[str, Dict[str, Any]]],
                                   bt_parent_cache: Dict[str, str],
                                   skip_keyword_parent_keys: Set[str],
                                   parent_down_waiters: Dict[str, threading.Event],
                                   shared_lock: threading.RLock) -> Optional[Tuple[str, TorrentInfo]]:
        res_id = str(entry.get("id") or "").strip()
        if not res_id:
            return None
        res_dir = str(entry.get("dir") or "bt").strip().lower() or "bt"
        skip_keyword_match = bool(entry.get("__skip_keyword_match"))

        title = str(entry.get("title") or "").strip()
        if not title:
            return None
        search_quality_code = str(entry.get("quality") or "").strip().lower()
        search_tag_label = str(entry.get("tag") or "").strip()

        if res_dir in ("tv", "ac", "mv"):
            self._ensure_parent_down_entries_cached(
                client=client,
                base_url=base_url,
                parent_dir=res_dir,
                parent_id=res_id,
                fetcher=fetcher,
                parent_title_cache=parent_title_cache,
                parent_year_cache=parent_year_cache,
                parent_default_dir=parent_default_dir,
                parent_down_entries_cache=parent_down_entries_cache,
                parent_down_entry_map_cache=parent_down_entry_map_cache,
                bt_parent_cache=bt_parent_cache,
                skip_keyword_parent_keys=skip_keyword_parent_keys,
                parent_down_waiters=parent_down_waiters,
                shared_lock=shared_lock,
                default_dir="bt",
                parent_title=title,
                parent_year=str(entry.get("year") or "").strip(),
                skip_keyword_match=skip_keyword_match,
            )
            return None

        detail_url = urljoin(base_url, f"{res_dir}/{res_id}")
        detail_data: Dict[str, Any] = {}
        tag_code = ""
        tag_label = ""
        down_item: Dict[str, Any] = {}
        with shared_lock:
            cache_key = str(bt_parent_cache.get(res_id) or "").strip()
            if cache_key:
                down_item = parent_down_entry_map_cache.get(cache_key, {}).get(res_id) or {}

        if down_item:
            res_dir = str(down_item.get("dir") or res_dir).strip().lower() or res_dir
            title = str(down_item.get("title") or title).strip() or title
            detail_url = urljoin(base_url, f"{res_dir}/{res_id}")
            tag_code = str(down_item.get("quality") or "").strip().lower()
            tag_label = str(down_item.get("quality_label") or "").strip()
        else:
            detail_html = fetcher(detail_url)
            if detail_html:
                _detail_data = self._extract_js_object(detail_html, "_obj.d")
                if isinstance(_detail_data, dict):
                    detail_data = _detail_data

            parent_dir, parent_id = self._parse_parent_route(detail_data.get("du"))
            if parent_dir and parent_id:
                cache_key = self._ensure_parent_down_entries_cached(
                    client=client,
                    base_url=base_url,
                    parent_dir=parent_dir,
                    parent_id=parent_id,
                    fetcher=fetcher,
                    parent_title_cache=parent_title_cache,
                    parent_year_cache=parent_year_cache,
                    parent_default_dir=parent_default_dir,
                    parent_down_entries_cache=parent_down_entries_cache,
                    parent_down_entry_map_cache=parent_down_entry_map_cache,
                    bt_parent_cache=bt_parent_cache,
                    skip_keyword_parent_keys=skip_keyword_parent_keys,
                    parent_down_waiters=parent_down_waiters,
                    shared_lock=shared_lock,
                    default_dir=res_dir,
                    parent_title="",
                    parent_year="",
                    skip_keyword_match=False,
                )
                with shared_lock:
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
            return None

        entry_hash = str(down_item.get("hash") or "").strip()
        enclosure, detail_data = self._resolve_enclosure(
            client=client,
            base_url=base_url,
            resource_dir=res_dir,
            resource_id=res_id,
            title=title,
            info_hash=entry_hash,
            detail_data=detail_data,
            fetcher=fetcher
        )
        if not enclosure:
            return None

        with shared_lock:
            parent_title = str(parent_title_cache.get(cache_key) or "").strip()
            parent_year = str(parent_year_cache.get(cache_key) or "").strip()

        down_size_text = str(down_item.get("size") or "").strip()
        detail_size_text = str(detail_data.get("s") or detail_data.get("size") or "").strip()
        search_size_text = str(entry.get("size") or "").strip()
        size_bytes = self._parse_size_bytes(down_size_text, detail_size_text, search_size_text)
        seeds_text = down_item.get("seeds") or entry.get("seeds")
        elapsed_text = str(down_item.get("time") or entry.get("time") or "").strip()
        tag_text = tag_label or search_tag_label
        detail_title = str(detail_data.get("title") or title).strip()
        title_for_match = self._build_match_title(
            title=title,
            parent_title=parent_title,
            parent_year=parent_year
        )
        if parent_title and cache_key not in skip_keyword_parent_keys:
            if not self._is_keyword_related(
                keyword,
                title_for_match,
                detail_title,
                parent_title,
                title
            ):
                return None
        desc_parts = [x for x in [tag_text, detail_title, parent_title] if x]
        description = " | ".join(desc_parts[:3])
        if parent_year and not re.search(r"(19|20)\d{2}", description):
            description = f"{description} {parent_year}".strip()
        description = self._append_unique_marker(
            description=description or detail_title or title,
            resource_id=res_id,
            enclosure=enclosure
        )
        return res_id, TorrentInfo(
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
        )

    def _build_child_result_entry(self, cache_key: str, parent_dir: str, parent_id: str,
                                  down_item: Dict[str, Any], site: dict, keyword: str,
                                  client: RequestUtils, base_url: str,
                                   fetcher: Callable[[str], str],
                                   parent_title_cache: Dict[str, str],
                                   parent_year_cache: Dict[str, str],
                                   skip_keyword_parent_keys: Set[str],
                                   shared_lock: threading.RLock,
                                   default_dir: str = "bt") -> Optional[Tuple[str, TorrentInfo]]:
        child_id = str(down_item.get("id") or "").strip()
        if not child_id:
            return None
        child_title = str(down_item.get("title") or "").strip()
        if not child_title:
            return None
        child_quality_code = str(down_item.get("quality") or "").strip().lower()
        child_quality_label = str(
            down_item.get("quality_label")
            or self._quality_label_by_code.get(child_quality_code)
            or ""
        ).strip()
        child_dir = str(down_item.get("dir") or default_dir).strip().lower() or "bt"
        child_detail_url = urljoin(base_url, f"{child_dir}/{child_id}")
        child_hash = str(down_item.get("hash") or "").strip()
        child_detail_data: Dict[str, Any] = {}
        enclosure, child_detail_data = self._resolve_enclosure(
            client=client,
            base_url=base_url,
            resource_dir=child_dir,
            resource_id=child_id,
            title=child_title,
            info_hash=child_hash,
            detail_data=child_detail_data,
            fetcher=fetcher
        )
        if not enclosure:
            return None

        with shared_lock:
            parent_title = str(parent_title_cache.get(cache_key) or "").strip()
            parent_year = str(parent_year_cache.get(cache_key) or "").strip()

        down_size_text = str(down_item.get("size") or "").strip()
        detail_size_text = str(child_detail_data.get("s") or child_detail_data.get("size") or "").strip()
        size_bytes = self._parse_size_bytes(down_size_text, detail_size_text)
        seeds_text = down_item.get("seeds")
        elapsed_text = str(down_item.get("time") or "").strip()
        tag_text = child_quality_label
        detail_title = str(child_detail_data.get("title") or child_title).strip()
        title_for_match = self._build_match_title(
            title=child_title,
            parent_title=parent_title,
            parent_year=parent_year
        )
        if parent_title and cache_key not in skip_keyword_parent_keys:
            if not self._is_keyword_related(
                keyword,
                title_for_match,
                detail_title,
                parent_title,
                child_title
            ):
                return None
        desc_parts = [x for x in [tag_text, detail_title, parent_title] if x]
        description = " | ".join(desc_parts[:3])
        if parent_year and not re.search(r"(19|20)\d{2}", description):
            description = f"{description} {parent_year}".strip()
        description = self._append_unique_marker(
            description=description or detail_title or child_title,
            resource_id=child_id,
            enclosure=enclosure
        )

        return child_id, TorrentInfo(
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
        )

    def _collect_pending_child_tasks(self,
                                     parent_default_dir: Dict[str, str],
                                     parent_down_entries_cache: Dict[str, List[Dict[str, Any]]],
                                     outstanding_search_ids: Set[str],
                                     pending_child_ids: Set[str],
                                     resolved_result_ids: Set[str],
                                     shared_lock: threading.RLock) -> List[Tuple[str, str, str, Dict[str, Any], str]]:
        with shared_lock:
            parent_snapshots = [
                (
                    str(cache_key or "").strip(),
                    str(parent_default_dir.get(cache_key) or "bt").strip().lower() or "bt",
                    [dict(item) for item in (down_entries or []) if isinstance(item, dict)],
                )
                for cache_key, down_entries in parent_down_entries_cache.items()
            ]

        pending_children: List[Tuple[str, str, str, Dict[str, Any], str]] = []
        for cache_key, default_dir, down_entries in parent_snapshots:
            if not cache_key or not down_entries:
                continue
            try:
                parent_dir, parent_id = cache_key.split("/", 1)
            except Exception:
                continue

            for down_item in down_entries:
                child_id = str(down_item.get("id") or "").strip()
                if not child_id:
                    continue
                if child_id in outstanding_search_ids or child_id in pending_child_ids or child_id in resolved_result_ids:
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

                pending_child_ids.add(child_id)
                pending_children.append((cache_key, parent_dir, parent_id, dict(down_item), default_dir))

        return pending_children

    def _ensure_parent_down_entries_cached(self, client: RequestUtils, base_url: str,
                                           parent_dir: str, parent_id: str,
                                           fetcher: Callable[[str], str],
                                           parent_title_cache: Dict[str, str],
                                           parent_year_cache: Dict[str, str],
                                           parent_default_dir: Dict[str, str],
                                           parent_down_entries_cache: Dict[str, List[Dict[str, Any]]],
                                           parent_down_entry_map_cache: Dict[str, Dict[str, Dict[str, Any]]],
                                           bt_parent_cache: Dict[str, str],
                                           skip_keyword_parent_keys: Set[str],
                                           parent_down_waiters: Dict[str, threading.Event],
                                           shared_lock: threading.RLock,
                                           default_dir: str = "bt",
                                           parent_title: str = "",
                                           parent_year: str = "",
                                           skip_keyword_match: bool = False) -> str:
        cache_key = f"{parent_dir}/{parent_id}"
        with shared_lock:
            parent_default_dir.setdefault(cache_key, default_dir)
            if parent_title:
                parent_title_cache.setdefault(cache_key, parent_title)
            if parent_year:
                parent_year_cache.setdefault(cache_key, parent_year)
            if skip_keyword_match:
                skip_keyword_parent_keys.add(cache_key)
            cached_entries = parent_down_entries_cache.get(cache_key)
            if cached_entries is not None:
                return cache_key
            waiter = parent_down_waiters.get(cache_key)
            if waiter is None:
                waiter = threading.Event()
                parent_down_waiters[cache_key] = waiter
                is_leader = True
            else:
                is_leader = False
        if not is_leader:
            waiter.wait()
            return cache_key
        try:
            down_entries = self._fetch_parent_down_entries(
                client=client,
                base_url=base_url,
                parent_dir=parent_dir,
                parent_id=parent_id,
                fetcher=fetcher
            )
            id_map: Dict[str, Dict[str, Any]] = {}
            for item in down_entries:
                child_id = str(item.get("id") or "").strip()
                if not child_id:
                    continue
                id_map[child_id] = item
            with shared_lock:
                if cache_key not in parent_down_entries_cache:
                    parent_down_entries_cache[cache_key] = down_entries
                    parent_down_entry_map_cache[cache_key] = id_map
                existing_map = parent_down_entry_map_cache.get(cache_key, {})
                for child_id in existing_map:
                    bt_parent_cache[child_id] = cache_key
        finally:
            with shared_lock:
                current = parent_down_waiters.pop(cache_key, None)
            if current is not None:
                current.set()
        return cache_key

    def _resolve_site_cookie(self, site: dict, base_url: str, ua: str,
                             proxies: Optional[Dict[str, str]], timeout: int,
                             keyword: str) -> Tuple[str, Dict[str, str]]:
        cookie = self._get_runtime_site_cookie(site=site, base_url=base_url)
        warm_cache: Dict[str, str] = {}

        username = str(
            self._login_username
            or site.get("username")
            or site.get("user")
            or site.get("account")
            or site.get("email")
            or ""
        ).strip()
        password = str(
            self._login_password
            or site.get("password")
            or site.get("passwd")
            or site.get("pass")
            or site.get("pwd")
            or ""
        ).strip()
        has_credentials = bool(username and password)

        # 预检搜索页是否就绪；遇到 PoW 会在同一 session 内自动解决
        ready, effective_cookie, ready_url, ready_body = self._is_search_response_ready(
            base_url=base_url, keyword=keyword, ua=ua,
            proxies=proxies, timeout=timeout, cookie=cookie,
        )
        if ready:
            if ready_url and ready_body:
                warm_cache[ready_url] = ready_body
            # 如果解了 PoW 导致 cookie 有更新，回写持久化
            if effective_cookie and effective_cookie != cookie:
                site["cookie"] = effective_cookie
                self._remember_runtime_site_cookie(site=site, base_url=base_url, cookie=effective_cookie)
                persisted = self._persist_site_cookie(site=site, cookie=effective_cookie)
                if persisted:
                    logger.info("观影(GYing)PoW 验证通过，已回写站点 cookie。")
                else:
                    logger.info("观影(GYing)PoW 验证通过，已刷新运行时 cookie。")
            return effective_cookie, warm_cache

        if not has_credentials:
            # 无账号密码时，尝试独立解决 PoW 挑战
            pow_cookie = self._try_solve_pow_standalone(
                base_url=base_url, ua=ua, proxies=proxies, timeout=timeout, existing_cookie=cookie
            )
            if pow_cookie:
                merged_cookie = self._merge_cookie_str(cookie, pow_cookie)
                ready2, merged_cookie, ready_url2, ready_body2 = self._is_search_response_ready(
                    base_url=base_url, keyword=keyword, ua=ua,
                    proxies=proxies, timeout=timeout, cookie=merged_cookie
                )
                if ready2:
                    if ready_url2 and ready_body2:
                        warm_cache[ready_url2] = ready_body2
                    site["cookie"] = merged_cookie
                    self._remember_runtime_site_cookie(site=site, base_url=base_url, cookie=merged_cookie)
                    persisted = self._persist_site_cookie(site=site, cookie=merged_cookie)
                    if persisted:
                        logger.info("观影(GYing)PoW 验证通过，已回写站点 cookie。")
                    else:
                        logger.info("观影(GYing)PoW 验证通过，已刷新运行时 cookie。")
                    return merged_cookie, warm_cache
            logger.warn("观影(GYing)cookie 已失效且未配置可用账号密码，无法自动登录。")
            return cookie, warm_cache

        refreshed = self._login_and_get_cookie(
            base_url=base_url,
            username=username,
            password=password,
            ua=ua,
            proxies=proxies,
            timeout=timeout,
            existing_cookie=effective_cookie or cookie,
        )
        if not refreshed:
            logger.warn("观影(GYing)自动登录失败，继续使用现有 cookie 搜索。")
            fallback_cookie = self._normalize_cookie_header(effective_cookie or cookie)
            if fallback_cookie and fallback_cookie != cookie:
                site["cookie"] = fallback_cookie
                self._remember_runtime_site_cookie(site=site, base_url=base_url, cookie=fallback_cookie)
                self._persist_site_cookie(site=site, cookie=fallback_cookie)
            return fallback_cookie or cookie, warm_cache

        # 登录成功后直接回写，不再发第二次预检请求（避免再次触发 PoW）
        site["cookie"] = refreshed
        self._remember_runtime_site_cookie(site=site, base_url=base_url, cookie=refreshed)
        persisted = self._persist_site_cookie(site=site, cookie=refreshed)
        if persisted:
            logger.info("观影(GYing)检测到 cookie 失效，已自动登录并刷新会话，且已回写站点 cookie。")
        else:
            logger.info("观影(GYing)检测到 cookie 失效，已自动登录并刷新会话。")
            logger.warn("观影(GYing)未能回写站点 cookie，本次搜索仍将使用新会话。")
        return refreshed, warm_cache

    def _try_solve_pow_standalone(self, base_url: str, ua: str,
                                  proxies: Optional[Dict[str, str]],
                                  timeout: int, existing_cookie: str = "") -> str:
        """
        在无登录流程的情况下独立求解 PoW 挑战，返回验证后的 cookie 字符串。
        失败或无 PoW 挑战时返回空字符串。
        """
        try:
            with requests.Session() as session:
                session.proxies.update(proxies or {})
                session.headers.update({"User-Agent": ua or settings.USER_AGENT, "Referer": base_url})
                if existing_cookie:
                    self._load_cookie_header_to_session(session=session, cookie=existing_cookie)
                resp = session.get(base_url, timeout=max(5, int(timeout or 20)))
                if not resp.ok or not self._is_pow_page(resp.text):
                    return ""
                ok = self._handle_pow_in_session(
                    session=session,
                    base_url=base_url,
                    html_text=resp.text,
                    ua=ua or settings.USER_AGENT,
                    proxies=proxies,
                    timeout=timeout,
                    target_url=resp.url or base_url,
                )
                if not ok:
                    return ""
                return self._cookie_jar_to_header(session.cookies)
        except Exception as err:
            logger.warn(f"观影(GYing)独立 PoW 求解异常：{err}")
            return ""

    @staticmethod
    def _merge_cookie_str(base: str, extra: str) -> str:
        """合并两个 cookie 字符串，extra 中同名字段覆盖 base。"""
        if not extra:
            return base
        if not base:
            return extra
        parts: Dict[str, str] = {}
        for raw in (base, extra):
            for item in str(raw or "").split(";"):
                item = item.strip()
                if "=" in item:
                    k, v = item.split("=", 1)
                    k = k.strip()
                    if k:
                        parts[k] = v.strip()
        return "; ".join(f"{k}={v}" for k, v in parts.items())

    def _persist_site_cookie(self, site: dict, cookie: str) -> bool:
        cookie_text = self._normalize_cookie_header(cookie)
        if not cookie_text or not isinstance(site, dict):
            return False

        site_id = site.get("id")
        site["cookie"] = cookie_text

        oper_candidates: List[Tuple[str, str]] = [
            ("app.db.site_oper", "SiteOper"),
            ("app.db.siteoper", "SiteOper"),
            ("app.db.site", "SiteOper"),
        ]
        method_candidates: Tuple[str, ...] = (
            "update_cookie",
            "update_site_cookie",
            "save_cookie",
            "set_cookie",
            "update",
            "update_site",
            "save",
            "upsert",
        )

        for module_name, class_name in oper_candidates:
            try:
                module = importlib.import_module(module_name)
                oper_cls = getattr(module, class_name, None)
                if not oper_cls:
                    continue
                oper = oper_cls()
            except Exception:
                continue

            for method_name in method_candidates:
                method = getattr(oper, method_name, None)
                if not callable(method):
                    continue
                if self._try_site_update_method(
                    method=method,
                    site_id=site_id,
                    cookie_text=cookie_text,
                ):
                    return True
        return False

    @staticmethod
    def _try_site_update_method(method: Callable[..., Any], site_id: Any,
                                cookie_text: str) -> bool:
        has_site_id = site_id is not None and str(site_id).strip() != ""
        patch_data = {"cookie": cookie_text}
        attempts: List[Tuple[Tuple[Any, ...], Dict[str, Any]]] = []
        if has_site_id:
            attempts.extend([
                ((site_id, cookie_text), {}),
                ((site_id, patch_data), {}),
                ((), {"site_id": site_id, "cookie": cookie_text}),
                ((), {"id": site_id, "cookie": cookie_text}),
                ((), {"site_id": site_id, "data": patch_data}),
                ((), {"id": site_id, "data": patch_data}),
                ((), {"site_id": site_id, "payload": patch_data}),
                ((), {"id": site_id, "payload": patch_data}),
                ((), {"site_id": site_id, "update": patch_data}),
                ((), {"id": site_id, "update": patch_data}),
            ])

        for args, kwargs in attempts:
            try:
                result = method(*args, **kwargs)
            except TypeError:
                continue
            except Exception:
                continue
            if GyingIndexer._is_site_update_success(result):
                return True
        return False

    @staticmethod
    def _normalize_cookie_header(cookie: str) -> str:
        raw = str(cookie or "").replace("\r", ";").replace("\n", ";").strip()
        if not raw:
            return ""
        parts: List[str] = []
        for item in raw.split(";"):
            token = str(item or "").strip()
            if not token or "=" not in token:
                continue
            name, value = token.split("=", 1)
            name = name.strip()
            value = value.strip()
            if not name:
                continue
            parts.append(f"{name}={value}")
        return "; ".join(parts)

    @staticmethod
    def _is_site_update_success(result: Any) -> bool:
        if result is None:
            return True
        if isinstance(result, bool):
            return result
        if isinstance(result, (int, float)):
            return int(result) >= 0
        if isinstance(result, dict):
            if "success" in result:
                return bool(result.get("success"))
            if "code" in result:
                try:
                    return int(result.get("code") or 0) in (0, 200)
                except Exception:
                    return False
            if "id" in result or "site_id" in result:
                return True
            return False
        return True

    @staticmethod
    def _cookie_jar_to_header(jar: requests.cookies.RequestsCookieJar) -> str:
        if not jar:
            return ""
        parts: List[str] = []
        for item in jar:
            name = str(getattr(item, "name", "") or "").strip()
            value = str(getattr(item, "value", "") or "").strip()
            if name:
                parts.append(f"{name}={value}")
        return "; ".join(parts)

    @staticmethod
    def _load_cookie_header_to_session(session: requests.Session, cookie: str) -> None:
        cookie_text = GyingIndexer._normalize_cookie_header(cookie)
        if not cookie_text:
            return
        session.headers.pop("Cookie", None)
        for item in cookie_text.split(";"):
            token = str(item or "").strip()
            if "=" not in token:
                continue
            name, value = token.split("=", 1)
            name = name.strip()
            value = value.strip()
            if name:
                session.cookies.set(name, value)

    @staticmethod
    def _is_login_shell(html_text: str) -> bool:
        text = str(html_text or "")
        if not text:
            return True
        return ("_BT.PC.HTML('login')" in text) or ('_BT.PC.HTML("login")' in text)

    @staticmethod
    def _is_retired_host_page(html_text: str) -> bool:
        text = str(html_text or "")
        return "当前网址将在不久后失效" in text and "获取新网址" in text and "/urlop/" in text

    @staticmethod
    def _is_pow_page(html_text: str) -> bool:
        text = str(html_text or "")
        if GyingIndexer._is_res_pow_page(text):
            return True
        return (
            (
                "正在确认你是不是机器人" in text
                or "浏览器安全验证" in text
                or "正在进行浏览器计算验证" in text
                or "安全验证" in text
            )
            and "challenge" in text
            and "diff" in text
        )

    @staticmethod
    def _is_res_pow_page(html_text: str) -> bool:
        text = str(html_text or "")
        if not text:
            return False
        return (
            ("/res/pow" in text or "powSolve-" in text or "pow.worker-" in text)
            and (
                "浏览器安全验证" in text
                or "正在进行浏览器计算验证" in text
                or "安全验证" in text
            )
        )

    @staticmethod
    def _detect_pow_challenge(html_text: str) -> Optional[Dict[str, Any]]:
        """从人机验证页面解析 PoW 挑战参数。"""
        text = str(html_text or "")
        payload = ""
        for pattern in (
            r'const\s+json\s*=\s*(\{.*?\})\s*;\s*const\s+jss\s*=',
            r'const\s+json\s*=\s*(\{.*?\})\s*;',
        ):
            match = re.search(pattern, text, re.S)
            if match:
                payload = str(match.group(1) or "").strip()
                break
        if not payload:
            return None
        try:
            obj = json.loads(payload)
            if all(k in obj for k in ("id", "challenge", "diff", "salt")):
                obj["type"] = "legacy"
                return obj
        except Exception:
            pass
        if GyingIndexer._is_res_pow_page(text):
            return {"type": "res_pow"}
        return None

    @staticmethod
    def _solve_pow(challenge_hashes: List[str], diff: int, salt: str) -> List[int]:
        """
        暴力求解 PoW。
        算法：SHA256(str(nonce) + salt_ascii)，nonce 从 0 枚举到 diff。
        返回按发现顺序（数值升序）排列的 nonce 列表，与 powSolve.js 行为一致。
        """
        remaining: Set[str] = set(challenge_hashes)
        found: List[int] = []
        salt_bytes = salt.encode("ascii")

        for nonce in range(diff + 2):
            if not remaining:
                break
            h = hashlib.sha256(str(nonce).encode("ascii") + salt_bytes).hexdigest()
            if h in remaining:
                found.append(nonce)
                remaining.discard(h)

        return found

    def _fetch_res_pow_challenge(self, session: requests.Session, base_url: str,
                                 proxies: Optional[Dict[str, str]], timeout: int,
                                 target_url: str = "") -> Optional[Dict[str, Any]]:
        challenge_url = urljoin(target_url or base_url, "/res/pow")
        headers = {
            "Accept": "application/json, text/plain, */*",
            "Referer": target_url or base_url,
        }
        try:
            resp = session.get(
                challenge_url,
                headers=headers,
                proxies=proxies,
                timeout=max(5, int(timeout or 20)),
            )
            body_text = str(resp.text or "").strip()
            logger.debug(f"观影(GYing)PoW 挑战响应：status={resp.status_code}，body={body_text[:200]}")
            if not resp.ok:
                return None
            obj = resp.json()
            if isinstance(obj, dict) and all(k in obj for k in ("N", "x", "t")):
                obj["type"] = "res_pow"
                return obj
        except Exception as err:
            logger.debug(f"观影(GYing)PoW 挑战获取异常：{err}")
        return None

    @staticmethod
    def _solve_res_pow(modulus_hex: str, seed_hex: str, rounds: int) -> str:
        """
        新版 /res/pow 校验。
        算法：从 x 开始做 t 轮 y = y^2 mod N，最终提交十六进制 y。
        """
        try:
            modulus = int(str(modulus_hex or "").strip(), 16)
            value = int(str(seed_hex or "").strip(), 16)
            total_rounds = int(rounds or 0)
        except Exception:
            return ""

        if modulus <= 0 or value < 0 or total_rounds <= 0:
            return ""

        for _ in range(total_rounds):
            value = (value * value) % modulus
        return format(value, "x")

    def _submit_pow_solution(self, session: requests.Session, base_url: str,
                             challenge_id: str, nonces: List[int],
                             ua: str, proxies: Optional[Dict[str, str]],
                             timeout: int, target_url: str = "") -> bool:
        """
        将 PoW 解答提交给服务端（POST 到触发 PoW 的当前页面 URL）。
        格式：application/x-www-form-urlencoded
        Body：action=verify&id=ID&nonce[]=N0&nonce[]=N1
        服务端验证通过后在 session 中写入 browser_verified cookie。
        """
        submit_url = str(target_url or base_url or "").strip() or base_url
        referer_url = submit_url or str(base_url or "").strip()
        parsed_submit = urlparse(referer_url)
        body_parts = [f"action=verify", f"id={challenge_id}"]
        for n in nonces:
            body_parts.append(f"nonce[]={n}")
        body = "&".join(body_parts)

        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Referer": referer_url,
            "Origin": f"{parsed_submit.scheme or 'https'}://{parsed_submit.netloc}",
        }
        try:
            resp = session.post(
                submit_url,
                data=body,
                headers=headers,
                proxies=proxies,
                timeout=max(5, int(timeout or 20)),
            )
            body_text = str(resp.text or "").strip()
            logger.debug(f"观影(GYing)PoW 提交响应：status={resp.status_code}，body={body_text[:200]}")
            if resp.ok:
                try:
                    obj = json.loads(body_text)
                    if isinstance(obj, dict) and obj.get("success") is True:
                        return True
                    if isinstance(obj, dict) and obj.get("success") is False:
                        logger.warn(f"观影(GYing)PoW 验证被拒绝：{body_text[:100]}")
                        return False
                except Exception:
                    pass
                # 无法解析 JSON，但有 cookie 也视为成功
                if session.cookies:
                    return True
        except Exception as e:
            logger.debug(f"观影(GYing)PoW 提交异常：{e}")
        return False

    def _submit_res_pow_solution(self, session: requests.Session, base_url: str,
                                 result_hex: str, ua: str,
                                 proxies: Optional[Dict[str, str]],
                                 timeout: int, target_url: str = "") -> bool:
        submit_url = urljoin(target_url or base_url, "/res/pow")
        referer_url = target_url or base_url
        parsed_submit = urlparse(referer_url)
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json, text/plain, */*",
            "Referer": referer_url,
            "Origin": f"{parsed_submit.scheme or 'https'}://{parsed_submit.netloc}",
        }
        try:
            resp = session.post(
                submit_url,
                data={"y": result_hex},
                headers=headers,
                proxies=proxies,
                timeout=max(5, int(timeout or 20)),
            )
            body_text = str(resp.text or "").strip()
            logger.debug(f"观影(GYing)PoW 提交响应：status={resp.status_code}，body={body_text[:200]}")
            if resp.ok:
                try:
                    obj = resp.json()
                    if isinstance(obj, dict) and obj.get("success") is True:
                        return True
                    if isinstance(obj, dict) and obj.get("success") is False:
                        logger.warn(f"观影(GYing)PoW 验证被拒绝：{body_text[:100]}")
                        return False
                except Exception:
                    pass
                if session.cookies:
                    return True
        except Exception as err:
            logger.debug(f"观影(GYing)PoW 提交异常：{err}")
        return False

    def _handle_pow_in_session(self, session: requests.Session, base_url: str,
                               html_text: str, ua: str,
                               proxies: Optional[Dict[str, str]],
                               timeout: int, target_url: str = "") -> bool:
        """
        若当前页面为 PoW 验证页，则自动求解并通过 session 提交，返回是否成功。
        target_url：触发 PoW 的页面 URL，提交 nonce 将 POST 到该 URL。
        """
        pow_data = self._detect_pow_challenge(html_text)
        if not pow_data:
            return False

        if str(pow_data.get("type") or "") == "res_pow":
            pow_data = self._fetch_res_pow_challenge(
                session=session,
                base_url=base_url,
                proxies=proxies,
                timeout=timeout,
                target_url=target_url or base_url,
            )
            if not pow_data:
                logger.warn("观影(GYing)PoW 挑战获取失败")
                return False

            modulus_hex = str(pow_data.get("N") or "")
            seed_hex = str(pow_data.get("x") or "")
            rounds = int(pow_data.get("t") or 0)
            if not modulus_hex or not seed_hex or not rounds:
                return False

            logger.info(f"观影(GYing)检测到人机验证（PoW），轮数={rounds}，正在计算解答...")
            logger.debug(
                f"观影(GYing)PoW 挑战参数：N={modulus_hex[:32]}...，x={seed_hex[:32]}...，t={rounds}"
            )
            result_hex = self._solve_res_pow(modulus_hex=modulus_hex, seed_hex=seed_hex, rounds=rounds)
            if not result_hex:
                logger.warn("观影(GYing)PoW 求解失败：未生成有效结果")
                return False

            logger.info(f"观影(GYing)PoW 计算完成，结果长度={len(result_hex)}，正在提交...")
            ok = self._submit_res_pow_solution(
                session=session,
                base_url=base_url,
                result_hex=result_hex,
                ua=ua,
                proxies=proxies,
                timeout=timeout,
                target_url=target_url or base_url,
            )
            if ok:
                logger.info("观影(GYing)PoW 验证提交成功")
            else:
                logger.warn("观影(GYing)PoW 验证提交失败")
            return ok

        challenge_hashes: List[str] = list(pow_data.get("challenge") or [])
        diff: int = int(pow_data.get("diff") or 0)
        salt: str = str(pow_data.get("salt") or "")
        challenge_id: str = str(pow_data.get("id") or "")

        if not challenge_hashes or not diff or not salt or not challenge_id:
            return False

        logger.info(f"观影(GYing)检测到人机验证（PoW），难度={diff}，正在计算解答...")
        logger.debug(f"观影(GYing)PoW 挑战参数：id={challenge_id}，salt={salt}，challenges={challenge_hashes}")
        nonces = self._solve_pow(challenge_hashes, diff, salt)
        if not nonces:
            logger.warn("观影(GYing)PoW 求解失败：在指定范围内未找到匹配 nonce")
            return False

        logger.info(f"观影(GYing)PoW 计算完成，nonces={nonces}，正在提交...")
        ok = self._submit_pow_solution(
            session=session,
            base_url=base_url,
            challenge_id=challenge_id,
            nonces=nonces,
            ua=ua,
            proxies=proxies,
            timeout=timeout,
            target_url=target_url or base_url,
        )
        if ok:
            logger.info("观影(GYing)PoW 验证提交成功")
        else:
            logger.warn("观影(GYing)PoW 验证提交失败")
        return ok

    def _is_search_response_ready(self, base_url: str, keyword: str, ua: str,
                                  proxies: Optional[Dict[str, str]], timeout: int,
                                  cookie: str) -> Tuple[bool, str, str, str]:
        """
        检查搜索页是否就绪，返回 (ready, effective_cookie, search_url, response_body)。
        若遇到 PoW，在同一 session 内自动求解后再验证，并将新 cookie 一并返回。
        """
        try:
            url = self._build_search_url(base_url=base_url, keyword=keyword or "测试", mode="precise")
            with requests.Session() as session:
                session.proxies.update(proxies or {})
                session.headers.update({"User-Agent": ua or settings.USER_AGENT, "Referer": base_url})
                if cookie:
                    self._load_cookie_header_to_session(session=session, cookie=cookie)

                resp = session.get(url=url, timeout=max(5, int(timeout or 20)))
                if not resp.ok:
                    return False, cookie, url, ""

                body = str(resp.text or "")
                if self._is_login_shell(body):
                    return False, cookie, url, ""

                # 遇到 PoW：在同一 session 内解题，然后重试搜索 URL
                if self._is_pow_page(body):
                    ok = self._handle_pow_in_session(
                        session=session, base_url=base_url,
                        html_text=body, ua=ua, proxies=proxies, timeout=timeout,
                        target_url=resp.url or url,
                    )
                    if not ok:
                        return False, cookie, url, ""
                    # PoW 解决后重试
                    try:
                        resp2 = session.get(url=url, timeout=max(5, int(timeout or 20)))
                        body = str(resp2.text or "") if resp2.ok else ""
                    except Exception:
                        return False, cookie, url, ""
                    # 合并原有 cookie + 新 cookie（browser_verified 等），避免丢失登录 session
                    new_cookie = self._merge_cookie_str(cookie, self._cookie_jar_to_header(session.cookies))
                    if "_obj.search" in body:
                        return True, new_cookie or cookie, url, body
                    return False, new_cookie or cookie, url, body

                return "_obj.search" in body, cookie, url, body
        except Exception:
            return False, cookie, "", ""

    def _login_and_get_cookie(self, base_url: str, username: str, password: str, ua: str,
                              proxies: Optional[Dict[str, str]], timeout: int,
                              existing_cookie: str = "") -> str:
        root_candidates = self._build_login_roots(base_url=base_url)
        login_cookie_candidates: List[str] = []
        seen_cookie_candidates: Set[str] = set()
        for candidate in (existing_cookie, ""):
            normalized = self._normalize_cookie_header(candidate)
            if normalized in seen_cookie_candidates:
                continue
            seen_cookie_candidates.add(normalized)
            login_cookie_candidates.append(normalized)

        for candidate_index, login_cookie in enumerate(login_cookie_candidates):
            candidate_label = "现有cookie" if login_cookie else "全新session"
            if candidate_index > 0:
                logger.info(f"观影(GYing)自动登录回退到{candidate_label}模式重试。")

            for root in root_candidates:
                try:
                    with requests.Session() as session:
                        session.proxies.update(proxies or {})
                        session.headers.update({
                            "User-Agent": ua or settings.USER_AGENT,
                            "Referer": root,
                        })
                        if login_cookie:
                            self._load_cookie_header_to_session(session=session, cookie=login_cookie)
                        current_root = self._normalize_base_url(root) or root
                        login_url = urljoin(current_root, "/user/login")
                        payload = {
                            "username": username,
                            "password": password,
                            "cookietime": "10506240",
                            "siteid": "1",
                            "dosubmit": "1",
                            "code": "",
                        }
                        ajax_headers = {
                            "X-Requested-With": "XMLHttpRequest",
                            "Accept": "application/json, text/javascript, */*; q=0.01",
                            "Origin": f"{urlparse(current_root).scheme}://{urlparse(current_root).netloc}",
                            "Referer": login_url,
                        }

                        if login_cookie:
                            resp = session.post(
                                login_url,
                                data=payload,
                                headers=ajax_headers,
                                timeout=max(5, int(timeout or 20)),
                            )
                            if resp.ok:
                                ok = False
                                try:
                                    obj = resp.json()
                                    ok = int(obj.get("code") or 0) == 200
                                except Exception:
                                    text = str(resp.text or "")
                                    ok = ("登录成功" in text) or ("\"code\":200" in text) or ("{'code':200}" in text)
                                if ok:
                                    cookie_text = self._merge_cookie_str(
                                        login_cookie,
                                        self._cookie_jar_to_header(session.cookies),
                                    )
                                    if cookie_text:
                                        return cookie_text

                        resp = session.get(root, timeout=max(5, int(timeout or 20)))
                        current_root = self._normalize_base_url(resp.url or root) or root

                        # 处理 PoW 人机验证
                        if resp.ok and self._is_pow_page(resp.text):
                            pow_ok = self._handle_pow_in_session(
                                session=session,
                                base_url=current_root,
                                html_text=resp.text,
                                ua=ua or settings.USER_AGENT,
                                proxies=proxies,
                                timeout=timeout,
                                target_url=resp.url or current_root,
                            )
                            if not pow_ok:
                                continue

                        login_url = urljoin(current_root, "/user/login")
                        ajax_headers = {
                            "X-Requested-With": "XMLHttpRequest",
                            "Accept": "application/json, text/javascript, */*; q=0.01",
                            "Origin": f"{urlparse(current_root).scheme}://{urlparse(current_root).netloc}",
                            "Referer": login_url,
                        }
                        resp = session.post(
                            login_url,
                            data=payload,
                            headers=ajax_headers,
                            timeout=max(5, int(timeout or 20)),
                        )
                        if not resp.ok:
                            continue
                        ok = False
                        try:
                            obj = resp.json()
                            ok = int(obj.get("code") or 0) == 200
                        except Exception:
                            text = str(resp.text or "")
                            ok = ("登录成功" in text) or ("\"code\":200" in text) or ("{'code':200}" in text)
                        if not ok:
                            continue
                        cookie_text = self._cookie_jar_to_header(session.cookies)
                        if login_cookie:
                            cookie_text = self._merge_cookie_str(login_cookie, cookie_text)
                        if cookie_text:
                            return cookie_text
                except Exception as err:
                    logger.warn(f"观影(GYing)自动登录异常：{err}")
                    continue
        return ""

    @staticmethod
    def _build_login_roots(base_url: str) -> List[str]:
        parsed = urlparse(str(base_url or "").strip())
        scheme = parsed.scheme or "https"
        host = parsed.netloc or ""
        host = host.strip()
        if not host:
            return ["https://www.xn--kivn76b41nnhi.com/"]
        pure = host[4:] if host.startswith("www.") else host
        candidates: List[str] = []
        if pure and not host.startswith("www."):
            candidates.append(f"{scheme}://www.{pure}/")
        candidates.append(f"{scheme}://{host}/")
        ret: List[str] = []
        seen: Set[str] = set()
        for item in candidates:
            text = str(item or "").strip()
            if not text:
                continue
            if text in seen:
                continue
            seen.add(text)
            ret.append(text)
        return ret

    def _make_cached_get(self, client: RequestUtils,
                         url_cache: Dict[str, str],
                         request_state: Dict[str, Any],
                         base_url: str = "",
                         ua: str = "",
                         proxies: Optional[Dict[str, str]] = None,
                         timeout: int = 20,
                         cookie: str = "",
                         site: Optional[dict] = None) -> Callable[[str], str]:
        """
        返回带缓存的 GET 函数，并内置 PoW 人机验证处理：
        当任意请求返回 PoW 验证页时，自动求解并用新 cookie 重试，保证搜索全程不被拦截。
        """
        pow_resolved_cookie: List[str] = [""]  # 已解决的 PoW cookie（闭包共享）
        cache_lock = threading.RLock()
        pow_lock = threading.Lock()
        cache_miss = object()
        inflight_events: Dict[str, threading.Event] = {}

        def _record_http_timing(phase: str, target: str, started_at: float,
                                status: str = "", extra: str = "") -> None:
            elapsed_ms = (perf_counter() - started_at) * 1000
            with cache_lock:
                request_state["http_time_ms"] = float(request_state.get("http_time_ms") or 0.0) + elapsed_ms
            suffix = f"，{extra}" if extra else ""
            logger.debug(
                f"观影(GYing)HTTP耗时：phase={phase}，url={target}，status={status or '-'}，"
                f"耗时={elapsed_ms:.1f}ms{suffix}"
            )

        def _getter(url: str) -> str:
            target = str(url or "").strip()
            if not target:
                return ""
            with cache_lock:
                cached = url_cache.get(target, cache_miss)
                if cached is not cache_miss:
                    request_state["cache_hit"] = int(request_state.get("cache_hit") or 0) + 1
                    logger.debug(f"观影(GYing)请求缓存命中：url={target}")
                    return str(cached or "")
                waiter = inflight_events.get(target)
                if waiter is None:
                    waiter = threading.Event()
                    inflight_events[target] = waiter
                    is_leader = True
                    request_state["http"] = int(request_state.get("http") or 0) + 1
                    resolved_cookie = str(pow_resolved_cookie[0] or "").strip()
                else:
                    is_leader = False
                    resolved_cookie = ""

            if not is_leader:
                wait_started_at = perf_counter()
                waiter.wait()
                wait_elapsed_ms = (perf_counter() - wait_started_at) * 1000
                with cache_lock:
                    cached = url_cache.get(target, "")
                    request_state["cache_hit"] = int(request_state.get("cache_hit") or 0) + 1
                logger.debug(f"观影(GYing)请求单飞复用：url={target}，等待耗时={wait_elapsed_ms:.1f}ms")
                return str(cached or "")

            # 已解过 PoW：直接用新 cookie 发请求，跳过旧 cookie 的 client
            try:
                if resolved_cookie:
                    req_started_at = perf_counter()
                    try:
                        resp = requests.get(
                            target,
                            headers={
                                "User-Agent": ua or settings.USER_AGENT,
                                "Referer": base_url,
                                "Cookie": resolved_cookie,
                            },
                            proxies=proxies,
                            timeout=max(5, int(timeout or 20)),
                        )
                        text = resp.text if resp.ok else ""
                        _record_http_timing(
                            phase="resolved-cookie",
                            target=target,
                            started_at=req_started_at,
                            status=str(resp.status_code),
                            extra=f"body={'有' if text else '空'}"
                        )
                    except Exception as err:
                        logger.warn(f"观影(GYing)请求异常：{err}")
                        _record_http_timing(
                            phase="resolved-cookie",
                            target=target,
                            started_at=req_started_at,
                            status="EXC",
                            extra=str(err)
                        )
                        text = ""
                else:
                    req_started_at = perf_counter()
                    text = client.get(target) or ""
                    _record_http_timing(
                        phase="client",
                        target=target,
                        started_at=req_started_at,
                        status="OK",
                        extra=f"body={'有' if text else '空'}"
                    )

                # 若响应为 PoW 验证页，自动求解并重试
                if self._is_pow_page(text) and base_url:
                    logger.info(f"观影(GYing)搜索中途遇到 PoW 验证（URL={target}），正在自动求解...")
                    with pow_lock:
                        with cache_lock:
                            latest_cookie = str(pow_resolved_cookie[0] or "").strip()
                        if latest_cookie:
                            retry_started_at = perf_counter()
                            try:
                                resp = requests.get(
                                    target,
                                    headers={
                                        "User-Agent": ua or settings.USER_AGENT,
                                        "Referer": base_url,
                                        "Cookie": latest_cookie,
                                    },
                                    proxies=proxies,
                                    timeout=max(5, int(timeout or 20)),
                                )
                                text = resp.text if resp.ok else ""
                                _record_http_timing(
                                    phase="pow-reuse-retry",
                                    target=target,
                                    started_at=retry_started_at,
                                    status=str(resp.status_code),
                                    extra=f"body={'有' if text else '空'}"
                                )
                            except Exception as err:
                                logger.warn(f"观影(GYing)PoW 重试请求异常：{err}")
                                _record_http_timing(
                                    phase="pow-reuse-retry",
                                    target=target,
                                    started_at=retry_started_at,
                                    status="EXC",
                                    extra=str(err)
                                )
                                text = ""
                        else:
                            existing = str(cookie or "").strip()
                            pow_extra = self._solve_pow_from_html(
                                html_text=text,
                                target_url=target,
                                base_url=base_url,
                                ua=ua, proxies=proxies, timeout=timeout,
                                existing_cookie=existing,
                            )
                            if not pow_extra:
                                logger.warn(f"观影(GYing)搜索中途 PoW 求解失败，跳过 URL={target}")
                                text = ""
                            else:
                                merged = self._merge_cookie_str(existing, pow_extra)
                                with cache_lock:
                                    pow_resolved_cookie[0] = merged
                                logger.info("观影(GYing)搜索中途 PoW 求解成功，正在重试请求...")
                                # 回写 cookie，后续搜索直接使用新 cookie
                                if site is not None:
                                    site["cookie"] = merged
                                    self._remember_runtime_site_cookie(site=site, base_url=base_url, cookie=merged)
                                    self._persist_site_cookie(site=site, cookie=merged)

                                # 用新 cookie 重试当前 URL
                                retry_started_at = perf_counter()
                                try:
                                    resp = requests.get(
                                        target,
                                        headers={
                                            "User-Agent": ua or settings.USER_AGENT,
                                            "Referer": base_url,
                                            "Cookie": merged,
                                        },
                                        proxies=proxies,
                                        timeout=max(5, int(timeout or 20)),
                                    )
                                    text = resp.text if resp.ok else ""
                                    _record_http_timing(
                                        phase="pow-retry",
                                        target=target,
                                        started_at=retry_started_at,
                                        status=str(resp.status_code),
                                        extra=f"body={'有' if text else '空'}"
                                    )
                                except Exception as err:
                                    logger.warn(f"观影(GYing)PoW 重试请求异常：{err}")
                                    _record_http_timing(
                                        phase="pow-retry",
                                        target=target,
                                        started_at=retry_started_at,
                                        status="EXC",
                                        extra=str(err)
                                    )
                                    text = ""

                with cache_lock:
                    url_cache[target] = text or ""
                return text or ""
            finally:
                with cache_lock:
                    current = inflight_events.pop(target, None)
                if current is not None:
                    current.set()

        return _getter

    def _solve_pow_from_html(self, html_text: str, target_url: str, base_url: str,
                             ua: str, proxies: Optional[Dict[str, str]],
                             timeout: int, existing_cookie: str = "") -> str:
        """
        为 target_url 创建新 session，在同一 session 内 GET 目标页面获取服务端绑定的挑战，
        再求解并提交——确保 challenge_id 与 session 匹配。
        提交后若 session 无 cookie，用同一 session 再重试 target_url，
        看服务端是否通过 session 状态（而非 cookie）来授权。
        """
        try:
            with requests.Session() as session:
                session.proxies.update(proxies or {})
                session.headers.update({
                    "User-Agent": ua or settings.USER_AGENT,
                    "Referer": base_url,
                })
                if existing_cookie:
                    self._load_cookie_header_to_session(session=session, cookie=existing_cookie)

                # 用同一 session 请求 target_url，让服务端在该 session 里建立挑战绑定
                resp = session.get(target_url, timeout=max(5, int(timeout or 20)))
                if not resp.ok:
                    return ""

                fresh_html = resp.text
                final_target_url = str(resp.url or target_url or "").strip() or target_url
                if not self._is_pow_page(fresh_html):
                    # existing_cookie 已有效，无需 PoW
                    return self._cookie_jar_to_header(session.cookies)

                # 用同一 session 内的 HTML 求解（challenge_id 与 session 绑定）
                ok = self._handle_pow_in_session(
                    session=session,
                    base_url=base_url,
                    html_text=fresh_html,
                    ua=ua, proxies=proxies, timeout=timeout,
                    target_url=final_target_url,
                )
                if not ok:
                    logger.warn("观影(GYing)PoW 验证提交失败")
                    return ""

                cookie_text = self._cookie_jar_to_header(session.cookies)
                if cookie_text:
                    logger.info("观影(GYing)PoW 验证完成，已获取 session cookie")
                    return cookie_text

                # 提交成功但 session 无 cookie：用同一 session 再请求一次 target_url
                # 服务端可能通过 session 状态（而非 Set-Cookie）授权后续请求
                logger.info("观影(GYing)PoW 提交后 session 无 cookie，尝试用同一 session 重试...")
                try:
                    resp2 = session.get(final_target_url, timeout=max(5, int(timeout or 20)))
                    if resp2.ok and not self._is_pow_page(resp2.text):
                        # session 已通过验证，把 session cookies 返回（可能在这次请求才设置）
                        cookie_text = self._cookie_jar_to_header(session.cookies)
                        logger.info(f"观影(GYing)PoW session 重试成功，cookie={cookie_text[:60] or '(空)'}")
                        return cookie_text or "pow_verified=1"  # 兜底标志，防止空字符串被误判为失败
                except Exception:
                    pass

                logger.warn("观影(GYing)PoW 验证后 session 仍无 cookie，重试可能无效")
                return ""
        except Exception as err:
            logger.warn(f"观影(GYing)PoW 求解异常：{err}")
            return ""

    def _match_target_site(self, site: dict) -> bool:
        site_id = str(site.get("id") or "").strip().lower()
        if site_id == "gying" or any(token in site_id for token in ("gying", "观影", "xn--kivn76b41nnhi.com")):
            return True
        site_name = str(site.get("name") or site.get("title") or "").strip().lower()
        if site_name and ("gying" in site_name or "观影" in site_name):
            return True

        host_candidates = [
            site.get("domain"),
            site.get("url"),
            site.get("host"),
            site.get("base_url"),
        ]
        ext_domains = site.get("ext_domains") or []
        if isinstance(ext_domains, list):
            host_candidates.extend(ext_domains)
        hosts = self._all_hosts()
        for candidate in host_candidates:
            host = self._extract_host(candidate)
            if host and self._is_host_match(host, hosts):
                return True
        return False

    def _resolve_base_url(self, site: dict, ua: str = "",
                          proxies: Optional[Dict[str, str]] = None,
                          timeout: int = 20) -> str:
        candidates = self._build_base_url_candidates(site=site)
        if not candidates:
            candidates = ["https://www.xn--kivn76b41nnhi.com/"]
        pinned_hosts = set(self._ordered_extra_hosts())

        for candidate in candidates:
            resolved = self._refresh_base_url_if_needed(
                base_url=candidate,
                ua=ua,
                proxies=proxies,
                timeout=timeout,
                pinned_primary=self._extract_host(candidate) in pinned_hosts,
            )
            if resolved:
                return resolved

        return "https://www.xn--kivn76b41nnhi.com/"

    @staticmethod
    def _build_search_url(base_url: str, keyword: str,
                          mode: str = "precise", page_no: int = 1,
                          quality_code: Optional[str] = None) -> str:
        mode_value = "1" if mode == "fuzzy" else "3"
        page_value = max(1, int(page_no or 1))
        query = f"search?q={quote(keyword)}&type=&mode={mode_value}"
        if page_value > 1:
            query += f"&page={page_value}"
        return urljoin(base_url, query)

    def _collect_search_entries(self, client: RequestUtils, base_url: str, keyword: str,
                                fetcher: Optional[Callable[[str], str]] = None) -> List[Dict[str, Any]]:
        entry_map: Dict[str, Dict[str, Any]] = {}
        keyword_plan = self._expand_search_keywords(client=client, base_url=base_url, keyword=keyword)
        getter = fetcher or client.get

        for query_keyword in keyword_plan:
            precise_url = self._build_search_url(
                base_url=base_url,
                keyword=query_keyword,
                mode="precise"
            )
            precise_html = getter(precise_url)
            precise_data = self._extract_js_object(precise_html, "_obj.search") if precise_html else None
            precise_entries = self._extract_entries_from_search(
                search_data=precise_data if isinstance(precise_data, dict) else {},
                forced_quality=None
            )

            for item in precise_entries:
                key = str(item.get("id") or "").strip()
                if key and key not in entry_map:
                    row = dict(item)
                    row["__skip_keyword_match"] = False
                    entry_map[key] = row

            if precise_entries:
                continue

            fuzzy_url = self._build_search_url(
                base_url=base_url,
                keyword=query_keyword,
                mode="fuzzy"
            )
            fuzzy_html = getter(fuzzy_url)
            fuzzy_data = self._extract_js_object(fuzzy_html, "_obj.search") if fuzzy_html else None
            fuzzy_entries = self._extract_entries_from_search(
                search_data=fuzzy_data if isinstance(fuzzy_data, dict) else {},
                forced_quality=None
            )
            for item in fuzzy_entries:
                key = str(item.get("id") or "").strip()
                if key and key not in entry_map:
                    row = dict(item)
                    row_dir = str(row.get("dir") or "").strip().lower()
                    row["__skip_keyword_match"] = row_dir in ("tv", "ac", "mv")
                    entry_map[key] = row
        return list(entry_map.values())

    def _expand_search_keywords(self, client: RequestUtils, base_url: str, keyword: str) -> List[str]:
        primary = str(keyword or "").strip()
        if not primary:
            return []
        # 极速模式：仅使用 MoviePilot 传入的关键词，不再请求站内联想词接口。
        return [primary]

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
        years = self._as_list(list_obj.get("year"))

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
                "quality": row_quality,
                "year": str(self._safe_at(years, idx) or "").strip()
            })
        return entries

    def _all_hosts(self) -> Set[str]:
        hosts = set(self._default_hosts)
        for host in self._ordered_extra_hosts():
            hosts.add(host)
        return hosts

    def _registered_hosts(self) -> List[str]:
        ordered_extra_hosts = self._ordered_extra_hosts()
        if ordered_extra_hosts:
            # extra_hosts 只控制实际访问优先级；索引器注册仍保留内置旧域名，
            # 确保站点管理暂未改到新域名时，搜索入口也能被插件接管。
            return self._expand_registered_hosts(ordered_extra_hosts + sorted(self._default_hosts))
        return self._expand_registered_hosts(sorted(self._default_hosts))

    @staticmethod
    def _expand_registered_hosts(hosts: List[str]) -> List[str]:
        expanded: List[str] = []
        seen: Set[str] = set()
        for host in hosts:
            pure_host = GyingIndexer._extract_host(host)
            if not pure_host:
                continue
            aliases = [pure_host]
            if pure_host == "xn--kivn76b41nnhi.com":
                aliases.append(f"www.{pure_host}")
            for alias in aliases:
                if alias in seen:
                    continue
                seen.add(alias)
                expanded.append(alias)
        return expanded

    def _ordered_extra_hosts(self) -> List[str]:
        ret: List[str] = []
        seen: Set[str] = set()
        for line in self._extra_hosts.splitlines():
            host = self._extract_host(line)
            if not host or host in seen:
                continue
            seen.add(host)
            ret.append(host)
        return ret

    def _build_base_url_candidates(self, site: dict) -> List[str]:
        candidates: List[str] = []
        seen: Set[str] = set()

        ordered_extra_hosts = self._ordered_extra_hosts()
        for host in ordered_extra_hosts:
            base_url = self._preferred_site_base_url(host)
            if base_url and base_url not in seen:
                seen.add(base_url)
                candidates.append(base_url)

        if ordered_extra_hosts:
            return candidates

        for raw in (
            site.get("url") if isinstance(site, dict) else "",
            site.get("domain") if isinstance(site, dict) else "",
            "https://www.xn--kivn76b41nnhi.com/",
        ):
            host = self._extract_host(raw)
            base_url = self._preferred_site_base_url(host) if host else self._normalize_base_url(raw)
            if base_url and base_url not in seen:
                seen.add(base_url)
                candidates.append(base_url)

        return candidates

    def _register_builtin_indexer(self) -> None:
        ordered_extra_hosts = self._ordered_extra_hosts()
        hosts = self._registered_hosts()
        if not hosts:
            return

        primary = ordered_extra_hosts[0] if ordered_extra_hosts else (
            "xn--kivn76b41nnhi.com" if "xn--kivn76b41nnhi.com" in hosts else hosts[0]
        )
        indexer = self._build_indexer_schema(primary_host=primary, all_hosts=hosts)

        for host in hosts:
            try:
                SitesHelper().add_indexer(domain=host, indexer=indexer)
            except Exception as err:
                logger.warn(f"观影(GYing)索引器注册失败：域名={host}，错误={err}")
        logger.info(f"观影(GYing)索引器注册完成：域名列表={', '.join(hosts)}")

    @staticmethod
    def _build_indexer_schema(primary_host: str, all_hosts: List[str]) -> Dict[str, Any]:
        ext_domains = [GyingIndexer._preferred_site_base_url(host) for host in all_hosts if host != primary_host]
        return {
            "id": "gying",
            "name": "GYing",
            "domain": GyingIndexer._preferred_site_base_url(primary_host),
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
    def _preferred_site_base_url(host: Any) -> str:
        pure_host = GyingIndexer._extract_host(host)
        if not pure_host:
            return ""
        if pure_host == "xn--kivn76b41nnhi.com":
            return f"https://www.{pure_host}/"
        return f"https://{pure_host}/"

    @staticmethod
    def _normalize_base_url(raw: Any) -> str:
        text = str(raw or "").strip()
        if not text:
            return ""
        if "://" not in text:
            text = f"https://{text}"
        try:
            parsed = urlparse(text)
        except Exception:
            return ""
        scheme = parsed.scheme or "https"
        host = GyingIndexer._to_ascii_host(parsed.hostname or "")
        if not host:
            return ""
        netloc = host
        if parsed.port:
            netloc = f"{host}:{parsed.port}"
        return f"{scheme}://{netloc}/"

    @staticmethod
    def _to_ascii_host(raw: Any) -> str:
        text = str(raw or "").strip().lower()
        if not text:
            return ""
        try:
            return text.encode("idna").decode("ascii").lower()
        except Exception:
            return text

    def _fetch_latest_hosts(self, base_url: str, ua: str,
                            proxies: Optional[Dict[str, str]],
                            timeout: int) -> List[str]:
        discover_url = urljoin(base_url, "urlop/")
        try:
            resp = requests.get(
                discover_url,
                headers={
                    "User-Agent": ua or settings.USER_AGENT,
                    "Referer": base_url,
                },
                proxies=proxies,
                timeout=max(5, int(timeout or 20)),
            )
            if not resp.ok:
                return []
            data = resp.json()
        except Exception as err:
            logger.warn(f"观影(GYing)拉取最新地址失败：{err}")
            return []

        hosts: List[str] = []
        seen: Set[str] = set()
        for item in data.get("host") or []:
            host = self._to_ascii_host(item)
            if not host:
                continue
            if host.startswith("www."):
                host = host[4:]
            if host in seen:
                continue
            seen.add(host)
            hosts.append(host)
        return hosts

    def _probe_base_url(self, target: str, ua: str,
                        proxies: Optional[Dict[str, str]],
                        timeout: int) -> Tuple[bool, bool, str]:
        try:
            resp = requests.get(
                target,
                headers={
                    "User-Agent": ua or settings.USER_AGENT,
                    "Referer": target,
                },
                proxies=proxies,
                timeout=max(5, int(timeout or 20)),
            )
            if not resp.ok:
                return False, False, ""
            normalized = self._normalize_base_url(resp.url or target) or self._normalize_base_url(target)
            return True, self._is_retired_host_page(resp.text), normalized
        except Exception as err:
            logger.warn(f"观影(GYing)探测站点域名状态失败：host={target}，err={err}")
            return False, False, ""

    def _refresh_base_url_if_needed(self, base_url: str, ua: str,
                                    proxies: Optional[Dict[str, str]],
                                    timeout: int,
                                    pinned_primary: bool = False) -> str:
        runtime_key = self._site_runtime_key(site=None, base_url=base_url)
        base_target = str(base_url or "").strip()
        cached_target = str(self._runtime_site_base_urls.get(runtime_key) or "").strip()
        target = base_target if pinned_primary and base_target else (cached_target or base_target)
        if not target:
            return ""

        target_ok, target_retired, resolved_target = self._probe_base_url(
            target=target,
            ua=ua,
            proxies=proxies,
            timeout=timeout,
        )
        if target_ok and not target_retired:
            final_target = resolved_target or target
            self._runtime_site_base_urls[runtime_key] = final_target
            return final_target
        if pinned_primary:
            return ""

        target_host = self._extract_host(target)
        if target_host in self._default_hosts:
            latest_hosts = self._fetch_latest_hosts(
                base_url=target,
                ua=ua,
                proxies=proxies,
                timeout=timeout,
            )
            switched = self._pick_available_latest_base_url(
                current_target=target,
                latest_hosts=latest_hosts,
                ua=ua,
                proxies=proxies,
                timeout=timeout,
            )
            if switched:
                self._runtime_site_base_urls[runtime_key] = switched
                return switched
            return ""

        latest_hosts = self._fetch_latest_hosts(
            base_url=target,
            ua=ua,
            proxies=proxies,
            timeout=timeout,
        )
        switched = self._pick_available_latest_base_url(
            current_target=target,
            latest_hosts=latest_hosts,
            ua=ua,
            proxies=proxies,
            timeout=timeout,
        )
        if switched:
            self._runtime_site_base_urls[runtime_key] = switched
            return switched
        return ""

    def _pick_available_latest_base_url(self, current_target: str, latest_hosts: List[str],
                                        ua: str, proxies: Optional[Dict[str, str]],
                                        timeout: int) -> str:
        if not latest_hosts:
            return current_target

        scheme = urlparse(current_target).scheme or "https"
        for host in latest_hosts:
            candidate = f"{scheme}://www.{host}/"
            try:
                resp = requests.get(
                    candidate,
                    headers={
                        "User-Agent": ua or settings.USER_AGENT,
                        "Referer": candidate,
                    },
                    proxies=proxies,
                    timeout=max(5, int(timeout or 20)),
                )
                if resp.ok and not self._is_retired_host_page(resp.text):
                    return candidate
            except Exception as err:
                logger.debug(f"观影(GYing)探测最新地址失败：host={host}，err={err}")
                continue
        return current_target

    def _site_runtime_key(self, site: Optional[dict], base_url: str = "") -> str:
        ordered_extra_hosts = self._ordered_extra_hosts()
        if ordered_extra_hosts:
            primary_host = self._extract_host(ordered_extra_hosts[0])
            if primary_host:
                return f"host:{primary_host}"
        if isinstance(site, dict):
            site_id = str(site.get("id") or "").strip().lower()
            if site_id:
                return f"id:{site_id}"
            host = GyingIndexer._extract_host(site.get("url") or site.get("domain"))
            if host:
                return f"host:{host}"
        host = GyingIndexer._extract_host(base_url)
        if host:
            return f"host:{host}"
        return "host:xn--kivn76b41nnhi.com"

    def _get_runtime_site_cookie(self, site: dict, base_url: str) -> str:
        runtime_key = self._site_runtime_key(site=site, base_url=base_url)
        runtime_cookie = self._normalize_cookie_header(self._runtime_site_cookies.get(runtime_key) or "")
        site_cookie = self._normalize_cookie_header(site.get("cookie") or "")
        if runtime_cookie:
            return runtime_cookie
        return site_cookie

    def _remember_runtime_site_cookie(self, site: dict, base_url: str, cookie: str) -> None:
        runtime_key = self._site_runtime_key(site=site, base_url=base_url)
        cookie_text = self._normalize_cookie_header(cookie)
        if cookie_text:
            self._runtime_site_cookies[runtime_key] = cookie_text

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
        host = GyingIndexer._to_ascii_host(host)
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

    @staticmethod
    def _normalize_match_text(text: Any) -> str:
        return re.sub(r"[^0-9a-z\u4e00-\u9fff]+", "", str(text or "").lower())

    @staticmethod
    def _keyword_tokens(keyword: str) -> List[str]:
        text = str(keyword or "").strip().lower()
        if not text:
            return []
        raw_parts = re.split(r"[\s\-\._\[\]\(\)\{\}/\\|:]+", text)
        tokens: List[str] = []
        seen: Set[str] = set()
        for raw in raw_parts:
            token = GyingIndexer._normalize_match_text(raw)
            if not token:
                continue
            if token.isdigit():
                # 年份等纯数字不作为强制命中条件，避免误杀结果
                continue
            if len(token) < 2:
                continue
            if token in seen:
                continue
            seen.add(token)
            tokens.append(token)
        return tokens

    @staticmethod
    def _is_keyword_related(keyword: str, *texts: Any) -> bool:
        keyword_norm = GyingIndexer._normalize_match_text(keyword)
        if not keyword_norm:
            return True
        merged_norm = GyingIndexer._normalize_match_text(" ".join([str(x or "") for x in texts]))
        if not merged_norm:
            return False
        if keyword_norm in merged_norm:
            return True
        tokens = GyingIndexer._keyword_tokens(keyword)
        if not tokens:
            return False
        return all(token in merged_norm for token in tokens)

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
        is_original = False
        is_4k = False
        is_1080 = False
        has_zh_sub = False

        # 有站点标签时，严格按站点标签分类（1080/中字1080/4K/中字4K/原盘）。
        if label_norm:
            is_original = "原盘" in label_norm
            is_4k = ("4k" in label_norm) or ("2160" in label_norm)
            is_1080 = ("1080" in label_norm) and not is_4k
            has_zh_sub = ("中字" in label_norm) or ("中文" in label_norm)
            if not (is_original or is_4k or is_1080):
                return False
        else:
            # 标签缺失时再回退标题判断，避免误判覆盖站点标签。
            is_original = self._match_original(title)
            is_4k = self._is_4k(title_norm)
            is_1080 = self._is_1080(title_norm)
            has_zh_sub = self._has_chinese_subtitle(quality_label=quality_label, title=title)
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

    def _fetch_parent_down_entries(self, client: RequestUtils, base_url: str,
                                   parent_dir: str, parent_id: str,
                                   fetcher: Optional[Callable[[str], str]] = None) -> List[Dict[str, Any]]:
        getter = fetcher or client.get
        url = urljoin(base_url, f"res/downurl/{parent_dir}/{parent_id}")
        text = getter(url)
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

    def _load_detail_data(self, client: RequestUtils, base_url: str,
                          resource_dir: str, resource_id: str,
                          detail_data: Optional[Dict[str, Any]] = None,
                          fetcher: Optional[Callable[[str], str]] = None) -> Dict[str, Any]:
        cached = detail_data or {}
        if cached:
            return cached
        getter = fetcher or client.get
        detail_url = urljoin(base_url, f"{resource_dir}/{resource_id}")
        detail_html = getter(detail_url)
        if not detail_html:
            return {}
        _detail_data = self._extract_js_object(detail_html, "_obj.d")
        if isinstance(_detail_data, dict):
            return _detail_data
        return {}

    def _resolve_enclosure(self, client: RequestUtils, base_url: str,
                           resource_dir: str, resource_id: str,
                           title: str, info_hash: str = "",
                           detail_data: Optional[Dict[str, Any]] = None,
                           fetcher: Optional[Callable[[str], str]] = None) -> Tuple[str, Dict[str, Any]]:
        payload = detail_data or {}
        enclosure = self._build_magnet_from_hash(info_hash=info_hash, title=title)
        if enclosure:
            return enclosure, payload

        payload = self._load_detail_data(
            client=client,
            base_url=base_url,
            resource_dir=resource_dir,
            resource_id=resource_id,
            detail_data=payload,
            fetcher=fetcher
        )
        download_candidates = self._extract_download_candidates_from_node(
            node=payload,
            base_url=base_url
        )
        if not download_candidates:
            download_candidates = self._fetch_download_candidates_from_downurl(
                client=client,
                base_url=base_url,
                resource_dir=resource_dir,
                resource_id=resource_id,
                fetcher=fetcher
            )
        enclosure = self._pick_preferred_enclosure(download_candidates)
        return enclosure, payload

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
                                                resource_dir: str, resource_id: str,
                                                fetcher: Optional[Callable[[str], str]] = None) -> List[str]:
        """
        回退接口：部分条目详情页不直接包含 magnet，需要从 downurl 接口读取。
        返回磁力与可下载链接候选（优先磁力，其次 torrent/媒体直链）。
        """
        getter = fetcher or client.get
        url = urljoin(base_url, f"res/downurl/{resource_dir}/{resource_id}")
        text = getter(url)
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
            logger.debug(f"观影(GYing)JSON解析失败：标记={marker}，错误={err}")
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
