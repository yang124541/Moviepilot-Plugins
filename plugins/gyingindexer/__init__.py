import json
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import quote, urljoin, urlparse

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
    plugin_name = "观影索引（GYing）"
    plugin_desc = "为 GYing 提供磁力搜索与清晰度过滤支持。"
    plugin_icon = "spider.png"
    plugin_version = "1.0.0"
    plugin_author = "yang124541"
    author_url = "https://github.com/jxxghp/MoviePilot-Plugins"
    plugin_config_prefix = "gyingindexer_"
    plugin_order = 30
    auth_level = 2

    _enabled = False
    _strict_quality = True
    _extra_hosts = ""

    _default_hosts: Set[str] = {
        "gying.si",
        "gying.org",
        "gying.net",
        "gyg.la",
        "gyg.si",
    }
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

    def init_plugin(self, config: dict = None):
        if config:
            self._enabled = bool(config.get("enabled"))
            self._strict_quality = bool(config.get("strict_quality", True))
            self._extra_hosts = (config.get("extra_hosts") or "").strip()
        if self._enabled:
            self._register_builtin_indexer()

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
                                "props": {"cols": 12, "md": 6},
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
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "strict_quality",
                                            "label": "仅保留中字1080P/4K",
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
            "strict_quality": True,
            "extra_hosts": "",
        }

    def get_page(self) -> List[dict]:
        return []

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

        search_url = self._build_search_url(base_url=base_url, keyword=keyword)
        logger.info(f"GYing search start: {search_url}")

        try:
            client = RequestUtils(
                ua=ua,
                cookies=cookie,
                proxies=proxies,
                timeout=timeout,
                referer=referer
            )
            html = client.get(search_url)
            if not html:
                logger.warn(f"GYing search empty: {search_url}")
                return []

            search_data = self._extract_js_object(html, "_obj.search")
            if not isinstance(search_data, dict):
                logger.warn("GYing search parse failed: _obj.search not found")
                return []

            list_obj = search_data.get("l") or {}
            ids = self._as_list(list_obj.get("i"))
            dirs = self._as_list(list_obj.get("d"))
            titles = self._as_list(list_obj.get("title"))
            sizes = self._as_list(list_obj.get("size"))
            seeds = self._as_list(list_obj.get("seeds"))
            times = self._as_list(list_obj.get("time"))
            tags = self._as_list(list_obj.get("k"))

            results: List[TorrentInfo] = []
            for idx, btid in enumerate(ids):
                if str(self._safe_at(dirs, idx) or "").lower() != "bt":
                    continue
                btid = str(btid or "").strip()
                if not btid:
                    continue

                title = str(self._safe_at(titles, idx) or "").strip()
                if not title:
                    continue
                if self._strict_quality and not self._match_quality(title):
                    continue

                detail_url = urljoin(base_url, f"bt/{btid}")
                detail_html = client.get(detail_url)
                if not detail_html:
                    continue
                detail_data = self._extract_js_object(detail_html, "_obj.d")
                if not isinstance(detail_data, dict):
                    continue
                magnet = str(detail_data.get("magnet") or "").strip()
                if not magnet.startswith("magnet:?"):
                    continue

                size_text = str(self._safe_at(sizes, idx) or "").strip()
                seeds_text = self._safe_at(seeds, idx)
                elapsed_text = str(self._safe_at(times, idx) or "").strip()
                tag_text = str(self._safe_at(tags, idx) or "").strip()
                detail_title = str(detail_data.get("title") or "").strip()

                results.append(TorrentInfo(
                    site=site.get("id"),
                    site_name=site.get("name"),
                    site_cookie=site.get("cookie"),
                    site_ua=site.get("ua"),
                    site_proxy=site.get("proxy"),
                    site_order=site.get("pri"),
                    site_downloader=site.get("downloader"),
                    title=title,
                    description=tag_text or detail_title,
                    enclosure=magnet,
                    page_url=detail_url,
                    size=StringUtils.num_filesize(size_text),
                    seeders=self._to_int(seeds_text),
                    peers=0,
                    grabs=0,
                    pubdate=None,
                    date_elapsed=elapsed_text,
                    downloadvolumefactor=0,
                    uploadvolumefactor=1,
                ))

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
    def _build_search_url(base_url: str, keyword: str) -> str:
        return urljoin(base_url, f"s/1-4--1/{quote(keyword)}")

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

    def _match_quality(self, title: str) -> bool:
        title_norm = re.sub(r"\s+", "", title).lower()
        has_subtitle = any(token in title_norm for token in self._subtitle_tokens)
        is_1080p = "1080p" in title_norm
        is_4k = "2160p" in title_norm or bool(re.search(r"(?<!\d)4k(?!\d)", title_norm))
        return has_subtitle and (is_1080p or is_4k)

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
