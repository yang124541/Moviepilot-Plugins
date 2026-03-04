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
    plugin_name = "观影（GYing）"
    plugin_desc = "为 GYing 提供磁力搜索与清晰度过滤支持。"
    plugin_icon = "spider.png"
    plugin_version = "1.0.11"
    plugin_author = "yang124541"
    author_url = "https://github.com/jxxghp/MoviePilot-Plugins"
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
    _target_quality_codes: Tuple[str, ...] = ("i5", "i9")
    _max_search_pages: int = 8
    _resolved_original_codes: Set[str] = set()
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
                                            "label": "包含原盘",
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

        logger.info(f"GYing search start: {keyword}")

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
            for entry in search_entries:
                btid = str(entry.get("id") or "").strip()
                if not btid:
                    continue

                title = str(entry.get("title") or "").strip()
                if not title:
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

                search_size_text = str(entry.get("size") or "").strip()
                detail_size_text = str(detail_data.get("s") or detail_data.get("size") or "").strip()
                size_bytes = self._parse_size_bytes(detail_size_text, search_size_text)
                seeds_text = entry.get("seeds")
                elapsed_text = str(entry.get("time") or "").strip()
                tag_text = str(entry.get("tag") or "").strip()
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
                    size=size_bytes,
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
    def _build_search_url(base_url: str, keyword: str,
                          page_no: int = 1, quality_code: Optional[str] = None) -> str:
        quality = quality_code or ""
        return urljoin(base_url, f"s/{page_no}-4-{quality}-1/{quote(keyword)}")

    def _collect_search_entries(self, client: RequestUtils, base_url: str, keyword: str) -> List[Dict[str, Any]]:
        entry_map: Dict[str, Dict[str, Any]] = {}
        discovered_original_codes: Set[str] = set()

        quality_plan: List[Optional[str]]
        quality_plan: List[Optional[str]] = []
        if self._enable_zh1080:
            quality_plan.append("i5")
        if self._enable_zh4k:
            quality_plan.append("i9")

        for quality_code in quality_plan:
            for page_no in range(1, self._max_search_pages + 1):
                search_url = self._build_search_url(
                    base_url=base_url,
                    keyword=keyword,
                    page_no=page_no,
                    quality_code=quality_code
                )
                html = client.get(search_url)
                if not html:
                    break
                search_data = self._extract_js_object(html, "_obj.search")
                if not isinstance(search_data, dict):
                    break
                discovered_original_codes.update(self._extract_original_codes(search_data))

                page_entries = self._extract_entries_from_search(
                    search_data=search_data,
                    forced_quality=quality_code
                )
                if not page_entries:
                    break

                new_count = 0
                for item in page_entries:
                    key = str(item.get("id") or "").strip()
                    if not key:
                        continue
                    if key not in entry_map:
                        entry_map[key] = item
                        new_count += 1

                if page_no > 1 and new_count == 0:
                    break

        # 严格模式下，补抓全量页：
        # 1) 分类页为空时兜底；
        # 2) 需要包含“原盘”时，合并全量页中的原盘资源。
        need_full_scan = (
            self._enable_1080 or
            self._enable_4k or
            self._include_original or
            not quality_plan
        )
        if need_full_scan:
            for page_no in range(1, self._max_search_pages + 1):
                search_url = self._build_search_url(
                    base_url=base_url,
                    keyword=keyword,
                    page_no=page_no,
                    quality_code=None
                )
                html = client.get(search_url)
                if not html:
                    break
                search_data = self._extract_js_object(html, "_obj.search")
                if not isinstance(search_data, dict):
                    break
                discovered_original_codes.update(self._extract_original_codes(search_data))
                page_entries = self._extract_entries_from_search(
                    search_data=search_data,
                    forced_quality=None
                )
                if not page_entries:
                    break
                for item in page_entries:
                    key = str(item.get("id") or "").strip()
                    if key and key not in entry_map:
                        entry_map[key] = item

        self._resolved_original_codes = {str(x).lower() for x in discovered_original_codes if x}
        if self._resolved_original_codes:
            logger.info(f"GYing detected original codes: {sorted(self._resolved_original_codes)}")
        logger.info(f"GYing entries collected: {len(entry_map)}")
        return list(entry_map.values())

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
            if str(self._safe_at(dirs, idx) or "").lower() != "bt":
                continue
            title = str(self._safe_at(titles, idx) or "").strip()
            if not title:
                continue

            row_quality = str(self._safe_at(qualities, idx) or forced_quality or "").strip().lower()
            if not self._should_keep_entry(title=title, quality_code=(row_quality or forced_quality)):
                continue

            entries.append({
                "id": btid,
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

    def _has_chinese_subtitle(self, title_norm: str, quality_code: str = "") -> bool:
        q = str(quality_code or "").strip().lower()
        if q in self._target_quality_codes:
            return True
        return any(token in title_norm for token in self._subtitle_tokens)

    def _should_keep_entry(self, title: str, quality_code: str = "") -> bool:
        title_norm = re.sub(r"\s+", "", str(title or "")).lower()
        if not title_norm:
            return False

        q = str(quality_code or "").strip().lower()
        is_original = (q in self._resolved_original_codes) or self._match_original(title)
        is_4k = (q == "i9") or self._is_4k(title_norm)
        is_1080 = (q == "i5") or self._is_1080(title_norm)
        if q == "i9":
            is_1080 = False
        elif q == "i5":
            is_4k = False
        has_zh_sub = self._has_chinese_subtitle(title_norm=title_norm, quality_code=quality_code)

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
        for raw in size_texts:
            text = str(raw or "").strip()
            if not text:
                continue

            has_unit = bool(re.search(r"[a-zA-Z]", text))
            if has_unit:
                size = StringUtils.num_filesize(text)
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
