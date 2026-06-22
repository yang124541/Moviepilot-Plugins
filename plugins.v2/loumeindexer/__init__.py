import hashlib
import random
import re
from collections import deque
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import datetime
from html import unescape
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse, quote

import requests as _requests
from fastapi.concurrency import run_in_threadpool

from app.core.config import settings
from app.core.context import TorrentInfo
from app.helper.sites import SitesHelper
from app.log import logger
from app.plugins import _PluginBase
from app.schemas.types import MediaType


class LoumeIndexer(_PluginBase):
    plugin_name = "BT之家"
    plugin_desc = "为 1lou.me 提供种子搜索支持。"
    plugin_icon = "https://raw.githubusercontent.com/yang124541/moviepilot-plugin/main/loume.png"
    plugin_version = "1.1.8"
    plugin_author = "yang124541"
    author_url = "https://github.com/yang124541/moviepilot-plugin"
    plugin_config_prefix = "loumeindexer_"
    plugin_order = 32
    auth_level = 2

    _enabled = False
    _extra_hosts = ""
    _detail_concurrency = 5

    _default_host = "1lou.me"
    _default_base_url = "https://www.1lou.me/"
    _allowed_forum_ids = {1, 2, 3, 4}

    # 搜索最大分页
    _max_search_pages: int = 5

    def init_plugin(self, config: dict = None):
        if config:
            self._enabled = bool(config.get("enabled"))
            self._extra_hosts = (config.get("extra_hosts") or "").strip()
            self._detail_concurrency = self._clamp_detail_concurrency(
                config.get("detail_concurrency")
            )

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
                                "props": {"cols": 12, "md": 3},
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
                                            "model": "detail_concurrency",
                                            "type": "number",
                                            "label": "资源页并发数",
                                            "min": 1,
                                            "max": 100,
                                            "placeholder": "5",
                                            "hint": "并发抓取帖子详情页与种子附件，允许范围 1-100",
                                            "persistentHint": True,
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
                                            "rows": 2,
                                            "label": "额外域名（每行一个）",
                                            "placeholder": "www.1lou.me",
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
                                        "component": "VAlert",
                                        "props": {
                                            "type": "info",
                                            "variant": "tonal",
                                            "text": "1lou.me 是一个 BT 资源论坛，提供影视剧集种子下载。"
                                                    "站点 URL 请在 MoviePilot 站点管理中配置为 https://www.1lou.me/。"
                                                    "若日志提示 Cloudflare 人机验证，请在站点管理中更新浏览器导出的完整 Cookie（至少包含 cf_clearance）后重试。",
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
            "extra_hosts": "",
            "detail_concurrency": 5,
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
        timeout = int(site.get("timeout") or 20)
        ua = site.get("ua") or settings.USER_AGENT
        proxies = settings.PROXY if site.get("proxy") else None
        logger.info(f"BT之家(1lou)开始搜索：关键词='{keyword}'")
        media_profile = self._resolve_moviepilot_title_candidates(
            keyword=keyword,
            mtype=mtype,
        )

        try:
            # 构建带 cookie 的 session
            session = _requests.Session()
            cookie_from_site = str(site.get("cookie") or "").strip()
            use_spoof_ip = not self._has_cloudflare_clearance(cookie_from_site)
            if cookie_from_site:
                session.headers.update({"Cookie": cookie_from_site})

            session.headers.update({
                "User-Agent": ua,
                "Referer": base_url,
                "Accept-Language": "zh-CN,zh;q=0.9",
            })

            thread_items: List[Dict[str, Any]] = []
            blocked_by_cf = False
            used_search_keyword = str(keyword or "").strip()
            search_keywords = self._build_search_keywords(
                keyword=keyword,
                media_profile=media_profile,
            )
            for candidate_keyword in search_keywords:
                search_client_ip = self._rand_ip() if use_spoof_ip else ""
                thread_items, blocked_by_cf = self._search_threads(
                    session=session,
                    base_url=base_url,
                    keyword=candidate_keyword,
                    timeout=timeout,
                    proxies=proxies,
                    client_ip=search_client_ip,
                )
                if blocked_by_cf or thread_items:
                    used_search_keyword = candidate_keyword
                    break

            if not thread_items:
                if blocked_by_cf:
                    return []
                logger.info(
                    f"BT之家(1lou)搜索无结果：关键词='{keyword}'，"
                    f"已尝试搜索词={search_keywords}"
                )
                return []

            filtered_thread_items = [
                item for item in thread_items
                if self._is_allowed_search_forum_id(item.get("forum_id"))
            ]
            if not filtered_thread_items:
                logger.info(
                    f"BT之家(1lou)搜索无结果：关键词='{keyword}'，"
                    f"搜索结果均不在允许分类(1/2/3/4)内"
                )
                return []

            attach_map = self._fetch_thread_attachments_concurrently(
                base_url=base_url,
                thread_items=filtered_thread_items,
                session_headers=dict(session.headers),
                session_cookies=session.cookies.get_dict(),
                timeout=timeout,
                proxies=proxies,
            )
            results: List[TorrentInfo] = []

            for item in filtered_thread_items:
                tid = str(item.get("tid") or "").strip()
                if not tid:
                    continue
                title = str(item.get("title") or "").strip()
                if not title:
                    continue

                thread_url = urljoin(base_url, f"thread-{tid}.htm")

                attach_items = attach_map.get(tid) or []
                if not attach_items:
                    # 没有附件，跳过
                    continue

                for attach in attach_items:
                    aid = str(attach.get("aid") or "").strip()
                    filename = str(attach.get("filename") or "").strip()
                    enclosure = str(attach.get("enclosure") or "").strip()
                    if not aid:
                        continue

                    # 种子下载 URL
                    download_url = urljoin(base_url, f"attach-download-{aid}.htm")
                    if not enclosure:
                        enclosure = download_url

                    # 用文件名或帖子标题作为 TorrentInfo 标题
                    torrent_title = filename if filename else title

                    description = title
                    if filename and filename != title:
                        description = f"{filename} | {title}"
                    # 解析文件大小：只看种子名和标题，避免正文里的其它数字单位被误判为体积
                    size_bytes = self._extract_size_bytes(
                        filename=filename,
                        title=title,
                    )

                    results.append(TorrentInfo(
                        site=site.get("id"),
                        site_name=site.get("name"),
                        site_cookie=site.get("cookie"),
                        site_ua=site.get("ua"),
                        site_proxy=site.get("proxy"),
                        site_order=site.get("pri"),
                        site_downloader=site.get("downloader"),
                        title=torrent_title,
                        description=description,
                        enclosure=enclosure,
                        page_url=thread_url,
                        size=size_bytes,
                        seeders=0,
                        peers=0,
                        grabs=0,
                        pubdate=None,
                        downloadvolumefactor=0,
                        uploadvolumefactor=1,
                    ))

            cost = (datetime.now() - start_at).seconds
            logger.info(
                f"BT之家(1lou)搜索完成：关键词='{keyword}'，"
                f"实际搜索词='{used_search_keyword}'，"
                f"找到帖子={len(thread_items)}，分类过滤后帖子={len(filtered_thread_items)}，返回种子={len(results)}，耗时={cost}s"
            )
            return results

        except Exception as err:
            logger.error(f"BT之家(1lou)搜索异常：关键词='{keyword}'，错误={err}")
            return []

    def _fetch_thread_attachments_concurrently(
            self,
            base_url: str,
            thread_items: List[Dict[str, Any]],
            session_headers: Dict[str, str],
            session_cookies: Dict[str, str],
            timeout: int,
            proxies: Optional[Dict[str, str]]) -> Dict[str, List[Dict[str, Any]]]:
        attach_map: Dict[str, List[Dict[str, Any]]] = {}
        worker_count = min(
            len(thread_items),
            self._clamp_detail_concurrency(self._detail_concurrency),
        )
        if worker_count <= 1:
            for item in thread_items:
                tid = str(item.get("tid") or "").strip()
                if not tid:
                    continue
                attach_map[tid] = self._fetch_single_thread_attachments(
                    base_url=base_url,
                    tid=tid,
                    session_headers=session_headers,
                    session_cookies=session_cookies,
                    timeout=timeout,
                    proxies=proxies,
                )
            return attach_map

        logger.info(
            f"BT之家(1lou)开始并发抓取帖子详情："
            f"帖子数={len(thread_items)}，并发数={worker_count}"
        )

        task_queue = deque(
            (str(item.get("tid") or "").strip(), item)
            for item in thread_items
            if str(item.get("tid") or "").strip()
        )
        with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="1lou") as executor:
            running_tasks: Dict[Any, Tuple[str, Dict[str, Any]]] = {}
            while task_queue or running_tasks:
                while task_queue and len(running_tasks) < worker_count:
                    tid, item = task_queue.popleft()
                    future = executor.submit(
                        self._fetch_single_thread_attachments,
                        base_url,
                        tid,
                        session_headers,
                        session_cookies,
                        timeout,
                        proxies,
                    )
                    running_tasks[future] = (tid, item)

                if not running_tasks:
                    break

                done, _ = wait(tuple(running_tasks.keys()), return_when=FIRST_COMPLETED)
                for future in done:
                    tid, item = running_tasks.pop(future)
                    try:
                        attach_map[tid] = future.result() or []
                    except Exception as err:
                        title = str((item or {}).get("title") or "").strip()
                        logger.debug(
                            f"BT之家(1lou)并发抓取帖子详情异常："
                            f"tid={tid}，标题='{title}'，错误={err}"
                        )
                        attach_map[tid] = []
        return attach_map

    def _resolve_moviepilot_title_candidates(
            self,
            keyword: str,
            mtype: MediaType = None) -> Dict[str, Any]:
        profile: Dict[str, Any] = {
            "title": str(keyword or "").strip(),
            "names": [],
            "source": "none",
        }
        keyword_text = str(keyword or "").strip()
        if not keyword_text:
            return profile

        try:
            from app.core.metainfo import MetaInfo
        except Exception:
            return profile

        meta = MetaInfo(title=keyword_text)
        meta_name = str(getattr(meta, "name", "") or "").strip()
        meta_cn_name = str(getattr(meta, "cn_name", "") or "").strip()
        if meta_name or meta_cn_name:
            profile["names"] = self._unique_nonempty([meta_cn_name, meta_name, keyword_text])

        try:
            from app.modules.themoviedb.tmdb_cache import TmdbCache
            cached = TmdbCache().get(meta) or {}
            cached_title = str(cached.get("title") or "").strip()
            if cached_title:
                profile["title"] = cached_title
                profile["names"] = self._unique_nonempty(
                    [cached_title]
                    + list(cached.get("names") or [])
                    + profile["names"]
                )
                profile["source"] = "cache"
        except Exception:
            pass

        try:
            from app.modules.themoviedb.tmdbapi import TmdbApi
        except Exception:
            return profile

        api = None
        try:
            api = TmdbApi(language=settings.TMDB_LOCALE)
        except Exception:
            try:
                api = TmdbApi()
            except Exception:
                return profile

        try:
            normalized_mtype = self._normalize_profile_mtype(meta.type or mtype)
            matched = api.match(
                name=str(meta.name or keyword_text).strip(),
                mtype=normalized_mtype,
                year=str(getattr(meta, "year", "") or "").strip() or None,
            ) or {}
            matched_tmdbid = self._to_int(matched.get("id"))
            tmdb_info = matched
            if matched_tmdbid > 0 and hasattr(api, "get_info"):
                detailed = api.get_info(
                    mtype=self._normalize_profile_mtype(
                        matched.get("media_type") or matched.get("type") or normalized_mtype
                    ),
                    tmdbid=matched_tmdbid,
                ) or {}
                if detailed:
                    tmdb_info = detailed
            if tmdb_info:
                resolved_title = str(
                    tmdb_info.get("title")
                    or tmdb_info.get("name")
                    or profile["title"]
                ).strip()
                if resolved_title:
                    profile["title"] = resolved_title
                profile["names"] = self._unique_nonempty(
                    [profile["title"]]
                    + list(tmdb_info.get("names") or [])
                    + [
                        tmdb_info.get("original_title"),
                        tmdb_info.get("original_name"),
                    ]
                    + profile["names"]
                )
                if matched_tmdbid > 0:
                    profile["source"] = "match"
        except Exception:
            pass
        finally:
            try:
                if api and hasattr(api, "close"):
                    api.close()
            except Exception:
                pass
        return profile

    @staticmethod
    def _build_search_keywords(keyword: str,
                               media_profile: Optional[Dict[str, Any]] = None) -> List[str]:
        candidates: List[Any] = [keyword]
        if media_profile:
            candidates.extend(media_profile.get("names") or [])
            candidates.append(media_profile.get("title"))
        return LoumeIndexer._unique_nonempty(candidates)

    def _fetch_single_thread_attachments(
            self,
            base_url: str,
            tid: str,
            session_headers: Dict[str, str],
            session_cookies: Dict[str, str],
            timeout: int,
            proxies: Optional[Dict[str, str]]) -> List[Dict[str, Any]]:
        header_cookie = str((session_headers or {}).get("Cookie") or "").strip()
        client_ip = "" if self._has_cloudflare_clearance(header_cookie) else self._rand_ip()
        session = self._build_worker_session(
            session_headers=session_headers,
            session_cookies=session_cookies,
            client_ip=client_ip,
        )
        try:
            return self._fetch_thread_attachments(
                session=session,
                base_url=base_url,
                tid=tid,
                timeout=timeout,
                proxies=proxies,
            )
        finally:
            try:
                session.close()
            except Exception:
                pass

    @staticmethod
    def _build_worker_session(
            session_headers: Dict[str, str],
            session_cookies: Dict[str, str],
            client_ip: str = "") -> _requests.Session:
        session = _requests.Session()
        if session_headers:
            session.headers.update(dict(session_headers))
        if client_ip:
            session.headers["X-Forwarded-For"] = client_ip
            session.headers["X-Real-IP"] = client_ip
        if session_cookies:
            session.cookies.update(dict(session_cookies))
        return session

    @staticmethod
    def _encode_keyword(keyword: str) -> str:
        """
        将关键词编码为 1lou.me 的 URL 格式。
        规则：对每个字节用 _XX 表示（类似 URL encode 但用 _ 代替 %）。
        ASCII 字母/数字/连字符保留原样，其余均编码。
        """
        encoded_pct = quote(keyword, safe="-_.~")
        # quote 用 %XX，1lou 用 _XX（大写）
        return encoded_pct.replace("%", "_")

    def _build_search_urls(self, base_url: str, keyword: str) -> List[str]:
        """构建搜索各页的 URL 列表"""
        enc = self._encode_keyword(keyword)
        urls = []
        # 第 1 页
        urls.append(urljoin(base_url, f"search-{enc}.htm"))
        # 第 2..N 页
        for page in range(2, self._max_search_pages + 1):
            urls.append(urljoin(base_url, f"search-{enc}-1-{page}.htm"))
        return urls

    def _search_threads(self, session: _requests.Session, base_url: str,
                        keyword: str, timeout: int,
                        proxies: Optional[Dict[str, str]],
                        client_ip: str = "") -> Tuple[List[Dict[str, Any]], bool]:
        """搜索帖子列表，分页合并结果"""
        all_items: List[Dict[str, Any]] = []
        seen_tids: set = set()
        search_urls = self._build_search_urls(base_url, keyword)
        blocked_by_cf = False

        for page_num, url in enumerate(search_urls, start=1):
            try:
                headers = dict(session.headers)
                if client_ip:
                    headers["X-Forwarded-For"] = client_ip
                    headers["X-Real-IP"] = client_ip
                resp = session.get(
                    url,
                    timeout=timeout,
                    proxies=proxies,
                    verify=False,
                    allow_redirects=True,
                    headers=headers,
                )
                if self._is_cloudflare_challenge_response(resp):
                    self._log_cloudflare_challenge(cookie_text=headers.get("Cookie") or "")
                    blocked_by_cf = True
                    break
                if not resp.ok:
                    logger.debug(f"BT之家(1lou)搜索请求失败：status={resp.status_code}，page={page_num}")
                    break
                html = resp.text
            except Exception as e:
                logger.debug(f"BT之家(1lou)搜索请求异常：page={page_num}，{e}")
                break

            items = self._parse_thread_list(html)
            if not items:
                break

            added = 0
            for item in items:
                tid = str(item.get("tid") or "").strip()
                if tid and tid not in seen_tids:
                    seen_tids.add(tid)
                    all_items.append(item)
                    added += 1

            # 若本页无新内容，或无下一页，则停止
            if added == 0 or not self._has_next_page(html):
                break

        return all_items, blocked_by_cf

    @staticmethod
    def _is_cloudflare_challenge_response(resp: Optional[_requests.Response]) -> bool:
        if resp is None:
            return False
        try:
            status_code = int(resp.status_code or 0)
        except Exception:
            status_code = 0
        headers = getattr(resp, "headers", {}) or {}
        cf_mitigated = str(headers.get("Cf-Mitigated") or headers.get("cf-mitigated") or "").strip().lower()
        if cf_mitigated == "challenge":
            return True
        server = str(headers.get("Server") or headers.get("server") or "").strip().lower()
        text = str(getattr(resp, "text", "") or "")
        return (
            status_code == 403
            and "cloudflare" in server
            and (
                "Just a moment..." in text
                or "challenges.cloudflare.com" in text
                or "cf-challenge" in text
                or "challenge-platform" in text
            )
        )

    @staticmethod
    def _log_cloudflare_challenge(cookie_text: str = "") -> None:
        if cookie_text:
            logger.warn(
                "BT之家(1lou)请求被 Cloudflare 人机验证拦截：当前站点 Cookie 可能已失效，"
                "请在站点管理中更新浏览器导出的完整 Cookie（至少包含 cf_clearance）后重试。"
            )
            return
        logger.warn(
            "BT之家(1lou)请求被 Cloudflare 人机验证拦截：站点目前不能被普通 requests 直接访问，"
            "请在站点管理中填写浏览器导出的完整 Cookie（至少包含 cf_clearance）后重试。"
        )

    @staticmethod
    def _has_cloudflare_clearance(cookie_text: str = "") -> bool:
        return "cf_clearance=" in str(cookie_text or "").lower()

    @staticmethod
    def _parse_thread_list(html: str) -> List[Dict[str, Any]]:
        """
        解析搜索/论坛列表页，提取帖子列表。
        页面结构：每个帖子是 <li ... data-tid="{tid}"> 块，
        块内有 <a href="thread-{tid}.htm">标题</a>（可能含子标签）。
        """
        items: List[Dict[str, Any]] = []
        seen: set = set()

        # 优先匹配当前 1lou 搜索页结构：
        # <ul class="threadlist"> ... <li data-tid="123"> ... <a href="thread-123.htm">标题</a>
        block_pattern = re.compile(
            r'<li\b[^>]*\bdata-tid="(\d+)"[^>]*>(.*?)</li>',
            re.IGNORECASE | re.DOTALL
        )
        for match in block_pattern.finditer(html):
            tid = str(match.group(1) or "").strip()
            if not tid or tid in seen:
                continue
            block = match.group(2) or ""
            link_match = re.search(
                rf'href="thread-{tid}\.htm"[^>]*>(.*?)</a>',
                block,
                re.IGNORECASE | re.DOTALL
            )
            if not link_match:
                continue
            title = LoumeIndexer._clean_html_text(link_match.group(1))
            if not title:
                continue
            forum_id = LoumeIndexer._extract_search_forum_id(block)
            seen.add(tid)
            items.append({"tid": tid, "title": title, "forum_id": forum_id})

        if items:
            return items

        # 回退：全局提取 thread-*.htm 链接，兼容列表块结构变化但链接规则不变的场景
        for match in re.finditer(
            r'href="thread-(\d+)\.htm"[^>]*>(.*?)</a>',
            html,
            re.IGNORECASE | re.DOTALL
        ):
            tid = str(match.group(1) or "").strip()
            if not tid or tid in seen:
                continue
            title = LoumeIndexer._clean_html_text(match.group(2))
            if not title:
                continue
            seen.add(tid)
            items.append({"tid": tid, "title": title, "forum_id": None})

        return items

    @staticmethod
    def _has_next_page(html: str) -> bool:
        """检查是否有下一页（分页区域存在 search-*-1-N.htm 链接）"""
        return bool(re.search(
            r'href="search-[^"]*-1-\d+\.htm"|rel="next"',
            html,
            re.IGNORECASE
        ))

    def _fetch_thread_attachments(self, session: _requests.Session, base_url: str,
                                  tid: str, timeout: int,
                                  proxies: Optional[Dict[str, str]]) -> List[Dict[str, Any]]:
        """获取帖子详情页中的种子附件列表"""
        thread_url = urljoin(base_url, f"thread-{tid}.htm")
        try:
            resp = session.get(
                thread_url,
                timeout=timeout,
                proxies=proxies,
                verify=False,
                allow_redirects=True,
            )
            if not resp.ok:
                return []
            attach_items = self._parse_attachments(resp.text)
            if not attach_items:
                return []

            for attach in attach_items:
                aid = str(attach.get("aid") or "").strip()
                filename = str(attach.get("filename") or "").strip()
                if not aid:
                    continue
                download_url = urljoin(base_url, f"attach-download-{aid}.htm")
                attach["enclosure"] = self._resolve_attachment_enclosure(
                    session=session,
                    download_url=download_url,
                    filename=filename,
                    timeout=timeout,
                    proxies=proxies,
                ) or download_url
            return attach_items
        except Exception as e:
            logger.debug(f"BT之家(1lou)获取帖子详情失败：tid={tid}，{e}")
            return []

    @staticmethod
    def _parse_attachments(html: str) -> List[Dict[str, Any]]:
        """
        解析帖子详情页中的附件列表。
        HTML 结构：
          <ul class="attachlist">
            <li aid="192817">
              <a href="attach-download-192817.htm" target="_blank">
                <i class="icon filetype torrent"></i>
                Yakamoz.S-245.S01.DUBBED.WEBRip.x264-ION10.torrent
              </a>
            </li>
          </ul>
        """
        items: List[Dict[str, Any]] = []

        # 找 attachlist
        attachlist_match = re.search(
            r'<ul[^>]*class="attachlist"[^>]*>(.*?)</ul>',
            html,
            re.IGNORECASE | re.DOTALL
        )
        if not attachlist_match:
            return []

        attachlist_html = attachlist_match.group(1)

        # 优先按当前结构解析：
        # <li aid="2925671"><a href="attach-download-2925671.htm">xxx.torrent</a></li>
        li_pattern = re.compile(
            r'<li\b[^>]*\baid="(\d+)"[^>]*>(.*?)</li>',
            re.IGNORECASE | re.DOTALL
        )
        for match in li_pattern.finditer(attachlist_html):
            aid = str(match.group(1) or "").strip()
            if not aid:
                continue
            block = match.group(2) or ""
            link_match = re.search(
                r'href="attach-download-(\d+)\.htm"[^>]*>(.*?)</a>',
                block,
                re.IGNORECASE | re.DOTALL
            )
            if not link_match:
                continue
            filename = LoumeIndexer._clean_html_text(link_match.group(2))
            if filename.lower().endswith(".torrent"):
                items.append({"aid": aid, "filename": filename})

        # 若上面正则未匹配（HTML 结构变化），尝试更宽松的方式
        if not items:
            for match in re.finditer(
                r'href="attach-download-(\d+)\.htm"[^>]*>(.*?)</a>',
                attachlist_html,
                re.IGNORECASE | re.DOTALL
            ):
                aid = str(match.group(1) or "").strip()
                if not aid:
                    continue
                filename = LoumeIndexer._clean_html_text(match.group(2))
                if filename.lower().endswith(".torrent"):
                    items.append({"aid": aid, "filename": filename})

        return items

    def _resolve_attachment_enclosure(
            self,
            session: _requests.Session,
            download_url: str,
            filename: str,
            timeout: int,
            proxies: Optional[Dict[str, str]]) -> str:
        try:
            resp = session.get(
                download_url,
                timeout=timeout,
                proxies=proxies,
                verify=False,
                allow_redirects=True,
            )
            if not resp.ok:
                return download_url
            magnet = self._torrent_to_magnet(
                resp.content,
                fallback_name=filename,
                exact_source=download_url,
            )
            if magnet:
                return magnet
        except Exception as err:
            logger.debug(f"BT之家(1lou)附件转磁力失败：文件='{filename}'，错误={err}")
        return download_url

    @staticmethod
    def _clean_html_text(raw: Any) -> str:
        text = str(raw or "")
        if not text:
            return ""
        text = re.sub(r'<br\s*/?>', ' ', text, flags=re.IGNORECASE)
        text = re.sub(r'<[^>]+>', ' ', text)
        text = unescape(text)
        text = re.sub(r'\s+', ' ', text).strip()
        return text

    @staticmethod
    def _is_netdisk_thread_title(raw: Any) -> bool:
        title = str(raw or "").strip().lower()
        if not title:
            return False
        netdisk_tokens = (
            "夸克下载",
            "夸克网盘",
            "百度网盘",
            "百度云",
            "阿里云盘",
            "阿里网盘",
            "迅雷云盘",
            "uc网盘",
            "uc下载",
            "115网盘",
            "115下载",
            "网盘下载",
            "网盘资源",
        )
        return any(token in title for token in netdisk_tokens)

    def _is_allowed_search_forum_id(self, raw: Any) -> bool:
        try:
            forum_id = int(raw)
        except Exception:
            return False
        return forum_id in self._allowed_forum_ids

    @staticmethod
    def _extract_search_forum_id(block: Any) -> Optional[int]:
        text = str(block or "")
        if not text:
            return None
        matches = re.findall(r'href="forum-(\d+)-\d+\.htm(?:\?[^"]*)?"', text, re.IGNORECASE)
        for raw in matches:
            try:
                return int(raw)
            except Exception:
                continue
        return None

    @staticmethod
    def _parse_size_from_text(text: str) -> int:
        """从文本中解析文件大小，如 7.26GB、10.62G、774.46M"""
        m = re.search(r'(\d+(?:\.\d+)?)\s*(TB|GB|MB|KB|T|G|M|K)\b', str(text or ""), re.IGNORECASE)
        if not m:
            return 0
        val = float(m.group(1))
        unit = m.group(2).upper()
        mul = {
            "TB": 1 << 40,
            "T": 1 << 40,
            "GB": 1 << 30,
            "G": 1 << 30,
            "MB": 1 << 20,
            "M": 1 << 20,
            "KB": 1 << 10,
            "K": 1 << 10,
        }
        return int(val * mul.get(unit, 0))

    @classmethod
    def _extract_size_bytes(cls, filename: str = "", title: str = "") -> int:
        for text in [filename, title]:
            size_bytes = cls._parse_size_from_text(text)
            if size_bytes > 0:
                return size_bytes
        return 0

    @staticmethod
    def _torrent_to_magnet(
            data: bytes,
            fallback_name: str = "",
            exact_source: str = "") -> str:
        if not data:
            return ""
        try:
            parsed, info_start, info_end = LoumeIndexer._bdecode_with_info_range(data)
        except Exception:
            return ""
        if info_start < 0 or info_end <= info_start:
            return ""

        info_hash = hashlib.sha1(data[info_start:info_end]).hexdigest()
        dn = str(fallback_name or "").strip()
        xl = 0
        tr_list: List[str] = []
        ws_list: List[str] = []
        if isinstance(parsed, dict):
            info = parsed.get(b"info")
            if isinstance(info, dict):
                name_bytes = info.get(b"name.utf-8") or info.get(b"name")
                if isinstance(name_bytes, (bytes, bytearray)):
                    dn = bytes(name_bytes).decode("utf-8", errors="ignore").strip()
                xl = LoumeIndexer._extract_torrent_total_length(info)
            announce = parsed.get(b"announce")
            if isinstance(announce, (bytes, bytearray)):
                tr_list.append(bytes(announce).decode("utf-8", errors="ignore").strip())
            announce_list = parsed.get(b"announce-list")
            if isinstance(announce_list, list):
                for tier in announce_list:
                    if isinstance(tier, list):
                        for item in tier:
                            if isinstance(item, (bytes, bytearray)):
                                tr_list.append(bytes(item).decode("utf-8", errors="ignore").strip())
                    elif isinstance(tier, (bytes, bytearray)):
                        tr_list.append(bytes(tier).decode("utf-8", errors="ignore").strip())
            url_list = parsed.get(b"url-list")
            if isinstance(url_list, list):
                for item in url_list:
                    if isinstance(item, (bytes, bytearray)):
                        ws_list.append(bytes(item).decode("utf-8", errors="ignore").strip())
            elif isinstance(url_list, (bytes, bytearray)):
                ws_list.append(bytes(url_list).decode("utf-8", errors="ignore").strip())
            httpseeds = parsed.get(b"httpseeds")
            if isinstance(httpseeds, list):
                for item in httpseeds:
                    if isinstance(item, (bytes, bytearray)):
                        ws_list.append(bytes(item).decode("utf-8", errors="ignore").strip())
            elif isinstance(httpseeds, (bytes, bytearray)):
                ws_list.append(bytes(httpseeds).decode("utf-8", errors="ignore").strip())

        magnet = f"magnet:?xt=urn:btih:{info_hash}"
        if dn:
            magnet += f"&dn={quote(dn)}"
        if xl > 0:
            magnet += f"&xl={xl}"
        if exact_source:
            magnet += f"&xs={quote(str(exact_source).strip(), safe=':/?&=')}"

        seen = set()
        for tr in tr_list:
            tracker = str(tr or "").strip()
            if not tracker or tracker in seen:
                continue
            seen.add(tracker)
            magnet += f"&tr={quote(tracker, safe=':/?&=')}"

        ws_seen = set()
        for ws in ws_list:
            web_seed = str(ws or "").strip()
            if not web_seed or web_seed in ws_seen:
                continue
            ws_seen.add(web_seed)
            magnet += f"&ws={quote(web_seed, safe=':/?&=')}"
        return magnet

    @staticmethod
    def _extract_torrent_total_length(info: Any) -> int:
        if not isinstance(info, dict):
            return 0
        length = info.get(b"length")
        if isinstance(length, int) and length > 0:
            return length
        files = info.get(b"files")
        total = 0
        if isinstance(files, list):
            for item in files:
                if not isinstance(item, dict):
                    continue
                file_length = item.get(b"length")
                if isinstance(file_length, int) and file_length > 0:
                    total += file_length
        return total

    @staticmethod
    def _rand_ip() -> str:
        """生成随机公网 IP，用于降低站点按 IP 维度的风控命中概率"""
        return f"{random.randint(1, 223)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(1, 254)}"

    @staticmethod
    def _bdecode_with_info_range(data: bytes) -> Tuple[Any, int, int]:
        info_start = -1
        info_end = -1

        def parse(idx: int) -> Tuple[Any, int]:
            nonlocal info_start, info_end
            if idx >= len(data):
                raise ValueError("unexpected eof")
            token = data[idx:idx + 1]
            if token == b"i":
                end = data.index(b"e", idx + 1)
                return int(data[idx + 1:end]), end + 1
            if token == b"l":
                idx += 1
                arr = []
                while data[idx:idx + 1] != b"e":
                    item, idx = parse(idx)
                    arr.append(item)
                return arr, idx + 1
            if token == b"d":
                idx += 1
                obj = {}
                while data[idx:idx + 1] != b"e":
                    key, idx = parse(idx)
                    if not isinstance(key, (bytes, bytearray)):
                        raise ValueError("invalid key")
                    value_start = idx
                    value, idx = parse(idx)
                    obj[bytes(key)] = value
                    if bytes(key) == b"info" and info_start < 0:
                        info_start = value_start
                        info_end = idx
                return obj, idx + 1
            if b"0" <= token <= b"9":
                colon = data.index(b":", idx)
                length = int(data[idx:colon])
                start = colon + 1
                end = start + length
                return data[start:end], end
            raise ValueError("invalid bencode")

        obj, _ = parse(0)
        return obj, info_start, info_end

    @staticmethod
    def _clamp_detail_concurrency(value: Any) -> int:
        try:
            concurrency = int(value or 5)
        except Exception:
            concurrency = 5
        return max(1, min(concurrency, 100))

    @staticmethod
    def _normalize_profile_mtype(mtype: MediaType = None):
        if mtype is None:
            return None
        value = getattr(mtype, "value", mtype)
        text = str(value or "").strip()
        if not text:
            return None
        normalized = text.lower()
        if normalized in ("movie", "movies", "film"):
            return MediaType.MOVIE
        if normalized in ("tv", "television", "series"):
            return MediaType.TV
        return mtype

    @staticmethod
    def _unique_nonempty(items: List[Any]) -> List[str]:
        ret: List[str] = []
        seen = set()
        for item in items or []:
            text = str(item or "").strip()
            if not text:
                continue
            lowered = text.lower()
            if lowered in seen:
                continue
            seen.add(lowered)
            ret.append(text)
        return ret

    @staticmethod
    def _to_int(value: Any) -> int:
        try:
            return int(str(value or "").strip())
        except Exception:
            return 0

    def _match_target_site(self, site: dict) -> bool:
        site_id = str(site.get("id") or "").strip().lower()
        if site_id in ("1lou", "loume", "1lou.me"):
            return True

        all_hosts = self._all_hosts()
        for candidate in [site.get("domain"), site.get("url")]:
            host = self._extract_host(candidate)
            if host and self._is_host_match(host, all_hosts):
                return True
        return False

    def _resolve_base_url(self, site: dict) -> str:
        candidates = self._build_base_url_candidates(site=site)
        if candidates:
            return candidates[0]
        return self._default_base_url

    def _all_hosts(self) -> set:
        hosts = {self._default_host, "www.1lou.me"}
        for host in self._ordered_extra_hosts():
            hosts.add(host)
        return hosts

    def _registered_hosts(self) -> List[str]:
        ordered_extra_hosts = self._ordered_extra_hosts()
        if ordered_extra_hosts:
            # extra_hosts 只控制实际访问优先级；索引器注册仍保留内置旧域名，
            # 确保站点管理暂未改到新域名时，搜索入口也能被插件接管。
            return list(dict.fromkeys(ordered_extra_hosts + sorted(self._all_hosts())))
        return sorted(self._all_hosts())

    def _ordered_extra_hosts(self) -> List[str]:
        ret: List[str] = []
        seen = set()
        for line in (self._extra_hosts or "").splitlines():
            host = self._extract_host(line)
            if not host or host in seen:
                continue
            seen.add(host)
            ret.append(host)
        return ret

    def _build_base_url_candidates(self, site: dict) -> List[str]:
        candidates: List[str] = []
        seen = set()

        ordered_extra_hosts = self._ordered_extra_hosts()
        for host in ordered_extra_hosts:
            base_url = self._normalize_base_url(host)
            if base_url and base_url not in seen:
                seen.add(base_url)
                candidates.append(base_url)

        if ordered_extra_hosts:
            return candidates

        for raw in (
            self._default_base_url,
            site.get("url") if isinstance(site, dict) else "",
            site.get("domain") if isinstance(site, dict) else "",
        ):
            base_url = self._normalize_base_url(raw)
            if base_url and base_url not in seen:
                seen.add(base_url)
                candidates.append(base_url)
        return candidates

    def _register_builtin_indexer(self) -> None:
        hosts = self._registered_hosts()
        if not hosts:
            return
        indexer = self._build_indexer_schema(hosts)
        for host in hosts:
            try:
                SitesHelper().add_indexer(domain=host, indexer=indexer)
            except Exception as err:
                logger.debug(f"BT之家(1lou)索引器注册失败：域名={host}，错误={err}")
        logger.info(f"BT之家(1lou)索引器注册完成：域名列表={', '.join(hosts)}")

    @staticmethod
    def _build_indexer_schema(all_hosts: List[str]) -> Dict[str, Any]:
        primary = "www.1lou.me" if "www.1lou.me" in all_hosts else all_hosts[0]
        ext_domains = [f"https://{h}/" for h in all_hosts if h != primary]
        return {
            "id": "1lou",
            "name": "BT之家",
            "domain": f"https://{primary}/",
            "ext_domains": ext_domains,
            "encoding": "UTF-8",
            "public": True,
            "proxy": True,
            "result_num": 100,
            "timeout": 30,
            "search": {
                "paths": [
                    {
                        "path": "search.htm",
                        "method": "get"
                    }
                ]
            },
            "torrents": {
                "list": {
                    "selector": "li.__never_match__"
                },
                "fields": {
                    "id": {"selector": "a"},
                    "title": {"selector": "a"},
                    "details": {"selector": "a", "attribute": "href"},
                    "download": {"selector": "a", "attribute": "href"},
                    "downloadvolumefactor": {"case": {"*": 0}},
                    "uploadvolumefactor": {"case": {"*": 1}},
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
        return host

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
        if not parsed.netloc:
            return ""
        scheme = parsed.scheme or "https"
        return f"{scheme}://{parsed.netloc}/"

    @staticmethod
    def _is_host_match(host: str, allowed_hosts: set) -> bool:
        pure = host.lower().lstrip(".")
        pure_no_www = pure[4:] if pure.startswith("www.") else pure
        for allowed in allowed_hosts:
            a = allowed.lower().lstrip(".")
            a_no_www = a[4:] if a.startswith("www.") else a
            if pure == a or pure_no_www == a_no_www:
                return True
        return False
