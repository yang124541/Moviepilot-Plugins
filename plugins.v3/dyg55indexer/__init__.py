import hashlib
import random
import re
from collections import deque
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import datetime, timedelta
from html import unescape
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote, urljoin, urlparse

import requests as _requests
from fastapi.concurrency import run_in_threadpool

from app.sdk.config import settings
from app.sdk.logging import logger
from app.sdk.media import TorrentInfo
from app.sdk.network import SitesHelper
from app.plugins import _PluginBase
from app.schemas.types import MediaType


class Dyg55Indexer(_PluginBase):
    plugin_name = "电影港（dyg55）"
    plugin_desc = "为 dyg55.com 提供电影港 BT 种子搜索支持。"
    plugin_icon = "Dyg55Indexer.png"
    plugin_version = "2.0.1"
    plugin_author = "yang124541"
    author_url = "https://github.com/yang124541/moviepilot-plugin"
    plugin_config_prefix = "dyg55indexer_"
    plugin_order = 33
    auth_level = 2

    _enabled = False
    _extra_hosts = ""
    _detail_concurrency = 5

    _default_host = "dyg55.com"
    _default_base_url = "https://www.dyg55.com/"

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
                                            "label": "详情并发数",
                                            "min": 1,
                                            "max": 100,
                                            "placeholder": "5",
                                            "hint": "并发抓取详情页、下载地址和种子内容，允许范围 1-100",
                                            "persistentHint": True,
                                        },
                                    }
                                ],
                            },
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
                                            "placeholder": "www.dyg55.com",
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

        media_profile = self._resolve_moviepilot_media_profile(keyword=keyword, mtype=mtype)
        self._log_moviepilot_media_profile(keyword=keyword, media_profile=media_profile)
        imdb_keyword = self._normalize_imdb_id(media_profile.get("imdb_id"))
        keyword_text = str(keyword or "").strip()
        used_search_mode = "imdb" if imdb_keyword else "keyword"
        used_search_keyword = imdb_keyword or keyword_text

        logger.info(
            f"电影港(dyg55)开始搜索：关键词='{keyword}'，"
            f"搜索方式='{'IMDb' if imdb_keyword else '关键词'}'"
        )

        try:
            session = _requests.Session()
            session.headers.update({
                "User-Agent": ua,
                "Referer": base_url,
                "Accept-Language": "zh-CN,zh;q=0.9",
            })
            cookie_from_site = str(site.get("cookie") or "").strip()
            if cookie_from_site:
                session.headers["Cookie"] = cookie_from_site

            search_items: List[Dict[str, Any]] = []
            if imdb_keyword:
                search_items = self._search_items(
                    session=session,
                    base_url=base_url,
                    keyword=imdb_keyword,
                    timeout=timeout,
                    proxies=proxies,
                )
                logger.debug(
                    f"电影港(dyg55)IMDb搜索结果：imdb_id='{imdb_keyword}'，"
                    f"命中视频={len(search_items)}"
                )
            if (not search_items) and keyword_text and (
                not imdb_keyword or keyword_text.lower() != imdb_keyword.lower()
            ):
                if imdb_keyword:
                    logger.debug(
                        f"电影港(dyg55)IMDb未命中，回退关键词搜索：关键词='{keyword_text}'"
                    )
                used_search_mode = "keyword"
                used_search_keyword = keyword_text
                search_items = self._search_items(
                    session=session,
                    base_url=base_url,
                    keyword=keyword_text,
                    timeout=timeout,
                    proxies=proxies,
                )
            if not search_items:
                cost = (datetime.now() - start_at).seconds
                logger.info(
                    f"电影港(dyg55)搜索完成：关键词='{keyword}'，"
                    f"搜索方式='{used_search_mode}'，"
                    f"搜索词='{used_search_keyword}'，"
                    f"找到视频=0，返回磁力=0，耗时={cost}s"
                )
                return []

            results = self._fetch_details_concurrently(
                site=site,
                base_url=base_url,
                items=search_items,
                session_headers=dict(session.headers),
                timeout=timeout,
                proxies=proxies,
            )

            cost = (datetime.now() - start_at).seconds
            logger.info(
                f"电影港(dyg55)搜索完成：关键词='{keyword}'，"
                f"搜索方式='{used_search_mode}'，"
                f"搜索词='{used_search_keyword}'，"
                f"找到视频={len(search_items)}，返回磁力={len(results)}，耗时={cost}s"
            )
            return results
        except Exception as err:
            logger.error(f"电影港(dyg55)搜索异常：关键词='{keyword}'，错误={err}")
            return []

    def _resolve_moviepilot_media_profile(
            self,
            keyword: str,
            mtype: MediaType = None) -> Dict[str, Any]:
        profile: Dict[str, Any] = {
            "title": str(keyword or "").strip(),
            "year": self._extract_year_token(keyword),
            "tmdb_id": "",
            "imdb_id": "",
            "names": [],
            "actors": [],
            "source": "none",
            "resolved_mtype": "",
        }
        keyword_text = str(keyword or "").strip()
        if not keyword_text:
            return profile

        try:
            from app.sdk.media import MetaInfo
        except Exception as err:
            logger.debug(f"电影港(dyg55)加载主程序 MetaInfo 失败：{err}")
            return profile

        meta = MetaInfo(title=keyword_text)
        if not getattr(meta, "name", None):
            return profile
        if not getattr(meta, "year", None) and profile["year"]:
            meta.year = profile["year"]
        if mtype and not getattr(meta, "type", None):
            meta.type = mtype

        tmdb_info: Dict[str, Any] = {}
        try:
            from app.modules.themoviedb.tmdb_cache import TmdbCache
            cached = TmdbCache().get(meta) or {}
            if cached:
                profile["tmdb_id"] = str(cached.get("id") or "").strip()
                profile["title"] = str(cached.get("title") or profile["title"]).strip()
                profile["year"] = str(cached.get("year") or profile["year"]).strip()
                profile["resolved_mtype"] = str(
                    self._normalize_profile_mtype(
                        cached.get("media_type")
                        or cached.get("type")
                        or meta.type
                        or mtype
                    ) or ""
                ).strip()
                profile["source"] = "cache"
                if profile["title"]:
                    profile["names"] = self._unique_nonempty([profile["title"]])
        except Exception as err:
            logger.debug(f"电影港(dyg55)读取 TMDB 缓存失败：{err}")

        try:
            from app.modules.themoviedb.tmdbapi import TmdbApi
        except Exception as err:
            logger.debug(f"电影港(dyg55)加载 TmdbApi 失败：{err}")
            return profile

        api = None
        try:
            api = TmdbApi(language=settings.TMDB_LOCALE)
        except Exception:
            try:
                api = TmdbApi()
            except Exception as err:
                logger.debug(f"电影港(dyg55)初始化 TmdbApi 失败：{err}")
                return profile

        try:
            normalized_mtype = self._normalize_profile_mtype(meta.type or mtype)
            cache_tmdbid = self._to_int(profile["tmdb_id"])
            detail_mtype = self._normalize_profile_mtype(
                profile["resolved_mtype"] or normalized_mtype
            )
            if cache_tmdbid > 0 and hasattr(api, "get_info"):
                cached_detail = api.get_info(
                    mtype=detail_mtype or normalized_mtype,
                    tmdbid=cache_tmdbid,
                ) or {}
                if cached_detail:
                    tmdb_info = cached_detail
                    profile["source"] = "cache+detail"
            if not tmdb_info and hasattr(api, "match"):
                logger.debug(
                    f"电影港(dyg55)主程序媒体识别：先执行TMDB match，"
                    f"关键词='{str(meta.name or keyword_text).strip()}'，"
                    f"类型='{str(normalized_mtype or '').strip() or 'unknown'}'，"
                    f"年份='{str(getattr(meta, 'year', '') or profile['year']).strip() or ''}'"
                )
                matched_tmdb_info = api.match(
                    name=str(meta.name or keyword_text).strip(),
                    mtype=normalized_mtype,
                    year=str(getattr(meta, "year", "") or profile["year"]).strip() or None,
                ) or {}
                matched_tmdbid = self._to_int(matched_tmdb_info.get("id"))
                matched_mtype = self._normalize_profile_mtype(
                    matched_tmdb_info.get("media_type")
                    or matched_tmdb_info.get("type")
                    or normalized_mtype
                )
                profile["resolved_mtype"] = str(
                    matched_mtype or normalized_mtype or ""
                ).strip()
                if matched_tmdbid > 0 and hasattr(api, "get_info"):
                    detailed = api.get_info(
                        mtype=matched_mtype or normalized_mtype,
                        tmdbid=matched_tmdbid,
                    ) or {}
                    if detailed:
                        tmdb_info = detailed
                        profile["source"] = "match+detail"
                if not tmdb_info and matched_tmdb_info:
                    tmdb_info = matched_tmdb_info
                    if matched_tmdbid > 0:
                        profile["source"] = "match"
            if tmdb_info:
                external_ids = tmdb_info.get("external_ids") or {}
                profile["tmdb_id"] = str(tmdb_info.get("id") or profile["tmdb_id"]).strip()
                profile["imdb_id"] = self._normalize_imdb_id(external_ids.get("imdb_id"))
                profile["title"] = str(
                    tmdb_info.get("title")
                    or tmdb_info.get("name")
                    or profile["title"]
                ).strip()
                profile["year"] = str(
                    self._extract_year_token(
                        tmdb_info.get("release_date")
                        or tmdb_info.get("first_air_date")
                        or profile["year"]
                    ) or profile["year"]
                ).strip()
                profile["names"] = self._unique_nonempty(
                    [profile["title"]]
                    + list(tmdb_info.get("names") or [])
                    + [
                        tmdb_info.get("original_title"),
                        tmdb_info.get("original_name"),
                    ]
                )
                profile["actors"] = self._extract_tmdb_actor_names(tmdb_info)
        except Exception as err:
            logger.debug(f"电影港(dyg55)识别主程序媒体信息失败：{err}")
        finally:
            try:
                if api and hasattr(api, "close"):
                    api.close()
            except Exception:
                pass

        return profile

    def _log_moviepilot_media_profile(self, keyword: str, media_profile: Dict[str, Any]) -> None:
        first_actor = str(((media_profile.get("actors") or [""])[0]) or "").strip()
        logger.debug(
            f"电影港(dyg55)主程序媒体信息：关键词='{str(keyword or '').strip()}'，"
            f"来源='{str(media_profile.get('source') or 'none').strip()}'，"
            f"类型='{str(media_profile.get('resolved_mtype') or '').strip() or 'unknown'}'，"
            f"tmdb_id={'有' if str(media_profile.get('tmdb_id') or '').strip() else '无'}，"
            f"imdb_id={'有' if str(media_profile.get('imdb_id') or '').strip() else '无'}，"
            f"主演={'有' if first_actor else '无'}，"
            f"第一主演='{first_actor}'"
        )

    def _search_items(self, session: _requests.Session, base_url: str,
                      keyword: str, timeout: int,
                      proxies: Optional[Dict[str, str]]) -> List[Dict[str, Any]]:
        try:
            resp = session.get(
                urljoin(base_url, "search.php"),
                params={"s": keyword},
                timeout=timeout,
                proxies=proxies,
                allow_redirects=True,
            )
            if not resp.ok:
                logger.debug(f"电影港(dyg55)搜索请求失败：status={resp.status_code}")
                return []
            return self._parse_search_results(resp.text, base_url=base_url)
        except Exception as err:
            logger.debug(f"电影港(dyg55)搜索请求异常：关键词='{keyword}'，错误={err}")
            return []

    def _fetch_details_concurrently(
            self,
            site: dict,
            base_url: str,
            items: List[Dict[str, Any]],
            session_headers: Dict[str, str],
            timeout: int,
            proxies: Optional[Dict[str, str]]) -> List[TorrentInfo]:
        worker_count = min(
            len(items),
            self._clamp_detail_concurrency(self._detail_concurrency),
        )
        if worker_count <= 1:
            results: List[TorrentInfo] = []
            for item in items:
                results.extend(
                    self._fetch_single_item_result(
                        site=site,
                        base_url=base_url,
                        item=item,
                        session_headers=session_headers,
                        timeout=timeout,
                        proxies=proxies,
                    )
                )
            return results

        ordered_results: Dict[int, List[TorrentInfo]] = {}
        task_queue = deque((index, item) for index, item in enumerate(items))
        with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="dyg55") as executor:
            running_tasks: Dict[Any, Tuple[int, Dict[str, Any]]] = {}
            while task_queue or running_tasks:
                while task_queue and len(running_tasks) < worker_count:
                    index, item = task_queue.popleft()
                    future = executor.submit(
                        self._fetch_single_item_result,
                        site,
                        base_url,
                        item,
                        session_headers,
                        timeout,
                        proxies,
                    )
                    running_tasks[future] = (index, item)

                if not running_tasks:
                    break

                done, _ = wait(tuple(running_tasks.keys()), return_when=FIRST_COMPLETED)
                for future in done:
                    index, item = running_tasks.pop(future)
                    try:
                        ordered_results[index] = future.result() or []
                    except Exception as err:
                        logger.debug(
                            f"电影港(dyg55)并发抓取异常："
                            f"file_id={item.get('file_id')}，错误={err}"
                        )
                        ordered_results[index] = []

        results: List[TorrentInfo] = []
        for index in range(len(items)):
            results.extend(ordered_results.get(index) or [])
        return results

    def _fetch_single_item_result(
            self,
            site: dict,
            base_url: str,
            item: Dict[str, Any],
            session_headers: Dict[str, str],
            timeout: int,
            proxies: Optional[Dict[str, str]]) -> List[TorrentInfo]:
        detail_url = str(item.get("detail_url") or "").strip()
        file_id = str(item.get("file_id") or "").strip()
        if not detail_url or not file_id:
            return []

        session = self._build_worker_session(session_headers=session_headers)
        try:
            resp = session.get(
                detail_url,
                timeout=timeout,
                proxies=proxies,
                allow_redirects=True,
            )
            if not resp.ok:
                return []
            detail_html = resp.text

            down_meta = self._parse_down_meta(detail_html)
            if not down_meta:
                logger.debug(f"电影港(dyg55)未找到下载参数：file_id={file_id}")
                return []

            download_url = self._load_download_url(
                session=session,
                base_url=base_url,
                detail_url=detail_url,
                file_id=file_id,
                uid=str(down_meta.get("uid") or "0"),
                formhash=str(down_meta.get("formhash") or ""),
                timeout=timeout,
                proxies=proxies,
            )
            if not download_url:
                return []

            torrent_data = self._download_torrent_bytes(
                session=session,
                download_url=download_url,
                detail_url=detail_url,
                timeout=timeout,
                proxies=proxies,
            )
            magnet = self._torrent_to_magnet(
                data=torrent_data,
                fallback_name=str(item.get("title") or ""),
                exact_source=download_url,
            )
            enclosure = magnet or download_url
            if not enclosure:
                return []

            detail_meta = self._parse_detail_meta(detail_html)
            title = str(detail_meta.get("title") or item.get("title") or "").strip()

            size_bytes = int(item.get("size_bytes") or 0)
            if size_bytes <= 0:
                size_bytes = self._extract_torrent_total_length_from_bytes(torrent_data)

            pubdate = self._parse_pubdate_text(
                str(detail_meta.get("upload_date") or item.get("date_text") or "")
            )
            description = self._build_description(item=item, detail_meta=detail_meta)
            description = self._append_invisible_unique_marker(
                description=description,
                seed="|".join([
                    str(detail_url or "").strip(),
                    str(enclosure or "").strip(),
                    str(title or "").strip(),
                    str(size_bytes or 0),
                ])
            )
            unique_page_url = self._build_unique_result_page_url(
                detail_url=detail_url,
                enclosure=enclosure,
                result_title=title,
                size_bytes=size_bytes,
            )

            return [TorrentInfo(
                site=site.get("id"),
                site_name=site.get("name"),
                site_cookie=site.get("cookie"),
                site_ua=site.get("ua"),
                site_proxy=site.get("proxy"),
                site_order=site.get("pri"),
                site_downloader=site.get("downloader"),
                title=title,
                description=description,
                enclosure=enclosure,
                page_url=unique_page_url,
                size=size_bytes,
                seeders=0,
                peers=0,
                grabs=0,
                pubdate=None,
                date_elapsed=str(item.get("date_text") or ""),
                downloadvolumefactor=0,
                uploadvolumefactor=1,
            )]
        except Exception as err:
            logger.debug(f"电影港(dyg55)抓取详情失败：file_id={file_id}，错误={err}")
            return []
        finally:
            try:
                session.close()
            except Exception:
                pass

    @staticmethod
    def _build_worker_session(session_headers: Dict[str, str]) -> _requests.Session:
        session = _requests.Session()
        if session_headers:
            session.headers.update(dict(session_headers))
        return session

    @staticmethod
    def _build_description(item: Dict[str, Any], detail_meta: Dict[str, str]) -> str:
        parts: List[str] = []
        category = str(detail_meta.get("category") or "").strip()
        if category:
            parts.append(category)
        score = str(item.get("score_text") or "").strip()
        if score:
            parts.append(score)
        size_text = str(item.get("size_text") or "").strip()
        if size_text:
            parts.append(size_text)
        tags = [str(tag).strip() for tag in (item.get("tags") or []) if str(tag).strip()]
        if tags:
            parts.append("/".join(tags))
        upload_date = str(detail_meta.get("upload_date") or "").strip()
        if upload_date:
            parts.append(upload_date)
        return " | ".join(parts)

    def _load_download_url(self, session: _requests.Session,
                           base_url: str, detail_url: str,
                           file_id: str, uid: str, formhash: str,
                           timeout: int,
                           proxies: Optional[Dict[str, str]]) -> str:
        if not formhash:
            return ""
        payload = {
            "action": "load_down",
            "task": "down",
            "file_id": file_id,
            "uid": uid or "0",
            "formhash": formhash,
            "t": str(random.random()),
        }
        try:
            resp = session.post(
                urljoin(base_url, "ajax.php"),
                data=payload,
                headers={
                    "Referer": detail_url,
                    "Origin": base_url.rstrip("/"),
                    "X-Requested-With": "XMLHttpRequest",
                },
                timeout=timeout,
                proxies=proxies,
            )
            if not resp.ok:
                return ""
            text = str(resp.text or "").strip()
            parts = text.split("|", 1)
            if len(parts) == 2 and parts[0].strip().lower() == "true":
                return urljoin(detail_url, parts[1].strip())
            return ""
        except Exception as err:
            logger.debug(f"电影港(dyg55)加载下载地址失败：file_id={file_id}，错误={err}")
            return ""

    @staticmethod
    def _download_torrent_bytes(session: _requests.Session,
                                download_url: str, detail_url: str,
                                timeout: int,
                                proxies: Optional[Dict[str, str]]) -> bytes:
        try:
            resp = session.get(
                download_url,
                headers={"Referer": detail_url},
                timeout=timeout,
                proxies=proxies,
                allow_redirects=True,
            )
            if not resp.ok:
                return b""
            return bytes(resp.content or b"")
        except Exception:
            return b""

    @classmethod
    def _parse_search_results(cls, html: str, base_url: str) -> List[Dict[str, Any]]:
        if not html:
            return []
        items: List[Dict[str, Any]] = []
        seen = set()
        pattern = re.compile(
            r'<li\s+class="item-list">(.*?)</li>',
            re.IGNORECASE | re.DOTALL,
        )
        for match in pattern.finditer(html):
            block = match.group(1) or ""
            href_match = re.search(
                r'href="([^"]*file-(\d+)\.html)"',
                block,
                re.IGNORECASE,
            )
            if not href_match:
                continue
            detail_href = str(href_match.group(1) or "").strip()
            file_id = str(href_match.group(2) or "").strip()
            if not detail_href or not file_id or file_id in seen:
                continue

            title_match = re.search(
                r'<div\s+class="list-name">\s*<a[^>]*>(.*?)</a>',
                block,
                re.IGNORECASE | re.DOTALL,
            )
            title = cls._clean_html_text(title_match.group(1) if title_match else "")
            if not title:
                continue

            score_text = cls._clean_html_text(
                cls._first_group(block, r'<span\s+class="green">(.*?)</span>')
            )
            size_text = cls._clean_html_text(
                cls._first_group(block, r'<span\s+class="light-blue">(.*?)</span>')
            )
            date_text = cls._clean_html_text(
                cls._first_group(block, r'<div\s+class="list-time">(.*?)</div>')
            )
            tags = [
                cls._clean_html_text(tag)
                for tag in re.findall(
                    r'<span\s+class="(?:red|yellow)">(.*?)</span>',
                    block,
                    re.IGNORECASE | re.DOTALL,
                )
            ]
            tags = [tag for tag in tags if tag]

            items.append({
                "file_id": file_id,
                "detail_url": urljoin(base_url, detail_href),
                "title": title,
                "score_text": score_text,
                "size_text": size_text,
                "size_bytes": cls._parse_size_from_text(size_text),
                "date_text": date_text,
                "tags": tags,
            })
            seen.add(file_id)
        return items

    @classmethod
    def _parse_down_meta(cls, html: str) -> Dict[str, str]:
        match = re.search(
            r'bd_m\("(?P<file_id>\d+)","(?P<uid>\d+)","(?P<formhash>[^"]+)","(?P<title>.*?)",".*?"\)',
            html or "",
            re.IGNORECASE | re.DOTALL,
        )
        if not match:
            return {}
        return {
            "file_id": str(match.group("file_id") or "").strip(),
            "uid": str(match.group("uid") or "0").strip(),
            "formhash": str(match.group("formhash") or "").strip(),
            "title": cls._clean_html_text(match.group("title") or ""),
        }

    @classmethod
    def _parse_detail_meta(cls, html: str) -> Dict[str, str]:
        meta: Dict[str, str] = {}
        if not html:
            return meta

        title_match = re.search(
            r'<h1\s+class="title">(.*?)</h1>',
            html,
            re.IGNORECASE | re.DOTALL,
        )
        title = cls._clean_html_text(title_match.group(1) if title_match else "")
        title = re.sub(r'_?BT种子下载$', "", title, flags=re.IGNORECASE).strip()
        if title:
            meta["title"] = title

        upload_match = re.search(
            r'于\s*(\d{4}-\d{2}-\d{2})\s*上传在\s*(.*?)</span>',
            html,
            re.IGNORECASE | re.DOTALL,
        )
        if upload_match:
            meta["upload_date"] = str(upload_match.group(1) or "").strip()
            category_html = str(upload_match.group(2) or "")
            category_parts = [
                cls._clean_html_text(part)
                for part in re.findall(r'>([^<>]+)</a>', category_html)
            ]
            category_parts = [part for part in category_parts if part]
            if category_parts:
                meta["category"] = " / ".join(category_parts)
        return meta

    @staticmethod
    def _first_group(text: str, pattern: str) -> str:
        match = re.search(pattern, text or "", re.IGNORECASE | re.DOTALL)
        return str(match.group(1) or "") if match else ""

    @staticmethod
    def _clean_html_text(raw: Any) -> str:
        text = str(raw or "")
        if not text:
            return ""
        text = re.sub(r'<br\s*/?>', ' ', text, flags=re.IGNORECASE)
        text = re.sub(r'<[^>]+>', ' ', text)
        text = unescape(text)
        return re.sub(r'\s+', ' ', text).strip()

    @staticmethod
    def _parse_size_from_text(text: str) -> int:
        match = re.search(
            r'(\d+(?:\.\d+)?)\s*(TB|GB|MB|KB|T|G|M|K)\b',
            str(text or ""),
            re.IGNORECASE,
        )
        if not match:
            return 0
        value = float(match.group(1))
        unit = match.group(2).upper()
        factors = {
            "TB": 1 << 40,
            "T": 1 << 40,
            "GB": 1 << 30,
            "G": 1 << 30,
            "MB": 1 << 20,
            "M": 1 << 20,
            "KB": 1 << 10,
            "K": 1 << 10,
        }
        return int(value * factors.get(unit, 0))

    @staticmethod
    def _parse_pubdate_text(text: str) -> Optional[datetime]:
        raw = str(text or "").strip()
        if not raw:
            return None
        now = datetime.now()
        normalized = re.sub(r"\s+", " ", raw.replace("T", " ").replace("Z", "")).strip()
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
            try:
                return datetime.strptime(normalized, fmt)
            except Exception:
                pass
        rules = [
            (r"(\d+)\s*秒前", "seconds"),
            (r"(\d+)\s*分钟前", "minutes"),
            (r"(\d+)\s*小时前", "hours"),
            (r"(\d+)\s*天前", "days"),
        ]
        for pattern, unit in rules:
            match = re.search(pattern, normalized)
            if match:
                return now - timedelta(**{unit: int(match.group(1))})
        if "刚刚" in normalized:
            return now
        if "昨天" in normalized:
            return now - timedelta(days=1)
        if "前天" in normalized:
            return now - timedelta(days=2)
        return None

    @staticmethod
    def _torrent_to_magnet(
            data: bytes,
            fallback_name: str = "",
            exact_source: str = "") -> str:
        if not data:
            return ""
        try:
            parsed, info_start, info_end = Dyg55Indexer._bdecode_with_info_range(data)
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
                xl = Dyg55Indexer._extract_torrent_total_length(info)
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
            if tracker and tracker not in seen:
                seen.add(tracker)
                magnet += f"&tr={quote(tracker, safe=':/?&=')}"

        ws_seen = set()
        for ws in ws_list:
            web_seed = str(ws or "").strip()
            if web_seed and web_seed not in ws_seen:
                ws_seen.add(web_seed)
                magnet += f"&ws={quote(web_seed, safe=':/?&=')}"
        return magnet

    @classmethod
    def _extract_torrent_total_length_from_bytes(cls, data: bytes) -> int:
        if not data:
            return 0
        try:
            parsed, _, _ = cls._bdecode_with_info_range(data)
        except Exception:
            return 0
        if not isinstance(parsed, dict):
            return 0
        info = parsed.get(b"info")
        if not isinstance(info, dict):
            return 0
        return cls._extract_torrent_total_length(info)

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
                if isinstance(item, dict):
                    file_length = item.get(b"length")
                    if isinstance(file_length, int) and file_length > 0:
                        total += file_length
        return total

    @staticmethod
    def _build_unique_result_page_url(
            detail_url: str,
            enclosure: str,
            result_title: str = "",
            size_bytes: int = 0) -> str:
        """
        MoviePilot 卡片视图使用 torrent_info.page_url 作为 Vue key。
        电影港同一详情页可能返回多条资源，若共用同一个详情页 URL，切页返回时容易触发前端 key 复用异常。
        这里追加仅前端可见的 fragment，保证每条结果 key 稳定且唯一。
        """
        base = str(detail_url or "").strip()
        if not base:
            return ""
        seed_text = "|".join([
            base,
            str(enclosure or "").strip(),
            str(result_title or "").strip(),
            str(size_bytes or 0),
        ])
        suffix = hashlib.sha1(seed_text.encode("utf-8")).hexdigest()[:12]
        return f"{base}#dyg55-{suffix}"

    @staticmethod
    def _append_invisible_unique_marker(description: str, seed: str) -> str:
        """
        MoviePilot 搜索链路会按 site_name + title + description 去重。
        这里追加不可见零宽标识，让电影港结果在不影响界面显示的前提下保持唯一。
        """
        base = str(description or "").strip()
        token = str(seed or "").strip()
        if not token:
            return base
        digest = hashlib.sha1(token.encode("utf-8")).hexdigest()[:10]
        bits = bin(int(digest, 16))[2:].zfill(len(digest) * 4)
        marker = "\u2063" + "".join("\u200b" if bit == "0" else "\u200c" for bit in bits)
        if marker in base:
            return base
        return f"{base}{marker}"

    @staticmethod
    def _bdecode_with_info_range(data: bytes) -> Tuple[Any, int, int]:
        info_start = -1
        info_end = -1

        def parse(idx: int) -> Tuple[Any, int]:
            nonlocal info_start, info_end
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
                    value_start = idx
                    value, idx = parse(idx)
                    obj[bytes(key)] = value
                    if bytes(key) == b"info" and info_start < 0:
                        info_start = value_start
                        info_end = idx
                return obj, idx + 1
            colon = data.index(b":", idx)
            length = int(data[idx:colon])
            start = colon + 1
            end = start + length
            return data[start:end], end

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
    def _extract_tmdb_actor_names(tmdb_info: Dict[str, Any]) -> List[str]:
        results: List[str] = []
        credits = tmdb_info.get("credits") or {}
        cast_items = credits.get("cast") or tmdb_info.get("actors") or []
        for item in cast_items[:12]:
            if isinstance(item, dict):
                name = str(item.get("name") or item.get("original_name") or "").strip()
            else:
                name = str(item or "").strip()
            if name:
                results.append(name)
        return Dyg55Indexer._unique_nonempty(results)

    @staticmethod
    def _normalize_profile_mtype(mtype: MediaType = None):
        if not mtype:
            return None
        raw = str(mtype).strip().lower()
        if raw.endswith(".tv") or raw == "tv" or "电视剧" in raw:
            return MediaType.TV
        if raw.endswith(".movie") or raw == "movie" or "电影" in raw:
            return MediaType.MOVIE
        return mtype

    @staticmethod
    def _extract_year_token(value: Any) -> str:
        match = re.search(r"(19|20)\d{2}", str(value or ""))
        return match.group(0) if match else ""

    @staticmethod
    def _normalize_imdb_id(value: Any) -> str:
        match = re.search(r"(tt\d{5,})", str(value or "").strip(), re.IGNORECASE)
        return match.group(1).lower() if match else ""

    @staticmethod
    def _unique_nonempty(items: List[Any]) -> List[str]:
        results: List[str] = []
        seen = set()
        for item in items or []:
            text = str(item or "").strip()
            if not text or text in seen:
                continue
            seen.add(text)
            results.append(text)
        return results

    @staticmethod
    def _to_int(value: Any) -> int:
        try:
            return int(str(value or "").strip())
        except Exception:
            return 0

    def _match_target_site(self, site: dict) -> bool:
        site_id = str(site.get("id") or "").strip().lower()
        if site_id in ("dyg55", "dyg"):
            return True
        all_hosts = self._all_hosts()
        for candidate in [site.get("domain"), site.get("url")]:
            host = self._extract_host(candidate)
            if host and self._is_host_match(host, all_hosts):
                return True
        return False

    def _resolve_base_url(self, site: dict) -> str:
        candidates = self._build_base_url_candidates(site=site)
        return candidates[0] if candidates else self._default_base_url

    def _all_hosts(self) -> set:
        hosts = {self._default_host, "www.dyg55.com"}
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
            if host and host not in seen:
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
            site.get("url") if isinstance(site, dict) else "",
            site.get("domain") if isinstance(site, dict) else "",
            self._default_base_url,
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
                logger.debug(f"电影港(dyg55)索引器注册失败：域名={host}，错误={err}")
        logger.info(f"电影港(dyg55)索引器注册完成：域名列表={', '.join(hosts)}")

    @staticmethod
    def _build_indexer_schema(all_hosts: List[str]) -> Dict[str, Any]:
        primary = "www.dyg55.com" if "www.dyg55.com" in all_hosts else all_hosts[0]
        ext_domains = [f"https://{h}/" for h in all_hosts if h != primary]
        return {
            "id": "dyg55",
            "name": "电影港",
            "domain": f"https://{primary}/",
            "ext_domains": ext_domains,
            "encoding": "UTF-8",
            "public": True,
            "proxy": True,
            "result_num": 100,
            "timeout": 30,
            "search": {"paths": [{"path": "search.php", "method": "get"}]},
            "torrents": {
                "list": {"selector": "li.__never_match__"},
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
        text = str(raw or "").strip().lower()
        if not text:
            return ""
        if "://" not in text:
            text = f"https://{text}"
        try:
            return (urlparse(text).hostname or "").lower()
        except Exception:
            return ""

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
        return f"{parsed.scheme or 'https'}://{parsed.netloc}/"

    @staticmethod
    def _is_host_match(host: str, allowed_hosts: set) -> bool:
        pure = host.lower().lstrip(".")
        pure_no_www = pure[4:] if pure.startswith("www.") else pure
        for allowed in allowed_hosts:
            candidate = allowed.lower().lstrip(".")
            candidate_no_www = candidate[4:] if candidate.startswith("www.") else candidate
            if pure == candidate or pure_no_www == candidate_no_www:
                return True
        return False
