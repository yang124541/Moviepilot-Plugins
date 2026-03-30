import hashlib
import re
import random
from collections import deque
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import datetime, timedelta
from html import unescape
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote, urljoin, urlparse

import requests as _requests
from fastapi.concurrency import run_in_threadpool

from app.core.config import settings
from app.core.context import TorrentInfo
from app.helper.sites import SitesHelper
from app.log import logger
from app.plugins import _PluginBase
from app.schemas.types import MediaType


class BtbtlaIndexer(_PluginBase):
    plugin_name = "BT影视"
    plugin_desc = "为 btbtla.com 提供磁力搜索支持。"
    plugin_icon = "https://raw.githubusercontent.com/yang124541/Moviepilot-Plugins/main/btbtla.png"
    plugin_version = "1.1.1"
    plugin_author = "yang124541"
    author_url = "https://github.com/yang124541/moviepilot-plugin"
    plugin_config_prefix = "btbtlaindexer_"
    plugin_order = 33
    auth_level = 2

    _enabled = False
    _extra_hosts = ""
    _detail_concurrency = 5

    _default_host = "btbtla.com"
    _default_base_url = "https://www.btbtla.com/"
    _max_search_pages = 5
    _excluded_tab_labels = {"other", "夸克网盘"}

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
                                            "hint": "并发抓取影片详情页与 tdown 磁力页，允许范围 1-100",
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
                                            "placeholder": "www.btbtla.com",
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
                                            "text": "BT影视当前搜索链路为 /search/{关键词} -> /detail/{id}.html -> /tdown/{id}.html。"
                                                    "站点 URL 建议配置为 https://www.btbtla.com/",
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
        media_profile = self._resolve_moviepilot_media_profile(
            keyword=keyword,
            mtype=mtype,
        )
        first_actor = str(((media_profile.get("actors") or [""])[0]) or "").strip()
        logger.debug(
            f"BT影视(btbtla)主程序媒体信息：关键词='{keyword}'，"
            f"来源='{str(media_profile.get('source') or 'none').strip()}'，"
            f"类型='{str(media_profile.get('resolved_mtype') or '').strip() or 'unknown'}'，"
            f"tmdb_id={'有' if str(media_profile.get('tmdb_id') or '').strip() else '无'}，"
            f"imdb_id={'有' if str(media_profile.get('imdb_id') or '').strip() else '无'}，"
            f"主演={'有' if first_actor else '无'}，"
            f"第一主演='{first_actor}'"
        )

        logger.info(f"BT影视(btbtla)开始搜索：关键词='{keyword}'")

        session = _requests.Session()
        cookie_from_site = str(site.get("cookie") or "").strip()
        if cookie_from_site:
            session.headers.update({"Cookie": cookie_from_site})
        session.headers.update({
            "User-Agent": ua,
            "Referer": base_url,
            "Accept-Language": "zh-CN,zh;q=0.9",
        })

        try:
            search_items = self._search_videos(
                session=session,
                base_url=base_url,
                keyword=keyword,
                timeout=timeout,
                proxies=proxies,
                client_ip=self._rand_ip(),
            )
            search_video_count = len(search_items)
            if not search_items:
                cost = (datetime.now() - start_at).seconds
                logger.info(
                    f"BT影视(btbtla)搜索完成：关键词='{keyword}'，"
                    f"找到视频=0，返回磁力=0，耗时={cost}s"
                )
                return []

            detail_pages = self._fetch_detail_entries_concurrently(
                base_url=base_url,
                detail_items=search_items,
                session_headers=dict(session.headers),
                session_cookies=session.cookies.get_dict(),
                timeout=timeout,
                proxies=proxies,
            )
            selected_detail_pages = self._select_best_detail_pages(
                detail_pages=detail_pages,
                keyword=keyword,
                mtype=mtype,
                media_profile=media_profile,
            )
            download_items = self._flatten_detail_download_items(selected_detail_pages)
            if not download_items:
                cost = (datetime.now() - start_at).seconds
                logger.info(
                    f"BT影视(btbtla)搜索完成：关键词='{keyword}'，"
                    f"找到视频={search_video_count}，返回磁力=0，耗时={cost}s"
                )
                return []

            results = self._fetch_download_pages_concurrently(
                site=site,
                base_url=base_url,
                download_items=download_items,
                session_headers=dict(session.headers),
                session_cookies=session.cookies.get_dict(),
                timeout=timeout,
                proxies=proxies,
            )
            cost = (datetime.now() - start_at).seconds
            logger.info(
                f"BT影视(btbtla)搜索完成：关键词='{keyword}'，"
                f"找到视频={search_video_count}，返回磁力={len(results)}，耗时={cost}s"
            )
            return results
        except Exception as err:
            logger.error(f"BT影视(btbtla)搜索异常：关键词='{keyword}'，错误={err}")
            return []
        finally:
            try:
                session.close()
            except Exception:
                pass

    def _fetch_detail_entries_concurrently(
            self,
            base_url: str,
            detail_items: List[Dict[str, Any]],
            session_headers: Dict[str, str],
            session_cookies: Dict[str, str],
            timeout: int,
            proxies: Optional[Dict[str, str]]) -> List[Dict[str, Any]]:
        worker_count = min(
            len(detail_items),
            self._clamp_detail_concurrency(self._detail_concurrency),
        )
        if worker_count <= 1:
            results: List[Dict[str, Any]] = []
            for item in detail_items:
                detail_page = self._fetch_single_detail_entries(
                    base_url=base_url,
                    detail_item=item,
                    session_headers=session_headers,
                    session_cookies=session_cookies,
                    timeout=timeout,
                    proxies=proxies,
                )
                if detail_page:
                    results.append(detail_page)
            return results

        logger.debug(
            f"BT影视(btbtla)开始并发抓取影片详情："
            f"影片数={len(detail_items)}，并发数={worker_count}"
        )

        ordered_entries: Dict[int, Dict[str, Any]] = {}
        task_queue = deque((index, item) for index, item in enumerate(detail_items))
        with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="btbtla-detail") as executor:
            running_tasks: Dict[Any, Tuple[int, Dict[str, Any]]] = {}
            while task_queue or running_tasks:
                while task_queue and len(running_tasks) < worker_count:
                    index, item = task_queue.popleft()
                    future = executor.submit(
                        self._fetch_single_detail_entries,
                        base_url,
                        item,
                        session_headers,
                        session_cookies,
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
                        ordered_entries[index] = future.result() or {}
                    except Exception as err:
                        title = str((item or {}).get("title") or "").strip()
                        logger.debug(
                            f"BT影视(btbtla)并发抓取详情异常："
                            f"标题='{title}'，错误={err}"
                        )
                        ordered_entries[index] = {}

        results: List[Dict[str, Any]] = []
        for index in range(len(detail_items)):
            detail_page = ordered_entries.get(index)
            if detail_page:
                results.append(detail_page)
        return results

    def _fetch_single_detail_entries(
            self,
            base_url: str,
            detail_item: Dict[str, Any],
            session_headers: Dict[str, str],
            session_cookies: Dict[str, str],
            timeout: int,
            proxies: Optional[Dict[str, str]]) -> Dict[str, Any]:
        session = self._build_worker_session(
            session_headers=session_headers,
            session_cookies=session_cookies,
            client_ip=self._rand_ip(),
        )
        try:
            return self._fetch_detail_download_entries(
                session=session,
                base_url=base_url,
                detail_item=detail_item,
                timeout=timeout,
                proxies=proxies,
            )
        finally:
            try:
                session.close()
            except Exception:
                pass

    def _fetch_detail_download_entries(
            self,
            session: _requests.Session,
            base_url: str,
            detail_item: Dict[str, Any],
            timeout: int,
            proxies: Optional[Dict[str, str]]) -> Dict[str, Any]:
        detail_rel = str(detail_item.get("detail_url") or "").strip()
        if not detail_rel:
            return {}
        detail_url = urljoin(base_url, detail_rel)

        try:
            resp = session.get(
                detail_url,
                timeout=timeout,
                proxies=proxies,
                allow_redirects=True,
            )
        except Exception as err:
            logger.debug(f"BT影视(btbtla)获取详情失败：url={detail_url}，错误={err}")
            return {}
        if not resp.ok:
            return {}

        html = resp.text
        video_title = self._extract_first_match(
            html,
            r'<h1 class="page-title">\s*(.*?)\s*</h1>',
        ) or str(detail_item.get("title") or "").strip()
        plot = self._extract_first_match(
            html,
            r'<span class="video-info-itemtitle">剧情：</span>\s*'
            r'<div class="video-info-item video-info-content vod_content">\s*<span>(.*?)</span>',
        ) or str(detail_item.get("description") or "").strip()
        year = self._extract_first_match(
            html,
            r'<div class="video-info-aux[^"]*"[^>]*>.*?<a class="tag-link" href="/">\s*((?:19|20)\d{2})\s*</a>',
        ) or str(detail_item.get("year") or "").strip()
        aliases = self._extract_detail_aliases(html)
        actors = self._extract_detail_actors(html)

        download_rows = self._parse_download_rows(html)
        cat = str(detail_item.get("cat") or "").strip()
        area = str(detail_item.get("area") or "").strip()
        results: List[Dict[str, Any]] = []
        for row in download_rows:
            tdown_rel = str(row.get("tdown_url") or "").strip()
            if not tdown_rel:
                continue
            results.append({
                "detail_url": detail_url,
                "tdown_url": tdown_rel,
                "title": video_title,
                "plot": plot,
                "year": year,
                "cat": cat,
                "area": area,
                "filename": str(row.get("filename") or "").strip(),
                "size_text": str(row.get("size_text") or "").strip(),
                "download_count": row.get("download_count") or 0,
            })
        return {
            "detail_url": detail_url,
            "title": video_title,
            "search_title": str(detail_item.get("title") or "").strip(),
            "plot": plot,
            "year": year,
            "cat": cat,
            "area": area,
            "aliases": aliases,
            "actors": actors,
            "download_items": results,
        }

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
            from app.core.metainfo import MetaInfo
        except Exception as err:
            logger.debug(f"BT影视(btbtla)加载主程序 MetaInfo 失败：{err}")
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
                profile["source"] = "cache"
                if profile["title"]:
                    profile["names"] = self._unique_nonempty([profile["title"]])
        except Exception as err:
            logger.debug(f"BT影视(btbtla)读取 TMDB 缓存失败：{err}")

        try:
            from app.modules.themoviedb.tmdbapi import TmdbApi
        except Exception as err:
            logger.debug(f"BT影视(btbtla)加载 TmdbApi 失败：{err}")
            return profile

        api = None
        try:
            api = TmdbApi(language=settings.TMDB_LOCALE)
        except Exception:
            try:
                api = TmdbApi()
            except Exception as err:
                logger.debug(f"BT影视(btbtla)初始化 TmdbApi 失败：{err}")
                return profile

        try:
            normalized_mtype = self._normalize_profile_mtype(meta.type or mtype)
            matched_tmdb_info: Dict[str, Any] = {}
            if hasattr(api, "match"):
                logger.debug(
                    f"BT影视(btbtla)主程序媒体识别：先执行TMDB match，"
                    f"关键词='{str(meta.name or keyword_text).strip()}'，"
                    f"类型='{normalized_mtype or 'unknown'}'，"
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
                profile["resolved_mtype"] = str(matched_mtype or normalized_mtype or "").strip()
                if matched_tmdbid > 0:
                    logger.debug(
                        f"BT影视(btbtla)主程序媒体识别：TMDB match命中，"
                        f"id={matched_tmdbid}，"
                        f"类型='{profile['resolved_mtype'] or 'unknown'}'，"
                        f"标题='{str(matched_tmdb_info.get('title') or matched_tmdb_info.get('name') or '').strip()}'"
                    )
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
                profile["imdb_id"] = str(external_ids.get("imdb_id") or "").strip()
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
            logger.debug(f"BT影视(btbtla)识别主程序媒体信息失败：{err}")
        finally:
            try:
                if api and hasattr(api, "close"):
                    api.close()
            except Exception:
                pass

        return profile

    def _select_best_detail_pages(
            self,
            detail_pages: List[Dict[str, Any]],
            keyword: str,
            mtype: MediaType = None,
            media_profile: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        if not detail_pages:
            return []
        if len(detail_pages) <= 1:
            return detail_pages

        scored_pages: List[Tuple[int, int, Dict[str, Any]]] = []
        for index, detail_page in enumerate(detail_pages):
            scored_pages.append((
                self._score_detail_page(
                    detail_page=detail_page,
                    keyword=keyword,
                    mtype=mtype,
                    media_profile=media_profile or {},
                ),
                index,
                detail_page,
            ))
        scored_pages.sort(key=lambda item: (item[0], -item[1]), reverse=True)
        if not scored_pages:
            return []

        top_score = scored_pages[0][0]
        threshold = top_score
        if top_score >= 1000:
            threshold = top_score - 10
        elif top_score >= 800:
            threshold = top_score - 30
        elif top_score >= 500:
            threshold = top_score - 50

        selected = [item[2] for item in scored_pages if item[0] >= threshold and item[0] > 0]
        if not selected:
            selected = [scored_pages[0][2]]
        selected = selected[:2] if len(selected) > 2 else selected

        logger.debug(
            f"BT影视(btbtla)详情页预筛选：候选数={len(detail_pages)}，"
            f"选中数={len(selected)}，最高分={top_score}"
        )
        return selected

    @staticmethod
    def _flatten_detail_download_items(detail_pages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        for detail_page in detail_pages or []:
            items.extend(detail_page.get("download_items") or [])
        return items

    def _score_detail_page(
            self,
            detail_page: Dict[str, Any],
            keyword: str,
            mtype: MediaType = None,
            media_profile: Optional[Dict[str, Any]] = None) -> int:
        profile = media_profile or {}
        profile_names = self._unique_nonempty(
            list(profile.get("names") or [])
            + [profile.get("title"), keyword]
        )
        profile_name_norms = [
            self._normalize_match_text(name)
            for name in profile_names
            if self._normalize_match_text(name)
        ]
        page_names = self._detail_page_name_candidates(detail_page)
        page_name_norms = [
            self._normalize_match_text(name)
            for name in page_names
            if self._normalize_match_text(name)
        ]

        score = 0
        if not detail_page.get("download_items"):
            score -= 120

        for page_name_norm in page_name_norms:
            for profile_name_norm in profile_name_norms:
                if not profile_name_norm:
                    continue
                if page_name_norm == profile_name_norm:
                    score = max(score, 1000)
                elif page_name_norm.startswith(profile_name_norm) or profile_name_norm.startswith(page_name_norm):
                    score = max(score, 840)
                elif profile_name_norm in page_name_norm or page_name_norm in profile_name_norm:
                    score = max(score, 680)

        detail_year = self._extract_year_token(detail_page.get("year"))
        profile_year = self._extract_year_token(profile.get("year") or keyword)
        if detail_year and profile_year:
            if detail_year == profile_year:
                score += 120
            else:
                score -= 180

        actor_score = self._score_actor_match(
            detail_actors=detail_page.get("actors") or [],
            profile_actors=profile.get("actors") or [],
        )
        score += actor_score

        if self._looks_like_non_target_entry(detail_page=detail_page, keyword=keyword, mtype=mtype):
            score -= 220
        return score

    def _score_actor_match(self, detail_actors: List[str], profile_actors: List[str]) -> int:
        if not detail_actors or not profile_actors:
            return 0
        first_actor = str(profile_actors[0] or "").strip()
        first_actor_norm = self._normalize_match_text(first_actor)
        if not first_actor_norm:
            return 0
        detail_norms = {
            self._normalize_match_text(name)
            for name in detail_actors
            if self._normalize_match_text(name)
        }
        if first_actor_norm not in detail_norms:
            return 0
        return 220

    def _looks_like_non_target_entry(
            self,
            detail_page: Dict[str, Any],
            keyword: str,
            mtype: MediaType = None) -> bool:
        names = " ".join(self._detail_page_name_candidates(detail_page))
        raw = str(names or "").lower()
        if not raw:
            return False
        keyword_norm = self._normalize_match_text(keyword)
        names_norm = self._normalize_match_text(names)
        if keyword_norm and keyword_norm == names_norm:
            return False

        if any(token in raw for token in ("前传", "外传", "番外", "特别篇", "剧场版", "纪录片")):
            return True
        if self._is_movie_media_type(mtype):
            return self._looks_like_tv_season_text(raw)
        return False

    def _detail_page_name_candidates(self, detail_page: Dict[str, Any]) -> List[str]:
        return self._unique_nonempty(
            [
                detail_page.get("title"),
                detail_page.get("search_title"),
            ] + list(detail_page.get("aliases") or [])
        )

    @staticmethod
    def _extract_detail_aliases(html: str) -> List[str]:
        for label in ("别名：", "别名", "又名：", "又名", "译名：", "译名"):
            value = BtbtlaIndexer._extract_video_info_value(html=html, label=label)
            if value:
                return BtbtlaIndexer._split_info_items(value)
        return []

    @staticmethod
    def _extract_detail_actors(html: str) -> List[str]:
        for label in ("主演：", "主演", "演员：", "演员"):
            value = BtbtlaIndexer._extract_video_info_value(html=html, label=label)
            if value:
                return BtbtlaIndexer._split_info_items(value)
        return []

    @staticmethod
    def _extract_video_info_value(html: str, label: str) -> str:
        pattern = (
            r'<span class="video-info-itemtitle">\s*%s\s*</span>\s*'
            r'<div class="video-info-item(?:\s+video-info-content)?[^"]*">\s*(.*?)\s*</div>'
        ) % re.escape(str(label or "").strip())
        return BtbtlaIndexer._extract_first_match(html, pattern)

    @staticmethod
    def _split_info_items(text: str) -> List[str]:
        raw = BtbtlaIndexer._clean_html_text(text)
        if not raw:
            return []
        parts = re.split(r"[|/／,，、;；]+", raw)
        return BtbtlaIndexer._unique_nonempty(parts)

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
        return BtbtlaIndexer._unique_nonempty(results)

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
    def _normalize_match_text(text: Any) -> str:
        raw = str(text or "").strip().lower()
        if not raw:
            return ""
        return re.sub(r"[^0-9a-z\u4e00-\u9fff]+", "", raw)

    @staticmethod
    def _extract_year_token(value: Any) -> str:
        match = re.search(r"(19|20)\d{2}", str(value or ""))
        return match.group(0) if match else ""

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
    def _looks_like_tv_season_text(text: str) -> bool:
        raw = str(text or "").lower()
        if not raw:
            return False
        return bool(
            re.search(r"第[一二三四五六七八九十百\d]+季", raw)
            or re.search(r"\bs\d{1,2}\b", raw)
            or re.search(r"\bseason\s*\d{1,2}\b", raw)
            or re.search(r"\bpart\s*\d{1,2}\b", raw)
            or ("第二季" in raw)
            or ("第三季" in raw)
            or ("第四季" in raw)
        )

    @staticmethod
    def _is_movie_media_type(mtype: MediaType = None) -> bool:
        if not mtype:
            return False
        raw = str(mtype).strip().lower()
        return raw.endswith(".movie") or raw == "movie" or "电影" in raw

    def _fetch_download_pages_concurrently(
            self,
            site: dict,
            base_url: str,
            download_items: List[Dict[str, Any]],
            session_headers: Dict[str, str],
            session_cookies: Dict[str, str],
            timeout: int,
            proxies: Optional[Dict[str, str]]) -> List[TorrentInfo]:
        worker_count = min(
            len(download_items),
            self._clamp_detail_concurrency(self._detail_concurrency),
        )
        if worker_count <= 1:
            results: List[TorrentInfo] = []
            for item in download_items:
                torrent = self._fetch_single_download_result(
                    site=site,
                    base_url=base_url,
                    download_item=item,
                    session_headers=session_headers,
                    session_cookies=session_cookies,
                    timeout=timeout,
                    proxies=proxies,
                )
                if torrent:
                    results.append(torrent)
            return results

        logger.debug(
            f"BT影视(btbtla)开始并发抓取磁力页："
            f"资源数={len(download_items)}，并发数={worker_count}"
        )

        ordered_results: Dict[int, Optional[TorrentInfo]] = {}
        task_queue = deque((index, item) for index, item in enumerate(download_items))
        with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="btbtla-tdown") as executor:
            running_tasks: Dict[Any, Tuple[int, Dict[str, Any]]] = {}
            while task_queue or running_tasks:
                while task_queue and len(running_tasks) < worker_count:
                    index, item = task_queue.popleft()
                    future = executor.submit(
                        self._fetch_single_download_result,
                        site,
                        base_url,
                        item,
                        session_headers,
                        session_cookies,
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
                        ordered_results[index] = future.result()
                    except Exception as err:
                        title = str((item or {}).get("filename") or (item or {}).get("title") or "").strip()
                        logger.debug(
                            f"BT影视(btbtla)并发抓取磁力页异常："
                            f"标题='{title}'，错误={err}"
                        )
                        ordered_results[index] = None

        results: List[TorrentInfo] = []
        for index in range(len(download_items)):
            item = ordered_results.get(index)
            if item:
                results.append(item)
        return results

    def _fetch_single_download_result(
            self,
            site: dict,
            base_url: str,
            download_item: Dict[str, Any],
            session_headers: Dict[str, str],
            session_cookies: Dict[str, str],
            timeout: int,
            proxies: Optional[Dict[str, str]]) -> Optional[TorrentInfo]:
        session = self._build_worker_session(
            session_headers=session_headers,
            session_cookies=session_cookies,
            client_ip=self._rand_ip(),
        )
        try:
            return self._fetch_download_page(
                session=session,
                site=site,
                base_url=base_url,
                download_item=download_item,
                timeout=timeout,
                proxies=proxies,
            )
        finally:
            try:
                session.close()
            except Exception:
                pass

    def _fetch_download_page(
            self,
            session: _requests.Session,
            site: dict,
            base_url: str,
            download_item: Dict[str, Any],
            timeout: int,
            proxies: Optional[Dict[str, str]]) -> Optional[TorrentInfo]:
        tdown_rel = str(download_item.get("tdown_url") or "").strip()
        if not tdown_rel:
            return None
        tdown_url = urljoin(base_url, tdown_rel)

        try:
            resp = session.get(
                tdown_url,
                timeout=timeout,
                proxies=proxies,
                allow_redirects=True,
            )
        except Exception as err:
            logger.debug(f"BT影视(btbtla)获取磁力页失败：url={tdown_url}，错误={err}")
            return None
        if not resp.ok:
            return None

        html = resp.text
        magnet = unescape(
            self._extract_first_match(
                html,
                r'href="(magnet:\?[^"]+)"',
            )
        ).strip()
        info_hash = self._extract_first_match(
            html,
            r'<span class="video-info-itemtitle">Hash:</span>\s*'
            r'<div class="video-info-item"><span class="slash">/</span>\s*([0-9a-fA-F]{40})\s*</div>',
        ).strip().lower()
        if not magnet and info_hash:
            magnet = f"magnet:?xt=urn:btih:{info_hash}"
        if not magnet:
            return None

        video_title = self._extract_first_match(
            html,
            r'<h1 class="page-title">\s*(.*?)\s*</h1>',
        ) or str(download_item.get("title") or "").strip()
        filename = self._extract_first_match(
            html,
            r'<span class="video-info-itemtitle">种子片名:</span>\s*'
            r'<div class="video-info-item"><span class="slash">/</span>\s*(.*?)\s*</div>',
        ) or str(download_item.get("filename") or "").strip()
        size_text = self._extract_first_match(
            html,
            r'<span class="video-info-itemtitle">影片大小:</span>\s*'
            r'<div class="video-info-item">\s*(.*?)\s*</div>',
        ) or str(download_item.get("size_text") or "").strip()
        pubdate_text = self._extract_first_match(
            html,
            r'<span class="video-info-itemtitle">种子时间:</span>\s*'
            r'<div class="video-info-item">\s*(.*?)\s*</div>',
        )

        title = self._build_match_title(
            title=filename or video_title,
            parent_title=video_title,
            year=str(download_item.get("year") or "").strip(),
        )
        description_parts = [
            filename or video_title,
            str(download_item.get("cat") or "").strip(),
            str(download_item.get("area") or "").strip(),
        ]
        description = " | ".join([part for part in description_parts if part])
        unique_page_url = self._build_unique_result_page_url(
            page_url=tdown_url,
            enclosure=magnet,
            result_title=title or filename or video_title,
            size_text=size_text,
        )
        return TorrentInfo(
            site=site.get("id"),
            site_name=site.get("name"),
            site_cookie=site.get("cookie"),
            site_ua=site.get("ua"),
            site_proxy=site.get("proxy"),
            site_order=site.get("pri"),
            site_downloader=site.get("downloader"),
            title=title or filename or video_title,
            description=description,
            enclosure=magnet,
            page_url=unique_page_url,
            size=self._parse_size_bytes(size_text, filename),
            seeders=0,
            peers=0,
            grabs=self._to_int(download_item.get("download_count")),
            pubdate=self._parse_pubdate_text(pubdate_text),
            date_elapsed=pubdate_text,
            downloadvolumefactor=0,
            uploadvolumefactor=1,
        )

    def _search_videos(self, session: _requests.Session, base_url: str,
                       keyword: str, timeout: int,
                       proxies: Optional[Dict[str, str]],
                       client_ip: str = "") -> List[Dict[str, Any]]:
        all_items: List[Dict[str, Any]] = []
        seen_detail_urls = set()
        encoded_keyword = quote(str(keyword or "").strip(), safe="")

        for page_no in range(1, self._max_search_pages + 1):
            if page_no == 1:
                search_url = urljoin(base_url, f"search/{encoded_keyword}")
            else:
                search_url = urljoin(base_url, f"search/{encoded_keyword}/page/{page_no}")

            headers = dict(session.headers)
            if client_ip:
                headers["X-Forwarded-For"] = client_ip
                headers["X-Real-IP"] = client_ip

            try:
                resp = session.get(
                    search_url,
                    timeout=timeout,
                    proxies=proxies,
                    allow_redirects=True,
                    headers=headers,
                )
            except Exception as err:
                logger.debug(f"BT影视(btbtla)搜索页请求异常：page={page_no}，错误={err}")
                break
            if not resp.ok:
                logger.debug(f"BT影视(btbtla)搜索页请求失败：page={page_no}，status={resp.status_code}")
                break

            page_items = self._parse_search_results(resp.text)
            if not page_items:
                break

            added = 0
            for item in page_items:
                detail_url = str(item.get("detail_url") or "").strip()
                if not detail_url or detail_url in seen_detail_urls:
                    continue
                seen_detail_urls.add(detail_url)
                all_items.append(item)
                added += 1

            if added == 0:
                break

        return all_items

    @staticmethod
    def _parse_search_results(html: str) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        seen = set()
        pattern = re.compile(
            r'<div class="module-item">\s*'
            r'<div class="module-item-cover">.*?'
            r'<a href="(?P<detail>/detail/\d+\.html)" title="(?P<title>[^"]+)">.*?'
            r'<div class="module-item-caption">\s*(?P<caption>.*?)</div>.*?'
            r'<div class="module-item-content">.*?'
            r'<div class="module-item-style video-text">(?P<desc>.*?)</div>',
            re.IGNORECASE | re.DOTALL,
        )
        for match in pattern.finditer(html):
            detail_url = str(match.group("detail") or "").strip()
            if not detail_url or detail_url in seen:
                continue
            seen.add(detail_url)

            caption = str(match.group("caption") or "")
            spans = re.findall(r'<span[^>]*>(.*?)</span>', caption, flags=re.IGNORECASE | re.DOTALL)
            cleaned_spans = [BtbtlaIndexer._clean_html_text(span) for span in spans if BtbtlaIndexer._clean_html_text(span)]
            year = cleaned_spans[0] if len(cleaned_spans) > 0 else ""
            cat = cleaned_spans[1] if len(cleaned_spans) > 1 else ""
            area = cleaned_spans[2] if len(cleaned_spans) > 2 else ""

            items.append({
                "detail_url": detail_url,
                "title": BtbtlaIndexer._clean_html_text(match.group("title")),
                "description": BtbtlaIndexer._clean_html_text(match.group("desc")),
                "year": year,
                "cat": cat,
                "area": area,
            })
        return items

    @staticmethod
    def _parse_download_rows(html: str) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        seen = set()
        tab_labels = [
            BtbtlaIndexer._clean_html_text(match.group(1))
            for match in re.finditer(
                r'<div class="module-tab-item downtab-item[^"]*">\s*'
                r'<span[^>]*>(.*?)</span><small>\d+</small>',
                html,
                re.IGNORECASE | re.DOTALL,
            )
        ]
        block_pattern = re.compile(
            r'<div class="module-list module-player-list sort-list module-downlist[^"]*">'
            r'(?P<block>.*?)(?=(?:<div class="module-list module-player-list sort-list module-downlist[^"]*">)'
            r'|(?:<script type="text/javascript">))',
            re.IGNORECASE | re.DOTALL,
        )
        blocks = [match.group("block") or "" for match in block_pattern.finditer(html)]
        row_pattern = re.compile(
            r'<a class="module-row-text[^"]*" href="(?P<tdown>/tdown/\d+\.html)" title="(?P<title_attr>[^"]*)">'
            r'(?P<body>.*?)</a>\s*<div class="module-row-shortcuts">(?P<shortcuts>.*?)</div>',
            re.IGNORECASE | re.DOTALL,
        )

        if tab_labels and blocks:
            parse_units = list(zip(tab_labels, blocks))
        else:
            parse_units = [("", html)]

        for tab_label, block_html in parse_units:
            if BtbtlaIndexer._is_excluded_download_tab(tab_label):
                continue
            for match in row_pattern.finditer(block_html):
                tdown_url = str(match.group("tdown") or "").strip()
                if not tdown_url or tdown_url in seen:
                    continue
                seen.add(tdown_url)

                body = str(match.group("body") or "")
                h4_match = re.search(r'<h4>(.*?)</h4>', body, flags=re.IGNORECASE | re.DOTALL)
                h4_html = str(h4_match.group(1) or "") if h4_match else ""
                size_match = re.search(r'<span>\s*\[(.*?)\]\s*</span>', h4_html, flags=re.IGNORECASE | re.DOTALL)
                size_text = BtbtlaIndexer._clean_html_text(size_match.group(1)) if size_match else ""
                filename_html = re.sub(r'<span>\s*\[.*?\]\s*</span>', ' ', h4_html, flags=re.IGNORECASE | re.DOTALL)
                filename = BtbtlaIndexer._clean_html_text(filename_html)
                if not filename:
                    title_attr = BtbtlaIndexer._clean_html_text(match.group("title_attr"))
                    filename = BtbtlaIndexer._strip_video_prefix_from_title_attr(title_attr)
                if not filename:
                    continue

                download_count = 0
                shortcuts = str(match.group("shortcuts") or "")
                count_match = re.search(r'<span>\s*(\d+)\s*</span>', shortcuts, re.IGNORECASE | re.DOTALL)
                if count_match:
                    try:
                        download_count = int(count_match.group(1))
                    except Exception:
                        download_count = 0

                items.append({
                    "tdown_url": tdown_url,
                    "filename": filename,
                    "size_text": BtbtlaIndexer._clean_html_text(size_text),
                    "download_count": download_count,
                    "tab_label": tab_label,
                })
        return items

    @staticmethod
    def _is_excluded_download_tab(label: Any) -> bool:
        text = str(label or "").strip().lower()
        if not text:
            return False
        if text in BtbtlaIndexer._excluded_tab_labels:
            return True
        return "夸克" in text

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
    def _strip_video_prefix_from_title_attr(text: str) -> str:
        raw = str(text or "").strip()
        if not raw:
            return ""
        cleaned = re.sub(r'^《[^》]+》', '', raw).strip()
        cleaned = re.sub(r'\s+\d+(?:\.\d+)?\s*(?:TB|GB|MB|KB)\.torrent$', '', cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r'\.torrent$', '', cleaned, flags=re.IGNORECASE)
        return cleaned.strip()

    @staticmethod
    def _extract_first_match(text: Any, pattern: str) -> str:
        raw = str(text or "")
        if not raw:
            return ""
        match = re.search(pattern, raw, flags=re.IGNORECASE | re.DOTALL)
        if not match:
            return ""
        return BtbtlaIndexer._clean_html_text(match.group(1))

    @staticmethod
    def _build_match_title(title: str, parent_title: str = "", year: str = "") -> str:
        value = str(title or "").strip()
        parent = str(parent_title or "").strip()
        normalized_value = value.lower()
        normalized_parent = parent.lower()
        if parent and normalized_parent not in normalized_value:
            value = f"{parent} {value}".strip()
        if year and re.match(r"^(19|20)\d{2}$", year) and not re.search(r"(19|20)\d{2}", value):
            value = f"{value} {year}".strip()
        return value

    @staticmethod
    def _build_unique_result_page_url(
            page_url: str,
            enclosure: str,
            result_title: str = "",
            size_text: str = "") -> str:
        """
        MoviePilot 卡片视图使用 torrent_info.page_url 作为 Vue key。
        BT影视结果切页返回时若复用相同或不稳定的 page_url，前端容易出现结果复用异常。
        这里追加仅前端可见的 fragment，保持原链接可打开的同时，让每条结果 key 稳定且唯一。
        """
        base = str(page_url or "").strip()
        if not base:
            return ""
        seed_text = "|".join([
            base,
            str(enclosure or "").strip(),
            str(result_title or "").strip(),
            str(size_text or "").strip(),
        ])
        suffix = hashlib.sha1(seed_text.encode("utf-8")).hexdigest()[:12]
        return f"{base}#btbtla-{suffix}"

    @staticmethod
    def _parse_pubdate_text(text: str) -> Optional[datetime]:
        raw = str(text or "").strip()
        if not raw:
            return None

        normalized = raw.replace("T", " ").replace("Z", "")
        normalized = re.sub(r"\s+", " ", normalized).strip()
        absolute_formats = [
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%Y/%m/%d %H:%M:%S",
            "%Y/%m/%d %H:%M",
            "%Y-%m-%d",
            "%Y/%m/%d",
        ]
        for fmt in absolute_formats:
            try:
                return datetime.strptime(normalized, fmt)
            except Exception:
                pass

        now = datetime.now()
        relative_rules = [
            (r"(\d+)\s*秒前", "seconds"),
            (r"(\d+)\s*分钟前", "minutes"),
            (r"(\d+)\s*小时前", "hours"),
            (r"(\d+)\s*天前", "days"),
        ]
        for pattern, unit in relative_rules:
            match = re.search(pattern, normalized)
            if not match:
                continue
            amount = int(match.group(1))
            return now - timedelta(**{unit: amount})

        if "刚刚" in normalized:
            return now
        if "昨天" in normalized:
            return now - timedelta(days=1)
        return None

    @staticmethod
    def _parse_size_bytes(*size_texts: str) -> int:
        for raw in size_texts:
            text = str(raw or "").strip()
            if not text:
                continue
            match = re.search(
                r'(\d+(?:\.\d+)?)\s*(TB|GB|MB|KB|T|G|M|K)\b',
                text,
                re.IGNORECASE,
            )
            if not match:
                continue
            value = float(match.group(1))
            unit = match.group(2).upper()
            factor = {
                "TB": 1 << 40,
                "T": 1 << 40,
                "GB": 1 << 30,
                "G": 1 << 30,
                "MB": 1 << 20,
                "M": 1 << 20,
                "KB": 1 << 10,
                "K": 1 << 10,
            }.get(unit, 0)
            if factor > 0:
                return int(value * factor)
        return 0

    @staticmethod
    def _to_int(value: Any) -> int:
        if value is None:
            return 0
        if isinstance(value, int):
            return value
        text = str(value).strip().replace(",", "")
        if not text:
            return 0
        match = re.search(r"-?\d+", text)
        if not match:
            return 0
        try:
            return int(match.group(0))
        except Exception:
            return 0

    @staticmethod
    def _rand_ip() -> str:
        return f"{random.randint(1, 223)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(1, 254)}"

    @staticmethod
    def _clamp_detail_concurrency(value: Any) -> int:
        try:
            concurrency = int(value or 5)
        except Exception:
            concurrency = 5
        return max(1, min(concurrency, 100))

    def _match_target_site(self, site: dict) -> bool:
        site_id = str(site.get("id") or "").strip().lower()
        if site_id in ("btbtla", "btla", "btbtla.com"):
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
        hosts = {self._default_host, "www.btbtla.com"}
        for host in self._ordered_extra_hosts():
            hosts.add(host)
        return hosts

    def _registered_hosts(self) -> List[str]:
        ordered_extra_hosts = self._ordered_extra_hosts()
        if ordered_extra_hosts:
            return ordered_extra_hosts
        return sorted(self._all_hosts())

    def _ordered_extra_hosts(self) -> List[str]:
        results: List[str] = []
        seen = set()
        for line in (self._extra_hosts or "").splitlines():
            host = self._extract_host(line)
            if not host or host in seen:
                continue
            seen.add(host)
            results.append(host)
        return results

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
                logger.debug(f"BT影视(btbtla)索引器注册失败：域名={host}，错误={err}")
        logger.info(f"BT影视(btbtla)索引器注册完成：域名列表={', '.join(hosts)}")

    @staticmethod
    def _build_indexer_schema(all_hosts: List[str]) -> Dict[str, Any]:
        primary = "www.btbtla.com" if "www.btbtla.com" in all_hosts else all_hosts[0]
        ext_domains = [f"https://{host}/" for host in all_hosts if host != primary]
        return {
            "id": "btbtla",
            "name": "BT影视",
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
                        "path": "search/test",
                        "method": "get"
                    }
                ]
            },
            "torrents": {
                "list": {
                    "selector": "div.__never_match__"
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
            candidate = allowed.lower().lstrip(".")
            candidate_no_www = candidate[4:] if candidate.startswith("www.") else candidate
            if pure == candidate or pure_no_www == candidate_no_www:
                return True
        return False
