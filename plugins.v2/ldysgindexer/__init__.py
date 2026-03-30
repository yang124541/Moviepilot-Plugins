import json
import random
import re
import threading
from hashlib import sha1
from collections import deque
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import datetime, timedelta
from time import perf_counter
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

from fastapi.concurrency import run_in_threadpool

from app.core.config import settings
from app.core.context import TorrentInfo
from app.helper.sites import SitesHelper
from app.log import logger
from app.plugins import _PluginBase
from app.schemas.types import MediaType
from app.utils.http import RequestUtils


_CHROME_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
               "AppleWebKit/537.36 (KHTML, like Gecko) "
               "Chrome/120.0.0.0 Safari/537.36")


class LdysgIndexer(_PluginBase):
    plugin_name = "老电影（ldysg）"
    plugin_desc = "为 ldysg.com 提供老旧电影磁力搜索支持，自动识别验证码。"
    plugin_icon = "https://raw.githubusercontent.com/yang124541/Moviepilot-Plugins/main/ldysg.png?v=1.3.10"
    plugin_version = "1.3.10"
    plugin_author = "yang124541"
    author_url = "https://github.com/yang124541/moviepilot-plugin"
    plugin_config_prefix = "ldysgindexer_"
    plugin_order = 31
    auth_level = 2
    plugin_depend = ["ddddocr"]

    _enabled = False
    _extra_hosts = ""
    _detail_concurrency = 5
    _thread_local = threading.local()
    _shared_ocr = None
    _shared_ocr_init_lock = threading.Lock()
    _shared_ocr_predict_lock = threading.Lock()

    _default_host = "ldysg.com"
    _default_base_url = "https://www.ldysg.com/"

    def init_plugin(self, config: dict = None):
        self._install_ddddocr()

        if config:
            self._enabled = bool(config.get("enabled"))
            self._extra_hosts = (config.get("extra_hosts") or "").strip()
            self._detail_concurrency = self._clamp_detail_concurrency(
                config.get("detail_concurrency")
            )

        if self._enabled:
            self._register_builtin_indexer()

    @staticmethod
    def _install_ddddocr():
        try:
            import ddddocr  # noqa: F401
        except ImportError:
            import subprocess
            import sys
            logger.info("ldysg: 正在安装 ddddocr ...")
            subprocess.run(
                [sys.executable, "-m", "pip", "install", "ddddocr", "-q"],
                check=False,
            )

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
                                            "hint": "只并发抓取搜索结果详情页，允许范围 1-100",
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
                                            "placeholder": "www.ldysg.com",
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
        self._close_thread_local_resources()

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

        search_started = perf_counter()
        base_url = self._resolve_base_url(site)
        timeout = int(site.get("timeout") or 20)
        ua = site.get("ua") or settings.USER_AGENT
        proxies = settings.PROXY if site.get("proxy") else None
        client_ip = self._rand_ip()

        # 构建 cookie（使用站点配置 cookie）
        cookie = str(site.get("cookie") or "").strip()

        media_profile = self._resolve_moviepilot_media_profile(keyword=keyword, mtype=mtype)
        self._log_moviepilot_media_profile(keyword=keyword, media_profile=media_profile)
        imdb_keyword = self._normalize_imdb_id(media_profile.get("imdb_id"))
        keyword_text = str(keyword or "").strip()
        used_search_mode = "imdb" if imdb_keyword else "keyword"
        used_search_keyword = imdb_keyword or keyword_text

        logger.info(
            f"老电影资源(ldysg)开始搜索：关键词='{keyword}'，"
            f"搜索方式='{'IMDb' if imdb_keyword else '关键词'}'"
        )

        try:
            client = RequestUtils(
                ua=ua,
                cookies=cookie,
                proxies=proxies,
                timeout=timeout,
                referer=base_url,
            )

            # 搜索视频列表
            video_list_started = perf_counter()
            video_items: List[Dict[str, Any]] = []
            if imdb_keyword:
                video_items = self._search_videos(
                    client=client,
                    base_url=base_url,
                    keyword=imdb_keyword,
                    ua=ua,
                    proxies=proxies,
                    timeout=timeout,
                    cookie=cookie,
                    client_ip=client_ip,
                )
                logger.debug(
                    f"老电影资源(ldysg)IMDb搜索结果：imdb_id='{imdb_keyword}'，"
                    f"命中视频={len(video_items)}"
                )
            if (not video_items) and keyword_text and (
                not imdb_keyword or keyword_text.lower() != imdb_keyword.lower()
            ):
                if imdb_keyword:
                    logger.debug(
                        f"老电影资源(ldysg)IMDb未命中，回退关键词搜索：关键词='{keyword_text}'"
                    )
                used_search_mode = "keyword"
                used_search_keyword = keyword_text
                video_items = self._search_videos(
                    client=client,
                    base_url=base_url,
                    keyword=keyword_text,
                    ua=ua,
                    proxies=proxies,
                    timeout=timeout,
                    cookie=cookie,
                    client_ip=client_ip,
                )
            video_list_cost = perf_counter() - video_list_started
            if not video_items:
                total_cost = perf_counter() - search_started
                total_cost_text = self._format_duration(total_cost)
                elapsed_seconds = max(1, int(total_cost + 0.5))
                timing_summary = {
                    "搜索总耗时": total_cost_text,
                    "关键词": keyword,
                    "视频列表耗时": self._format_duration(video_list_cost),
                    "视频数": 0,
                    "视频明细": [],
                    "返回磁力": 0,
                    "耗时秒": elapsed_seconds,
                }
                logger.info(
                    f"老电影资源(ldysg)搜索完成：关键词='{keyword}'，"
                    f"搜索方式='{used_search_mode}'，"
                    f"搜索词='{used_search_keyword}'，"
                    f"找到视频=0，返回磁力=0，耗时={elapsed_seconds}s"
                )
                logger.debug(
                    "老电影资源(ldysg)搜索耗时明细："
                    f"{json.dumps(timing_summary, ensure_ascii=False)}"
                )
                return []

            results, timing_items = self._fetch_video_details_concurrently(
                site=site,
                client=client,
                base_url=base_url,
                video_items=video_items,
                ua=ua,
                proxies=proxies,
                timeout=timeout,
                cookie=cookie,
            )

            total_cost = perf_counter() - search_started
            total_cost_text = self._format_duration(total_cost)
            elapsed_seconds = max(1, int(total_cost + 0.5))
            timing_summary = {
                "搜索总耗时": total_cost_text,
                "关键词": keyword,
                "视频列表耗时": self._format_duration(video_list_cost),
                "视频数": len(video_items),
                "视频明细": timing_items,
                "返回磁力": len(results),
                "耗时秒": elapsed_seconds,
            }
            logger.info(
                f"老电影资源(ldysg)搜索完成：关键词='{keyword}'，"
                f"搜索方式='{used_search_mode}'，"
                f"搜索词='{used_search_keyword}'，"
                f"找到视频={len(video_items)}，返回磁力={len(results)}，耗时={elapsed_seconds}s"
            )
            logger.debug(
                "老电影资源(ldysg)搜索耗时明细："
                f"{json.dumps(timing_summary, ensure_ascii=False)}"
            )
            return results
        except Exception as err:
            logger.error(f"老电影资源(ldysg)搜索异常：关键词='{keyword}'，错误={err}")
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
            from app.core.metainfo import MetaInfo
        except Exception as err:
            logger.debug(f"老电影资源(ldysg)加载主程序 MetaInfo 失败：{err}")
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
            logger.debug(f"老电影资源(ldysg)读取 TMDB 缓存失败：{err}")

        try:
            from app.modules.themoviedb.tmdbapi import TmdbApi
        except Exception as err:
            logger.debug(f"老电影资源(ldysg)加载 TmdbApi 失败：{err}")
            return profile

        api = None
        try:
            api = TmdbApi(language=settings.TMDB_LOCALE)
        except Exception:
            try:
                api = TmdbApi()
            except Exception as err:
                logger.debug(f"老电影资源(ldysg)初始化 TmdbApi 失败：{err}")
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
                    f"老电影资源(ldysg)主程序媒体识别：先执行TMDB match，"
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
            logger.debug(f"老电影资源(ldysg)识别主程序媒体信息失败：{err}")
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
            f"老电影资源(ldysg)主程序媒体信息：关键词='{str(keyword or '').strip()}'，"
            f"来源='{str(media_profile.get('source') or 'none').strip()}'，"
            f"类型='{str(media_profile.get('resolved_mtype') or '').strip() or 'unknown'}'，"
            f"tmdb_id={'有' if str(media_profile.get('tmdb_id') or '').strip() else '无'}，"
            f"imdb_id={'有' if str(media_profile.get('imdb_id') or '').strip() else '无'}，"
            f"主演={'有' if first_actor else '无'}，"
            f"第一主演='{first_actor}'"
        )

    def _fetch_video_details_concurrently(
            self,
            site: dict,
            client: RequestUtils,
            base_url: str,
            video_items: List[Dict[str, Any]],
            ua: str,
            proxies: Optional[Dict[str, str]],
            timeout: int,
            cookie: str) -> Tuple[List[TorrentInfo], List[Dict[str, Any]]]:
        worker_count = min(
            len(video_items),
            self._clamp_detail_concurrency(self._detail_concurrency),
        )
        if worker_count <= 1:
            results: List[TorrentInfo] = []
            timing_items: List[Dict[str, Any]] = []
            for item in video_items:
                item_results, timing_item = self._fetch_single_video_result(
                    site=site,
                    client=client,
                    base_url=base_url,
                    item=item,
                    ua=ua,
                    proxies=proxies,
                    timeout=timeout,
                    cookie=cookie,
                )
                results.extend(item_results)
                if timing_item:
                    timing_items.append(timing_item)
            return results, timing_items

        ordered_results: Dict[int, List[TorrentInfo]] = {}
        ordered_timing_items: Dict[int, Dict[str, Any]] = {}
        task_queue = deque(
            (index, item)
            for index, item in enumerate(video_items)
        )
        with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="ldysg") as executor:
            running_tasks: Dict[Any, Tuple[int, Dict[str, Any]]] = {}
            while task_queue or running_tasks:
                while task_queue and len(running_tasks) < worker_count:
                    index, item = task_queue.popleft()
                    future = executor.submit(
                        self._fetch_single_video_result,
                        site,
                        client,
                        base_url,
                        item,
                        ua,
                        proxies,
                        timeout,
                        cookie,
                    )
                    running_tasks[future] = (index, item)

                if not running_tasks:
                    break

                done, _ = wait(tuple(running_tasks.keys()), return_when=FIRST_COMPLETED)
                for future in done:
                    index, item = running_tasks.pop(future)
                    try:
                        item_results, timing_item = future.result()
                        ordered_results[index] = item_results or []
                        if timing_item:
                            ordered_timing_items[index] = timing_item
                    except Exception as err:
                        title = self._display_title((item or {}).get("title"))
                        logger.debug(
                            f"老电影资源(ldysg)并发抓取资源页异常："
                            f"片名='{title}'，错误={err}"
                        )
                        ordered_results[index] = []
                        ordered_timing_items[index] = {
                            "片名": title,
                            "首次get_vbt耗时": self._format_duration(0),
                            "验证码耗时": self._format_duration(0),
                            "验证码图片下载耗时": self._format_duration(0),
                            "OCR识别耗时": self._format_duration(0),
                            "验证码提交耗时": self._format_duration(0),
                            "成功返回种子列表耗时": self._format_duration(0),
                            "重试重新取验证码耗时": self._format_duration(0),
                            "种子总耗时": self._format_duration(0),
                            "验证码重试次数": 0,
                            "资源数": 0,
                            "状态": "抓取异常",
                        }

        results: List[TorrentInfo] = []
        timing_items: List[Dict[str, Any]] = []
        for index in range(len(video_items)):
            results.extend(ordered_results.get(index) or [])
            timing_item = ordered_timing_items.get(index)
            if timing_item:
                timing_items.append(timing_item)
        return results, timing_items

    def _fetch_single_video_result(
            self,
            site: dict,
            client: RequestUtils,
            base_url: str,
            item: Dict[str, Any],
            ua: str,
            proxies: Optional[Dict[str, str]],
            timeout: int,
            cookie: str) -> Tuple[List[TorrentInfo], Dict[str, Any]]:
        vid = str(item.get("id") or "").strip()
        if not vid:
            return [], {}
        title = str(item.get("title") or "").strip()
        if not title:
            return [], {}
        year = str(item.get("year") or "").strip()
        area = str(item.get("area") or "").strip()
        cat = str(item.get("cat") or "").strip()
        item_client_ip = self._rand_ip()

        vbt_items, timing_item = self._fetch_vbt(
            client=client,
            base_url=base_url,
            vid=vid,
            title=title,
            ua=ua,
            proxies=proxies,
            timeout=timeout,
            cookie=cookie,
            client_ip=item_client_ip,
        )
        if not vbt_items:
            return [], timing_item

        detail_url = urljoin(base_url, f"id/{vid}")
        results: List[TorrentInfo] = []
        for vbt in vbt_items:
            url = str(vbt.get("url") or "").strip()
            if not url or not url.lower().startswith("magnet:"):
                continue

            name = str(vbt.get("name") or title).strip()
            size_text = str(vbt.get("size") or "").strip()
            size_bytes = self._parse_size_bytes(size_text)
            unique_page_url = self._build_unique_result_page_url(
                detail_url=detail_url,
                magnet_url=url,
                result_name=name,
                size_text=size_text,
            )
            seeders = self._extract_int_field(
                vbt,
                ["seeders", "seeder", "seeds", "seed", "up", "upnum", "hot"],
            )
            elapsed_text = self._extract_text_field(
                vbt,
                [
                    "time",
                    "date_elapsed",
                    "date",
                    "pubdate",
                    "publish_time",
                    "publishTime",
                    "addtime",
                    "add_time",
                    "created_at",
                    "createdAt",
                    "ctime",
                    "uptime",
                ],
            )
            pubdate = self._parse_pubdate_text(elapsed_text)

            title_for_match = self._build_match_title(
                title=name,
                parent_title=title,
                year=year,
            )
            desc_parts = [x for x in [name, title, area, cat] if x]
            description = " | ".join(desc_parts[:3])
            if year and not re.search(r"(19|20)\d{2}", description):
                description = f"{description} {year}".strip()

            results.append(TorrentInfo(
                site=site.get("id"),
                site_name=site.get("name"),
                site_cookie=site.get("cookie"),
                site_ua=site.get("ua"),
                site_proxy=site.get("proxy"),
                site_order=site.get("pri"),
                site_downloader=site.get("downloader"),
                title=title_for_match or name,
                description=description,
                enclosure=url,
                page_url=unique_page_url,
                size=size_bytes,
                seeders=seeders,
                peers=0,
                grabs=0,
                pubdate=pubdate,
                date_elapsed=elapsed_text,
                downloadvolumefactor=0,
                uploadvolumefactor=1,
            ))
        timing_item["资源数"] = len(results)
        return results, timing_item

    def _search_videos(self, client: RequestUtils, base_url: str, keyword: str,
                       ua: str, proxies: Optional[Dict[str, str]],
                       timeout: int, cookie: str, client_ip: str) -> List[Dict[str, Any]]:
        """调用 POST /api.php?fun=get_video 搜索视频列表，分页合并所有结果"""
        api_url = urljoin(base_url, "api.php")
        all_items: List[Dict[str, Any]] = []
        seen_ids = set()
        page = 1
        max_pages = 5

        while page <= max_pages:
            payload = {
                "fun": "get_video",
                "title": keyword,
                "p": str(page),
                "issear": "1",
            }
            try:
                session = self._get_thread_local_session()
                headers = {
                    "User-Agent": _CHROME_UA,
                    "Referer": base_url,
                    "Origin": base_url.rstrip("/"),
                    "X-Requested-With": "XMLHttpRequest",
                    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                    "Accept": "application/json, text/javascript, */*; q=0.01",
                    "Accept-Language": "zh-CN,zh;q=0.9",
                    "X-Forwarded-For": client_ip,
                    "X-Real-IP": client_ip,
                }
                if cookie:
                    headers["Cookie"] = cookie
                resp = session.post(
                    api_url,
                    data=payload,
                    headers=headers,
                    proxies=proxies,
                    timeout=max(5, timeout),
                )
                if not resp.ok:
                    logger.debug(f"老电影资源(ldysg)搜索请求失败：status={resp.status_code}")
                    break
                data = resp.json()
            except Exception as e:
                logger.debug(f"老电影资源(ldysg)搜索请求异常：{e}")
                break

            items = data.get("data") or []
            if not items:
                break

            for item in items:
                vid = str(item.get("id") or "").strip()
                if vid and vid not in seen_ids:
                    seen_ids.add(vid)
                    all_items.append(item)

            psum = int(data.get("psum") or 1)
            if page >= psum:
                break
            page += 1

        return all_items

    def _fetch_vbt(self, client: RequestUtils, base_url: str, vid: str, title: str,
                   ua: str, proxies: Optional[Dict[str, str]],
                   timeout: int, cookie: str, client_ip: str) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """
        调用 POST /api.php 获取单个视频的磁力/网盘链接列表。
        站点对每次请求都要求图片验证码：
          1. 首次以 vcode='1' 发起请求，服务端返回 401 及验证码图片 URL
          2. 下载验证码图片，用 ddddocr OCR 识别数字
          3. 用识别结果重新发起请求，返回 200 及资源列表
        """
        api_url = urljoin(base_url, "api.php")
        referer = urljoin(base_url, f"id/{vid}")
        session = self._get_thread_local_session()
        vbt_started = perf_counter()
        captcha_started = None
        timing_item: Dict[str, Any] = {
            "片名": self._display_title(title),
            "首次get_vbt耗时": self._format_duration(0),
            "验证码耗时": self._format_duration(0),
            "验证码图片下载耗时": self._format_duration(0),
            "OCR识别耗时": self._format_duration(0),
            "验证码提交耗时": self._format_duration(0),
            "成功返回种子列表耗时": self._format_duration(0),
            "重试重新取验证码耗时": self._format_duration(0),
            "种子总耗时": self._format_duration(0),
            "验证码重试次数": 0,
            "资源数": 0,
            "状态": "未开始",
        }
        first_vbt_cost = 0.0
        captcha_download_cost = 0.0
        captcha_ocr_cost = 0.0
        captcha_submit_cost = 0.0
        success_return_cost = 0.0
        retry_refresh_cost = 0.0
        headers = {
            "User-Agent": _CHROME_UA,
            "Referer": referer,
            "Origin": base_url.rstrip("/"),
            "X-Requested-With": "XMLHttpRequest",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Accept-Language": "zh-CN,zh;q=0.9",
            "X-Forwarded-For": client_ip,
            "X-Real-IP": client_ip,
        }
        if cookie:
            headers["Cookie"] = cookie

        # 先访问详情页，让同一会话尽量贴近浏览器真实流程，再发起 get_vbt。
        try:
            detail_headers = {
                "User-Agent": _CHROME_UA,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
                "Accept-Language": "zh-CN,zh;q=0.9",
                "Referer": base_url,
                "X-Forwarded-For": client_ip,
                "X-Real-IP": client_ip,
            }
            if cookie:
                detail_headers["Cookie"] = cookie
            session.get(
                referer,
                headers=detail_headers,
                proxies=proxies,
                timeout=max(5, timeout),
            )
        except Exception as e:
            logger.debug(f"老电影资源(ldysg)详情页预热异常：vid={vid}，{e}")

        def _build_timing(status: str, retries: int = 0) -> Dict[str, Any]:
            total_cost = perf_counter() - vbt_started
            captcha_cost = 0 if captcha_started is None else perf_counter() - captcha_started
            timing_item["首次get_vbt耗时"] = self._format_duration(first_vbt_cost)
            timing_item["验证码耗时"] = self._format_duration(captcha_cost)
            timing_item["验证码图片下载耗时"] = self._format_duration(captcha_download_cost)
            timing_item["OCR识别耗时"] = self._format_duration(captcha_ocr_cost)
            timing_item["验证码提交耗时"] = self._format_duration(captcha_submit_cost)
            timing_item["成功返回种子列表耗时"] = self._format_duration(success_return_cost)
            timing_item["重试重新取验证码耗时"] = self._format_duration(retry_refresh_cost)
            timing_item["种子总耗时"] = self._format_duration(total_cost)
            timing_item["验证码重试次数"] = retries
            timing_item["状态"] = status
            return timing_item

        def _post_vbt(vcode: str) -> Optional[dict]:
            try:
                resp = session.post(
                    api_url,
                    data={"fun": "get_vbt", "id": vid, "issear": "0", "vcode": vcode},
                    headers=headers,
                    proxies=proxies,
                    timeout=max(5, timeout),
                )
                return resp
            except Exception as e:
                logger.debug(f"老电影资源(ldysg)请求异常：vid={vid}，{e}")
                return None

        def _refresh_captcha_resp() -> Optional[dict]:
            nonlocal retry_refresh_cost
            refresh_started = perf_counter()
            resp = _post_vbt("1")
            retry_refresh_cost += perf_counter() - refresh_started
            return resp

        # 第一次请求（触发验证码）
        first_vbt_started = perf_counter()
        resp1 = _post_vbt("1")
        first_vbt_cost = perf_counter() - first_vbt_started
        if resp1 is None:
            return [], _build_timing("首请求失败")

        # 直接返回 200，说明本次无需验证码（偶发）
        if resp1.status_code == 200:
            try:
                items = self._extract_vbt_items(resp1.json())
                return items, _build_timing("无需验证码")
            except Exception:
                return [], _build_timing("响应解析失败")

        # 返回 401 → 拿验证码图片 URL 并 OCR
        if resp1.status_code == 401:
            captcha_resp = resp1
            max_captcha_rounds = 3
            captcha_started = perf_counter()

            for captcha_round in range(1, max_captcha_rounds + 1):
                try:
                    err_data = captcha_resp.json()
                except Exception:
                    logger.debug(f"老电影资源(ldysg)401 响应解析失败：vid={vid}")
                    return [], _build_timing("验证码响应解析失败", captcha_round - 1)

                captcha_url = str(err_data.get("vcode") or "").strip()
                if not captcha_url:
                    logger.debug(f"老电影资源(ldysg)401 无验证码 URL：vid={vid}")
                    return [], _build_timing("缺少验证码地址", captcha_round - 1)

                # 验证码 URL 可能是相对路径
                if not captcha_url.startswith("http"):
                    captcha_url = urljoin(base_url, captcha_url)

                # 跳过视频验证码（无法 OCR）
                if captcha_url.lower().endswith(".mp4"):
                    logger.debug(f"老电影资源(ldysg)视频验证码无法识别，跳过：vid={vid}")
                    return [], _build_timing("视频验证码跳过", captcha_round - 1)

                solved, ocr_timing = self._ocr_captcha(
                    captcha_url,
                    proxies=proxies,
                    timeout=timeout,
                    referer=referer,
                    ua=ua,
                    client_ip=client_ip,
                )
                captcha_download_cost += float(ocr_timing.get("download_cost") or 0)
                captcha_ocr_cost += float(ocr_timing.get("ocr_cost") or 0)
                if not solved:
                    retry_prefix = f"验证码重试第{captcha_round - 1}次，" if captcha_round > 1 else ""
                    logger.debug(
                        f"老电影资源(ldysg){retry_prefix}"
                        f"验证码识别结果=''，验证码验证失败，"
                        f"片名='{self._display_title(title)}'，body='验证码识别失败'"
                    )
                    if captcha_round < max_captcha_rounds:
                        captcha_resp = _refresh_captcha_resp()
                        if captcha_resp is None or captcha_resp.status_code != 401:
                            logger.debug(
                                f"老电影资源(ldysg)验证码验证失败，"
                                f"片名='{self._display_title(title)}'，"
                                f"status={captcha_resp.status_code if captcha_resp is not None else 'None'}，"
                                f"body='{self._preview_response_body(captcha_resp) or '重新获取验证码失败'}'"
                            )
                            return [], _build_timing("重新获取验证码失败", captcha_round)
                        continue
                    return [], _build_timing("验证码识别失败", captcha_round - 1)

                submit_started = perf_counter()
                resp2 = _post_vbt(solved)
                submit_cost = perf_counter() - submit_started
                captcha_submit_cost += submit_cost
                retry_prefix = f"验证码重试第{captcha_round - 1}次，" if captcha_round > 1 else ""

                if resp2 is not None and resp2.status_code == 200:
                    logger.debug(
                        f"老电影资源(ldysg){retry_prefix}"
                        f"验证码识别结果='{solved}'，验证码验证成功，"
                        f"片名='{self._display_title(title)}'"
                    )
                    try:
                        success_return_started = perf_counter()
                        items = self._extract_vbt_items(resp2.json())
                        success_return_cost = perf_counter() - success_return_started
                        return items, _build_timing("验证码通过", captcha_round - 1)
                    except Exception:
                        return [], _build_timing("种子响应解析失败", captcha_round - 1)

                body_preview = self._preview_response_body(resp2)
                logger.debug(
                    f"老电影资源(ldysg){retry_prefix}"
                    f"验证码识别结果='{solved}'，验证码验证失败，"
                    f"片名='{self._display_title(title)}'，"
                    f"status={resp2.status_code if resp2 is not None else 'None'}，"
                    f"body='{body_preview}'"
                )

                if captcha_round < max_captcha_rounds:
                    captcha_resp = _refresh_captcha_resp()
                    if captcha_resp is None:
                        logger.debug(
                            f"老电影资源(ldysg)验证码验证失败，"
                            f"片名='{self._display_title(title)}'，status=None，body='重新获取验证码失败'"
                        )
                        return [], _build_timing("重新获取验证码失败", captcha_round)
                    if captcha_resp.status_code != 401:
                        logger.debug(
                            f"老电影资源(ldysg)验证码验证失败，"
                            f"片名='{self._display_title(title)}'，"
                            f"status={captcha_resp.status_code}，"
                            f"body='{self._preview_response_body(captcha_resp)}'"
                        )
                        return [], _build_timing("重新获取验证码异常", captcha_round)
                    continue

                return [], _build_timing("验证码验证失败", captcha_round - 1)

        if resp1.status_code == 406:
            try:
                msg = resp1.json().get("msg", "")
            except Exception:
                msg = ""
            logger.warning(f"老电影资源(ldysg)今日访问已达上限，请24小时后重试：{msg}")
            return [], _build_timing("今日访问上限")
        else:
            logger.debug(f"老电影资源(ldysg)获取资源失败：vid={vid}，status={resp1.status_code}")
            return [], _build_timing(f"获取资源失败({resp1.status_code})")

    @staticmethod
    def _extract_vbt_items(data: dict) -> List[Dict[str, Any]]:
        """从 get_vbt 响应中提取资源条目列表"""
        vbt = data.get("vbt") or {}
        items: List[Dict[str, Any]] = []
        if isinstance(vbt, dict):
            for source_items in vbt.values():
                if isinstance(source_items, list):
                    items.extend(source_items)
        elif isinstance(vbt, list):
            items = vbt
        return items

    @staticmethod
    def _preview_response_body(resp: Any, limit: int = 120) -> str:
        """格式化响应体预览，优先输出中文 JSON，便于日志排查。"""
        if resp is None:
            return ""
        try:
            text = json.dumps(resp.json(), ensure_ascii=False)
        except Exception:
            text = str(getattr(resp, "text", "") or "")
        return re.sub(r"\s+", " ", text).strip()[:limit]

    @staticmethod
    def _display_title(title: Any) -> str:
        text = str(title or "").strip().replace("'", " ")
        return text or "未知片名"

    @staticmethod
    def _format_duration(seconds: float) -> str:
        return f"{max(0, seconds):.2f}s"

    @staticmethod
    def _is_captcha_wrong_response(resp: Any) -> bool:
        if resp is None or getattr(resp, "status_code", None) != 403:
            return False
        try:
            msg = str((resp.json() or {}).get("msg") or "").strip()
        except Exception:
            msg = str(getattr(resp, "text", "") or "")
        return "验证码错误" in msg

    @staticmethod
    def _ocr_captcha(captcha_url: str, proxies: Optional[Dict[str, str]] = None,
                     timeout: int = 10, referer: str = "https://www.ldysg.com/",
                     ua: str = "", client_ip: str = "") -> Tuple[str, Dict[str, float]]:
        """
        下载验证码图片并用 ddddocr 识别。
        ddddocr 专为中文网站图片验证码设计，识别效果好。
        若未安装则返回空字符串。
        """
        try:
            import ddddocr  # type: ignore
        except ImportError:
            logger.warning("老电影资源(ldysg)未安装 ddddocr，无法自动识别验证码。"
                           "请在 MoviePilot 环境中执行：pip install ddddocr")
            return "", {"download_cost": 0.0, "ocr_cost": 0.0}

        try:
            session = LdysgIndexer._get_thread_local_session()
            headers = {
                "Accept": "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8",
                "Accept-Language": "zh-CN,zh;q=0.9",
                "Referer": referer or "https://www.ldysg.com/",
                "User-Agent": _CHROME_UA,
            }
            if client_ip:
                headers["X-Forwarded-For"] = client_ip
                headers["X-Real-IP"] = client_ip
            download_started = perf_counter()
            img_resp = session.get(
                captcha_url,
                proxies=proxies,
                timeout=max(5, timeout),
                headers=headers,
            )
            download_cost = perf_counter() - download_started
            if not img_resp.ok:
                return "", {"download_cost": download_cost, "ocr_cost": 0.0}
            img_bytes = img_resp.content
            if not img_bytes:
                return "", {"download_cost": download_cost, "ocr_cost": 0.0}

            ocr = LdysgIndexer._get_shared_ocr()
            ocr_started = perf_counter()
            with LdysgIndexer._shared_ocr_predict_lock:
                result = str(ocr.classification(img_bytes) or "").strip()
            ocr_cost = perf_counter() - ocr_started
            # 只保留数字和字母，去除空白
            result = re.sub(r"\s+", "", result)
            return result, {"download_cost": download_cost, "ocr_cost": ocr_cost}
        except Exception as e:
            logger.debug(f"老电影资源(ldysg)验证码 OCR 异常：{e}")
            return "", {"download_cost": 0.0, "ocr_cost": 0.0}

    @classmethod
    def _get_thread_local_session(cls):
        session = getattr(cls._thread_local, "session", None)
        if session is None:
            import requests
            session = requests.Session()
            cls._thread_local.session = session
        return session

    @classmethod
    def _get_shared_ocr(cls):
        ocr = cls._shared_ocr
        if ocr is None:
            with cls._shared_ocr_init_lock:
                ocr = cls._shared_ocr
                if ocr is None:
                    import ddddocr  # type: ignore
                    ocr = ddddocr.DdddOcr(show_ad=False)
                    cls._shared_ocr = ocr
        return ocr

    @classmethod
    def _close_thread_local_resources(cls) -> None:
        session = getattr(cls._thread_local, "session", None)
        if session is not None:
            try:
                session.close()
            except Exception:
                pass
            finally:
                cls._thread_local.session = None

        with cls._shared_ocr_init_lock:
            if cls._shared_ocr is not None:
                cls._shared_ocr = None

    @staticmethod
    def _rand_ip() -> str:
        """生成随机公网 IP，用于绕过站点 IP 维度的访问频率限制"""
        return f"{random.randint(1, 223)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(1, 254)}"

    @staticmethod
    def _clamp_detail_concurrency(value: Any) -> int:
        try:
            concurrency = int(value or 5)
        except Exception:
            concurrency = 5
        return max(1, min(concurrency, 100))

    @staticmethod
    def _build_match_title(title: str, parent_title: str = "", year: str = "") -> str:
        """构造 MoviePilot 匹配用标题"""
        t = str(title or "").strip()
        pt = str(parent_title or "").strip()
        yr = str(year or "").strip()

        if pt and pt not in t:
            t = f"{pt} {t}".strip()
        if yr and not re.search(r"(19|20)\d{2}", t):
            t = f"{t} {yr}".strip()
        return t

    @staticmethod
    def _parse_size_bytes(size_text: str) -> int:
        """解析大小文本为字节数，如 '1.5GB'、'720MB'"""
        text = str(size_text or "").strip()
        if not text:
            return 0
        m = re.search(r"([\d.]+)\s*(tb|gb|mb|kb|b)", text, re.IGNORECASE)
        if not m:
            return 0
        val = float(m.group(1))
        unit = m.group(2).upper()
        mul = {"TB": 1 << 40, "GB": 1 << 30, "MB": 1 << 20, "KB": 1 << 10, "B": 1}
        return int(val * mul.get(unit, 0))

    @staticmethod
    def _build_unique_result_page_url(
            detail_url: str,
            magnet_url: str,
            result_name: str = "",
            size_text: str = "") -> str:
        """
        MoviePilot 卡片视图使用 torrent_info.page_url 作为 Vue key。
        ldysg 同一视频下会返回多条磁力，若共用同一个详情页 URL，会导致前端复用错误，
        进而出现筛选串站点、排序不生效的问题。
        这里追加仅前端可见的 fragment，既保持详情页可打开，又保证每条资源 key 唯一。
        """
        seed_text = "|".join([
            str(detail_url or "").strip(),
            str(magnet_url or "").strip(),
            str(result_name or "").strip(),
            str(size_text or "").strip(),
        ])
        suffix = sha1(seed_text.encode("utf-8")).hexdigest()[:12]
        return f"{detail_url}#ldysg-{suffix}"

    @staticmethod
    def _extract_text_field(data: Dict[str, Any], keys: List[str]) -> str:
        for key in keys:
            value = data.get(key)
            if value is None:
                continue
            text = str(value).strip()
            if text:
                return text
        return ""

    @staticmethod
    def _extract_int_field(data: Dict[str, Any], keys: List[str]) -> int:
        for key in keys:
            value = data.get(key)
            if value is None:
                continue
            if isinstance(value, bool):
                continue
            if isinstance(value, (int, float)):
                return int(value)
            text = str(value).strip()
            if not text:
                continue
            match = re.search(r"-?\d+", text)
            if match:
                try:
                    return int(match.group(0))
                except Exception:
                    continue
        return 0

    @staticmethod
    def _parse_pubdate_text(text: str) -> Optional[datetime]:
        raw = str(text or "").strip()
        if not raw:
            return None

        now = datetime.now()
        normalized = raw.replace("T", " ").replace("Z", "")
        normalized = re.sub(r"\s+", " ", normalized).strip()

        absolute_formats = [
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%Y/%m/%d %H:%M:%S",
            "%Y/%m/%d %H:%M",
            "%Y-%m-%d",
            "%Y/%m/%d",
            "%Y.%m.%d %H:%M:%S",
            "%Y.%m.%d %H:%M",
            "%Y.%m.%d",
        ]
        for fmt in absolute_formats:
            try:
                return datetime.strptime(normalized, fmt)
            except Exception:
                pass

        month_day_formats = [
            "%m-%d %H:%M:%S",
            "%m-%d %H:%M",
            "%m/%d %H:%M:%S",
            "%m/%d %H:%M",
            "%m-%d",
            "%m/%d",
        ]
        for fmt in month_day_formats:
            try:
                parsed = datetime.strptime(normalized, fmt)
                return parsed.replace(year=now.year)
            except Exception:
                pass

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
        if "前天" in normalized:
            return now - timedelta(days=2)
        return None

    def _match_target_site(self, site: dict) -> bool:
        site_id = str(site.get("id") or "").strip().lower()
        if site_id == "ldysg":
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
        hosts = {self._default_host, "www.ldysg.com"}
        for host in self._ordered_extra_hosts():
            hosts.add(host)
        return hosts

    def _registered_hosts(self) -> List[str]:
        ordered_extra_hosts = self._ordered_extra_hosts()
        if ordered_extra_hosts:
            return ordered_extra_hosts
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
                logger.debug(f"老电影资源(ldysg)索引器注册失败：域名={host}，错误={err}")
        logger.info(f"老电影资源(ldysg)索引器注册完成：域名列表={', '.join(hosts)}")

    @staticmethod
    def _build_indexer_schema(all_hosts: List[str]) -> Dict[str, Any]:
        primary = "www.ldysg.com" if "www.ldysg.com" in all_hosts else all_hosts[0]
        ext_domains = [f"https://{h}/" for h in all_hosts if h != primary]
        return {
            "id": "ldysg",
            "name": "老电影资源",
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
                        "path": "id/1",
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
        if pure.startswith("www."):
            pure_no_www = pure[4:]
        else:
            pure_no_www = pure
        for allowed in allowed_hosts:
            a = allowed.lower().lstrip(".")
            if a.startswith("www."):
                a_no_www = a[4:]
            else:
                a_no_www = a
            if pure == a or pure_no_www == a_no_www:
                return True
        return False

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
        return LdysgIndexer._unique_nonempty(results)

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
