import json
import random
import re
from datetime import datetime
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


class LdysgIndexer(_PluginBase):
    plugin_name = "老电影（ldysg）"
    plugin_desc = "为 ldysg.com 提供老旧电影磁力搜索支持，自动识别验证码。"
    plugin_icon = "https://raw.githubusercontent.com/yang124541/Moviepilot-Plugins/main/ldysg.png"
    plugin_version = "1.1.3"
    plugin_author = "yang124541"
    author_url = "https://github.com/yang124541/moviepilot-plugin"
    plugin_config_prefix = "ldysgindexer_"
    plugin_order = 31
    auth_level = 2
    plugin_depend = ["ddddocr"]

    _enabled = False
    _extra_hosts = ""

    _default_host = "ldysg.com"
    _default_base_url = "https://www.ldysg.com/"

    def init_plugin(self, config: dict = None):
        self._install_ddddocr()

        if config:
            self._enabled = bool(config.get("enabled"))
            self._extra_hosts = (config.get("extra_hosts") or "").strip()

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
                                            "text": "ldysg.com 提供老旧电影高清无水印下载。"
                                                    "搜索结果仅包含磁力链接，MoviePilot 将自动使用磁力链接下载。"
                                                    "若资源需要验证码则自动跳过。",
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
        client_ip = self._rand_ip()

        # 构建 cookie（使用站点配置 cookie）
        cookie = str(site.get("cookie") or "").strip()

        logger.info(f"老电影资源(ldysg)开始搜索：关键词='{keyword}'")

        try:
            client = RequestUtils(
                ua=ua,
                cookies=cookie,
                proxies=proxies,
                timeout=timeout,
                referer=base_url,
            )

            # 搜索视频列表
            video_items = self._search_videos(
                client=client,
                base_url=base_url,
                keyword=keyword,
                ua=ua,
                proxies=proxies,
                timeout=timeout,
                cookie=cookie,
                client_ip=client_ip,
            )
            if not video_items:
                logger.info(f"老电影资源(ldysg)搜索无结果：关键词='{keyword}'")
                return []

            results: List[TorrentInfo] = []
            for item in video_items:
                vid = str(item.get("id") or "").strip()
                if not vid:
                    continue
                title = str(item.get("title") or "").strip()
                if not title:
                    continue
                year = str(item.get("year") or "").strip()
                area = str(item.get("area") or "").strip()
                cat = str(item.get("cat") or "").strip()

                # 获取资源链接
                vbt_items = self._fetch_vbt(
                    client=client,
                    base_url=base_url,
                    vid=vid,
                    ua=ua,
                    proxies=proxies,
                    timeout=timeout,
                    cookie=cookie,
                    client_ip=client_ip,
                )
                if not vbt_items:
                    continue

                detail_url = urljoin(base_url, f"id/{vid}")
                for vbt in vbt_items:
                    url = str(vbt.get("url") or "").strip()
                    if not url:
                        continue
                    # 只接受磁力链接
                    if not url.lower().startswith("magnet:"):
                        continue

                    name = str(vbt.get("name") or title).strip()
                    size_text = str(vbt.get("size") or "").strip()
                    size_bytes = self._parse_size_bytes(size_text)

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
                        page_url=detail_url,
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
                f"老电影资源(ldysg)搜索完成：关键词='{keyword}'，"
                f"找到视频={len(video_items)}，返回磁力={len(results)}，耗时={cost}s"
            )
            return results
        except Exception as err:
            logger.error(f"老电影资源(ldysg)搜索异常：关键词='{keyword}'，错误={err}")
            return []

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
                import requests as _requests
                headers = {
                    "User-Agent": ua or settings.USER_AGENT,
                    "Referer": base_url,
                    "X-Requested-With": "XMLHttpRequest",
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Accept": "application/json, text/javascript, */*; q=0.01",
                    "X-Forwarded-For": client_ip,
                    "X-Real-IP": client_ip,
                }
                if cookie:
                    headers["Cookie"] = cookie
                resp = _requests.post(
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

    def _fetch_vbt(self, client: RequestUtils, base_url: str, vid: str,
                   ua: str, proxies: Optional[Dict[str, str]],
                   timeout: int, cookie: str, client_ip: str) -> List[Dict[str, Any]]:
        """
        调用 POST /api.php 获取单个视频的磁力/网盘链接列表。
        站点对每次请求都要求图片验证码：
          1. 首次以 vcode='1' 发起请求，服务端返回 401 及验证码图片 URL
          2. 下载验证码图片，用 ddddocr OCR 识别数字
          3. 用识别结果重新发起请求，返回 200 及资源列表
        """
        import requests as _requests

        api_url = urljoin(base_url, "api.php")
        referer = urljoin(base_url, f"id/{vid}")
        headers = {
            "User-Agent": ua or settings.USER_AGENT,
            "Referer": referer,
            "X-Requested-With": "XMLHttpRequest",
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "X-Forwarded-For": client_ip,
            "X-Real-IP": client_ip,
        }
        if cookie:
            headers["Cookie"] = cookie

        def _post_vbt(vcode: str) -> Optional[dict]:
            try:
                resp = _requests.post(
                    api_url,
                    data={"fun": "get_vbt", "id": vid, "issear": "1", "vcode": vcode},
                    headers=headers,
                    proxies=proxies,
                    timeout=max(5, timeout),
                )
                return resp
            except Exception as e:
                logger.debug(f"老电影资源(ldysg)请求异常：vid={vid}，{e}")
                return None

        # 第一次请求（触发验证码）
        resp1 = _post_vbt("1")
        if resp1 is None:
            return []

        # 直接返回 200，说明本次无需验证码（偶发）
        if resp1.status_code == 200:
            try:
                return self._extract_vbt_items(resp1.json())
            except Exception:
                return []

        # 返回 401 → 拿验证码图片 URL 并 OCR
        if resp1.status_code == 401:
            captcha_resp = resp1
            max_captcha_rounds = 2

            for captcha_round in range(1, max_captcha_rounds + 1):
                try:
                    err_data = captcha_resp.json()
                except Exception:
                    logger.debug(f"老电影资源(ldysg)401 响应解析失败：vid={vid}")
                    return []

                captcha_url = str(err_data.get("vcode") or "").strip()
                if not captcha_url:
                    logger.debug(f"老电影资源(ldysg)401 无验证码 URL：vid={vid}")
                    return []

                # 验证码 URL 可能是相对路径
                if not captcha_url.startswith("http"):
                    captcha_url = urljoin(base_url, captcha_url)

                # 跳过视频验证码（无法 OCR）
                if captcha_url.lower().endswith(".mp4"):
                    logger.debug(f"老电影资源(ldysg)视频验证码无法识别，跳过：vid={vid}")
                    return []

                solved = self._ocr_captcha(
                    captcha_url,
                    proxies=proxies,
                    timeout=timeout,
                    referer=referer,
                    ua=ua,
                    client_ip=client_ip,
                )
                if not solved:
                    logger.debug(f"老电影资源(ldysg)验证码识别失败，跳过：vid={vid}")
                    return []

                logger.debug(
                    f"老电影资源(ldysg)验证码识别结果='{solved}'，"
                    f"vid={vid}，captcha_round={captcha_round}/{max_captcha_rounds}"
                )
                resp2 = None
                max_submit_attempts = 3
                attempts_used = 0
                retryable_statuses = {429, 500, 502, 503, 504}
                for attempt in range(1, max_submit_attempts + 1):
                    attempts_used = attempt
                    resp2 = _post_vbt(solved)
                    if resp2 is None:
                        logger.debug(
                            f"老电影资源(ldysg)验证码提交第{attempt}/{max_submit_attempts}次失败："
                            f"vid={vid}，status=None，captcha_round={captcha_round}/{max_captcha_rounds}"
                        )
                        if attempt < max_submit_attempts:
                            logger.debug(
                                f"老电影资源(ldysg)验证码提交准备重试({attempt + 1}/{max_submit_attempts})："
                                f"vid={vid}，原因=无响应，captcha_round={captcha_round}/{max_captcha_rounds}"
                            )
                        continue

                    if resp2.status_code == 200:
                        if attempt > 1:
                            logger.debug(
                                f"老电影资源(ldysg)验证码提交重试成功：vid={vid}，"
                                f"attempt={attempt}/{max_submit_attempts}，"
                                f"captcha_round={captcha_round}/{max_captcha_rounds}"
                            )
                        break

                    body_preview = self._preview_response_body(resp2)
                    logger.debug(
                        f"老电影资源(ldysg)验证码提交第{attempt}/{max_submit_attempts}次返回异常："
                        f"vid={vid}，status={resp2.status_code}，body='{body_preview}'，"
                        f"captcha_round={captcha_round}/{max_captcha_rounds}"
                    )
                    if resp2.status_code not in retryable_statuses or attempt >= max_submit_attempts:
                        break
                    logger.debug(
                        f"老电影资源(ldysg)验证码提交准备重试({attempt + 1}/{max_submit_attempts})："
                        f"vid={vid}，原因=status={resp2.status_code}，"
                        f"captcha_round={captcha_round}/{max_captcha_rounds}"
                    )

                if resp2 is not None and resp2.status_code == 200:
                    try:
                        return self._extract_vbt_items(resp2.json())
                    except Exception:
                        return []

                if self._is_captcha_wrong_response(resp2) and captcha_round < max_captcha_rounds:
                    logger.debug(
                        f"老电影资源(ldysg)验证码提交返回“验证码错误”，准备重新获取新验证码："
                        f"vid={vid}，next_captcha_round={captcha_round + 1}/{max_captcha_rounds}"
                    )
                    captcha_resp = _post_vbt("1")
                    if captcha_resp is None:
                        logger.debug(f"老电影资源(ldysg)重新获取验证码失败：vid={vid}，status=None")
                        return []
                    if captcha_resp.status_code != 401:
                        logger.debug(
                            f"老电影资源(ldysg)重新获取验证码失败：vid={vid}，"
                            f"status={captcha_resp.status_code}，"
                            f"body='{self._preview_response_body(captcha_resp)}'"
                        )
                        return []
                    continue

                logger.debug(
                    f"老电影资源(ldysg)验证码提交失败：vid={vid}，"
                    f"status={resp2.status_code if resp2 is not None else 'None'}，"
                    f"attempts={attempts_used}，captcha_round={captcha_round}/{max_captcha_rounds}"
                )
                return []

        if resp1.status_code == 406:
            try:
                msg = resp1.json().get("msg", "")
            except Exception:
                msg = ""
            logger.warning(f"老电影资源(ldysg)今日访问已达上限，请24小时后重试：{msg}")
        else:
            logger.debug(f"老电影资源(ldysg)获取资源失败：vid={vid}，status={resp1.status_code}")
        return []

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
                     ua: str = "", client_ip: str = "") -> str:
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
            return ""

        try:
            import requests as _requests
            headers = {
                "Referer": referer or "https://www.ldysg.com/",
            }
            if ua:
                headers["User-Agent"] = ua
            if client_ip:
                headers["X-Forwarded-For"] = client_ip
                headers["X-Real-IP"] = client_ip
            img_resp = _requests.get(
                captcha_url,
                proxies=proxies,
                timeout=max(5, timeout),
                headers=headers,
            )
            if not img_resp.ok:
                return ""
            img_bytes = img_resp.content
            if not img_bytes:
                return ""

            ocr = ddddocr.DdddOcr(show_ad=False)
            result = str(ocr.classification(img_bytes) or "").strip()
            # 只保留数字和字母，去除空白
            result = re.sub(r"\s+", "", result)
            return result
        except Exception as e:
            logger.debug(f"老电影资源(ldysg)验证码 OCR 异常：{e}")
            return ""

    @staticmethod
    def _rand_ip() -> str:
        """生成随机公网 IP，用于绕过站点 IP 维度的访问频率限制"""
        return f"{random.randint(1, 223)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(1, 254)}"

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
        raw = str(site.get("url") or site.get("domain") or "").strip()
        if not raw:
            return self._default_base_url
        if "://" not in raw:
            raw = f"https://{raw}"
        parsed = urlparse(raw)
        if not parsed.netloc:
            return self._default_base_url
        return f"{parsed.scheme}://{parsed.netloc}/"

    def _all_hosts(self) -> set:
        hosts = {self._default_host, "www.ldysg.com"}
        for line in (self._extra_hosts or "").splitlines():
            host = self._extract_host(line)
            if host:
                hosts.add(host)
        return hosts

    def _register_builtin_indexer(self) -> None:
        all_hosts = sorted(self._all_hosts())
        indexer = self._build_indexer_schema(all_hosts)
        for host in all_hosts:
            try:
                SitesHelper().add_indexer(domain=host, indexer=indexer)
            except Exception as err:
                logger.debug(f"老电影资源(ldysg)索引器注册失败：域名={host}，错误={err}")
        logger.info(f"老电影资源(ldysg)索引器注册完成：域名列表={', '.join(all_hosts)}")

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
