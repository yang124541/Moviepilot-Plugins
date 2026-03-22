import re
from datetime import datetime
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
    plugin_name = "BT之家（1lou.me）"
    plugin_desc = "为 1lou.me 提供种子搜索支持，支持账号登录。"
    plugin_icon = "https://www.1lou.me/view/img/favicon.ico"
    plugin_version = "1.0.1"
    plugin_author = "yang124541"
    author_url = "https://github.com/yang124541/moviepilot-plugin"
    plugin_config_prefix = "loumeindexer_"
    plugin_order = 32
    auth_level = 2

    _enabled = False
    _login_username = ""
    _login_password = ""
    _extra_hosts = ""

    _default_host = "1lou.me"
    _default_base_url = "https://www.1lou.me/"

    # 搜索最大分页
    _max_search_pages: int = 5

    def init_plugin(self, config: dict = None):
        if config:
            self._enabled = bool(config.get("enabled"))
            self._login_username = str(config.get("login_username") or "").strip()
            self._login_password = str(config.get("login_password") or "").strip()
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
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "login_username",
                                            "label": "账号（Email）",
                                            "placeholder": "请输入 1lou.me 邮箱账号",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "login_password",
                                            "label": "密码",
                                            "type": "password",
                                            "placeholder": "请输入 1lou.me 密码",
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
                                            "text": "1lou.me（BT之家）是一个BT资源论坛，提供影视剧集种子下载。"
                                                    "登录后可下载种子附件，未登录仅能获取种子文件名信息。"
                                                    "站点 URL 请在 MoviePilot 站点管理中配置为 https://www.1lou.me/",
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
            "login_username": "",
            "login_password": "",
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

        logger.info(f"BT之家(1lou)开始搜索：关键词='{keyword}'")

        try:
            # 构建带 cookie 的 session
            session = _requests.Session()
            cookie_from_site = str(site.get("cookie") or "").strip()

            # 尝试登录获取 cookie（若有账号密码配置）
            if self._login_username and self._login_password:
                login_cookie = self._login(
                    session=session,
                    base_url=base_url,
                    ua=ua,
                    proxies=proxies,
                    timeout=timeout,
                )
                if not login_cookie:
                    logger.warning("BT之家(1lou)登录失败，将尝试使用站点 cookie")
                    if cookie_from_site:
                        session.headers.update({"Cookie": cookie_from_site})
            elif cookie_from_site:
                session.headers.update({"Cookie": cookie_from_site})

            session.headers.update({
                "User-Agent": ua,
                "Referer": base_url,
                "Accept-Language": "zh-CN,zh;q=0.9",
            })

            # 搜索帖子列表
            thread_items = self._search_threads(
                session=session,
                base_url=base_url,
                keyword=keyword,
                timeout=timeout,
                proxies=proxies,
            )

            if not thread_items:
                logger.info(f"BT之家(1lou)搜索无结果：关键词='{keyword}'")
                return []

            results: List[TorrentInfo] = []

            for item in thread_items:
                tid = str(item.get("tid") or "").strip()
                if not tid:
                    continue
                title = str(item.get("title") or "").strip()
                if not title:
                    continue

                thread_url = urljoin(base_url, f"thread-{tid}.htm")

                # 获取帖子详情页的种子附件
                attach_items = self._fetch_thread_attachments(
                    session=session,
                    base_url=base_url,
                    tid=tid,
                    timeout=timeout,
                    proxies=proxies,
                )

                if not attach_items:
                    # 没有附件，跳过
                    continue

                for attach in attach_items:
                    aid = str(attach.get("aid") or "").strip()
                    filename = str(attach.get("filename") or "").strip()
                    if not aid:
                        continue

                    # 种子下载 URL
                    download_url = urljoin(base_url, f"attach-download-{aid}.htm")

                    # 用文件名或帖子标题作为 TorrentInfo 标题
                    torrent_title = filename if filename else title

                    # 解析文件大小（从标题推断）
                    size_bytes = self._parse_size_from_title(title)

                    description = title
                    if filename and filename != title:
                        description = f"{filename} | {title}"

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
                        enclosure=download_url,
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
                f"找到帖子={len(thread_items)}，返回种子={len(results)}，耗时={cost}s"
            )
            return results

        except Exception as err:
            logger.error(f"BT之家(1lou)搜索异常：关键词='{keyword}'，错误={err}")
            return []

    def _login(self, session: _requests.Session, base_url: str,
               ua: str, proxies: Optional[Dict[str, str]], timeout: int) -> bool:
        """登录 1lou.me，成功后 session 会持有有效 cookie"""
        login_url = urljoin(base_url, "user-login.htm")
        headers = {
            "User-Agent": ua,
            "Referer": login_url,
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
        payload = {
            "email": self._login_username,
            "password": self._login_password,
        }
        try:
            # 先 GET 登录页（获取可能的 token/session）
            session.get(login_url, headers=headers, proxies=proxies,
                        timeout=timeout, verify=False)
            # 提交登录表单
            resp = session.post(
                login_url,
                data=payload,
                headers=headers,
                proxies=proxies,
                timeout=timeout,
                verify=False,
                allow_redirects=True,
            )
            if resp.status_code in (200, 302):
                # 检查是否登录成功（页面中无登录按钮，或含用户名）
                if "user-login.htm" not in resp.url and resp.status_code == 200:
                    # 重定向走了，可能成功
                    logger.info("BT之家(1lou)登录成功")
                    return True
                # 检查响应内容
                text = resp.text
                if "user-login.htm" in text and "icon-user" in text:
                    # 还在登录页，失败
                    logger.warning("BT之家(1lou)登录失败：账号或密码错误")
                    return False
                logger.info("BT之家(1lou)登录成功")
                return True
        except Exception as e:
            logger.warning(f"BT之家(1lou)登录异常：{e}")
        return False

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
                        proxies: Optional[Dict[str, str]]) -> List[Dict[str, Any]]:
        """搜索帖子列表，分页合并结果"""
        all_items: List[Dict[str, Any]] = []
        seen_tids: set = set()
        search_urls = self._build_search_urls(base_url, keyword)

        for page_num, url in enumerate(search_urls, start=1):
            try:
                resp = session.get(
                    url,
                    timeout=timeout,
                    proxies=proxies,
                    verify=False,
                    allow_redirects=True,
                )
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

        return all_items

    @staticmethod
    def _parse_thread_list(html: str) -> List[Dict[str, Any]]:
        """
        解析搜索/论坛列表页，提取帖子列表。
        页面结构：每个帖子是 <li ... data-tid="{tid}"> 块，
        块内有 <a href="thread-{tid}.htm">标题</a>（可能含子标签）。
        """
        items: List[Dict[str, Any]] = []
        seen: set = set()

        tid_pattern = re.compile(r'data-tid="(\d+)"')
        matches = list(tid_pattern.finditer(html))

        for i, m in enumerate(matches):
            tid = m.group(1)
            if tid in seen:
                continue
            seen.add(tid)

            # 当前帖子块的范围
            start = m.start()
            end = matches[i + 1].start() if i + 1 < len(matches) else len(html)
            block = html[start:end]

            # 在块内找 thread-{tid}.htm 对应链接，取第一个非空标题
            link_iter = re.finditer(
                rf'href="thread-{tid}\.htm"[^>]*>(.*?)</a>',
                block,
                re.IGNORECASE | re.DOTALL
            )
            for lm in link_iter:
                raw = lm.group(1)
                title = re.sub(r'<[^>]+>', '', raw).strip()
                if title:
                    items.append({"tid": tid, "title": title})
                    break

        return items

    @staticmethod
    def _has_next_page(html: str) -> bool:
        """检查是否有下一页（分页区域存在 search-*-1-N.htm 链接）"""
        return bool(re.search(r'href="search-[^"]*-1-\d+\.htm"', html, re.IGNORECASE))

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
            return self._parse_attachments(resp.text)
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

        # 解析每个 li
        li_pattern = re.compile(
            r'<li\s+aid="(\d+)"[^>]*>.*?<a\s+href="attach-download-\d+\.htm"[^>]*>'
            r'.*?(?:<i[^>]*></i>)?\s*(.*?)\s*</a>',
            re.IGNORECASE | re.DOTALL
        )
        for m in li_pattern.finditer(attachlist_html):
            aid = m.group(1)
            raw_name = m.group(2)
            # 去除 HTML 标签
            filename = re.sub(r'<[^>]+>', '', raw_name).strip()

            # 只保留 .torrent 文件
            if filename.lower().endswith(".torrent"):
                items.append({"aid": aid, "filename": filename})

        # 若上面正则未匹配（HTML 结构变化），尝试更宽松的方式
        if not items:
            aid_pattern = re.compile(r'<li\s+aid="(\d+)"', re.IGNORECASE)
            link_pattern = re.compile(
                r'href="attach-download-(\d+)\.htm"[^>]*>.*?</a>',
                re.IGNORECASE | re.DOTALL
            )
            for li_m in aid_pattern.finditer(attachlist_html):
                aid = li_m.group(1)
                # 在这个 li 之后找链接
                pos = li_m.end()
                nearby = attachlist_html[pos:pos + 500]
                lm = re.search(
                    r'href="attach-download-\d+\.htm"[^>]*>\s*(?:<[^>]+>)?\s*(.*?)\s*</a>',
                    nearby,
                    re.IGNORECASE | re.DOTALL
                )
                if lm:
                    raw = lm.group(1)
                    filename = re.sub(r'<[^>]+>', '', raw).strip()
                    if filename.lower().endswith(".torrent"):
                        items.append({"aid": aid, "filename": filename})

        return items

    @staticmethod
    def _parse_size_from_title(title: str) -> int:
        """从标题中解析文件大小，如 [BD-MKV/7.26GB]"""
        m = re.search(r'([\d.]+)\s*(TB|GB|MB|KB)', title, re.IGNORECASE)
        if not m:
            return 0
        val = float(m.group(1))
        unit = m.group(2).upper()
        mul = {"TB": 1 << 40, "GB": 1 << 30, "MB": 1 << 20, "KB": 1 << 10}
        return int(val * mul.get(unit, 0))

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
        hosts = {self._default_host, "www.1lou.me"}
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
                logger.debug(f"BT之家(1lou)索引器注册失败：域名={host}，错误={err}")
        logger.info(f"BT之家(1lou)索引器注册完成：域名列表={', '.join(all_hosts)}")

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
    def _is_host_match(host: str, allowed_hosts: set) -> bool:
        pure = host.lower().lstrip(".")
        pure_no_www = pure[4:] if pure.startswith("www.") else pure
        for allowed in allowed_hosts:
            a = allowed.lower().lstrip(".")
            a_no_www = a[4:] if a.startswith("www.") else a
            if pure == a or pure_no_www == a_no_www:
                return True
        return False
