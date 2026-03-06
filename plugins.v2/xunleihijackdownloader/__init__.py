import re
import shutil
import threading
import time
import hashlib
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, Union
from urllib.parse import quote

import requests
from apscheduler.schedulers.background import BackgroundScheduler

import app.schemas as schemas
from app.core.config import settings
from app.log import logger
from app.plugins import _PluginBase
from app.schemas.types import TorrentStatus


class XunleiHijackDownloader(_PluginBase):
    plugin_name = "迅雷下载接管"
    plugin_desc = "接管 MoviePilot 下载到迅雷，并可自动搬运到监控目录。"
    plugin_icon = "https://raw.githubusercontent.com/yang124541/moviepilot-plugin/main/xunlei.png"
    plugin_version = "1.0.24"
    plugin_author = "yang124541"
    author_url = "https://github.com/yang124541/moviepilot-plugin"
    plugin_config_prefix = "xunleihijackdownloader_"
    plugin_order = 29
    auth_level = 2

    _enabled = False
    _hijack_download = True
    _fallback_to_builtin = True
    _base_url = ""
    _authorization = ""
    _pan_auth = ""
    _auto_refresh_pan_auth = True
    _file_id = ""
    _device_id = ""

    _move_enabled = False
    _source_download_dir = ""
    _target_watch_dir = ""
    _move_interval_minutes = 3
    _move_safe_seconds = 60

    _scheduler: Optional[BackgroundScheduler] = None
    _move_lock = threading.Lock()
    _moved_task_keys: Set[str] = set()
    _moved_task_order: List[str] = []
    _task_name_cache: Dict[str, str] = {}
    _max_moved_keys = 2000
    _last_request_error = ""

    def init_plugin(self, config: dict = None):
        self.stop_service()
        self._moved_task_order = self._load_moved_task_keys()
        self._moved_task_keys = set(self._moved_task_order)
        self._task_name_cache = {}
        self._auto_refresh_pan_auth = True
        if config:
            self._enabled = bool(config.get("enabled", False))
            self._hijack_download = bool(config.get("hijack_download", True))
            self._fallback_to_builtin = bool(config.get("fallback_to_builtin", True))
            self._base_url = self._normalize_base_url(config.get("base_url") or "")
            self._authorization = str(config.get("authorization") or "").strip()
            self._pan_auth = str(config.get("pan_auth") or "").strip()
            self._file_id = str(config.get("file_id") or "").strip()
            self._device_id = str(config.get("device_id") or "").strip()
            self._move_enabled = bool(config.get("move_enabled", False))
            self._source_download_dir = str(config.get("source_download_dir") or "").strip()
            self._target_watch_dir = str(config.get("target_watch_dir") or "").strip()
            self._move_interval_minutes = self._to_positive_int(config.get("move_interval_minutes"), 3)
            self._move_safe_seconds = self._to_non_negative_int(config.get("move_safe_seconds"), 60)

        if self._enabled and self._auto_refresh_pan_auth and not self._pan_auth:
            self._pan_auth = self._fetch_pan_auth() or self._pan_auth
        if self._enabled and self._move_enabled:
            self._start_move_scheduler()
        self._save_config()
        logger.info(
            f"XunleiHijack[v{self.plugin_version}] initialized: "
            f"enabled={self._enabled}, base_url={self._base_url}, move_enabled={self._move_enabled}"
        )

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return []

    def get_api(self) -> List[Dict[str, Any]]:
        return [
            {
                "path": "/task/start",
                "endpoint": self.api_start_task,
                "methods": ["GET"],
                "summary": "开始迅雷任务",
                "description": "在插件数据页手动开始指定任务",
            },
            {
                "path": "/task/pause",
                "endpoint": self.api_pause_task,
                "methods": ["GET"],
                "summary": "暂停迅雷任务",
                "description": "在插件数据页手动暂停指定任务",
            },
            {
                "path": "/task/delete",
                "endpoint": self.api_delete_task,
                "methods": ["GET"],
                "summary": "删除迅雷任务",
                "description": "在插件数据页手动删除指定任务",
            },
        ]

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
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {"component": "VSwitch", "props": {"model": "enabled", "label": "启用插件"}}
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {"component": "VSwitch", "props": {"model": "hijack_download", "label": "接管搜索/订阅下载"}}
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {"component": "VSwitch", "props": {"model": "fallback_to_builtin", "label": "失败回退内建下载器"}}
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
                                    {"component": "VAlert", "props": {"type": "info", "variant": "tonal", "text": "接管开启后，MoviePilot 的搜索下载与订阅下载会优先走迅雷。"}}
                                ],
                            }
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {"component": "VTextField", "props": {"model": "base_url", "label": "迅雷Docker地址", "placeholder": "http://192.168.2.3:2345"}}
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {"component": "VTextField", "props": {"model": "authorization", "label": "Authorization值"}}
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
                                    {"component": "VTextField", "props": {"model": "file_id", "label": "迅雷Docker容器file_id"}}
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {"component": "VTextField", "props": {"model": "device_id", "label": "设备 ID（可留空自动获取）"}}
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
                                    {"component": "VSwitch", "props": {"model": "move_enabled", "label": "下载完成自动搬运"}}
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
                                        "component": "VPathField",
                                        "props": {
                                            "model": "source_download_dir",
                                            "label": "迅雷下载目录（源）",
                                            "placeholder": "/downloads_ssd",
                                            "storage": "local",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VPathField",
                                        "props": {
                                            "model": "target_watch_dir",
                                            "label": "MoviePilot 监控目录（目标）",
                                            "placeholder": "/downloads_hdd/watch",
                                            "storage": "local",
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
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {"component": "VTextField", "props": {"model": "move_interval_minutes", "label": "搬运轮询间隔(分钟)", "type": "number"}}
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {"component": "VTextField", "props": {"model": "move_safe_seconds", "label": "完成后等待(秒)", "type": "number"}}
                                ],
                            },
                        ],
                    },
                ],
            }
        ], {
            "enabled": False,
            "hijack_download": True,
            "fallback_to_builtin": True,
            "base_url": "",
            "authorization": "",
            "file_id": "",
            "device_id": "",
            "move_enabled": False,
            "source_download_dir": "",
            "target_watch_dir": "",
            "move_interval_minutes": 3,
            "move_safe_seconds": 60,
        }

    def get_page(self) -> List[dict]:
        page: List[dict] = [
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
                                    "text": "展示迅雷任务实时状态：图片、文件图标、文件名、大小、剩余时间、速度、进度及开始/暂停/删除。已迁移任务自动隐藏。",
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
                        "props": {"cols": 12, "class": "d-flex justify-end"},
                        "content": [
                            {
                                "component": "VBtn",
                                "props": {
                                    "size": "small",
                                    "variant": "text",
                                    "color": "primary",
                                    "type": "button",
                                    "prependIcon": "mdi-refresh",
                                    "text": "刷新列表",
                                    "onclick": "window.location.reload()",
                                },
                            }
                        ],
                    }
                ],
            },
        ]

        if not self._enabled:
            page.append({
                "component": "VRow",
                "content": [
                    {
                        "component": "VCol",
                        "props": {"cols": 12},
                        "content": [
                            {
                                "component": "VAlert",
                                "props": {
                                    "type": "warning",
                                    "variant": "tonal",
                                    "text": "插件未启用，请先在配置页开启“启用插件”。",
                                },
                            }
                        ],
                    }
                ],
            })
            return page

        tasks = self._list_download_tasks()
        visible_tasks = [task for task in tasks if not self._is_moved_task(task)]
        if not visible_tasks:
            page.append({
                "component": "VRow",
                "content": [
                    {
                        "component": "VCol",
                        "props": {"cols": 12},
                        "content": [
                            {
                                "component": "VAlert",
                                "props": {
                                    "type": "success",
                                    "variant": "tonal",
                                    "text": "暂无可展示的迅雷任务（已迁移任务已过滤）。",
                                },
                            }
                        ],
                    }
                ],
            })
            return page

        header_cells = [
            {"component": "th", "props": {"text": "图片", "class": "text-left"}},
            {"component": "th", "props": {"text": "图标", "class": "text-left"}},
            {"component": "th", "props": {"text": "文件名", "class": "text-left"}},
            {"component": "th", "props": {"text": "大小", "class": "text-left"}},
            {"component": "th", "props": {"text": "剩余时间", "class": "text-left"}},
            {"component": "th", "props": {"text": "当前下载速度", "class": "text-left"}},
            {"component": "th", "props": {"text": "进度", "class": "text-left"}},
            {"component": "th", "props": {"text": "操作", "class": "text-left"}},
        ]
        body_rows = [self._build_task_row(task=task) for task in visible_tasks]

        page.append({
            "component": "VRow",
            "content": [
                {
                    "component": "VCol",
                    "props": {"cols": 12},
                    "content": [
                        {
                            "component": "VTable",
                            "props": {"hover": True, "class": "table-striped"},
                            "content": [
                                {"component": "thead", "content": [{"component": "tr", "content": header_cells}]},
                                {"component": "tbody", "content": body_rows},
                            ],
                        }
                    ],
                }
            ],
        })
        return page

    def api_start_task(self, task_id: str = "", hash: str = "") -> schemas.Response:
        return self._api_task_action(task_id=task_id or hash, action="start")

    def api_pause_task(self, task_id: str = "", hash: str = "") -> schemas.Response:
        return self._api_task_action(task_id=task_id or hash, action="pause")

    def api_delete_task(self, task_id: str = "", hash: str = "", delete_file: bool = True) -> schemas.Response:
        return self._api_task_action(task_id=task_id or hash, action="delete", delete_file=delete_file)

    def _api_task_action(self, task_id: str, action: str, delete_file: bool = True) -> schemas.Response:
        task_key = str(task_id or "").strip()
        if not task_key:
            return schemas.Response(success=False, message="任务ID不能为空。")
        if action != "delete" and task_key in self._moved_task_keys:
            return schemas.Response(success=False, message="任务已迁移，无法继续操作。")
        ok = self._operate_tasks(ids={task_key}, action=action, delete_file=bool(delete_file))
        if ok and action == "delete":
            self._remember_moved_key(task_key)
        action_name = {"start": "开始", "pause": "暂停", "delete": "删除"}.get(action, action)
        if ok:
            return schemas.Response(success=True, message=f"{action_name}任务成功。")
        return schemas.Response(success=False, message=f"{action_name}任务失败，请检查迅雷连接与认证。")

    def _build_task_row(self, task: Dict[str, Any]) -> Dict[str, Any]:
        plugin_id = self.__class__.__name__
        task_id = self._task_key(task)
        task_name = self._task_name(task) or task_id or "xunlei-task"
        task_done = self._is_task_completed(task)
        task_paused = self._is_task_paused(task)
        task_failed = self._is_task_failed(task)

        progress = self._task_progress(task)
        progress_text = f"{progress:.1f}%"
        size_text = self._format_bytes(self._task_size(task))
        left_time = self._task_left_time(task, progress) or "--"
        speed_text = self._task_speed_text(task, key="download_speed") or "0B/s"
        state_text = self._task_state_text(task)
        image_url = self._task_image_url(task)
        icon_name = self._task_file_icon(task_name=task_name, done=task_done)

        can_start = bool(task_id) and not task_done and (task_paused or task_failed)
        can_pause = bool(task_id) and not task_done and not task_paused and not task_failed
        can_delete = bool(task_id)
        quoted_id = quote(task_id or "", safe="")
        start_api = f"/plugin/{plugin_id}/task/start?task_id={quoted_id}&apikey={{apikey}}"
        pause_api = f"/plugin/{plugin_id}/task/pause?task_id={quoted_id}&apikey={{apikey}}"
        delete_api = f"/plugin/{plugin_id}/task/delete?task_id={quoted_id}&delete_file=true&apikey={{apikey}}"
        progress_color = "success" if task_done else ("warning" if task_paused else ("error" if task_failed else "primary"))

        image_node: Dict[str, Any]
        if image_url:
            image_node = {
                "component": "VImg",
                "props": {"src": image_url, "width": 72, "height": 40, "cover": True},
            }
        else:
            image_node = {
                "component": "VAvatar",
                "props": {"size": 40, "rounded": "sm", "color": "grey-lighten-3"},
                "content": [{"component": "VIcon", "props": {"icon": "mdi-image-off-outline", "size": 20}}],
            }

        return {
            "component": "tr",
            "content": [
                {
                    "component": "td",
                    "content": [image_node],
                },
                {
                    "component": "td",
                    "content": [{"component": "VIcon", "props": {"icon": icon_name, "size": 20}}],
                },
                {
                    "component": "td",
                    "content": [
                        {"component": "div", "props": {"text": task_name, "class": "text-body-2"}},
                        {"component": "div", "props": {"text": state_text, "class": "text-caption text-medium-emphasis"}},
                    ],
                },
                {"component": "td", "props": {"text": size_text}},
                {"component": "td", "props": {"text": left_time}},
                {"component": "td", "props": {"text": speed_text}},
                {
                    "component": "td",
                    "props": {"style": "min-width: 160px;"},
                    "content": [
                        {
                            "component": "VProgressLinear",
                            "props": {"modelValue": progress, "height": 8, "rounded": True, "color": progress_color},
                        },
                        {"component": "div", "props": {"text": progress_text, "class": "text-caption mt-1"}},
                    ],
                },
                {
                    "component": "td",
                    "content": [
                        {
                            "component": "div",
                            "props": {"class": "d-flex flex-wrap ga-1"},
                            "content": [
                                self._build_task_action_button(
                                    text="开始",
                                    color="success",
                                    disabled=not can_start,
                                    api_path=start_api,
                                    success_message="开始任务成功，请点击刷新查看状态。",
                                    failure_message="开始任务失败。",
                                ),
                                self._build_task_action_button(
                                    text="暂停",
                                    color="warning",
                                    disabled=not can_pause,
                                    api_path=pause_api,
                                    success_message="暂停任务成功，请点击刷新查看状态。",
                                    failure_message="暂停任务失败。",
                                ),
                                self._build_task_action_button(
                                    text="删除",
                                    color="error",
                                    disabled=not can_delete,
                                    api_path=delete_api,
                                    success_message="删除任务成功，请点击刷新查看状态。",
                                    failure_message="删除任务失败。",
                                ),
                            ],
                        }
                    ],
                },
            ],
        }

    @staticmethod
    def _build_task_action_button(text: str, color: str, disabled: bool, api_path: str,
                                  success_message: str, failure_message: str) -> Dict[str, Any]:
        button = {
            "component": "VBtn",
            "props": {
                "size": "x-small",
                "variant": "text",
                "color": color,
                "text": text,
                "disabled": bool(disabled),
            },
        }
        if not disabled:
            button["events"] = {
                "click": {
                    "api": api_path,
                    "method": "get",
                    "success_message": success_message,
                    "failure_message": failure_message,
                }
            }
        return button

    def get_module(self) -> Dict[str, Any]:
        if not self._enabled:
            return {}
        module_map = {
            "list_torrents": self.list_torrents,
            "start_torrents": self.start_torrents,
            "stop_torrents": self.stop_torrents,
            "remove_torrents": self.remove_torrents,
            "downloader_info": self.downloader_info,
            "transfer_completed": self.transfer_completed,
        }
        if self._hijack_download:
            module_map["download"] = self.download
        return module_map

    def stop_service(self):
        if self._scheduler:
            self._scheduler.remove_all_jobs()
            if self._scheduler.running:
                self._scheduler.shutdown()
            self._scheduler = None

    def download(self,
                 content: Union[Path, str, bytes],
                 download_dir: Path,
                 cookie: str,
                 episodes: Set[int] = None,
                 category: Optional[str] = None,
                 label: Optional[str] = None,
                 downloader: Optional[str] = None
                 ) -> Optional[Tuple[Optional[str], Optional[str], Optional[str], str]]:
        magnet = self._normalize_magnet(content)
        if not magnet:
            if self._fallback_to_builtin:
                logger.warn(f"XunleiHijack[v{self.plugin_version}] fallback builtin: 不支持当前下载内容类型，未解析出 magnet。")
                return None
            return "xunlei", None, None, "迅雷接管失败：仅支持磁力链接。"
        task_id, err = self._add_task(magnet)
        if not task_id:
            if self._fallback_to_builtin:
                logger.warn(f"XunleiHijack[v{self.plugin_version}] fallback builtin: {err or '迅雷添加任务失败'}")
                return None
            return "xunlei", None, None, err or "迅雷添加任务失败。"
        return "xunlei", task_id, "NoSubfolder", "添加下载成功"

    def list_torrents(self,
                      status: TorrentStatus = None,
                      hashs: Union[list, str] = None,
                      downloader: Optional[str] = None
                      ) -> Optional[List[Union[schemas.TransferTorrent, schemas.DownloadingTorrent]]]:
        if downloader and not self._is_xunlei_downloader(downloader):
            return None
        if status not in (TorrentStatus.TRANSFER, TorrentStatus.DOWNLOADING):
            return None

        tasks = self._list_download_tasks()
        if not tasks:
            return []
        hash_set = self._normalize_hashs(hashs)

        results: List[Union[schemas.TransferTorrent, schemas.DownloadingTorrent]] = []
        for task in tasks:
            task_hash = self._task_key(task) or ""
            if hash_set and task_hash not in hash_set:
                continue
            if self._is_moved_task(task):
                continue

            title = self._task_name(task) or task_hash or "xunlei-task"
            progress = self._task_progress(task)
            done = self._is_task_completed(task)
            source_path = self._resolve_source_path(Path(self._source_download_dir), title) if self._source_download_dir else None
            if status == TorrentStatus.TRANSFER:
                # 开启自动搬运时，不再向转移链路暴露迅雷任务，避免重复搬运。
                if self._move_enabled:
                    continue
                if not done:
                    continue
                if source_path and source_path.exists():
                    path = source_path
                elif self._source_download_dir:
                    path = Path(self._source_download_dir) / Path(title).name
                else:
                    path = None
                if not path:
                    continue
                results.append(schemas.TransferTorrent(
                    downloader="xunlei",
                    title=title,
                    path=path,
                    hash=task_hash,
                    size=int(self._task_size(task) or 0),
                    progress=progress,
                    state="completed" if done else "downloading",
                    tags=""
                ))
            elif status == TorrentStatus.DOWNLOADING:
                if done:
                    continue
                results.append(schemas.DownloadingTorrent(
                    downloader="xunlei",
                    hash=task_hash,
                    title=title,
                    name=title,
                    size=float(self._task_size(task) or 0),
                    progress=progress,
                    state="downloading",
                    dlspeed=self._task_speed_text(task, key="download_speed"),
                    upspeed=self._task_speed_text(task, key="upload_speed"),
                    left_time=self._task_left_time(task, progress),
                ))
        return results

    def start_torrents(self, hashs: Union[list, str], downloader: Optional[str] = None) -> Optional[bool]:
        ids = self._normalize_hashs(hashs)
        should_handle = self._should_handle_torrent_control(ids=ids, downloader=downloader)
        if should_handle is None:
            return None
        if not should_handle:
            return False
        return self._operate_tasks(ids=ids, action="start")

    def stop_torrents(self, hashs: Union[list, str], downloader: Optional[str] = None) -> Optional[bool]:
        ids = self._normalize_hashs(hashs)
        should_handle = self._should_handle_torrent_control(ids=ids, downloader=downloader)
        if should_handle is None:
            return None
        if not should_handle:
            return False
        return self._operate_tasks(ids=ids, action="pause")

    def remove_torrents(self, hashs: Union[str, list], delete_file: Optional[bool] = True,
                        downloader: Optional[str] = None) -> Optional[bool]:
        ids = self._normalize_hashs(hashs)
        should_handle = self._should_handle_torrent_control(ids=ids, downloader=downloader)
        if should_handle is None:
            return None
        if not should_handle:
            return False
        ok = self._operate_tasks(ids=ids, action="delete", delete_file=bool(delete_file))
        if ok:
            for _id in ids:
                self._remember_moved_key(_id)
        return ok

    def downloader_info(self, downloader: Optional[str] = None) -> Optional[List[schemas.DownloaderInfo]]:
        if downloader and not self._is_xunlei_downloader(downloader):
            return None
        tasks = self._list_download_tasks()
        dl_speed = 0.0
        up_speed = 0.0
        for task in tasks:
            dl_speed += float(self._task_number(task, "download_speed") or 0)
            up_speed += float(self._task_number(task, "upload_speed") or 0)
        return [schemas.DownloaderInfo(
            download_speed=dl_speed,
            upload_speed=up_speed,
            download_size=0.0,
            upload_size=0.0,
            free_space=0.0
        )]

    def transfer_completed(self, hashs: str, downloader: Optional[str] = None) -> None:
        if downloader and not self._is_xunlei_downloader(downloader):
            return None
        key = str(hashs or "").strip()
        if key:
            self._remember_moved_key(key)
        return None

    def _save_config(self) -> None:
        self.update_config({
            "enabled": self._enabled,
            "hijack_download": self._hijack_download,
            "fallback_to_builtin": self._fallback_to_builtin,
            "base_url": self._base_url,
            "authorization": self._authorization,
            "pan_auth": self._pan_auth,
            "file_id": self._file_id,
            "device_id": self._device_id,
            "move_enabled": self._move_enabled,
            "source_download_dir": self._source_download_dir,
            "target_watch_dir": self._target_watch_dir,
            "move_interval_minutes": self._move_interval_minutes,
            "move_safe_seconds": self._move_safe_seconds,
        })

    def _load_moved_task_keys(self) -> List[str]:
        try:
            payload = self.get_data("moved_task_keys")
            if isinstance(payload, list):
                ordered: List[str] = []
                seen: Set[str] = set()
                for item in payload:
                    token = str(item or "").strip()
                    if not token or token in seen:
                        continue
                    seen.add(token)
                    ordered.append(token)
                if len(ordered) > self._max_moved_keys:
                    ordered = ordered[-self._max_moved_keys:]
                return ordered
        except Exception:
            pass
        return []

    def _remember_moved_key(self, key: str) -> None:
        token = str(key or "").strip()
        if not token:
            return
        if token in self._moved_task_keys:
            self._moved_task_order = [x for x in self._moved_task_order if x != token]
        else:
            self._moved_task_keys.add(token)
        self._moved_task_order.append(token)
        if len(self._moved_task_order) > self._max_moved_keys:
            self._moved_task_order = self._moved_task_order[-self._max_moved_keys:]
            self._moved_task_keys = set(self._moved_task_order)
        try:
            self.save_data("moved_task_keys", list(self._moved_task_order))
        except Exception as err:
            logger.warn(f"XunleiHijack persist moved keys failed: {err}")

    def _start_move_scheduler(self) -> None:
        self._scheduler = BackgroundScheduler(timezone=settings.TZ)
        self._scheduler.add_job(
            self._move_completed_downloads,
            "interval",
            minutes=max(1, int(self._move_interval_minutes)),
            id="xunlei_hijack_move_interval",
            replace_existing=True
        )
        self._scheduler.add_job(
            self._move_completed_downloads,
            "date",
            run_date=datetime.now() + timedelta(seconds=20),
            id="xunlei_hijack_move_bootstrap",
            replace_existing=True
        )
        self._scheduler.start()
        logger.info(
            f"XunleiHijack move scheduler started: interval={self._move_interval_minutes}m, "
            f"source={self._source_download_dir}, target={self._target_watch_dir}"
        )

    def _get_headers(self) -> Dict[str, str]:
        pan_auth = self._pan_auth
        if self._auto_refresh_pan_auth and not pan_auth:
            pan_auth = self._fetch_pan_auth() or ""
            if pan_auth:
                self._pan_auth = pan_auth
                self._save_config()
        headers = {
            "Accept": "*/*",
            "Authorization": self._authorization,
            "Content-Type": "application/json",
            "Origin": self._base_url,
            "Referer": f"{self._base_url}/webman/3rdparty/pan-xunlei-com/index.cgi/",
            "User-Agent": settings.USER_AGENT,
            "device-space": "",
        }
        if pan_auth:
            headers["pan-auth"] = pan_auth
        return headers

    def _request_json(self,
                      method: str,
                      url: str,
                      headers: Optional[Dict[str, str]] = None,
                      payload: Optional[Dict[str, Any]] = None,
                      timeout: int = 20,
                      retry_auth: bool = True,
                      retry_count: int = 2) -> Tuple[Optional[requests.Response], Any]:
        req_headers = dict(headers or self._get_headers())

        def _once(local_headers: Dict[str, str]) -> Tuple[Optional[requests.Response], Any, str]:
            try:
                kwargs: Dict[str, Any] = {"headers": local_headers, "timeout": timeout}
                if payload is not None:
                    kwargs["json"] = payload
                resp = requests.request(method=method.upper(), url=url, **kwargs)
                obj: Any = {}
                if resp.text:
                    try:
                        obj = resp.json()
                    except Exception:
                        obj = {}
                return resp, obj, ""
            except Exception as err:
                detail = str(err).strip() or repr(err)
                return None, {}, f"{type(err).__name__}: {detail}"

        resp: Optional[requests.Response] = None
        obj: Any = {}
        err_text = ""
        for attempt in range(max(1, int(retry_count) + 1)):
            resp, obj, err_text = _once(req_headers)
            if resp is not None:
                self._last_request_error = ""
                break
            self._last_request_error = err_text
            if attempt < max(1, int(retry_count) + 1) - 1:
                time.sleep(min(0.5 * (attempt + 1), 1.5))

        if (
            retry_auth
            and self._auto_refresh_pan_auth
            and self._authorization
            and self._should_refresh_pan_auth(resp=resp, obj=obj)
        ):
            fresh = self._fetch_pan_auth()
            if fresh and fresh != req_headers.get("pan-auth"):
                self._pan_auth = fresh
                self._save_config()
                req_headers["pan-auth"] = fresh
                resp, obj, err_text = _once(req_headers)
                if resp is None:
                    self._last_request_error = err_text
                else:
                    self._last_request_error = ""
        if resp is None:
            if not self._last_request_error:
                self._last_request_error = "unknown-request-error"
            logger.warn(
                f"XunleiHijack[v{self.plugin_version}] request failed: {method.upper()} {url} -> {self._last_request_error}"
            )
        elif not resp.ok:
            body_hint = ""
            try:
                body_hint = (resp.text or "").strip().replace("\n", " ")[:200]
            except Exception:
                body_hint = ""
            self._last_request_error = f"HTTP {resp.status_code}" + (f" body={body_hint}" if body_hint else "")
        return resp, obj

    def _fetch_pan_auth(self) -> Optional[str]:
        if not self._base_url or not self._authorization:
            return None
        try:
            url = f"{self._base_url}/webman/3rdparty/pan-xunlei-com/index.cgi/"
            resp = requests.get(url, headers={"Authorization": self._authorization}, timeout=15)
            resp.raise_for_status()
            m = re.search(r'uiauth\(.*?\)\s*{\s*return\s*"([^"]+)"', resp.text)
            if m:
                return str(m.group(1)).strip()
            token = resp.cookies.get("pan_auth")
            if token:
                return str(token).strip()
        except Exception as err:
            logger.warn(f"XunleiHijack fetch pan_auth failed: {err}")
        return None

    def _fetch_device_id(self, force_refresh: bool = False) -> Optional[str]:
        if self._device_id and not force_refresh:
            return self._device_id
        if not self._base_url or not self._authorization:
            return None
        try:
            url = (
                f"{self._base_url}/webman/3rdparty/pan-xunlei-com/index.cgi/drive/v1/tasks"
                f"?type=user%23runner&device_space="
            )
            resp, obj = self._request_json(
                method="GET",
                url=url,
                headers={**self._get_headers(), "device-space": ""},
                timeout=20,
                retry_auth=True
            )
            if not resp or not resp.ok or not isinstance(obj, dict):
                raise ValueError(f"http={resp.status_code if resp else 'request-failed'} {self._last_request_error}")
            tasks = obj.get("tasks")
            if isinstance(tasks, list):
                for task in tasks:
                    if not isinstance(task, dict):
                        continue
                    params = task.get("params") if isinstance(task.get("params"), dict) else {}
                    token = str(params.get("target") or task.get("target") or "").strip()
                    if token:
                        self._device_id = token
                        self._save_config()
                        logger.info(f"XunleiHijack[v{self.plugin_version}] got device_id from runner tasks: {token}")
                        return token
        except Exception as err:
            logger.warn(f"XunleiHijack[v{self.plugin_version}] fetch device_id failed: {err}")
        return None

    def _add_task(self, magnet: str) -> Tuple[Optional[str], Optional[str]]:
        if not self._base_url:
            return None, "迅雷地址未配置。"
        if not self._authorization:
            return None, "Authorization 未配置。"
        if not self._file_id:
            return None, "file_id 未配置。"
        if not self._fetch_device_id(force_refresh=True):
            return None, "device_id 未配置且自动获取失败。"

        headers = self._get_headers()
        if self._auto_refresh_pan_auth and not headers.get("pan-auth"):
            return None, "pan_auth 自动获取失败，请检查 Authorization/迅雷地址 是否正确。"

        analysis = self._analyze_magnet(magnet, headers)
        file_name = analysis.get("name") or f"xunlei-{int(time.time())}"

        def _build_payload(device_id: str) -> Dict[str, Any]:
            params = {
                "parent_folder_id": self._file_id,
                "url": magnet,
                "target": device_id,
            }
            total_count = int(analysis.get("total_count") or 0)
            indices = analysis.get("indices") or []
            if total_count and indices:
                params["total_file_count"] = str(total_count)
                params["sub_file_index"] = ",".join(indices)
            payload = {
                "params": params,
                "name": file_name,
                "type": "user#download-url",
                "space": device_id,
                "file_name": file_name,
            }
            total_size = int(analysis.get("total_size") or 0)
            if total_size > 0:
                payload["file_size"] = str(total_size)
            return payload

        try:
            url = f"{self._base_url}/webman/3rdparty/pan-xunlei-com/index.cgi/drive/v1/task"

            def _submit_once(device_id: str) -> Tuple[Optional[requests.Response], Any]:
                payload = _build_payload(device_id)
                # 对齐 liqman：device-space 头固定为空，实际空间走 payload 中的 target/space。
                req_headers = {**headers, "device-space": ""}
                return self._request_json(
                    method="POST",
                    url=url,
                    headers=req_headers,
                    payload=payload,
                    timeout=30,
                    retry_auth=True,
                )

            def _resolve_fail(resp: Optional[requests.Response], data: Any, device_id: str) -> Tuple[Optional[str], bool]:
                merged = self._merge_error_texts(data)
                if "task_create_count_limit" in merged or "任务创建次数达到上限" in merged:
                    logger.warn(
                        f"XunleiHijack[v{self.plugin_version}] add task limited: device={device_id}, {self._last_request_error}"
                    )
                    return "迅雷任务创建失败：任务创建次数达到上限，请稍后重试。", False
                if "space_name_invalid" in merged:
                    logger.warn(
                        f"XunleiHijack[v{self.plugin_version}] add task invalid space: device={device_id}, {self._last_request_error}"
                    )
                    return "迅雷任务创建失败：device_id 对应空间无效，请重新抓取参数。", False
                if "device_space_not_active" in merged:
                    logger.warn(
                        f"XunleiHijack[v{self.plugin_version}] add task inactive space: device={device_id}, {self._last_request_error}"
                    )
                    return None, True
                if not resp:
                    return f"迅雷任务创建请求失败：网络请求失败（{self._last_request_error or 'unknown'}）", False
                if not resp.ok:
                    return f"迅雷任务创建请求失败：HTTP {resp.status_code}（{self._last_request_error or 'unknown'}）", False
                err = self._extract_api_error(data)
                if err:
                    return f"迅雷任务创建失败：{err}", False
                return "迅雷任务创建失败：接口返回异常。", False

            first_device = str(self._device_id or "").strip()
            resp, data = _submit_once(first_device)
            if resp and resp.ok:
                task_id = self._task_id(data)
                if task_id:
                    self._task_name_cache[task_id] = file_name
                    return task_id, None
            first_err, allow_refresh_retry = _resolve_fail(resp=resp, data=data, device_id=first_device)
            if not allow_refresh_retry:
                return None, first_err

            refresh_device = self._fetch_device_id(force_refresh=True)
            refresh_device = str(refresh_device or "").strip()
            if not refresh_device or refresh_device == first_device:
                return None, "迅雷任务创建失败：当前 device_space 未激活，且刷新 device_id 无变化。"

            self._device_id = refresh_device
            self._save_config()
            logger.info(
                f"XunleiHijack[v{self.plugin_version}] refresh submit device_id: {first_device or 'EMPTY'} -> {refresh_device}"
            )
            resp2, data2 = _submit_once(refresh_device)
            if resp2 and resp2.ok:
                task_id = self._task_id(data2)
                if task_id:
                    self._task_name_cache[task_id] = file_name
                    return task_id, None
            second_err, _ = _resolve_fail(resp=resp2, data=data2, device_id=refresh_device)
            return None, second_err
        except Exception as err:
            return None, f"迅雷任务创建请求失败：{err}"

    def _analyze_magnet(self, magnet: str, headers: Dict[str, str]) -> Dict[str, Any]:
        result = {"name": "", "total_size": 0, "total_count": 0, "indices": []}
        try:
            url = f"{self._base_url}/webman/3rdparty/pan-xunlei-com/index.cgi/drive/v1/resource/list"
            resp, obj = self._request_json(
                method="POST",
                url=url,
                headers=headers,
                payload={"page_size": 1000, "urls": magnet},
                timeout=30,
                retry_auth=True,
            )
            if not resp or not resp.ok:
                return result
            resources = self._extract_resources(obj)
            if not resources:
                return result
            result["name"] = str(resources[0].get("name") or "").strip()
            files = self._flatten_files(resources)
            if not files:
                return result
            indices, total_size = [], 0
            for idx, item in enumerate(files):
                file_index = item.get("file_index")
                if file_index is None:
                    file_index = idx
                indices.append(str(file_index))
                total_size += int(item.get("file_size") or 0)
            result["indices"] = indices
            result["total_size"] = total_size
            result["total_count"] = len(files)
        except Exception as err:
            logger.warn(f"XunleiHijack analyze magnet failed: {err}")
        return result

    def _move_completed_downloads(self):
        if not self._enabled or not self._move_enabled:
            return
        if not self._source_download_dir or not self._target_watch_dir:
            return
        if not self._move_lock.acquire(blocking=False):
            return
        try:
            source_root = Path(self._source_download_dir)
            target_root = Path(self._target_watch_dir)
            if not source_root.exists():
                return
            target_root.mkdir(parents=True, exist_ok=True)
            tasks = self._list_download_tasks()
            if not tasks:
                return
            now_ts = time.time()
            for task in tasks:
                if not self._is_task_completed(task):
                    continue
                key = self._task_key(task) or self._task_name(task)
                if not key or key in self._moved_task_keys:
                    continue
                src = self._resolve_source_path(source_root, self._task_name(task))
                if not src or not src.exists():
                    src = self._resolve_source_path_fallback(source_root, self._task_name(task))
                if not src or not src.exists():
                    continue
                if self._move_safe_seconds > 0 and now_ts - src.stat().st_mtime < self._move_safe_seconds:
                    continue
                dst = self._dedupe_target(target_root / src.name)
                shutil.move(str(src), str(dst))
                self._remember_moved_key(key)
                logger.info(f"XunleiHijack moved: {src} -> {dst}")
        except Exception as err:
            logger.error(f"XunleiHijack move job failed: {err}")
        finally:
            self._move_lock.release()

    def _list_download_tasks(self) -> List[Dict[str, Any]]:
        try:
            headers = self._get_headers()
            device_id = self._fetch_device_id() or self._device_id
            url = (
                f"{self._base_url}/webman/3rdparty/pan-xunlei-com/index.cgi/drive/v1/tasks"
                f"?type=user%23download-url&device_space="
            )
            resp, obj = self._request_json(
                method="GET",
                url=url,
                headers={**headers, "device-space": ""},
                timeout=20,
                retry_auth=True
            )
            if not resp or not resp.ok:
                raise ValueError(f"http={resp.status_code if resp else 'request-failed'} {self._last_request_error}")
            if isinstance(obj, dict):
                tasks = obj.get("tasks")
                if isinstance(tasks, list):
                    if not device_id:
                        for task in tasks:
                            if isinstance(task, dict):
                                params = task.get("params") if isinstance(task.get("params"), dict) else {}
                                target = str(params.get("target") or task.get("target") or "").strip()
                                if target:
                                    self._device_id = target
                                    self._save_config()
                                    break
                    return [x for x in tasks if isinstance(x, dict)]
                list_obj = obj.get("list")
                if isinstance(list_obj, list):
                    return [x for x in list_obj if isinstance(x, dict)]
        except Exception as err:
            logger.warn(f"XunleiHijack[v{self.plugin_version}] list tasks failed: {err}")
        return []

    def _operate_tasks(self, ids: Set[str], action: str, delete_file: bool = True) -> bool:
        if not ids:
            return False
        if not self._base_url:
            return False
        if not self._fetch_device_id():
            return False
        headers = self._get_headers()
        if self._auto_refresh_pan_auth and not headers.get("pan-auth"):
            return False

        payloads = [
            {
                "action": action,
                "ids": list(ids),
                "device_space": self._device_id,
                "delete_file": bool(delete_file),
            },
            {
                "type": action,
                "task_ids": list(ids),
                "device_space": self._device_id,
                "delete_file": bool(delete_file),
            }
        ]
        urls = [
            f"{self._base_url}/webman/3rdparty/pan-xunlei-com/index.cgi/drive/v1/task",
            f"{self._base_url}/webman/3rdparty/pan-xunlei-com/index.cgi/drive/v1/tasks",
            f"{self._base_url}/webman/3rdparty/pan-xunlei-com/index.cgi/drive/v1/task/action",
        ]

        for url in urls:
            for payload in payloads:
                try:
                    resp, obj = self._request_json(
                        method="POST",
                        url=url,
                        headers={**headers, "device-space": self._device_id},
                        payload=payload,
                        timeout=20,
                        retry_auth=True,
                    )
                    if not resp or not resp.ok:
                        continue
                    if not self._is_operation_success(obj=obj, ids=ids):
                        continue
                    return True
                except Exception:
                    continue
        return False

    @staticmethod
    def _is_operation_success(obj: Any, ids: Set[str]) -> bool:
        if not isinstance(obj, dict):
            return False
        if obj.get("error") or obj.get("err"):
            return False

        code_ok = False
        if isinstance(obj.get("success"), bool):
            return bool(obj.get("success"))
        if isinstance(obj.get("result"), bool):
            return bool(obj.get("result"))

        code = obj.get("code")
        if code is not None:
            try:
                code_ok = int(code) in (0, 200)
                if not code_ok:
                    return False
            except Exception:
                pass

        id_hints: Set[str] = set()
        for key in ("id", "task_id", "gid"):
            value = obj.get(key)
            if value:
                id_hints.add(str(value).strip())
        for key in ("ids", "task_ids", "success_ids", "updated_ids"):
            value = obj.get(key)
            if isinstance(value, list):
                for item in value:
                    token = str(item or "").strip()
                    if token:
                        id_hints.add(token)
            elif value:
                id_hints.add(str(value).strip())
        data = obj.get("data")
        if isinstance(data, dict):
            for key in ("id", "task_id", "gid"):
                value = data.get(key)
                if value:
                    id_hints.add(str(value).strip())
            for key in ("ids", "task_ids", "success_ids", "updated_ids"):
                value = data.get(key)
                if isinstance(value, list):
                    for item in value:
                        token = str(item or "").strip()
                        if token:
                            id_hints.add(token)

        if ids and id_hints:
            return len(ids.intersection(id_hints)) > 0
        if code_ok:
            return True
        # 没有明确ID回执时，至少要求显式成功字段
        return bool(obj.get("ok") is True or obj.get("status") in ("ok", "success"))

    @staticmethod
    def _is_task_completed(task: Dict[str, Any]) -> bool:
        values = []
        for key in ("phase", "status", "state"):
            value = task.get(key)
            if value is not None:
                values.append(str(value).strip().lower())
        params = task.get("params")
        if isinstance(params, dict):
            for key in ("phase", "status", "state"):
                value = params.get(key)
                if value is not None:
                    values.append(str(value).strip().lower())
        for text in values:
            if any(k in text for k in ("complete", "completed", "finished", "success", "done", "phase_type_complete")):
                return True
        progress = task.get("progress")
        if progress is None and isinstance(params, dict):
            progress = params.get("progress")
        if progress is not None:
            try:
                return float(progress) >= 100
            except Exception:
                pass
        return False

    def _task_name(self, task: Dict[str, Any]) -> str:
        for key in ("file_name", "name", "title"):
            value = task.get(key)
            if value:
                return str(value).strip()
        params = task.get("params")
        if isinstance(params, dict):
            for key in ("file_name", "name", "title"):
                value = params.get(key)
                if value:
                    return str(value).strip()
        task_id = self._task_key(task)
        if task_id:
            return str(self._task_name_cache.get(task_id) or "").strip()
        return ""

    def _task_progress(self, task: Dict[str, Any]) -> float:
        value = self._task_number(task, "progress")
        if value is None:
            return 0.0
        try:
            v = float(value)
            if v <= 1:
                return round(v * 100, 2)
            return max(0.0, min(100.0, v))
        except Exception:
            return 0.0

    def _task_size(self, task: Dict[str, Any]) -> int:
        value = self._task_number(task, "file_size")
        if value is None:
            value = self._task_number(task, "size")
        try:
            return int(value or 0)
        except Exception:
            return 0

    def _task_number(self, task: Dict[str, Any], key: str) -> Optional[float]:
        value = task.get(key)
        params = task.get("params") if isinstance(task.get("params"), dict) else {}
        if value is None:
            value = params.get(key)
        if value is None:
            return None
        try:
            return float(value)
        except Exception:
            return None

    def _task_text(self, task: Dict[str, Any], key: str) -> str:
        value = task.get(key)
        params = task.get("params") if isinstance(task.get("params"), dict) else {}
        if value is None:
            value = params.get(key)
        if value is None:
            return ""
        return str(value).strip()

    @staticmethod
    def _task_status_values(task: Dict[str, Any]) -> List[str]:
        values: List[str] = []
        for key in ("phase", "status", "state"):
            value = task.get(key)
            if value is not None:
                values.append(str(value).strip().lower())
        params = task.get("params")
        if isinstance(params, dict):
            for key in ("phase", "status", "state"):
                value = params.get(key)
                if value is not None:
                    values.append(str(value).strip().lower())
        return values

    def _task_speed_text(self, task: Dict[str, Any], key: str) -> Optional[str]:
        value = self._task_number(task, key)
        if value is None:
            return None
        try:
            size = float(value)
            units = ["B/s", "KB/s", "MB/s", "GB/s"]
            idx = 0
            while size >= 1024 and idx < len(units) - 1:
                size /= 1024.0
                idx += 1
            return f"{size:.1f}{units[idx]}"
        except Exception:
            return None

    def _task_left_time(self, task: Dict[str, Any], progress: float) -> Optional[str]:
        for key in ("left_time", "remaining_time", "remain_time", "time_remaining", "eta", "predict_left_time"):
            value = self._task_number(task, key)
            if value is not None and value >= 0:
                return self._format_seconds(value)
            text_value = self._task_text(task, key)
            if text_value:
                return text_value
        total_size = float(self._task_size(task) or 0)
        speed = float(self._task_number(task, "download_speed") or 0)
        if total_size <= 0 or speed <= 0:
            return None
        left_bytes = total_size * max(0.0, 100.0 - progress) / 100.0
        if left_bytes <= 0:
            return "00:00:00"
        return self._format_seconds(left_bytes / speed)

    def _task_state_text(self, task: Dict[str, Any]) -> str:
        if self._is_task_completed(task):
            return "已完成"
        if self._is_task_failed(task):
            return "失败"
        if self._is_task_paused(task):
            return "已暂停"
        for text in self._task_status_values(task):
            if any(k in text for k in ("waiting", "wait", "pending", "queue")):
                return "排队中"
        return "下载中"

    def _is_task_paused(self, task: Dict[str, Any]) -> bool:
        for text in self._task_status_values(task):
            if any(k in text for k in ("pause", "paused", "suspend", "stop", "stopped", "halt")):
                return True
        return False

    def _is_task_failed(self, task: Dict[str, Any]) -> bool:
        for text in self._task_status_values(task):
            if any(k in text for k in ("fail", "failed", "error", "invalid")):
                return True
        return False

    def _task_image_url(self, task: Dict[str, Any]) -> str:
        for key in ("icon_link", "thumbnail", "thumb", "cover", "poster", "image", "image_url", "icon"):
            value = self._task_text(task, key)
            if value and re.match(r"^https?://", value, flags=re.IGNORECASE):
                return value
        return ""

    @staticmethod
    def _task_file_icon(task_name: str, done: bool = False) -> str:
        suffix = Path(str(task_name or "")).suffix.lower()
        if done:
            return "mdi-check-circle-outline"
        if suffix in (".mkv", ".mp4", ".avi", ".mov", ".flv", ".wmv", ".ts", ".m2ts"):
            return "mdi-file-video-outline"
        if suffix in (".srt", ".ass", ".ssa", ".sub"):
            return "mdi-file-document-outline"
        if suffix in (".rar", ".zip", ".7z", ".tar", ".gz"):
            return "mdi-folder-zip-outline"
        if suffix in (".jpg", ".jpeg", ".png", ".webp", ".gif"):
            return "mdi-file-image-outline"
        if suffix in (".torrent",):
            return "mdi-file-download-outline"
        return "mdi-file-outline"

    def _is_moved_task(self, task: Dict[str, Any]) -> bool:
        task_key = self._task_key(task)
        task_name = self._task_name(task)
        if task_key and task_key in self._moved_task_keys:
            return True
        if task_name and task_name in self._moved_task_keys:
            return True
        return False

    @staticmethod
    def _format_seconds(seconds: float) -> str:
        try:
            s = int(max(0, float(seconds)))
        except Exception:
            return "--"
        day, rem = divmod(s, 86400)
        hour, rem = divmod(rem, 3600)
        minute, second = divmod(rem, 60)
        if day > 0:
            return f"{day}d {hour:02d}:{minute:02d}:{second:02d}"
        return f"{hour:02d}:{minute:02d}:{second:02d}"

    @staticmethod
    def _format_bytes(size: int) -> str:
        try:
            value = float(size or 0)
        except Exception:
            value = 0.0
        units = ["B", "KB", "MB", "GB", "TB"]
        idx = 0
        while value >= 1024 and idx < len(units) - 1:
            value /= 1024.0
            idx += 1
        if idx == 0:
            return f"{int(value)}{units[idx]}"
        return f"{value:.2f}{units[idx]}"

    @staticmethod
    def _task_id(data: Any) -> str:
        if isinstance(data, list):
            for item in data:
                ret = XunleiHijackDownloader._task_id(item)
                if ret:
                    return ret
        if not isinstance(data, dict):
            return ""
        for key in ("task_id", "id", "gid"):
            value = data.get(key)
            if value:
                return str(value)
        for key in ("task", "data", "result"):
            ret = XunleiHijackDownloader._task_id(data.get(key))
            if ret:
                return ret
        tasks = data.get("tasks")
        if isinstance(tasks, list):
            ret = XunleiHijackDownloader._task_id(tasks)
            if ret:
                return ret
        return ""

    def _task_key(self, task: Dict[str, Any]) -> str:
        return self._task_id(task)

    @staticmethod
    def _is_xunlei_downloader(downloader: str) -> bool:
        text = str(downloader or "").strip().lower()
        return text in ("xunlei", "迅雷", "迅雷下载接管")

    @staticmethod
    def _normalize_hashs(hashs: Union[list, str]) -> Set[str]:
        if hashs is None:
            return set()
        if isinstance(hashs, str):
            token = str(hashs).strip()
            return {token} if token else set()
        ret = set()
        for item in hashs:
            token = str(item or "").strip()
            if token:
                ret.add(token)
        return ret

    @staticmethod
    def _extract_resources(obj: Any) -> List[Dict[str, Any]]:
        if not isinstance(obj, dict):
            return []
        list_obj = obj.get("list")
        if isinstance(list_obj, dict) and isinstance(list_obj.get("resources"), list):
            return [x for x in list_obj.get("resources") if isinstance(x, dict)]
        if isinstance(obj.get("resources"), list):
            return [x for x in obj.get("resources") if isinstance(x, dict)]
        return []

    def _flatten_files(self, resources: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        ret: List[Dict[str, Any]] = []
        for item in resources:
            if not isinstance(item, dict):
                continue
            if item.get("is_dir"):
                sub = item.get("dir") if isinstance(item.get("dir"), dict) else {}
                res = sub.get("resources") if isinstance(sub.get("resources"), list) else []
                ret.extend(self._flatten_files([x for x in res if isinstance(x, dict)]))
            else:
                ret.append(item)
        return ret

    @staticmethod
    def _resolve_source_path(source_root: Path, task_name: str) -> Optional[Path]:
        if not task_name:
            return None
        safe_name = Path(task_name).name
        if not safe_name:
            return None
        short = source_root / safe_name
        try:
            root_resolved = source_root.resolve(strict=False)
            short_resolved = short.resolve(strict=False)
            if root_resolved not in short_resolved.parents and short_resolved != root_resolved:
                return None
        except Exception:
            return None
        if short.exists():
            return short
        return None

    @staticmethod
    def _resolve_source_path_fallback(source_root: Path, task_name: str) -> Optional[Path]:
        task_raw = str(task_name or "").strip()
        if not task_raw or not source_root.exists():
            return None
        task_base = Path(task_raw).name
        task_stem = Path(task_base).stem
        task_norm = XunleiHijackDownloader._normalize_name(task_stem or task_base)
        if not task_norm:
            return None
        candidates: List[Tuple[int, float, Path]] = []
        try:
            for item in source_root.iterdir():
                name = item.name
                stem = item.stem
                score = 0
                if name.lower() == task_base.lower():
                    score = 100
                elif stem.lower() == task_stem.lower() and task_stem:
                    score = 95
                else:
                    item_norm = XunleiHijackDownloader._normalize_name(stem or name)
                    if item_norm == task_norm:
                        score = 90
                    elif len(task_norm) >= 8 and (item_norm.startswith(task_norm) or task_norm.startswith(item_norm)):
                        score = 80
                if score > 0:
                    mtime = 0.0
                    try:
                        mtime = item.stat().st_mtime
                    except Exception:
                        pass
                    candidates.append((score, mtime, item))
        except Exception:
            return None
        if not candidates:
            return None
        candidates.sort(key=lambda x: (x[0], x[1]), reverse=True)
        return candidates[0][2]

    @staticmethod
    def _normalize_name(text: str) -> str:
        name = str(text or "").strip().lower()
        name = re.sub(r"[\\/:*?\"<>|]+", " ", name)
        name = re.sub(r"[\s._\-\[\]\(\)\{\}]+", " ", name).strip()
        return name

    @staticmethod
    def _extract_api_error(obj: Any) -> str:
        if not isinstance(obj, dict):
            return ""
        code = obj.get("code")
        if code is not None:
            try:
                if int(code) not in (0, 200):
                    return str(obj.get("error") or obj.get("err") or obj.get("message") or obj.get("msg") or f"code={code}")
            except Exception:
                pass
        for key in ("error", "err"):
            value = obj.get(key)
            if value:
                return str(value)
        return ""

    def _merge_error_texts(self, obj: Any = None) -> str:
        texts: List[str] = [str(self._last_request_error or "")]
        if isinstance(obj, dict):
            for key in ("error", "err", "message", "msg", "detail", "error_description", "error_code"):
                value = obj.get(key)
                if value is not None:
                    texts.append(str(value))
        return " ".join(texts).lower()

    @staticmethod
    def _should_refresh_pan_auth(resp: Optional[requests.Response], obj: Any) -> bool:
        if resp is not None and resp.status_code in (401, 403):
            return True
        if not isinstance(obj, dict):
            return False
        code = obj.get("code")
        if code is not None:
            try:
                if int(code) in (401, 403):
                    return True
            except Exception:
                pass
        text = " ".join([
            str(obj.get("error") or ""),
            str(obj.get("err") or ""),
            str(obj.get("message") or ""),
            str(obj.get("msg") or ""),
            str(obj.get("detail") or ""),
        ]).lower()
        return any(flag in text for flag in ("unauthorized", "forbidden", "token", "login", "expired", "auth failed"))

    def _should_handle_torrent_control(self, ids: Set[str], downloader: Optional[str]) -> Optional[bool]:
        if downloader and not self._is_xunlei_downloader(downloader):
            return None
        if not ids:
            return False
        if downloader:
            return True
        tasks = self._list_download_tasks()
        if not tasks:
            return None
        task_ids = {self._task_key(x) for x in tasks if isinstance(x, dict) and self._task_key(x)}
        if not task_ids:
            return None
        return len(ids.intersection(task_ids)) > 0

    @staticmethod
    def _dedupe_target(path: Path) -> Path:
        if not path.exists():
            return path
        stem, suffix, parent, idx = path.stem, path.suffix, path.parent, 1
        while True:
            candidate = parent / f"{stem}.{idx}{suffix}"
            if not candidate.exists():
                return candidate
            idx += 1

    @staticmethod
    def _normalize_magnet(content: Union[Path, str, bytes]) -> str:
        if isinstance(content, str):
            text = content.strip()
            if text.lower().startswith("magnet:?"):
                return text
            path = Path(text)
            if path.exists() and path.is_file():
                try:
                    return XunleiHijackDownloader._torrent_to_magnet(path.read_bytes())
                except Exception:
                    return ""
            return ""
        if isinstance(content, bytes):
            try:
                text = content.decode("utf-8", errors="ignore").strip()
                if text.lower().startswith("magnet:?"):
                    return text
                return XunleiHijackDownloader._torrent_to_magnet(content)
            except Exception:
                return ""
        if isinstance(content, Path):
            try:
                if content.exists() and content.is_file():
                    return XunleiHijackDownloader._torrent_to_magnet(content.read_bytes())
            except Exception:
                return ""
            return ""
        return ""

    @staticmethod
    def _torrent_to_magnet(data: bytes) -> str:
        if not data:
            return ""
        parsed, info_start, info_end = XunleiHijackDownloader._bdecode_with_info_range(data)
        if info_start < 0 or info_end <= info_start:
            return ""
        info_hash = hashlib.sha1(data[info_start:info_end]).hexdigest()
        dn = ""
        tr_list: List[str] = []
        if isinstance(parsed, dict):
            info = parsed.get(b"info")
            if isinstance(info, dict):
                name_bytes = info.get(b"name.utf-8") or info.get(b"name")
                if isinstance(name_bytes, (bytes, bytearray)):
                    dn = bytes(name_bytes).decode("utf-8", errors="ignore").strip()
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

        magnet = f"magnet:?xt=urn:btih:{info_hash}"
        if dn:
            magnet += f"&dn={quote(dn)}"
        seen = set()
        for tr in tr_list:
            url = str(tr or "").strip()
            if not url or url in seen:
                continue
            seen.add(url)
            magnet += f"&tr={quote(url, safe=':/?&=')}"
            if len(seen) >= 20:
                break
        return magnet

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
    def _normalize_base_url(url: str) -> str:
        return str(url or "").strip().rstrip("/")

    @staticmethod
    def _to_positive_int(value: Any, default: int) -> int:
        try:
            n = int(value)
            return n if n > 0 else default
        except Exception:
            return default

    @staticmethod
    def _to_non_negative_int(value: Any, default: int) -> int:
        try:
            n = int(value)
            return n if n >= 0 else default
        except Exception:
            return default
