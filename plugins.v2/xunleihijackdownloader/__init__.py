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
    plugin_version = "1.0.57"
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
    _completed_seen_at: Dict[str, float] = {}
    _completed_seen_order: List[str] = []
    _completed_seen_name: Dict[str, str] = {}
    _max_moved_keys = 2000
    _max_completed_seen_keys = 4000
    _last_request_error = ""

    def init_plugin(self, config: dict = None):
        self.stop_service()
        self._moved_task_order = self._load_moved_task_keys()
        self._moved_task_keys = set(self._moved_task_order)
        self._task_name_cache = {}
        self._completed_seen_at = {}
        self._completed_seen_order = []
        self._completed_seen_name = {}
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
                "allow_anonymous": True,
                "summary": "开始迅雷任务",
                "description": "在插件数据页手动开始指定任务",
            },
            {
                "path": "/task/pause",
                "endpoint": self.api_pause_task,
                "methods": ["GET"],
                "allow_anonymous": True,
                "summary": "暂停迅雷任务",
                "description": "在插件数据页手动暂停指定任务",
            },
            {
                "path": "/task/delete",
                "endpoint": self.api_delete_task,
                "methods": ["GET"],
                "allow_anonymous": True,
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
                "props": {"align": "center"},
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
                                    "icon": False,
                                    "text": "展示迅雷任务实时状态：图片、文件图标、文件名、大小、剩余时间、速度、进度及开始/暂停/删除。已迁移任务自动隐藏。",
                                },
                            }
                        ],
                    },
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

        for task in visible_tasks:
            page.append({
                "component": "VRow",
                "content": [
                    {
                        "component": "VCol",
                        "props": {"cols": 12},
                        "content": [self._build_task_row(task=task)],
                    }
                ],
            })
        return page

    def api_start_task(self, task_id: str = "", hash: str = "", space: str = "") -> schemas.Response:
        return self._api_task_action(task_id=task_id or hash, action="start", space=space)

    def api_pause_task(self, task_id: str = "", hash: str = "", space: str = "") -> schemas.Response:
        return self._api_task_action(task_id=task_id or hash, action="pause", space=space)

    def api_delete_task(self, task_id: str = "", hash: str = "", delete_file: bool = True, space: str = "") -> schemas.Response:
        return self._api_task_action(task_id=task_id or hash, action="delete", delete_file=delete_file, space=space)

    def _api_task_action(self, task_id: str, action: str, delete_file: bool = True, space: str = "") -> schemas.Response:
        task_key = str(task_id or "").strip()
        if not task_key:
            return schemas.Response(success=False, message="任务ID不能为空。")
        if action != "delete" and task_key in self._moved_task_keys:
            return schemas.Response(success=False, message="任务已迁移，无法继续操作。")
        action_candidates: List[str]
        if action == "start":
            action_candidates = ["start", "resume", "continue", "unpause"]
        elif action == "pause":
            action_candidates = ["pause", "stop", "suspend"]
        elif action == "delete":
            action_candidates = ["delete", "remove"]
        else:
            action_candidates = [action]

        ok = False
        for act in action_candidates:
            ok = self._operate_tasks(
                ids={task_key},
                action=act,
                delete_file=bool(delete_file),
                preferred_space=space,
            )
            if ok:
                break
        if ok and action == "delete":
            self._remember_moved_key(task_key)
        action_name = {"start": "开始", "pause": "暂停", "delete": "删除"}.get(action, action)
        if ok:
            return schemas.Response(success=True, message=f"{action_name}任务成功。")
        detail = str(self._last_request_error or "").strip()
        if detail:
            if len(detail) > 180:
                detail = detail[:180] + "..."
            return schemas.Response(success=False, message=f"{action_name}任务失败：{detail}")
        return schemas.Response(success=False, message=f"{action_name}任务失败，请检查迅雷连接与认证。")

    def _build_task_row(self, task: Dict[str, Any]) -> Dict[str, Any]:
        plugin_id = self.__class__.__name__
        task_id = self._task_key(task)
        task_name = self._task_name(task) or task_id or "xunlei-task"
        task_done = self._is_task_completed(task)
        task_paused = self._is_task_paused(task)
        task_failed = self._is_task_failed(task)

        progress = self._task_progress(task)
        size_text = self._format_bytes(self._task_size(task))
        left_time = self._task_left_time(task, progress) or "--"
        speed_text = self._task_speed_text(task, key="download_speed") or "0B/s"
        image_url = self._task_image_url(task)

        can_start = bool(task_id)
        can_pause = bool(task_id)
        can_delete = bool(task_id)
        quoted_id = quote(task_id or "", safe="")
        space_qs = "&space="
        start_api = f"/api/v1/plugin/{plugin_id}/task/start?task_id={quoted_id}{space_qs}"
        pause_api = f"/api/v1/plugin/{plugin_id}/task/pause?task_id={quoted_id}{space_qs}"
        delete_api = f"/api/v1/plugin/{plugin_id}/task/delete?task_id={quoted_id}&delete_file=true{space_qs}"
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
            "component": "VCard",
            "props": {"variant": "text", "class": "mb-1"},
            "content": [
                {
                    "component": "VCardText",
                    "props": {"class": "py-2"},
                    "content": [
                        {
                            "component": "VRow",
                            "props": {"align": "center", "noGutters": True},
                            "content": [
                                {"component": "VCol", "props": {"cols": 1, "md": 1}, "content": [image_node]},
                                {
                                    "component": "VCol",
                                    "props": {"cols": 6, "md": 6},
                                    "content": [{"component": "VListItem", "props": {"title": task_name, "density": "compact"}}],
                                },
                                {
                                    "component": "VCol",
                                    "props": {"cols": 3, "md": 3},
                                    "content": [
                                        {
                                            "component": "VListItem",
                                            "props": {
                                                "density": "compact",
                                                "title": f"{size_text}    {left_time}    {speed_text}",
                                            },
                                        },
                                        {
                                            "component": "VProgressLinear",
                                            "props": {
                                                "modelValue": progress,
                                                "height": 5,
                                                "rounded": True,
                                                "color": progress_color,
                                            },
                                        },
                                    ],
                                },
                                {
                                    "component": "VCol",
                                    "props": {"cols": 2, "md": 2, "class": "d-flex justify-end ga-1"},
                                    "content": [
                                        self._build_task_action_button(
                                            text="开始",
                                            color="success",
                                            icon="mdi-play",
                                            disabled=not can_start,
                                            api_path=start_api,
                                            success_message="开始任务成功，请点击刷新查看状态。",
                                            failure_message="开始任务失败。",
                                        ),
                                        self._build_task_action_button(
                                            text="暂停",
                                            color="warning",
                                            icon="mdi-pause",
                                            disabled=not can_pause,
                                            api_path=pause_api,
                                            success_message="暂停任务成功，请点击刷新查看状态。",
                                            failure_message="暂停任务失败。",
                                        ),
                                        self._build_task_action_button(
                                            text="删除",
                                            color="error",
                                            icon="mdi-close",
                                            disabled=not can_delete,
                                            api_path=delete_api,
                                            success_message="删除任务成功，请点击刷新查看状态。",
                                            failure_message="删除任务失败。",
                                        ),
                                    ],
                                },
                            ],
                        }
                    ],
                },
                {"component": "VDivider"},
            ],
        }

    @staticmethod
    def _build_task_action_button(text: str, color: str, icon: str, disabled: bool, api_path: str,
                                  success_message: str, failure_message: str) -> Dict[str, Any]:
        button = {
            "component": "VBtn",
            "props": {
                "size": "x-small",
                "density": "compact",
                "variant": "text",
                "color": color,
                "text": text,
                "prependIcon": icon,
                "title": text,
                "disabled": bool(disabled),
                "class": "ml-1 px-1",
            },
        }
        if not disabled:
            button["props"]["onclick"] = XunleiHijackDownloader._build_action_onclick(api_path=api_path)
        return button

    @staticmethod
    def _build_action_onclick(api_path: str) -> str:
        path = str(api_path or "").replace("\\", "\\\\").replace("'", "\\'")
        return (
            "(async()=>{"
            f"try{{const r=await fetch('{path}',{{method:'GET',credentials:'same-origin'}});"
            "const j=await r.json().catch(()=>null);"
            "if(r.ok&&(!j||j.success!==false)){window.location.reload();return;}"
            "alert((j&&j.message)?j.message:'操作失败，请查看日志');"
            "}catch(e){alert('请求失败，请检查网络或权限');}"
            "})();"
        )

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

    def _remember_completed_seen(self, move_key: str, now_ts: float, task_name: str = "") -> float:
        token = str(move_key or "").strip()
        if not token:
            return float(now_ts)
        ts = float(now_ts)
        old = self._completed_seen_at.get(token)
        if old is not None:
            try:
                old_ts = float(old)
                if old_ts > 0:
                    ts = min(ts, old_ts)
            except Exception:
                pass
            self._completed_seen_order = [x for x in self._completed_seen_order if x != token]
        self._completed_seen_at[token] = ts
        name = Path(str(task_name or "").strip()).name
        if name:
            self._completed_seen_name[token] = name
        elif token not in self._completed_seen_name:
            self._completed_seen_name[token] = ""
        self._completed_seen_order.append(token)
        if len(self._completed_seen_order) > self._max_completed_seen_keys:
            overflow = self._completed_seen_order[:-self._max_completed_seen_keys]
            self._completed_seen_order = self._completed_seen_order[-self._max_completed_seen_keys:]
            for key in overflow:
                self._completed_seen_at.pop(key, None)
                self._completed_seen_name.pop(key, None)
        return ts

    def _drop_completed_seen(self, move_key: str) -> None:
        token = str(move_key or "").strip()
        if not token:
            return
        self._completed_seen_at.pop(token, None)
        self._completed_seen_name.pop(token, None)
        if token in self._completed_seen_order:
            self._completed_seen_order = [x for x in self._completed_seen_order if x != token]

    @staticmethod
    def _parse_unix_timestamp(value: Any) -> Optional[float]:
        if value is None:
            return None
        raw: Optional[float] = None
        if isinstance(value, (int, float)):
            raw = float(value)
        else:
            text = str(value or "").strip()
            if not text:
                return None
            if re.fullmatch(r"-?\d+(?:\.\d+)?", text):
                try:
                    raw = float(text)
                except Exception:
                    raw = None
            else:
                iso = text.replace("Z", "+00:00")
                try:
                    dt = datetime.fromisoformat(iso)
                    raw = dt.timestamp()
                except Exception:
                    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y/%m/%d %H:%M"):
                        try:
                            raw = datetime.strptime(text, fmt).timestamp()
                            break
                        except Exception:
                            continue
        if raw is None or raw <= 0:
            return None
        # 兼容毫秒时间戳
        if raw > 1e12:
            raw = raw / 1000.0
        if raw > 1e11:
            raw = raw / 1000.0
        return raw if raw > 0 else None

    def _task_completed_timestamp(self, task: Dict[str, Any]) -> Optional[float]:
        keys = [
            "completed_time", "completed_at", "complete_time", "complete_at",
            "finished_time", "finished_at", "finish_time", "finish_at",
            "end_time", "end_at", "ended_time", "done_time", "done_at",
            "mtime", "update_time", "updated_at",
        ]
        for value in self._task_lookup_values(task=task, keys=keys):
            ts = self._parse_unix_timestamp(value)
            if ts:
                return ts
        return None

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

    def _fetch_device_id(self, force_refresh: bool = False, exclude_device: str = "") -> Optional[str]:
        if self._device_id and not force_refresh:
            return self._device_id
        old_device = str(self._device_id or "").strip()
        if force_refresh and self._device_id:
            self._device_id = ""
            self._save_config()
        if not self._base_url or not self._authorization:
            return None
        headers = self._get_headers()
        candidates: List[str] = []

        try:
            for task_type in ("user%23runner", "user%23download-url"):
                url = (
                    f"{self._base_url}/webman/3rdparty/pan-xunlei-com/index.cgi/drive/v1/tasks"
                    f"?type={task_type}&device_space="
                )
                resp, obj = self._request_json(
                    method="GET",
                    url=url,
                    headers={**headers, "device-space": ""},
                    timeout=20,
                    retry_auth=True,
                )
                if not resp or not resp.ok or not isinstance(obj, dict):
                    continue
                tasks = obj.get("tasks")
                if not isinstance(tasks, list):
                    continue
                for task in tasks:
                    if not isinstance(task, dict):
                        continue
                    params = task.get("params") if isinstance(task.get("params"), dict) else {}
                    token = str(params.get("target") or task.get("target") or "").strip()
                    self._append_device_candidate(candidates, token)
        except Exception as err:
            logger.warn(f"XunleiHijack[v{self.plugin_version}] fetch device_id failed: {err}")

        for endpoint in (
            "/webman/3rdparty/pan-xunlei-com/index.cgi/drive/v1/devices",
            "/webman/3rdparty/pan-xunlei-com/index.cgi/drive/v1/device",
        ):
            try:
                url = f"{self._base_url}{endpoint}"
                resp, obj = self._request_json(
                    method="GET",
                    url=url,
                    headers=headers,
                    timeout=20,
                    retry_auth=True,
                )
                if not resp or not resp.ok or not isinstance(obj, dict):
                    continue
                for key in ("devices", "list", "data"):
                    payload = obj.get(key)
                    if isinstance(payload, list):
                        for item in payload:
                            if not isinstance(item, dict):
                                continue
                            device = str(item.get("id") or item.get("device_id") or item.get("target") or item.get("space") or "").strip()
                            self._append_device_candidate(candidates, device)
                    elif isinstance(payload, dict):
                        device = str(payload.get("id") or payload.get("device_id") or payload.get("target") or payload.get("space") or "").strip()
                        self._append_device_candidate(candidates, device)
            except Exception:
                continue

        picked = self._pick_active_device_id(
            candidates=candidates,
            exclude_device=exclude_device,
            old_device=old_device,
        )
        if picked:
            self._device_id = picked
            self._save_config()
            return picked
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

            refresh_device = self._fetch_device_id(force_refresh=True, exclude_device=first_device)
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
            logger.warn(
                f"XunleiHijack move skipped: source/target not configured, "
                f"source={self._source_download_dir or 'EMPTY'}, target={self._target_watch_dir or 'EMPTY'}"
            )
            return
        if not self._move_lock.acquire(blocking=False):
            logger.info("XunleiHijack move skipped: previous job still running.")
            return
        try:
            source_root = Path(self._source_download_dir)
            target_root = Path(self._target_watch_dir)
            if not source_root.exists() or not source_root.is_dir():
                logger.warn(
                    f"XunleiHijack move skipped: source path invalid, "
                    f"source={source_root}, exists={source_root.exists()}, is_dir={source_root.is_dir()}"
                )
                return
            target_root.mkdir(parents=True, exist_ok=True)
            tasks = self._list_download_tasks()
            now_ts = time.time()
            stats = {
                "moved": 0,
                "skip_not_completed": 0,
                "skip_no_move_key": 0,
                "skip_already_moved": 0,
                "skip_source_not_found": 0,
                "skip_safe_wait": 0,
                "skip_cached_missing_name": 0,
                "move_failed": 0,
            }
            samples: List[str] = []
            processed_keys: Set[str] = set()
            cached_total = len(self._completed_seen_at)

            def add_sample(text: str) -> None:
                if len(samples) < 6:
                    samples.append(text)

            def try_move_by_name(move_key: str, task_name: str, task_id: str, task_tag: str, from_cache: bool) -> None:
                src = self._resolve_source_path(source_root, task_name)
                if not src or not src.exists():
                    src = self._resolve_source_path_fallback(source_root, task_name)
                if not src or not src.exists():
                    stats["skip_source_not_found"] += 1
                    flag = "cache_source_not_found" if from_cache else "source_not_found"
                    add_sample(
                        f"{task_tag} skip:{flag} source_root={source_root} task_name={task_name}"
                    )
                    return
                try:
                    dst = self._dedupe_target(target_root / src.name)
                    shutil.move(str(src), str(dst))
                    self._remember_moved_key(move_key)
                    self._drop_completed_seen(move_key)
                    if task_id and task_id != "-":
                        # 兼容历史 moved key（曾使用纯 task_id）
                        self._remember_moved_key(task_id)
                    stats["moved"] += 1
                    if from_cache:
                        logger.info(f"XunleiHijack moved(cache): {src} -> {dst}")
                    else:
                        logger.info(f"XunleiHijack moved: {src} -> {dst}")
                except Exception as move_err:
                    stats["move_failed"] += 1
                    logger.warn(
                        f"XunleiHijack move item failed: key={move_key}, "
                        f"name={task_name}, err={move_err}"
                    )

            if not tasks:
                logger.info(
                    f"XunleiHijack move scan: no tasks, source={source_root}, target={target_root}, "
                    f"cached_completed={cached_total}"
                )

            for task in tasks:
                task_id = self._task_key(task) or "-"
                task_name = Path(str(self._task_name(task) or "")).name or "-"
                task_status = ",".join(self._task_status_values(task)) or "-"
                task_progress = self._task_progress(task)
                task_tag = f"id={task_id},name={task_name}"
                move_key = self._task_move_key(task)
                if not move_key:
                    stats["skip_no_move_key"] += 1
                    add_sample(f"{task_tag} skip:no_move_key status={task_status}")
                    continue
                if not self._is_task_completed(task):
                    stats["skip_not_completed"] += 1
                    self._drop_completed_seen(move_key)
                    add_sample(f"{task_tag} skip:not_completed status={task_status} progress={task_progress:.2f}")
                    continue
                if move_key in self._moved_task_keys:
                    stats["skip_already_moved"] += 1
                    self._drop_completed_seen(move_key)
                    continue
                processed_keys.add(move_key)
                done_ts = self._task_completed_timestamp(task)
                if done_ts is None:
                    done_ts = now_ts
                done_ts = self._remember_completed_seen(move_key=move_key, now_ts=done_ts, task_name=task_name)
                if self._move_safe_seconds > 0:
                    elapsed = now_ts - done_ts
                    if elapsed < self._move_safe_seconds:
                        stats["skip_safe_wait"] += 1
                        add_sample(
                            f"{task_tag} skip:safe_wait completed_elapsed={elapsed:.1f}s < {self._move_safe_seconds}s"
                        )
                        continue
                try_move_by_name(
                    move_key=move_key,
                    task_name=task_name,
                    task_id=task_id,
                    task_tag=task_tag,
                    from_cache=False,
                )

            for move_key in list(self._completed_seen_order):
                if move_key in processed_keys:
                    continue
                if move_key in self._moved_task_keys:
                    stats["skip_already_moved"] += 1
                    self._drop_completed_seen(move_key)
                    continue
                done_ts = float(self._completed_seen_at.get(move_key) or now_ts)
                task_name = Path(str(self._completed_seen_name.get(move_key) or "").strip()).name
                task_tag = f"id=-,name={task_name or '-'}"
                if not task_name:
                    stats["skip_cached_missing_name"] += 1
                    add_sample(f"{task_tag} skip:cached_missing_name key={move_key}")
                    continue
                if self._move_safe_seconds > 0:
                    elapsed = now_ts - done_ts
                    if elapsed < self._move_safe_seconds:
                        stats["skip_safe_wait"] += 1
                        add_sample(
                            f"{task_tag} skip:cache_safe_wait completed_elapsed={elapsed:.1f}s < {self._move_safe_seconds}s"
                        )
                        continue
                try_move_by_name(
                    move_key=move_key,
                    task_name=task_name,
                    task_id="-",
                    task_tag=task_tag,
                    from_cache=True,
                )
            logger.info(
                f"XunleiHijack move scan summary: source={source_root}, target={target_root}, "
                f"total={len(tasks)}, cached_completed={cached_total}, moved={stats['moved']}, "
                f"skip_not_completed={stats['skip_not_completed']}, "
                f"skip_no_move_key={stats['skip_no_move_key']}, skip_already_moved={stats['skip_already_moved']}, "
                f"skip_source_not_found={stats['skip_source_not_found']}, skip_safe_wait={stats['skip_safe_wait']}, "
                f"skip_cached_missing_name={stats['skip_cached_missing_name']}, move_failed={stats['move_failed']}"
            )
            if samples:
                logger.info("XunleiHijack move scan samples: " + " | ".join(samples))
        except Exception as err:
            logger.error(f"XunleiHijack move job failed: {err}")
        finally:
            self._move_lock.release()

    def _list_download_tasks(self) -> List[Dict[str, Any]]:
        try:
            headers = self._get_headers()
            device_id = str(self._fetch_device_id() or self._device_id or "").strip()

            def _extract_tasks(data: Any) -> List[Dict[str, Any]]:
                if not isinstance(data, dict):
                    return []
                payload = data.get("tasks")
                if isinstance(payload, list):
                    return [x for x in payload if isinstance(x, dict)]
                list_obj = data.get("list")
                if isinstance(list_obj, list):
                    return [x for x in list_obj if isinstance(x, dict)]
                return []

            spaces: List[str] = [""]
            if device_id and device_id not in spaces:
                spaces.append(device_id)

            probes: List[Tuple[str, Dict[str, str]]] = [
                ("default", {}),
                ("phase_complete", {"phase": "PHASE_TYPE_COMPLETE"}),
                ("phase_finished", {"phase": "PHASE_TYPE_FINISHED"}),
                ("phase_complete_lc", {"phase": "phase_type_complete"}),
                ("status_completed", {"status": "completed"}),
                ("state_completed", {"state": "completed"}),
            ]

            merged_tasks: List[Dict[str, Any]] = []
            seen_keys: Set[str] = set()
            probe_stats: List[str] = []

            def _task_merge_key(task: Dict[str, Any]) -> str:
                task_id = self._task_key(task)
                if task_id:
                    return f"id:{task_id}"
                name = Path(str(self._task_name(task) or task.get("name") or task.get("title") or "")).name
                phase = str(task.get("phase") or "").strip().lower()
                progress = str(task.get("progress") or "").strip()
                return f"name:{name}|phase:{phase}|progress:{progress}"

            last_err = ""
            for space in spaces:
                for probe_name, extra_params in probes:
                    query = [
                        "type=user%23download-url",
                        f"device_space={quote(space) if space else ''}",
                    ]
                    for k, v in extra_params.items():
                        token = str(v or "").strip()
                        if token:
                            query.append(f"{k}={quote(token)}")
                    url = (
                        f"{self._base_url}/webman/3rdparty/pan-xunlei-com/index.cgi/drive/v1/tasks"
                        f"?{'&'.join(query)}"
                    )
                    resp, obj = self._request_json(
                        method="GET",
                        url=url,
                        headers={**headers, "device-space": space},
                        timeout=20,
                        retry_auth=True
                    )
                    if not resp or not resp.ok:
                        last_err = f"http={resp.status_code if resp else 'request-failed'} {self._last_request_error}"
                        logger.warn(
                            f"XunleiHijack[v{self.plugin_version}] list tasks failed: "
                            f"space={space or 'EMPTY'}, probe={probe_name}, {last_err}"
                        )
                        continue
                    tasks = _extract_tasks(obj)
                    logger.info(
                        f"XunleiHijack[v{self.plugin_version}] list tasks: "
                        f"space={space or 'EMPTY'}, probe={probe_name}, count={len(tasks)}"
                    )
                    if not tasks:
                        continue
                    probe_stats.append(f"{space or 'EMPTY'}:{probe_name}={len(tasks)}")
                    for task in tasks:
                        key = _task_merge_key(task)
                        if not key or key in seen_keys:
                            continue
                        seen_keys.add(key)
                        merged_tasks.append(task)
                    if not device_id:
                        for task in tasks:
                            if isinstance(task, dict):
                                params = task.get("params") if isinstance(task.get("params"), dict) else {}
                                target = str(params.get("target") or task.get("target") or "").strip()
                                if target:
                                    self._device_id = target
                                    self._save_config()
                                    break
            if merged_tasks:
                logger.info(
                    f"XunleiHijack[v{self.plugin_version}] list tasks merged: "
                    f"total={len(merged_tasks)}, hit_probes={'; '.join(probe_stats[:8])}"
                )
                return merged_tasks
            if last_err:
                logger.info(
                    f"XunleiHijack[v{self.plugin_version}] list tasks empty after all spaces: "
                    f"device_id={device_id or 'EMPTY'}, last_error={last_err}"
                )
            else:
                logger.info(
                    f"XunleiHijack[v{self.plugin_version}] list tasks empty: device_id={device_id or 'EMPTY'}"
                )
        except Exception as err:
            logger.warn(f"XunleiHijack[v{self.plugin_version}] list tasks failed: {err}")
        return []

    def _operate_tasks(self, ids: Set[str], action: str, delete_file: bool = True, preferred_space: str = "") -> bool:
        id_list = [str(item or "").strip() for item in ids if str(item or "").strip()]
        if not id_list:
            self._last_request_error = "task_id 为空"
            return False
        if not self._base_url:
            self._last_request_error = "迅雷地址未配置"
            return False
        preferred_space = str(preferred_space or "").strip()
        if not preferred_space and not self._fetch_device_id():
            self._last_request_error = "device_id 未配置且自动获取失败"
            return False
        headers = self._get_headers()
        if self._auto_refresh_pan_auth and not headers.get("pan-auth"):
            self._last_request_error = "pan_auth 自动获取失败"
            return False

        first_id = id_list[0]
        payload_templates: List[Dict[str, Any]] = [
            {"action": action, "ids": id_list},
            {"action": action, "task_ids": id_list},
            {"action": action, "id": first_id},
            {"action": action, "task_id": first_id},
            {"type": action, "ids": id_list},
            {"type": action, "task_ids": id_list},
            {"type": action, "id": first_id},
            {"type": action, "task_id": first_id},
        ]
        methods = ["PATCH", "POST", "PUT", "DELETE"]
        urls = [
            f"{self._base_url}/webman/3rdparty/pan-xunlei-com/index.cgi/drive/v1/task/action",
            f"{self._base_url}/webman/3rdparty/pan-xunlei-com/index.cgi/drive/v1/tasks/action",
            f"{self._base_url}/webman/3rdparty/pan-xunlei-com/index.cgi/drive/v1/task",
            f"{self._base_url}/webman/3rdparty/pan-xunlei-com/index.cgi/drive/v1/tasks",
        ]
        
        def _attempt_with_device(device_space: str) -> Tuple[bool, List[str]]:
            payloads: List[Dict[str, Any]] = []
            for base in payload_templates:
                raw_payload = dict(base)
                space_payload = {**base, "device_space": device_space}
                target_payload = {**base, "target": device_space, "space": device_space}
                full_payload = {**base, "device_space": device_space, "target": device_space, "space": device_space}
                if action == "delete":
                    raw_payload["delete_file"] = bool(delete_file)
                    raw_payload["delete_files"] = bool(delete_file)
                    space_payload["delete_file"] = bool(delete_file)
                    space_payload["delete_files"] = bool(delete_file)
                    target_payload["delete_file"] = bool(delete_file)
                    target_payload["delete_files"] = bool(delete_file)
                    full_payload["delete_file"] = bool(delete_file)
                    full_payload["delete_files"] = bool(delete_file)
                payloads.append(raw_payload)
                payloads.append(space_payload)
                payloads.append(target_payload)
                payloads.append(full_payload)

            header_variants = [
                {**headers, "device-space": device_space},
                {**headers, "device-space": ""},
                headers,
            ]
            local_hints: List[str] = []
            phase_candidates = self._phase_candidates_for_action(action=action)
            if phase_candidates:
                update_urls = [
                    f"{self._base_url}/webman/3rdparty/pan-xunlei-com/index.cgi/drive/v1/task",
                    f"{self._base_url}/webman/3rdparty/pan-xunlei-com/index.cgi/drive/v1/tasks",
                ]
                update_methods = ["PATCH", "PUT", "POST"]
                phase_payloads: List[Dict[str, Any]] = []
                for phase in phase_candidates:
                    per_phase = [
                        {"id": first_id, "phase": phase},
                        {"task_id": first_id, "phase": phase},
                        {"ids": id_list, "phase": phase},
                        {"task_ids": id_list, "phase": phase},
                        {"id": first_id, "set_params": {"phase": phase}},
                        {"task_id": first_id, "set_params": {"phase": phase}},
                    ]
                    for base in per_phase:
                        phase_payloads.append(dict(base))
                        phase_payloads.append({**base, "space": device_space})
                        phase_payloads.append({**base, "space": device_space, "type": "user#download-url"})
                        phase_payloads.append({**base, "space": device_space, "type": "user#runner"})
                        phase_payloads.append({**base, "space": device_space, "target": device_space})
                        phase_payloads.append({**base, "space": device_space, "device_space": device_space})
                        phase_payloads.append({**base, "target": device_space, "device_space": device_space})
                for method in update_methods:
                    for url in update_urls:
                        for request_headers in header_variants:
                            for payload in phase_payloads:
                                try:
                                    resp, obj = self._request_json(
                                        method=method,
                                        url=url,
                                        headers=request_headers,
                                        payload=payload,
                                        timeout=20,
                                        retry_auth=True,
                                    )
                                    if not resp or not resp.ok:
                                        hint = str(self._last_request_error or "").strip()
                                        if hint:
                                            local_hints.append(hint)
                                        continue
                                    if self._is_operation_success(obj=obj, ids=set(id_list), resp=resp):
                                        return True, local_hints
                                    hint = str(self._extract_api_error(obj) or "").strip()
                                    if not hint:
                                        merged = str(self._merge_error_texts(obj) or "").strip()
                                        if merged and merged != str(self._last_request_error or "").strip().lower():
                                            hint = merged
                                    if hint:
                                        local_hints.append(hint)
                                except Exception as err:
                                    local_hints.append(str(err))
                                    continue
            for method in methods:
                for url in urls:
                    for request_headers in header_variants:
                        for payload in payloads:
                            try:
                                resp, obj = self._request_json(
                                    method=method,
                                    url=url,
                                    headers=request_headers,
                                    payload=payload,
                                    timeout=20,
                                    retry_auth=True,
                                )
                                if not resp or not resp.ok:
                                    hint = str(self._last_request_error or "").strip()
                                    if hint:
                                        local_hints.append(hint)
                                    continue
                                if self._is_operation_success(obj=obj, ids=set(id_list), resp=resp):
                                    return True, local_hints
                                hint = str(self._extract_api_error(obj) or "").strip()
                                if not hint:
                                    merged = str(self._merge_error_texts(obj) or "").strip()
                                    if merged and merged != str(self._last_request_error or "").strip().lower():
                                        hint = merged
                                if hint:
                                    local_hints.append(hint)
                            except Exception as err:
                                local_hints.append(str(err))
                                continue
            return False, local_hints

        first_device = preferred_space if preferred_space else ""
        ok, failure_hints = _attempt_with_device(first_device)
        if ok:
            return True

        if self._device_id and str(self._device_id).strip() != first_device:
            fallback_device = str(self._device_id).strip()
            ok, extra_hints = _attempt_with_device(fallback_device)
            failure_hints.extend(extra_hints)
            if ok:
                return True
            first_device = fallback_device

        merged_failures = " ".join([str(item or "") for item in failure_hints]).lower()
        if self._refresh_device_id_on_inactive_space(error_text=merged_failures):
            refreshed_device = str(self._device_id or "").strip()
            if refreshed_device and refreshed_device != first_device:
                ok, retry_hints = _attempt_with_device(refreshed_device)
                failure_hints.extend(retry_hints)
                if ok:
                    return True
            else:
                failure_hints.append("device_space_not_active: 刷新后 device_id 无变化")

        if failure_hints:
            deduped: List[str] = []
            for hint in failure_hints:
                text = str(hint or "").strip()
                if not text:
                    continue
                if text not in deduped:
                    deduped.append(text)
                if len(deduped) >= 3:
                    break
            if deduped:
                self._last_request_error = " | ".join([x[:120] for x in deduped])
        if not self._last_request_error:
            self._last_request_error = f"action={action} 未获得成功响应"
        return False

    @staticmethod
    def _phase_candidates_for_action(action: str) -> List[str]:
        token = str(action or "").strip().lower()
        if token in ("start", "resume", "continue", "unpause"):
            return ["phase_type_running", "PHASE_TYPE_RUNNING", "running", "RUNNING"]
        if token in ("pause", "stop", "suspend"):
            return ["phase_type_paused", "PHASE_TYPE_PAUSED", "paused", "PAUSED"]
        return []

    @staticmethod
    def _is_operation_success(obj: Any, ids: Set[str], resp: Optional[requests.Response] = None) -> bool:
        if resp is not None and resp.ok:
            try:
                if not str(resp.text or "").strip():
                    return True
            except Exception:
                if resp.status_code in (200, 201, 202, 204):
                    return True
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
            if any(k in text for k in (
                "complete",
                "completed",
                "finished",
                "success",
                "done",
                "phase_type_complete",
                "phase_type_finished",
                "phase_type_seeding",
                "seeding",
            )):
                return True
        for key in ("completed", "is_completed", "finished", "is_finished", "done", "is_done", "success"):
            value = task.get(key)
            if value is None and isinstance(params, dict):
                value = params.get(key)
            if isinstance(value, bool):
                if value:
                    return True
                continue
            text = str(value or "").strip().lower()
            if text in ("1", "true", "yes", "ok", "success", "completed", "done", "finished"):
                return True
        progress = task.get("progress")
        if progress is None and isinstance(params, dict):
            progress = params.get("progress")
        if progress is not None:
            try:
                p = float(progress)
                if p <= 1:
                    return p >= 0.999
                return p >= 100
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
    def _coerce_float(value: Any) -> Optional[float]:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            try:
                return float(value)
            except Exception:
                return None
        text = str(value).strip()
        if not text:
            return None
        text = text.replace(",", "")
        try:
            return float(text)
        except Exception:
            pass
        match = re.search(r"-?\d+(?:\.\d+)?", text)
        if not match:
            return None
        try:
            return float(match.group(0))
        except Exception:
            return None

    def _task_lookup_values(self, task: Dict[str, Any], keys: List[str], max_depth: int = 4) -> List[Any]:
        if not isinstance(task, dict) or not keys:
            return []
        wanted = {str(k or "").strip().lower() for k in keys if str(k or "").strip()}
        values: List[Any] = []

        def walk(node: Any, depth: int):
            if depth > max_depth:
                return
            if isinstance(node, dict):
                for k, v in node.items():
                    try:
                        key_text = str(k or "").strip().lower()
                    except Exception:
                        key_text = ""
                    if key_text in wanted:
                        values.append(v)
                    if isinstance(v, (dict, list)):
                        walk(v, depth + 1)
            elif isinstance(node, list):
                for item in node:
                    if isinstance(item, (dict, list)):
                        walk(item, depth + 1)

        walk(task, 0)
        params = task.get("params")
        if isinstance(params, dict):
            walk(params, 0)
        return values

    def _task_number_by_keys(self, task: Dict[str, Any], keys: List[str]) -> Optional[float]:
        for value in self._task_lookup_values(task=task, keys=keys):
            number = self._coerce_float(value)
            if number is not None:
                return number
        return None

    def _task_text_by_keys(self, task: Dict[str, Any], keys: List[str]) -> str:
        for value in self._task_lookup_values(task=task, keys=keys):
            text = str(value or "").strip()
            if text:
                return text
        return ""

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
        speed_text_keys = [
            key,
            "download_speed",
            "speed",
            "dl_speed",
            "current_speed",
            "downloadspeed",
            "speed_download",
            "download_speed_text",
            "speed_text",
        ]
        speed_text = self._task_text_by_keys(task=task, keys=speed_text_keys)
        if speed_text and re.search(r"/s", speed_text, flags=re.IGNORECASE):
            return speed_text.replace(" ", "")
        value = self._task_number_by_keys(task=task, keys=speed_text_keys)
        if value is None:
            return None
        try:
            size = float(value)
            units = ["B/s", "KB/s", "MB/s", "GB/s"]
            idx = 0
            while size >= 1024 and idx < len(units) - 1:
                size /= 1024.0
                idx += 1
            if idx == 0:
                return f"{int(size)}{units[idx]}"
            return f"{size:.1f}{units[idx]}"
        except Exception:
            return None

    def _task_left_time(self, task: Dict[str, Any], progress: float) -> Optional[str]:
        left_time_keys = [
            "left_time",
            "remaining_time",
            "remain_time",
            "time_remaining",
            "eta",
            "predict_left_time",
            "left_time_text",
            "remaining_time_text",
            "eta_text",
        ]
        value = self._task_number_by_keys(task=task, keys=left_time_keys)
        if value is not None and value >= 0:
            seconds = float(value)
            # 极大值通常为毫秒
            if seconds > 315360000:
                seconds = seconds / 1000.0
            return self._format_seconds(seconds)

        text_value = self._task_text_by_keys(task=task, keys=left_time_keys)
        if text_value:
            text_value = text_value.strip()
            if ":" in text_value or "秒" in text_value or "分" in text_value or "h" in text_value.lower():
                return text_value
        total_size = float(self._task_size(task) or 0)
        speed = float(self._task_number_by_keys(task=task, keys=[
            "download_speed",
            "speed",
            "dl_speed",
            "current_speed",
            "downloadspeed",
            "speed_download",
        ]) or 0)
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
        move_key = self._task_move_key(task)
        if move_key and move_key in self._moved_task_keys:
            return True
        # 兼容历史 moved key（曾使用纯 task_id）
        task_key = self._task_key(task)
        if task_key and task_key in self._moved_task_keys:
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
        for key in ("task_id", "id", "gid", "taskid", "taskId", "record_id", "download_id"):
            value = data.get(key)
            if value:
                return str(value)
        for key in ("task", "data", "result", "params", "item"):
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

    def _task_move_key(self, task: Dict[str, Any]) -> str:
        task_id = self._task_key(task)
        if task_id:
            return f"id:{task_id}"
        task_name = Path(str(self._task_name(task) or "")).name
        task_norm = self._normalize_name(task_name)
        if not task_norm:
            return ""
        return f"name:{task_norm}|size:{int(self._task_size(task) or 0)}"

    @staticmethod
    def _task_space(task: Dict[str, Any]) -> str:
        if not isinstance(task, dict):
            return ""
        for key in ("target", "space", "device_space", "deviceSpace"):
            value = task.get(key)
            if value:
                return str(value).strip()
        params = task.get("params")
        if isinstance(params, dict):
            for key in ("target", "space", "device_space", "deviceSpace"):
                value = params.get(key)
                if value:
                    return str(value).strip()
        return ""

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
        if not task_raw or not source_root.exists() or not source_root.is_dir():
            return None
        task_base = Path(task_raw).name
        task_stem = Path(task_base).stem
        task_norm = XunleiHijackDownloader._normalize_name(task_stem or task_base)
        if not task_norm:
            return None
        candidates: List[Tuple[int, float, Path]] = []
        try:
            scanned = 0
            # 递归匹配子目录，避免仅扫描第一层导致漏搬；限制上限防止大目录过慢。
            for item in source_root.rglob("*"):
                scanned += 1
                if scanned > 5000:
                    break
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

    def _is_device_space_not_active(self, obj: Any = None, error_text: str = "") -> bool:
        texts = [str(error_text or "").lower(), str(self._last_request_error or "").lower()]
        if isinstance(obj, dict):
            for key in ("error", "err", "message", "msg", "detail", "error_code"):
                value = obj.get(key)
                if value is not None:
                    texts.append(str(value).lower())
        merged = " ".join(texts)
        return any(
            flag in merged for flag in (
                "device_space_not_active",
                "device space not active",
                "space_name_invalid",
                "invalid space",
            )
        )

    def _refresh_device_id_on_inactive_space(self, obj: Any = None, error_text: str = "") -> bool:
        if not self._is_device_space_not_active(obj=obj, error_text=error_text):
            return False
        old_device = str(self._device_id or "").strip()
        new_device = self._fetch_device_id(force_refresh=True, exclude_device=old_device)
        if new_device:
            logger.warn(
                f"XunleiHijack[v{self.plugin_version}] detect inactive device_space, "
                f"refresh device_id: {old_device or 'EMPTY'} -> {new_device}"
            )
            return True
        logger.warn(
            f"XunleiHijack[v{self.plugin_version}] detect inactive device_space, "
            f"but refresh device_id failed."
        )
        return False

    @staticmethod
    def _append_device_candidate(candidates: List[str], device: str) -> None:
        token = str(device or "").strip()
        if not token:
            return

        def _add(value: str) -> None:
            text = str(value or "").strip()
            if text and text not in candidates:
                candidates.append(text)

        _add(token)
        if token.endswith("#"):
            _add(token[:-1])
        elif "#" not in token:
            _add(f"{token}#")

    def _is_device_candidate_active(self, device: str) -> bool:
        token = str(device or "").strip()
        if not token or not self._base_url:
            return False
        url = (
            f"{self._base_url}/webman/3rdparty/pan-xunlei-com/index.cgi/drive/v1/tasks"
            f"?type=user%23download-url&device_space={quote(token)}"
        )
        resp, obj = self._request_json(
            method="GET",
            url=url,
            headers={**self._get_headers(), "device-space": token},
            timeout=20,
            retry_auth=True,
            retry_count=1,
        )
        if not resp:
            return False
        if self._is_device_space_not_active(obj=obj, error_text=self._last_request_error):
            return False
        return bool(resp.ok)

    def _pick_active_device_id(self, candidates: List[str], exclude_device: str = "", old_device: str = "") -> Optional[str]:
        dedup: List[str] = []
        for item in candidates:
            token = str(item or "").strip()
            if token and token not in dedup:
                dedup.append(token)

        exclude = str(exclude_device or "").strip()
        old = str(old_device or "").strip()

        preferred = [x for x in dedup if x != exclude]
        if old and old not in preferred and old != exclude:
            preferred.append(old)

        for device in preferred:
            if self._is_device_candidate_active(device):
                return device

        # 全部探测失败时，返回一个候选，避免完全不可用。
        if preferred:
            return preferred[0]
        return None

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
