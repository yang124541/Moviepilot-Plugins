import json
import re
import shutil
import threading
import time
import hashlib
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Set, Tuple, Union
from urllib.parse import quote

import requests
from apscheduler.schedulers.background import BackgroundScheduler

import app.schemas as schemas
from app.core.config import settings
from app.log import logger
from app.plugins import _PluginBase
from app.schemas.types import TorrentStatus

try:
    from app.db.downloadhistory_oper import DownloadHistoryOper
except Exception:
    DownloadHistoryOper = None

try:
    from app.helper.directory import DirectoryHelper
except Exception:
    DirectoryHelper = None


class XunleiHijackDownloader(_PluginBase):
    plugin_name = "迅雷下载接管"
    plugin_desc = "接管 MoviePilot 下载到迅雷，并可自动搬运到监控目录。"
    plugin_icon = "https://raw.githubusercontent.com/yang124541/moviepilot-plugin/main/xunlei.png"
    plugin_version = "2.3.0"
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
    _completed_seen_fail_count: Dict[str, int] = {}
    _completed_seen_next_try_at: Dict[str, float] = {}
    _max_moved_keys = 2000
    _max_completed_seen_keys = 4000
    _completed_seen_ttl_seconds = 86400
    _completed_seen_max_missing = 20
    _last_request_error = ""
    _task_list_cache: Dict[str, Dict[str, Any]] = {}
    _task_list_cache_ttl_seconds = 1.0
    _task_list_cache_ttl_ui_seconds = 1.0
    _ui_last_active_ts = 0.0
    _ui_keepalive_seconds = 20.0
    _movie_video_suffixes: Set[str] = {
        ".mkv", ".mp4", ".avi", ".mov", ".flv", ".wmv", ".ts", ".m2ts",
        ".mpg", ".mpeg", ".iso", ".rmvb", ".webm", ".m4v"
    }

    def init_plugin(self, config: dict = None):
        self.stop_service()
        self._moved_task_order = self._load_moved_task_keys()
        self._moved_task_keys = set(self._moved_task_order)
        self._task_name_cache = {}
        self._task_list_cache = {}
        self._ui_last_active_ts = 0.0
        self._completed_seen_at = {}
        self._completed_seen_order = []
        self._completed_seen_name = {}
        self._completed_seen_fail_count = {}
        self._completed_seen_next_try_at = {}
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
        self._load_completed_seen_cache()

        if self._enabled and self._auto_refresh_pan_auth and not self._pan_auth:
            self._pan_auth = self._fetch_pan_auth() or self._pan_auth
        if self._enabled and self._move_enabled:
            self._start_move_scheduler()
        self._save_config()
        logger.info(
            f"迅雷接管[v{self.plugin_version}]初始化完成："
            f"启用={self._enabled}，地址={self._base_url}，自动搬运={self._move_enabled}"
        )

    def get_state(self) -> bool:
        return self._enabled

    def _touch_ui_active(self) -> None:
        self._ui_last_active_ts = time.time()

    def _is_ui_active(self) -> bool:
        try:
            last_ts = float(self._ui_last_active_ts or 0.0)
        except Exception:
            last_ts = 0.0
        if last_ts <= 0:
            return False
        return (time.time() - last_ts) <= float(self._ui_keepalive_seconds)

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
            {
                "path": "/task/metrics",
                "endpoint": self.api_task_metrics,
                "methods": ["GET"],
                "allow_anonymous": True,
                "summary": "获取迅雷任务速率信息",
                "description": "用于数据页局部刷新“大小/剩余时间/速度”",
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
        plugin_id = self.__class__.__name__
        if not self._enabled:
            return [{
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
            }]
        self._touch_ui_active()
        page: List[dict] = [
            {
                "component": "VRow",
                "props": {"align": "center", "id": "xunlei-plugin-page-root"},
                "content": [
                    {
                        "component": "VCol",
                        "props": {"cols": 12},
                        "content": [
                            {
                                "component": "img",
                                "props": {
                                    "src": "data:image/gif;base64,R0lGODlhAQABAIAAAAAAAP///ywAAAAAAQABAAACAUwAOw==",
                                    "style": "display:none;width:0;height:0;",
                                    "onload": self._build_page_metrics_poller_onerror(
                                        plugin_id=plugin_id,
                                        interval_ms=1000,
                                    ),
                                    "onerror": self._build_page_metrics_poller_onerror(
                                        plugin_id=plugin_id,
                                        interval_ms=1000,
                                    ),
                                },
                            },
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

        tasks = self._list_download_tasks(include_runner=False, phase_mode="all", purpose="ui")
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
                "props": {"class": "my-0 py-0", "noGutters": True},
                "content": [
                    {
                        "component": "VCol",
                        "props": {"cols": 12, "class": "py-0"},
                        "content": [self._build_task_row(task=task)],
                    }
                ],
            })
        return page

    def api_start_task(self, task_id: str = "", hash: str = "", space: str = "", task_type: str = "") -> schemas.Response:
        if not self._enabled:
            return schemas.Response(success=False, message="插件未启用。")
        return self._api_task_action(task_id=task_id or hash, action="start", space=space, task_type=task_type)

    def api_pause_task(self, task_id: str = "", hash: str = "", space: str = "", task_type: str = "") -> schemas.Response:
        if not self._enabled:
            return schemas.Response(success=False, message="插件未启用。")
        return self._api_task_action(task_id=task_id or hash, action="pause", space=space, task_type=task_type)

    def api_delete_task(self, task_id: str = "", hash: str = "", delete_file: bool = True, space: str = "", task_type: str = "") -> schemas.Response:
        if not self._enabled:
            return schemas.Response(success=False, message="插件未启用。")
        return self._api_task_action(task_id=task_id or hash, action="delete", delete_file=delete_file, space=space, task_type=task_type)

    def api_task_metrics(self) -> Dict[str, Any]:
        if not self._enabled:
            return {"success": True, "items": []}
        try:
            self._touch_ui_active()
            tasks = self._list_download_tasks(include_runner=False, phase_mode="all", purpose="ui")
            items: List[Dict[str, Any]] = []
            for task in tasks:
                if self._is_moved_task(task):
                    continue
                dom_key = self._task_dom_key(task)
                if not dom_key:
                    continue
                progress = self._task_progress(task)
                size_text = self._format_bytes(self._task_size(task))
                left_time, speed_text = self._task_metric_texts(task=task, progress=progress)
                progress_color = "primary"
                task_state = self._task_progress_state(task)
                items.append({
                    "key": dom_key,
                    "size_text": size_text,
                    "left_time": left_time,
                    "speed_text": speed_text,
                    "progress": progress,
                    "progress_color": progress_color,
                    "state": task_state,
                    "metric": f"{size_text}    {left_time}    {speed_text}",
                })
            return {"success": True, "items": items}
        except Exception as err:
            logger.warn(f"迅雷任务指标接口失败：{err}")
            return {"success": False, "items": []}

    def _api_task_action(self, task_id: str, action: str, delete_file: bool = True, space: str = "", task_type: str = "") -> schemas.Response:
        task_key = str(task_id or "").strip()
        if not task_key:
            return schemas.Response(success=False, message="任务ID不能为空。")
        self._touch_ui_active()
        logger.info(
            f"收到任务控制请求[v{self.plugin_version}]：action={action}，task_id={task_key}，"
            f"space={space or 'EMPTY'}，type={task_type or 'EMPTY'}"
        )
        if action != "delete" and f"id:{task_key}" in self._moved_task_keys:
            return schemas.Response(success=False, message="任务已迁移，无法继续操作。")
        action_candidates: List[str] = [action]

        ok = False
        for act in action_candidates:
            ok = self._operate_tasks(
                ids={task_key},
                action=act,
                delete_file=bool(delete_file),
                preferred_space=space,
                preferred_type=task_type,
            )
            if ok:
                break
        if ok and action == "delete":
            self._remember_moved_key(task_key)
        if ok:
            self._task_list_cache = {}
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
        dom_key = self._task_dom_key(task)
        task_done = self._is_task_completed(task)
        task_state = self._task_progress_state(task)
        task_failed = self._is_task_failed(task)

        progress = self._task_progress(task)
        size_text = self._format_bytes(self._task_size(task))
        left_time, speed_text = self._task_metric_texts(task=task, progress=progress)
        can_start = bool(task_id)
        can_pause = bool(task_id)
        can_delete = bool(task_id)
        quoted_id = quote(task_id or "", safe="")
        task_space = self._task_space(task)
        task_type = self._task_type(task)
        space_qs = f"&space={quote(task_space or '', safe='')}"
        type_qs = f"&task_type={quote(task_type or '', safe='')}"
        start_api = f"/api/v1/plugin/{plugin_id}/task/start?task_id={quoted_id}{space_qs}{type_qs}"
        pause_api = f"/api/v1/plugin/{plugin_id}/task/pause?task_id={quoted_id}{space_qs}{type_qs}"
        delete_api = f"/api/v1/plugin/{plugin_id}/task/delete?task_id={quoted_id}&delete_file=true{space_qs}{type_qs}"
        btn_key = dom_key or quoted_id or "unknown"
        toggle_btn_id = f"xunlei-action-toggle-{btn_key}"
        delete_btn_id = f"xunlei-action-delete-{btn_key}"
        progress_color = "primary"
        toggle_is_retry = (task_state == "failed")
        toggle_is_start = (task_state == "paused") or toggle_is_retry
        toggle_text = "重试" if toggle_is_retry else ("开始" if toggle_is_start else "暂停")
        toggle_color = "warning" if toggle_is_retry else ("success" if toggle_is_start else "warning")
        toggle_icon = "mdi-refresh" if toggle_is_retry else ("mdi-play" if toggle_is_start else "mdi-pause")
        toggle_api = start_api if toggle_is_start else pause_api
        toggle_success_message = (
            "重试任务成功，请点击刷新查看状态。"
            if toggle_is_retry
            else ("开始任务成功，请点击刷新查看状态。" if toggle_is_start else "暂停任务成功，请点击刷新查看状态。")
        )
        toggle_failure_message = (
            "重试任务失败。"
            if toggle_is_retry
            else ("开始任务失败。" if toggle_is_start else "暂停任务失败。")
        )

        image_node: Dict[str, Any] = {
            "component": "VImg",
            "props": {
                "src": "https://backstage-img-ssl.a.88cdn.com/65d616355857aef8af40b89f187a8cf2770cb0ce",
                "width": 28,
                "height": 28,
                "cover": False,
            },
        }
        toggle_button = self._build_task_action_button(
            text=toggle_text,
            color=toggle_color,
            icon=toggle_icon,
            disabled=not (can_start or can_pause),
            api_path=toggle_api,
            button_id=toggle_btn_id,
            success_message=toggle_success_message,
            failure_message=toggle_failure_message,
        )
        toggle_button["props"].update({
            "data-xunlei-api-start": str(start_api or ""),
            "data-xunlei-api-pause": str(pause_api or ""),
            "data-xunlei-start-success": "开始任务成功，请点击刷新查看状态。",
            "data-xunlei-pause-success": "暂停任务成功，请点击刷新查看状态。",
            "data-xunlei-retry-success": "重试任务成功，请点击刷新查看状态。",
            "data-xunlei-start-failure": "开始任务失败。",
            "data-xunlei-pause-failure": "暂停任务失败。",
            "data-xunlei-retry-failure": "重试任务失败。",
            "data-xunlei-hover-color": "retry" if toggle_is_retry else ("success" if toggle_is_start else "warning"),
            "data-xunlei-icon-mode": "retry" if toggle_is_retry else ("start" if toggle_is_start else "pause"),
        })

        return {
            "component": "VCard",
            "props": {"id": f"xunlei-task-row-{dom_key or btn_key}", "variant": "text", "style": "margin-top:10px;margin-bottom:0;"},
            "content": [
                {
                    "component": "VCardText",
                    "props": {"class": "px-2", "style": "padding-top:8px;padding-bottom:8px;"},
                    "content": [
                        {
                            "component": "VRow",
                            "props": {"align": "center", "noGutters": True, "class": "my-0 py-0", "style": "position:relative;flex-wrap:nowrap;padding-right:152px;"},
                            "content": [
                                {"component": "VCol", "props": {"cols": "auto", "class": "py-0 pr-4 d-flex align-center"}, "content": [image_node]},
                                {
                                    "component": "VCol",
                                    "props": {"class": "py-0", "style": "flex:1 1 auto;min-width:0;padding-right:16px;"},
                                    "content": [
                                        {
                                            "component": "VListItem",
                                            "props": {
                                                "title": task_name,
                                                "density": "compact",
                                                "class": "px-0",
                                                "style": "padding-inline-start:0;padding-inline-end:0;",
                                            },
                                        }
                                    ],
                                },
                                {
                                    "component": "VCol",
                                    "props": {"class": "py-0", "style": "flex:0 1 560px;min-width:320px;max-width:760px;margin-left:auto;"},
                                    "content": [
                                        {
                                            "component": "div",
                                            "props": {
                                                "class": "py-0",
                                                "style": "min-height:18px;font-size:10px;line-height:1.2;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;",
                                            },
                                            "content": [
                                                {
                                                    "component": "span",
                                                    "props": {
                                                        "id": f"xunlei-metric-size-{dom_key}",
                                                        "textContent": size_text,
                                                        "style": "display:inline-block;margin-right:45px;",
                                                    },
                                                },
                                                {
                                                    "component": "span",
                                                    "props": {
                                                        "id": f"xunlei-metric-left-{dom_key}",
                                                        "textContent": left_time,
                                                        "style": "display:inline-block;margin-right:45px;",
                                                    },
                                                },
                                                {
                                                    "component": "span",
                                                    "props": {
                                                        "id": f"xunlei-metric-speed-{dom_key}",
                                                        "textContent": speed_text,
                                                        "style": "display:inline-block;",
                                                    },
                                                }
                                            ],
                                        },
                                        {
                                            "component": "VProgressLinear",
                                            "props": {
                                                "id": f"xunlei-progress-{dom_key}",
                                                "modelValue": progress,
                                                "height": 5,
                                                "rounded": True,
                                                "color": "primary",
                                                "style": "width:100%;",
                                            },
                                        },
                                    ],
                                },
                                {
                                    "component": "VCol",
                                    "props": {"cols": "auto", "class": "d-flex justify-end ga-1 py-0", "style": "position:absolute;right:76px;top:50%;transform:translateY(-50%);width:76px;max-width:76px;flex:0 0 76px;z-index:1;"},
                                    "content": [
                                        toggle_button,
                                        self._build_task_action_button(
                                            text="删除",
                                            color="error",
                                            icon="mdi-close",
                                            disabled=not can_delete,
                                            api_path=delete_api,
                                            button_id=delete_btn_id,
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
                                  button_id: str,
                                  success_message: str, failure_message: str) -> Dict[str, Any]:
        button = {
            "component": "VBtn",
            "props": {
                "size": "x-small",
                "density": "compact",
                "variant": "text",
                "color": color,
                "text": "",
                "prependIcon": icon,
                "title": text,
                "disabled": bool(disabled),
                "class": "ml-1 xunlei-action-btn",
                "rounded": "sm",
                "style": "position:relative;min-width:28px;width:28px;height:28px;padding:0;opacity:1;background:transparent;transition:box-shadow .15s ease,opacity .08s linear;",
                "id": str(button_id or ""),
                "data-xunlei-api": str(api_path or ""),
                "data-xunlei-success": str(success_message or ""),
                "data-xunlei-failure": str(failure_message or ""),
                "data-xunlei-hover-color": str(color or "primary"),
            },
        }
        return button

    @staticmethod
    def _build_action_onclick(api_path: str) -> str:
        path = str(api_path or "").replace("\\", "\\\\").replace("'", "\\'")
        return (
            "(async()=>{"
            f"try{{const r=await fetch('{path}',{{method:'GET',credentials:'same-origin'}});"
            "const j=await r.json().catch(()=>null);"
            "if(r.ok&&(!j||j.success!==false)){return;}"
            "alert((j&&j.message)?j.message:'操作失败，请查看日志');"
            "}catch(e){alert('请求失败，请检查网络或权限');}"
            "})();"
        )

    @staticmethod
    def _build_page_metrics_poller_onerror(plugin_id: str, interval_ms: int = 1000) -> str:
        plugin = str(plugin_id or "").strip().replace("\\", "\\\\").replace("'", "\\'")
        try:
            ms = int(interval_ms)
        except Exception:
            ms = 1000
        if ms < 1000:
            ms = 1000
        metrics_api = f"/api/v1/plugin/{plugin}/task/metrics"
        return (
            "(function(){"
            "try{"
            "const rootId='xunlei-plugin-page-root';"
            "const onPage=()=>{try{return !!document.getElementById(rootId);}catch(_e){return false;}};"
            "const stopPoller=()=>{"
            "try{if(window.__xunleiMetricsPollerTimer){clearInterval(window.__xunleiMetricsPollerTimer);}}catch(_e){}"
            "window.__xunleiMetricsPollerTimer=0;"
            "};"
            "if(window.__xunleiMetricsPollerTimer){"
            "if(!onPage()){stopPoller();return;}"
            "try{if(typeof window.__xunleiBindActionButtons==='function'){window.__xunleiBindActionButtons();}}catch(_e){}"
            "return;"
            "}"
            f"const u='{metrics_api}';"
            f"const minGapMs={ms};"
            "const centerActionIcon=(node)=>{"
            "try{"
            "if(!node){return;}"
            "node.style.display='inline-flex';"
            "node.style.alignItems='center';"
            "node.style.justifyContent='center';"
            "const prepend=node.querySelector('.v-btn__prepend');"
            "const content=node.querySelector('.v-btn__content');"
            "const nodeId=String((node&&node.id)||'');"
            "const isToggle=(nodeId.indexOf('xunlei-action-toggle-')===0);"
            "if(isToggle){"
            "const apiText=String(node.getAttribute('data-xunlei-api')||'').toLowerCase();"
            "let iconMode=String(node.getAttribute('data-xunlei-icon-mode')||'').toLowerCase();"
            "if(iconMode!=='start'&&iconMode!=='pause'&&iconMode!=='retry'){iconMode='';}"
            "if(!iconMode){iconMode=(apiText.indexOf('/task/start')>=0)?'start':'pause';}"
            "const playSvg='<svg viewBox=\"0 0 24 24\" width=\"14\" height=\"14\" aria-hidden=\"true\" focusable=\"false\"><polygon points=\"7,5 19,12 7,19\" fill=\"#4CAF50\"></polygon></svg>';"
            "const pauseSvg='<svg viewBox=\"0 0 24 24\" width=\"14\" height=\"14\" aria-hidden=\"true\" focusable=\"false\"><rect x=\"6\" y=\"5\" width=\"4\" height=\"14\" rx=\"1\" fill=\"#FB8C00\"></rect><rect x=\"14\" y=\"5\" width=\"4\" height=\"14\" rx=\"1\" fill=\"#FB8C00\"></rect></svg>';"
            "const retrySvg='<svg viewBox=\"0 0 24 24\" width=\"14\" height=\"14\" aria-hidden=\"true\" focusable=\"false\"><path fill=\"currentColor\" d=\"M17.65 6.35A7.96 7.96 0 0 0 12 4a8 8 0 1 0 8 8h-2a6 6 0 1 1-1.76-4.24L13 11h7V4z\"></path></svg>';"
            "if(prepend){prepend.style.display='none';}"
            "if(content){content.style.display='inline-flex';content.style.alignItems='center';content.style.justifyContent='center';content.style.fontSize='13px';content.style.lineHeight='1';content.style.fontWeight='700';content.style.color=(iconMode==='retry')?'#90A4AE':'';content.innerHTML=(iconMode==='retry')?retrySvg:(iconMode==='start'?playSvg:pauseSvg);}"
            "node.style.color='';"
            "node.style.opacity='1';"
            "return;"
            "}"
            "if(prepend){"
            "prepend.style.display='inline-flex';"
            "prepend.style.alignItems='center';"
            "prepend.style.justifyContent='center';"
            "prepend.style.marginInlineStart='0';"
            "prepend.style.marginInlineEnd='0';"
            "prepend.style.position='absolute';"
            "prepend.style.left='50%';"
            "prepend.style.top='50%';"
            "prepend.style.transform='translate(-50%,-50%)';"
            "}"
            "if(content){"
            "content.style.display='inline-flex';"
            "content.style.alignItems='center';"
            "content.style.justifyContent='center';"
            "}"
            "const icon=node.querySelector('.v-icon');"
            "if(icon){icon.style.margin='0';icon.style.lineHeight='1';}"
            "node.style.opacity='1';"
            "}catch(_e){}"
            "};"
            "const bindActionButtons=()=>{"
            "try{"
            "const hoverShadow=(c)=>{"
            "const k=String(c||'').toLowerCase();"
            "if(k==='success'){return '0 0 0 12px rgba(76,175,80,.62),0 0 24px rgba(76,175,80,.54)';}"
            "if(k==='warning'){return '0 0 0 12px rgba(251,140,0,.62),0 0 24px rgba(251,140,0,.54)';}"
            "if(k==='retry'){return '0 0 0 12px rgba(144,164,174,.52),0 0 24px rgba(144,164,174,.44)';}"
            "if(k==='error'){return '0 0 0 12px rgba(255,82,82,.62),0 0 24px rgba(255,82,82,.54)';}"
            "return '0 0 0 12px rgba(25,118,210,.62),0 0 24px rgba(25,118,210,.54)';"
            "};"
            "const nodes=document.querySelectorAll('[id^=\"xunlei-action-\"]');"
            "if(!nodes||!nodes.length){return;}"
            "for(const node of nodes){"
            "if(!node||node.dataset.xunleiBound==='1'){continue;}"
            "node.dataset.xunleiBound='1';"
            "centerActionIcon(node);"
            "node.addEventListener('mouseenter',()=>{"
            "try{if(node.disabled===true||node.getAttribute('aria-disabled')==='true'){return;}"
            "node.style.boxShadow=hoverShadow(node.getAttribute('data-xunlei-hover-color'));}catch(_e){}"
            "},true);"
            "node.addEventListener('mouseleave',()=>{"
            "try{node.style.boxShadow='none';}catch(_e){}"
            "},true);"
            "node.addEventListener('click',async(ev)=>{"
            "try{if(ev){ev.preventDefault();ev.stopPropagation();}}catch(_e){}"
            "if(node.getAttribute('aria-disabled')==='true'||node.disabled===true){return;}"
            "if(node.dataset.xunleiBusy==='1'){return;}"
            "const api=node.getAttribute('data-xunlei-api')||'';"
            "if(!api){return;}"
            "const nodeId=String((node&&node.id)||'');"
            "const deletePrefix='xunlei-action-delete-';"
            "const isDelete=(nodeId.indexOf(deletePrefix)===0);"
            "const rowKey=isDelete?nodeId.slice(deletePrefix.length):'';"
            "const failMsg=node.getAttribute('data-xunlei-failure')||'操作失败';"
            "node.dataset.xunleiBusy='1';"
            "node.style.pointerEvents='none';"
            "try{"
            "const r=await fetch(api,{method:'GET',credentials:'same-origin',cache:'no-store'});"
            "const j=await r.json().catch(()=>null);"
            "const ok=(r.ok&&(!j||j.success!==false));"
            "if(!ok){alert((j&&j.message)?j.message:failMsg);return;}"
            "if(isDelete&&rowKey){"
            "const row=document.getElementById('xunlei-task-row-'+rowKey);"
            "if(row){try{row.remove();}catch(_e){try{if(row.parentNode){row.parentNode.removeChild(row);}}catch(__e){}}}"
            "}"
            "}catch(e){alert('请求失败，请检查网络或权限');}"
            "finally{node.dataset.xunleiBusy='0';node.style.pointerEvents='auto';}"
            "},true);"
            "}"
            "}catch(e){}"
            "};"
            "window.__xunleiBindActionButtons=bindActionButtons;"
            "const normalizeState=(raw)=>{"
            "const s=String(raw||'').trim().toLowerCase();"
            "if(!s){return 'downloading';}"
            "if(s.indexOf('phase_type_complete')>=0||s.indexOf('phase_type_finished')>=0||s.indexOf('complete')>=0||s.indexOf('finished')>=0||s.indexOf('done')>=0){return 'completed';}"
            "if(s.indexOf('phase_type_error')>=0||s.indexOf('failed')>=0||s.indexOf('fail')>=0||s.indexOf('error')>=0||s.indexOf('invalid')>=0||s.indexOf('失败')>=0||s.indexOf('错误')>=0){return 'failed';}"
            "if(s.indexOf('phase_type_paused')>=0||s.indexOf('paused')>=0||s.indexOf('pause')>=0||s.indexOf('suspend')>=0||s.indexOf('stopped')>=0||s.indexOf('halt')>=0||s.indexOf('暂停')>=0||s.indexOf('已暂停')>=0||s.indexOf('已停止')>=0){return 'paused';}"
            "if(s.indexOf('phase_type_pending')>=0||s.indexOf('pending')>=0||s.indexOf('waiting')>=0||s.indexOf('queue')>=0||s.indexOf('排队')>=0||s.indexOf('等待')>=0){return 'queued';}"
            "return 'downloading';"
            "};"
            "const resolveState=(it)=>normalizeState((it&&it.state)?it.state:'');"
            "const forceActionIcon=(btn,iconName)=>{"
            "try{"
            "if(!btn){return;}"
            "const name=String(iconName||'mdi-play').trim()||'mdi-play';"
            "const prepend=btn.querySelector('.v-btn__prepend');"
            "const content=btn.querySelector('.v-btn__content');"
            "let host=prepend||content||btn;"
            "let iconEl=(host&&host.querySelector)?host.querySelector('.v-icon'):null;"
            "if(!iconEl){iconEl=btn.querySelector('.v-icon');}"
            "const iconTag=(iconEl&&iconEl.tagName)?String(iconEl.tagName).toLowerCase():'';"
            "const needRebuild=(!iconEl)||(iconTag==='svg')||(!!(iconEl&&iconEl.querySelector&&iconEl.querySelector('svg')));"
            "if(needRebuild){"
            "if(prepend){host=prepend;prepend.style.display='inline-flex';prepend.style.alignItems='center';prepend.style.justifyContent='center';}"
            "else if(content){host=content;content.style.display='inline-flex';content.style.alignItems='center';content.style.justifyContent='center';}"
            "if(!host){return;}"
            "host.innerHTML='';"
            "iconEl=document.createElement('i');"
            "iconEl.className='v-icon notranslate mdi '+name;"
            "iconEl.setAttribute('aria-hidden','true');"
            "host.appendChild(iconEl);"
            "return;"
            "}"
            "for(const cls of Array.from(iconEl.classList)){if(String(cls||'').indexOf('mdi-')===0){iconEl.classList.remove(cls);}}"
            "iconEl.classList.add('mdi');"
            "iconEl.classList.add(name);"
            "}catch(_e){}"
            "};"
            "const applyToggleAction=(k,state)=>{"
            "const btn=document.getElementById('xunlei-action-toggle-'+k);"
            "if(!btn){return;}"
            "const stateToken=String(state||'');"
            "const isFailed=(stateToken==='failed');"
            "const useStart=(stateToken==='paused'||isFailed);"
            "const apiStart=btn.getAttribute('data-xunlei-api-start')||'';"
            "const apiPause=btn.getAttribute('data-xunlei-api-pause')||'';"
            "const okStart=btn.getAttribute('data-xunlei-start-success')||'开始任务成功，请点击刷新查看状态。';"
            "const okPause=btn.getAttribute('data-xunlei-pause-success')||'暂停任务成功，请点击刷新查看状态。';"
            "const okRetry=btn.getAttribute('data-xunlei-retry-success')||'重试任务成功，请点击刷新查看状态。';"
            "const failStart=btn.getAttribute('data-xunlei-start-failure')||'开始任务失败。';"
            "const failPause=btn.getAttribute('data-xunlei-pause-failure')||'暂停任务失败。';"
            "const failRetry=btn.getAttribute('data-xunlei-retry-failure')||'重试任务失败。';"
            "btn.setAttribute('title',isFailed?'重试':(useStart?'开始':'暂停'));"
            "btn.setAttribute('data-xunlei-api',useStart?apiStart:apiPause);"
            "btn.setAttribute('data-xunlei-success',isFailed?okRetry:(useStart?okStart:okPause));"
            "btn.setAttribute('data-xunlei-failure',isFailed?failRetry:(useStart?failStart:failPause));"
            "btn.setAttribute('data-xunlei-hover-color',isFailed?'retry':(useStart?'success':'warning'));"
            "btn.setAttribute('data-xunlei-icon-mode',isFailed?'retry':(useStart?'start':'pause'));"
            "btn.classList.remove('text-success','text-warning');"
            "btn.classList.add(isFailed?'text-warning':(useStart?'text-success':'text-warning'));"
            "centerActionIcon(btn);"
            "};"
            "const applyProgress=(k,it)=>{"
            "const pRaw=(it&&it.progress!=null)?Number(it.progress):NaN;"
            "const p=Number.isFinite(pRaw)?Math.max(0,Math.min(100,pRaw)):0;"
            "const state=resolveState(it);"
            "const bar=document.getElementById('xunlei-progress-'+k);"
            "if(!bar){return;}"
            "bar.setAttribute('aria-valuenow',String(p));"
            "bar.setAttribute('data-xunlei-state',state);"
            "const det=bar.querySelector('.v-progress-linear__determinate');"
            "if(det){det.style.width=p+'%';det.style.opacity='1';}"
            "const bg=bar.querySelector('.v-progress-linear__background');"
            "if(bg){bg.style.opacity='0.2';}"
            "};"
            "const f=async()=>{"
            "try{"
            "if(!onPage()){stopPoller();return;}"
            "if(document&&document.visibilityState==='hidden'){return;}"
            "if(window.__xunleiMetricsInflight===1){return;}"
            "const now=Date.now();"
            "const lastRun=Number(window.__xunleiMetricsLastRunAt||0);"
            "if(lastRun&&now-lastRun<minGapMs){return;}"
            "window.__xunleiMetricsLastRunAt=now;"
            "window.__xunleiMetricsInflight=1;"
            "bindActionButtons();"
            "const r=await fetch(u,{method:'GET',credentials:'same-origin',cache:'no-store'});"
            "const j=await r.json().catch(()=>null);"
            "if(!r.ok||!j||j.success===false||!Array.isArray(j.items)){return;}"
            "for(const it of j.items){"
            "const k=(it&&it.key)?String(it.key):'';"
            "if(!k){continue;}"
            "const sizeEl=document.getElementById('xunlei-metric-size-'+k);"
            "if(sizeEl){sizeEl.textContent=(it&&it.size_text)?String(it.size_text):'--';}"
            "const leftEl=document.getElementById('xunlei-metric-left-'+k);"
            "const speedEl=document.getElementById('xunlei-metric-speed-'+k);"
            "const state=resolveState(it);"
            "let leftText=(it&&it.left_time)?String(it.left_time):'--';"
            "let speedText=(it&&it.speed_text!=null)?String(it.speed_text):'0B/s';"
            "if(state==='paused'){leftText='已暂停';speedText='';}"
            "else if(state==='queued'){leftText='排队中';speedText='';}"
            "else if(state==='failed'){leftText='下载失败';speedText='';}"
            "else if(state==='completed'){leftText='已完成';speedText='';}"
            "if(leftEl){leftEl.textContent=leftText;}"
            "if(speedEl){speedEl.textContent=speedText;}"
            "applyToggleAction(k,state);"
            "applyProgress(k,it);"
            "}"
            "}catch(e){}"
            "finally{window.__xunleiMetricsInflight=0;}"
            "};"
            "window.__xunleiMetricsPollNow=()=>{try{f();}catch(e){}};"
            "window.__xunleiMetricsLastRunAt=0;"
            "window.__xunleiMetricsInflight=0;"
            "bindActionButtons();"
            "f();"
            "window.__xunleiMetricsPollerTimer=setInterval(f,minGapMs);"
            "window.addEventListener('beforeunload',()=>{try{stopPoller();}catch(_e){}},{once:true});"
            "}catch(e){}"
            "})();"
        )

    def _task_dom_key(self, task: Dict[str, Any]) -> str:
        task_id = self._task_key(task)
        if task_id:
            key = f"id_{task_id}"
        else:
            key = self._task_move_key(task)
        key = str(key or "").strip()
        if not key:
            return ""
        return re.sub(r"[^a-zA-Z0-9_\\-]+", "_", key)[:120]

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
                logger.warn(f"迅雷接管[v{self.plugin_version}]回退内建下载器：不支持当前下载内容类型，未解析出磁力链接。")
                return None
            return "xunlei", None, None, "迅雷接管失败：仅支持磁力链接。"
        task_id, err = self._add_task(magnet)
        if not task_id:
            if self._fallback_to_builtin:
                logger.warn(f"迅雷接管[v{self.plugin_version}]回退内建下载器：{err or '迅雷添加任务失败'}")
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

        hash_set = self._normalize_hashs(hashs)
        if status == TorrentStatus.TRANSFER:
            tasks = self._list_download_tasks(include_runner=False, phase_mode="completed", purpose="external")
        else:
            tasks = self._list_download_tasks(include_runner=False, phase_mode="active", purpose="external")
        if not tasks:
            return []

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
        tasks = self._list_download_tasks(include_runner=True, phase_mode="active", purpose="external")
        dl_speed = 0.0
        up_speed = 0.0
        for task in tasks:
            dl_speed += float(self._task_speed_number(task, key="download_speed") or 0)
            up_speed += float(self._task_speed_number(task, key="upload_speed") or 0)
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
            logger.warn(f"保存已搬运任务键失败：{err}")

    def _load_completed_seen_cache(self) -> None:
        self._completed_seen_at = {}
        self._completed_seen_order = []
        self._completed_seen_name = {}
        self._completed_seen_fail_count = {}
        self._completed_seen_next_try_at = {}
        try:
            payload = self.get_data("completed_seen_cache")
        except Exception:
            payload = None
        items = payload if isinstance(payload, list) else []
        now_ts = time.time()
        for item in items:
            if not isinstance(item, dict):
                continue
            token = str(item.get("key") or "").strip()
            if not token or token in self._moved_task_keys:
                continue
            ts = self._parse_unix_timestamp(item.get("ts"))
            if ts is None:
                continue
            if now_ts - ts > float(self._completed_seen_ttl_seconds):
                continue
            if token in self._completed_seen_at:
                continue
            self._completed_seen_order.append(token)
            self._completed_seen_at[token] = float(ts)
            name = Path(str(item.get("name") or "").strip()).name
            self._completed_seen_name[token] = name
            try:
                fail_count = int(item.get("fail") or 0)
            except Exception:
                fail_count = 0
            self._completed_seen_fail_count[token] = max(0, fail_count)
            next_try = self._parse_unix_timestamp(item.get("next_try_at"))
            self._completed_seen_next_try_at[token] = float(next_try or 0.0)
        if len(self._completed_seen_order) > self._max_completed_seen_keys:
            overflow = self._completed_seen_order[:-self._max_completed_seen_keys]
            self._completed_seen_order = self._completed_seen_order[-self._max_completed_seen_keys:]
            for key in overflow:
                self._completed_seen_at.pop(key, None)
                self._completed_seen_name.pop(key, None)
                self._completed_seen_fail_count.pop(key, None)
                self._completed_seen_next_try_at.pop(key, None)

    def _save_completed_seen_cache(self) -> None:
        try:
            items: List[Dict[str, Any]] = []
            for key in self._completed_seen_order[-self._max_completed_seen_keys:]:
                ts = self._completed_seen_at.get(key)
                if ts is None:
                    continue
                items.append({
                    "key": key,
                    "ts": float(ts),
                    "name": str(self._completed_seen_name.get(key) or ""),
                    "fail": int(self._completed_seen_fail_count.get(key) or 0),
                    "next_try_at": float(self._completed_seen_next_try_at.get(key) or 0.0),
                })
            self.save_data("completed_seen_cache", items)
        except Exception as err:
            logger.warn(f"保存已完成缓存失败：{err}")

    def _remember_completed_seen(self, move_key: str, now_ts: float, task_name: str = "") -> float:
        token = str(move_key or "").strip()
        if not token:
            return float(now_ts)
        ts = float(now_ts)
        changed = False
        old = self._completed_seen_at.get(token)
        if old is not None:
            try:
                old_ts = float(old)
                if old_ts > 0:
                    ts = min(ts, old_ts)
            except Exception:
                pass
        if old is None:
            self._completed_seen_order.append(token)
            changed = True
        if self._completed_seen_at.get(token) != ts:
            self._completed_seen_at[token] = ts
            changed = True
        name = Path(str(task_name or "").strip()).name
        if name and name != "-":
            if str(self._completed_seen_name.get(token) or "") != name:
                self._completed_seen_name[token] = name
                changed = True
        elif token not in self._completed_seen_name:
            self._completed_seen_name[token] = ""
            changed = True
        if int(self._completed_seen_fail_count.get(token) or 0) != 0:
            self._completed_seen_fail_count[token] = 0
            changed = True
        if float(self._completed_seen_next_try_at.get(token) or 0.0) != 0.0:
            self._completed_seen_next_try_at[token] = 0.0
            changed = True
        if len(self._completed_seen_order) > self._max_completed_seen_keys:
            overflow = self._completed_seen_order[:-self._max_completed_seen_keys]
            self._completed_seen_order = self._completed_seen_order[-self._max_completed_seen_keys:]
            for key in overflow:
                self._completed_seen_at.pop(key, None)
                self._completed_seen_name.pop(key, None)
                self._completed_seen_fail_count.pop(key, None)
                self._completed_seen_next_try_at.pop(key, None)
            changed = True
        if changed:
            self._save_completed_seen_cache()
        return ts

    def _drop_completed_seen(self, move_key: str) -> None:
        token = str(move_key or "").strip()
        if not token:
            return
        changed = False
        if token in self._completed_seen_at:
            self._completed_seen_at.pop(token, None)
            changed = True
        if token in self._completed_seen_name:
            self._completed_seen_name.pop(token, None)
            changed = True
        if token in self._completed_seen_fail_count:
            self._completed_seen_fail_count.pop(token, None)
            changed = True
        if token in self._completed_seen_next_try_at:
            self._completed_seen_next_try_at.pop(token, None)
            changed = True
        if token in self._completed_seen_order:
            self._completed_seen_order = [x for x in self._completed_seen_order if x != token]
            changed = True
        if changed:
            self._save_completed_seen_cache()

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
            f"迅雷自动搬运调度已启动：间隔={self._move_interval_minutes}分钟，"
            f"源目录={self._source_download_dir}，目标目录={self._target_watch_dir}"
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
                f"迅雷请求失败[v{self.plugin_version}]：{method.upper()} {url} -> {self._last_request_error}"
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
            logger.warn(f"获取 pan_auth 失败：{err}")
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
            logger.warn(f"获取 device_id 失败[v{self.plugin_version}]：{err}")

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
                        f"添加任务触发次数限制[v{self.plugin_version}]：device={device_id}，{self._last_request_error}"
                    )
                    return "迅雷任务创建失败：任务创建次数达到上限，请稍后重试。", False
                if "space_name_invalid" in merged:
                    logger.warn(
                        f"添加任务空间无效[v{self.plugin_version}]：device={device_id}，{self._last_request_error}"
                    )
                    return "迅雷任务创建失败：device_id 对应空间无效，请重新抓取参数。", False
                if "device_space_not_active" in merged:
                    logger.warn(
                        f"添加任务空间未激活[v{self.plugin_version}]：device={device_id}，{self._last_request_error}"
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
                f"提交任务刷新 device_id[v{self.plugin_version}]：{first_device or 'EMPTY'} -> {refresh_device}"
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
            logger.warn(f"解析磁力文件列表失败：{err}")
        return result

    def _move_completed_downloads(self):
        if not self._enabled or not self._move_enabled:
            return
        if not self._source_download_dir or not self._target_watch_dir:
            logger.warn(
                f"跳过自动搬运：源/目标目录未配置，"
                f"源={self._source_download_dir or 'EMPTY'}，目标={self._target_watch_dir or 'EMPTY'}"
            )
            return
        if not self._move_lock.acquire(blocking=False):
            logger.info("跳过自动搬运：上一轮任务仍在执行。")
            return
        try:
            source_root = Path(self._source_download_dir)
            target_root = Path(self._target_watch_dir)
            if not source_root.exists() or not source_root.is_dir():
                logger.warn(
                    f"跳过自动搬运：源目录无效，"
                    f"源={source_root}，exists={source_root.exists()}，is_dir={source_root.is_dir()}"
                )
                return
            target_root.mkdir(parents=True, exist_ok=True)
            tasks = self._list_download_tasks(include_runner=False, phase_mode="completed", purpose="move")
            now_ts = time.time()
            stats = {
                "moved": 0,
                "skip_not_completed": 0,
                "skip_no_move_key": 0,
                "skip_already_moved": 0,
                "skip_source_not_found": 0,
                "skip_target_unresolved": 0,
                "skip_safe_wait": 0,
                "skip_cached_missing_name": 0,
                "skip_cached_backoff": 0,
                "skip_cached_expired": 0,
                "move_failed": 0,
            }
            samples: List[str] = []
            processed_keys: Set[str] = set()
            cached_total = len(self._completed_seen_at)
            cache_dirty = False

            def add_sample(text: str) -> None:
                if len(samples) < 6:
                    samples.append(text)

            def try_move_by_name(move_key: str, task_name: str, task_id: str, task_tag: str, from_cache: bool) -> None:
                nonlocal cache_dirty
                src = self._resolve_source_path(source_root, task_name)
                if not src or not src.exists():
                    src = self._resolve_movie_renamed_source_path(source_root=source_root, task_id=task_id)
                if not src or not src.exists():
                    src = self._resolve_source_path_fallback(source_root, task_name)
                if not src or not src.exists():
                    stats["skip_source_not_found"] += 1
                    flag = "source_not_found"
                    if from_cache:
                        fail_count = int(self._completed_seen_fail_count.get(move_key) or 0) + 1
                        self._completed_seen_fail_count[move_key] = fail_count
                        backoff_seconds = max(60, min(900, max(1, int(self._move_interval_minutes)) * 60 * 3))
                        self._completed_seen_next_try_at[move_key] = now_ts + backoff_seconds
                        cache_dirty = True
                        if fail_count >= int(self._completed_seen_max_missing):
                            self._drop_completed_seen(move_key)
                            flag = "缓存源不存在_达到上限已丢弃"
                        else:
                            flag = "缓存源不存在"
                    add_sample(
                        f"{task_tag} 跳过：{flag}，源目录={source_root}，任务名={task_name}"
                    )
                    return
                try:
                    src = self._rename_movie_path_if_needed(src=src, task_id=task_id, task_name=task_name)
                    if not src or not src.exists():
                        stats["move_failed"] += 1
                        add_sample(f"{task_tag} skip: rename failed, task_id={task_id}")
                        return
                    dst = self._build_move_target_path(
                        target_root=target_root,
                        src=src,
                        task_id=task_id,
                        task_name=task_name,
                    )
                    if not dst:
                        stats["skip_target_unresolved"] += 1
                        add_sample(
                            f"{task_tag} 跳过：目标目录解析失败，task_id={task_id}"
                        )
                        return
                    shutil.move(str(src), str(dst))
                    self._remember_moved_key(move_key)
                    self._drop_completed_seen(move_key)
                    if task_id and task_id != "-":
                        # 兼容历史 moved key（曾使用纯 task_id）
                        self._remember_moved_key(task_id)
                    stats["moved"] += 1
                    if from_cache:
                        logger.info(f"自动搬运成功(缓存)：{src} -> {dst}")
                    else:
                        logger.info(f"自动搬运成功：{src} -> {dst}")
                except Exception as move_err:
                    stats["move_failed"] += 1
                    logger.warn(
                        f"单任务搬运失败：key={move_key}，"
                        f"name={task_name}，err={move_err}"
                    )

            if not tasks:
                logger.info(
                    f"自动搬运扫描：当前无任务，源={source_root}，目标={target_root}，"
                    f"缓存已完成数={cached_total}"
                )

            for task in tasks:
                task_id = self._task_key(task) or "-"
                task_name = Path(str(self._task_name(task) or "")).name
                task_status = ",".join(self._task_status_values(task)) or "-"
                task_progress = self._task_progress(task)
                task_tag = f"id={task_id},name={task_name or '-'}"
                move_key = self._task_move_key(task)
                if not move_key:
                    stats["skip_no_move_key"] += 1
                    add_sample(f"{task_tag} 跳过：缺少搬运键，状态={task_status}")
                    continue
                if not self._is_task_completed(task):
                    stats["skip_not_completed"] += 1
                    self._drop_completed_seen(move_key)
                    add_sample(f"{task_tag} 跳过：未完成，状态={task_status}，进度={task_progress:.2f}")
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
                            f"{task_tag} 跳过：安全等待中，完成后已过{elapsed:.1f}s < {self._move_safe_seconds}s"
                        )
                        continue
                task_name_for_move = task_name or Path(str(self._completed_seen_name.get(move_key) or "").strip()).name
                if not task_name_for_move:
                    stats["skip_cached_missing_name"] += 1
                    add_sample(f"{task_tag} 跳过：缺少任务名，key={move_key}")
                    continue
                try_move_by_name(
                    move_key=move_key,
                    task_name=task_name_for_move,
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
                if now_ts - done_ts > float(self._completed_seen_ttl_seconds):
                    stats["skip_cached_expired"] += 1
                    self._drop_completed_seen(move_key)
                    continue
                next_try_at = float(self._completed_seen_next_try_at.get(move_key) or 0.0)
                if next_try_at > now_ts:
                    stats["skip_cached_backoff"] += 1
                    continue
                task_name = Path(str(self._completed_seen_name.get(move_key) or "").strip()).name
                task_tag = f"id=-,name={task_name or '-'}"
                if not task_name:
                    stats["skip_cached_missing_name"] += 1
                    add_sample(f"{task_tag} 跳过：缓存缺少任务名，key={move_key}")
                    continue
                if self._move_safe_seconds > 0:
                    elapsed = now_ts - done_ts
                    if elapsed < self._move_safe_seconds:
                        stats["skip_safe_wait"] += 1
                        add_sample(
                            f"{task_tag} 跳过：缓存安全等待中，完成后已过{elapsed:.1f}s < {self._move_safe_seconds}s"
                        )
                        continue
                cached_task_id = "-"
                if isinstance(move_key, str) and move_key.startswith("id:"):
                    cached_task_id = str(move_key.split(":", 1)[1] or "-").strip() or "-"
                try_move_by_name(
                    move_key=move_key,
                    task_name=task_name,
                    task_id=cached_task_id,
                    task_tag=task_tag,
                    from_cache=True,
                )
            if cache_dirty:
                self._save_completed_seen_cache()
            logger.info(
                f"自动搬运扫描汇总：源={source_root}，目标={target_root}，"
                f"总任务={len(tasks)}，缓存已完成={cached_total}，成功搬运={stats['moved']}，"
                f"未完成跳过={stats['skip_not_completed']}，"
                f"无搬运键跳过={stats['skip_no_move_key']}，已搬运跳过={stats['skip_already_moved']}，"
                f"源不存在跳过={stats['skip_source_not_found']}，"
                f"目标未解析跳过={stats['skip_target_unresolved']}，"
                f"安全等待跳过={stats['skip_safe_wait']}，"
                f"缓存缺名跳过={stats['skip_cached_missing_name']}，"
                f"缓存退避跳过={stats['skip_cached_backoff']}，缓存过期跳过={stats['skip_cached_expired']}，"
                f"搬运失败={stats['move_failed']}"
            )
            if samples:
                logger.info("自动搬运扫描样本： " + " | ".join(samples))
        except Exception as err:
            logger.error(f"自动搬运任务执行失败：{err}")
        finally:
            self._move_lock.release()

    def _list_download_tasks(self, include_runner: bool = False, phase_mode: str = "active",
                             purpose: str = "external") -> List[Dict[str, Any]]:
        if not self._enabled:
            return []
        purpose_token = str(purpose or "external").strip().lower()
        if purpose_token not in ("ui", "move", "control", "external"):
            purpose_token = "external"
        ui_mode = (purpose_token == "ui")
        if purpose_token not in ("ui", "move", "control") and not self._is_ui_active():
            return []
        mode = str(phase_mode or "active").strip().lower()
        if mode not in ("active", "completed", "all"):
            mode = "active"
        cache_key = ("runner" if include_runner else "download") + f":{mode}:{purpose_token}"
        now_ts = time.time()
        cache_obj = self._task_list_cache.get(cache_key) if isinstance(self._task_list_cache, dict) else None
        if isinstance(cache_obj, dict):
            try:
                ts = float(cache_obj.get("ts") or 0.0)
            except Exception:
                ts = 0.0
            cache_ttl = float(self._task_list_cache_ttl_ui_seconds if ui_mode else self._task_list_cache_ttl_seconds)
            if ts > 0 and (now_ts - ts) < cache_ttl:
                cached_tasks = cache_obj.get("tasks")
                if isinstance(cached_tasks, list):
                    return [x for x in cached_tasks if isinstance(x, dict)]
        try:
            headers = self._get_headers()
            # UI 场景不做主动探测，避免页面加载被长超时请求阻塞。
            if ui_mode:
                device_id = str(self._device_id or "").strip()
            else:
                device_id = str(self._fetch_device_id() or self._device_id or "").strip()
            req_timeout = 4 if ui_mode else 20
            req_retry_count = 0 if ui_mode else 2
            req_retry_auth = False if ui_mode else True

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

            spaces: List[str] = []
            if device_id:
                spaces.append(device_id)
            else:
                logger.info(
                    f"跳过任务请求[v{self.plugin_version}]：未获取到 device_id，不再使用 EMPTY 空间探测。"
                )
                self._task_list_cache[cache_key] = {"ts": now_ts, "tasks": []}
                return []

            active_phases = "PHASE_TYPE_PENDING,PHASE_TYPE_RUNNING,PHASE_TYPE_PAUSED,PHASE_TYPE_ERROR"
            complete_phase = "PHASE_TYPE_COMPLETE"
            if mode == "completed":
                phase_in = complete_phase
            elif mode == "all":
                phase_in = f"{active_phases},{complete_phase}"
            else:
                phase_in = active_phases
            type_in = "user#download-url,user#download"
            if include_runner:
                type_in = f"user#runner,{type_in}"
            probe_name = f"{'runner+' if include_runner else ''}download_{mode}"
            filter_obj: Dict[str, Any] = {
                "phase": {"in": phase_in},
                "type": {"in": type_in},
            }

            merged_tasks: Dict[str, Dict[str, Any]] = {}
            merged_scores: Dict[str, int] = {}
            probe_stats: List[str] = []

            def _task_merge_key(task: Dict[str, Any]) -> str:
                task_id = self._task_key(task)
                if task_id:
                    return f"id:{task_id}"
                name = Path(str(self._task_name(task) or task.get("name") or task.get("title") or "")).name
                phase = str(task.get("phase") or "").strip().lower()
                progress = str(task.get("progress") or "").strip()
                return f"name:{name}|phase:{phase}|progress:{progress}"

            def _source_score(task: Dict[str, Any], probe_name: str) -> int:
                score = 0
                if probe_name.startswith("runner"):
                    score += 200
                completed = self._is_task_completed(task)
                if completed:
                    score += 120
                else:
                    score += 10
                if probe_name.endswith("completed"):
                    score += 40
                speed_value = float(self._task_speed_number(task=task, key="download_speed") or 0)
                if speed_value > 0 and not completed:
                    score += 30
                score += int(float(self._task_progress(task) or 0) / 25)
                return score

            last_err = ""
            pan_auth = str(self._pan_auth or headers.get("pan-auth") or "").strip()
            for space in spaces:
                filters_text = json.dumps(filter_obj, ensure_ascii=False, separators=(",", ":"))
                query = [
                    f"space={quote(space) if space else ''}",
                    "page_token=",
                    f"filters={quote(filters_text)}",
                    "limit=100",
                    "device_space=",
                ]
                if pan_auth:
                    query.append(f"pan_auth={quote(pan_auth)}")
                url = (
                    f"{self._base_url}/webman/3rdparty/pan-xunlei-com/index.cgi/drive/v1/tasks"
                    f"?{'&'.join(query)}"
                )
                resp, obj = self._request_json(
                    method="GET",
                    url=url,
                    headers={**headers, "device-space": ""},
                    timeout=req_timeout,
                    retry_auth=req_retry_auth,
                    retry_count=req_retry_count
                )
                if not resp or not resp.ok:
                    last_err = f"http={resp.status_code if resp else 'request-failed'} {self._last_request_error}"
                    inactive_space = bool(space) and self._is_device_space_not_active(
                        obj=obj, error_text=self._last_request_error
                    )
                    if inactive_space:
                        refreshed = self._refresh_device_id_on_inactive_space(
                            obj=obj, error_text=self._last_request_error
                        )
                        if refreshed:
                            new_space = str(self._device_id or "").strip()
                            if new_space and new_space not in spaces:
                                spaces.append(new_space)
                        logger.info(
                            f"检测到空间未激活，停止该空间后续请求："
                            f"space={space or 'EMPTY'}，probe={probe_name}，{last_err}"
                        )
                        continue
                    logger.warn(
                        f"拉取任务失败[v{self.plugin_version}]："
                        f"space={space or 'EMPTY'}，probe={probe_name}，{last_err}"
                    )
                    continue
                tasks = _extract_tasks(obj)
                logger.debug(
                    f"拉取任务结果[v{self.plugin_version}]："
                    f"space={space or 'EMPTY'}，probe={probe_name}，数量={len(tasks)}"
                )
                if not tasks:
                    continue
                probe_stats.append(f"{space or 'EMPTY'}:{probe_name}={len(tasks)}")
                for task in tasks:
                    key = _task_merge_key(task)
                    if not key:
                        continue
                    new_score = _source_score(task=task, probe_name=probe_name)
                    old_score = merged_scores.get(key, -1)
                    if key not in merged_tasks or new_score >= old_score:
                        merged_tasks[key] = task
                        merged_scores[key] = new_score
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
                merged_list = list(merged_tasks.values())
                self._task_list_cache[cache_key] = {"ts": now_ts, "tasks": merged_list}
                logger.info(
                    f"任务合并结果[v{self.plugin_version}]："
                    f"总数={len(merged_tasks)}，命中探针={'; '.join(probe_stats[:8])}"
                )
                return merged_list
            if last_err:
                logger.info(
                    f"任务列表为空（已遍历全部空间）[v{self.plugin_version}]："
                    f"device_id={device_id or 'EMPTY'}，最后错误={last_err}"
                )
            else:
                logger.info(
                    f"任务列表为空[v{self.plugin_version}]：device_id={device_id or 'EMPTY'}"
                )
            self._task_list_cache[cache_key] = {"ts": now_ts, "tasks": []}
        except Exception as err:
            logger.warn(f"拉取任务列表异常[v{self.plugin_version}]：{err}")
            self._task_list_cache[cache_key] = {"ts": now_ts, "tasks": []}
        return []

    def _operate_tasks(self, ids: Set[str], action: str, delete_file: bool = True,
                       preferred_space: str = "", preferred_type: str = "") -> bool:
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
        preferred_type = str(preferred_type or "").strip()
        pan_auth = str(self._pan_auth or headers.get("pan-auth") or "").strip()

        first_id = id_list[0]
        action_token = str(action or "").strip().lower()
        single_phase = ""
        if action_token in ("start", "resume", "continue", "unpause"):
            single_phase = "running"
        elif action_token in ("pause", "stop", "suspend"):
            single_phase = "pause"
        elif action_token in ("delete", "remove"):
            single_phase = "delete"
        if single_phase:
            device_space = str(preferred_space or self._device_id or "").strip()
            task_type = str(preferred_type or "user#download-url").strip()
            spec_text = json.dumps({"phase": single_phase}, ensure_ascii=False, separators=(",", ":"))
            query_parts = []
            if pan_auth:
                query_parts.append(f"pan_auth={quote(pan_auth)}")
            query_parts.append("device_space=")
            url = (
                f"{self._base_url}/webman/3rdparty/pan-xunlei-com/index.cgi/method/patch/drive/v1/task"
                f"?{'&'.join(query_parts)}"
            )
            payload: Dict[str, Any] = {
                "id": first_id,
                "set_params": {"spec": spec_text},
                "spec": spec_text,
            }
            if device_space:
                payload["space"] = device_space
            if task_type:
                payload["type"] = task_type
            resp, obj = self._request_json(
                method="POST",
                url=url,
                headers={**headers, "device-space": ""},
                payload=payload,
                timeout=8,
                retry_auth=False,
                retry_count=0,
            )
            if resp and resp.ok:
                if self._is_http_status_zero(obj) or self._is_operation_success(obj=obj, ids={first_id}, resp=resp):
                    return True
                if isinstance(obj, dict) and not obj.get("error") and not obj.get("err"):
                    return True
            detail = str(self._extract_api_error(obj) or "").strip()
            if detail:
                self._last_request_error = detail
            if not self._last_request_error:
                self._last_request_error = f"action={action} 单次请求未成功"
            if action_token != "delete":
                return False

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
        if preferred_type:
            payload_templates.append({"action": action, "id": first_id, "type": preferred_type})
            payload_templates.append({"action": action, "task_id": first_id, "type": preferred_type})
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
            verify_budget = 2
            query_texts: List[str] = []
            base_query = []
            if pan_auth:
                base_query.append(f"pan_auth={quote(pan_auth)}")
            query_texts.append("&".join(base_query + ["device_space="]))
            if device_space:
                query_texts.append("&".join(base_query + [f"device_space={quote(device_space)}"]))
            query_texts = [x for i, x in enumerate(query_texts) if x and x not in query_texts[:i]]
            wrapper_update_urls: List[str] = []
            wrapper_action_urls: List[str] = []
            for query_text in query_texts:
                wrapper_update_urls.extend([
                    f"{self._base_url}/webman/3rdparty/pan-xunlei-com/index.cgi/method/patch/drive/v1/task?{query_text}",
                    f"{self._base_url}/webman/3rdparty/pan-xunlei-com/index.cgi/method/patch/drive/v1/tasks?{query_text}",
                ])
                wrapper_action_urls.extend([
                    f"{self._base_url}/webman/3rdparty/pan-xunlei-com/index.cgi/method/post/drive/v1/task/action?{query_text}",
                    f"{self._base_url}/webman/3rdparty/pan-xunlei-com/index.cgi/method/post/drive/v1/tasks/action?{query_text}",
                ])
            phase_candidates = self._phase_candidates_for_action(action=action)
            phase_spec_candidates = self._phase_spec_candidates_for_action(action=action)
            if phase_candidates:
                update_urls = [
                    *wrapper_update_urls,
                    f"{self._base_url}/webman/3rdparty/pan-xunlei-com/index.cgi/drive/v1/task",
                    f"{self._base_url}/webman/3rdparty/pan-xunlei-com/index.cgi/drive/v1/tasks",
                ]
                update_methods = ["POST", "PATCH", "PUT"]
                phase_payloads: List[Dict[str, Any]] = []
                type_candidates: List[str] = []
                if preferred_type:
                    type_candidates.append(preferred_type)
                type_candidates.extend(["user#download-url", "user#download", "user#runner"])
                type_candidates = [x for i, x in enumerate(type_candidates) if x and x not in type_candidates[:i]]
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
                        phase_payloads.append({**base, "space": device_space, "target": device_space})
                        phase_payloads.append({**base, "space": device_space, "device_space": device_space})
                        phase_payloads.append({**base, "target": device_space, "device_space": device_space})
                        for task_type in type_candidates:
                            phase_payloads.append({**base, "space": device_space, "type": task_type})
                for phase_spec in phase_spec_candidates:
                    spec_text = json.dumps({"phase": phase_spec}, ensure_ascii=False, separators=(",", ":"))
                    per_spec = [
                        {"id": first_id, "set_params": {"spec": spec_text}, "spec": spec_text},
                        {"task_id": first_id, "set_params": {"spec": spec_text}, "spec": spec_text},
                    ]
                    for base in per_spec:
                        phase_payloads.append(dict(base))
                        phase_payloads.append({**base, "space": device_space})
                        phase_payloads.append({**base, "space": device_space, "target": device_space})
                        phase_payloads.append({**base, "space": device_space, "device_space": device_space})
                        for task_type in type_candidates:
                            phase_payloads.append({**base, "space": device_space, "type": task_type})
                if phase_payloads:
                    uniq_payloads: List[Dict[str, Any]] = []
                    seen_payload_keys: Set[str] = set()
                    for payload in phase_payloads:
                        try:
                            key = json.dumps(payload, ensure_ascii=False, sort_keys=True)
                        except Exception:
                            key = str(payload)
                        if key in seen_payload_keys:
                            continue
                        seen_payload_keys.add(key)
                        uniq_payloads.append(payload)
                    phase_payloads = uniq_payloads
                for method in update_methods:
                    for url in update_urls:
                        if "/method/patch/" in url and method not in ("POST", "PATCH"):
                            continue
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
                                    op_success = self._is_operation_success(obj=obj, ids=set(id_list), resp=resp)
                                    http_status_zero = self._is_http_status_zero(obj=obj)
                                    if op_success or http_status_zero:
                                        if self._action_needs_state_verify(action=action):
                                            if verify_budget > 0:
                                                verify_budget -= 1
                                                if self._verify_task_action_effect(task_id=first_id, action=action):
                                                    return True, local_hints
                                            local_hints.append("控制回执成功但任务状态未变化，继续尝试其它控制报文")
                                            continue
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
            action_urls = [*wrapper_action_urls, *urls]
            for method in methods:
                for url in action_urls:
                    if "/method/post/" in url and method != "POST":
                        continue
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
                                op_success = self._is_operation_success(obj=obj, ids=set(id_list), resp=resp)
                                http_status_zero = self._is_http_status_zero(obj=obj)
                                if op_success or http_status_zero:
                                    if self._action_needs_state_verify(action=action):
                                        if verify_budget > 0:
                                            verify_budget -= 1
                                            if self._verify_task_action_effect(task_id=first_id, action=action):
                                                return True, local_hints
                                        local_hints.append("控制回执成功但任务状态未变化，继续尝试其它控制报文")
                                        continue
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
            return ["phase_type_running", "PHASE_TYPE_RUNNING", "running", "RUNNING", "start", "START"]
        if token in ("pause", "stop", "suspend"):
            return ["phase_type_paused", "PHASE_TYPE_PAUSED", "pause", "PAUSE", "paused", "PAUSED"]
        if token in ("delete", "remove"):
            return ["phase_type_delete", "PHASE_TYPE_DELETE", "delete", "DELETE"]
        return []

    @staticmethod
    def _phase_spec_candidates_for_action(action: str) -> List[str]:
        token = str(action or "").strip().lower()
        if token in ("start", "resume", "continue", "unpause"):
            return ["running", "RUNNING", "start", "START"]
        if token in ("pause", "stop", "suspend"):
            return ["pause", "PAUSE", "paused", "PAUSED"]
        if token in ("delete", "remove"):
            return ["delete", "DELETE"]
        return []

    @staticmethod
    def _action_needs_state_verify(action: str) -> bool:
        token = str(action or "").strip().lower()
        return token in ("start", "resume", "continue", "unpause", "pause", "stop", "suspend")

    @staticmethod
    def _is_http_status_zero(obj: Any) -> bool:
        if not isinstance(obj, dict):
            return False
        value = obj.get("HttpStatus")
        if value is None:
            return False
        try:
            return int(value) == 0
        except Exception:
            return False

    def _verify_task_action_effect(self, task_id: str, action: str, timeout_seconds: float = 1.8) -> bool:
        key = str(task_id or "").strip()
        if not key:
            return False
        if not self._action_needs_state_verify(action=action):
            return True
        deadline = time.time() + max(0.6, float(timeout_seconds or 1.8))
        while time.time() <= deadline:
            self._task_list_cache = {}
            tasks = self._list_download_tasks(include_runner=True, phase_mode="all", purpose="control")
            target = None
            for task in tasks:
                if self._task_key(task) == key:
                    target = task
                    break
            if target is not None:
                action_token = str(action or "").strip().lower()
                if action_token in ("pause", "stop", "suspend"):
                    if self._is_task_paused(target):
                        return True
                elif action_token in ("start", "resume", "continue", "unpause"):
                    if not self._is_task_paused(target) and not self._is_task_failed(target):
                        return True
                else:
                    return True
            if time.time() >= deadline:
                break
            time.sleep(0.35)
        return False

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
            # 兼容中文完成态文本
            if ("下载完成" in text or "已完成" in text or "任务完成" in text or "完成下载" in text) and "未完成" not in text:
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
        for key in ("phase", "status", "state", "phase_name", "status_text", "state_text", "message"):
            value = task.get(key)
            if value is not None:
                values.append(str(value).strip().lower())
        params = task.get("params")
        if isinstance(params, dict):
            for key in ("phase", "status", "state", "phase_name", "status_text", "state_text", "message"):
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
        value = self._task_speed_number(task=task, key=key)
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

    def _task_speed_number(self, task: Dict[str, Any], key: str) -> Optional[float]:
        speed_keys = [
            key,
            "download_speed" if key != "upload_speed" else "upload_speed",
            "speed",
            "dl_speed" if key != "upload_speed" else "up_speed",
            "current_speed",
            "downloadspeed" if key != "upload_speed" else "uploadspeed",
            "speed_download" if key != "upload_speed" else "speed_upload",
            "download_speed_text" if key != "upload_speed" else "upload_speed_text",
            "speed_text",
        ]
        values = self._task_lookup_values(task=task, keys=speed_keys)
        if not values:
            return None
        parsed: List[float] = []
        for value in values:
            speed_num = self._parse_speed_value(value)
            if speed_num is None:
                continue
            parsed.append(float(speed_num))
        if not parsed:
            return None
        for value in reversed(parsed):
            if value > 0:
                return value
        return parsed[-1]

    @staticmethod
    def _parse_speed_value(value: Any) -> Optional[float]:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            try:
                return float(value)
            except Exception:
                return None
        text = str(value or "").strip().replace(" ", "")
        if not text:
            return None
        matched = re.search(r"(-?\d+(?:\.\d+)?)\s*([kmgtep]?i?b|[kmgtep]?b|[kmgtep])?/s", text, flags=re.IGNORECASE)
        if matched:
            try:
                number = float(matched.group(1))
            except Exception:
                return None
            unit = str(matched.group(2) or "b").lower()
            unit = unit.replace("ib", "b")
            factor_map = {
                "b": 1,
                "k": 1024,
                "kb": 1024,
                "m": 1024 ** 2,
                "mb": 1024 ** 2,
                "g": 1024 ** 3,
                "gb": 1024 ** 3,
                "t": 1024 ** 4,
                "tb": 1024 ** 4,
                "p": 1024 ** 5,
                "pb": 1024 ** 5,
                "e": 1024 ** 6,
                "eb": 1024 ** 6,
            }
            factor = factor_map.get(unit)
            if factor is None:
                return number
            return number * factor
        number = XunleiHijackDownloader._coerce_float(text)
        if number is None:
            return None
        return float(number)

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

    def _task_metric_texts(self, task: Dict[str, Any], progress: float) -> Tuple[str, str]:
        state = self._task_progress_state(task)
        if state == "paused":
            return "已暂停", ""
        if state == "queued":
            return "排队中", ""
        if state == "failed":
            return "下载失败", ""
        if state == "completed":
            return "已完成", ""
        left_time = self._task_left_time(task, progress) or "--"
        speed_text = self._task_speed_text(task, key="download_speed") or "0B/s"
        return left_time, speed_text

    def _task_state_text(self, task: Dict[str, Any]) -> str:
        if self._is_task_completed(task):
            return "已完成"
        if self._is_task_failed(task):
            return "下载失败"
        if self._is_task_paused(task):
            return "已暂停"
        for text in self._task_status_values(task):
            if any(k in text for k in ("waiting", "wait", "pending", "queue", "排队", "等待")):
                return "排队中"
        return "下载中"

    @staticmethod
    def _phase_token_to_state(text: str) -> str:
        token = str(text or "").strip().lower()
        if not token:
            return ""
        if any(k in token for k in ("phase_type_complete", "phase_type_finished", "complete", "completed", "finished", "done", "success")):
            return "completed"
        if any(k in token for k in ("phase_type_error", "error", "failed", "fail", "invalid", "失败", "错误")):
            return "failed"
        if any(k in token for k in ("phase_type_paused", "pause", "paused", "suspend", "stopped", "halt", "暂停", "已暂停", "停止", "已停止")):
            return "paused"
        if any(k in token for k in ("phase_type_running", "running", "run", "resume", "resumed", "下载中", "进行中", "启动中")):
            return "downloading"
        if any(k in token for k in ("phase_type_pending", "pending", "wait", "waiting", "queue", "排队", "等待")):
            return "queued"
        return ""

    def _task_phase_state(self, task: Dict[str, Any]) -> str:
        # 第一优先级：迅雷 phase 字段
        phase_values: List[str] = []
        for key in ("phase", "phase_name"):
            value = task.get(key)
            if value is not None:
                phase_values.append(str(value).strip())
        params = task.get("params")
        if isinstance(params, dict):
            for key in ("phase", "phase_name"):
                value = params.get(key)
                if value is not None:
                    phase_values.append(str(value).strip())
        for text in phase_values:
            state = self._phase_token_to_state(text)
            if state:
                return state
        # 第二优先级：兼容 status/state 文本
        fallback_values: List[str] = []
        for key in ("status", "state"):
            value = task.get(key)
            if value is not None:
                fallback_values.append(str(value).strip())
        if isinstance(params, dict):
            for key in ("status", "state"):
                value = params.get(key)
                if value is not None:
                    fallback_values.append(str(value).strip())
        for text in fallback_values:
            state = self._phase_token_to_state(text)
            if state:
                return state
        return ""

    def _task_progress_state(self, task: Dict[str, Any]) -> str:
        phase_state = self._task_phase_state(task)
        if phase_state:
            return phase_state
        if self._is_task_completed(task):
            return "completed"
        if self._is_task_failed(task):
            return "failed"
        paused = self._is_task_paused(task)
        running = self._is_task_running(task)
        if paused and not running:
            return "paused"
        if running and not paused:
            return "downloading"
        if paused and running:
            speed = float(self._task_speed_number(task=task, key="download_speed") or 0)
            return "downloading" if speed > 0 else "paused"
        for text in self._task_status_values(task):
            if any(k in text for k in ("waiting", "wait", "pending", "queue", "排队", "等待")):
                return "queued"
        return "downloading"

    def _task_progress_color(self, task: Dict[str, Any]) -> str:
        state = self._task_progress_state(task)
        if state == "completed":
            return "success"
        if state == "paused":
            return "warning"
        if state == "failed":
            return "error"
        if state == "queued":
            return "secondary"
        return "primary"

    def _is_task_paused(self, task: Dict[str, Any]) -> bool:
        phase_state = self._task_phase_state(task)
        if phase_state == "paused":
            return True
        if phase_state == "downloading":
            return False
        paused_words = ("pause", "paused", "suspend", "stopped", "halt", "暂停", "已暂停", "停止", "已停止")
        running_words = ("running", "run", "resume", "resumed", "下载中", "进行中", "启动中")
        values = self._task_status_values(task)
        paused = any(any(k in text for k in paused_words) for text in values)
        if not paused:
            return False
        running = any(any(k in text for k in running_words) for text in values)
        if running:
            speed = float(self._task_speed_number(task=task, key="download_speed") or 0)
            return speed <= 0
        return True

    def _is_task_running(self, task: Dict[str, Any]) -> bool:
        phase_state = self._task_phase_state(task)
        if phase_state == "downloading":
            return True
        if phase_state == "paused":
            return False
        running_words = ("running", "run", "resume", "resumed", "下载中", "进行中", "启动中")
        paused_words = ("pause", "paused", "suspend", "stopped", "halt", "暂停", "已暂停", "停止", "已停止")
        values = self._task_status_values(task)
        running = any(any(k in text for k in running_words) for text in values)
        if not running:
            return False
        paused = any(any(k in text for k in paused_words) for text in values)
        if paused:
            speed = float(self._task_speed_number(task=task, key="download_speed") or 0)
            return speed > 0
        return True

    def _is_task_failed(self, task: Dict[str, Any]) -> bool:
        for text in self._task_status_values(task):
            if any(k in text for k in ("fail", "failed", "error", "invalid", "失败", "错误")):
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
        # 仅对已完成任务做已迁移过滤，避免下载中任务被历史 key 误判
        if not self._is_task_completed(task):
            return False
        move_key = self._task_move_key(task)
        if move_key and move_key in self._moved_task_keys:
            return True
        # 兼容历史 moved key（曾使用纯 task_id）
        task_key = self._task_key(task)
        if task_key and task_key in self._moved_task_keys:
            return True
        # 兼容“已搬移但未命中 moved_key 缓存”的场景：
        # 完成态任务若在源下载目录中已不存在，则视为已搬离并从插件列表隐藏。
        source_dir = str(self._source_download_dir or "").strip()
        task_name = Path(str(self._task_name(task) or "")).name
        if source_dir and task_name:
            try:
                source_root = Path(source_dir)
                if source_root.exists() and source_root.is_dir():
                    src = self._resolve_source_path(source_root, task_name)
                    if not src or not src.exists():
                        renamed_src = self._resolve_movie_renamed_source_path(
                            source_root=source_root,
                            task_id=task_key,
                        ) if task_key else None
                        if renamed_src and renamed_src.exists():
                            return False
                        return True
            except Exception:
                pass
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
    def _task_type(task: Dict[str, Any]) -> str:
        if not isinstance(task, dict):
            return ""
        for key in ("type", "task_type", "taskType"):
            value = task.get(key)
            if value:
                return str(value).strip()
        params = task.get("params")
        if isinstance(params, dict):
            for key in ("type", "task_type", "taskType"):
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

    def _resolve_movie_renamed_source_path(self, source_root: Path, task_id: str) -> Optional[Path]:
        if not source_root.exists() or not source_root.is_dir():
            return None
        token = str(task_id or "").strip()
        if not token or token == "-":
            return None
        meta = self._resolve_movie_rename_meta(task_id=token)
        if not bool(meta.get("is_movie")):
            return None
        movie_name = self._build_movie_scrape_name(
            title=str(meta.get("title") or "").strip(),
            year=str(meta.get("year") or "").strip(),
        )
        if not movie_name:
            return None
        movie_norm = self._normalize_name(movie_name)
        if not movie_norm:
            return None

        candidates: List[Tuple[int, float, Path]] = []
        try:
            scanned = 0
            for item in source_root.rglob("*"):
                scanned += 1
                if scanned > 5000:
                    break
                try:
                    name = item.name
                except Exception:
                    continue
                stem = item.stem if item.is_file() else name
                score = 0
                if name.lower() == movie_name.lower():
                    score = 100
                elif stem.lower() == movie_name.lower():
                    score = 95
                else:
                    item_norm = self._normalize_name(stem or name)
                    if item_norm == movie_norm:
                        score = 90
                if score <= 0:
                    continue
                mtime = 0.0
                try:
                    mtime = float(item.stat().st_mtime or 0.0)
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
                f"检测到 device_space 未激活[v{self.plugin_version}]，"
                f"已刷新 device_id：{old_device or 'EMPTY'} -> {new_device}"
            )
            return True
        logger.warn(
            f"检测到 device_space 未激活[v{self.plugin_version}]，"
            f"但刷新 device_id 失败。"
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
        tasks = self._list_download_tasks(include_runner=False, phase_mode="active", purpose="external")
        if not tasks:
            return None
        task_ids = {self._task_key(x) for x in tasks if isinstance(x, dict) and self._task_key(x)}
        if not task_ids:
            return None
        return len(ids.intersection(task_ids)) > 0

    def _build_move_target_path(self, target_root: Path, src: Path, task_id: str, task_name: str) -> Optional[Path]:
        """
        根据 MoviePilot 目录配置决定迅雷搬运目标路径：
        - 仅复用 app/chain/download.py 的下载目录拼接逻辑
        - 仅使用 MoviePilot 下载历史中的媒体类型/类别
        - 不做插件侧分析或兜底
        """
        try:
            history_dir = self._resolve_history_download_dir(task_id=task_id)
            if not history_dir:
                logger.warn(
                    f"跳过搬运：按 MoviePilot 规则无法解析目标目录，"
                    f"task_id={task_id or '-'}，task_name={task_name or '-'}"
                )
                return None
            base_dir = history_dir
        except Exception as err:
            logger.warn(f"构建搬运目标路径失败：task={task_name}，err={err}")
            return None
        base_dir.mkdir(parents=True, exist_ok=True)
        return self._dedupe_target(base_dir / src.name)

    def _resolve_history_download_dir(self, task_id: str) -> Optional[Path]:
        """
        复用 MoviePilot app/chain/download.py 的下载目录拼装逻辑：
        - DirectoryHelper().get_dir(media, include_unsorted=True)
        - download_type_folder / download_category_folder
        """
        token = str(task_id or "").strip()
        if not token or token == "-" or not DownloadHistoryOper or not DirectoryHelper:
            return None
        try:
            history = DownloadHistoryOper().get_by_hash(token)
            if not history:
                return None
            media_type = str(getattr(history, "type", "") or "").strip()
            media_category = str(getattr(history, "media_category", "") or "").strip()
            if not media_type:
                return None
            media = SimpleNamespace(
                type=SimpleNamespace(value=media_type),
                category=media_category,
            )
            dir_conf = DirectoryHelper().get_dir(media=media, include_unsorted=True)
            if not dir_conf:
                return None
            download_path = str(getattr(dir_conf, "download_path", "") or "").strip()
            if not download_path:
                return None
            download_dir = Path(download_path)
            if not getattr(dir_conf, "media_type", None) and bool(getattr(dir_conf, "download_type_folder", False)):
                download_dir = download_dir / media_type
            if (
                not getattr(dir_conf, "media_category", None)
                and bool(getattr(dir_conf, "download_category_folder", False))
                and media_category
            ):
                download_dir = download_dir / media_category
            return download_dir
        except Exception as err:
            logger.debug(f"按下载历史解析目录失败：task_id={task_id}，err={err}")
        return None

    def _rename_movie_path_if_needed(self, src: Path, task_id: str, task_name: str = "") -> Path:
        token = str(task_id or "").strip()
        if not token or token == "-" or not src or not src.exists():
            return src
        meta = self._resolve_movie_rename_meta(task_id=token)
        if not bool(meta.get("is_movie")):
            return src
        movie_name = self._build_movie_scrape_name(
            title=str(meta.get("title") or "").strip(),
            year=str(meta.get("year") or "").strip(),
        )
        if not movie_name:
            logger.warn(f"movie rename skipped: missing title, task_id={token}, task_name={task_name or '-'}")
            return src
        try:
            if src.is_file():
                return self._rename_movie_file(src=src, movie_name=movie_name, task_id=token)
            if src.is_dir():
                return self._rename_movie_dir(src_dir=src, movie_name=movie_name, task_id=token)
        except Exception as err:
            logger.warn(f"movie rename failed: task_id={token}, task_name={task_name or '-'}, err={err}")
        return src

    def _rename_movie_file(self, src: Path, movie_name: str, task_id: str) -> Path:
        suffix = str(src.suffix or "")
        desired_name = f"{movie_name}{suffix}" if suffix else movie_name
        return self._rename_path(
            src=src,
            desired_name=desired_name,
            task_id=task_id,
            rename_kind="movie_file",
        )

    def _rename_movie_dir(self, src_dir: Path, movie_name: str, task_id: str) -> Path:
        renamed_dir = self._rename_path(
            src=src_dir,
            desired_name=movie_name,
            task_id=task_id,
            rename_kind="movie_dir",
        )
        if not renamed_dir.exists() or not renamed_dir.is_dir():
            return renamed_dir
        main_video = self._pick_primary_video_file(root_dir=renamed_dir)
        if not main_video:
            return renamed_dir
        suffix = str(main_video.suffix or "")
        desired_name = f"{movie_name}{suffix}" if suffix else movie_name
        self._rename_path(
            src=main_video,
            desired_name=desired_name,
            task_id=task_id,
            rename_kind="movie_main_file",
        )
        return renamed_dir

    def _pick_primary_video_file(self, root_dir: Path) -> Optional[Path]:
        if not root_dir.exists() or not root_dir.is_dir():
            return None
        candidates: List[Tuple[int, int, Path]] = []
        for item in root_dir.rglob("*"):
            if not item.is_file():
                continue
            if item.suffix.lower() not in self._movie_video_suffixes:
                continue
            try:
                size = int(item.stat().st_size or 0)
            except Exception:
                size = 0
            try:
                depth = len(item.relative_to(root_dir).parts)
            except Exception:
                depth = 999
            candidates.append((size, -depth, item))
        if not candidates:
            return None
        candidates.sort(key=lambda x: (x[0], x[1]), reverse=True)
        return candidates[0][2]

    def _rename_path(self, src: Path, desired_name: str, task_id: str, rename_kind: str) -> Path:
        if not src or not src.exists():
            return src
        safe_name = self._sanitize_file_name(desired_name)
        if not safe_name:
            return src
        if safe_name.lower() == src.name.lower():
            return src
        target = src.with_name(safe_name)
        if target.exists():
            target = self._dedupe_target(target)
        try:
            src.rename(target)
            logger.info(f"movie rename: {rename_kind}, task_id={task_id}, {src.name} -> {target.name}")
            return target
        except Exception as err:
            logger.warn(
                f"movie rename failed: {rename_kind}, task_id={task_id}, src={src.name}, "
                f"target={target.name}, err={err}"
            )
            return src

    def _resolve_movie_rename_meta(self, task_id: str) -> Dict[str, Any]:
        meta: Dict[str, Any] = {
            "is_movie": False,
            "title": "",
            "year": "",
        }
        token = str(task_id or "").strip()
        if not token or token == "-" or not DownloadHistoryOper:
            return meta
        try:
            history = DownloadHistoryOper().get_by_hash(token)
            if not history:
                return meta
            media_type = str(getattr(history, "type", "") or "").strip()
            is_movie = self._is_movie_media_type(media_type=media_type)
            meta["is_movie"] = bool(is_movie)
            meta["title"] = self._extract_history_title(history=history)
            meta["year"] = self._extract_history_year(history=history)
            return meta
        except Exception as err:
            logger.debug(f"parse movie rename meta failed: task_id={task_id}, err={err}")
        return meta

    @staticmethod
    def _is_movie_media_type(media_type: str) -> bool:
        raw = str(media_type or "").strip()
        if not raw:
            return False
        if "\u7535\u5f71" in raw:
            return True
        token = re.sub(r"[\s._-]+", "", raw.lower())
        return any(k in token for k in ("movie", "film", "mv"))

    @staticmethod
    def _history_text_attr(history: Any, keys: Tuple[str, ...]) -> str:
        for key in keys:
            try:
                value = getattr(history, key, None)
            except Exception:
                value = None
            if value is None:
                continue
            text = str(value).strip()
            if text and text.lower() not in ("none", "null"):
                return text
        return ""

    def _extract_history_title(self, history: Any) -> str:
        raw = self._history_text_attr(
            history=history,
            keys=(
                "title", "name", "tmdb_name", "media_name", "movie_name",
                "cn_name", "original_title", "en_name", "display_title",
            ),
        )
        return self._sanitize_file_name(raw)

    def _extract_history_year(self, history: Any) -> str:
        raw = self._history_text_attr(
            history=history,
            keys=(
                "year", "release_year", "publish_year", "air_year",
                "release_date", "first_air_date", "pubdate", "date",
            ),
        )
        match = re.search(r"(19|20)\d{2}", str(raw or ""))
        return match.group(0) if match else ""

    def _build_movie_scrape_name(self, title: str, year: str) -> str:
        clean_title = self._sanitize_file_name(title)
        if not clean_title:
            return ""
        match = re.search(r"(19|20)\d{2}", str(year or ""))
        if match:
            return f"{clean_title}({match.group(0)})"
        return clean_title

    @staticmethod
    def _sanitize_file_name(name: str) -> str:
        text = str(name or "").strip()
        if not text:
            return ""
        text = re.sub(r"[\\/:*?\"<>|]+", " ", text)
        text = re.sub(r"\s+", " ", text).strip()
        return text.strip(" .")

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
