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
    plugin_version = "1.0.2"
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
    _task_name_cache: Dict[str, str] = {}

    def init_plugin(self, config: dict = None):
        self.stop_service()
        self._moved_task_keys = set()
        self._task_name_cache = {}
        if config:
            self._enabled = bool(config.get("enabled", False))
            self._hijack_download = bool(config.get("hijack_download", True))
            self._fallback_to_builtin = bool(config.get("fallback_to_builtin", True))
            self._base_url = self._normalize_base_url(config.get("base_url") or "")
            self._authorization = str(config.get("authorization") or "").strip()
            self._pan_auth = str(config.get("pan_auth") or "").strip()
            self._auto_refresh_pan_auth = bool(config.get("auto_refresh_pan_auth", True))
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
                                    {"component": "VTextField", "props": {"model": "base_url", "label": "迅雷地址", "placeholder": "http://192.168.2.3:2345"}}
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {"component": "VTextField", "props": {"model": "authorization", "label": "Authorization"}}
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
                                    {"component": "VTextField", "props": {"model": "pan_auth", "label": "pan_auth（可留空自动获取）"}}
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {"component": "VTextField", "props": {"model": "file_id", "label": "父目录 file_id"}}
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
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
                                    {"component": "VSwitch", "props": {"model": "auto_refresh_pan_auth", "label": "自动刷新 pan_auth"}}
                                ],
                            },
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
                                    {"component": "VTextField", "props": {"model": "source_download_dir", "label": "迅雷下载目录（源）", "placeholder": "/downloads_ssd"}}
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {"component": "VTextField", "props": {"model": "target_watch_dir", "label": "MoviePilot 监控目录（目标）", "placeholder": "/downloads_hdd/watch"}}
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
            "pan_auth": "",
            "auto_refresh_pan_auth": True,
            "file_id": "",
            "device_id": "",
            "move_enabled": False,
            "source_download_dir": "",
            "target_watch_dir": "",
            "move_interval_minutes": 3,
            "move_safe_seconds": 60,
        }

    def get_page(self) -> List[dict]:
        pass

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
                return None
            return "xunlei", None, None, "迅雷接管失败：仅支持磁力链接。"
        task_id, err = self._add_task(magnet)
        if not task_id:
            if self._fallback_to_builtin:
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
        if downloader and not self._is_xunlei_downloader(downloader):
            return None
        ids = self._normalize_hashs(hashs)
        if not ids:
            return False
        return self._operate_tasks(ids=ids, action="start")

    def stop_torrents(self, hashs: Union[list, str], downloader: Optional[str] = None) -> Optional[bool]:
        if downloader and not self._is_xunlei_downloader(downloader):
            return None
        ids = self._normalize_hashs(hashs)
        if not ids:
            return False
        return self._operate_tasks(ids=ids, action="pause")

    def remove_torrents(self, hashs: Union[str, list], delete_file: Optional[bool] = True,
                        downloader: Optional[str] = None) -> Optional[bool]:
        if downloader and not self._is_xunlei_downloader(downloader):
            return None
        ids = self._normalize_hashs(hashs)
        if not ids:
            return False
        ok = self._operate_tasks(ids=ids, action="delete", delete_file=bool(delete_file))
        if ok:
            for _id in ids:
                self._moved_task_keys.add(_id)
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
            self._moved_task_keys.add(key)
        return None

    def _save_config(self) -> None:
        self.update_config({
            "enabled": self._enabled,
            "hijack_download": self._hijack_download,
            "fallback_to_builtin": self._fallback_to_builtin,
            "base_url": self._base_url,
            "authorization": self._authorization,
            "pan_auth": self._pan_auth,
            "auto_refresh_pan_auth": self._auto_refresh_pan_auth,
            "file_id": self._file_id,
            "device_id": self._device_id,
            "move_enabled": self._move_enabled,
            "source_download_dir": self._source_download_dir,
            "target_watch_dir": self._target_watch_dir,
            "move_interval_minutes": self._move_interval_minutes,
            "move_safe_seconds": self._move_safe_seconds,
        })

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
        }
        if pan_auth:
            headers["pan-auth"] = pan_auth
        return headers

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

    def _fetch_device_id(self) -> Optional[str]:
        if self._device_id:
            return self._device_id
        if not self._base_url:
            return None
        try:
            url = f"{self._base_url}/webman/3rdparty/pan-xunlei-com/index.cgi/drive/v1/tasks?type=user%23runner&device_space="
            resp = requests.get(url, headers=self._get_headers(), timeout=20)
            resp.raise_for_status()
            obj = resp.json() if resp.text else {}
            tasks = obj.get("tasks") if isinstance(obj, dict) else None
            if not isinstance(tasks, list):
                tasks = []
            for task in tasks:
                params = task.get("params") if isinstance(task, dict) and isinstance(task.get("params"), dict) else {}
                device = str(params.get("target") or (task.get("target") if isinstance(task, dict) else "") or "").strip()
                if device:
                    self._device_id = device
                    self._save_config()
                    return device
        except Exception as err:
            logger.warn(f"XunleiHijack fetch device id failed: {err}")
        return None

    def _add_task(self, magnet: str) -> Tuple[Optional[str], Optional[str]]:
        if not self._base_url:
            return None, "迅雷地址未配置。"
        if not self._authorization:
            return None, "Authorization 未配置。"
        if not self._file_id:
            return None, "file_id 未配置。"
        if not self._fetch_device_id():
            return None, "device_id 未配置且自动获取失败。"

        headers = self._get_headers()
        if self._auto_refresh_pan_auth and not headers.get("pan-auth"):
            return None, "pan_auth 获取失败，请手动填写。"

        analysis = self._analyze_magnet(magnet, headers)
        file_name = analysis.get("name") or f"xunlei-{int(time.time())}"
        params = {
            "parent_folder_id": self._file_id,
            "url": magnet,
            "target": self._device_id,
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
            "space": self._device_id,
            "file_name": file_name,
        }
        total_size = int(analysis.get("total_size") or 0)
        if total_size > 0:
            payload["file_size"] = str(total_size)

        try:
            url = f"{self._base_url}/webman/3rdparty/pan-xunlei-com/index.cgi/drive/v1/task"
            resp = requests.post(url, json=payload, headers=headers, timeout=30)
            resp.raise_for_status()
            data = resp.json() if resp.text else {}
            if isinstance(data, dict) and data.get("error"):
                return None, f"迅雷任务创建失败：{data.get('error')}"
            task_id = self._task_id(data)
            if not task_id:
                task_id = f"xunlei-{int(time.time())}"
            self._task_name_cache[task_id] = file_name
            return task_id, None
        except Exception as err:
            return None, f"迅雷任务创建请求失败：{err}"

    def _analyze_magnet(self, magnet: str, headers: Dict[str, str]) -> Dict[str, Any]:
        result = {"name": "", "total_size": 0, "total_count": 0, "indices": []}
        try:
            url = f"{self._base_url}/webman/3rdparty/pan-xunlei-com/index.cgi/drive/v1/resource/list"
            resp = requests.post(url, json={"page_size": 1000, "urls": magnet}, headers=headers, timeout=30)
            resp.raise_for_status()
            obj = resp.json() if resp.text else {}
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
                    continue
                if self._move_safe_seconds > 0 and now_ts - src.stat().st_mtime < self._move_safe_seconds:
                    continue
                dst = self._dedupe_target(target_root / src.name)
                shutil.move(str(src), str(dst))
                self._moved_task_keys.add(key)
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
                f"?type=user%23download-url&device_space={quote(device_id)}"
            )
            resp = requests.get(url, headers=headers, timeout=20)
            resp.raise_for_status()
            obj = resp.json() if resp.text else {}
            if isinstance(obj, dict):
                tasks = obj.get("tasks")
                if isinstance(tasks, list):
                    return [x for x in tasks if isinstance(x, dict)]
                list_obj = obj.get("list")
                if isinstance(list_obj, list):
                    return [x for x in list_obj if isinstance(x, dict)]
        except Exception as err:
            logger.warn(f"XunleiHijack list tasks failed: {err}")
        return []

    def _operate_tasks(self, ids: Set[str], action: str, delete_file: bool = True) -> bool:
        if not ids:
            return False
        if not self._base_url:
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
                    resp = requests.post(url, headers=headers, json=payload, timeout=20)
                    if not resp.ok:
                        continue
                    obj = resp.json() if resp.text else {}
                    if isinstance(obj, dict) and obj.get("error"):
                        continue
                    return True
                except Exception:
                    continue
        return False

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

    @staticmethod
    def _task_left_time(task: Dict[str, Any], progress: float) -> Optional[str]:
        return None

    @staticmethod
    def _task_id(data: Any) -> str:
        if not isinstance(data, dict):
            return ""
        for key in ("task_id", "id", "gid"):
            value = data.get(key)
            if value:
                return str(value)
        task = data.get("task")
        if isinstance(task, dict):
            for key in ("task_id", "id", "gid"):
                value = task.get(key)
                if value:
                    return str(value)
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
