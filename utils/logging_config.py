"""Logging configuration extracted from ae_server.py (lines 56-95)."""

import os
import logging
from datetime import datetime
from logging.handlers import RotatingFileHandler

# ==================== 配置日志 ====================
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(
    log_dir, f"ae_server_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
)

_file_handler = RotatingFileHandler(
    log_file,
    maxBytes=20 * 1024 * 1024,
    backupCount=14,
    encoding="utf-8",
)
_file_handler.setFormatter(
    logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
)
_console_handler = logging.StreamHandler()
_console_handler.setFormatter(
    logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
)
logging.basicConfig(level=logging.INFO, handlers=[_file_handler, _console_handler])


def flush_logging_handlers() -> None:
    """将日志立即刷入文件与终端（含 Flask 工作线程内）。"""
    for handler in logging.getLogger().handlers:
        if hasattr(handler, "flush"):
            handler.flush()


def _log_asctime_local() -> str:
    """与 logging.basicConfig 默认 asctime 一致：YYYY-MM-DD HH:MM:SS,mmm（本地时区）。"""
    t = datetime.now()
    return t.strftime("%Y-%m-%d %H:%M:%S") + f",{t.microsecond // 1000:03d}"


def _position_change_fmt_value(value) -> str:
    if isinstance(value, float):
        return f"{value:.6f}"
    return str(value)
