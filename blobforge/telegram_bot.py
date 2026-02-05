"""
BlobForge Telegram Bot Integration

A fully interactive Telegram bot for managing PDF conversion jobs.
Uses inline keyboards for navigation and actions.

Requires: python-telegram-bot library
Install: pip install python-telegram-bot[job-queue] or pip install blobforge[telegram]
"""
from __future__ import annotations

import os
import io
import json
import time
import asyncio
import tempfile
import logging
import hashlib
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List, Tuple, TYPE_CHECKING
from functools import wraps

logger = logging.getLogger(__name__)

# Try to import telegram bot library
try:
    from telegram import (
        Update, 
        InlineKeyboardButton, 
        InlineKeyboardMarkup,
        ReplyKeyboardMarkup,
        ReplyKeyboardRemove,
        InputFile,
        BotCommand,
    )
    from telegram.ext import (
        Application,
        CommandHandler,
        CallbackQueryHandler,
        MessageHandler,
        ContextTypes,
        filters,
        ConversationHandler,
    )
    from telegram.constants import ParseMode, ChatAction
    from telegram.error import TelegramError
    TELEGRAM_AVAILABLE = True
except ImportError:
    TELEGRAM_AVAILABLE = False
    logger.warning("python-telegram-bot not installed. Install with: pip install python-telegram-bot[job-queue]")
    
    # Define stub classes for type hints when telegram is not installed
    # These are only used for type checking, not at runtime
    class Update: pass
    class InlineKeyboardButton: pass
    class InlineKeyboardMarkup: pass
    class ReplyKeyboardMarkup: pass
    class ReplyKeyboardRemove: pass
    class InputFile: pass
    class BotCommand: pass
    class Application: pass
    class CommandHandler: pass
    class CallbackQueryHandler: pass
    class MessageHandler: pass
    class ConversationHandler: pass
    class TelegramError(Exception): pass
    
    class ContextTypes:
        DEFAULT_TYPE = None
    
    class ParseMode:
        MARKDOWN_V2 = "MarkdownV2"
        HTML = "HTML"
    
    class ChatAction:
        TYPING = "typing"
        UPLOAD_DOCUMENT = "upload_document"
    
    class filters:
        class Document:
            PDF = None
        TEXT = None
        COMMAND = None

from .config import (
    S3_BUCKET, S3_PREFIX, S3_PREFIX_RAW, S3_PREFIX_TODO, S3_PREFIX_PROCESSING,
    S3_PREFIX_DONE, S3_PREFIX_FAILED, S3_PREFIX_DEAD, PRIORITIES, DEFAULT_PRIORITY,
    get_remote_config, save_remote_config, refresh_remote_config, WORKER_ID,
    get_stale_timeout_minutes, get_max_retries
)
from .s3_client import S3Client
from . import ingestor
from . import janitor as janitor_module
from . import status as status_module


# =============================================================================
# Constants
# =============================================================================

# Callback data prefixes for routing
CB_MENU = "menu"
CB_DASHBOARD = "dash"
CB_QUEUE = "queue"
CB_WORKERS = "workers"
CB_CONFIG = "config"
CB_LOGS = "logs"
CB_JOB = "job"
CB_PRIORITY = "prio"
CB_JANITOR = "janitor"
CB_RETRY = "retry"
CB_CANCEL = "cancel"
CB_DEAD = "dead"
CB_SEARCH = "search"
CB_MANIFEST = "manifest"
CB_PAGE = "page"
CB_CONFIRM = "confirm"
CB_DOWNLOAD = "download"
CB_PREVIEW = "preview"

# Items per page for lists
ITEMS_PER_PAGE = 5

# Emoji icons
EMOJI = {
    "dashboard": "📊",
    "queue": "📋",
    "workers": "👷",
    "config": "⚙️",
    "ingest": "📥",
    "search": "🔍",
    "logs": "📜",
    "janitor": "🧹",
    "retry": "🔄",
    "cancel": "🚫",
    "dead": "☠️",
    "manifest": "📒",
    "download": "📥",
    "preview": "👁️",
    "ok": "✅",
    "error": "❌",
    "warning": "⚠️",
    "processing": "🔄",
    "queued": "⏳",
    "done": "✅",
    "failed": "❌",
    "stale": "🔴",
    "active": "🟢",
    "back": "⬅️",
    "refresh": "🔄",
    "help": "❓",
    "critical": "🔴",
    "high": "🟠",
    "normal": "🟡",
    "low": "🟢",
    "background": "⚪",
}

# Priority emoji mapping
PRIORITY_EMOJI = {
    "1_critical": "🔴",
    "2_high": "🟠",
    "3_normal": "🟡",
    "4_low": "🟢",
    "5_background": "⚪",
}


# =============================================================================
# Authorization Decorator
# =============================================================================

def get_allowed_users() -> List[int]:
    """Get list of allowed Telegram user IDs from environment."""
    allowed = os.getenv("BLOBFORGE_TELEGRAM_ALLOWED_USERS", "")
    if not allowed:
        return []
    try:
        return [int(uid.strip()) for uid in allowed.split(",") if uid.strip()]
    except ValueError:
        return []


def authorized(func):
    """Decorator to check if user is authorized."""
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        allowed_users = get_allowed_users()
        user_id = update.effective_user.id
        
        # If no users configured, allow all (with warning logged)
        if not allowed_users:
            logger.warning(f"No BLOBFORGE_TELEGRAM_ALLOWED_USERS configured - allowing all users")
            return await func(update, context, *args, **kwargs)
        
        if user_id not in allowed_users:
            logger.warning(f"Unauthorized access attempt by user {user_id}")
            if update.callback_query:
                await update.callback_query.answer("⛔ Unauthorized", show_alert=True)
            else:
                await update.message.reply_text("⛔ You are not authorized to use this bot.")
            return
        
        return await func(update, context, *args, **kwargs)
    return wrapper


# =============================================================================
# Helper Functions
# =============================================================================

def escape_markdown_v2(text: str) -> str:
    """Escape special characters for MarkdownV2."""
    special_chars = ['_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!']
    for char in special_chars:
        text = text.replace(char, f'\\{char}')
    return text


def truncate(text: str, max_len: int = 30) -> str:
    """Truncate text with ellipsis."""
    if len(text) <= max_len:
        return text
    return text[:max_len-3] + "..."


def format_size(size_bytes: int) -> str:
    """Format bytes as human-readable size."""
    if size_bytes is None or size_bytes == 0:
        return "-"
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} TB"


def format_duration(seconds: float) -> str:
    """Format seconds as human-readable duration."""
    if seconds < 60:
        return f"{int(seconds)}s"
    elif seconds < 3600:
        mins = int(seconds // 60)
        secs = int(seconds % 60)
        return f"{mins}m {secs}s"
    else:
        hours = int(seconds // 3600)
        mins = int((seconds % 3600) // 60)
        return f"{hours}h {mins}m"


def get_status_emoji(status: str) -> str:
    """Get emoji for job status."""
    status_map = {
        "done": EMOJI["done"],
        "completed": EMOJI["done"],
        "processing": EMOJI["processing"],
        "queued": EMOJI["queued"],
        "failed": EMOJI["failed"],
        "dead": EMOJI["dead"],
        "stale": EMOJI["stale"],
    }
    return status_map.get(status.lower(), "❓")


# =============================================================================
# Keyboard Builders
# =============================================================================

def build_main_menu_keyboard() -> InlineKeyboardMarkup:
    """Build the main menu keyboard."""
    keyboard = [
        [
            InlineKeyboardButton(f"{EMOJI['dashboard']} Dashboard", callback_data=f"{CB_DASHBOARD}:view"),
            InlineKeyboardButton(f"{EMOJI['queue']} Queue Stats", callback_data=f"{CB_QUEUE}:stats"),
        ],
        [
            InlineKeyboardButton(f"{EMOJI['workers']} Workers", callback_data=f"{CB_WORKERS}:list"),
            InlineKeyboardButton(f"{EMOJI['config']} Config", callback_data=f"{CB_CONFIG}:view"),
        ],
        [
            InlineKeyboardButton(f"{EMOJI['search']} Search", callback_data=f"{CB_SEARCH}:prompt"),
            InlineKeyboardButton(f"{EMOJI['manifest']} Manifest", callback_data=f"{CB_MANIFEST}:stats"),
        ],
        [
            InlineKeyboardButton(f"{EMOJI['janitor']} Run Janitor", callback_data=f"{CB_JANITOR}:run"),
            InlineKeyboardButton(f"{EMOJI['dead']} Dead Letter", callback_data=f"{CB_DEAD}:list"),
        ],
        [
            InlineKeyboardButton(f"{EMOJI['help']} Help", callback_data=f"{CB_MENU}:help"),
        ],
    ]
    return InlineKeyboardMarkup(keyboard)


def build_back_button(callback_data: str = f"{CB_MENU}:main") -> InlineKeyboardButton:
    """Build a back button."""
    return InlineKeyboardButton(f"{EMOJI['back']} Back", callback_data=callback_data)


def build_refresh_button(callback_data: str) -> InlineKeyboardButton:
    """Build a refresh button."""
    return InlineKeyboardButton(f"{EMOJI['refresh']} Refresh", callback_data=callback_data)


def build_pagination_keyboard(
    current_page: int, 
    total_pages: int, 
    callback_prefix: str,
    extra_buttons: List[InlineKeyboardButton] = None
) -> InlineKeyboardMarkup:
    """Build a pagination keyboard."""
    keyboard = []
    
    # Pagination row
    nav_row = []
    if current_page > 0:
        nav_row.append(InlineKeyboardButton("◀️ Prev", callback_data=f"{callback_prefix}:{current_page - 1}"))
    nav_row.append(InlineKeyboardButton(f"{current_page + 1}/{total_pages}", callback_data="noop"))
    if current_page < total_pages - 1:
        nav_row.append(InlineKeyboardButton("Next ▶️", callback_data=f"{callback_prefix}:{current_page + 1}"))
    
    if nav_row:
        keyboard.append(nav_row)
    
    # Extra buttons row
    if extra_buttons:
        keyboard.append(extra_buttons)
    
    # Back button
    keyboard.append([build_back_button()])
    
    return InlineKeyboardMarkup(keyboard)


def build_job_actions_keyboard(job_hash: str, status: str, priority: str = None) -> InlineKeyboardMarkup:
    """Build action keyboard for a specific job."""
    keyboard = []
    
    if status == "done" or status == "completed":
        keyboard.append([
            InlineKeyboardButton(f"{EMOJI['download']} Download", callback_data=f"{CB_DOWNLOAD}:{job_hash}"),
            InlineKeyboardButton(f"{EMOJI['preview']} Preview", callback_data=f"{CB_PREVIEW}:{job_hash}"),
        ])
    elif status == "queued":
        keyboard.append([
            InlineKeyboardButton(f"📊 Change Priority", callback_data=f"{CB_PRIORITY}:select:{job_hash}"),
        ])
    elif status == "processing":
        keyboard.append([
            InlineKeyboardButton(f"{EMOJI['cancel']} Cancel Job", callback_data=f"{CB_CANCEL}:confirm:{job_hash}"),
        ])
    elif status == "failed":
        keyboard.append([
            InlineKeyboardButton(f"{EMOJI['retry']} Retry Now", callback_data=f"{CB_RETRY}:job:{job_hash}"),
        ])
    elif status == "dead":
        keyboard.append([
            InlineKeyboardButton(f"{EMOJI['retry']} Retry", callback_data=f"{CB_RETRY}:dead:{job_hash}"),
        ])
    
    # Always show logs button
    keyboard.append([
        InlineKeyboardButton(f"{EMOJI['logs']} View Logs", callback_data=f"{CB_LOGS}:{job_hash}"),
    ])
    
    # Back and refresh
    keyboard.append([
        build_back_button(),
        build_refresh_button(f"{CB_JOB}:status:{job_hash}"),
    ])
    
    return InlineKeyboardMarkup(keyboard)


def build_priority_keyboard(job_hash: str) -> InlineKeyboardMarkup:
    """Build priority selection keyboard."""
    keyboard = []
    
    for prio in PRIORITIES:
        emoji = PRIORITY_EMOJI.get(prio, "⚪")
        label = prio.split("_")[1].capitalize()
        keyboard.append([
            InlineKeyboardButton(f"{emoji} {label}", callback_data=f"{CB_PRIORITY}:set:{job_hash}:{prio}")
        ])
    
    keyboard.append([build_back_button(f"{CB_JOB}:status:{job_hash}")])
    
    return InlineKeyboardMarkup(keyboard)


def build_confirmation_keyboard(action: str, data: str) -> InlineKeyboardMarkup:
    """Build a yes/no confirmation keyboard."""
    keyboard = [
        [
            InlineKeyboardButton("✅ Yes", callback_data=f"{CB_CONFIRM}:yes:{action}:{data}"),
            InlineKeyboardButton("❌ No", callback_data=f"{CB_CONFIRM}:no:{action}:{data}"),
        ]
    ]
    return InlineKeyboardMarkup(keyboard)


# =============================================================================
# Message Formatters
# =============================================================================

def format_dashboard_message(s3: S3Client) -> str:
    """Format the dashboard status message."""
    stale_timeout = get_stale_timeout_minutes()
    bucket_escaped = escape_markdown_v2(S3_BUCKET)
    
    lines = [
        f"📊 *BlobForge Dashboard*",
        f"",
        f"🗄️ Bucket: `{bucket_escaped}`",
        f"⏱️ Stale timeout: {stale_timeout} min",
        f"",
    ]
    
    # Queue stats
    lines.append("*📋 Queue Status*")
    total_todo = 0
    for prio in PRIORITIES:
        count = s3.count_prefix(f"{S3_PREFIX_TODO}/{prio}/")
        emoji = PRIORITY_EMOJI.get(prio, "⚪")
        label = prio.split("_")[1].capitalize()
        lines.append(f"  {emoji} {label}: `{count}`")
        total_todo += count
    lines.append(f"  *Total queued: `{total_todo}`*")
    lines.append("")
    
    # Processing - enhanced with job details
    active_jobs = s3.scan_processing_detailed()
    stale_count = sum(1 for j in active_jobs if j.get('stale', False))
    lines.append("*🔄 Processing*")
    lines.append(f"  Active: `{len(active_jobs)}`")
    if stale_count:
        lines.append(f"  ⚠️ Stale: `{stale_count}` \\(run janitor\\)")
    
    # Show details for active jobs
    if active_jobs:
        lines.append("")
        for job in sorted(active_jobs, key=lambda x: x.get('age', timedelta(0)), reverse=True)[:5]:
            status_icon = "🔴" if job.get('stale') else "🟢"
            job_hash = job['hash'][:8]
            
            progress = job.get('progress', {}) or {}
            worker = escape_markdown_v2(truncate(job.get('worker', '?'), 10))
            elapsed = escape_markdown_v2(format_duration(job.get('age', timedelta(0)).total_seconds()))
            
            # Get filename
            filename = progress.get('original_filename', '')
            if not filename:
                entry = s3.lookup_by_hash(job['hash'])
                if entry and entry.get('paths'):
                    filename = os.path.basename(entry['paths'][0])
            filename = escape_markdown_v2(truncate(filename, 20)) if filename else "\\-"
            
            # Get stage info with marker/tqdm progress
            stage = progress.get('stage', '-')
            marker_progress = progress.get('marker', {})
            eta_str = ""
            
            if marker_progress:
                tqdm_stage = marker_progress.get('tqdm_stage', '')
                tqdm_current = marker_progress.get('tqdm_current', 0)
                tqdm_total = marker_progress.get('tqdm_total', 0)
                tqdm_eta = marker_progress.get('tqdm_eta')
                
                if tqdm_stage and tqdm_total:
                    stage = f"{tqdm_stage}: {tqdm_current}/{tqdm_total}"
                elif tqdm_stage:
                    stage = tqdm_stage
                
                if tqdm_eta is not None and tqdm_eta > 0:
                    if tqdm_eta < 60:
                        eta_str = f" ~{int(tqdm_eta)}s"
                    elif tqdm_eta < 3600:
                        eta_str = f" ~{int(tqdm_eta // 60)}m"
                    else:
                        eta_str = f" ~{int(tqdm_eta // 3600)}h"
            
            stage_escaped = escape_markdown_v2(truncate(stage, 25))
            eta_escaped = escape_markdown_v2(eta_str)
            
            # Build details (pages, size, CPU)
            details = []
            page_count = progress.get('page_count')
            if page_count:
                details.append(f"{page_count}pg")
            
            file_size = progress.get('file_size_formatted', '')
            if file_size:
                details.append(escape_markdown_v2(file_size))
            
            system = progress.get('system', {})
            cpu = system.get('cpu_percent')
            if cpu is not None:
                details.append(f"CPU:{cpu}%")
            
            details_str = " ".join(details) if details else ""
            
            lines.append(f"  {status_icon} `{job_hash}` {filename}")
            lines.append(f"      ⏱️{elapsed} │ {stage_escaped}{eta_escaped}")
            if details_str:
                lines.append(f"      📎 {details_str}")
        
        if len(active_jobs) > 5:
            lines.append(f"  _\\.\\.\\.and {len(active_jobs) - 5} more_")
    
    lines.append("")
    
    # Results
    done_count = s3.count_prefix(f"{S3_PREFIX_DONE}/")
    failed_count = s3.count_prefix(f"{S3_PREFIX_FAILED}/")
    dead_count = s3.count_prefix(f"{S3_PREFIX_DEAD}/")
    
    lines.append("*📈 Results*")
    lines.append(f"  ✅ Completed: `{done_count}`")
    lines.append(f"  ⏳ Failed: `{failed_count}`")
    lines.append(f"  ☠️ Dead: `{dead_count}`")
    lines.append("")
    
    # Progress
    total_jobs = total_todo + len(active_jobs) + done_count + failed_count + dead_count
    if total_jobs > 0:
        pct = (done_count / total_jobs) * 100
        bar_filled = int(pct / 5)
        bar = "█" * bar_filled + "░" * (20 - bar_filled)
        pct_str = escape_markdown_v2(f"{pct:.1f}")
        lines.append(f"*📊 Progress*")
        lines.append(f"`[{bar}]` {pct_str}%")
    
    return "\n".join(lines)


def format_queue_stats_message(s3: S3Client) -> str:
    """Format detailed queue statistics."""
    lines = [
        f"📋 *Queue Statistics*",
        f"",
    ]
    
    # Todo queues with sample jobs
    lines.append("*Queued Jobs:*")
    total = 0
    for prio in PRIORITIES:
        keys = s3.list_keys(f"{S3_PREFIX_TODO}/{prio}/")
        count = len(keys)
        total += count
        emoji = PRIORITY_EMOJI.get(prio, "⚪")
        label = prio.split("_")[1].capitalize()
        lines.append(f"  {emoji} {label}: `{count}`")
    lines.append(f"  *Total: `{total}`*")
    lines.append("")
    
    # Processing
    lines.append("*Processing:*")
    proc_jobs = s3.scan_processing_detailed()
    lines.append(f"  Active: `{len(proc_jobs)}`")
    
    stale_count = sum(1 for j in proc_jobs if j.get('stale', False))
    if stale_count:
        lines.append(f"  ⚠️ Stale: `{stale_count}`")
    
    # Show first few processing jobs
    for job in proc_jobs[:3]:
        status = "🔴" if job.get('stale') else "🟢"
        worker = escape_markdown_v2(truncate(job.get('worker', '?'), 12))
        job_hash = job['hash'][:8]
        progress = job.get('progress', {}) or {}
        stage = escape_markdown_v2(truncate(progress.get('stage', '-'), 20))
        lines.append(f"  {status} `{job_hash}` \\({worker}\\): {stage}")
    
    if len(proc_jobs) > 3:
        lines.append(f"  _\\.\\.\\.and {len(proc_jobs) - 3} more_")
    
    lines.append("")
    
    # Failed queue
    failed = s3.list_failed()
    failed = [f for f in failed if not f['Key'].endswith("/")]
    lines.append(f"*Failed \\(pending retry\\):* `{len(failed)}`")
    
    # Dead letter
    dead = s3.list_dead()
    dead = [d for d in dead if not d['Key'].endswith("/")]
    lines.append(f"*Dead\\-letter:* `{len(dead)}`")
    
    return "\n".join(lines)


def format_workers_message(s3: S3Client, active_only: bool = False) -> str:
    """Format workers list message."""
    stale_timeout = get_stale_timeout_minutes()
    
    if active_only:
        workers = s3.get_active_workers(stale_minutes=stale_timeout)
        title = f"Active Workers \\(heartbeat \\< {stale_timeout}m\\)"
    else:
        workers = s3.list_workers()
        title = "All Registered Workers"
    
    lines = [
        f"👷 *{title}*",
        f"",
    ]
    
    if not workers:
        lines.append("_No workers found_")
        return "\n".join(lines)
    
    # Sort by last heartbeat
    workers.sort(key=lambda w: w.get('last_heartbeat', ''), reverse=True)
    
    for w in workers[:10]:
        worker_id = escape_markdown_v2(w.get('worker_id', '?')[:12])
        hostname = escape_markdown_v2(truncate(w.get('hostname', '?'), 15))
        status = w.get('status', '?')
        
        status_icon = "🟢" if status in ("active", "processing") else "🔴" if status == "stopped" else "⚪"
        
        metrics = w.get('metrics', {})
        jobs_completed = metrics.get('jobs_completed', 0)
        jobs_per_hour = metrics.get('jobs_per_hour', 0)
        jph_str = escape_markdown_v2(f"{jobs_per_hour:.1f}")
        
        system = w.get('system', {})
        cpu = system.get('cpu_percent', '-')
        mem = system.get('memory_percent', '-')
        
        lines.append(f"{status_icon} *{worker_id}*")
        lines.append(f"  Host: `{hostname}`")
        lines.append(f"  Jobs: `{jobs_completed}` done, `{jph_str}`/hr")
        if cpu != '-':
            lines.append(f"  System: CPU `{cpu}%`, MEM `{mem}%`")
        lines.append("")
    
    if len(workers) > 10:
        lines.append(f"  _\\.\\.\\.and {len(workers) - 10} more workers_")
    
    lines.append(f"\n📊 *Total: {len(workers)} worker\\(s\\)*")
    
    return "\n".join(lines)


def format_config_message() -> str:
    """Format config view message."""
    refresh_remote_config()
    config = get_remote_config()
    
    bucket_escaped = escape_markdown_v2(S3_BUCKET)
    lines = [
        f"⚙️ *Remote Configuration*",
        f"",
        f"_Stored in S3: `{bucket_escaped}/registry/config\\.json`_",
        f"",
    ]
    
    for key, value in sorted(config.items()):
        # Escape special chars
        key_escaped = escape_markdown_v2(key)
        value_escaped = escape_markdown_v2(str(value))
        lines.append(f"• `{key_escaped}`: `{value_escaped}`")
    
    return "\n".join(lines)


def format_job_status_message(s3: S3Client, job_hash: str) -> Tuple[str, str]:
    """Format job status message. Returns (message, status)."""
    lines = []
    status = "unknown"
    
    # Check done
    if s3.exists(f"{S3_PREFIX_DONE}/{job_hash}.zip"):
        status = "done"
        lines.append(f"✅ *Job Status: COMPLETED*")
        lines.append(f"")
        lines.append(f"Hash: `{job_hash[:24]}\\.\\.\\.`")
        lines.append(f"Output: Ready for download")
        
        # Get manifest info
        entry = s3.lookup_by_hash(job_hash)
        if entry:
            paths = entry.get('paths', [])
            if paths:
                path_escaped = escape_markdown_v2(truncate(paths[0], 40))
                lines.append(f"File: `{path_escaped}`")
            size = entry.get('size', 0)
            if size:
                size_str = escape_markdown_v2(format_size(size))
                lines.append(f"Size: `{size_str}`")
        
        return "\n".join(lines), status
    
    # Check dead
    if s3.exists(f"{S3_PREFIX_DEAD}/{job_hash}"):
        status = "dead"
        lines.append(f"☠️ *Job Status: DEAD \\(max retries exceeded\\)*")
        lines.append(f"")
        lines.append(f"Hash: `{job_hash[:24]}\\.\\.\\.`")
        
        data = s3.get_object_json(f"{S3_PREFIX_DEAD}/{job_hash}")
        if data:
            error_escaped = escape_markdown_v2(truncate(data.get('error', 'Unknown'), 50))
            lines.append(f"Error: `{error_escaped}`")
            lines.append(f"Retries: `{data.get('total_retries', '?')}`")
        
        return "\n".join(lines), status
    
    # Check failed
    if s3.exists(f"{S3_PREFIX_FAILED}/{job_hash}"):
        status = "failed"
        lines.append(f"❌ *Job Status: FAILED \\(pending retry\\)*")
        lines.append(f"")
        lines.append(f"Hash: `{job_hash[:24]}\\.\\.\\.`")
        
        data = s3.get_object_json(f"{S3_PREFIX_FAILED}/{job_hash}")
        if data:
            error_escaped = escape_markdown_v2(truncate(data.get('error', 'Unknown'), 50))
            worker_escaped = escape_markdown_v2(data.get('worker', '?'))
            lines.append(f"Error: `{error_escaped}`")
            lines.append(f"Retries: `{data.get('retries', 0)}`")
            lines.append(f"Worker: `{worker_escaped}`")
        
        lines.append(f"\n_Janitor will retry automatically_")
        return "\n".join(lines), status
    
    # Check processing
    if s3.exists(f"{S3_PREFIX_PROCESSING}/{job_hash}"):
        status = "processing"
        lines.append(f"🔄 *Job Status: PROCESSING*")
        lines.append(f"")
        lines.append(f"Hash: `{job_hash[:24]}\\.\\.\\.`")
        
        data = s3.get_object_json(f"{S3_PREFIX_PROCESSING}/{job_hash}")
        if data:
            worker_escaped = escape_markdown_v2(data.get('worker', '?'))
            lines.append(f"Worker: `{worker_escaped}`")
            started = data.get('started')
            if started:
                started_dt = datetime.fromtimestamp(started / 1000.0)
                elapsed = datetime.now() - started_dt
                elapsed_str = escape_markdown_v2(format_duration(elapsed.total_seconds()))
                lines.append(f"Elapsed: `{elapsed_str}`")
            
            progress = data.get('progress', {})
            if progress:
                # Basic stage
                stage = progress.get('stage', '-')
                
                # Enhanced marker/tqdm progress
                marker_progress = progress.get('marker', {})
                eta_str = ""
                if marker_progress:
                    tqdm_stage = marker_progress.get('tqdm_stage', '')
                    tqdm_current = marker_progress.get('tqdm_current', 0)
                    tqdm_total = marker_progress.get('tqdm_total', 0)
                    tqdm_percent = marker_progress.get('tqdm_percent', 0)
                    tqdm_eta = marker_progress.get('tqdm_eta')
                    
                    if tqdm_stage and tqdm_total:
                        stage = f"{tqdm_stage}: {tqdm_current}/{tqdm_total} ({tqdm_percent:.0f}%)"
                    elif tqdm_stage:
                        stage = tqdm_stage
                    
                    if tqdm_eta is not None and tqdm_eta > 0:
                        if tqdm_eta < 60:
                            eta_str = f"~{int(tqdm_eta)}s"
                        elif tqdm_eta < 3600:
                            eta_str = f"~{int(tqdm_eta // 60)}m {int(tqdm_eta % 60)}s"
                        else:
                            hours = int(tqdm_eta // 3600)
                            mins = int((tqdm_eta % 3600) // 60)
                            eta_str = f"~{hours}h {mins}m"
                
                stage_escaped = escape_markdown_v2(stage)
                lines.append(f"Stage: `{stage_escaped}`")
                if eta_str:
                    lines.append(f"ETA: `{escape_markdown_v2(eta_str)}`")
                
                # File info
                filename = progress.get('original_filename', '')
                if filename:
                    lines.append(f"File: `{escape_markdown_v2(truncate(filename, 40))}`")
                
                file_size = progress.get('file_size_formatted', '')
                if file_size:
                    lines.append(f"Size: `{escape_markdown_v2(file_size)}`")
                
                page_count = progress.get('page_count')
                if page_count:
                    lines.append(f"Pages: `{page_count}`")
                
                # System metrics
                system = progress.get('system', {})
                cpu = system.get('cpu_percent')
                mem = system.get('memory_percent')
                if cpu is not None or mem is not None:
                    metrics = []
                    if cpu is not None:
                        metrics.append(f"CPU: {cpu}%")
                    if mem is not None:
                        metrics.append(f"MEM: {mem}%")
                    lines.append(f"System: `{escape_markdown_v2(' | '.join(metrics))}`")
        
        return "\n".join(lines), status
    
    # Check todo
    for prio in PRIORITIES:
        key = f"{S3_PREFIX_TODO}/{prio}/{job_hash}"
        if s3.exists(key):
            status = "queued"
            emoji = PRIORITY_EMOJI.get(prio, "⚪")
            label = prio.split("_")[1].capitalize()
            
            lines.append(f"⏳ *Job Status: QUEUED*")
            lines.append(f"")
            lines.append(f"Hash: `{job_hash[:24]}\\.\\.\\.`")
            lines.append(f"Priority: {emoji} {label}")
            
            data = s3.get_object_json(key)
            if data:
                retries = data.get('retries', 0)
                if retries > 0:
                    lines.append(f"Previous retries: `{retries}`")
            
            return "\n".join(lines), status
    
    # Check raw only
    if s3.exists(f"{S3_PREFIX_RAW}/{job_hash}.pdf"):
        status = "raw"
        lines.append(f"📄 *Job Status: RAW ONLY*")
        lines.append(f"")
        lines.append(f"Hash: `{job_hash[:24]}\\.\\.\\.`")
        lines.append(f"\n_PDF exists but not queued for processing_")
        return "\n".join(lines), status
    
    # Not found
    lines.append(f"❓ *Job Status: NOT FOUND*")
    lines.append(f"")
    lines.append(f"Hash: `{job_hash}`")
    return "\n".join(lines), status


def format_manifest_stats_message(s3: S3Client) -> str:
    """Format manifest statistics."""
    manifest = s3.get_manifest()
    entries = manifest.get('entries', {})
    
    version_escaped = escape_markdown_v2(str(manifest.get('version', '?')))
    updated_escaped = escape_markdown_v2(str(manifest.get('updated_at', 'Never')))
    lines = [
        f"📒 *Manifest Statistics*",
        f"",
        f"Version: `{version_escaped}`",
        f"Updated: `{updated_escaped}`",
        f"Entries: `{len(entries)}`",
        f"",
    ]
    
    if entries:
        total_size = sum(e.get('size', 0) for e in entries.values())
        total_paths = sum(len(e.get('paths', [])) for e in entries.values())
        all_tags = set()
        for e in entries.values():
            all_tags.update(e.get('tags', []))
        
        size_str = escape_markdown_v2(format_size(total_size))
        lines.append(f"Total size: `{size_str}`")
        lines.append(f"Total paths: `{total_paths}`")
        lines.append(f"Unique tags: `{len(all_tags)}`")
        
        # Top tags
        from collections import Counter
        tag_counts = Counter()
        for e in entries.values():
            tag_counts.update(e.get('tags', []))
        
        if tag_counts:
            lines.append(f"\n*Top Tags:*")
            for tag, count in tag_counts.most_common(5):
                tag_escaped = escape_markdown_v2(truncate(tag, 25))
                lines.append(f"  • `{tag_escaped}`: {count}")
    
    return "\n".join(lines)


def format_dead_letter_list(s3: S3Client, page: int = 0) -> Tuple[str, int]:
    """Format dead letter queue list. Returns (message, total_pages)."""
    dead_jobs = s3.list_dead()
    dead_jobs = [d for d in dead_jobs if not d['Key'].endswith("/")]
    
    total = len(dead_jobs)
    total_pages = max(1, (total + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE)
    
    lines = [
        f"☠️ *Dead\\-Letter Queue*",
        f"",
        f"Total: `{total}` jobs",
        f"",
    ]
    
    if not dead_jobs:
        lines.append("_Queue is empty_")
        return "\n".join(lines), 1
    
    start = page * ITEMS_PER_PAGE
    end = start + ITEMS_PER_PAGE
    page_jobs = dead_jobs[start:end]
    
    for job in page_jobs:
        job_hash = job['Key'].split('/')[-1]
        data = s3.get_object_json(job['Key'])
        
        error = escape_markdown_v2(truncate(data.get('error', 'Unknown'), 30)) if data else "Unknown"
        retries = data.get('total_retries', '?') if data else '?'
        
        # Get filename from manifest
        entry = s3.lookup_by_hash(job_hash)
        filename = escape_markdown_v2(truncate(entry.get('paths', ['?'])[0], 25)) if entry else job_hash[:12]
        
        lines.append(f"• `{job_hash[:12]}\\.\\.\\.`")
        lines.append(f"  File: {filename}")
        lines.append(f"  Error: {error}")
        lines.append(f"  Retries: {retries}")
        lines.append("")
    
    return "\n".join(lines), total_pages


# =============================================================================
# Command Handlers
# =============================================================================

@authorized
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /start command."""
    await update.message.reply_text(
        f"🤖 *BlobForge Bot*\n\n"
        f"Welcome to the BlobForge PDF conversion management bot\\!\n\n"
        f"Use the menu below to navigate, or send a PDF file to add it for processing\\.",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=build_main_menu_keyboard()
    )


@authorized
async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /help command."""
    help_text = """
🤖 *BlobForge Bot Help*

*Commands:*
/start \\- Show main menu
/dashboard \\- View system status
/queue \\- Queue statistics
/workers \\- List workers
/config \\- View configuration
/search \\<query\\> \\- Search by filename
/status \\<hash\\> \\- Check job status
/janitor \\- Run janitor
/help \\- Show this help

*File Upload:*
Send a PDF file to add it for processing\\.

*Navigation:*
Use the inline buttons to navigate and perform actions\\.
"""
    await update.message.reply_text(
        help_text,
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=build_main_menu_keyboard()
    )


@authorized
async def cmd_dashboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /dashboard command."""
    await update.message.reply_chat_action(ChatAction.TYPING)
    s3 = S3Client()
    
    message = format_dashboard_message(s3)
    keyboard = InlineKeyboardMarkup([
        [build_refresh_button(f"{CB_DASHBOARD}:view")],
        [build_back_button()],
    ])
    
    await update.message.reply_text(
        message,
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=keyboard
    )


@authorized
async def cmd_queue(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /queue command."""
    await update.message.reply_chat_action(ChatAction.TYPING)
    s3 = S3Client()
    
    message = format_queue_stats_message(s3)
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton(f"{EMOJI['retry']} Retry All Failed", callback_data=f"{CB_RETRY}:all:failed"),
        ],
        [build_refresh_button(f"{CB_QUEUE}:stats"), build_back_button()],
    ])
    
    await update.message.reply_text(
        message,
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=keyboard
    )


@authorized
async def cmd_workers(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /workers command."""
    await update.message.reply_chat_action(ChatAction.TYPING)
    s3 = S3Client()
    
    message = format_workers_message(s3, active_only=True)
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🟢 Active Only", callback_data=f"{CB_WORKERS}:active"),
            InlineKeyboardButton("📋 All Workers", callback_data=f"{CB_WORKERS}:all"),
        ],
        [build_refresh_button(f"{CB_WORKERS}:list"), build_back_button()],
    ])
    
    await update.message.reply_text(
        message,
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=keyboard
    )


@authorized
async def cmd_config(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /config command."""
    message = format_config_message()
    keyboard = InlineKeyboardMarkup([
        [build_refresh_button(f"{CB_CONFIG}:view"), build_back_button()],
    ])
    
    await update.message.reply_text(
        message,
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=keyboard
    )


@authorized
async def cmd_search(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /search command."""
    if not context.args:
        await update.message.reply_text(
            "🔍 *Search*\n\nUsage: `/search <filename or tag>`",
            parse_mode=ParseMode.MARKDOWN_V2
        )
        return
    
    query = " ".join(context.args).lower()
    await update.message.reply_chat_action(ChatAction.TYPING)
    s3 = S3Client()
    
    results = s3.search_manifest(query)
    
    if not results:
        query_escaped = escape_markdown_v2(query)
        await update.message.reply_text(
            f"🔍 No results found for: `{query_escaped}`",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=InlineKeyboardMarkup([[build_back_button()]])
        )
        return
    
    query_escaped = escape_markdown_v2(query)
    lines = [
        f"🔍 *Search Results*",
        f"Query: `{query_escaped}`",
        f"Found: `{len(results)}` matches",
        f"",
    ]
    
    for entry in results[:10]:
        job_hash = entry['hash']
        paths = entry.get('paths', [])
        filename = escape_markdown_v2(truncate(paths[0], 35)) if paths else job_hash[:12]
        
        # Check status
        if s3.exists(f"{S3_PREFIX_DONE}/{job_hash}.zip"):
            status = "✅"
        elif s3.exists(f"{S3_PREFIX_PROCESSING}/{job_hash}"):
            status = "🔄"
        elif any(s3.exists(f"{S3_PREFIX_TODO}/{p}/{job_hash}") for p in PRIORITIES):
            status = "⏳"
        else:
            status = "📄"
        
        lines.append(f"{status} `{job_hash[:12]}\\.\\.\\.`")
        lines.append(f"    {filename}")
    
    if len(results) > 10:
        lines.append(f"\n_\\.\\.\\.and {len(results) - 10} more_")
    
    # Build keyboard with job selection
    keyboard = []
    for entry in results[:5]:
        job_hash = entry['hash']
        paths = entry.get('paths', [])
        filename = truncate(paths[0], 25) if paths else job_hash[:8]
        keyboard.append([
            InlineKeyboardButton(f"📄 {filename}", callback_data=f"{CB_JOB}:status:{job_hash}")
        ])
    
    keyboard.append([build_back_button()])
    
    await update.message.reply_text(
        "\n".join(lines),
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )


@authorized
async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /status command."""
    if not context.args:
        await update.message.reply_text(
            "📊 *Status*\n\nUsage: `/status <job_hash>`",
            parse_mode=ParseMode.MARKDOWN_V2
        )
        return
    
    job_hash = context.args[0]
    await update.message.reply_chat_action(ChatAction.TYPING)
    s3 = S3Client()
    
    message, status = format_job_status_message(s3, job_hash)
    keyboard = build_job_actions_keyboard(job_hash, status)
    
    await update.message.reply_text(
        message,
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=keyboard
    )


@authorized
async def cmd_janitor(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /janitor command."""
    await update.message.reply_text(
        f"🧹 *Run Janitor?*\n\n"
        f"This will recover stale jobs and retry failed ones\\.",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=build_confirmation_keyboard("janitor", "run")
    )


# =============================================================================
# File Handler
# =============================================================================

@authorized
async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle uploaded PDF files."""
    document = update.message.document
    
    if not document.file_name.lower().endswith('.pdf'):
        await update.message.reply_text(
            "❌ Please send a PDF file\\.",
            parse_mode=ParseMode.MARKDOWN_V2
        )
        return
    
    await update.message.reply_chat_action(ChatAction.UPLOAD_DOCUMENT)
    
    # Download file
    file = await document.get_file()
    
    # Create temp file
    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
        await file.download_to_drive(tmp.name)
        tmp_path = tmp.name
    
    try:
        # Calculate hash
        with open(tmp_path, 'rb') as f:
            file_hash = hashlib.sha256(f.read()).hexdigest()
        
        # Check if already exists
        s3 = S3Client()
        if s3.exists(f"{S3_PREFIX_RAW}/{file_hash}.pdf"):
            # Already exists - check status
            message, status = format_job_status_message(s3, file_hash)
            keyboard = build_job_actions_keyboard(file_hash, status)
            
            await update.message.reply_text(
                f"📄 *File Already Exists*\n\n{message}",
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=keyboard
            )
            return
        
        # Ask for priority
        context.user_data['pending_file'] = tmp_path
        context.user_data['pending_filename'] = document.file_name
        context.user_data['pending_hash'] = file_hash
        
        keyboard = []
        for prio in PRIORITIES:
            emoji = PRIORITY_EMOJI.get(prio, "⚪")
            label = prio.split("_")[1].capitalize()
            keyboard.append([
                InlineKeyboardButton(
                    f"{emoji} {label}", 
                    callback_data=f"ingest:prio:{prio}"
                )
            ])
        keyboard.append([
            InlineKeyboardButton("❌ Cancel", callback_data="ingest:cancel")
        ])
        
        await update.message.reply_text(
            f"📥 *Ingest File*\n\n"
            f"File: `{escape_markdown_v2(truncate(document.file_name, 40))}`\n"
            f"Size: `{escape_markdown_v2(format_size(document.file_size))}`\n"
            f"Hash: `{file_hash[:16]}\\.\\.\\.`\n\n"
            f"Select priority:",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        
    except Exception as e:
        logger.exception("Error handling document")
        error_escaped = escape_markdown_v2(str(e))
        await update.message.reply_text(
            f"❌ Error processing file: `{error_escaped}`",
            parse_mode=ParseMode.MARKDOWN_V2
        )
        try:
            os.unlink(tmp_path)
        except:
            pass


# =============================================================================
# Callback Query Handler
# =============================================================================

@authorized
async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle all callback queries."""
    query = update.callback_query
    await query.answer()
    
    data = query.data
    parts = data.split(":")
    action = parts[0]
    
    s3 = S3Client()
    
    try:
        # === Main Menu ===
        if action == CB_MENU:
            sub = parts[1] if len(parts) > 1 else "main"
            if sub == "main":
                await query.edit_message_text(
                    f"🤖 *BlobForge Bot*\n\nSelect an option:",
                    parse_mode=ParseMode.MARKDOWN_V2,
                    reply_markup=build_main_menu_keyboard()
                )
            elif sub == "help":
                help_text = """
🤖 *BlobForge Help*

• *Dashboard* \\- System overview
• *Queue* \\- Job queue statistics
• *Workers* \\- Active workers
• *Config* \\- Remote configuration
• *Search* \\- Find jobs by filename
• *Manifest* \\- File manifest stats
• *Janitor* \\- Recover stale jobs
• *Dead Letter* \\- Failed jobs queue

_Send a PDF to add it for processing_
"""
                await query.edit_message_text(
                    help_text,
                    parse_mode=ParseMode.MARKDOWN_V2,
                    reply_markup=InlineKeyboardMarkup([[build_back_button()]])
                )
        
        # === Dashboard ===
        elif action == CB_DASHBOARD:
            message = format_dashboard_message(s3)
            keyboard = InlineKeyboardMarkup([
                [build_refresh_button(f"{CB_DASHBOARD}:view")],
                [build_back_button()],
            ])
            await query.edit_message_text(
                message,
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=keyboard
            )
        
        # === Queue Stats ===
        elif action == CB_QUEUE:
            message = format_queue_stats_message(s3)
            keyboard = InlineKeyboardMarkup([
                [
                    InlineKeyboardButton(f"{EMOJI['retry']} Retry All Failed", callback_data=f"{CB_RETRY}:all:failed"),
                ],
                [build_refresh_button(f"{CB_QUEUE}:stats"), build_back_button()],
            ])
            await query.edit_message_text(
                message,
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=keyboard
            )
        
        # === Workers ===
        elif action == CB_WORKERS:
            sub = parts[1] if len(parts) > 1 else "list"
            active_only = sub in ("list", "active")
            
            message = format_workers_message(s3, active_only=active_only)
            keyboard = InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("🟢 Active Only", callback_data=f"{CB_WORKERS}:active"),
                    InlineKeyboardButton("📋 All Workers", callback_data=f"{CB_WORKERS}:all"),
                ],
                [build_refresh_button(f"{CB_WORKERS}:{sub}"), build_back_button()],
            ])
            await query.edit_message_text(
                message,
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=keyboard
            )
        
        # === Config ===
        elif action == CB_CONFIG:
            message = format_config_message()
            keyboard = InlineKeyboardMarkup([
                [build_refresh_button(f"{CB_CONFIG}:view"), build_back_button()],
            ])
            await query.edit_message_text(
                message,
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=keyboard
            )
        
        # === Manifest ===
        elif action == CB_MANIFEST:
            message = format_manifest_stats_message(s3)
            keyboard = InlineKeyboardMarkup([
                [build_refresh_button(f"{CB_MANIFEST}:stats"), build_back_button()],
            ])
            await query.edit_message_text(
                message,
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=keyboard
            )
        
        # === Dead Letter ===
        elif action == CB_DEAD:
            sub = parts[1] if len(parts) > 1 else "list"
            page = int(parts[2]) if len(parts) > 2 else 0
            
            message, total_pages = format_dead_letter_list(s3, page)
            
            extra = [
                InlineKeyboardButton(f"{EMOJI['retry']} Retry All", callback_data=f"{CB_RETRY}:all:dead"),
                InlineKeyboardButton(f"🗑️ Clear All", callback_data=f"{CB_CONFIRM}:ask:clear_dead:all"),
            ]
            
            keyboard = build_pagination_keyboard(page, total_pages, f"{CB_DEAD}:page", extra)
            await query.edit_message_text(
                message,
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=keyboard
            )
        
        # === Search Prompt ===
        elif action == CB_SEARCH:
            await query.edit_message_text(
                f"🔍 *Search*\n\n"
                f"Send a filename or tag to search\\.\n\n"
                f"Or use: `/search <query>`",
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=InlineKeyboardMarkup([[build_back_button()]])
            )
            context.user_data['awaiting_search'] = True
        
        # === Janitor ===
        elif action == CB_JANITOR:
            sub = parts[1] if len(parts) > 1 else "run"
            if sub == "run":
                await query.edit_message_text(
                    f"🧹 *Run Janitor?*\n\n"
                    f"This will recover stale jobs and retry failed ones\\.",
                    parse_mode=ParseMode.MARKDOWN_V2,
                    reply_markup=build_confirmation_keyboard("janitor", "run")
                )
        
        # === Job Status ===
        elif action == CB_JOB:
            sub = parts[1] if len(parts) > 1 else "status"
            job_hash = parts[2] if len(parts) > 2 else None
            
            if sub == "status" and job_hash:
                message, status = format_job_status_message(s3, job_hash)
                keyboard = build_job_actions_keyboard(job_hash, status)
                await query.edit_message_text(
                    message,
                    parse_mode=ParseMode.MARKDOWN_V2,
                    reply_markup=keyboard
                )
        
        # === Priority Change ===
        elif action == CB_PRIORITY:
            sub = parts[1] if len(parts) > 1 else ""
            job_hash = parts[2] if len(parts) > 2 else None
            
            if sub == "select" and job_hash:
                await query.edit_message_text(
                    f"📊 *Change Priority*\n\nJob: `{job_hash[:16]}\\.\\.\\.`\n\nSelect new priority:",
                    parse_mode=ParseMode.MARKDOWN_V2,
                    reply_markup=build_priority_keyboard(job_hash)
                )
            elif sub == "set" and len(parts) >= 4:
                job_hash = parts[2]
                new_prio = parts[3]
                
                # Find current priority
                current_prio = None
                for p in PRIORITIES:
                    if s3.exists(f"{S3_PREFIX_TODO}/{p}/{job_hash}"):
                        current_prio = p
                        break
                
                if current_prio:
                    if current_prio != new_prio:
                        # Move job
                        content = s3.get_object(f"{S3_PREFIX_TODO}/{current_prio}/{job_hash}")
                        s3.put_object(f"{S3_PREFIX_TODO}/{new_prio}/{job_hash}", content)
                        s3.delete_object(f"{S3_PREFIX_TODO}/{current_prio}/{job_hash}")
                    
                    emoji = PRIORITY_EMOJI.get(new_prio, "⚪")
                    label = new_prio.split("_")[1].capitalize()
                    await query.answer(f"Priority changed to {label}", show_alert=True)
                
                # Show updated status
                message, status = format_job_status_message(s3, job_hash)
                keyboard = build_job_actions_keyboard(job_hash, status)
                await query.edit_message_text(
                    message,
                    parse_mode=ParseMode.MARKDOWN_V2,
                    reply_markup=keyboard
                )
        
        # === Retry ===
        elif action == CB_RETRY:
            sub = parts[1] if len(parts) > 1 else ""
            
            if sub == "all":
                queue_type = parts[2] if len(parts) > 2 else "failed"
                
                count = 0
                if queue_type == "failed":
                    failed_jobs = s3.list_failed()
                    for job in failed_jobs:
                        key = job['Key']
                        if key.endswith("/"):
                            continue
                        job_hash = key.split('/')[-1]
                        data = s3.get_object_json(key)
                        retry_count = data.get('retries', 0) + 1 if data else 0
                        marker = json.dumps({"retries": retry_count, "queued_at": int(time.time() * 1000)})
                        s3.put_object(f"{S3_PREFIX_TODO}/{DEFAULT_PRIORITY}/{job_hash}", marker)
                        s3.delete_object(key)
                        count += 1
                elif queue_type == "dead":
                    dead_jobs = s3.list_dead()
                    for job in dead_jobs:
                        key = job['Key']
                        if key.endswith("/"):
                            continue
                        job_hash = key.split('/')[-1]
                        marker = json.dumps({"retries": 0, "queued_at": int(time.time() * 1000)})
                        s3.put_object(f"{S3_PREFIX_TODO}/{DEFAULT_PRIORITY}/{job_hash}", marker)
                        s3.delete_object(key)
                        count += 1
                
                await query.answer(f"Retried {count} jobs", show_alert=True)
                
                # Refresh queue view
                message = format_queue_stats_message(s3)
                keyboard = InlineKeyboardMarkup([
                    [InlineKeyboardButton(f"{EMOJI['retry']} Retry All Failed", callback_data=f"{CB_RETRY}:all:failed")],
                    [build_refresh_button(f"{CB_QUEUE}:stats"), build_back_button()],
                ])
                await query.edit_message_text(
                    message,
                    parse_mode=ParseMode.MARKDOWN_V2,
                    reply_markup=keyboard
                )
            
            elif sub == "job" or sub == "dead":
                job_hash = parts[2] if len(parts) > 2 else None
                if job_hash:
                    if sub == "dead":
                        dead_key = f"{S3_PREFIX_DEAD}/{job_hash}"
                        if s3.exists(dead_key):
                            s3.delete_object(dead_key)
                    else:
                        failed_key = f"{S3_PREFIX_FAILED}/{job_hash}"
                        if s3.exists(failed_key):
                            s3.delete_object(failed_key)
                    
                    marker = json.dumps({"retries": 0, "queued_at": int(time.time() * 1000)})
                    s3.put_object(f"{S3_PREFIX_TODO}/{DEFAULT_PRIORITY}/{job_hash}", marker)
                    
                    await query.answer("Job queued for retry", show_alert=True)
                    
                    message, status = format_job_status_message(s3, job_hash)
                    keyboard = build_job_actions_keyboard(job_hash, status)
                    await query.edit_message_text(
                        message,
                        parse_mode=ParseMode.MARKDOWN_V2,
                        reply_markup=keyboard
                    )
        
        # === Cancel ===
        elif action == CB_CANCEL:
            sub = parts[1] if len(parts) > 1 else ""
            job_hash = parts[2] if len(parts) > 2 else None
            
            if sub == "confirm" and job_hash:
                await query.edit_message_text(
                    f"🚫 *Cancel Job?*\n\n"
                    f"Job: `{job_hash[:16]}\\.\\.\\.`\n\n"
                    f"This will move the job back to the queue\\.",
                    parse_mode=ParseMode.MARKDOWN_V2,
                    reply_markup=build_confirmation_keyboard("cancel_job", job_hash)
                )
        
        # === Logs ===
        elif action == CB_LOGS:
            job_hash = parts[1] if len(parts) > 1 else None
            
            if job_hash:
                logs = s3.list_job_logs(job_hash)
                
                if not logs:
                    # Try to get error from dead/failed
                    error_msg = None
                    if s3.exists(f"{S3_PREFIX_DEAD}/{job_hash}"):
                        data = s3.get_object_json(f"{S3_PREFIX_DEAD}/{job_hash}")
                        error_msg = data.get('error') if data else None
                    elif s3.exists(f"{S3_PREFIX_FAILED}/{job_hash}"):
                        data = s3.get_object_json(f"{S3_PREFIX_FAILED}/{job_hash}")
                        error_msg = data.get('error') if data else None
                    
                    if error_msg:
                        error_escaped = escape_markdown_v2(error_msg[:500])
                        message = f"📜 *Logs for* `{job_hash[:16]}\\.\\.\\.`\n\n*Error:*\n`{error_escaped}`"
                    else:
                        message = f"📜 *Logs for* `{job_hash[:16]}\\.\\.\\.`\n\n_No logs available_"
                else:
                    error_data = s3.get_job_error_detail(job_hash)
                    if error_data:
                        timestamp_escaped = escape_markdown_v2(str(error_data.get('timestamp', '?')))
                        error_escaped = escape_markdown_v2(truncate(error_data.get('error', '?'), 100))
                        message = f"📜 *Logs for* `{job_hash[:16]}\\.\\.\\.`\n\n"
                        message += f"*Timestamp:* `{timestamp_escaped}`\n"
                        message += f"*Error:* `{error_escaped}`\n"
                        
                        tb = error_data.get('traceback', '')
                        if tb and tb != 'NoneType: None\n':
                            tb_escaped = escape_markdown_v2(tb[:800])
                            message += f"\n*Traceback:*\n```\n{tb_escaped}\n```"
                    else:
                        logs_str = escape_markdown_v2(', '.join(logs))
                        message = f"📜 *Logs for* `{job_hash[:16]}\\.\\.\\.`\n\nAvailable: {logs_str}"
                
                await query.edit_message_text(
                    message,
                    parse_mode=ParseMode.MARKDOWN_V2,
                    reply_markup=InlineKeyboardMarkup([
                        [build_back_button(f"{CB_JOB}:status:{job_hash}")]
                    ])
                )
        
        # === Download ===
        elif action == CB_DOWNLOAD:
            job_hash = parts[1] if len(parts) > 1 else None
            
            if job_hash:
                done_key = f"{S3_PREFIX_DONE}/{job_hash}.zip"
                if s3.exists(done_key):
                    await query.answer("Preparing download...")
                    
                    # Download to temp file
                    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp:
                        s3.download_file(done_key, tmp.name)
                        
                        # Get original filename
                        entry = s3.lookup_by_hash(job_hash)
                        if entry and entry.get('paths'):
                            original = entry['paths'][0]
                            filename = os.path.splitext(os.path.basename(original))[0] + "_converted.zip"
                        else:
                            filename = f"{job_hash[:12]}.zip"
                        
                        await query.message.reply_document(
                            document=open(tmp.name, 'rb'),
                            filename=filename,
                            caption=f"✅ Converted output for `{job_hash[:16]}\\.\\.\\.`",
                            parse_mode=ParseMode.MARKDOWN_V2
                        )
                        
                        os.unlink(tmp.name)
                else:
                    await query.answer("Job output not found", show_alert=True)
        
        # === Preview ===
        elif action == CB_PREVIEW:
            job_hash = parts[1] if len(parts) > 1 else None
            
            if job_hash:
                done_key = f"{S3_PREFIX_DONE}/{job_hash}.zip"
                if s3.exists(done_key):
                    await query.answer("Loading preview...")
                    
                    import zipfile
                    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp:
                        s3.download_file(done_key, tmp.name)
                        
                        with zipfile.ZipFile(tmp.name, 'r') as zf:
                            files = zf.namelist()
                            
                            lines = [
                                f"👁️ *Preview*",
                                f"Job: `{job_hash[:16]}\\.\\.\\.`",
                                f"",
                                f"*Contents:*",
                            ]
                            
                            for f in files[:10]:
                                info = zf.getinfo(f)
                                f_escaped = escape_markdown_v2(f)
                                size_escaped = escape_markdown_v2(format_size(info.file_size))
                                lines.append(f"• `{f_escaped}` \\({size_escaped}\\)")
                            
                            # Show markdown preview
                            if 'content.md' in files:
                                with zf.open('content.md') as f:
                                    content = f.read().decode('utf-8')
                                    preview_lines = content.split('\n')[:20]
                                    preview = '\n'.join(preview_lines)
                                    # Escape for markdown
                                    preview = escape_markdown_v2(preview)
                                    lines.append(f"\n*Markdown Preview:*")
                                    lines.append(f"```\n{preview[:800]}\n```")
                                    
                                    if len(content.split('\n')) > 20:
                                        lines.append(f"_\\.\\.\\.{len(content.split(chr(10)))} total lines_")
                        
                        os.unlink(tmp.name)
                    
                    await query.edit_message_text(
                        "\n".join(lines),
                        parse_mode=ParseMode.MARKDOWN_V2,
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton(f"{EMOJI['download']} Download", callback_data=f"{CB_DOWNLOAD}:{job_hash}")],
                            [build_back_button(f"{CB_JOB}:status:{job_hash}")]
                        ])
                    )
                else:
                    await query.answer("Job output not found", show_alert=True)
        
        # === Confirmation ===
        elif action == CB_CONFIRM:
            sub = parts[1] if len(parts) > 1 else ""
            confirm_action = parts[2] if len(parts) > 2 else ""
            confirm_data = parts[3] if len(parts) > 3 else ""
            
            if sub == "ask":
                if confirm_action == "clear_dead":
                    dead_count = len([d for d in s3.list_dead() if not d['Key'].endswith("/")])
                    await query.edit_message_text(
                        f"⚠️ *Clear Dead\\-Letter Queue?*\n\n"
                        f"This will permanently delete {dead_count} failed jobs\\.\n"
                        f"This cannot be undone\\.",
                        parse_mode=ParseMode.MARKDOWN_V2,
                        reply_markup=build_confirmation_keyboard("clear_dead", "all")
                    )
            
            elif sub == "yes":
                if confirm_action == "janitor":
                    await query.answer("Running janitor...")
                    
                    # Run janitor
                    stats = janitor_module.run_janitor_quiet()
                    
                    await query.edit_message_text(
                        f"🧹 *Janitor Complete*\n\n"
                        f"• Recovered stale: `{stats.get('stale_recovered', 0)}`\n"
                        f"• Retried failed: `{stats.get('failed_retried', 0)}`\n"
                        f"• Moved to dead: `{stats.get('moved_to_dead', 0)}`",
                        parse_mode=ParseMode.MARKDOWN_V2,
                        reply_markup=InlineKeyboardMarkup([[build_back_button()]])
                    )
                
                elif confirm_action == "clear_dead":
                    dead_jobs = s3.list_dead()
                    count = 0
                    for job in dead_jobs:
                        if not job['Key'].endswith("/"):
                            s3.delete_object(job['Key'])
                            count += 1
                    
                    await query.answer(f"Cleared {count} jobs", show_alert=True)
                    await query.edit_message_text(
                        f"✅ *Dead\\-letter queue cleared*\n\nDeleted `{count}` jobs\\.",
                        parse_mode=ParseMode.MARKDOWN_V2,
                        reply_markup=InlineKeyboardMarkup([[build_back_button()]])
                    )
                
                elif confirm_action == "cancel_job":
                    job_hash = confirm_data
                    if s3.exists(f"{S3_PREFIX_PROCESSING}/{job_hash}"):
                        lock_data = s3.get_lock_info(job_hash)
                        priority = lock_data.get('priority', DEFAULT_PRIORITY) if lock_data else DEFAULT_PRIORITY
                        
                        s3.move_to_todo(job_hash, priority, increment_retry=False)
                        s3.release_lock(job_hash)
                        
                        await query.answer("Job cancelled", show_alert=True)
                    
                    message, status = format_job_status_message(s3, job_hash)
                    keyboard = build_job_actions_keyboard(job_hash, status)
                    await query.edit_message_text(
                        message,
                        parse_mode=ParseMode.MARKDOWN_V2,
                        reply_markup=keyboard
                    )
            
            elif sub == "no":
                # Go back to main menu or previous view
                await query.edit_message_text(
                    f"🤖 *BlobForge Bot*\n\nSelect an option:",
                    parse_mode=ParseMode.MARKDOWN_V2,
                    reply_markup=build_main_menu_keyboard()
                )
        
        # === Ingest (file upload) ===
        elif action == "ingest":
            sub = parts[1] if len(parts) > 1 else ""
            
            if sub == "prio":
                priority = parts[2] if len(parts) > 2 else DEFAULT_PRIORITY
                
                tmp_path = context.user_data.get('pending_file')
                filename = context.user_data.get('pending_filename')
                file_hash = context.user_data.get('pending_hash')
                
                if tmp_path and os.path.exists(tmp_path):
                    try:
                        # Ingest the file
                        ingestor.ingest([tmp_path], priority=priority, dry_run=False, original_name=filename)
                        
                        await query.answer("File ingested!", show_alert=True)
                        
                        message, status = format_job_status_message(s3, file_hash)
                        keyboard = build_job_actions_keyboard(file_hash, status)
                        await query.edit_message_text(
                            f"✅ *File Ingested*\n\n{message}",
                            parse_mode=ParseMode.MARKDOWN_V2,
                            reply_markup=keyboard
                        )
                    except Exception as e:
                        await query.answer(f"Error: {str(e)}", show_alert=True)
                    finally:
                        try:
                            os.unlink(tmp_path)
                        except:
                            pass
                        context.user_data.pop('pending_file', None)
                        context.user_data.pop('pending_filename', None)
                        context.user_data.pop('pending_hash', None)
                else:
                    await query.answer("File expired, please re-upload", show_alert=True)
            
            elif sub == "cancel":
                tmp_path = context.user_data.get('pending_file')
                if tmp_path:
                    try:
                        os.unlink(tmp_path)
                    except:
                        pass
                    context.user_data.pop('pending_file', None)
                    context.user_data.pop('pending_filename', None)
                    context.user_data.pop('pending_hash', None)
                
                await query.edit_message_text(
                    f"🤖 *BlobForge Bot*\n\nSelect an option:",
                    parse_mode=ParseMode.MARKDOWN_V2,
                    reply_markup=build_main_menu_keyboard()
                )
        
        # === No-op (for pagination info) ===
        elif action == "noop":
            await query.answer()
    
    except Exception as e:
        logger.exception(f"Error handling callback: {data}")
        await query.answer(f"Error: {str(e)}", show_alert=True)


# =============================================================================
# Text Message Handler (for search)
# =============================================================================

@authorized
async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle text messages (for search)."""
    if context.user_data.get('awaiting_search'):
        context.user_data['awaiting_search'] = False
        
        query = update.message.text.lower()
        await update.message.reply_chat_action(ChatAction.TYPING)
        s3 = S3Client()
        
        results = s3.search_manifest(query)
        
        if not results:
            await update.message.reply_text(
                f"🔍 No results found for: `{query}`",
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_markup=InlineKeyboardMarkup([[build_back_button()]])
            )
            return
        
        lines = [
            f"🔍 *Search Results*",
            f"Query: `{query}`",
            f"Found: `{len(results)}` matches",
            f"",
        ]
        
        for entry in results[:10]:
            job_hash = entry['hash']
            paths = entry.get('paths', [])
            filename = truncate(paths[0], 35) if paths else job_hash[:12]
            lines.append(f"• `{job_hash[:12]}...` \\- {escape_markdown_v2(filename)}")
        
        # Build keyboard with job selection
        keyboard = []
        for entry in results[:5]:
            job_hash = entry['hash']
            paths = entry.get('paths', [])
            filename = truncate(paths[0], 25) if paths else job_hash[:8]
            keyboard.append([
                InlineKeyboardButton(f"📄 {filename}", callback_data=f"{CB_JOB}:status:{job_hash}")
            ])
        
        keyboard.append([build_back_button()])
        
        await update.message.reply_text(
            "\n".join(lines),
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )


# =============================================================================
# Bot Application Setup
# =============================================================================

def create_bot(token: str) -> "Application":
    """Create and configure the Telegram bot application."""
    if not TELEGRAM_AVAILABLE:
        raise ImportError("python-telegram-bot not installed. Install with: pip install python-telegram-bot[job-queue]")
    
    app = Application.builder().token(token).build()
    
    # Command handlers
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("dashboard", cmd_dashboard))
    app.add_handler(CommandHandler("queue", cmd_queue))
    app.add_handler(CommandHandler("workers", cmd_workers))
    app.add_handler(CommandHandler("config", cmd_config))
    app.add_handler(CommandHandler("search", cmd_search))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("janitor", cmd_janitor))
    
    # Callback query handler
    app.add_handler(CallbackQueryHandler(handle_callback))
    
    # Document handler (for PDF uploads)
    app.add_handler(MessageHandler(filters.Document.PDF, handle_document))
    
    # Text message handler (for search)
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    
    return app


async def set_bot_commands(app: "Application"):
    """Set bot commands in Telegram."""
    commands = [
        BotCommand("start", "Show main menu"),
        BotCommand("dashboard", "View system dashboard"),
        BotCommand("queue", "Queue statistics"),
        BotCommand("workers", "List workers"),
        BotCommand("config", "View configuration"),
        BotCommand("search", "Search by filename"),
        BotCommand("status", "Check job status"),
        BotCommand("janitor", "Run janitor"),
        BotCommand("help", "Show help"),
    ]
    await app.bot.set_my_commands(commands)


def run_bot(token: str = None):
    """
    Run the Telegram bot.
    
    Args:
        token: Bot token from BotFather. If not provided, reads from BLOBFORGE_TELEGRAM_TOKEN env var.
    """
    if not TELEGRAM_AVAILABLE:
        print("Error: python-telegram-bot not installed.")
        print("Install with: pip install python-telegram-bot[job-queue]")
        print("Or: pip install blobforge[telegram]")
        return 1
    
    # Get token from env var if not provided
    if token is None:
        token = os.environ.get("BLOBFORGE_TELEGRAM_TOKEN")
    
    if not token:
        print("Error: No Telegram bot token provided.")
        print("Set the BLOBFORGE_TELEGRAM_TOKEN environment variable.")
        print("Get a token from @BotFather on Telegram.")
        return 1
    
    # Verify allowed users is configured
    allowed_users_str = os.environ.get("BLOBFORGE_TELEGRAM_ALLOWED_USERS", "")
    if not allowed_users_str:
        print("Warning: BLOBFORGE_TELEGRAM_ALLOWED_USERS not set.")
        print("The bot will reject all users. Set comma-separated user IDs to allow access.")
        print("Example: BLOBFORGE_TELEGRAM_ALLOWED_USERS=123456789,987654321")
        print("")
    
    app = create_bot(token)
    
    # Set commands on startup
    async def post_init(application: Application):
        await set_bot_commands(application)
        logger.info("Bot commands set")
    
    app.post_init = post_init
    
    logger.info("Starting BlobForge Telegram bot...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)
    
    return 0
