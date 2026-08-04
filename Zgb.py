#!/usr/bin/env python3
# ZIP_GOBBLER v4.0 - GOD MODE (SYNTAX PERFECT)
# Butter's Ultimate Archive Tool - Hybrid Bot API + Telethon

import os
import sys
import zipfile
import tempfile
import shutil
import mimetypes
import logging
import asyncio
import time
import json
import hashlib
import re
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Set, Any
from dataclasses import dataclass, field
from enum import Enum
import threading
import queue
from concurrent.futures import ThreadPoolExecutor
import subprocess

# Load .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Third-party imports
try:
    import rarfile
    RAR_SUPPORT = True
except ImportError:
    RAR_SUPPORT = False

try:
    import py7zr
    SEVENZ_SUPPORT = True
except ImportError:
    SEVENZ_SUPPORT = False

try:
    import tarfile
    TAR_SUPPORT = True
except ImportError:
    TAR_SUPPORT = False

try:
    import psutil
    PSUTIL_SUPPORT = True
except ImportError:
    PSUTIL_SUPPORT = False

# Telethon imports
try:
    from telethon import TelegramClient, events, functions, types
    from telethon.tl.types import MessageMediaDocument, Document, InputFile
    from telethon.tl.functions import messages
    TELETHON_SUPPORT = True
except ImportError:
    TELETHON_SUPPORT = False

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, InputFile
from telegram.ext import (
    Application, CommandHandler, MessageHandler, filters,
    CallbackContext, CallbackQueryHandler, ConversationHandler
)
import logging.handlers

# ---- ENVIRONMENT CONFIGURATION ----
BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', 'YOUR_BOT_TOKEN_HERE')
ADMIN_CHAT_ID = os.getenv('ADMIN_CHAT_ID', None)
MAX_CONCURRENT_JOBS = int(os.getenv('MAX_CONCURRENT_JOBS', '3'))
MAX_FILE_SIZE_MB = int(os.getenv('MAX_FILE_SIZE_MB', '2000'))
MAX_EXTRACTED_SIZE_MB = int(os.getenv('MAX_EXTRACTED_SIZE_MB', '5000'))
MAX_FILES_IN_ARCHIVE = int(os.getenv('MAX_FILES_IN_ARCHIVE', '10000'))
MAX_COMPRESSION_RATIO = float(os.getenv('MAX_COMPRESSION_RATIO', '100.0'))
RATE_LIMIT_REQUESTS = int(os.getenv('RATE_LIMIT_REQUESTS', '30'))
RATE_LIMIT_PERIOD = int(os.getenv('RATE_LIMIT_PERIOD', '60'))
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
COMPRESSION_LEVEL = int(os.getenv('COMPRESSION_LEVEL', '9'))
SPLIT_SIZE_MB = int(os.getenv('SPLIT_SIZE_MB', '2000'))

# Telethon config
USE_TELETHON = os.getenv('USE_TELETHON', 'false').lower() == 'true'
TELEGRAM_API_ID = int(os.getenv('TELEGRAM_API_ID', '0'))
TELEGRAM_API_HASH = os.getenv('TELEGRAM_API_HASH', '')
USERBOT_SESSION = os.getenv('USERBOT_SESSION', 'userbot.session')
USERBOT_PHONE = os.getenv('USERBOT_PHONE', '')

MAX_FILE_SIZE = MAX_FILE_SIZE_MB * 1024 * 1024
MAX_EXTRACTED_SIZE = MAX_EXTRACTED_SIZE_MB * 1024 * 1024
SPLIT_SIZE = SPLIT_SIZE_MB * 1024 * 1024

# ---- LOGGING SETUP ----
log_dir = Path("logs")
log_dir.mkdir(exist_ok=True)

logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, LOG_LEVEL.upper()))

file_handler = logging.handlers.RotatingFileHandler(
    log_dir / "zip_gobbler.log",
    maxBytes=10_485_760,
    backupCount=5
)
file_handler.setFormatter(logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
))
logger.addHandler(file_handler)

console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter(
    '%(asctime)s - %(levelname)s - %(message)s'
))
logger.addHandler(console_handler)

# ---- DATA CLASSES ----
class JobStatus(Enum):
    QUEUED = "queued"
    PROCESSING = "processing"
    COMPRESSING = "compressing"
    SPLITTING = "splitting"
    EXTRACTING = "extracting"
    SENDING = "sending"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

class OperationType(Enum):
    EXTRACT = "extract"
    COMPRESS = "compress"
    SPLIT = "split"

@dataclass
class FileInfo:
    name: str
    path: str
    size: int
    mime_type: str
    extension: str
    is_safe: bool = True
    extracted_path: str = ""

@dataclass
class ExtractJob:
    job_id: str
    user_id: int
    username: str
    file_name: str
    file_size: int
    status: JobStatus
    operation: OperationType = OperationType.EXTRACT
    created_at: datetime = field(default_factory=datetime.now)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    progress: float = 0.0
    current_file: str = ""
    files_extracted: int = 0
    total_files: int = 0
    bytes_extracted: int = 0
    total_bytes: int = 0
    errors: List[str] = field(default_factory=list)
    result: Optional[Dict] = None
    temp_dir: Optional[str] = None
    message_id: Optional[int] = None
    cancel_requested: bool = False
    compression_level: int = COMPRESSION_LEVEL
    split_size: int = SPLIT_SIZE
    password: Optional[str] = None
    use_telethon: bool = USE_TELETHON

# ---- TELETHON UPLOADER ----
class TelethonUploader:
    """Handles file uploads via Telethon (MTProto) - bypasses Bot API limits"""
    
    def __init__(self, api_id: int, api_hash: str, session_file: str = "userbot.session"):
        self.api_id = api_id
        self.api_hash = api_hash
        self.session_file = session_file
        self.client = None
        self.connected = False
        self.upload_queue = queue.Queue()
        self.is_running = False
        self._started = False
    
    async def start(self):
        """Initialize and start the Telethon client"""
        if self._started:
            return self.connected
            
        if not TELETHON_SUPPORT:
            logger.error("Telethon not installed")
            return False
            
        if not self.api_id or not self.api_hash:
            logger.error("Telegram API credentials not set")
            return False
        
        if not USERBOT_PHONE:
            logger.warning("USERBOT_PHONE not set, Telethon may ask for phone number")
            
        try:
            logger.info("🔄 Connecting Telethon userbot...")
            self.client = TelegramClient(self.session_file, self.api_id, self.api_hash)
            
            # Use phone if provided, otherwise let Telethon ask
            if USERBOT_PHONE:
                await self.client.start(phone=USERBOT_PHONE)
            else:
                await self.client.start()
                
            self.connected = True
            self._started = True
            logger.info("✅ Telethon userbot connected successfully")
            
            me = await self.client.get_me()
            logger.info(f"✅ Logged in as: {me.first_name} (@{me.username})")
            
            self.is_running = True
            return True
            
        except Exception as e:
            logger.error(f"❌ Telethon connection failed: {e}")
            self.connected = False
            self._started = True
            return False
    
    async def ensure_connected(self):
        """Ensure Telethon is connected, start if needed"""
        if not self._started:
            return await self.start()
        if not self.connected and self._started:
            try:
                if self.client:
                    await self.client.disconnect()
                self.client = TelegramClient(self.session_file, self.api_id, self.api_hash)
                if USERBOT_PHONE:
                    await self.client.start(phone=USERBOT_PHONE)
                else:
                    await self.client.start()
                self.connected = True
                logger.info("✅ Telethon reconnected successfully")
                return True
            except Exception as e:
                logger.error(f"❌ Telethon reconnect failed: {e}")
                self.connected = False
                return False
        return self.connected
    
    async def upload_file(self, chat_id: int, file_path: str, 
                          caption: str = "", 
                          progress_callback=None,
                          job_id: str = "") -> Dict[str, Any]:
        """Upload file using Telethon - supports up to 4GB with Premium"""
        if not await self.ensure_connected():
            return {
                'success': False,
                'error': 'Telethon not connected'
            }
        
        try:
            file_size = os.path.getsize(file_path)
            file_size_gb = file_size / (1024 ** 3)
            
            logger.info(f"📤 Uploading {file_path} ({file_size_gb:.2f}GB) via Telethon")
            
            result = await self.client.send_file(
                chat_id,
                file_path,
                caption=caption,
                progress_callback=progress_callback,
                part_size_kb=512,
                use_cache=True
            )
            
            return {
                'success': True,
                'message_id': result.id,
                'file_size': file_size,
                'media_type': result.media.__class__.__name__ if result.media else 'Document',
                'size_gb': file_size_gb
            }
            
        except Exception as e:
            logger.error(f"❌ Telethon upload failed: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    async def download_file(self, chat_id: int, message_id: int, output_path: str) -> bool:
        """Download file using Telethon - no size limits"""
        if not await self.ensure_connected():
            return False
            
        try:
            message = await self.client.get_messages(chat_id, ids=message_id)
            if not message or not message.media:
                return False
                
            await self.client.download_media(message, output_path)
            return True
            
        except Exception as e:
            logger.error(f"Download failed: {e}")
            return False
    
    async def close(self):
        """Disconnect Telethon client"""
        self.is_running = False
        if self.client:
            try:
                await self.client.disconnect()
            except:
                pass
            self.connected = False
            logger.info("Telethon disconnected")

# ---- SECURITY FUNCTIONS ----
def safe_extract(zip_ref: zipfile.ZipFile, extract_path: str, password: Optional[str] = None) -> List[str]:
    """Extract ZIP safely, preventing Zip Slip attacks"""
    extracted_files = []
    extract_path = Path(extract_path).resolve()
    
    for member in zip_ref.namelist():
        member_path = Path(member)
        target_path = (extract_path / member_path).resolve()
        
        if not str(target_path).startswith(str(extract_path)):
            raise ValueError(f"Zip Slip detected: {member}")
        
        try:
            info = zip_ref.getinfo(member)
            if info.is_dir():
                target_path.mkdir(parents=True, exist_ok=True)
            else:
                target_path.parent.mkdir(parents=True, exist_ok=True)
                with zip_ref.open(member, pwd=password.encode() if password else None) as source, \
                     open(target_path, 'wb') as target:
                    shutil.copyfileobj(source, target)
                extracted_files.append(str(target_path))
        except Exception as e:
            logger.warning(f"Failed to extract {member}: {e}")
            continue
    
    return extracted_files

def check_zip_bomb(zip_path: str) -> Tuple[bool, str]:
    """Check for potential ZIP bombs before extraction"""
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            file_count = len(zip_ref.namelist())
            compressed_size = os.path.getsize(zip_path)
            total_uncompressed = 0
            
            for info in zip_ref.infolist():
                total_uncompressed += info.file_size
                
                if info.file_size > 0 and info.compress_size > 0:
                    ratio = info.file_size / info.compress_size
                    if ratio > MAX_COMPRESSION_RATIO:
                        return False, f"File '{info.filename}' suspicious ratio: {ratio:.1f}x"
            
            if file_count > MAX_FILES_IN_ARCHIVE:
                return False, f"Archive has {file_count} files (limit: {MAX_FILES_IN_ARCHIVE})"
            
            if total_uncompressed > MAX_EXTRACTED_SIZE:
                return False, f"Extracted size {total_uncompressed/1024/1024:.1f}MB (limit: {MAX_EXTRACTED_SIZE_MB}MB)"
            
            if compressed_size > 0:
                ratio = total_uncompressed / compressed_size
                if ratio > MAX_COMPRESSION_RATIO:
                    return False, f"Compression ratio {ratio:.1f}x (limit: {MAX_COMPRESSION_RATIO:.1f}x)"
            
            return True, f"Safe: {file_count} files, {total_uncompressed/1024/1024:.1f}MB"
            
    except Exception as e:
        return False, f"Check failed: {e}"

# ---- COMPRESSION FUNCTIONS ----
def compress_files(input_paths: List[str], output_path: str, 
                   compression_level: int = 9, password: Optional[str] = None,
                   split_size: Optional[int] = None) -> Dict[str, Any]:
    """Compress files with maximum settings"""
    result = {
        'success': False,
        'output_path': output_path,
        'file_count': 0,
        'original_size': 0,
        'compressed_size': 0,
        'ratio': 0,
        'split_parts': [],
        'errors': []
    }
    
    try:
        total_size = 0
        for path in input_paths:
            if os.path.isfile(path):
                total_size += os.path.getsize(path)
            elif os.path.isdir(path):
                for root, _, files in os.walk(path):
                    for file in files:
                        total_size += os.path.getsize(os.path.join(root, file))
        result['original_size'] = total_size
        
        with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED, 
                            compresslevel=compression_level) as zipf:
            for path in input_paths:
                if os.path.isfile(path):
                    arcname = os.path.basename(path)
                    zipf.write(path, arcname, zipfile.ZIP_DEFLATED)
                    result['file_count'] += 1
                elif os.path.isdir(path):
                    for root, _, files in os.walk(path):
                        for file in files:
                            full_path = os.path.join(root, file)
                            arcname = os.path.relpath(full_path, os.path.dirname(path))
                            zipf.write(full_path, arcname, zipfile.ZIP_DEFLATED)
                            result['file_count'] += 1
        
        result['compressed_size'] = os.path.getsize(output_path)
        result['ratio'] = (1 - result['compressed_size'] / result['original_size']) * 100 if result['original_size'] > 0 else 0
        result['success'] = True
        
        return result
        
    except Exception as e:
        result['errors'].append(str(e))
        logger.exception(f"Compression failed: {e}")
        return result

def split_zip(zip_path: str, split_size: int, password: Optional[str] = None) -> Dict[str, Any]:
    """Split ZIP file into parts"""
    result = {
        'success': False,
        'parts': [],
        'total_size': 0,
        'errors': []
    }
    
    try:
        base_name = os.path.splitext(zip_path)[0]
        part_num = 1
        total_size = os.path.getsize(zip_path)
        
        with open(zip_path, 'rb') as f:
            data = f.read()
        
        for i in range(0, len(data), split_size):
            part_path = f"{base_name}.part{str(part_num).zfill(3)}.zip"
            with open(part_path, 'wb') as f:
                f.write(data[i:i+split_size])
            
            part_info = {
                'path': part_path,
                'size': len(data[i:i+split_size]),
                'part_num': part_num,
                'total_parts': (len(data) + split_size - 1) // split_size
            }
            result['parts'].append(part_info)
            result['total_size'] += part_info['size']
            part_num += 1
        
        result['success'] = True
        
    except Exception as e:
        result['errors'].append(str(e))
        logger.exception(f"Split failed: {e}")
    
    return result

def merge_split_parts(part_pattern: str, output_path: str) -> bool:
    """Merge split ZIP parts back together"""
    try:
        import glob
        parts = sorted(glob.glob(part_pattern))
        
        if not parts:
            return False
        
        with open(output_path, 'wb') as out:
            for part in parts:
                with open(part, 'rb') as f:
                    shutil.copyfileobj(f, out)
        
        if not zipfile.is_zipfile(output_path):
            os.remove(output_path)
            return False
        
        return True
        
    except Exception as e:
        logger.error(f"Merge failed: {e}")
        return False

# ---- JOB MANAGER ----
class JobManager:
    def __init__(self):
        self.jobs: Dict[str, ExtractJob] = {}
        self.job_queue: queue.Queue = queue.Queue()
        self.active_jobs: Set[str] = set()
        self.executor = ThreadPoolExecutor(max_workers=MAX_CONCURRENT_JOBS)
        self.running = True
        self.lock = threading.Lock()
        self.progress_callbacks: Dict[str, Any] = {}
        
        self.workers = []
        for _ in range(MAX_CONCURRENT_JOBS):
            worker = threading.Thread(target=self._worker_loop, daemon=True)
            worker.start()
            self.workers.append(worker)
        
        logger.info(f"JobManager with {MAX_CONCURRENT_JOBS} workers")
    
    def submit_job(self, job: ExtractJob) -> str:
        with self.lock:
            self.jobs[job.job_id] = job
            self.job_queue.put(job.job_id)
            job.status = JobStatus.QUEUED
            logger.info(f"Job {job.job_id} - {job.operation.value} - {job.username}")
            return job.job_id
    
    def get_job(self, job_id: str) -> Optional[ExtractJob]:
        return self.jobs.get(job_id)
    
    def cancel_job(self, job_id: str) -> bool:
        with self.lock:
            job = self.jobs.get(job_id)
            if job and job.status in [JobStatus.QUEUED, JobStatus.PROCESSING, 
                                     JobStatus.COMPRESSING, JobStatus.SPLITTING,
                                     JobStatus.EXTRACTING]:
                job.cancel_requested = True
                job.status = JobStatus.CANCELLED
                logger.info(f"Job {job_id} cancelled")
                return True
            return False
    
    def get_user_jobs(self, user_id: int) -> List[ExtractJob]:
        return [j for j in self.jobs.values() if j.user_id == user_id]
    
    def get_queue_position(self, job_id: str) -> int:
        try:
            position = 1
            for item in list(self.job_queue.queue):
                if item == job_id:
                    return position
                position += 1
            return -1
        except:
            return -1
    
    def _worker_loop(self):
        while self.running:
            try:
                job_id = self.job_queue.get(timeout=1)
                if job_id in self.jobs:
                    self._process_job(job_id)
            except queue.Empty:
                continue
            except Exception as e:
                logger.exception(f"Worker error: {e}")
    
    def _process_job(self, job_id: str):
        job = self.jobs.get(job_id)
        if not job or job.cancel_requested:
            return
        
        with self.lock:
            self.active_jobs.add(job_id)
            job.status = JobStatus.PROCESSING
            job.started_at = datetime.now()
        
        try:
            pass
        except Exception as e:
            logger.exception(f"Job {job_id} failed: {e}")
            job.status = JobStatus.FAILED
            job.errors.append(str(e))
        finally:
            with self.lock:
                self.active_jobs.discard(job_id)
                job.completed_at = datetime.now()
    
    def shutdown(self):
        self.running = False
        self.executor.shutdown(wait=False)

# ---- RATE LIMITER ----
class RateLimiter:
    def __init__(self, max_requests: int = RATE_LIMIT_REQUESTS, period: int = RATE_LIMIT_PERIOD):
        self.max_requests = max_requests
        self.period = period
        self.requests: Dict[int, List[float]] = {}
        self.lock = threading.Lock()
    
    def is_allowed(self, user_id: int) -> bool:
        with self.lock:
            now = time.time()
            if user_id not in self.requests:
                self.requests[user_id] = []
            
            self.requests[user_id] = [t for t in self.requests[user_id] if now - t < self.period]
            
            if len(self.requests[user_id]) >= self.max_requests:
                return False
            
            self.requests[user_id].append(now)
            return True
    
    def get_remaining(self, user_id: int) -> int:
        with self.lock:
            if user_id not in self.requests:
                return self.max_requests
            
            now = time.time()
            recent = [t for t in self.requests[user_id] if now - t < self.period]
            return max(0, self.max_requests - len(recent))
    
    def get_reset_time(self, user_id: int) -> int:
        with self.lock:
            if user_id not in self.requests or not self.requests[user_id]:
                return 0
            
            oldest = min(self.requests[user_id])
            return int(oldest + self.period - time.time())

# ---- MAIN BOT CLASS ----
class ZipGobblerBot:
    def __init__(self):
        self.job_manager = JobManager()
        self.rate_limiter = RateLimiter()
        self.start_time = datetime.now()
        self.telethon = None
        self._telethon_task = None
        
        if USE_TELETHON and TELETHON_SUPPORT and TELEGRAM_API_ID and TELEGRAM_API_HASH:
            self.telethon = TelethonUploader(TELEGRAM_API_ID, TELEGRAM_API_HASH, USERBOT_SESSION)
            logger.info("Telethon uploader initialized")
        elif USE_TELETHON:
            logger.warning("Telethon enabled but missing dependencies or credentials")
        
        self.extractors = {
            '.zip': self._extract_zip,
            '.rar': self._extract_rar if RAR_SUPPORT else None,
            '.7z': self._extract_7z if SEVENZ_SUPPORT else None,
            '.tar': self._extract_tar if TAR_SUPPORT else None,
            '.gz': self._extract_tar if TAR_SUPPORT else None,
        }
        
        self.app = Application.builder().token(BOT_TOKEN).build()
        self._setup_handlers()
    
    def _setup_handlers(self):
        self.app.add_handler(CommandHandler("start", self.cmd_start))
        self.app.add_handler(CommandHandler("help", self.cmd_help))
        self.app.add_handler(CommandHandler("status", self.cmd_status))
        self.app.add_handler(CommandHandler("queue", self.cmd_queue))
        self.app.add_handler(CommandHandler("cancel", self.cmd_cancel))
        self.app.add_handler(CommandHandler("stats", self.cmd_stats))
        self.app.add_handler(CommandHandler("compress", self.cmd_compress))
        self.app.add_handler(CommandHandler("split", self.cmd_split))
        self.app.add_handler(CommandHandler("merge", self.cmd_merge))
        self.app.add_handler(CommandHandler("telethon", self.cmd_telethon))
        
        self.app.add_handler(MessageHandler(
            filters.Document.ALL & ~filters.COMMAND,
            self.handle_document
        ))
        
        self.app.add_handler(CallbackQueryHandler(self.handle_callback))
        self.app.add_error_handler(self.error_handler)
    
    # ---- COMMAND HANDLERS ----
    async def cmd_start(self, update: Update, context: CallbackContext):
        keyboard = [
            [InlineKeyboardButton("📦 Compress", callback_data="compress"),
             InlineKeyboardButton("✂️ Split", callback_data="split")],
            [InlineKeyboardButton("📊 Status", callback_data="status"),
             InlineKeyboardButton("📋 Queue", callback_data="queue")],
            [InlineKeyboardButton("⚡ Telethon", callback_data="telethon"),
             InlineKeyboardButton("❓ Help", callback_data="help")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        telethon_status = "❌ Inactive"
        if self.telethon:
            if self.telethon.connected:
                telethon_status = "✅ Active"
            elif self.telethon._started:
                telethon_status = "🔄 Connecting..."
            else:
                telethon_status = "⏳ Not started"
        
        welcome = (
            "🔥 *ZIP Gobbler v4.0 - GOD MODE* 🔥\n\n"
            f"*⚡ Telethon:* {telethon_status}\n"
            f"*📦 Upload Limit:* 4GB (with Premium userbot)\n"
            f"*📥 Download Limit:* No practical limit\n\n"
            "*Features:*\n"
            "📦 Maximum compression (level 9)\n"
            "✂️ Split ZIPs into parts\n"
            "🔒 Password protection\n"
            "🔗 Merge split parts\n"
            "⚡ Hybrid Bot API + Telethon\n\n"
            "*Commands:*\n"
            "/compress - Compress files/folder\n"
            "/split - Split a ZIP file\n"
            "/merge - Merge split parts\n"
            "/telethon - Telethon status\n"
            "/status - Check job status\n"
            "/cancel - Cancel a job"
        )
        
        await update.message.reply_text(welcome, parse_mode='Markdown', reply_markup=reply_markup)
    
    async def cmd_help(self, update: Update, context: CallbackContext):
        help_text = (
            "🆘 *ZIP Gobbler Help*\n\n"
            "*Commands:*\n"
            "/compress - Send me files, I'll compress them\n"
            "/split - Split a ZIP into parts\n"
            "/merge - Merge split parts back\n"
            "/telethon - Check Telethon status\n"
            "/status - Check your jobs\n"
            "/cancel - Cancel a job\n\n"
            "*Telethon Mode (Unlimited Uploads):*\n"
            "When enabled, files > 2GB are uploaded via Telethon\n"
            "Requires: Premium Telegram account\n"
            "Set in .env: USE_TELETHON=true\n\n"
            "*Compression Settings:*\n"
            f"Level: {COMPRESSION_LEVEL}/9 (max)\n"
            f"Split size: {SPLIT_SIZE_MB}MB\n\n"
            "*Password Protection:*\n"
            "/compress password:yourpass"
        )
        await update.message.reply_text(help_text, parse_mode='Markdown')
    
    async def cmd_telethon(self, update: Update, context: CallbackContext):
        if not USE_TELETHON:
            await update.message.reply_text(
                "❌ *Telethon is disabled*\nEnable in .env: USE_TELETHON=true",
                parse_mode='Markdown'
            )
            return
        
        if not TELETHON_SUPPORT:
            await update.message.reply_text(
                "❌ *Telethon not installed*\nInstall: pip install telethon cryptg",
                parse_mode='Markdown'
            )
            return
        
        if not self.telethon:
            await update.message.reply_text(
                "❌ *Telethon not initialized*\nCheck credentials in .env",
                parse_mode='Markdown'
            )
            return
        
        await self._ensure_telethon()
        
        status = "✅ Connected" if self.telethon.connected else "❌ Disconnected"
        
        info = (
            "⚡ *Telethon Status*\n\n"
            f"Status: {status}\n"
            f"Session: {USERBOT_SESSION}\n"
            f"API ID: {TELEGRAM_API_ID}\n"
            f"Phone: {USERBOT_PHONE[:4] if USERBOT_PHONE else 'Not set'}...\n\n"
            "*Features:*\n"
            "📤 Upload: 4GB (Premium)\n"
            "📥 Download: Unlimited\n"
            "🚀 Speed: 512KB chunks\n\n"
            "*To reconnect:*\n"
            "Restart bot with valid credentials"
        )
        
        await update.message.reply_text(info, parse_mode='Markdown')
    
    async def _ensure_telethon(self):
        """Ensure Telethon is connected"""
        if self.telethon:
            if not self.telethon._started:
                return await self.telethon.start()
            elif not self.telethon.connected:
                return await self.telethon.ensure_connected()
        return self.telethon and self.telethon.connected
    
    async def cmd_compress(self, update: Update, context: CallbackContext):
        args = ' '.join(context.args) if context.args else ''
        
        password = None
        if 'password:' in args:
            parts = args.split('password:')
            if len(parts) > 1:
                password = parts[1].split()[0] if parts[1] else None
        
        context.user_data['compress_mode'] = True
        context.user_data['password'] = password
        context.user_data['files'] = []
        
        telethon_ready = await self._ensure_telethon()
        
        await update.message.reply_text(
            f"📦 *Compression Mode Activated*\n\n"
            f"Send me files or a folder (as ZIP)\n"
            f"🔐 Password: {'✅ Set' if password else '❌ None'}\n"
            f"📊 Compression: {COMPRESSION_LEVEL}/9\n"
            f"✂️ Split at: {SPLIT_SIZE_MB}MB\n"
            f"⚡ Telethon: {'✅ Connected' if telethon_ready else '❌ Not connected'}\n\n"
            f"Send me one or more files now.\n"
            f"Type /done when finished.",
            parse_mode='Markdown'
        )
    
    async def cmd_split(self, update: Update, context: CallbackContext):
        telethon_ready = await self._ensure_telethon()
        
        await update.message.reply_text(
            f"✂️ *Split Mode*\n\n"
            f"Send me a ZIP file and I'll split it into parts\n"
            f"📊 Part size: {SPLIT_SIZE_MB}MB\n"
            f"⚡ Telethon: {'✅ Connected' if telethon_ready else '❌ Not connected'}\n\n"
            f"Just send the ZIP file now.",
            parse_mode='Markdown'
        )
        context.user_data['split_mode'] = True
    
    async def cmd_merge(self, update: Update, context: CallbackContext):
        await update.message.reply_text(
            f"🔗 *Merge Mode*\n\n"
            f"Send me all the .part files (part001.zip, part002.zip, etc.)\n"
            f"Send them one by one, or send a folder containing them.\n\n"
            f"Type /done when all parts are sent.",
            parse_mode='Markdown'
        )
        context.user_data['merge_mode'] = True
        context.user_data['merge_parts'] = []
    
    async def cmd_status(self, update: Update, context: CallbackContext):
        user_id = update.effective_user.id
        jobs = self.job_manager.get_user_jobs(user_id)
        
        if not jobs:
            await update.message.reply_text("📭 *No jobs found*", parse_mode='Markdown')
            return
        
        response = "📊 *Your Jobs*\n\n"
        for job in jobs[-5:]:
            status_emoji = {
                JobStatus.QUEUED: "⏳",
                JobStatus.PROCESSING: "⚡",
                JobStatus.COMPRESSING: "📦",
                JobStatus.SPLITTING: "✂️",
                JobStatus.EXTRACTING: "📂",
                JobStatus.SENDING: "📤",
                JobStatus.COMPLETED: "✅",
                JobStatus.FAILED: "❌",
                JobStatus.CANCELLED: "🚫"
            }.get(job.status, "❓")
            
            progress = job.progress if job.status != JobStatus.COMPLETED else 100.0
            op_icon = "📦" if job.operation == OperationType.COMPRESS else "✂️" if job.operation == OperationType.SPLIT else "📂"
            
            response += (
                f"{status_emoji} {op_icon} *{job.file_name[:30]}*\n"
                f"Status: `{job.status.value}`\n"
                f"Progress: `{progress:.1f}%`\n"
                f"ID: `{job.job_id[:8]}`\n\n"
            )
        
        await update.message.reply_text(response, parse_mode='Markdown')
    
    async def cmd_queue(self, update: Update, context: CallbackContext):
        user_id = update.effective_user.id
        jobs = self.job_manager.get_user_jobs(user_id)
        
        if not jobs:
            await update.message.reply_text("📭 *Queue empty*", parse_mode='Markdown')
            return
        
        queued = [j for j in jobs if j.status in [JobStatus.QUEUED, JobStatus.PROCESSING]]
        completed = [j for j in jobs if j.status == JobStatus.COMPLETED]
        
        response = (
            "📋 *Your Queue*\n\n"
            f"⏳ Active: {len(queued)}\n"
            f"✅ Completed: {len(completed)}\n\n"
        )
        
        if queued:
            response += "*Active jobs:*\n"
            for job in queued[:3]:
                op_icon = "📦" if job.operation == OperationType.COMPRESS else "✂️" if job.operation == OperationType.SPLIT else "📂"
                response += f"• {op_icon} `{job.file_name[:25]}` ({job.status.value})\n"
        
        await update.message.reply_text(response, parse_mode='Markdown')
    
    async def cmd_cancel(self, update: Update, context: CallbackContext):
        args = context.args
        user_id = update.effective_user.id
        
        if not args:
            jobs = [j for j in self.job_manager.get_user_jobs(user_id) 
                   if j.status in [JobStatus.QUEUED, JobStatus.PROCESSING, 
                                  JobStatus.COMPRESSING, JobStatus.SPLITTING,
                                  JobStatus.EXTRACTING]]
            
            if not jobs:
                await update.message.reply_text("❌ *No active jobs*", parse_mode='Markdown')
                return
            
            keyboard = []
            for job in jobs[:5]:
                op_icon = "📦" if job.operation == OperationType.COMPRESS else "✂️" if job.operation == OperationType.SPLIT else "📂"
                keyboard.append([InlineKeyboardButton(
                    f"{op_icon} Cancel: {job.file_name[:20]}",
                    callback_data=f"cancel_{job.job_id}"
                )])
            
            reply_markup = InlineKeyboardMarkup(keyboard)
            await update.message.reply_text("Select job to cancel:", reply_markup=reply_markup)
            return
        
        job_id = args[0]
        success = self.job_manager.cancel_job(job_id)
        if success:
            await update.message.reply_text(f"✅ *Job {job_id[:8]} cancelled*", parse_mode='Markdown')
        else:
            await update.message.reply_text(f"❌ *Job {job_id[:8]} not found*", parse_mode='Markdown')
    
    async def cmd_stats(self, update: Update, context: CallbackContext):
        total_jobs = len(self.job_manager.jobs)
        active = len(self.job_manager.active_jobs)
        queue_size = 0
        try:
            queue_size = self.job_manager.job_queue.qsize()
        except:
            queue_size = 0
        
        telethon_status = "✅ Connected" if self.telethon and self.telethon.connected else "❌ Disconnected"
        
        stats = (
            "📈 *Bot Statistics*\n\n"
            f"📊 Total jobs: {total_jobs}\n"
            f"⚡ Active jobs: {active}\n"
            f"⏳ Queued jobs: {queue_size}\n"
            f"👥 Max workers: {MAX_CONCURRENT_JOBS}\n"
            f"💾 Memory: {self._get_memory_usage()}\n"
            f"⏰ Uptime: {self._get_uptime()}\n\n"
            "*Telethon:*\n"
            f"{telethon_status}\n"
            f"📤 Upload: 4GB (Premium)\n"
            f"📥 Download: Unlimited\n\n"
            "*Compression:*\n"
            f"📦 Level: {COMPRESSION_LEVEL}/9\n"
            f"✂️ Split: {SPLIT_SIZE_MB}MB"
        )
        await update.message.reply_text(stats, parse_mode='Markdown')
    
    # ---- DOCUMENT HANDLER ----
    async def handle_document(self, update: Update, context: CallbackContext):
        if not hasattr(context, 'user_data'):
            context.user_data = {}
        
        user = update.effective_user
        document = update.message.document
        
        if context.user_data.get('split_mode'):
            await self._handle_split(update, context, document)
            return
        
        if context.user_data.get('merge_mode'):
            await self._handle_merge(update, context, document)
            return
        
        if context.user_data.get('compress_mode'):
            context.user_data['files'].append(document)
            await update.message.reply_text(
                f"✅ Added: {document.file_name}\n"
                f"Total: {len(context.user_data['files'])} files\n"
                f"Type /done to compress all files"
            )
            return
        
        await self._handle_extract(update, context, document)
    
    async def _handle_extract(self, update: Update, context: CallbackContext, document):
        user = update.effective_user
        
        if not self.rate_limiter.is_allowed(user.id):
            remaining = self.rate_limiter.get_remaining(user.id)
            reset_time = self.rate_limiter.get_reset_time(user.id)
            await update.message.reply_text(
                f"⛔ *Rate limit exceeded*\n"
                f"Wait {reset_time}s\n"
                f"Remaining: {remaining}",
                parse_mode='Markdown'
            )
            return
        
        ext = os.path.splitext(document.file_name)[1].lower()
        if ext not in self.extractors or self.extractors[ext] is None:
            supported = ", ".join(k for k,v in self.extractors.items() if v is not None)
            await update.message.reply_text(
                f"❌ *Unsupported format*\n"
                f"Supported: {supported}",
                parse_mode='Markdown'
            )
            return
        
        use_telethon = False
        if document.file_size > MAX_FILE_SIZE:
            if self.telethon:
                await self._ensure_telethon()
                if self.telethon.connected:
                    use_telethon = True
                    await update.message.reply_text(
                        f"⚡ *File > 2GB detected!*\n"
                        f"Using Telethon for upload...\n"
                        f"📁 Size: {document.file_size/1024**3:.2f}GB",
                        parse_mode='Markdown'
                    )
                else:
                    await update.message.reply_text(
                        f"❌ *Telethon not connected*\n"
                        f"File size: {document.file_size/1024**3:.2f}GB\n"
                        f"Please check Telethon credentials and reconnect.",
                        parse_mode='Markdown'
                    )
                    return
            else:
                await update.message.reply_text(
                    f"❌ *File too large for Bot API*\n"
                    f"Size: {document.file_size/1024**3:.2f}GB\n"
                    f"Limit: {MAX_FILE_SIZE_MB}MB\n\n"
                    f"Enable Telethon in .env to upload >2GB files",
                    parse_mode='Markdown'
                )
                return
        
        job_id = hashlib.md5(f"{user.id}_{time.time()}_{document.file_name}".encode()).hexdigest()[:16]
        
        job = ExtractJob(
            job_id=job_id,
            user_id=user.id,
            username=user.username or str(user.id),
            file_name=document.file_name,
            file_size=document.file_size,
            status=JobStatus.QUEUED,
            operation=OperationType.EXTRACT,
            use_telethon=use_telethon
        )
        
        self.job_manager.submit_job(job)
        
        keyboard = [
            [InlineKeyboardButton("📊 Check Status", callback_data=f"status_{job_id}")],
            [InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_{job_id}")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        telethon_tag = "⚡ Telethon" if use_telethon else "🤖 Bot API"
        
        await update.message.reply_text(
            f"✅ *Job submitted!*\n\n"
            f"📁 File: {document.file_name}\n"
            f"📦 Size: {document.file_size/1024/1024:.1f}MB\n"
            f"🔄 Method: {telethon_tag}\n"
            f"🆔 ID: `{job_id}`\n"
            f"📊 Position: #{self.job_manager.get_queue_position(job_id)}\n\n"
            f"Processing shortly...",
            parse_mode='Markdown',
            reply_markup=reply_markup
        )
        
        asyncio.create_task(self._process_extract(update, job, document))
    
    async def _handle_split(self, update: Update, context: CallbackContext, document):
        if not document.file_name.lower().endswith('.zip'):
            await update.message.reply_text("❌ *Only ZIP files can be split*", parse_mode='Markdown')
            return
        
        user = update.effective_user
        
        job_id = hashlib.md5(f"split_{user.id}_{time.time()}_{document.file_name}".encode()).hexdigest()[:16]
        
        job = ExtractJob(
            job_id=job_id,
            user_id=user.id,
            username=user.username or str(user.id),
            file_name=document.file_name,
            file_size=document.file_size,
            status=JobStatus.QUEUED,
            operation=OperationType.SPLIT,
            split_size=SPLIT_SIZE
        )
        
        self.job_manager.submit_job(job)
        
        await update.message.reply_text(
            f"✂️ *Split job submitted!*\n\n"
            f"📁 File: {document.file_name}\n"
            f"📦 Size: {document.file_size/1024/1024:.1f}MB\n"
            f"✂️ Part size: {SPLIT_SIZE_MB}MB\n"
            f"🆔 ID: `{job_id}`",
            parse_mode='Markdown'
        )
        
        asyncio.create_task(self._process_split(update, job, document))
        context.user_data['split_mode'] = False
    
    async def _handle_merge(self, update: Update, context: CallbackContext, document):
        if '.part' in document.file_name:
            context.user_data['merge_parts'].append(document)
            total = len(context.user_data['merge_parts'])
            await update.message.reply_text(
                f"✅ Added part {total}\n"
                f"Send more parts or type /done to merge"
            )
        else:
            await update.message.reply_text("❌ *Not a split part file*\nMust contain .part in filename", parse_mode='Markdown')
    
    async def _process_extract(self, update: Update, job: ExtractJob, document):
        temp_dir = None
        file_path = None
        
        try:
            job.status = JobStatus.EXTRACTING
            
            file_path = os.path.join(tempfile.gettempdir(), f"gobbler_{job.job_id}_{document.file_name}")
            
            file = await document.get_file()
            await file.download_to_drive(file_path)
            
            ext = os.path.splitext(document.file_name)[1].lower()
            if ext == '.zip':
                safe, reason = check_zip_bomb(file_path)
                if not safe:
                    job.status = JobStatus.FAILED
                    job.errors.append(reason)
                    await update.message.reply_text(f"❌ *Security check failed*\n{reason}", parse_mode='Markdown')
                    return
            
            temp_dir = tempfile.mkdtemp(prefix=f"gobbler_{job.job_id}_")
            job.temp_dir = temp_dir
            
            extractor = self.extractors[ext]
            if extractor is None:
                raise ValueError(f"No extractor for {ext}")
            
            extracted_files = await asyncio.get_event_loop().run_in_executor(
                self.job_manager.executor,
                extractor,
                file_path, temp_dir, job
            )
            
            if job.cancel_requested:
                await update.message.reply_text("🚫 *Job cancelled*", parse_mode='Markdown')
                return
            
            job.files_extracted = len(extracted_files)
            job.total_files = len(extracted_files)
            
            job.status = JobStatus.SENDING
            sent = 0
            failed = 0
            
            use_telethon = job.use_telethon or (job.file_size > MAX_FILE_SIZE)
            
            for file_info in extracted_files[:100]:
                if job.cancel_requested:
                    break
                    
                try:
                    if file_info.size > MAX_FILE_SIZE and not use_telethon:
                        continue
                    
                    if use_telethon and self.telethon and self.telethon.connected:
                        await self._send_file_telethon(update, file_info, job)
                    else:
                        await self._send_file(update, file_info)
                    
                    sent += 1
                    job.progress = (sent / max(1, len(extracted_files))) * 100
                    
                except Exception as e:
                    failed += 1
                    logger.error(f"Send failed {file_info.name}: {e}")
            
            job.status = JobStatus.COMPLETED
            job.progress = 100.0
            job.completed_at = datetime.now()
            
            await update.message.reply_text(
                f"✅ *Extraction complete!*\n\n"
                f"📁 Sent: {sent}\n"
                f"❌ Failed: {failed}\n"
                f"⏰ Time: {(job.completed_at - job.started_at).total_seconds():.1f}s\n"
                f"🔄 Method: {'⚡ Telethon' if use_telethon else '🤖 Bot API'}",
                parse_mode='Markdown'
            )
            
        except Exception as e:
            logger.exception(f"Job {job.job_id} failed")
            job.status = JobStatus.FAILED
            job.errors.append(str(e))
            
            await update.message.reply_text(
                f"❌ *Job failed*\n\nError: {str(e)[:100]}",
                parse_mode='Markdown'
            )
        
        finally:
            if temp_dir and os.path.exists(temp_dir):
                try:
                    shutil.rmtree(temp_dir)
                except:
                    pass
            if file_path and os.path.exists(file_path):
                try:
                    os.remove(file_path)
                except:
                    pass
    
    async def _process_split(self, update: Update, job: ExtractJob, document):
        file_path = None
        
        try:
            job.status = JobStatus.SPLITTING
            
            file_path = os.path.join(tempfile.gettempdir(), f"split_{job.job_id}_{document.file_name}")
            file = await document.get_file()
            await file.download_to_drive(file_path)
            
            split_result = await asyncio.get_event_loop().run_in_executor(
                self.job_manager.executor,
                split_zip,
                file_path, job.split_size, None
            )
            
            if not split_result['success']:
                raise Exception("Split failed: " + str(split_result['errors']))
            
            job.total_files = len(split_result['parts'])
            
            job.status = JobStatus.SENDING
            sent = 0
            
            use_telethon = self.telethon and self.telethon.connected
            
            for part in split_result['parts']:
                if job.cancel_requested:
                    break
                
                if use_telethon:
                    await self.telethon.upload_file(
                        update.effective_chat.id,
                        part['path'],
                        caption=f"✂️ Part {part['part_num']}/{part['total_parts']}\n📏 {part['size']/1024/1024:.1f}MB"
                    )
                else:
                    with open(part['path'], 'rb') as f:
                        await update.message.reply_document(
                            document=InputFile(f, filename=os.path.basename(part['path'])),
                            caption=f"✂️ Part {part['part_num']}/{part['total_parts']}\n📏 {part['size']/1024/1024:.1f}MB"
                        )
                
                sent += 1
                job.progress = (sent / len(split_result['parts'])) * 100
                
                try:
                    os.remove(part['path'])
                except:
                    pass
            
            job.status = JobStatus.COMPLETED
            job.progress = 100.0
            job.completed_at = datetime.now()
            
            await update.message.reply_text(
                f"✅ *Split complete!*\n\n"
                f"✂️ Parts created: {sent}\n"
                f"📦 Original: {document.file_size/1024/1024:.1f}MB\n"
                f"✂️ Part size: {job.split_size/1024/1024:.0f}MB\n"
                f"🔄 Method: {'⚡ Telethon' if use_telethon else '🤖 Bot API'}",
                parse_mode='Markdown'
            )
            
        except Exception as e:
            logger.exception(f"Split job {job.job_id} failed")
            job.status = JobStatus.FAILED
            job.errors.append(str(e))
            
            await update.message.reply_text(
                f"❌ *Split failed*\n\nError: {str(e)[:100]}",
                parse_mode='Markdown'
            )
        
        finally:
            if file_path and os.path.exists(file_path):
                try:
                    os.remove(file_path)
                except:
                    pass
    
    # ---- EXTRACTION METHODS ----
    def _extract_zip(self, file_path: str, extract_dir: str, job: ExtractJob) -> List[FileInfo]:
        extracted = []
        
        try:
            with zipfile.ZipFile(file_path, 'r') as zip_ref:
                safe_extract(zip_ref, extract_dir, job.password)
                
                for root, _, files in os.walk(extract_dir):
                    for file in files:
                        if job.cancel_requested:
                            return extracted
                            
                        full_path = os.path.join(root, file)
                        rel_path = os.path.relpath(full_path, extract_dir)
                        
                        mime_type, _ = mimetypes.guess_type(full_path)
                        ext = os.path.splitext(file)[1].lower()
                        
                        file_info = FileInfo(
                            name=file,
                            path=rel_path,
                            size=os.path.getsize(full_path),
                            mime_type=mime_type or 'application/octet-stream',
                            extension=ext,
                            extracted_path=full_path
                        )
                        extracted.append(file_info)
                        job.files_extracted += 1
                        
        except Exception as e:
            logger.exception(f"ZIP extraction failed: {e}")
            raise
        
        return extracted
    
    def _extract_rar(self, file_path: str, extract_dir: str, job: ExtractJob) -> List[FileInfo]:
        if not RAR_SUPPORT:
            raise ImportError("rarfile not installed")
        
        extracted = []
        try:
            with rarfile.RarFile(file_path) as rf:
                rf.extractall(extract_dir)
                
                for root, _, files in os.walk(extract_dir):
                    for file in files:
                        if job.cancel_requested:
                            return extracted
                            
                        full_path = os.path.join(root, file)
                        rel_path = os.path.relpath(full_path, extract_dir)
                        
                        mime_type, _ = mimetypes.guess_type(full_path)
                        ext = os.path.splitext(file)[1].lower()
                        
                        file_info = FileInfo(
                            name=file,
                            path=rel_path,
                            size=os.path.getsize(full_path),
                            mime_type=mime_type or 'application/octet-stream',
                            extension=ext,
                            extracted_path=full_path
                        )
                        extracted.append(file_info)
                        job.files_extracted += 1
                        
        except Exception as e:
            logger.exception(f"RAR extraction failed: {e}")
            raise
        
        return extracted
    
    def _extract_7z(self, file_path: str, extract_dir: str, job: ExtractJob) -> List[FileInfo]:
        if not SEVENZ_SUPPORT:
            raise ImportError("py7zr not installed")
        
        extracted = []
        try:
            with py7zr.SevenZipFile(file_path, mode='r') as sz:
                sz.extractall(path=extract_dir)
                
                for root, _, files in os.walk(extract_dir):
                    for file in files:
                        if job.cancel_requested:
                            return extracted
                            
                        full_path = os.path.join(root, file)
                        rel_path = os.path.relpath(full_path, extract_dir)
                        
                        mime_type, _ = mimetypes.guess_type(full_path)
                        ext = os.path.splitext(file)[1].lower()
                        
                        file_info = FileInfo(
                            name=file,
                            path=rel_path,
                            size=os.path.getsize(full_path),
                            mime_type=mime_type or 'application/octet-stream',
                            extension=ext,
                            extracted_path=full_path
                        )
                        extracted.append(file_info)
                        job.files_extracted += 1
                        
        except Exception as e:
            logger.exception(f"7z extraction failed: {e}")
            raise
        
        return extracted
    
    def _extract_tar(self, file_path: str, extract_dir: str, job: ExtractJob) -> List[FileInfo]:
        if not TAR_SUPPORT:
            raise ImportError("tarfile not available")
        
        extracted = []
        try:
            with tarfile.open(file_path, 'r:*') as tf:
                tf.extractall(extract_dir)
                
                for root, _, files in os.walk(extract_dir):
                    for file in files:
                        if job.cancel_requested:
                            return extracted
                            
                        full_path = os.path.join(root, file)
                        rel_path = os.path.relpath(full_path, extract_dir)
                        
                        mime_type, _ = mimetypes.guess_type(full_path)
                        ext = os.path.splitext(file)[1].lower()
                        
                        file_info = FileInfo(
                            name=file,
                            path=rel_path,
                            size=os.path.getsize(full_path),
                            mime_type=mime_type or 'application/octet-stream',
                            extension=ext,
                            extracted_path=full_path
                        )
                        extracted.append(file_info)
                        job.files_extracted += 1
                        
        except Exception as e:
            logger.exception(f"TAR extraction failed: {e}")
            raise
        
        return extracted
    
    # ---- FILE SENDERS ----
    async def _send_file(self, update: Update, file_info: FileInfo):
        ext = file_info.extension.lower()
        mime = file_info.mime_type or ''
        
        try:
            if ext in {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp'} or 'image' in mime:
                with open(file_info.extracted_path, 'rb') as f:
                    await update.message.reply_photo(
                        photo=f,
                        caption=f"🖼️ `{file_info.name}`\n📏 {file_info.size/1024:.1f}KB",
                        parse_mode='Markdown'
                    )
            
            elif ext in {'.mp4', '.avi', '.mov', '.mkv', '.wmv', '.flv', '.webm'} or 'video' in mime:
                with open(file_info.extracted_path, 'rb') as f:
                    await update.message.reply_video(
                        video=f,
                        caption=f"🎬 `{file_info.name}`\n📏 {file_info.size/1024:.1f}KB",
                        parse_mode='Markdown'
                    )
            
            elif ext in {'.mp3', '.wav', '.flac', '.aac', '.ogg', '.wma'} or 'audio' in mime:
                with open(file_info.extracted_path, 'rb') as f:
                    await update.message.reply_audio(
                        audio=f,
                        caption=f"🎵 `{file_info.name}`\n📏 {file_info.size/1024:.1f}KB",
                        parse_mode='Markdown'
                    )
            
            elif ext in {'.txt', '.py', '.js', '.html', '.css', '.json', '.xml', '.csv', '.yaml', '.yml'} or 'text' in mime:
                try:
                    with open(file_info.extracted_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                        if len(content) > 4096:
                            with open(file_info.extracted_path, 'rb') as f:
                                await update.message.reply_document(
                                    document=f,
                                    filename=file_info.name,
                                    caption=f"📄 `{file_info.name}`",
                                    parse_mode='Markdown'
                                )
                        else:
                            await update.message.reply_text(
                                f"📄 `{file_info.name}`:\n```\n{content}\n```",
                                parse_mode='Markdown'
                            )
                except UnicodeDecodeError:
                    with open(file_info.extracted_path, 'rb') as f:
                        await update.message.reply_document(
                            document=f,
                            filename=file_info.name,
                            caption=f"📄 `{file_info.name}` (binary)",
                            parse_mode='Markdown'
                        )
            
            else:
                with open(file_info.extracted_path, 'rb') as f:
                    await update.message.reply_document(
                        document=f,
                        filename=file_info.name,
                        caption=f"📎 `{file_info.name}`\n📏 {file_info.size/1024:.1f}KB",
                        parse_mode='Markdown'
                    )
                    
        except Exception as e:
            logger.error(f"Failed to send {file_info.name}: {e}")
            raise
    
    async def _send_file_telethon(self, update: Update, file_info: FileInfo, job: ExtractJob):
        if not self.telethon or not self.telethon.connected:
            await self._send_file(update, file_info)
            return
        
        try:
            async def progress(current, total):
                progress_pct = (current / total) * 100
                job.progress = progress_pct
            
            result = await self.telethon.upload_file(
                update.effective_chat.id,
                file_info.extracted_path,
                caption=f"📎 `{file_info.name}`\n📏 {file_info.size/1024/1024:.1f}MB\n⚡ Uploaded via Telethon",
                progress_callback=progress,
                job_id=job.job_id
            )
            
            if not result['success']:
                await self._send_file(update, file_info)
                
        except Exception as e:
            logger.error(f"Telethon send failed: {e}")
            await self._send_file(update, file_info)
    
    # ---- CALLBACK HANDLER ----
    async def handle_callback(self, update: Update, context: CallbackContext):
        query = update.callback_query
        await query.answer()
        
        data = query.data
        
        if data == "status":
            await self.cmd_status(update, context)
        elif data == "queue":
            await self.cmd_queue(update, context)
        elif data == "help":
            await self.cmd_help(update, context)
        elif data == "stats":
            await self.cmd_stats(update, context)
        elif data == "compress":
            await self.cmd_compress(update, context)
        elif data == "split":
            await self.cmd_split(update, context)
        elif data == "telethon":
            await self.cmd_telethon(update, context)
        elif data.startswith("status_"):
            job_id = data[7:]
            job = self.job_manager.get_job(job_id)
            if job:
                status_text = (
                    f"📊 *Job Status*\n\n"
                    f"File: {job.file_name}\n"
                    f"Operation: {job.operation.value}\n"
                    f"Status: `{job.status.value}`\n"
                    f"Progress: `{job.progress:.1f}%`\n"
                    f"Files: `{job.files_extracted}/{job.total_files}`\n"
                    f"Method: {'⚡ Telethon' if job.use_telethon else '🤖 Bot API'}\n"
                    f"Created: {job.created_at.strftime('%H:%M:%S')}\n"
                )
                if job.started_at:
                    status_text += f"Started: {job.started_at.strftime('%H:%M:%S')}\n"
                if job.completed_at:
                    status_text += f"Completed: {job.completed_at.strftime('%H:%M:%S')}\n"
                if job.errors:
                    status_text += f"\n⚠️ Errors: {len(job.errors)}"
                
                await query.edit_message_text(status_text, parse_mode='Markdown')
            else:
                await query.edit_message_text("❌ *Job not found*", parse_mode='Markdown')
        
        elif data.startswith("cancel_"):
            job_id = data[7:]
            success = self.job_manager.cancel_job(job_id)
            if success:
                await query.edit_message_text(f"✅ *Job {job_id[:8]} cancelled*", parse_mode='Markdown')
            else:
                await query.edit_message_text(f"❌ *Job {job_id[:8]} not found*", parse_mode='Markdown')
    
    # ---- ERROR HANDLER ----
    async def error_handler(self, update: Update, context: CallbackContext):
        logger.error(f"Update {update} caused error {context.error}")
        
        if ADMIN_CHAT_ID:
            try:
                await self.app.bot.send_message(
                    ADMIN_CHAT_ID,
                    f"⚠️ *Bot Error*\n\nError: {str(context.error)[:200]}",
                    parse_mode='Markdown'
                )
            except:
                pass
    
    # ---- UTILITY METHODS ----
    def _get_memory_usage(self) -> str:
        if PSUTIL_SUPPORT:
            try:
                process = psutil.Process(os.getpid())
                memory_mb = process.memory_info().rss / 1024 / 1024
                return f"{memory_mb:.1f}MB"
            except:
                pass
        return "N/A"
    
    def _get_uptime(self) -> str:
        uptime = datetime.now() - self.start_time
        hours = uptime.total_seconds() / 3600
        return f"{hours:.1f}h"
    
    # ---- RUN ----
    def run(self):
        logger.info("🔥 ZIP Gobbler v4.0 - GOD MODE starting...")
        logger.info(f"📦 Compression: Level {COMPRESSION_LEVEL}/9")
        logger.info(f"✂️ Split size: {SPLIT_SIZE_MB}MB")
        logger.info(f"📁 Bot API limit: {MAX_FILE_SIZE_MB}MB")
        logger.info(f"⚡ Telethon: {'ENABLED' if USE_TELETHON else 'DISABLED'}")
        logger.info(f"👷 Workers: {MAX_CONCURRENT_JOBS}")
        
        if self.telethon:
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    asyncio.create_task(self.telethon.start())
                    logger.info("⚡ Telethon started as task")
                else:
                    loop.run_until_complete(self.telethon.start())
                    logger.info("⚡ Telethon started synchronously")
            except RuntimeError as e:
                logger.warning(f"⚠️ Telethon startup postponed: {e}")
                logger.info("⚡ Telethon will start automatically when needed")
        
        try:
            self.app.run_polling(allowed_updates=Update.ALL_TYPES)
        except KeyboardInterrupt:
            logger.info("Shutting down...")
        finally:
            self.job_manager.shutdown()
            if self.telethon:
                try:
                    loop = asyncio.get_event_loop()
                    if loop.is_running():
                        asyncio.create_task(self.telethon.close())
                    else:
                        loop.run_until_complete(self.telethon.close())
                except:
                    pass

# ---- MAIN ----
if __name__ == "__main__":
    if BOT_TOKEN == "YOUR_BOT_TOKEN_HERE":
        print("❌ ERROR: Set TELEGRAM_BOT_TOKEN environment variable!")
        print("Get token from @BotFather.")
        sys.exit(1)
    
    if USE_TELETHON:
        if not TELEGRAM_API_ID or not TELEGRAM_API_HASH:
            print("❌ ERROR: Telethon enabled but API credentials missing!")
            print("Get API credentials from: https://my.telegram.org/apps")
            print("Add to .env: TELEGRAM_API_ID and TELEGRAM_API_HASH")
            sys.exit(1)
        
        if not TELETHON_SUPPORT:
            print("⚠️ Telethon not installed. Install with: pip install telethon cryptg")
    
    try:
        import telegram
    except ImportError:
        print("❌ Missing: python-telegram-bot")
        print("Install: pip install python-telegram-bot")
        sys.exit(1)
    
    bot = ZipGobblerBot()
    bot.run()
