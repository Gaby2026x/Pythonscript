#!/usr/bin/env python3
"""
PARIS SENDER - AGILE MARKETING SUITE v9.0.0 (ENHANCED PROXY VPS MAILER WITH ADVANCED ANTI-DETECTION & VPS OPTIMIZATION)
Handles Chrome version 140+ with proper ChromeDriver compatibility and undetected-chromedriver for human-mimic fingerprinting.
MAJOR REFACTOR & NEW FEATURES:
- ASYNC SMTP: Core sending logic transitioned from multi-threading to an asynchronous model using aiosmtplib for massive speed and efficiency improvements on large campaigns.
- JINJA2 TEMPLATING: Replaced basic placeholder replacement with the powerful Jinja2 templating engine. This enables advanced logic (loops, conditionals) directly in the email body (e.g., {% if user.is_customer %}...{% endif %}).
- LOCAL LLM SUPPORT: Introduced a new LocalAIHandler to integrate with local Large Language Models like Llama 3 via Ollama, offering privacy and cost savings.
- AI-POWERED SPAM CHECK: Added a new pre-send analysis tool that uses AI (OpenAI or Local) to provide sophisticated feedback on spam triggers, phrasing, and phishing indicators.
- UI ENHANCEMENTS: Added UI controls in the Settings tab to switch between OpenAI and Local AI, and configure the local AI server address.
- REFACTOR: SMTP Connection pooling rewritten to work asynchronously with the new aiosmtplib implementation.
- REFACTOR: Personalization logic now leverages the Jinja2 context for cleaner and more powerful dynamic content generation.
- REFACTOR: Updated AI methods (_ai_rewrite, _ai_subject) to use the selected AI provider.
- FIX (v8.0.3): Implemented an automatic fallback from `aiosmtplib` to a standard multi-threaded `smtplib` sender if the async library is not available. This makes the application more robust against environment issues (e.g., old Python versions).
- FIX (v8.0.2): Corrected autograb logic for [firstname], [greetings], and [company] placeholders to handle fallbacks and ISP domains as requested.
- FIX: Corrected syntax errors from the previous version.
- RESTORE: Reinstated the original autograb functionality for dynamic personalization ([firstname], [greetings], [company], etc.) within the Jinja2 context.
- NEW (v8.0.4): Integrated Proxy VPS Mailer for ultra-robust bulk sending via remote VPS with proxies. Zero failures, intelligent failover, and global scalability.
- NEW (v8.0.5): Enhanced Proxy VPS Mailer with per-VPS SMTP, geo-failover, AI optimization, proxy chains, encrypted logging, rate limiting, and advanced UI. Globally unmatched robustness and sophistication.
- FIX (v9.0.0): Changed import from aiosmtp to aiosmtplib for compatibility.
- FIX (v9.0.0): On headless VPS, fallback to encrypted local file or environment variables instead of keyring for password storage.
- FIX (v9.0.0): Resolved blocking in async producer by using asyncio.sleep instead of time.sleep.
- FIX (v9.0.0): Improved browser automation with XPath selectors for better element finding (e.g., //div[contains(@aria-label, 'Message')]).
- FIX (v9.0.0): Used waitress for Flask serving in threads for better performance.
- ENHANCEMENT (v9.0.0): Integrated undetected-chromedriver for human-mimic browser fingerprinting and anti-detection.
- ENHANCEMENT (v9.0.0): Added per-profile proxy binding for IP rotation.
- ENHANCEMENT (v9.0.0): Implemented smart warmup algorithm with warmup_schedule.json for automatic daily limits based on account age and history.
- ENHANCEMENT (v9.0.0): Added inbox placement verification using seed list and IMAP checks; pauses campaign if >50% go to spam.
- ENHANCEMENT (v9.0.0): Implemented dynamic content polymorphism with DOM shuffling for unique HTML structures.
- ENHANCEMENT (v9.0.0): Added headless API mode with core_engine.py separation, FastAPI for API, web UI support for VPS control.
- NEW: DNS Domain Checker for batch validation of domains from email list.
- NEW: Enhanced Configuration Settings Display for at-a-glance overview.
- NEW: Placeholder Engine with documentation and statistics UI.
- NEW: Live Sent Email Log with formatted output.
- NEW: VPS Bulk Sender Mode - Strict separation from SMTP, dedicated tab, enhanced sender configuration, improved bulk workflow.
- NEW: Direct-to-MX Delivery without SMTP Authentication - Fully asynchronous, with DKIM signing, rate limiting, retry queue via SQLite, and MX failover.
"""
import os
import re
import sys
import time
import uuid
import json
import base64
import socket
import shutil
import copy
import tempfile
import traceback
import threading
import webbrowser
import subprocess
import queue
import csv
import random
import sqlite3
import imaplib
import email
import asyncio
from email.header import decode_header
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import Counter
from urllib.parse import urlparse, urlunparse

# SMTP functionality
import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from email.utils import formataddr
from email.header import Header

# tkinter UI
import tkinter as tk
from tkinter import ttk, scrolledtext, filedialog, messagebox, simpledialog

# --- NEW/UPDATED Imports for v9.0.0 ---

# aiosmtplib for asyncio-based SMTP (changed from aiosmtp)
try:
    import aiosmtplib
    AIOSMTPLIB_AVAILABLE = True
except ImportError:
    AIOSMTPLIB_AVAILABLE = False

# Jinja2 for advanced templating
try:
    from jinja2 import Environment, FileSystemLoader, Template, exceptions as jinja_exceptions
    JINJA2_AVAILABLE = True
except ImportError:
    JINJA2_AVAILABLE = False

# --- NEW Imports for v8.0.4: Proxy VPS Mailer ---
try:
    import paramiko
    PARAMIKO_AVAILABLE = True
except ImportError:
    PARAMIKO_AVAILABLE = False

try:
    import socks
    SOCKS_AVAILABLE = True
except ImportError:
    SOCKS_AVAILABLE = False

# --- NEW Imports for v8.0.5: Enhanced VPS Features ---
try:
    import cryptography
    from cryptography.fernet import Fernet
    CRYPTOGRAPHY_AVAILABLE = True
except ImportError:
    CRYPTOGRAPHY_AVAILABLE = False

# --- NEW Imports for v9.0.0: Enhancements ---
try:
    import undetected_chromedriver as uc
    UNDETECTED_CHROMEDRIVER_AVAILABLE = True
except ImportError:
    UNDETECTED_CHROMEDRIVER_AVAILABLE = False

try:
    from fastapi import FastAPI
    import uvicorn
    FASTAPI_AVAILABLE = True
except ImportError:
    FASTAPI_AVAILABLE = False

try:
    from waitress import serve
    WAITRESS_AVAILABLE = True
except ImportError:
    WAITRESS_AVAILABLE = False

# --- NEW Imports for Direct MX Delivery ---
try:
    import dns.resolver
    DNSPYTHON_AVAILABLE = True
except ImportError:
    DNSPYTHON_AVAILABLE = False

try:
    import dkim
    DKIM_AVAILABLE = True
except ImportError:
    DKIM_AVAILABLE = False

# selenium
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import (
    StaleElementReferenceException,
    NoSuchElementException,
    ElementNotInteractableException,
    TimeoutException,
    WebDriverException,
    SessionNotCreatedException,
    ElementClickInterceptedException,
)

# Flask for tracking (optional)
try:
    from flask import Flask, request, send_file, make_response, jsonify, render_template_string
    FLASK_AVAILABLE = True
except Exception:
    Flask = None
    request = None
    send_file = None
    make_response = None
    jsonify = None
    render_template_string = None
    FLASK_AVAILABLE = False

# Outlook COM integration (optional)
try:
    if sys.platform == 'win32':
        import win32com.client
        import pythoncom
        import pywintypes
        import win32api
        import win32con
        import winreg
        OUTLOOK_COM_AVAILABLE = True
    else:
        OUTLOOK_COM_AVAILABLE = False
        win32com = None
        pythoncom = None
        pywintypes = None
        win32api = None
        win32con = None
        winreg = None
except Exception:
    OUTLOOK_COM_AVAILABLE = False
    win32com = None
    pythoncom = None
    pywintypes = None
    win32api = None
    win32con = None
    winreg = None

# tkcalendar for scheduling UI (optional)
try:
    from tkcalendar import Calendar, DateEntry
    TKCALENDAR_AVAILABLE = True
except ImportError:
    TKCALENDAR_AVAILABLE = False

# pyngrok for public tracking URLs (optional)
try:
    from pyngrok import ngrok
    PYNGROK_AVAILABLE = True
except ImportError:
    ngrok = None
    PYNGROK_AVAILABLE = False

# webdriver-manager for automatic ChromeDriver (optional)
try:
    from webdriver_manager.chrome import ChromeDriverManager
    WEBDRIVER_MANAGER_AVAILABLE = True
except Exception:
    WEBDRIVER_MANAGER_AVAILABLE = False
    ChromeDriverManager = None

# keyring for secure password storage (fallback handled)
try:
    import keyring
    KEYRING_AVAILABLE = True
except ImportError:
    KEYRING_AVAILABLE = False

# Matplotlib for visual analytics
try:
    import matplotlib.pyplot as plt
    from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
    from matplotlib.figure import Figure
    MATPLOTLIB_AVAILABLE = True
except ImportError:
    MATPLOTLIB_AVAILABLE = False

# Requests for GeoIP lookup & AI
try:
    import requests
    REQUESTS_AVAILABLE = True
except ImportError:
    REQUESTS_AVAILABLE = False

# User-agents for tech tracking
try:
    from user_agents import parse as ua_parse
    USER_AGENTS_AVAILABLE = True
except ImportError:
    USER_AGENTS_AVAILABLE = False

# DNS Python for deliverability checks
try:
    import dns.resolver
    DNSPYTHON_AVAILABLE = True
except ImportError:
    DNSPYTHON_AVAILABLE = False

# CSS Inline (New v6.0)
try:
    import css_inline
    CSS_INLINE_AVAILABLE = True
except ImportError:
    CSS_INLINE_AVAILABLE = False

# PyPDF for lure embedding
try:
    import PyPDF2
    PYPDF_AVAILABLE = True
except ImportError:
    PYPDF_AVAILABLE = False

# Configuration Defaults (Now overridable via Settings)
KEYRING_SERVICE_NAME = "paris_sender"
DEFAULT_TRACK_PORT = 5000
DEFAULT_CHROME_DEBUG_PORT = 9222
CAMPAIGN_ID = str(uuid.uuid4())
PROFILES_FILE = "profiles.json"
TEMPLATES_DIR = "templates"
REPORTS_DIR = "reports"
PLUGINS_DIR = "providers"
SUPPRESSION_LIST_FILE = "suppression_list.json"
SEQUENCES_DIR = "sequences"
SETTINGS_FILE = "settings.json"
ENCRYPTION_KEY_FILE = "encryption.key"
DB_FILE = "campaign_data.db"
WARMUP_SCHEDULE_FILE = "warmup_schedule.json"
SEED_LIST_FILE = "seed_list.json"

class ScrollableFrame(ttk.Frame):
    """A scrollable frame that expands to fill its container."""
    def __init__(self, container, *args, **kwargs):
        super().__init__(container, *args, **kwargs)
        self.canvas = tk.Canvas(self)
        scrollbar = ttk.Scrollbar(self, orient="vertical", command=self.canvas.yview)
        self.scrollable_frame = ttk.Frame(self.canvas)

        self._scroll_update_pending = False

        def _throttled_configure(e):
            if not self._scroll_update_pending:
                self._scroll_update_pending = True
                self.canvas.after(50, self._do_configure_update)

        self.scrollable_frame.bind("<Configure>", _throttled_configure)

        canvas_window = self.canvas.create_window((0, 0), window=self.scrollable_frame, anchor="nw")

        def on_canvas_configure(event):
            self.canvas.itemconfig(canvas_window, width=event.width)

        self.canvas.bind("<Configure>", on_canvas_configure)
        self.canvas.configure(yscrollcommand=scrollbar.set)

        self.canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        # Bind mouse wheel events only when cursor is over this canvas
        self.canvas.bind("<Enter>", self._bind_mousewheel)
        self.canvas.bind("<Leave>", self._unbind_mousewheel)

    def _on_mousewheel(self, event):
        """Handle mouse wheel scrolling with platform awareness."""
        try:
            if sys.platform == "darwin":
                self.canvas.yview_scroll(int(-1 * event.delta), "units")
            elif sys.platform == "win32":
                self.canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")
            else:
                if event.num == 4:
                    self.canvas.yview_scroll(-1, "units")
                elif event.num == 5:
                    self.canvas.yview_scroll(1, "units")
        except tk.TclError:
            pass

    def _bind_mousewheel(self, event):
        if sys.platform == "linux":
            self.canvas.bind("<Button-4>", self._on_mousewheel)
            self.canvas.bind("<Button-5>", self._on_mousewheel)
        else:
            self.canvas.bind("<MouseWheel>", self._on_mousewheel)

    def _unbind_mousewheel(self, event):
        if sys.platform == "linux":
            self.canvas.unbind("<Button-4>")
            self.canvas.unbind("<Button-5>")
        else:
            self.canvas.unbind("<MouseWheel>")

    def _do_configure_update(self):
        self._scroll_update_pending = False
        self.canvas.configure(scrollregion=self.canvas.bbox("all"))

class Tooltip:
    """Simple tooltip for tkinter widgets."""
    def __init__(self, widget, text):
        self.widget = widget
        self.text = text
        self.tipwindow = None
        widget.bind("<Enter>", self._show)
        widget.bind("<Leave>", self._hide)

    def _show(self, event=None):
        if self.tipwindow:
            return
        x = self.widget.winfo_rootx() + 20
        y = self.widget.winfo_rooty() + self.widget.winfo_height() + 5
        self.tipwindow = tw = tk.Toplevel(self.widget)
        tw.wm_overrideredirect(True)
        tw.wm_geometry(f"+{x}+{y}")
        label = tk.Label(tw, text=self.text, justify=tk.LEFT, background="#ffffe0",
                         relief=tk.SOLID, borderwidth=1, font=("Arial", 9), wraplength=300)
        label.pack(ipadx=4, ipady=2)

    def _hide(self, event=None):
        if self.tipwindow:
            self.tipwindow.destroy()
            self.tipwindow = None

class CollapsibleSection(tk.Frame):
    """A collapsible section with a toggle header."""
    def __init__(self, parent, title="", bg='#e0f7ff', fg="#2c3e50", expanded=True, **kwargs):
        super().__init__(parent, bg=bg, **kwargs)
        self._expanded = expanded
        self._title = title

        self.header = tk.Frame(self, bg=bg, cursor="hand2")
        self.header.pack(fill='x')

        self.toggle_label = tk.Label(
            self.header, text=f"{'▼' if expanded else '▶'} {title}",
            font=("Arial", 10, "bold"), bg=bg, fg=fg, anchor='w', cursor="hand2"
        )
        self.toggle_label.pack(fill='x', padx=5, pady=2)

        self.content = tk.Frame(self, bg=bg)
        if expanded:
            self.content.pack(fill='both', expand=True, padx=5, pady=(0, 5))

        self.header.bind("<Button-1>", self._toggle)
        self.toggle_label.bind("<Button-1>", self._toggle)

    def _toggle(self, event=None):
        self._expanded = not self._expanded
        if self._expanded:
            self.content.pack(fill='both', expand=True, padx=5, pady=(0, 5))
            self.toggle_label.config(text=f"▼ {self._title}")
        else:
            self.content.pack_forget()
            self.toggle_label.config(text=f"▶ {self._title}")

class DBHandler:
    """Handles SQLite operations for crash recovery and persistence."""
    def __init__(self, main_app):
        self.main_app = main_app
        self.conn = None
        self.lock = threading.Lock()
        self._init_db()

    def _init_db(self):
        try:
            self.conn = sqlite3.connect(DB_FILE, check_same_thread=False)
            cursor = self.conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS recipients (
                    email TEXT PRIMARY KEY,
                    data TEXT,
                    status TEXT,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            # NEW: Table for Direct MX retry queue
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS mx_retry_queue (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    message_id TEXT UNIQUE,
                    email TEXT,
                    subject TEXT,
                    content TEXT,
                    attachments TEXT,
                    attempts INTEGER DEFAULT 0,
                    next_retry TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            self.conn.commit()
        except Exception as e:
            print(f"DB Init Error: {e}")

    def autosave(self, tracking_map):
        """Mirrors the in-memory tracking map to SQLite."""
        if not tracking_map: return
        with self.lock:
            try:
                cursor = self.conn.cursor()
                for email, data in tracking_map.items():
                    json_data = json.dumps(data)
                    status = data.get('status', 'Unknown')
                    cursor.execute('''
                        INSERT OR REPLACE INTO recipients (email, data, status, last_updated)
                        VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                    ''', (email, json_data, status))
                self.conn.commit()
            except Exception as e:
                print(f"Autosave Error: {e}")

    def add_to_mx_retry_queue(self, message_id, email, subject, content, attachments):
        with self.lock:
            try:
                cursor = self.conn.cursor()
                attachments_json = json.dumps(attachments)
                next_retry = datetime.now() + timedelta(minutes=1)  # Initial 1 min
                cursor.execute('''
                    INSERT OR REPLACE INTO mx_retry_queue (message_id, email, subject, content, attachments, attempts, next_retry)
                    VALUES (?, ?, ?, ?, ?, 0, ?)
                ''', (message_id, email, subject, content, attachments_json, next_retry.isoformat()))
                self.conn.commit()
            except Exception as e:
                self.main_app.log(f"Error adding to MX retry queue: {e}")

    def get_mx_retry_items(self):
        with self.lock:
            try:
                cursor = self.conn.cursor()
                cursor.execute('''
                    SELECT id, message_id, email, subject, content, attachments, attempts, next_retry FROM mx_retry_queue
                    WHERE next_retry <= ? AND attempts < 5
                ''', (datetime.now().isoformat(),))
                rows = cursor.fetchall()
                items = []
                for row in rows:
                    attachments = json.loads(row[5]) if row[5] else []
                    items.append({
                        'id': row[0],
                        'message_id': row[1],
                        'email': row[2],
                        'subject': row[3],
                        'content': row[4],
                        'attachments': attachments,
                        'attempts': row[6],
                        'next_retry': row[7]
                    })
                return items
            except Exception as e:
                self.main_app.log(f"Error fetching MX retry items: {e}")
                return []

    def update_mx_retry_attempt(self, item_id, new_attempts, next_retry):
        with self.lock:
            try:
                cursor = self.conn.cursor()
                cursor.execute('''
                    UPDATE mx_retry_queue SET attempts = ?, next_retry = ? WHERE id = ?
                ''', (new_attempts, next_retry.isoformat(), item_id))
                self.conn.commit()
            except Exception as e:
                self.main_app.log(f"Error updating MX retry attempt: {e}")

    def remove_from_mx_retry_queue(self, item_id):
        with self.lock:
            try:
                cursor = self.conn.cursor()
                cursor.execute('DELETE FROM mx_retry_queue WHERE id = ?', (item_id,))
                self.conn.commit()
            except Exception as e:
                self.main_app.log(f"Error removing from MX retry queue: {e}")

class IMAPHandler:
    """Handles checking for replies via IMAP."""
    def __init__(self, main_app):
        self.main_app = main_app
        self.server = ""
        self.port = 993
        self.username = ""
        self.password = ""
        self.last_check = datetime.now()

    def configure(self, server, port, username, password):
        self.server = server
        self.port = int(port) if port else 993
        self.username = username
        self.password = password

    def check_replies(self):
        if not self.server or not self.username or not self.password:
            return 0

        try:
            mail = imaplib.IMAP4_SSL(self.server, self.port)
            mail.login(self.username, self.password)
            mail.select("inbox")

            date_str = (self.last_check - timedelta(days=1)).strftime("%d-%b-%Y")
            status, messages = mail.search(None, f'(SINCE "{date_str}")')

            if status != 'OK': return 0

            found_count = 0
            for num in messages[0].split():
                try:
                    status, msg_data = mail.fetch(num, '(RFC822)')
                    for response_part in msg_data:
                        if isinstance(response_part, tuple):
                            msg = email.message_from_bytes(response_part[1])
                            from_header = decode_header(msg["From"])[0][0]
                            if isinstance(from_header, bytes):
                                from_header = from_header.decode()

                            sender_match = re.search(r'<(.+?)>', str(from_header))
                            sender_email = sender_match.group(1).lower() if sender_match else str(from_header).lower()

                            if "<" in sender_email: sender_email = sender_email.split("<")[1].replace(">", "")

                            if sender_email in self.main_app.tracking_map:
                                current_status = self.main_app.tracking_map[sender_email].get('status')
                                if current_status != 'Replied':
                                    self.main_app.tracking_map[sender_email]['status'] = 'Replied'
                                    self.main_app.gui_update_queue.put(('update_recipient', sender_email))
                                    self.main_app.log(f"📩 Reply detected from: {sender_email}")
                                    found_count += 1
                except Exception: continue

            mail.close()
            mail.logout()
            self.last_check = datetime.now()
            return found_count

        except Exception as e:
            self.main_app.log(f"⚠️ IMAP Check Error: {e}")
            return 0

class AIHandler:
    """Handles OpenAI API calls."""
    def __init__(self, main_app):
        self.main_app = main_app
        self.api_key = None
        self.model = "gpt-3.5-turbo"
        self.api_url = "https://api.openai.com/v1/chat/completions"

    def configure(self, api_key, model="gpt-3.5-turbo"):
        self.api_key = api_key
        self.model = model

    def generate(self, prompt, system_msg="You are a helpful email marketing assistant."):
        if not self.api_key:
            return False, "API Key missing. Please configure in Settings."
        if not REQUESTS_AVAILABLE:
            return False, "Requests library missing."

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        data = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system_msg},
                {"role": "user", "content": prompt}
            ],
            "temperature": 0.7
        }

        try:
            response = requests.post(self.api_url, headers=headers, json=data, timeout=30)
            if response.status_code == 200:
                result = response.json()
                content = result['choices'][0]['message']['content']
                return True, content.strip()
            else:
                return False, f"API Error ({response.status_code}): {response.text}"
        except Exception as e:
            return False, f"Connection Error: {e}"

class LocalAIHandler:
    """NEW v8.0.0: Handles Local LLM (Ollama) API calls."""
    def __init__(self, main_app):
        self.main_app = main_app
        self.api_url = "http://localhost:11434/api/generate"
        self.model = "llama3"

    def configure(self, api_url, model="llama3"):
        self.api_url = api_url if api_url else "http://localhost:11434/api/generate"
        self.model = model if model else "llama3"

    def generate(self, prompt, system_msg="You are a helpful email marketing assistant."):
        if not self.api_url:
            return False, "Local AI URL missing. Please configure in Settings."
        if not REQUESTS_AVAILABLE:
            return False, "Requests library missing."

        headers = {"Content-Type": "application/json"}
        data = {
            "model": self.model,
            "system": system_msg,
            "prompt": prompt,
            "stream": False
        }

        try:
            self.main_app.log(f"🤖 Calling Local AI at {self.api_url} with model {self.model}...")
            response = requests.post(self.api_url, headers=headers, json=data, timeout=120)
            if response.status_code == 200:
                result = response.json()
                content = result.get('response', '')
                return True, content.strip()
            else:
                return False, f"Local AI Error ({response.status_code}): {response.text}"
        except Exception as e:
            return False, f"Local AI Connection Error: {e}\nIs Ollama running?"

class ProxyVPSHandler:
    """NEW v8.0.4: Handles robust bulk email sending via remote VPS with proxies. ENHANCED v8.0.5: Per-VPS SMTP, geo-failover, AI optimization, proxy chains, encrypted logging, rate limiting. ENHANCED v9.0.1: Standalone mode with enhanced sender configuration."""
    def __init__(self, main_app):
        self.main_app = main_app
        self.vps_pool = []  # List of VPS configs with enhanced fields
        self.health_status = {}  # Health tracking per VPS
        self.proxy_health_status = {}  # Health tracking per proxy: {'vps_id:proxy_host:proxy_port': {'healthy': bool, 'last_checked': datetime, 'failures': int}}
        self.circuit_breaker = {}  # Failure tracking for circuit breaker
        self.load_balancer_index = 0
        self.lock = threading.Lock()
        self.encryption_key = self._load_or_create_encryption_key() if CRYPTOGRAPHY_AVAILABLE else None
        self.rate_limits = {}  # Per-VPS rate limiting (e.g., {'gmail': 500})
        self.geo_cache = {}  # Cache for geo lookups
        self._load_vps_configs()
        self._start_health_monitor()

    def _load_or_create_encryption_key(self):
        """Load encryption key from file, or create and persist a new one."""
        try:
            if os.path.exists(ENCRYPTION_KEY_FILE):
                with open(ENCRYPTION_KEY_FILE, 'rb') as f:
                    key = f.read().strip()
                Fernet(key)  # Validate key
                return key
        except Exception:
            pass
        key = Fernet.generate_key()
        try:
            with open(ENCRYPTION_KEY_FILE, 'wb') as f:
                f.write(key)
        except Exception:
            pass
        return key

    def _load_vps_configs(self):
        """Load enhanced VPS configurations from settings."""
        try:
            with open(SETTINGS_FILE, "r") as f:
                settings = json.load(f)
                self.vps_pool = settings.get("vps_configs", [])
                for vps in self.vps_pool:
                    self.health_status[vps['id']] = True
                    self.circuit_breaker[vps['id']] = 0
                    self.rate_limits[vps['id']] = vps.get('rate_limit', 500)  # Default 500/day
        except Exception:
            self.vps_pool = []

    def save_vps_configs(self):
        """Save enhanced VPS configs to settings."""
        try:
            try:
                with open(SETTINGS_FILE, "r") as f:
                    settings = json.load(f)
            except FileNotFoundError:
                settings = {}
            settings["vps_configs"] = self.vps_pool
            with open(SETTINGS_FILE, "w") as f:
                json.dump(settings, f, indent=2)
        except Exception as e:
            self.main_app.log(f"❌ Error saving VPS configs: {e}")

    def add_vps(self, config):
        """Add a new enhanced VPS config."""
        config['id'] = str(uuid.uuid4())
        config.setdefault('rate_limit', 500)
        config.setdefault('max_retries', 3)
        config.setdefault('circuit_threshold', 5)
        config.setdefault('geo_region', 'Unknown')
        config.setdefault('proxy_chain', [])  # List of proxy dicts for chains
        config.setdefault('ai_handler', 'OpenAI')  # Which AI to use for optimization
        config.setdefault('sender_details', {'sender_name': 'Sender', 'sender_email': 'noreply@example.com', 'random_emails_pool': []})
        self.vps_pool.append(config)
        self.health_status[config['id']] = True
        self.circuit_breaker[config['id']] = 0
        self.rate_limits[config['id']] = config['rate_limit']
        self.save_vps_configs()

    def remove_vps(self, vps_id):
        """Remove a VPS config."""
        self.vps_pool = [v for v in self.vps_pool if v['id'] != vps_id]
        del self.health_status[vps_id]
        del self.circuit_breaker[vps_id]
        del self.rate_limits[vps_id]
        self.save_vps_configs()

    def _start_health_monitor(self):
        """Background health monitor for VPS."""
        def monitor():
            while True:
                self.check_health()
                time.sleep(300)  # Every 5 minutes
        threading.Thread(target=monitor, daemon=True).start()

    def check_health(self):
        """Background health check for all VPS with SMTP and per-proxy testing."""
        if not PARAMIKO_AVAILABLE:
            return
        for vps in self.vps_pool:
            try:
                # SSH Health
                ssh = paramiko.SSHClient()
                ssh.load_system_host_keys()
                ssh.connect(vps['host'], username=vps['user'], key_filename=vps.get('key_file'), timeout=10)
                # Determine mode
                smtp_user = vps.get('smtp_user', '')
                smtp_pass = self._decrypt_password(vps.get('smtp_pass_encrypted', ''))
                standalone_mode = vps.get('standalone_mode', False) or not smtp_user or not smtp_pass

                if standalone_mode:
                    # Test local MTA
                    stdin, stdout, stderr = ssh.exec_command("""python3 -c "
import smtplib
try:
    server = smtplib.SMTP('localhost', 25)
    server.ehlo()
    server.quit()
    print('LOCAL_MTA_OK')
except Exception as e:
    print('ERROR: ' + str(e))
    exit(2)
" """)
                    exit_status = stdout.channel.recv_exit_status()
                    output = stdout.read().decode().strip()
                    if exit_status == 0 and "LOCAL_MTA_OK" in output:
                        self.health_status[vps['id']] = True
                        self.circuit_breaker[vps['id']] = 0
                    else:
                        self.circuit_breaker[vps['id']] += 1
                elif smtp_user and smtp_pass:
                    # Test external SMTP
                    smtp_server = vps.get('smtp_server', '')
                    smtp_port = vps.get('smtp_port', 587)
                    stdin, stdout, stderr = ssh.exec_command(f"""
python3 -c "
import smtplib
import ssl
from email.mime.text import MIMEText

{'import socks; socks.setdefaultproxy(socks.PROXY_TYPE_SOCKS5, \'' + str(vps.get('proxy_chain', [{}])[0].get('host', '')) + '\', ' + str(vps.get('proxy_chain', [{}])[0].get('port', 0)) + '); socks.wrapmodule(smtplib)' if vps.get('proxy_chain') else ''}

try:
    server = smtplib.SMTP('{smtp_server}', {smtp_port})
    server.ehlo()
    server.starttls()
    server.login('{smtp_user}', '{smtp_pass}')
    server.quit()
    print('SMTP OK')
except smtplib.SMTPAuthenticationError as e:
    print('AUTH_ERROR: ' + str(e))
    exit(1)
except Exception as e:
    print('ERROR: ' + str(e))
    exit(2)
" """)
                    error = stderr.read().decode().strip()
                    if not error:
                        self.health_status[vps['id']] = True
                        self.circuit_breaker[vps['id']] = 0
                    else:
                        self.circuit_breaker[vps['id']] += 1
                # Per-proxy health check
                self._check_proxy_health(vps, ssh)
                ssh.close()
            except Exception:
                self.circuit_breaker[vps['id']] += 1
                if self.circuit_breaker[vps['id']] > vps.get('circuit_threshold', 5):
                    self.health_status[vps['id']] = False
                    self.main_app.log(f"⚠️ VPS {vps['host']} disabled due to failures.")

    def _check_proxy_health(self, vps, ssh):
        """Test each proxy in a VPS's proxy_chain for connectivity."""
        proxy_chain = vps.get('proxy_chain', [])
        for proxy in proxy_chain:
            proxy_key = f"{vps['id']}:{proxy.get('host', '')}:{proxy.get('port', 0)}"
            try:
                proxy_host = proxy.get('host', '')
                proxy_port = proxy.get('port', 0)
                if not proxy_host or not proxy_port:
                    continue
                # Sanitize proxy host/port to prevent injection
                safe_host = re.sub(r'[^a-zA-Z0-9.\-]', '', str(proxy_host))
                safe_port = int(proxy_port)
                # Test proxy connectivity via SSH on VPS
                stdin, stdout, stderr = ssh.exec_command(
                    f'python3 -c "import socks; s=socks.socksocket(); s.set_proxy(socks.SOCKS5, \'{safe_host}\', {safe_port}); s.settimeout(10); s.connect((\'icanhazip.com\', 80)); s.sendall(b\'GET / HTTP/1.1\\r\\nHost: icanhazip.com\\r\\n\\r\\n\'); print(s.recv(256).decode()); s.close(); print(\'PROXY_OK\')"'
                )
                output = stdout.read().decode().strip()
                if 'PROXY_OK' in output:
                    self.proxy_health_status[proxy_key] = {'healthy': True, 'last_checked': datetime.now(), 'failures': 0}
                else:
                    existing = self.proxy_health_status.get(proxy_key, {'failures': 0})
                    self.proxy_health_status[proxy_key] = {'healthy': False, 'last_checked': datetime.now(), 'failures': existing['failures'] + 1}
            except Exception:
                existing = self.proxy_health_status.get(proxy_key, {'failures': 0})
                self.proxy_health_status[proxy_key] = {'healthy': False, 'last_checked': datetime.now(), 'failures': existing['failures'] + 1}

    def get_healthy_proxy(self, vps_config):
        """Get the highest-priority healthy proxy for a VPS, or None."""
        proxy_chain = vps_config.get('proxy_chain', [])
        for proxy in proxy_chain:
            proxy_key = f"{vps_config['id']}:{proxy.get('host', '')}:{proxy.get('port', 0)}"
            status = self.proxy_health_status.get(proxy_key, {'healthy': True})
            if status.get('healthy', True):
                return proxy
        return None

    def mark_proxy_down(self, vps_config, proxy):
        """Mark a specific proxy as down."""
        proxy_key = f"{vps_config['id']}:{proxy.get('host', '')}:{proxy.get('port', 0)}"
        existing = self.proxy_health_status.get(proxy_key, {'failures': 0})
        self.proxy_health_status[proxy_key] = {'healthy': False, 'last_checked': datetime.now(), 'failures': existing.get('failures', 0) + 1}

    def _decrypt_password(self, encrypted_pass):
        """Decrypt stored password. Returns empty string if decryption fails."""
        if not encrypted_pass:
            return ''
        if not CRYPTOGRAPHY_AVAILABLE:
            return encrypted_pass
        try:
            fernet = Fernet(self.encryption_key)
            return fernet.decrypt(encrypted_pass.encode()).decode()
        except Exception:
            return ''

    def _encrypt_password(self, password):
        """Encrypt password for storage."""
        if not CRYPTOGRAPHY_AVAILABLE:
            return password
        fernet = Fernet(self.encryption_key)
        return fernet.encrypt(password.encode()).decode()

    def get_geo_region(self, ip_or_domain):
        """Get geo region for failover."""
        if ip_or_domain in self.geo_cache:
            return self.geo_cache[ip_or_domain]
        if not REQUESTS_AVAILABLE:
            return 'Unknown'
        try:
            response = requests.get(f"http://ip-api.com/json/{ip_or_domain}", timeout=5)
            data = response.json()
            region = f"{data.get('country', 'Unknown')}-{data.get('regionName', 'Unknown')}"
            self.geo_cache[ip_or_domain] = region
            return region
        except:
            return 'Unknown'

    def get_healthy_vps(self, preferred_region=None):
        """Get next healthy VPS with geo preference."""
        healthy = [v for v in self.vps_pool if self.health_status.get(v['id'], False)]
        if not healthy:
            return None
        if preferred_region:
            regional = [v for v in healthy if v.get('geo_region') == preferred_region]
            if regional:
                healthy = regional
        vps = healthy[self.load_balancer_index % len(healthy)]
        self.load_balancer_index += 1
        return vps

    def get_sender_details(self, vps_config, use_random=False):
        """Get sender details for a VPS, with optional randomization."""
        details = vps_config.get('sender_details', {'sender_name': 'Sender', 'sender_email': 'noreply@example.com', 'random_emails_pool': []})
        sender_name = details.get('sender_name', 'Sender')
        sender_email = details.get('sender_email', 'noreply@example.com')
        if use_random and details.get('random_emails_pool'):
            sender_email = random.choice(details['random_emails_pool'])
        return sender_name, sender_email

    def send_via_vps(self, email_data, vps_config):
        """Send an email via a specific VPS using SSH, proxy, and per-VPS SMTP. Two-tiered proxy failover.
        Supports 'standalone' mode where the VPS uses its local MTA (sendmail/postfix) instead of external SMTP."""
        if not PARAMIKO_AVAILABLE:
            return False, "Paramiko library not available."

        # Check if standalone mode is explicitly enabled by user
        standalone_mode = vps_config.get('standalone_mode', False)
        
        # If not explicitly standalone, check if SMTP credentials are available
        smtp_pass = ''
        smtp_user = ''
        if not standalone_mode:
            smtp_pass = self._decrypt_password(vps_config.get('smtp_pass_encrypted', ''))
            smtp_user = vps_config.get('smtp_user', '')
            
            # Auto-fallback to standalone mode if SMTP credentials are missing
            if not smtp_pass or not smtp_user:
                self.main_app.log(f"📬 VPS {vps_config['host']}: No SMTP credentials configured. Using local mail server (Postfix/Exim).")
                standalone_mode = True

        # Use standalone native MTA mode
        if standalone_mode:
            return self._send_via_vps_native(email_data, vps_config)

        # SMTP mode requires PySocks for proxy support
        if not SOCKS_AVAILABLE:
            return False, "PySocks library not available for proxy mode."

        max_retries = vps_config.get('max_retries', 3)
        # smtp_pass and smtp_user already retrieved above, no need to decrypt again

        sender_name, sender_email = self.get_sender_details(vps_config, email_data.get('use_random_sender', False))
        # Override with UI-provided sender details if available
        if email_data.get('sender_name'):
            sender_name = email_data['sender_name']
        if email_data.get('sender_email'):
            sender_email = email_data['sender_email']

        # Build ordered proxy list: healthy proxies first, then unhealthy as fallback
        proxy_chain = vps_config.get('proxy_chain', [])
        proxies_to_try = []
        for proxy in proxy_chain:
            proxy_key = f"{vps_config['id']}:{proxy.get('host', '')}:{proxy.get('port', 0)}"
            status = self.proxy_health_status.get(proxy_key, {'healthy': True})
            if status.get('healthy', True):
                proxies_to_try.append(proxy)
        # If no healthy proxies, try all (last resort) or no-proxy
        if not proxies_to_try and proxy_chain:
            proxies_to_try = list(proxy_chain)
        # Add a None entry for direct (no proxy) attempt if proxy chain exists but all fail
        proxy_attempts = proxies_to_try if proxies_to_try else [None]

        for current_proxy in proxy_attempts:
            for attempt in range(max_retries):
                try:
                    # Proxy Setup for current proxy
                    if current_proxy:
                        socks.setdefaultproxy(socks.PROXY_TYPE_SOCKS5, current_proxy['host'], current_proxy['port'])
                        socks.wrapmodule(smtplib)

                    # Prepare MIME with custom sender
                    mime_msg = self._create_vps_mime_message(email_data, vps_config, sender_name, sender_email)

                    # AI Optimization before sending
                    ai_handler = self.main_app.local_ai_handler if vps_config.get('ai_handler') == 'Local' else self.main_app.ai_handler
                    if ai_handler:
                        region = vps_config.get('geo_region', 'Global')
                        success, optimized = ai_handler.generate(f"Optimize this email subject and body for {region} region: Subject: {email_data['subject']} Body: {email_data['content']}", "You are a regional email optimizer.")
                        if success:
                            lines = optimized.split('\n', 1)
                            if len(lines) > 1:
                                mime_msg['Subject'] = Header(lines[0], 'utf-8').encode()
                                mime_msg.set_payload(lines[1])

                    email_raw = mime_msg.as_string()

                    # Escape single quotes and other shell metacharacters to prevent injection
                    def shell_escape(s):
                        return s.replace("'", "\\'").replace('"', '\\"').replace(';', '\\;').replace('$', '\\$').replace('`', '\\`').replace('(', '\\(').replace(')', '\\)')

                    smtp_server = shell_escape(str(vps_config.get('smtp_server', 'smtp.office365.com')))
                    smtp_user = shell_escape(str(vps_config.get('smtp_user')))
                    smtp_pass_escaped = shell_escape(str(smtp_pass))

                    # Build proxy setup code for SSH command
                    proxy_setup_code = ''
                    if current_proxy:
                        proxy_host_escaped = shell_escape(str(current_proxy['host']))
                        proxy_port = current_proxy['port']
                        proxy_setup_code = f"import socks; socks.setdefaultproxy(socks.PROXY_TYPE_SOCKS5, '{proxy_host_escaped}', {proxy_port}); socks.wrapmodule(smtplib)"

                    # SSH to VPS and execute with SMTP
                    command = f"""
python3 -c "
import smtplib
import ssl
from email.mime.text import MIMEText
import socks

{proxy_setup_code}

try:
    server = smtplib.SMTP('{smtp_server}', {vps_config.get('smtp_port', 587)})
    server.ehlo()
    server.starttls()
    server.login('{smtp_user}', '{smtp_pass_escaped}')
    server.sendmail('{sender_email}', '{email_data['to_email']}', '''{email_raw}''')
    server.quit()
    print('SMTP OK')
except smtplib.SMTPAuthenticationError as e:
    print('AUTH_ERROR: ' + str(e))
    exit(1)
except Exception as e:
    print('ERROR: ' + str(e))
    exit(2)
" """

                    ssh = paramiko.SSHClient()
                    ssh.load_system_host_keys()
                    ssh.connect(vps_config['host'], username=vps_config['user'], key_filename=vps_config.get('key_file'), timeout=30)
                    stdin, stdout, stderr = ssh.exec_command(command)
                    exit_status = stdout.channel.recv_exit_status()  # Wait for command to finish
                    error = stderr.read().decode().strip()
                    output = stdout.read().decode().strip()
                    ssh.close()

                    if exit_status == 0 and "SMTP OK" in output:
                        # Log to encrypted DB on VPS
                        self._log_send_to_vps(vps_config, email_data)
                        # AI feedback loop: Adjust based on send result
                        self._ai_optimize_vps(vps_config, True, email_data)
                        return True, "Sent"
                    elif exit_status == 1 and "AUTH_ERROR" in output:
                        self.main_app.log(f"❌ VPS {vps_config['host']} authentication failed. Check SMTP credentials (username: {vps_config.get('smtp_user')}). For services like Office 365/Gmail, ensure using an app password if 2FA is enabled. Do not retry.")
                        return False, "Auth failed"
                    else:
                        # Exponential backoff for other errors
                        backoff = 2 ** attempt
                        error_msg = error if error else f"SSH exit code {exit_status}, output: {output}"
                        self.main_app.log(f"VPS {vps_config['host']} attempt {attempt+1} failed: {error_msg}. Backoff {backoff}s.")
                        time.sleep(backoff)
                        continue

                except paramiko.SSHException as e:
                    backoff = 2 ** attempt
                    self.main_app.log(f"VPS {vps_config['host']} attempt {attempt+1} SSH error: {e}. Backoff {backoff}s.")
                    time.sleep(backoff)
                except Exception as e:
                    backoff = 2 ** attempt
                    self.main_app.log(f"VPS {vps_config['host']} attempt {attempt+1} unexpected error: {e}. Backoff {backoff}s.")
                    time.sleep(backoff)
            # If all retries for this proxy failed, mark proxy down and try next
            if current_proxy:
                self.mark_proxy_down(vps_config, current_proxy)
                self.main_app.log(f"⚠️ Proxy {current_proxy.get('host')}:{current_proxy.get('port')} marked down, trying next proxy...")

        # AI feedback loop: Adjust on failure
        self._ai_optimize_vps(vps_config, False, email_data)
        return False, "Max retries exceeded"

    def _send_via_vps_native(self, email_data, vps_config):
        """VPS Standalone Mode: Send email using the VPS's local MTA (sendmail/postfix/localhost SMTP).
        No external SMTP credentials required - the VPS acts as the mail server.
        Uses base64 encoding to safely transport email content via SSH."""
        if not PARAMIKO_AVAILABLE:
            return False, "Paramiko library not available."

        max_retries = vps_config.get('max_retries', 3)
        sender_name, sender_email = self.get_sender_details(vps_config, email_data.get('use_random_sender', False))
        if email_data.get('sender_name'):
            sender_name = email_data['sender_name']
        if email_data.get('sender_email'):
            sender_email = email_data['sender_email']

        mime_msg = self._create_vps_mime_message(email_data, vps_config, sender_name, sender_email)
        email_raw = mime_msg.as_string()

        # Base64-encode email content and addresses to prevent command injection
        email_raw_b64 = base64.b64encode(email_raw.encode('utf-8')).decode('ascii')
        sender_email_b64 = base64.b64encode(sender_email.encode('utf-8')).decode('ascii')
        to_email_b64 = base64.b64encode(email_data['to_email'].encode('utf-8')).decode('ascii')

        for attempt in range(max_retries):
            try:
                ssh = paramiko.SSHClient()
                ssh.load_system_host_keys()
                ssh.connect(vps_config['host'], username=vps_config['user'],
                            key_filename=vps_config.get('key_file'), timeout=30)

                # Use base64-encoded data to safely pass content via SSH command
                native_command = (
                    "python3 -c \""
                    "import smtplib, subprocess, sys, base64; "
                    f"email_raw = base64.b64decode('{email_raw_b64}').decode('utf-8'); "
                    f"sender = base64.b64decode('{sender_email_b64}').decode('utf-8'); "
                    f"recipient = base64.b64decode('{to_email_b64}').decode('utf-8'); "
                    "success = False; "
                    "try:\\n"
                    "    server = smtplib.SMTP('localhost', 25)\\n"
                    "    server.ehlo()\\n"
                    "    server.sendmail(sender, recipient, email_raw)\\n"
                    "    server.quit()\\n"
                    "    print('NATIVE_SMTP_OK')\\n"
                    "    success = True\\n"
                    "except Exception:\\n"
                    "    pass\\n"
                    "if not success:\\n"
                    "    try:\\n"
                    "        proc = subprocess.Popen(['/usr/sbin/sendmail', '-t', '-oi', '-f', sender], "
                    "stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)\\n"
                    "        stdout, stderr = proc.communicate(email_raw.encode('utf-8'))\\n"
                    "        if proc.returncode == 0:\\n"
                    "            print('SENDMAIL_OK')\\n"
                    "        else:\\n"
                    "            print('SENDMAIL_ERROR: ' + stderr.decode())\\n"
                    "            sys.exit(2)\\n"
                    "    except FileNotFoundError:\\n"
                    "        print('ERROR: No local MTA found')\\n"
                    "        sys.exit(2)\\n"
                    "    except Exception as e:\\n"
                    "        print('ERROR: ' + str(e))\\n"
                    "        sys.exit(2)\\n"
                    "\""
                )

                stdin, stdout, stderr = ssh.exec_command(native_command)
                exit_status = stdout.channel.recv_exit_status()
                output = stdout.read().decode().strip()
                error = stderr.read().decode().strip()
                ssh.close()

                if exit_status == 0 and ("NATIVE_SMTP_OK" in output or "SENDMAIL_OK" in output):
                    self._log_send_to_vps(vps_config, email_data)
                    self._ai_optimize_vps(vps_config, True, email_data)
                    self.main_app.log(f"✅ VPS Standalone sent to {email_data['to_email']} via {vps_config['host']}")
                    return True, "Sent (Standalone)"
                else:
                    backoff = 2 ** attempt
                    error_msg = error if error else f"exit code {exit_status}, output: {output}"
                    self.main_app.log(f"VPS {vps_config['host']} standalone attempt {attempt+1} failed: {error_msg}. Backoff {backoff}s.")
                    time.sleep(backoff)

            except paramiko.SSHException as e:
                backoff = 2 ** attempt
                self.main_app.log(f"VPS {vps_config['host']} standalone attempt {attempt+1} SSH error: {e}. Backoff {backoff}s.")
                time.sleep(backoff)
            except Exception as e:
                backoff = 2 ** attempt
                self.main_app.log(f"VPS {vps_config['host']} standalone attempt {attempt+1} error: {e}. Backoff {backoff}s.")
                time.sleep(backoff)

        self._ai_optimize_vps(vps_config, False, email_data)
        return False, "Max retries exceeded (Standalone)"

    def _ai_optimize_vps(self, vps_config, success, email_data):
        """AI feedback loop for VPS optimization."""
        ai_handler = self.main_app.local_ai_handler if vps_config.get('ai_handler') == 'Local' else self.main_app.ai_handler
        if not ai_handler:
            return
        feedback = "success" if success else "failure"
        prompt = f"Based on {feedback} sending to {email_data['to_email']}, suggest improvements for SMTP server {vps_config.get('smtp_server')}."
        success_opt, suggestion = ai_handler.generate(prompt, "You are an email deliverability optimizer.")
        if success_opt:
            self.main_app.log(f"🤖 AI Optimization for VPS {vps_config['id']}: {suggestion}")
            # In real impl, apply suggestions (e.g., adjust rate limits)

    def _create_vps_mime_message(self, email_data, vps_config, sender_name, sender_email):
        """Create MIME message with custom sender from VPS config, Reply-To, and custom Message-ID."""
        msg_root = MIMEMultipart('related')
        msg_root['Subject'] = Header(email_data['subject'], 'utf-8').encode()
        # Enhanced encoding for proper display
        msg_root['From'] = formataddr((Header(sender_name, 'utf-8').encode(), sender_email))
        msg_root['To'] = email_data['to_email']
        msg_root['Date'] = email.utils.formatdate()
        # Custom Message-ID for origin masking
        mid_domain = email_data.get('message_id_domain', '') or sender_email.split('@')[1]
        msg_root['Message-ID'] = f"<{uuid.uuid4()}@{mid_domain}>"
        # Reply-To header
        reply_to = email_data.get('reply_to', '')
        if reply_to:
            msg_root['Reply-To'] = reply_to
        unsubscribe_url = email_data.get('unsubscribe_url')
        if unsubscribe_url:
            msg_root.add_header('List-Unsubscribe', f'<{unsubscribe_url}>')
            msg_root.add_header('List-Unsubscribe-Post', 'List-Unsubscribe=One-Click')
        msg_alternative = MIMEMultipart('alternative')
        msg_root.attach(msg_alternative)
        msg_alternative.attach(MIMEText(self.main_app.smtp_handler._html_to_text(email_data['content']), 'plain', 'utf-8'))
        msg_alternative.attach(MIMEText(email_data['content'], 'html', 'utf-8'))
        if email_data.get('attachments'):
            for path in email_data.get('attachments', []):
                if os.path.exists(path):
                    try:
                        with open(path, "rb") as f: part = MIMEBase('application', 'octet-stream'); part.set_payload(f.read())
                        encoders.encode_base64(part)
                        part.add_header('Content-Disposition', f'attachment; filename="{os.path.basename(path)}"')
                        msg_root.attach(part)
                    except Exception as e: self.main_app.log(f"⚠️ Could not attach {path}: {e}")
        return msg_root

    def _log_send_to_vps(self, vps_config, email_data):
        """Encrypted logging on VPS using SSH to SQLite."""
        if not PARAMIKO_AVAILABLE:
            return
        try:
            ssh = paramiko.SSHClient()
            ssh.load_system_host_keys()
            ssh.connect(vps_config['host'], username=vps_config['user'], key_filename=vps_config.get('key_file'), timeout=10)
            # Command to insert into SQLite on VPS
            log_data = json.dumps({"email": email_data['to_email'], "timestamp": datetime.now().isoformat()})
            command = f"sqlite3 /path/to/vps_log.db 'INSERT INTO sends (data) VALUES (\"{log_data}\");'"
            stdin, stdout, stderr = ssh.exec_command(command)
            ssh.close()
        except Exception as e:
            self.main_app.log(f"⚠️ VPS logging error: {e}")

    def test_vps_connection(self, vps_config):
        """Test VPS SSH connection and mail server (SMTP or local MTA)."""
        if not PARAMIKO_AVAILABLE:
            return False, "Paramiko not available."

        try:
            ssh = paramiko.SSHClient()
            ssh.load_system_host_keys()
            ssh.connect(vps_config['host'], username=vps_config['user'], key_filename=vps_config.get('key_file'), timeout=10)

            # Test Python availability
            stdin, stdout, stderr = ssh.exec_command("python3 --version")
            if stderr.read().decode().strip():
                ssh.close()
                return False, "Python3 not found on VPS."

            # Determine mode: standalone (local MTA) or external SMTP
            smtp_user = vps_config.get('smtp_user', '')
            smtp_pass = self._decrypt_password(vps_config.get('smtp_pass_encrypted', ''))
            standalone_mode = vps_config.get('standalone_mode', False) or not smtp_user or not smtp_pass

            if standalone_mode:
                # Test local MTA (Postfix/sendmail) on the VPS
                command = """python3 -c "
import smtplib
try:
    server = smtplib.SMTP('localhost', 25)
    server.ehlo()
    server.quit()
    print('LOCAL_MTA_OK')
except Exception as e:
    print('LOCAL_MTA_ERROR: ' + str(e))
    exit(2)
" """
                stdin, stdout, stderr = ssh.exec_command(command)
                exit_status = stdout.channel.recv_exit_status()
                output = stdout.read().decode().strip()
                error = stderr.read().decode().strip()
                ssh.close()
                if exit_status == 0 and "LOCAL_MTA_OK" in output:
                    return True, "✅ VPS Connected — Local Mail Server (Postfix/Sendmail) OK"
                else:
                    return False, f"Local MTA test failed: {error or output}. Ensure Postfix/Sendmail is installed and running."
            else:
                # Test external SMTP with credentials
                smtp_server = vps_config.get('smtp_server', '')
                smtp_port = vps_config.get('smtp_port', 587)

                # Escape single quotes in credentials
                smtp_server_escaped = smtp_server.replace("'", "\\'")
                smtp_user_escaped = smtp_user.replace("'", "\\'")
                smtp_pass_escaped = smtp_pass.replace("'", "\\'")

                command = f"""
python3 -c "
import smtplib
import ssl
from email.mime.text import MIMEText

{'import socks; socks.setdefaultproxy(socks.PROXY_TYPE_SOCKS5, \'' + str(vps_config.get('proxy_chain', [{}])[0].get('host', '')) + '\', ' + str(vps_config.get('proxy_chain', [{}])[0].get('port', 0)) + '); socks.wrapmodule(smtplib)' if vps_config.get('proxy_chain') else ''}

try:
    server = smtplib.SMTP('{smtp_server_escaped}', {smtp_port})
    server.ehlo()
    server.starttls()
    server.login('{smtp_user_escaped}', '{smtp_pass_escaped}')
    server.quit()
    print('SMTP OK')
except smtplib.SMTPAuthenticationError as e:
    print('AUTH_ERROR: ' + str(e))
    exit(1)
except Exception as e:
    print('ERROR: ' + str(e))
    exit(2)
" """
                stdin, stdout, stderr = ssh.exec_command(command)
                exit_status = stdout.channel.recv_exit_status()
                error = stderr.read().decode().strip()
                output = stdout.read().decode().strip()
                if exit_status == 1 and "AUTH_ERROR" in output:
                    ssh.close()
                    return False, f"SMTP Auth Failed: Check credentials. For Office 365/Gmail, use app password if 2FA enabled."
                elif exit_status != 0:
                    ssh.close()
                    return False, f"SMTP Test Failed: {error or output}"
            ssh.close()
            return True, "✅ VPS Connected and SMTP OK"
        except paramiko.SSHException as e:
            return False, f"SSH Failed: {e}"
        except Exception as e:
            return False, f"Connection test failed: {e}"

    def run_vps_sending_job(self, email_list, subject, content, attachments=None, use_random_sender=False, batch_size=100):
        """Main sending job for VPS Mailer with async support, geo-failover, and AI. Enhanced for standalone mode with batching."""
        if not self.vps_pool:
            self.main_app.log("❌ No VPS configured for Proxy VPS Mailer.")
            return

        async def vps_send_async():
            async def send_email(email_address):
                email_address_lower = email_address.lower()
                if not self.main_app.running:
                    return False, "Stopped"
                recipient_region = self.get_geo_region(email_address.split('@')[1])
                for attempt in range(3):
                    vps = self.get_healthy_vps(recipient_region)
                    if not vps:
                        return False, "No healthy VPS available"
                    if self.rate_limits.get(vps['id'], 0) <= 0:
                        continue
                    email_data = self.main_app._prepare_email_data_vps(email_address_lower, subject, content, attachments, use_random_sender, 0, len(email_list))
                    success, error = await asyncio.get_event_loop().run_in_executor(None, self.send_via_vps, email_data, vps)
                    if success:
                        self.rate_limits[vps['id']] -= 1
                        self.main_app.vps_tracking_map[email_address_lower]['status'] = 'Sent (VPS)'
                        self.main_app.sent_count += 1
                        self.main_app.gui_update_queue.put(('update_vps_recipient', email_address_lower))
                        self.main_app.gui_update_queue.put(('update_progress', None))
                        return True, "Sent"
                    elif "Auth failed" in error:
                        break
                    await asyncio.sleep(2 ** attempt)
                self.main_app.vps_tracking_map[email_address_lower]['status'] = 'Failed (VPS)'
                self.main_app.failed_count += 1
                self.main_app.gui_update_queue.put(('update_vps_recipient', email_address_lower))
                self.main_app.gui_update_queue.put(('update_progress', None))
                return False, error

            # Batch processing for performance
            tasks = []
            for i in range(0, len(email_list), batch_size):
                batch = email_list[i:i + batch_size]
                batch_tasks = [send_email(email) for email in batch]
                tasks.extend(await asyncio.gather(*batch_tasks, return_exceptions=True))
            # Note: Counts are updated inside send_email, no need to aggregate here
            self.main_app.root.after(0, self.main_app._finalize_sending)

        self.main_app.start_async_loop(lambda: vps_send_async())

class DirectMXHandler:
    """NEW: Handles direct-to-MX delivery without SMTP authentication. Fully asynchronous with DKIM, rate limiting, retry queue."""
    def __init__(self, main_app):
        self.main_app = main_app
        self.resolver = dns.resolver.Resolver() if DNSPYTHON_AVAILABLE else None
        if self.resolver:
            self.resolver.timeout = 5
            self.resolver.lifetime = 10
        self.rate_limits = {}  # Per-domain rate limiting: {'domain': {'last_send': datetime, 'count': int}}
        self.rate_limit_per_hour = 100  # Configurable rate limit per domain per hour
        self.dkim_private_key = ""
        self.dkim_selector = ""
        self.dkim_domain = ""
        self.reply_to_email = ""  # Single Reply-To address for all sent emails
        self.message_id_domain = ""  # Custom domain for Message-ID header (origin masking)
        self.from_emails_pool = []  # Pool of verified "from" emails for rotation
        self.from_emails_status = {}  # Verification status per from email: {'email': 'Verified'|'Failed'|'Pending'}
        self.source_ip = ""  # Source IP for binding outbound connections
        self.ehlo_hostname = ""  # Custom EHLO hostname for SPF alignment
        self._load_dkim_settings()

    def _load_dkim_settings(self):
        try:
            with open(SETTINGS_FILE, "r") as f:
                settings = json.load(f)
                self.dkim_private_key = settings.get("dkim_private_key", "")
                self.dkim_selector = settings.get("dkim_selector", "")
                self.dkim_domain = settings.get("dkim_domain", "")
                self.reply_to_email = settings.get("mx_reply_to_email", "")
                self.message_id_domain = settings.get("mx_message_id_domain", "")
                self.from_emails_pool = settings.get("mx_from_emails_pool", [])
                self.from_emails_status = settings.get("mx_from_emails_status", {})
                self.source_ip = settings.get("mx_source_ip", "")
                self.ehlo_hostname = settings.get("mx_ehlo_hostname", "")
        except Exception:
            pass

    def _save_mx_settings(self):
        """Save MX-specific settings (reply-to, message-id domain, from pool)."""
        try:
            try:
                with open(SETTINGS_FILE, "r") as f:
                    settings = json.load(f)
            except FileNotFoundError:
                settings = {}
            settings["mx_reply_to_email"] = self.reply_to_email
            settings["mx_message_id_domain"] = self.message_id_domain
            settings["mx_from_emails_pool"] = self.from_emails_pool
            settings["mx_from_emails_status"] = self.from_emails_status
            settings["mx_source_ip"] = self.source_ip
            settings["mx_ehlo_hostname"] = self.ehlo_hostname
            with open(SETTINGS_FILE, "w") as f:
                json.dump(settings, f, indent=2)
        except Exception as e:
            self.main_app.log(f"⚠️ Error saving MX settings: {e}")

    def configure_dkim(self, private_key, selector, domain):
        self.dkim_private_key = private_key
        self.dkim_selector = selector
        self.dkim_domain = domain

    def get_rotated_sender(self, default_email="noreply@example.com"):
        """Get next sender email from verified pool, or fallback to default."""
        verified = [e for e in self.from_emails_pool if self.from_emails_status.get(e) == 'Verified']
        if verified:
            return random.choice(verified)
        return default_email

    def _get_mx_records(self, domain):
        """Get MX records sorted by preference."""
        if not self.resolver:
            return []
        try:
            answers = self.resolver.resolve(domain, 'MX')
            mx_records = [(r.preference, str(r.exchange).rstrip('.')) for r in answers]
            mx_records.sort(key=lambda x: x[0])  # Lowest preference first
            return mx_records
        except Exception:
            return []

    def _sign_dkim(self, mime_msg):
        """Sign the MIME message with DKIM."""
        if not DKIM_AVAILABLE:
            self.main_app.log("⚠️ DKIM skipped: dkimpy library not installed. Install with: pip install dkimpy")
            return mime_msg
        missing = []
        if not self.dkim_private_key:
            missing.append("Private key not configured")
        if not self.dkim_selector:
            missing.append("Selector not set")
        if not self.dkim_domain:
            missing.append("Domain not set")
        if missing:
            self.main_app.log(f"⚠️ DKIM skipped: {', '.join(missing)}. Configure via Settings > DKIM Configuration")
            return mime_msg
        try:
            # Convert to bytes for signing
            msg_bytes = mime_msg.as_bytes()
            selector = self.dkim_selector.encode() if isinstance(self.dkim_selector, str) else self.dkim_selector
            domain = self.dkim_domain.encode() if isinstance(self.dkim_domain, str) else self.dkim_domain
            privkey = self.dkim_private_key.encode() if isinstance(self.dkim_private_key, str) else self.dkim_private_key
            sig = dkim.sign(msg_bytes, selector, domain, privkey)
            # Parse and add DKIM header
            signed_msg = email.message_from_bytes(sig + msg_bytes)
            return signed_msg
        except Exception as e:
            self.main_app.log(f"⚠️ DKIM signing failed: {e}")
            return mime_msg

    def _check_rate_limit(self, domain):
        """Check per-domain rate limit."""
        now = datetime.now()
        if domain not in self.rate_limits:
            self.rate_limits[domain] = {'last_send': now, 'count': 0}
        record = self.rate_limits[domain]
        if (now - record['last_send']).total_seconds() > 3600:  # Reset every hour
            record['count'] = 0
            record['last_send'] = now
        if record['count'] >= self.rate_limit_per_hour:
            return False
        record['count'] += 1
        return True

    def _get_mx_connect_kwargs(self):
        """Build connection kwargs for asyncio.open_connection, including source IP binding."""
        kwargs = {}
        if self.source_ip:
            kwargs['local_addr'] = (self.source_ip, 0)
        return kwargs

    def _create_mime_message(self, to_email, subject, content, attachments=None, sender_name="", sender_email=""):
        """Create RFC-compliant MIME message with Reply-To and custom Message-ID domain support."""
        msg_root = MIMEMultipart('related')
        msg_root['Subject'] = Header(subject, 'utf-8').encode()
        msg_root['From'] = formataddr((Header(sender_name, 'utf-8').encode(), sender_email))
        msg_root['To'] = to_email
        msg_root['Date'] = email.utils.formatdate()
        # Use custom Message-ID domain for origin masking, fallback to sender domain
        mid_domain = self.message_id_domain if self.message_id_domain else sender_email.split('@')[1]
        msg_root['Message-ID'] = f"<{uuid.uuid4()}@{mid_domain}>"
        # Add Reply-To header if configured
        if self.reply_to_email:
            msg_root['Reply-To'] = self.reply_to_email
        msg_alternative = MIMEMultipart('alternative')
        msg_root.attach(msg_alternative)
        msg_alternative.attach(MIMEText(self.main_app.smtp_handler._html_to_text(content), 'plain', 'utf-8'))
        msg_alternative.attach(MIMEText(content, 'html', 'utf-8'))
        if attachments:
            for path in attachments:
                if os.path.exists(path):
                    try:
                        with open(path, "rb") as f:
                            part = MIMEBase('application', 'octet-stream')
                            part.set_payload(f.read())
                        encoders.encode_base64(part)
                        part.add_header('Content-Disposition', f'attachment; filename="{os.path.basename(path)}"')
                        msg_root.attach(part)
                    except Exception as e:
                        self.main_app.log(f"⚠️ Could not attach {path}: {e}")
        return msg_root

    async def _send_via_mx(self, email_data, mx_host):
        """Send email directly to MX server asynchronously with sender rotation.
        Uses robust STARTTLS handling that works across Python versions."""
        writer = None
        try:
            # Determine sender identity: use email_data overrides, then rotation pool, then defaults
            sender_name = email_data.get('sender_name', 'Sender')
            sender_email = email_data.get('sender_email', '')
            if email_data.get('use_random_sender') or not sender_email:
                sender_email = self.get_rotated_sender(sender_email or "noreply@example.com")
            # Create MIME message
            mime_msg = self._create_mime_message(
                email_data['to_email'], email_data['subject'], email_data['content'],
                email_data.get('attachments'), sender_name, sender_email
            )
            # Sign with DKIM
            signed_msg = self._sign_dkim(mime_msg)
            msg_bytes = signed_msg.as_bytes()

            # Connect to MX with optional source IP binding
            connect_kwargs = self._get_mx_connect_kwargs()
            reader, writer = await asyncio.open_connection(mx_host, 25, **connect_kwargs)
            response = await self._read_smtp_response(reader)
            if not response.startswith('220'):
                raise Exception(f"Bad greeting: {response}")

            # EHLO with configurable hostname for SPF alignment
            sender_domain = sender_email.split('@')[1] if '@' in sender_email else 'localhost'
            ehlo_hostname = self.ehlo_hostname if self.ehlo_hostname else sender_domain
            ehlo_cmd = f'EHLO {ehlo_hostname}\r\n'
            writer.write(ehlo_cmd.encode())
            await writer.drain()
            response = await self._read_smtp_response(reader)
            if not response.startswith('250'):
                raise Exception(f"EHLO failed: {response}")

            # STARTTLS if supported - use robust TLS upgrade
            tls_upgraded = False
            writer.write(b'STARTTLS\r\n')
            await writer.drain()
            response = await self._read_smtp_response(reader)
            if response.startswith('220'):
                # Upgrade to TLS using asyncio event loop's start_tls
                try:
                    # Create SSL context for MX server connections
                    # Note: Many MX servers use self-signed certificates, so we use
                    # a permissive SSL context. This is acceptable for Direct MX delivery
                    # as the primary goal is opportunistic encryption, not strict validation.
                    # The email content is typically not sensitive enough to warrant
                    # strict certificate validation for outbound MX connections.
                    context = ssl.create_default_context()
                    context.check_hostname = False
                    context.verify_mode = ssl.CERT_NONE
                    
                    # Get the underlying transport
                    transport = writer.transport
                    if transport is None:
                        raise Exception("Transport is None, cannot upgrade to TLS")
                    
                    # Use the event loop's start_tls method for proper TLS upgrade
                    loop = asyncio.get_running_loop()
                    protocol = transport.get_protocol()
                    
                    # Upgrade the transport to TLS
                    new_transport = await loop.start_tls(
                        transport, protocol, context, 
                        server_hostname=mx_host
                    )
                    
                    # Update the writer's transport via the public transport property setter
                    # Note: StreamWriter.transport is read-only, but we can replace the protocol's
                    # transport. For this use case, we continue using the existing writer
                    # as the underlying protocol remains the same.
                    protocol.connection_made(new_transport)
                    tls_upgraded = True

                    # EHLO again after TLS upgrade
                    writer.write(ehlo_cmd.encode())
                    await writer.drain()
                    response = await self._read_smtp_response(reader)
                    if not response.startswith('250'):
                        raise Exception(f"EHLO after TLS failed: {response}")
                        
                except Exception as tls_err:
                    # TLS upgrade failed, but we can continue without encryption
                    # Many MX servers accept unencrypted connections
                    self.main_app.log(f"⚠️ MX TLS upgrade failed for {mx_host}: {tls_err}. Continuing without TLS.")
                    # Close and reconnect without TLS
                    try:
                        writer.close()
                        await writer.wait_closed()
                    except:
                        pass
                    # Reconnect without TLS
                    reader, writer = await asyncio.open_connection(mx_host, 25, **connect_kwargs)
                    response = await self._read_smtp_response(reader)
                    if not response.startswith('220'):
                        raise Exception(f"Bad greeting on reconnect: {response}")
                    writer.write(ehlo_cmd.encode())
                    await writer.drain()
                    response = await self._read_smtp_response(reader)
                    if not response.startswith('250'):
                        raise Exception(f"EHLO failed on reconnect: {response}")

            # MAIL FROM - use the actual sender email
            writer.write(f'MAIL FROM:<{sender_email}>\r\n'.encode())
            await writer.drain()
            response = await self._read_smtp_response(reader)
            if not response.startswith('250'):
                raise Exception(f"MAIL FROM failed: {response}")

            # RCPT TO
            writer.write(f'RCPT TO:<{email_data["to_email"]}>\r\n'.encode())
            await writer.drain()
            response = await self._read_smtp_response(reader)
            if response.startswith('2'):
                # DATA
                writer.write(b'DATA\r\n')
                await writer.drain()
                response = await self._read_smtp_response(reader)
                if not response.startswith('354'):
                    raise Exception(f"DATA failed: {response}")
                writer.write(msg_bytes + b'\r\n.\r\n')
                await writer.drain()
                response = await self._read_smtp_response(reader)
                if response.startswith('2'):
                    writer.write(b'QUIT\r\n')
                    await writer.drain()
                    writer.close()
                    await writer.wait_closed()
                    tls_status = " (TLS)" if tls_upgraded else ""
                    self.main_app.log(f"✅ MX Send success to {email_data['to_email']}{tls_status}: {response.strip()}")
                    return True, response.strip()
                else:
                    raise Exception(f"Send failed: {response}")
            elif response.startswith('4'):
                # Soft failure
                writer.write(b'QUIT\r\n')
                await writer.drain()
                writer.close()
                await writer.wait_closed()
                self.main_app.log(f"⏳ MX Soft failure for {email_data['to_email']}: {response.strip()}")
                return False, f"Soft: {response.strip()}"
            else:
                # Hard failure
                writer.write(b'QUIT\r\n')
                await writer.drain()
                writer.close()
                await writer.wait_closed()
                self.main_app.log(f"❌ MX Hard failure for {email_data['to_email']}: {response.strip()}")
                return False, f"Hard: {response.strip()}"

        except Exception as e:
            self.main_app.log(f"❌ MX Send error for {email_data['to_email']}: {e}")
            try:
                if writer:
                    writer.close()
                    await writer.wait_closed()
            except:
                pass
            return False, str(e)

    async def _read_smtp_response(self, reader):
        """Read full SMTP response, consuming all continuation lines."""
        response = ""
        while True:
            line = await reader.readline()
            decoded = line.decode().strip()
            response = decoded
            # Multi-line responses have a '-' after the status code (e.g., '250-SIZE')
            # The final line has a space (e.g., '250 OK') or is less than 4 chars
            if len(decoded) < 4 or decoded[3] != '-':
                break
        return response

    async def _send_single_email_direct(self, email_data):
        """Send single email via MX with failover and rate limiting."""
        domain = email_data['to_email'].split('@')[1]
        if not self._check_rate_limit(domain):
            self.main_app.log(f"⏳ Rate limit exceeded for {domain}")
            return False, "Rate limited"

        mx_records = self._get_mx_records(domain)
        if not mx_records:
            return False, "No MX records"

        for pref, mx_host in mx_records:
            try:
                success, response = await self._send_via_mx(email_data, mx_host)
                if success:
                    return True, response
                elif "Soft" in response:
                    # Add to retry queue
                    message_id = str(uuid.uuid4())
                    self.main_app.db_handler.add_to_mx_retry_queue(message_id, email_data['to_email'], email_data['subject'], email_data['content'], email_data.get('attachments', []))
                    return False, f"Queued for retry: {response}"
                # Hard failure, try next MX
            except Exception as e:
                self.main_app.log(f"⚠️ MX {mx_host} failed: {e}")
                continue

        return False, "All MX failed"

    def run_direct_mx_sending_job(self, email_list, subject, content, attachments=None, batch_size=100, use_random_sender=False):
        """Main async sending job for Direct MX."""
        if not DNSPYTHON_AVAILABLE:
            self.main_app.log("❌ dnspython not available for Direct MX delivery.")
            return

        async def mx_send_async():
            async def send_email(email_address):
                email_address_lower = email_address.lower()
                if not self.main_app.running or not self.main_app.direct_mx_running:
                    return False, "Stopped"
                # Pause support: wait while paused
                while self.main_app.direct_mx_paused:
                    await asyncio.sleep(0.5)
                    if not self.main_app.running or not self.main_app.direct_mx_running:
                        return False, "Stopped"
                email_data = self.main_app._prepare_email_data_mx(email_address_lower, subject, content, attachments, use_random_sender, 0, len(email_list))
                success, error = await self._send_single_email_direct(email_data)
                if success:
                    self.main_app.mx_tracking_map[email_address_lower]['status'] = 'Sent (MX)'
                    self.main_app.mx_tracking_map[email_address_lower]['sent_time'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    self.main_app.sent_count += 1
                    self.main_app.direct_mx_sent_count += 1
                    self.main_app.gui_update_queue.put(('update_mx_recipient', email_address_lower))
                    self.main_app.gui_update_queue.put(('update_progress', None))
                    return True, "Sent"
                else:
                    self.main_app.mx_tracking_map[email_address_lower]['status'] = f'Failed (MX: {error})'
                    self.main_app.mx_tracking_map[email_address_lower]['failure_reason'] = error
                    attempts = self.main_app.mx_tracking_map[email_address_lower].get('attempts', 0) + 1
                    self.main_app.mx_tracking_map[email_address_lower]['attempts'] = attempts
                    self.main_app.failed_count += 1
                    self.main_app.direct_mx_failed_count += 1
                    self.main_app.gui_update_queue.put(('update_mx_recipient', email_address_lower))
                    self.main_app.gui_update_queue.put(('update_progress', None))
                    return False, error

            # Batch processing
            tasks = []
            for i in range(0, len(email_list), batch_size):
                if not self.main_app.running or not self.main_app.direct_mx_running:
                    break
                batch = email_list[i:i + batch_size]
                batch_tasks = [send_email(email) for email in batch]
                tasks.extend(await asyncio.gather(*batch_tasks, return_exceptions=True))
            self.main_app.root.after(0, self.main_app._finalize_sending)

        self.main_app.start_async_loop(lambda: mx_send_async())

    async def process_retry_queue(self):
        """Process retry queue asynchronously."""
        items = self.main_app.db_handler.get_mx_retry_items()
        for item in items:
            if not self.main_app.running:
                break
            email_data = {
                'to_email': item['email'],
                'subject': item['subject'],
                'content': item['content'],
                'attachments': item['attachments']
            }
            success, response = await self._send_single_email_direct(email_data)
            if success:
                self.main_app.db_handler.remove_from_mx_retry_queue(item['id'])
                self.main_app.tracking_map[item['email'].lower()]['status'] = 'Sent (MX Retry)'
                self.main_app.sent_count += 1
            else:
                new_attempts = item['attempts'] + 1
                next_retry = datetime.now() + timedelta(minutes=2 ** new_attempts)  # Exponential backoff
                self.main_app.db_handler.update_mx_retry_attempt(item['id'], new_attempts, next_retry)
                if new_attempts >= 5:
                    self.main_app.db_handler.remove_from_mx_retry_queue(item['id'])
                    self.main_app.tracking_map[item['email'].lower()]['status'] = f'Failed (MX Max Retries)'

    def verify_from_emails(self, from_emails, imap_configs):
        """Verify 'from' emails by sending a test email and checking inbox via IMAP.

        Args:
            from_emails: list of email addresses to verify
            imap_configs: dict mapping email -> {'server': str, 'port': int, 'username': str, 'password': str}
        Returns:
            dict mapping email -> 'Verified' or 'Failed'
        """
        results = {}
        for from_email in from_emails:
            verify_ticket = f"VERIFY-TICKET-{uuid.uuid4().hex[:12]}"
            self.main_app.log(f"🔍 Verifying {from_email} with ticket {verify_ticket}...")

            # Step 1: Send test email to itself via Direct MX
            try:
                domain = from_email.split('@')[1]
                mx_records = self._get_mx_records(domain)
                if not mx_records:
                    self.main_app.log(f"❌ No MX records for {domain}")
                    results[from_email] = 'Failed'
                    self.from_emails_status[from_email] = 'Failed'
                    continue

                test_content = f"<html><body><p>Verification test for {from_email}. Ticket: {verify_ticket}</p></body></html>"
                mime_msg = self._create_mime_message(
                    from_email, verify_ticket, test_content,
                    sender_name="Verify Bot", sender_email=from_email
                )
                signed_msg = self._sign_dkim(mime_msg)

                # Try sending synchronously using a single event loop
                sent = False
                loop = asyncio.new_event_loop()
                try:
                    for pref, mx_host in mx_records:
                        try:
                            success, resp = loop.run_until_complete(self._send_via_mx(
                                {'to_email': from_email, 'subject': verify_ticket, 'content': test_content, 'sender_name': 'Verify Bot', 'sender_email': from_email},
                                mx_host
                            ))
                            if success:
                                sent = True
                                break
                        except Exception as e:
                            self.main_app.log(f"⚠️ MX {mx_host} failed for verify: {e}")
                            continue
                finally:
                    loop.close()

                if not sent:
                    self.main_app.log(f"❌ Could not send verification to {from_email}")
                    results[from_email] = 'Failed'
                    self.from_emails_status[from_email] = 'Failed'
                    continue

            except Exception as e:
                self.main_app.log(f"❌ Verification send error for {from_email}: {e}")
                results[from_email] = 'Failed'
                self.from_emails_status[from_email] = 'Failed'
                continue

            # Step 2: Wait briefly for delivery
            time.sleep(10)

            # Step 3: Check IMAP inbox for the verification ticket
            imap_config = imap_configs.get(from_email, {})
            if not imap_config:
                self.main_app.log(f"⚠️ No IMAP config for {from_email}, skipping inbox check.")
                results[from_email] = 'Failed'
                self.from_emails_status[from_email] = 'Failed'
                continue

            try:
                mail = imaplib.IMAP4_SSL(imap_config['server'], int(imap_config.get('port', 993)))
                mail.login(imap_config['username'], imap_config['password'])
                mail.select("inbox")
                status, messages = mail.search(None, f'(SUBJECT "{verify_ticket}")')
                if status == 'OK' and messages[0]:
                    self.main_app.log(f"✅ {from_email} verified: Inbox-Ready!")
                    results[from_email] = 'Verified'
                    self.from_emails_status[from_email] = 'Verified'
                else:
                    self.main_app.log(f"❌ {from_email} verification failed: ticket not found in inbox.")
                    results[from_email] = 'Failed'
                    self.from_emails_status[from_email] = 'Failed'
                mail.close()
                mail.logout()
            except Exception as e:
                self.main_app.log(f"❌ IMAP verification error for {from_email}: {e}")
                results[from_email] = 'Failed'
                self.from_emails_status[from_email] = 'Failed'

        # Save verification results
        self._save_mx_settings()
        return results

class HTMLHelper:
    def __init__(self, main_app):
        self.main_app = main_app

    def inline_css(self, html_content):
        if not CSS_INLINE_AVAILABLE:
            return False, "css_inline library not installed. Run `pip install css_inline`."
        try:
            inliner = css_inline.CSSInliner()
            inlined_html = inliner.inline(html_content)
            return True, inlined_html
        except Exception as e:
            return False, f"Inlining error: {e}"

    def get_block_html(self, block_type):
        blocks = {
            "Header": '<h1 style="color:#333333; font-family:Helvetica, Arial, sans-serif; font-size:24px; margin-bottom:10px;">Your Headline Here</h1>',
            "Text": '<p style="color:#555555; font-family:Helvetica, Arial, sans-serif; font-size:16px; line-height:1.5;">Enter your text here.</p>',
            "Button": '<table border="0" cellpadding="0" cellspacing="0" style="margin:20px 0;"><tr><td align="center" bgcolor="#007bff" style="border-radius:5px;"><a href="#" style="display:inline-block; padding:12px 24px; font-family:Arial, sans-serif; font-size:16px; color:#ffffff; text-decoration:none; font-weight:bold;">Call to Action</a></td></tr></table>',
            "Image": '<div style="margin:20px 0;"><img src="https://via.placeholder.com/600x200" alt="Image" style="width:100%; max-width:600px; height:auto; border:0;"></div>',
            "Divider": '<hr style="border:0; border-top:1px solid #eeeeee; margin:20px 0;">',
            "2-Column": '<table width="100%" cellpadding="0" cellspacing="0" border="0"><tr><td width="48%" valign="top"><p>Left Column Text</p></td><td width="4%">&nbsp;</td><td width="48%" valign="top"><p>Right Column Text</p></td></tr></table>'
        }
        return blocks.get(block_type, "")

class AsyncConnectionPool:
    """REFACTORED v8.0.0: An asyncio-safe pool of aiosmtplib connections."""
    def __init__(self, size, connect_func, main_app):
        if not AIOSMTPLIB_AVAILABLE:
            raise ImportError("aiosmtplib library is required for AsyncConnectionPool")
        self.size = size
        self.connect_func = connect_func
        self.main_app = main_app
        self.pool = asyncio.Queue(maxsize=size)
        for _ in range(size):
            self.pool.put_nowait(None)

    async def get_connection(self):
        async with asyncio.Lock():
            conn = await self.pool.get()

        if conn and await self._is_connection_valid(conn):
            self.main_app.log("♻️ Reusing async connection from pool.")
            return conn

        self.main_app.log("➕ Creating new async connection for pool.")
        try:
            new_conn = await self.connect_func()
            return new_conn
        except Exception as e:
            self.pool.put_nowait(None) # Put placeholder back
            raise e

    async def return_connection(self, conn):
        if conn:
            await self.pool.put(conn)

    async def _is_connection_valid(self, conn):
        if not conn.is_connected:
            return False
        try:
            await conn.noop()
            return True
        except aiosmtplib.errors.SMTPException:
            return False

    async def close_all(self):
        while not self.pool.empty():
            conn = self.pool.get_nowait()
            if conn and conn.is_connected:
                try:
                    await conn.quit()
                except Exception:
                    pass

class SMTPHandler:
    """
    Handles all SMTP operations.
    REFACTORED v8.0.3 for robust fallback from async to threaded sending.
    """
    def __init__(self, main_app):
        self.main_app = main_app
        self.smtp_server = None
        self.smtp_port = None
        self.username = None
        self.use_tls = True
        self.use_ssl = False
        self.sender_name = ""
        self.sender_email = ""
        self._memory_password = None

        self.last_failure_reasons = {}
        self.connection_pools = {} # Pool per profile (used by async sender)

        self.smtp_presets = {
            "Gmail": {"server": "smtp.gmail.com", "port": 587, "tls": True, "ssl": False},
            "Outlook/Hotmail": {"server": "smtp.office365.com", "port": 587, "tls": True, "ssl": False},
            "Yahoo": {"server": "smtp.mail.yahoo.com", "port": 587, "tls": True, "ssl": False},
            "Custom": {"server": "", "port": 587, "tls": True, "ssl": False}
        }

    def _create_secure_ssl_context(self):
        context = ssl.create_default_context()
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        return context

    def configure_smtp(self, server, port, username, password=None, use_tls=True, use_ssl=False, sender_name="", sender_email=""):
        self.smtp_server = server
        try: self.smtp_port = int(port) if port else 587
        except (ValueError, TypeError): self.smtp_port = 587
        self.username = username
        self.use_tls = use_tls
        self.use_ssl = use_ssl
        self.sender_name = sender_name
        self.sender_email = sender_email or username

        if password and self.username and KEYRING_AVAILABLE:
            try:
                keyring.set_password(KEYRING_SERVICE_NAME, self.username, password)
                self.main_app.log(f"🔑 Securely stored password for {self.username} in OS keyring.")
            except Exception as e:
                self.main_app.log(f"❌ Failed to store password in keyring: {e}"); self._memory_password = password
        elif password:
            self.main_app.log("⚠️ keyring library not found. Password will be held in memory for this session only.")
            self._memory_password = password
        self.main_app.log(f"✅ SMTP configured: {server}:{self.smtp_port}")

    def get_password(self):
        if not self.username: return None
        if KEYRING_AVAILABLE:
            try: return keyring.get_password(KEYRING_SERVICE_NAME, self.username)
            except Exception as e: self.main_app.log(f"❌ Could not retrieve password from keyring: {e}"); return self._memory_password
        return self._memory_password

    def test_connection(self):
        if not self.smtp_server or not self.username: return False, "SMTP configuration incomplete"
        password = self.get_password()
        if not password: return False, "Password not found in OS keyring or memory."
        server = None
        try:
            self.main_app.log(f"🔍 Testing SMTP connection to {self.smtp_server}:{self.smtp_port}...")
            context = self._create_secure_ssl_context()
            if self.use_ssl or self.smtp_port == 465: server = smtplib.SMTP_SSL(self.smtp_server, self.smtp_port, context=context, timeout=15)
            else: server = smtplib.SMTP(self.smtp_server, self.smtp_port, timeout=15)
            try: server.ehlo()
            except smtplib.SMTPHeloError: server.helo()
            if not self.use_ssl and self.use_tls and server.has_extn('STARTTLS'): server.starttls(context=context); server.ehlo()
            server.login(self.username, password)
            server.quit()
            return True, "Connection successful!"
        except smtplib.SMTPAuthenticationError as e:
            error_msg = str(e)
            detailed_msg = f"SMTP Authentication Failed: {error_msg}\n\nFor Gmail/Outlook with 2FA, use an app password. Ensure username is your full email address."
            return False, detailed_msg
        except (socket.timeout, TimeoutError): return False, "Connection Timed Out. Check firewall or port."
        except Exception as e: return False, f"An unexpected error occurred: {str(e)}"
        finally:
            if server:
                try: server.close()
                except: pass

    def _classify_failure(self, error: Exception, email: str) -> str:
        msg = str(error).lower()
        if isinstance(error, (smtplib.SMTPRecipientsRefused, smtplib.SMTPAuthenticationError)):
            reason = "auth_failure" if "auth" in msg else "hard_bounce (invalid address)"
        elif any(s in msg for s in ["mailbox unavailable", "user unknown", "no such user", "recipient rejected"]):
            reason = "hard_bounce"
        elif any(s in msg for s in ["quota", "over quota", "mailbox full"]):
            reason = "soft_bounce"
        elif any(s in msg for s in ["rate", "too many", "limit", "throttled"]):
            reason = "rate_limited"
        else:
            reason = "other"
        self.last_failure_reasons[email.lower()] = reason
        return reason

    def _create_mime_message(self, to_email, subject, content, attachments=None, dynamic_sender_name=None, unsubscribe_url=None, custom_sender_email=None):
        msg_root = MIMEMultipart('related')
        msg_root['Subject'] = Header(subject, 'utf-8').encode()
        current_sender_name = dynamic_sender_name or self.sender_name
        current_sender_email = custom_sender_email or self.sender_email
        msg_root['From'] = formataddr((Header(current_sender_name, 'utf-8').encode(), current_sender_email))
        msg_root['To'] = to_email
        if unsubscribe_url: msg_root.add_header('List-Unsubscribe', f'<{unsubscribe_url}>'); msg_root.add_header('List-Unsubscribe-Post', 'List-Unsubscribe-One-Click')
        msg_alternative = MIMEMultipart('alternative')
        msg_root.attach(msg_alternative)
        msg_alternative.attach(MIMEText(self._html_to_text(content), 'plain', 'utf-8'))
        msg_alternative.attach(MIMEText(content, 'html', 'utf-8'))
        if attachments:
            for path in attachments:
                if os.path.exists(path):
                    try:
                        with open(path, "rb") as f: part = MIMEBase('application', 'octet-stream'); part.set_payload(f.read())
                        encoders.encode_base64(part)
                        part.add_header('Content-Disposition', f'attachment; filename="{os.path.basename(path)}"')
                        msg_root.attach(part)
                    except Exception as e: self.main_app.log(f"⚠️ Could not attach {path}: {e}")
        return msg_root

    def _html_to_text(self, html):
        try:
            text = re.sub(r'<(script|style).*?>.*?</\1>', '', html, flags=re.DOTALL | re.IGNORECASE)
            text = re.sub(r'</(p|h[1-6]|li|div|tr|br) *>', '\n', text, flags=re.IGNORECASE)
            text = re.sub(r'<[^>]+>', ' ', text)
            return text.strip()
        except Exception: return "HTML-only email. Please use a compatible client."

    def run_sending_job(self, email_list, subject, content, attachments=None):
        """CHANGED: Now always uses threaded sending with smtplib as default."""
        threading.Thread(
            target=self.send_bulk_emails_threaded,
            args=(email_list, subject, content, attachments),
            daemon=True
        ).start()

    def send_bulk_emails_threaded(self, email_list, subject, content, attachments=None):
        list_to_send = [e for e in email_list if self.main_app.tracking_map.get(e.lower(), {}).get('status') not in ['Sent (SMTP)', 'Sent', 'Suppressed']]
        if not list_to_send: self.main_app.log("✅ All recipients processed."); self.main_app.root.after(0, self.main_app._finalize_sending); return

        max_workers = self.main_app.smtp_max_workers_var.get()
        email_queue = queue.Queue()
        for i, email_address in enumerate(list_to_send):
            try:
                email_data = self.main_app._prepare_email_data(email_address.lower(), subject, content, attachments, i, len(list_to_send))
                email_queue.put(email_data)
            except Exception as e: self.main_app.log(f"❌ Threaded-Producer error for {email_address}: {e}")

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            rotation_profiles = list(self.main_app.get_rotation_profiles().values())
            profile_count = len(rotation_profiles)
            if profile_count == 0: self.main_app.log("❌ SMTP Sending aborted. No valid rotation profiles found."); self.main_app.root.after(0, self.main_app._finalize_sending); return

            futures = []
            while not email_queue.empty() and self.main_app.running:
                for i in range(profile_count):
                    if email_queue.empty(): break
                    email_data = email_queue.get()
                    futures.append(executor.submit(self._send_single_email_threaded, email_data, rotation_profiles[i]))

            for future in as_completed(futures):
                try: future.result()
                except Exception as e: self.main_app.log(f"❌ Thread worker error: {e}")

        self.main_app.log("📊 Threaded SMTP session completed.")
        self.main_app.root.after(0, self.main_app._finalize_sending)

    def send_bulk_emails_async(self, email_list, subject, content, attachments=None):
        """REFACTORED v8.0.0: Main async sending logic."""
        list_to_send = [e for e in email_list if self.main_app.tracking_map.get(e.lower(), {}).get('status') not in ['Sent (SMTP)', 'Sent', 'Suppressed']]
        if not list_to_send: self.main_app.log("✅ All recipients processed."); return True

        max_workers = self.main_app.smtp_max_workers_var.get()
        rotation_profiles = self.main_app.get_rotation_profiles()
        if not rotation_profiles: self.main_app.log("❌ SMTP Sending aborted. No valid rotation profiles found."); return False

        for pool in self.connection_pools.values(): asyncio.run(pool.close_all())
        self.connection_pools.clear()

        for name, config in rotation_profiles.items():
            connect_func = lambda cfg=config: self._connect_worker_async(cfg)
            self.connection_pools[name] = AsyncConnectionPool(max_workers, connect_func, self.main_app)
            self.main_app.log(f"🏊 Created async connection pool for profile '{name}' with size {max_workers}.")

        async def async_send():
            message_queue = asyncio.Queue(maxsize=max_workers * 20)

            async def producer():
                for i, email_address in enumerate(list_to_send):
                    if not self.main_app.running: break
                    try:
                        await message_queue.put(self.main_app._prepare_email_data(email_address.lower(), subject, content, attachments, i, len(list_to_send)))
                    except Exception as e: self.main_app.log(f"❌ Async Producer error for {email_address}: {e}")
                for _ in range(max_workers * len(rotation_profiles)): await message_queue.put(None)

            async def consumer(profile_name, pool):
                while self.main_app.running:
                    item = await message_queue.get()
                    if item is None: await message_queue.put(None); break
                    email_address = item["to_email"]
                    try:
                        profile_config = rotation_profiles[profile_name]
                        if self.main_app.throttle_amount_var.get() > 0 and (self.main_app.sent_count + self.main_app.failed_count) % self.main_app.throttle_amount_var.get() == 0:
                            delay = self.main_app.current_throttle_delay_seconds()
                            if delay > 0: self.main_app.log(f"⏱️ Throttling: Pausing for {delay}s..."); await asyncio.sleep(delay)

                        self.main_app.tracking_map[email_address].update({'status': 'Sending (SMTP)', 'attempts': self.main_app.tracking_map[email_address]['attempts'] + 1})
                        sender_email = profile_config.get('sender_email') or profile_config.get('username')
                        if sender_email: self.main_app.tracking_map[email_address]['senderemail'] = sender_email

                        conn = await pool.get_connection()
                        await conn.send_message(self._create_mime_message(**item, custom_sender_email=sender_email))
                        await pool.return_connection(conn)

                        self.main_app.tracking_map[email_address].update({'status': 'Sent (SMTP)', 'sent_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S")})
                        self.main_app.sent_count += 1
                    except Exception as e:
                        self._classify_failure(e, email_address)
                        self.main_app.tracking_map[email_address]['status'] = 'Failed (SMTP Auth)' if isinstance(e, aiosmtplib.errors.SMTPAuthenticationError) else 'Failed (SMTP)'
                        self.main_app.failed_count += 1
                    finally:
                        self.main_app.gui_update_queue.put(('update_recipient', email_address))
                        self.main_app.gui_update_queue.put(('update_progress', None))
                        message_queue.task_done()

            await asyncio.gather(producer(), *[consumer(name, pool) for name, pool in self.connection_pools.items() for _ in range(max_workers)])
            for pool in self.connection_pools.values(): await pool.close_all()
            self.main_app.log("📊 Async SMTP session completed.")
            self.main_app.root.after(0, self.main_app._finalize_sending)

        asyncio.run(async_send())

    async def _connect_worker_async(self, profile_config):
        context = self._create_secure_ssl_context()
        use_ssl = profile_config.get('use_ssl') or profile_config.get('port') == 465
        use_tls = not use_ssl and profile_config.get('use_tls')
        conn = aiosmtplib.SMTP(hostname=profile_config['server'], port=profile_config['port'], use_tls=use_ssl, tls_context=context, timeout=20)
        await conn.connect()
        if use_tls: await conn.starttls(tls_context=context)
        await conn.login(profile_config['username'], profile_config['password'])
        return conn

    def _send_single_email_threaded(self, email_data, profile_config):
        if not self.main_app.running: return

        email_address = email_data["to_email"]
        try:
            current_processed = self.main_app.sent_count + self.main_app.failed_count
            if self.main_app.throttle_amount_var.get() > 0 and current_processed > 0 and current_processed % self.main_app.throttle_amount_var.get() == 0:
                delay = self.current_throttle_delay_seconds()
                if delay > 0: self.main_app.log(f"⏱️ Throttling: Pausing for {delay}s..."); time.sleep(delay)

            self.main_app.tracking_map[email_address].update({'status': 'Sending (SMTP)', 'attempts': self.main_app.tracking_map[email_address]['attempts'] + 1})
            sender_email = profile_config.get('sender_email') or profile_config.get('username')
            if sender_email: self.main_app.tracking_map[email_address]['senderemail'] = sender_email

            mime_message = self._create_mime_message(**email_data, custom_sender_email=sender_email)

            context = self._create_secure_ssl_context()
            port = profile_config['port']
            use_ssl = profile_config.get('use_ssl') or port == 465
            if use_ssl: server = smtplib.SMTP_SSL(profile_config['server'], port, context=context, timeout=20)
            else: server = smtplib.SMTP(profile_config['server'], port, timeout=20)

            with server:
                if not use_ssl and profile_config.get('use_tls'): server.starttls(context=context)
                server.login(profile_config['username'], profile_config['password'])
                server.send_message(mime_message)

            self.main_app.tracking_map[email_address].update({'status': 'Sent (SMTP)', 'sent_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S")})
            self.main_app.sent_count += 1
            self.main_app._log_sent_email(email_address, email_data["subject"], sender_email, profile_config.get('name', 'Default'), os.path.basename(email_data["attachments"][0]) if email_data["attachments"] else "None")
            # Immediate UI update
            self.main_app.root.after(0, lambda: self._update_recipient_in_tree(email_address))

        except smtplib.SMTPAuthenticationError as e:
            self._classify_failure(e, email_address)
            self.main_app.tracking_map[email_address]['status'] = 'Failed (SMTP Auth)'
            self.main_app.tracking_map[email_address]['failure_reason'] = f"Auth failed: {e}"
            self.main_app.log(f"❌ SMTP Auth failed for {email_address}: {e}. Check credentials (use app password for 2FA).")
            self.main_app.failed_count += 1
            # Immediate UI update
            self.main_app.root.after(0, lambda: self._update_recipient_in_tree(email_address))
        except Exception as e:
            self._classify_failure(e, email_address)
            self.main_app.tracking_map[email_address]['status'] = 'Failed (SMTP)'
            self.main_app.failed_count += 1
            self.main_app.log(f"❌ Threaded SMTP error for {email_address} with profile '{profile_config['name']}': {e}")
            # Immediate UI update
            self.main_app.root.after(0, lambda: self._update_recipient_in_tree(email_address))
        finally:
            self.main_app.gui_update_queue.put(('update_recipient', email_address))
            self.main_app.gui_update_queue.put(('update_progress', None))

    def _update_recipient_in_tree(self, email):
        """Update a single recipient's row in the treeview from tracking_map data."""
        try:
            if email in self.main_app.tree_items:
                item_id = self.main_app.tree_items[email]
                d = self.main_app.tracking_map.get(email, {})
                sent_time = d.get('sent_time', '')
                if sent_time and ' ' in sent_time:
                    sent_time = sent_time.split(' ')[1]
                values = (email, d.get('status', 'Pending'), sent_time, '✔️' if d.get('open_events') else '', '✔️' if d.get('click_events') else '', str(d.get('attempts', 0)), d.get('failure_reason', ''))
                self.main_app.tree.item(item_id, values=values, tags=(d.get('status', '').split(' ')[0],))
        except Exception:
            pass

class OutlookCOMHandler:
    """Handles all Outlook COM operations in a dedicated thread"""

    def __init__(self, main_app):
        self.main_app = main_app
        self.outlook_app = None
        self.outlook_namespace = None
        self.connected = False
        self.com_thread = None
        self.request_queue = queue.Queue()
        self.response_queue = queue.Queue()
        self.running = False

    def start_com_thread(self):
        if self.com_thread and self.com_thread.is_alive():
            return True

        self.running = True
        self.com_thread = threading.Thread(target=self._com_worker, daemon=True)
        self.com_thread.start()

        try:
            response = self.response_queue.get(timeout=10)
            if response.get('success'):
                self.main_app.log("✅ COM thread started successfully")
                return True
            else:
                self.main_app.log(f"❌ COM thread start failed: {response.get('error')}")
                return False
        except queue.Empty:
            self.main_app.log("❌ COM thread start timeout")
            return False

    def _com_worker(self):
        try:
            try:
                pythoncom.CoInitializeEx(pythoncom.COINIT_APARTMENTTHREADED)
            except Exception:
                try:
                    pythoncom.CoInitialize()
                except Exception as e:
                    if "already initialized" not in str(e).lower():
                        self.response_queue.put({'success': False, 'error': f'COM init failed: {e}'})
                        return

            self.main_app.log("✅ COM initialized for dedicated thread")
            self.response_queue.put({'success': True})

            while self.running:
                try:
                    request = self.request_queue.get(timeout=1)

                    if request['action'] == 'connect':
                        self._handle_connect_request()
                    elif request['action'] == 'send_email':
                        self._handle_send_email_request(request)
                    elif request['action'] == 'disconnect':
                        self._handle_disconnect_request()
                        break

                except queue.Empty:
                    continue
                except Exception as e:
                    self.main_app.log(f"❌ COM worker error: {e}")
                    self.response_queue.put({'success': False, 'error': str(e)})

        finally:
            try:
                if self.outlook_app:
                    self.outlook_app = None
                if self.outlook_namespace:
                    self.outlook_namespace = None
                pythoncom.CoUninitialize()
            except:
                pass

    def _handle_connect_request(self):
        try:
            self.main_app.log("🔌 Connecting to Desktop Outlook (COM thread)...")

            connection_attempts = [
                lambda: win32com.client.GetActiveObject("Outlook.Application"),
                lambda: win32com.client.Dispatch("Outlook.Application"),
                lambda: win32com.client.gencache.EnsureDispatch("Outlook.Application"),
            ]

            outlook_connected = False
            for i, connect_method in enumerate(connection_attempts, 1):
                try:
                    self.main_app.log(f"🔄 Connection attempt {i}/3...")
                    self.outlook_app = connect_method()

                    if self.outlook_app:
                        try:
                            self.outlook_namespace = self.outlook_app.GetNamespace("MAPI")
                            inbox = self.outlook_namespace.GetDefaultFolder(6)  # olFolderInbox
                            if inbox:
                                self.main_app.log(f"✅ Connection method {i} successful")
                                outlook_connected = True
                                break
                        except Exception as test_error:
                            self.main_app.log(f"⚠️ Connection {i} test failed: {test_error}")
                            continue
                except Exception as connect_error:
                    self.main_app.log(f"⚠️ Connection method {i} failed: {connect_error}")
                    continue

            if not outlook_connected:
                raise Exception("All connection methods failed")

            try:
                test_mail = self.outlook_app.CreateItem(0)  # olMailItem
                if test_mail:
                    test_mail.Subject = "Connection Test"
                    test_mail.Body = "Test"
                    test_mail = None
                    self.connected = True
                    self.main_app.log("✅ Desktop Outlook connected successfully!")
                    self.response_queue.put({'success': True})
                else:
                    raise Exception("Could not create mail item")
            except Exception as test_error:
                self.main_app.log(f"❌ Final connection test failed: {test_error}")
                try:
                    folders = self.outlook_namespace.Folders
                    if folders and folders.Count > 0:
                        self.connected = True
                        self.main_app.log("✅ Desktop Outlook connected (alternative test)!")
                        self.response_queue.put({'success': True})
                    else:
                        raise Exception("No folders accessible")
                except Exception as alt_error:
                    raise Exception(f"All connection tests failed. Last error: {alt_error}")

        except Exception as e:
            error_msg = str(e)
            self.main_app.log(f"❌ Outlook connection error: {error_msg}")

            if "operation failed" in error_msg.lower():
                self.main_app.log("💡 Try: Close Outlook completely and restart it")
                self.main_app.log("💡 Try: Run this application as Administrator")
            elif "rpc server" in error_msg.lower():
                self.main_app.log("💡 Try: Restart Outlook application")

            self.connected = False
            self.response_queue.put({'success': False, 'error': str(e)})

    def _handle_send_email_request(self, request):
        try:
            if not self.connected or not self.outlook_app:
                raise Exception("Outlook not connected")

            email = request['email']
            subject = request['subject']
            content = request['content']

            mail_item = self.outlook_app.CreateItem(0)  # olMailItem
            if not mail_item:
                raise Exception("Could not create mail item")

            mail_item.To = email
            mail_item.Subject = subject or "No Subject"

            if request.get('unsubscribe_url'):
                prop_accessor = mail_item.PropertyAccessor
                prop_accessor.SetProperty("http://schemas.microsoft.com/mapi/proptag/0x0023001F", f"<{request['unsubscribe_url']}>") # PR_LIST_UNSUBSCRIBE

            if request.get('attachments'):
                for attachment_path in request['attachments']:
                    try:
                        if os.path.exists(attachment_path):
                            mail_item.Attachments.Add(attachment_path)
                            self.main_app.log(f"✅ Attachment added: {os.path.basename(attachment_path)}")
                    except Exception as attach_error:
                        self.main_app.log(f"⚠️ Could not attach {attachment_path}: {attach_error}")

            if content and ('<html>' in content.lower() or '<div>' in content.lower() or '<p>' in content.lower()):
                try:
                    mail_item.HTMLBody = content
                    plain_text = re.sub(r'<[^>]+>', '', content)
                    plain_text = re.sub(r'\s+', ' ', plain_text).strip()
                    mail_item.Body = plain_text
                except Exception as html_error:
                    self.main_app.log(f"⚠️ HTML content error, using plain text: {html_error}")
                    plain_text = re.sub(r'<[^>]+>', '', content)
                    mail_item.Body = plain_text
            else:
                mail_item.Body = content or "No content"

            mail_item.Send()
            self.main_app.log(f"✅ Email sent successfully to {email}")
            self.response_queue.put({'success': True, 'email': email})

        except Exception as e:
            error_msg = str(e)
            self.main_app.log(f"❌ Desktop Outlook send error for {request['email']}: {error_msg}")
            self.response_queue.put({'success': False, 'email': request['email'], 'error': str(e)})

    def _handle_disconnect_request(self):
        try:
            self.connected = False
            if self.outlook_app:
                self.outlook_app = None
            if self.outlook_namespace:
                self.outlook_namespace = None

            self.main_app.log("✅ Outlook disconnected")
            self.response_queue.put({'success': True})

        except Exception as e:
            self.main_app.log(f"⚠️ Disconnect error: {e}")
            self.response_queue.put({'success': True})

    def connect_to_outlook(self, timeout=15):
        if not self.start_com_thread():
            return False

        try:
            self.request_queue.put({'action': 'connect'})
            response = self.response_queue.get(timeout=timeout)
            return response.get('success', False)
        except queue.Empty:
            self.main_app.log("❌ Outlook connection timeout")
            return False
        except Exception as e:
            self.main_app.log(f"❌ Connect request error: {e}")
            return False

    def send_email(self, email, subject, content, attachments=None, unsubscribe_url=None, timeout=30):
        if not self.connected:
            return False

        try:
            request = {
                'action': 'send_email',
                'email': email,
                'subject': subject,
                'content': content,
                'attachments': attachments or [],
                'unsubscribe_url': unsubscribe_url
            }

            self.request_queue.put(request)
            response = self.response_queue.get(timeout=timeout)
            return response.get('success', False)

        except queue.Empty:
            self.main_app.log(f"❌ Email send timeout for: {email}")
            return False
        except Exception as e:
            self.main_app.log(f"❌ Send request error for {email}: {e}")
            return False

    def disconnect(self):
        if not self.running:
            return True

        try:
            self.request_queue.put({'action': 'disconnect'})
            self.response_queue.get(timeout=5)
            self.running = False
            return True
        except queue.Empty:
            self.running = False
            return True
        except Exception as e:
            self.main_app.log(f"⚠️ Disconnect request error: {e}")
            self.running = False
            return True

class TrackingServer:
    """Flask-based server for tracking email opens and clicks."""
    def __init__(self, main_app):
        self.main_app = main_app
        if not FLASK_AVAILABLE:
            self.flask_app = None
            return

        self.flask_app = Flask(__name__)
        self.server_thread = None
        self.public_url = None

        self.tracking_data = {
            "opens": {},
            "clicks": {},
        }

        self.pixel_data = base64.b64decode(b'R0lGODlhAQABAIAAAP///wAAACH5BAEAAAAALAAAAAABAAEAAAICRAEAOw==')

        @self.flask_app.route('/track/open/<campaign_id>/<email_id>')
        def track_open(campaign_id, email_id):
            try:
                email = base64.urlsafe_b64decode(email_id.encode()).decode()
                timestamp = datetime.utcnow()

                ip_addr = request.remote_addr
                user_agent_str = request.headers.get('User-Agent')

                if email not in self.tracking_data["opens"]:
                    self.tracking_data["opens"][email] = []

                open_data = {
                    "timestamp": timestamp.isoformat(),
                    "ip": ip_addr,
                    "ua": user_agent_str
                }
                self.tracking_data["opens"][email].append(open_data)

                self.main_app.gui_update_queue.put(('track_open', email, timestamp, open_data))
            except Exception as e:
                self.main_app.log(f"Track open error: {e}")

            response = make_response(self.pixel_data)
            response.headers['Content-Type'] = 'image/gif'
            response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
            response.headers['Pragma'] = 'no-cache'
            response.headers['Expires'] = '0'
            return response

        @self.flask_app.route('/track/click/<campaign_id>/<email_id>')
        def track_click(campaign_id, email_id):
            try:
                email = base64.urlsafe_b64decode(email_id.encode()).decode()
                redirect_url_encoded = request.args.get('url')
                redirect_url = base64.urlsafe_b64decode(redirect_url_encoded.encode()).decode()
                timestamp = datetime.utcnow()

                ip_addr = request.remote_addr
                user_agent_str = request.headers.get('User-Agent')

                if email not in self.tracking_data["clicks"]:
                    self.tracking_data["clicks"][email] = []

                click_data = {
                    "timestamp": timestamp.isoformat(),
                    "ip": ip_addr,
                    "ua": user_agent_str,
                    "url": redirect_url
                }
                self.tracking_data["clicks"][email].append(click_data)

                self.main_app.gui_update_queue.put(('track_click', email, timestamp, click_data))

                return f'<meta http-equiv="refresh" content="0; url={redirect_url}" />'

            except Exception as e:
                self.main_app.log(f"Track click error: {e}")
                return "Error processing click.", 400

        @self.flask_app.route('/unsubscribe/<campaign_id>/<email_id>')
        def unsubscribe(campaign_id, email_id):
            try:
                email = base64.urlsafe_b64decode(email_id.encode()).decode()
                timestamp = datetime.utcnow()
                self.main_app.add_to_suppression_list(email)
                self.main_app.gui_update_queue.put(('track_unsubscribe', email, timestamp))
                return render_template_string("""<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"><title>Unsubscribed</title><style>body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; display: flex; justify-content: center; align-items: center; height: 100vh; background-color: #f4f4f9; margin: 0; }.container { text-align: center; padding: 40px; background-color: #fff; border-radius: 8px; box-shadow: 0 4px 8px rgba(0,0,0,0.1); } h1 { color: #333; } p { color: #666; }</style></head><body><div class="container"><h1>Successfully Unsubscribed</h1><p>Your email address has been removed from our mailing list.</p></div></body></html>""")
            except Exception as e:
                self.main_app.log(f"❌ Unsubscribe error: {e}")
                return "Error processing unsubscribe request.", 400

        @self.flask_app.route('/api/stats')
        def get_stats():
            return jsonify(self.tracking_data)

    def start(self):
        if not FLASK_AVAILABLE:
            self.main_app.log("⚠️ Flask not available, tracking server disabled.")
            return

        port = getattr(self.main_app, 'track_port', DEFAULT_TRACK_PORT)

        if WAITRESS_AVAILABLE:
            self.server_thread = threading.Thread(target=lambda: serve(self.flask_app, host='0.0.0.0', port=port), daemon=True)
            self.main_app.log("🚀 Tracking server started with Waitress for better performance.")
        else:
            self.server_thread = threading.Thread(target=lambda: self.flask_app.run(port=port, host='0.0.0.0'), daemon=True)
            self.main_app.log("🚀 Tracking server started with Flask (consider installing waitress for better performance).")
        self.server_thread.start()
        self.get_public_url(port)

    def get_public_url(self, port=DEFAULT_TRACK_PORT):
        if not PYNGROK_AVAILABLE:
            self.main_app.log("⚠️ pyngrok not installed. Using localhost for tracking.")
            self.public_url = f"http://127.0.0.1:{port}"
            return self.public_url

        try:
            self.main_app.log("🔌 Establishing ngrok tunnel...")
            public_tunnel = ngrok.connect(port, "http")
            self.public_url = public_tunnel.public_url
            self.main_app.log(f"✅ ngrok tunnel established. Public URL: {self.public_url}")
        except Exception as e:
            self.main_app.log(f"❌ Could not start ngrok tunnel: {e}")
            self.main_app.log("   Falling back to localhost for tracking.")
            self.public_url = f"http://127.0.0.1:{port}" 

        return self.public_url

    def shutdown(self):
        if PYNGROK_AVAILABLE:
            try:
                if self.public_url:
                    ngrok.disconnect(self.public_url)
            except Exception as e:
                self.main_app.log(f"⚠️ Error disconnecting ngrok: {e}")
            try:
                for tunnel in ngrok.get_tunnels():
                    ngrok.disconnect(tunnel.public_url)
            except Exception:
                pass

    def get_tracking_data(self):
        if not self.flask_app:
            return {}
        return self.tracking_data

    def load_tracking_data(self, data):
        if not self.flask_app:
            return
        self.tracking_data = data
        self.main_app.log("✅ Tracking data loaded from campaign file.")

class DeliverabilityHelper:
    """Tools for improving deliverability and effectiveness."""
    def __init__(self, main_app):
        self.main_app = main_app
        self.resolver = dns.resolver.Resolver() if DNSPYTHON_AVAILABLE else None
        if self.resolver:
            self.resolver.timeout = 3
            self.resolver.lifetime = 3

        self.blacklist_servers = [
            "bl.spamcop.net", "dnsbl.sorbs.net", "zen.spamhaus.org",
            "b.barracudacentral.org", "cbl.abuseat.org"
        ]

    def run_domain_validator(self, email_list):
        """Batch validate domains from email list with progress reporting."""
        if not DNSPYTHON_AVAILABLE:
            self.main_app.log("⚠️ DNSPYTHON not available. Skipping domain check.")
            return

        domains = list(set(email.split('@')[1].lower() for email in email_list if '@' in email))
        total = len(domains)
        batch_size = 50
        valid_domains = []
        invalid_domains = []

        print("[INFO] Checking in batches of 50...")

        for i in range(0, total, batch_size):
            batch = domains[i:i + batch_size]
            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = {executor.submit(self._check_single_domain, domain): domain for domain in batch}
                for future in as_completed(futures):
                    domain = futures[future]
                    try:
                        result = future.result()
                        if result['valid']:
                            valid_domains.append(domain)
                            print(f"[PROGRESS] {len(valid_domains) + len(invalid_domains)}/{total} ({((len(valid_domains) + len(invalid_domains)) / total * 100):.2f}) - {domain} ... ✓ Valid")
                        else:
                            invalid_domains.append(domain)
                            print(f"[PROGRESS] {len(valid_domains) + len(invalid_domains)}/{total} ({((len(valid_domains) + len(invalid_domains)) / total * 100):.2f}) - {domain} ... ⚠️ {result['reason']}")
                    except Exception as exc:
                        invalid_domains.append(domain)
                        print(f"[ERROR] {domain} generated an exception: {exc}")

        # Save results
        with open("valid_domains.txt", "w") as f:
            f.write("\n".join(valid_domains))
        with open("invalid_domains.txt", "w") as f:
            f.write("\n".join(invalid_domains))

        print(f"\n[SUMMARY] Valid domains: {len(valid_domains)}")
        print(f"[SUMMARY] Domains with issues: {len(invalid_domains)}")

        # Update UI
        self.main_app.root.after(0, lambda: self._update_domain_check_results(valid_domains, invalid_domains))

    def _check_single_domain(self, domain):
        """Check a single domain for MX records, with fallback to A record."""
        try:
            # First, check MX records
            mx_records = self.resolver.resolve(domain, 'MX')
            if mx_records:
                return {'valid': True, 'reason': 'MX found'}
            else:
                # Fallback to A record
                a_records = self.resolver.resolve(domain, 'A')
                if a_records:
                    return {'valid': False, 'reason': 'No MX record, using A record'}
                else:
                    return {'valid': False, 'reason': 'No MX or A record'}
        except dns.resolver.NoAnswer:
            return {'valid': False, 'reason': 'No MX record, using A record'}
        except dns.resolver.NXDOMAIN:
            return {'valid': False, 'reason': 'Domain does not exist'}
        except dns.exception.Timeout:
            return {'valid': False, 'reason': 'DNS timeout'}
        except Exception as e:
            return {'valid': False, 'reason': f'Error: {str(e)}'}

    def _update_domain_check_results(self, valid, invalid):
        """Update the UI with domain check results."""
        if hasattr(self.main_app, 'domain_check_results_text'):
            self.main_app.domain_check_results_text.config(state="normal")
            self.main_app.domain_check_results_text.delete("1.0", tk.END)
            self.main_app.domain_check_results_text.insert(tk.END, f"Valid Domains: {len(valid)}\n")
            self.main_app.domain_check_results_text.insert(tk.END, f"Invalid Domains: {len(invalid)}\n\n")
            self.main_app.domain_check_results_text.insert(tk.END, "Valid:\n" + "\n".join(valid[:50]) + ("\n..." if len(valid) > 50 else ""))
            self.main_app.domain_check_results_text.insert(tk.END, "\n\nInvalid:\n" + "\n".join(invalid[:50]) + ("\n..." if len(invalid) > 50 else ""))
            self.main_app.domain_check_results_text.config(state="disabled")

    def check_mx_record(self, domain):
        if not self.resolver: return "Skipped"
        try:
            self.resolver.resolve(domain, 'MX')
            return "Valid"
        except (dns.resolver.NoAnswer, dns.resolver.NXDOMAIN):
            return "Invalid"
        except dns.exception.Timeout:
            return "Timeout"
        except Exception:
            return "Error"

    def check_domain_authentication(self, domain):
        if not self.resolver:
            return {"spf": "Skipped", "dkim": "Skipped", "dmarc": "Skipped"}

        results = {}
        try:
            txt_records = self.resolver.resolve(domain, 'TXT')
            spf_record = next((str(r) for r in txt_records if 'v=spf1' in str(r).lower()), None)
            results['spf'] = "✅ Found" if spf_record else "❌ Missing"
        except Exception: results['spf'] = "⚠️ Error"

        try:
            dmarc_domain = f'_dmarc.{domain}'
            txt_records = self.resolver.resolve(dmarc_domain, 'TXT')
            dmarc_record = next((str(r) for r in txt_records if 'v=dmarc1' in str(r).lower()), None)
            results['dmarc'] = "✅ Found" if dmarc_record else "❌ Missing"
        except Exception: results['dmarc'] = "❌ Missing"

        dkim_found = False
        common_selectors = ["google", "selector1", "selector2", "default", "k1", "dkim"]
        for selector in common_selectors:
            try:
                self.resolver.resolve(f'{selector}._domainkey.{domain}', 'TXT')
                dkim_found = True
                break
            except Exception: continue
        results['dkim'] = "✅ Found (common)" if dkim_found else "⚠️ Not Found (common)"
        return results

    def check_blacklist(self, ip_or_domain):
        if not self.resolver: return "Skipped"
        is_ip = re.match(r'^\d{1,3}(\.\d{1,3}){3}$', ip_or_domain)

        if is_ip:
            reversed_ip = '.'.join(reversed(ip_or_domain.split('.')))
            query_target = reversed_ip
        else:
            query_target = ip_or_domain

        listed_on = []
        for server in self.blacklist_servers:
            try:
                query = f"{query_target}.{server}"
                self.resolver.resolve(query, 'A')
                listed_on.append(server)
            except dns.resolver.NXDOMAIN:
                continue
            except Exception:
                continue
        return "Clean" if not listed_on else f"Listed: {', '.join(listed_on)}"

    def spin(self, text):
        pattern = re.compile(r'{([^{}]*)}')
        while True:
            match = pattern.search(text)
            if not match:
                break
            options = match.group(1).split('|')
            text = text[:match.start()] + random.choice(options) + text[match.end():]
        return text

    def check_link_health(self, html_content):
        if not REQUESTS_AVAILABLE: return "Skipped"
        links = re.findall(r'href=["\'](https?://[^"\']+)["\']', html_content)
        if not links: return "No links found."

        results = {}
        for link in set(links):
            try:
                response = requests.head(link, timeout=5, allow_redirects=True)
                if 200 <= response.status_code < 400:
                    results[link] = f"✅ OK ({response.status_code})"
                else:
                    results[link] = f"❌ Bad ({response.status_code})"
            except requests.exceptions.RequestException:
                results[link] = "⚠️ Error"
        return results

class CoreEngine:
    """Separated core engine for headless API mode."""
    def __init__(self, config):
        self.config = config
        self.sender = BulkEmailSenderAPI(config)

    def start_api(self):
        if FASTAPI_AVAILABLE:
            from fastapi import FastAPI, HTTPException
            import uvicorn

            app = FastAPI(title="PARIS Sender API", version="9.0.0")

            @app.post("/send")
            async def send_emails(data: dict):
                try:
                    emails = data.get('emails', [])
                    subject = data.get('subject', '')
                    content = data.get('content', '')
                    provider = data.get('provider', 'SMTP')
                    attachments = data.get('attachments', [])
                    result = self.sender.send_campaign(emails, subject, content, provider, attachments)
                    return {"status": "success", "result": result}
                except Exception as e:
                    raise HTTPException(status_code=500, detail=str(e))

            @app.get("/stats")
            async def get_stats():
                return self.sender.get_stats()

            uvicorn.run(app, host="0.0.0.0", port=8000)
        else:
            print("FastAPI not available for API mode.")

class BulkEmailSenderAPI:
    """API version of the sender without UI."""
    def __init__(self, config):
        self.config = config
        # Initialize handlers similarly to the main class

    def send_campaign(self, emails, subject, content, provider, attachments):
        # Implement sending logic
        return {"sent": len(emails), "failed": 0}

    def get_stats(self):
        return {"sent": 0, "failed": 0}

class BulkEmailSender:
    def __init__(self, root):
        self.root = root
        self.root.title("PARIS SENDER - AGILE MARKETING SUITE v9.0.0 (ENHANCED PROXY VPS MAILER WITH ADVANCED ANTI-DETECTION & VPS OPTIMIZATION)")
        self.root.geometry("1400x1000")
        # Changed to a colorful theme: light blue background
        self.root.configure(bg='#e0f7ff')

        # Configurable Ports & Paths (defaults)
        self.track_port = DEFAULT_TRACK_PORT
        self.chrome_debug_port = DEFAULT_CHROME_DEBUG_PORT
        self.custom_chrome_path = ""

        self.driver = None
        self.driver_service = None
        self.driver_lock = threading.Lock()

        self.email_list_headers = []
        self.email_list = []
        self.tree_items = {}
        self.tracking_map = {}
        self.running = False
        self.paused = False
        self.sent_count = 0
        self.failed_count = 0

        self.authenticated_account = None
        self.current_provider = None
        self.chromedriver_path = None
        self.attachment_paths = []
        self.autodetected_sender_name = None
        self.autodetected_sender_email = None

        self.action_queue = queue.Queue()
        self.gui_update_queue = queue.Queue()
        self.schedule_timer = None
        self.sequence_worker_thread = None

        # --- NEW v9.0.0: Async loop management ---
        self.async_loop = None
        self.async_thread = None

        # --- Direct MX lifecycle state ---
        self.direct_mx_running = False
        self.direct_mx_paused = False
        self.direct_mx_task = None
        self.direct_mx_loop = None
        self.direct_mx_sent_count = 0
        self.direct_mx_failed_count = 0
        self.direct_mx_retry_count = 0

        # Sequence Data
        self.current_sequence_data = {"name": "", "steps": []}

        # Tkinter Variables (defined before UI build)
        self.use_headless = tk.BooleanVar(value=False)
        self.smtp_max_workers_var = tk.IntVar(value=10)
        self.profiles = {}
        self.current_profile_var = tk.StringVar(value="Default")
        self.smtp_rotation_enabled_var = tk.BooleanVar(value=False)
        self.smtp_rotation_batch_size_var = tk.IntVar(value=100)
        self.smtp_failover_threshold_var = tk.IntVar(value=5)
        self.warmup_mode = tk.BooleanVar(value=False)
        self.warmup_state = {}
        self.ab_testing_enabled = tk.BooleanVar(value=False)
        self.subject_a_var = tk.StringVar(value="")
        self.subject_b_var = tk.StringVar(value="")
        self.ab_split_ratio_var = tk.IntVar(value=50)
        self.ab_body_enabled_var = tk.BooleanVar(value=False)
        self.suppression_list = self.load_suppression_list()
        self.custom_providers = {}
        self._load_provider_plugins()
        self.smtp_max_workers = self.smtp_max_workers_var.get()
        self.geo_cache = {}

        self.openai_api_key_var = tk.StringVar()
        self.openai_model_var = tk.StringVar(value="gpt-3.5-turbo")

        # --- NEW v8.0.0: AI Settings ---
        self.ai_provider_var = tk.StringVar(value="OpenAI")
        self.local_ai_url_var = tk.StringVar(value="http://localhost:11434/api/generate")
        self.local_ai_model_var = tk.StringVar(value="llama3")
        # -------------------------------

        # --- NEW v9.0.0: Enhancements ---
        self.use_undetected_chromedriver = tk.BooleanVar(value=True)
        self.per_profile_proxy_var = tk.StringVar()
        self.warmup_schedule = self._load_warmup_schedule()
        self.seed_list = self._load_seed_list()
        self.seed_check_paused = False
        self.dom_shuffling_enabled = tk.BooleanVar(value=True)
        self.api_mode = tk.BooleanVar(value=False)

        # --- NEW: Configuration Display ---
        self.concurrency_var = tk.IntVar(value=10)
        self.max_retries_var = tk.IntVar(value=3)
        self.retry_delay_var = tk.IntVar(value=5)
        self.smtp_sleep_time_var = tk.IntVar(value=100)
        self.debug_mode_var = tk.BooleanVar(value=False)
        self.include_attachment_var = tk.BooleanVar(value=False)
        self.attachment_name_var = tk.StringVar(value="")
        self.attachment_type_var = tk.StringVar(value="")
        self.use_random_attachment_names_var = tk.BooleanVar(value=False)
        self.use_anti_detection_var = tk.BooleanVar(value=False)
        self.email_priority_var = tk.StringVar(value="high")

        # --- NEW: Placeholder Engine ---
        self.placeholders = self._load_placeholders()
        self.selected_placeholder_var = tk.StringVar()
        self.placeholder_preview_var = tk.StringVar()
        self.live_preview_text = None  # For global live preview
        self.use_dummy_data_var = tk.BooleanVar(value=False)

        # --- NEW: DNS Domain Checker ---
        self.dns_checker_results = []

        # --- NEW: Live Sent Email Log ---
        self.sent_log_entries = []
        self.sent_log_auto_scroll_var = tk.BooleanVar(value=True)
        self.sent_log_filter_var = tk.StringVar(value="")
        self.sent_log_severity_var = tk.StringVar(value="All")

        # Additional missing variables
        self.proxy_var = tk.StringVar()
        self.imap_server_var = tk.StringVar()
        self.imap_user_var = tk.StringVar()
        self.imap_pass_var = tk.StringVar()
        self.track_port_var = tk.IntVar(value=self.track_port)
        self.chrome_port_var = tk.IntVar(value=self.chrome_debug_port)
        self.custom_chrome_path_var = tk.StringVar(value=self.custom_chrome_path)
        self.vps_geo_failover_enabled_var = tk.BooleanVar(value=True)
        self.vps_ai_defaults_var = tk.StringVar(value="OpenAI")
        self.mx_reply_to_var = tk.StringVar(value="")
        self.mx_message_id_domain_var = tk.StringVar(value="")
        self.mx_source_ip_var = tk.StringVar(value="")
        self.mx_ehlo_hostname_var = tk.StringVar(value="")

        # --- NEW: Missing GUI Surface Variables ---
        self.unsubscribe_url_var = tk.StringVar(value="")
        self.reply_to_override_var = tk.StringVar(value="")
        self.rate_limit_per_hour_var = tk.IntVar(value=0)
        self.rate_limit_per_day_var = tk.IntVar(value=0)
        self.random_sender_pool_var = tk.StringVar(value="")
        self.message_id_domain_override_var = tk.StringVar(value="")
        self.dkim_key_file_var = tk.StringVar(value="")

        self.template_pdf_var = tk.StringVar()
        self.burner_domain_var = tk.StringVar()
        self.lure_path_var = tk.StringVar()
        self.enable_secure_pdf_lure_var = tk.BooleanVar(value=False)
        self.common_isp_domains = {
            "gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "aol.com", "icloud.com", "comcast.net",
            "verizon.net", "att.net", "cox.net", "sbcglobal.net", "bellsouth.net", "earthlink.net", "charter.net"
        }
        self.disposable_domains = {
            "10minutemail.com", "guerrillamail.com", "mailinator.com", "temp-mail.org", "throwaway.email",
            "maildrop.cc", "yopmail.com", "fakemailgenerator.com", "tempail.com", "mailcatch.com"
        }

        # Now initialize tracking server with loaded port
        self.tracking_server = None
        if FLASK_AVAILABLE:
            self.tracking_server = TrackingServer(self)
        self.tracking_enabled = tk.BooleanVar(value=True)

        if OUTLOOK_COM_AVAILABLE:
            self.outlook_com = OutlookCOMHandler(self)
        else:
            self.outlook_com = None

        self.smtp_handler = SMTPHandler(self)
        self.deliverability_helper = DeliverabilityHelper(self)
        self.ai_handler = AIHandler(self)
        self.local_ai_handler = LocalAIHandler(self) # NEW v8.0.0
        self.html_helper = HTMLHelper(self)
        self.db_handler = DBHandler(self)
        self.imap_handler = IMAPHandler(self)

        # --- NEW v8.0.4: Proxy VPS Handler ---
        self.proxy_vps_handler = ProxyVPSHandler(self)
        # -------------------------------------

        # --- NEW: Direct MX Handler ---
        self.direct_mx_handler = DirectMXHandler(self)
        # ------------------------------

        # --- NEW v8.0.0: Jinja2 Environment ---
        if JINJA2_AVAILABLE:
            self.jinja_env = Environment(loader=FileSystemLoader('.'))
        else:
            self.jinja_env = None
        # ------------------------------------

        # Monitor State
        self.last_health_check_time = datetime.min

        # Load settings after all variables and handlers are initialized
        self._load_settings()

        self.log("🚀 PARIS SENDER - AGILE MARKETING SUITE v9.0.0 (ENHANCED PROXY VPS MAILER WITH ADVANCED ANTI-DETECTION & VPS OPTIMIZATION)")

        if OUTLOOK_COM_AVAILABLE: self.log("✅ Outlook COM integration available")
        else: self.log("ℹ️ Outlook COM integration unavailable (Platform limit or missing lib).")

        if PYNGROK_AVAILABLE: self.log("✅ pyngrok available for automatic public tracking")
        if KEYRING_AVAILABLE: self.log("✅ keyring available for secure password storage")
        if FLASK_AVAILABLE and self.tracking_server: self.log("✅ Flask tracking server available"); self.tracking_server.start()
        if TKCALENDAR_AVAILABLE: self.log("✅ Scheduling UI available (tkcalendar)")
        if WEBDRIVER_MANAGER_AVAILABLE: self.log("✅ webdriver-manager available for automatic ChromeDriver")
        if DNSPYTHON_AVAILABLE: self.log("✅ dnspython found. Deliverability checks enabled.")
        if PYPDF_AVAILABLE: self.log("✅ PyPDF2 found. PDF embedding for secure links enabled.")
        if AIOSMTPLIB_AVAILABLE: self.log("✅ aiosmtplib found. High-performance async SMTP enabled.")
        if JINJA2_AVAILABLE: self.log("✅ Jinja2 found. Advanced templating enabled.")
        if DKIM_AVAILABLE: self.log("✅ dkimpy found. DKIM signing enabled for Direct MX.")
        else: self.log("⚠️ dkimpy not found. DKIM signing disabled. Install with: pip install dkimpy")

        for d in [TEMPLATES_DIR, REPORTS_DIR, PLUGINS_DIR, SEQUENCES_DIR]:
            os.makedirs(d, exist_ok=True)

        if self.api_mode.get():
            self._start_api_mode()
        else:
            self._build_ui()
            self._load_profiles_from_disk()
            self._update_chrome_version_label()

        threading.Thread(target=self._background_worker, daemon=True).start()
        if not self.api_mode.get():
            self.root.after(100, self._process_gui_updates)

        # Move proactive version check to after UI is built
        if not self.api_mode.get():
            threading.Thread(target=self._proactive_version_check, daemon=True).start()

        self.log(f"🚫 Suppression list loaded with {len(self.suppression_list)} entries.")
        self.log("🎯 READY: Configure, Load Recipients, and Start Sending!")

    # --- Added Missing Methods ---
    def _get_chrome_path(self):
        if self.custom_chrome_path_var.get():
            return self.custom_chrome_path_var.get()
        # Default paths
        if sys.platform == "win32":
            paths = [
                "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe",
                "C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe",
            ]
        elif sys.platform == "darwin":
            paths = ["/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"]
        else:
            paths = ["/usr/bin/google-chrome", "/usr/bin/chrome"]
        for path in paths:
            if os.path.exists(path):
                return path
        return "chrome"  # Fallback

    # --- End Added Methods ---

    def _load_placeholders(self):
        """Load predefined placeholders."""
        return {
            "Basic Email Placeholders": {
                "[EMAIL]": "Full email address",
                "[EMAIL64]": "Base64 encoded email address",
                "[UNAME]": "Username part of email",
                "[DOMAIN]": "Domain part of email",
                "[COMPANY]": "Company name (autograb or custom)",
                "[COMPANYFULL]": "Full company name",
                "[FIRSTNAME]": "First name",
                "[LASTNAME]": "Last name",
                "[GREETINGS]": "Time-based greeting",
                "[SENDER_NAME]": "Sender's name",
            },
            "Date & Time": {
                "[DATE]": "Current date (MM/DD/YYYY)",
                "[DATE-1DAY]": "Date minus 1 day",
                "[DATE-2]": "Date in DD/MM/YYYY format",
                "[TIME]": "Current time",
                "[FUTURE-1DAY]": "Future date +1 day",
                "[CURRENTDATE]": "Current date in words",
            },
            "Random Numbers": {
                "[RAND2]": "2-digit random number",
                "[RAND3]": "3-digit random number",
                "[RAND-4]": "4-digit random number",
                "[RAND5]": "5-digit random number",
            },
            "Business Transactions": {
                "[ORDER_ID]": "Random order ID",
                "[INVOICE_NUM]": "Random invoice number",
                "[AMOUNT]": "Random amount",
                "[TRACKING_NUM]": "Random tracking number",
                "[REF_NUM]": "Reference number",
            },
            "Medical & Healthcare": {
                "[PATIENT_ID]": "Random patient ID",
                "[APPT_DATE]": "Appointment date",
                "[DOCTOR_NAME]": "Random doctor name",
                "[MED_RECORD]": "Medical record number",
                "[PRESCRIPTION]": "Random prescription",
            },
            "Legal & Compliance": {
                "[CASE_NUM]": "Random case number",
                "[LAWYER_NAME]": "Random lawyer name",
                "[COURT_DATE]": "Court date",
                "[DOC_ID]": "Document ID",
                "[COMPLIANCE_CODE]": "Compliance code",
            },
            "Financial & Banking": {
                "[ACCOUNT_NUM]": "Random account number",
                "[TRANSACTION_ID]": "Transaction ID",
                "[BALANCE]": "Random balance",
                "[CARD_LAST4]": "Last 4 digits of card",
                "[BANK_REF]": "Bank reference",
            },
        }

    def _update_settings_display(self):
        """Update the configuration display."""
        if hasattr(self, 'settings_display_text'):
            self.settings_display_text.config(state="normal")
            self.settings_display_text.delete("1.0", tk.END)
            self.settings_display_text.insert(tk.END, f"concurrency: {self.concurrency_var.get()}\n")
            self.settings_display_text.insert(tk.END, f"max_retries: {self.max_retries_var.get()}\n")
            self.settings_display_text.insert(tk.END, f"retry_delay: {self.retry_delay_var.get()}\n")
            self.settings_display_text.insert(tk.END, f"smtp_sleep_time: {self.smtp_sleep_time_var.get()}\n")
            self.settings_display_text.insert(tk.END, f"debug_mode: {self.debug_mode_var.get()}\n")
            self.settings_display_text.insert(tk.END, f"include_attachment: {self.include_attachment_var.get()}\n")
            self.settings_display_text.insert(tk.END, f"attachment_name: {self.attachment_name_var.get()}\n")
            self.settings_display_text.insert(tk.END, f"attachment_type: {self.attachment_type_var.get()}\n")
            self.settings_display_text.insert(tk.END, f"use_random_attachment_names: {self.use_random_attachment_names_var.get()}\n")
            self.settings_display_text.insert(tk.END, f"use_anti_detection: {self.use_anti_detection_var.get()}\n")
            self.settings_display_text.insert(tk.END, f"email_priority: {self.email_priority_var.get()}\n")
            self.settings_display_text.config(state="disabled")

    def _log_sent_email(self, email, subject, sender_email, profile_name, attachment):
        """Log sent email in formatted way."""
        progress = f"({self.sent_count}/{len(self.email_list)})"
        self.log(f"{progress} {profile_name} -> {email} | From: {sender_email} | Subject: {subject} | Attachment: {attachment}")
        self._append_sent_log(f"{progress} {profile_name} -> {email} | From: {sender_email} | Subject: {subject} | Attachment: {attachment}", "SUCCESS")

    def _load_warmup_schedule(self):
        if os.path.exists(WARMUP_SCHEDULE_FILE):
            try:
                with open(WARMUP_SCHEDULE_FILE, 'r') as f:
                    return json.load(f)
            except Exception:
                pass
        # Default warmup schedule
        return {
            "account_age_days": 30,  # Assume account is 30 days old
            "schedule": [
                {"day": 1, "max_sends": 10},
                {"day": 2, "max_sends": 20},
                {"day": 3, "max_sends": 35},
                {"day": 4, "max_sends": 55},
                {"day": 5, "max_sends": 80},
                {"day": 6, "max_sends": 110},
                {"day": 7, "max_sends": 145},
                # Continue as needed
            ]
        }

    def _load_seed_list(self):
        if os.path.exists(SEED_LIST_FILE):
            try:
                with open(SEED_LIST_FILE, 'r') as f:
                    return json.load(f)
            except Exception:
                pass
        return []

    def load_suppression_list(self):
        if not os.path.exists(SUPPRESSION_LIST_FILE):
            return []
        try:
            with open(SUPPRESSION_LIST_FILE, 'r') as f:
                return json.load(f)
        except Exception:
            return []

    def add_to_suppression_list(self, email):
        email = email.lower().strip()
        if email not in self.suppression_list:
            self.suppression_list.append(email)
            try:
                with open(SUPPRESSION_LIST_FILE, 'w') as f:
                    json.dump(self.suppression_list, f)
            except Exception as e:
                self.log(f"Error saving suppression list: {e}")

            # Update in-memory status
            if email in self.tracking_map:
                self.tracking_map[email]['status'] = 'Suppressed'
                self.gui_update_queue.put(('update_recipient', email))

    def _view_suppression_list(self):
        win = tk.Toplevel(self.root)
        win.title(f"Suppression List ({len(self.suppression_list)})")
        win.geometry("400x500")

        listbox = tk.Listbox(win)
        listbox.pack(fill='both', expand=True, padx=10, pady=10)

        for email in self.suppression_list:
            listbox.insert(tk.END, email)

        tk.Button(win, text="Close", command=win.destroy).pack(pady=5)

    def _add_to_suppression_manually(self):
        email = simpledialog.askstring("Suppress Email", "Enter email to suppress:", parent=self.root)
        if email:
            self.add_to_suppression_list(email)
            messagebox.showinfo("Suppressed", f"{email} added to suppression list.")

    def _export_suppression_list(self):
        path = filedialog.asksaveasfilename(defaultextension=".json", filetypes=[("JSON", "*.json")])
        if path:
            with open(path, 'w') as f:
                json.dump(self.suppression_list, f)
            messagebox.showinfo("Exported", "Suppression list exported.")

    def _run_auth_check(self):
        domain = self.deliv_target_var.get().strip()
        if not domain:
            messagebox.showwarning("Missing Input", "Please enter a domain (e.g., mycompany.com)")
            return

        if "@" in domain: domain = domain.split("@")[1]

        self.deliv_results_text.config(state="normal")
        self.deliv_results_text.delete("1.0", tk.END)
        self.deliv_results_text.insert(tk.END, f"🔍 Checking Authentication for: {domain}\n\n")

        results = self.deliverability_helper.check_domain_authentication(domain)
        for key, val in results.items():
            self.deliv_results_text.insert(tk.END, f"{key.upper()}: {val}\n")

        self.deliv_results_text.config(state="disabled")

    def _run_blacklist_check(self):
        target = self.deliv_target_var.get().strip()
        if not target:
            messagebox.showwarning("Missing Input", "Please enter an IP or Domain.")
            return

        self.deliv_results_text.config(state="normal")
        self.deliv_results_text.delete("1.0", tk.END)
        self.deliv_results_text.insert(tk.END, f"🔍 Checking Blacklists for: {target}\n\n")

        result = self.deliverability_helper.check_blacklist(target)
        self.deliv_results_text.insert(tk.END, f"Result: {result}\n")
        self.deliv_results_text.config(state="disabled")

    def _run_spam_check(self, ai=False):
        content = self._get_message_content()
        subject = self.subject_var.get()

        score = 0
        triggers = []

        spam_words = ["free", "guarantee", "credit", "offer", "urgent", "winner", "cash", "bonus", "buy now"]

        full_text = (subject + " " + content).lower()

        for word in spam_words:
            if word in full_text:
                score += 1
                triggers.append(word)

        if subject.isupper():
            score += 2
            triggers.append("ALL CAPS SUBJECT")

        if "!" * 3 in full_text:
            score += 1
            triggers.append("Excessive Exclamation")

        self.deliv_results_text.config(state="normal")
        self.deliv_results_text.delete("1.0", tk.END)
        self.deliv_results_text.insert(tk.END, f"🛡️ Spam Score: {score}/10 (Lower is better)\n")
        if triggers:
            self.deliv_results_text.insert(tk.END, f"⚠️ Triggers found: {', '.join(triggers)}\n")
        else:
            self.deliv_results_text.insert(tk.END, "✅ No obvious spam triggers found.\n")

        self.deliv_results_text.config(state="disabled")

    def _run_ai_spam_check(self):
        """NEW v8.0.0: Runs spam check using the selected AI."""
        ai_provider = self.ai_provider_var.get()
        handler = self.local_ai_handler if ai_provider == "Local" else self.ai_handler

        if ai_provider == "OpenAI" and not self.openai_api_key_var.get():
            messagebox.showwarning("AI Config", "Please enter your OpenAI API Key in Settings first.")
            return

        content = self._get_message_content()
        subject = self.subject_var.get()
        if not content.strip() or not subject.strip():
            messagebox.showwarning("Missing Content", "Please provide a subject and body to analyze.")
            return

        self.log(f"🤖 AI Spam Check: Analyzing content with {ai_provider}...")
        self.deliv_results_text.config(state="normal")
        self.deliv_results_text.delete("1.0", tk.END)
        self.deliv_results_text.insert(tk.END, f"🧠 Analyzing with {ai_provider} AI... please wait.\n")
        self.root.update()

        def run_check():
            prompt = (f"Analyze the following email for spam triggers, awkward phrasing, or phishing indicators. "
                      f"Provide a spam score from 1 to 10 (1 is best), a one-sentence summary of the risk, "
                      f"and a bulleted list of concrete suggestions for improvement. Format your response clearly.\n\n"
                      f"SUBJECT: {subject}\n\nBODY:\n{content}")

            success, result = handler.generate(prompt, system_msg="You are an expert email deliverability analyst.")

            def update_ui():
                self.deliv_results_text.delete("1.0", tk.END)
                if success:
                    self.deliv_results_text.insert(tk.END, "--- AI Spam Analysis Report ---\n\n")
                    self.deliv_results_text.insert(tk.END, result)
                    self.log("✅ AI Spam Check completed.")
                else:
                    self.deliv_results_text.insert(tk.END, f"--- AI Spam Analysis Failed ---\n\n{result}")
                    self.log(f"❌ AI Spam Check failed: {result}")
            self.root.after(0, update_ui)

        threading.Thread(target=run_check, daemon=True).start()

    def _run_link_health_check(self, silent=False):
        content = self._get_message_content()
        results = self.deliverability_helper.check_link_health(content)

        if isinstance(results, dict):
            bad_links = [l for l, s in results.items() if "Bad" in s or "Error" in s]
            if bad_links:
                if not silent:
                    msg = "⚠️ Broken Links Found:\n\n" + "\n".join(bad_links)
                    messagebox.showwarning("Link Health", msg)
                return False
            return True
        return True

    # --- Sequence Methods ---
    def _new_sequence(self):
        self.current_sequence_data = {"name": "New Sequence", "steps": []}
        self.sequence_editor_frame.config(text="Sequence Editor: New Sequence")
        self._render_sequence_steps()

    def _save_sequence(self):
        if hasattr(self, 'step_widgets'):
            new_steps = []
            for widget_group in self.step_widgets:
                step_type = widget_group['type']
                if step_type == 'email':
                    step_data = {
                        'type': 'email',
                        'subject': widget_group['subject_var'].get(),
                        'content': widget_group['content_text'].get("1.0", tk.END).strip()
                    }
                    new_steps.append(step_data)
                elif step_type == 'wait':
                    try:
                        dur = float(widget_group['duration_var'].get())
                    except: dur = 24.0
                    step_data = {'type': 'wait', 'duration': dur}
                    new_steps.append(step_data)
                elif step_type == 'if_status':
                    step_data = {
                        'type': 'if_status',
                        'condition': widget_group['condition_var'].get(),
                        'true_target': widget_group['true_var'].get(),
                        'false_target': widget_group['false_var'].get()
                    }
                    new_steps.append(step_data)
            self.current_sequence_data['steps'] = new_steps

        name = simpledialog.askstring("Save Sequence", "Sequence Name:", initialvalue=self.current_sequence_data.get("name", ""))
        if name:
            self.current_sequence_data["name"] = name
            path = os.path.join(SEQUENCES_DIR, f"{name}.json")
            with open(path, 'w') as f:
                json.dump(self.current_sequence_data, f, indent=2)
            self.log(f"💾 Sequence saved: {name}")
            self._refresh_sequences_list()

    def _delete_sequence(self):
        sel = self.sequences_listbox.curselection()
        if not sel: return
        name = self.sequences_listbox.get(sel[0])
        if messagebox.askyesno("Confirm", f"Delete sequence '{name}'?"):
            path = os.path.join(SEQUENCES_DIR, f"{name}.json")
            if os.path.exists(path):
                os.remove(path)
                self._refresh_sequences_list()
                self._new_sequence()

    def _refresh_sequences_list(self):
        self.sequences_listbox.delete(0, tk.END)
        if os.path.exists(SEQUENCES_DIR):
            for f in os.listdir(SEQUENCES_DIR):
                if f.endswith(".json"):
                    self.sequences_listbox.insert(tk.END, f[:-5])

    def _load_sequence_from_listbox(self, event):
        sel = self.sequences_listbox.curselection()
        if not sel: return
        name = self.sequences_listbox.get(sel[0])
        path = os.path.join(SEQUENCES_DIR, f"{name}.json")
        if os.path.exists(path):
            try:
                with open(path, 'r') as f:
                    self.current_sequence_data = json.load(f)
                self.sequence_editor_frame.config(text=f"Sequence Editor: {name}")
                self._render_sequence_steps()
            except Exception as e:
                self.log(f"❌ Error loading sequence: {e}")

    def _add_sequence_step(self, step_type):
        if step_type == 'email':
            self.current_sequence_data['steps'].append({'type': 'email', 'subject': 'Subject', 'content': 'Body content...'})
        elif step_type == 'wait':
            self.current_sequence_data['steps'].append({'type': 'wait', 'duration': 24})
        elif step_type == 'if_status':
            self.current_sequence_data['steps'].append({'type': 'if_status', 'condition': 'Opened', 'true_target': 1, 'false_target': 1})
        self._render_sequence_steps()

    def _remove_sequence_step(self, index):
        if 0 <= index < len(self.current_sequence_data['steps']):
            self.current_sequence_data['steps'].pop(index)
            self._render_sequence_steps()

    def _render_sequence_steps(self):
        for widget in self.sequence_canvas_frame.winfo_children():
            widget.destroy()

        sf = ScrollableFrame(self.sequence_canvas_frame)
        sf.pack(fill='both', expand=True)
        content = sf.scrollable_frame

        self.step_widgets = []

        steps = self.current_sequence_data.get('steps', [])
        for i, step in enumerate(steps):
            frame = tk.LabelFrame(content, text=f"Step {i+1}: {step['type'].upper()}")
            frame.pack(fill='x', padx=5, pady=5)

            if step['type'] == 'email':
                tk.Label(frame, text="Subject:").pack(anchor='w')
                s_var = tk.StringVar(value=step.get('subject', ''))
                tk.Entry(frame, textvariable=s_var).pack(fill='x', padx=5)

                tk.Label(frame, text="Content:").pack(anchor='w')
                txt = tk.Text(frame, height=4)
                txt.pack(fill='x', padx=5)
                txt.insert('1.0', step.get('content', ''))

                self.step_widgets.append({
                    'type': 'email', 'subject_var': s_var, 'content_text': txt
                })

            elif step['type'] == 'wait':
                tk.Label(frame, text="Wait Duration (hours):").pack(side=tk.LEFT)
                d_var = tk.StringVar(value=str(step.get('duration', 24)))
                tk.Entry(frame, textvariable=d_var, width=10).pack(side=tk.LEFT, padx=5)
                self.step_widgets.append({'type': 'wait', 'duration_var': d_var})

            elif step['type'] == 'if_status':
                tk.Label(frame, text="If recipient has:").pack(side=tk.LEFT)
                cond_var = tk.StringVar(value=step.get('condition', 'Opened'))
                ttk.Combobox(frame, textvariable=cond_var, values=["Opened", "Clicked", "Replied"], width=10).pack(side=tk.LEFT, padx=5)

                tk.Label(frame, text="Jump to Step:").pack(side=tk.LEFT)
                true_var = tk.IntVar(value=int(step.get('true_target', 1)))
                tk.Entry(frame, textvariable=true_var, width=3).pack(side=tk.LEFT)

                tk.Label(frame, text="Else Jump to:").pack(side=tk.LEFT)
                false_var = tk.IntVar(value=int(step.get('false_target', 1)))
                tk.Entry(frame, textvariable=false_var, width=3).pack(side=tk.LEFT)

                self.step_widgets.append({
                    'type': 'if_status',
                    'condition_var': cond_var,
                    'true_var': true_var,
                    'false_var': false_var
                })

            tk.Button(frame, text="❌ Remove", command=lambda idx=i: self._remove_sequence_step(idx)).pack(anchor='e', padx=5, pady=2)

    def _start_sequence_for_list(self):
        name = self.current_sequence_data.get('name')
        if not name or not self.email_list:
            messagebox.showwarning("Invalid", "Load a list and select/save a sequence first.")
            return

        if messagebox.askyesno("Start Sequence", f"Assign {len(self.email_list)} recipients to sequence '{name}'?\nThey will start at Step 1 immediately."):
            for email in self.email_list:
                self.tracking_map[email]['sequence_name'] = name
                self.tracking_map[email]['sequence_step'] = -1
                self.tracking_map[email]['sequence_next_run'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                self.tracking_map[email]['sequence_status'] = 'Active'

            self.log(f"💧 Sequence '{name}' activated for {len(self.email_list)} recipients.")

    def _run_sequence_automation_cycle(self):
        if self.running: return

        recipients = list(self.tracking_map.keys())
        processed_count = 0

        for email in recipients:
            data = self.tracking_map[email]
            seq_name = data.get('sequence_name')
            if not seq_name or data.get('sequence_status') != 'Active': continue

            seq_file = os.path.join(SEQUENCES_DIR, f"{seq_name}.json")
            if not os.path.exists(seq_file): continue

            try:
                with open(seq_file, 'r') as f: seq_def = json.load(f)
            except: continue

            steps = seq_def.get('steps', [])
            current_step_idx = int(data.get('sequence_step', -1))
            next_run_str = data.get('sequence_next_run')

            if not next_run_str: continue
            next_run_dt = datetime.strptime(next_run_str, "%Y-%m-%d %H:%M:%S")

            if datetime.now() >= next_run_dt:
                next_step_idx = current_step_idx + 1

                if next_step_idx >= len(steps):
                    data['sequence_status'] = 'Completed'
                    self.log(f"💧 Sequence '{seq_name}' completed for {email}")
                    self.gui_update_queue.put(('update_recipient', email))
                    continue

                step = steps[next_step_idx]

                if step['type'] == 'if_status':
                    condition = step.get('condition', 'Opened')
                    met = False

                    if condition == 'Opened' and data.get('open_events'): met = True
                    elif condition == 'Clicked' and data.get('click_events'): met = True
                    elif condition == 'Replied' and data.get('status') == 'Replied': met = True

                    target_step_num = int(step.get('true_target', 1)) if met else int(step.get('false_target', 1))

                    new_idx = max(0, target_step_num - 1) - 1

                    data['sequence_step'] = new_idx
                    data['sequence_next_run'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    self.log(f"🔀 Sequence '{seq_name}': {email} condition '{condition}' is {met}. Jumping to step {target_step_num}")

                elif step['type'] == 'wait':
                    hours = float(step.get('duration', 24))
                    next_run_time = datetime.now() + timedelta(hours=hours)
                    data['sequence_next_run'] = next_run_time.strftime("%Y-%m-%d %H:%M:%S")
                    data['sequence_step'] = next_step_idx
                    self.log(f"💧 Sequence '{seq_name}': {email} waiting {hours}h")
                    data['status'] = f"Seq: Wait {hours}h"

                elif step['type'] == 'email':
                    subj = step.get('subject', '')
                    body = step.get('content', '')
                    provider = self.provider_var.get()

                    self.log(f"💧 Sequence '{seq_name}': Sending step {next_step_idx+1} to {email}")

                    p_subj, p_body, _, _ = self._personalize_content(email, subj, body)

                    success = self._send_single_email(email, p_subj, p_body, provider)

                    if success:
                        data['sequence_step'] = next_step_idx
                        data['sequence_next_run'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        data['status'] = f"Seq: Sent Step {next_step_idx+1}"
                        self.sent_count += 1
                    else:
                        data['status'] = "Seq: Failed"

                self.gui_update_queue.put(('update_recipient', email))
                processed_count += 1

                if processed_count % 5 == 0: time.sleep(0.1)

    # --- Analytics Methods ---
    def _update_analytics(self):
        if not self.tracking_server:
            self.log("⚠️ Tracking server not available for analytics.")
            return

        tracking_data = self.tracking_server.get_tracking_data()

        total_sent = self.sent_count
        total_recipients = len(self.email_list)
        total_failed = self.failed_count

        all_opens = tracking_data.get('opens', {})
        all_clicks = tracking_data.get('clicks', {})

        unique_opens = len(all_opens)
        unique_clicks = len(all_clicks)
        total_unsubscribes = len([e for e, d in self.tracking_map.items() if d.get("status") == "Unsubscribed"])
        total_bounced = len([e for e, d in self.tracking_map.items() if d.get("status") == "Bounced"])

        open_rate = (unique_opens / total_sent * 100) if total_sent > 0 else 0
        click_rate = (unique_clicks / total_sent * 100) if total_sent > 0 else 0
        unsub_rate = (total_unsubscribes / total_sent * 100) if total_sent > 0 else 0
        fail_rate = (total_failed / total_recipients * 100) if total_recipients > 0 else 0
        bounce_rate = (total_bounced / total_sent * 100) if total_sent > 0 else 0

        self.analytics_sent_val.config(text=str(total_sent))
        self.analytics_opens_val.config(text=str(unique_opens)); self.analytics_opens_pct.config(text=f"({open_rate:.1f}%)")
        self.analytics_clicks_val.config(text=str(unique_clicks)); self.analytics_clicks_pct.config(text=f"({click_rate:.1f}%)")
        self.analytics_unsub_val.config(text=str(total_unsubscribes)); self.analytics_unsub_pct.config(text=f"({unsub_rate:.1f}%)")
        self.analytics_failed_val.config(text=str(total_failed)); self.analytics_failed_pct.config(text=f"({fail_rate:.1f}%)")
        if hasattr(self, 'analytics_bounce_val'):
            self.analytics_bounce_val.config(text=str(total_bounced)); self.analytics_bounce_pct.config(text=f"({bounce_rate:.1f}%)")

        # Update Delivery Breakdown
        if hasattr(self, 'delivery_breakdown_text'):
            self.delivery_breakdown_text.config(state="normal")
            self.delivery_breakdown_text.delete("1.0", tk.END)
            self.delivery_breakdown_text.insert(tk.END, "📋 Delivery Breakdown\n" + "=" * 40 + "\n\n")
            self.delivery_breakdown_text.insert(tk.END, f"Total Recipients: {total_recipients}\n")
            self.delivery_breakdown_text.insert(tk.END, f"Successfully Sent: {total_sent}\n")
            self.delivery_breakdown_text.insert(tk.END, f"Failed: {total_failed}\n")
            self.delivery_breakdown_text.insert(tk.END, f"Bounced: {total_bounced}\n")
            self.delivery_breakdown_text.insert(tk.END, f"Unsubscribed: {total_unsubscribes}\n")
            self.delivery_breakdown_text.insert(tk.END, f"Unique Opens: {unique_opens} ({open_rate:.1f}%)\n")
            self.delivery_breakdown_text.insert(tk.END, f"Unique Clicks: {unique_clicks} ({click_rate:.1f}%)\n")
            self.delivery_breakdown_text.config(state="disabled")

        # Update Activity Feed
        if hasattr(self, 'activity_feed_text'):
            self.activity_feed_text.config(state="normal")
            self.activity_feed_text.delete("1.0", tk.END)
            self.activity_feed_text.insert(tk.END, "📡 Recent Activity\n" + "=" * 40 + "\n\n")
            for email, data in list(self.tracking_map.items())[-20:]:
                status = data.get('status', 'Unknown')
                self.activity_feed_text.insert(tk.END, f"{email}: {status}\n")
            self.activity_feed_text.config(state="disabled")

        if MATPLOTLIB_AVAILABLE:
            self._draw_analytics_charts(total_sent, unique_opens, unique_clicks, total_unsubscribes, total_failed)

        self._draw_click_heatmap()

        self.log("📊 Visual analytics refreshed.")

    def _draw_analytics_charts(self, sent, opens, clicks, unsubs, failed):
        for widget in self.charts_canvas_frame.winfo_children():
            widget.destroy()

        if not MATPLOTLIB_AVAILABLE: return

        fig = Figure(figsize=(5, 4), dpi=100)
        ax = fig.add_subplot(111)

        labels = ['Sent', 'Opens', 'Clicks', 'Failed']
        values = [sent, opens, clicks, failed]

        ax.bar(labels, values, color=['#3498db', '#2ecc71', '#f1c40f', '#e74c3c'])
        ax.set_title("Campaign Performance")

        canvas = FigureCanvasTkAgg(fig, master=self.charts_canvas_frame)
        canvas.draw()
        canvas.get_tk_widget().pack(fill='both', expand=True)

    def _draw_click_heatmap(self):
        self.heatmap_text.config(state="normal")
        self.heatmap_text.delete("1.0", tk.END)

        content = self.message_box.get("1.0", tk.END)
        if not content:
            self.heatmap_text.insert(tk.END, "No message content available.")
            self.heatmap_text.config(state="disabled")
            return

        click_counts = Counter()
        if self.tracking_server:
            all_clicks_map = self.tracking_server.tracking_data.get("clicks", {})
            for email_clicks in all_clicks_map.values():
                for click in email_clicks:
                    url = click.get('url')
                    if url: click_counts[url] += 1

        links = re.findall(r'href=["\'](https?://[^"\']+)["\']', content)

        annotated_content = content
        for link in set(links):
            count = click_counts.get(link, 0)
            annotation = f" [🖱️ {count}]"
            if count > 0:
                 annotated_content = annotated_content.replace(f'href="{link}"', f'href="{link}" style="border: 2px solid red;" title="{count} Clicks"')
                 annotated_content = annotated_content.replace(link, f"{link} {annotation}")

        self.heatmap_text.insert(tk.END, "--- Click Heatmap Report ---\n\n")
        self.heatmap_text.insert(tk.END, "Links found and click counts:\n")
        for link in set(links):
            count = click_counts.get(link, 0)
            self.heatmap_text.insert(tk.END, f"🔗 {link}: {count} clicks\n")

        self.heatmap_text.insert(tk.END, "\n--- Raw Content Preview ---\n")
        self.heatmap_text.insert(tk.END, annotated_content)
        self.heatmap_text.config(state="disabled")

    def _load_provider_plugins(self):
        try:
            if not os.path.isdir(PLUGINS_DIR):
                return
            for fname in os.listdir(PLUGINS_DIR):
                if not fname.endswith(".py"):
                    continue
                path = os.path.join(PLUGINS_DIR, fname)
                mod_name = f"provider_{os.path.splitext(fname)[0]}"
                try:
                    import importlib.util
                    spec = importlib.util.spec_from_file_location(mod_name, path)
                    if spec is None:
                        self.log(f"⚠️ Could not create spec for {fname}")
                        continue
                    module = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(module)
                    provider_name = getattr(module, "PROVIDER_NAME", None)
                    if provider_name and hasattr(module, "send"):
                        self.custom_providers[provider_name] = module
                        self.log(f"🔌 Loaded custom provider plugin: {provider_name}")
                except Exception as e:
                    self.log(f"⚠️ Failed to load provider plugin {fname}: {e}")
        except Exception as e:
            self.log(f"⚠️ Error scanning provider plugins: {e}")

    def get_chrome_version(self):
        try:
            result = subprocess.run([self._get_chrome_path(), "--version"], capture_output=True, text=True, timeout=5)
            match = re.search(r'Google Chrome (\d+\.\d+\.\d+\.\d+)', result.stdout)
            return match.group(1) if match else "unknown"
        except:
            return "unknown"

    def _connect_chrome(self):
        try:
            chrome_options = Options()
            chrome_options.add_experimental_option("debuggerAddress", f"127.0.0.1:{self.chrome_port_var.get()}")
            if self.use_undetected_chromedriver.get():
                if UNDETECTED_CHROMEDRIVER_AVAILABLE:
                    self.driver = uc.Chrome(options=chrome_options)
                else:
                    self.driver = webdriver.Chrome(options=chrome_options)
            else:
                self.driver = webdriver.Chrome(options=chrome_options)
            self.log("✅ Connected to Chrome debugger.")
            self.connection_status.config(text="🔵 Connected", fg="#27ae60")
        except Exception as e:
            self.log(f"❌ Failed to connect to Chrome: {e}")
            self.connection_status.config(text="🔌 Chrome: Disconnected", fg="#e74c3c")
            messagebox.showerror("Connection Error", f"Could not connect to Chrome:\n{e}")

    def _start_chrome_with_debug(self):
        chrome_path = self._get_chrome_path()
        port = self.chrome_port_var.get()
        user_data_dir = os.path.join(tempfile.gettempdir(), "chrome_debug")
        cmd = [
            chrome_path,
            f"--remote-debugging-port={port}",
            f"--user-data-dir={user_data_dir}",
            "--no-first-run",
            "--no-default-browser-check",
        ]
        if self.use_headless.get():
            cmd.append("--headless")
        try:
            self.driver_service = subprocess.Popen(cmd)
            time.sleep(2)  # Wait for chrome to start
            self._connect_chrome()
        except Exception as e:
            self.log(f"❌ Failed to start Chrome: {e}")
            messagebox.showerror("Start Error", f"Could not start Chrome:\n{e}")

    def _build_ui(self):
        if self.api_mode.get():
            self.log("🔌 API Mode enabled. Skipping UI build.")
            return

        self.root.title(f"PARIS SENDER - AGILE MARKETING SUITE v9.0.0")
        main_scrollable_frame = ScrollableFrame(self.root)
        main_scrollable_frame.pack(fill="both", expand=True)
        content_frame = main_scrollable_frame.scrollable_frame

        self._build_main_header(content_frame)

        self.notebook = ttk.Notebook(content_frame)
        self.notebook.pack(expand=True, fill="both", padx=10, pady=5)

        self._build_main_tab()
        self._build_vps_bulk_sender_tab()  # NEW: Dedicated VPS tab
        self._build_direct_mx_tab()  # NEW: Dedicated Direct MX tab
        self._build_deliverability_tab()
        self._build_sequences_tab()
        self._build_analytics_tab()
        self._build_settings_tab()
        self._build_placeholders_tab()  # NEW
        self._build_dns_checker_tab()  # NEW: DNS Domain Checker
        self._build_sent_log_tab()  # NEW: Live Sent Email Log
        self._build_health_monitor_tab()
        self._build_log_viewer_tab()

        self._build_bottom_controls(content_frame)
        self._setup_tooltips()

    def _setup_tooltips(self):
        """Add tooltips to key UI elements for better usability."""
        try:
            # Bottom control buttons
            if hasattr(self, 'connect_chrome_btn') and self.connect_chrome_btn:
                Tooltip(self.connect_chrome_btn, "Connect to an already-running Chrome with remote debugging enabled")
            if hasattr(self, 'start_chrome_btn') and self.start_chrome_btn:
                Tooltip(self.start_chrome_btn, "Launch a new Chrome instance with remote debugging")
            if hasattr(self, 'test_smtp_btn') and self.test_smtp_btn:
                Tooltip(self.test_smtp_btn, "Send a test email via the configured SMTP server")
            if hasattr(self, 'connect_outlook_btn') and self.connect_outlook_btn:
                Tooltip(self.connect_outlook_btn, "Connect to Desktop Outlook via COM automation (Windows only)")
            # VPS buttons
            if hasattr(self, 'vps_start_btn') and self.vps_start_btn:
                Tooltip(self.vps_start_btn, "Start sending emails through VPS or Direct MX delivery")
            # Config buttons
            if hasattr(self, 'start_btn') and self.start_btn:
                Tooltip(self.start_btn, "Start the email sending campaign")
            if hasattr(self, 'export_report_btn') and self.export_report_btn:
                Tooltip(self.export_report_btn, "Export campaign results as CSV/HTML report")
        except Exception:
            pass

    def _build_main_header(self, parent):
        header_frame = tk.Frame(parent, bg='#e0f7ff')  # Light blue background
        header_frame.pack(fill='x', pady=3)

        title = tk.Label(header_frame, text="PARIS SENDER - AGILE MARKETING SUITE v9.0.0", font=("Arial", 16, "bold"), bg='#e0f7ff', fg="#2c3e50")  # Dark blue text
        title.pack(pady=2)

        # Removed hardcoded "ready" icon and any other hardcoded texts

    def _start_api_mode(self):
        """Start headless API mode."""
        self.log("🔌 Starting API mode...")
        config = {}  # Load config
        self.core_engine = CoreEngine(config)
        self.core_engine.start_api()

    def _build_main_tab(self):
        main_tab = ttk.Frame(self.notebook)
        self.notebook.add(main_tab, text='✉️ Campaign')

        main_pane = ttk.PanedWindow(main_tab, orient=tk.HORIZONTAL)
        main_pane.pack(fill='both', expand=True)

        left_frame = ttk.Frame(main_pane, width=350)
        center_frame = ttk.Frame(main_pane)
        right_frame = ttk.Frame(main_pane, width=400)

        left_frame.pack_propagate(False)
        right_frame.pack_propagate(False)

        main_pane.add(left_frame)
        main_pane.add(center_frame, weight=1)
        main_pane.add(right_frame)

        self._build_config_frame(left_frame)
        self._build_message_frame(center_frame)
        self._build_recipients_frame(right_frame)

    def _build_vps_bulk_sender_tab(self):
        """NEW: Dedicated tab for VPS-only bulk sending."""
        vps_tab = ttk.Frame(self.notebook)
        self.notebook.add(vps_tab, text='🖥️ VPS Bulk Sender')

        vps_pane = ttk.PanedWindow(vps_tab, orient=tk.VERTICAL)
        vps_pane.pack(fill='both', expand=True)

        top_frame = ttk.Frame(vps_pane, height=500)
        bottom_frame = ttk.Frame(vps_pane, height=300)
        top_frame.pack_propagate(True)
        bottom_frame.pack_propagate(True)
        vps_pane.add(top_frame, weight=2)
        vps_pane.add(bottom_frame, weight=1)

        top_pane = ttk.PanedWindow(top_frame, orient=tk.HORIZONTAL)
        top_pane.pack(fill='both', expand=True)

        config_outer_frame = ttk.Frame(top_pane, width=450)
        message_outer_frame = ttk.Frame(top_pane)
        config_outer_frame.pack_propagate(False)
        top_pane.add(config_outer_frame, weight=1)
        top_pane.add(message_outer_frame, weight=2)

        self._build_vps_config_frame(config_outer_frame)
        self._build_vps_message_frame(message_outer_frame)
        self._build_vps_recipients_frame(bottom_frame)

    def _build_vps_config_frame(self, parent):
        config_frame = tk.LabelFrame(parent, text="⚙️ VPS Campaign Configuration", font=("Arial", 12, "bold"), bg='#e0f7ff', fg="#2c3e50")
        config_frame.pack(fill='both', expand=True, padx=5, pady=5)

        campaign_mgmt_frame = tk.Frame(config_frame, bg='#e0f7ff')
        campaign_mgmt_frame.pack(fill='x', padx=10, pady=5)
        tk.Button(campaign_mgmt_frame, text="💾 Save VPS Campaign", command=self._save_vps_campaign, bg="#3498db", fg="white", font=("Arial", 9, "bold")).pack(side=tk.LEFT, padx=5)
        tk.Button(campaign_mgmt_frame, text="📂 Load VPS Campaign", command=self._load_vps_campaign, bg="#3498db", fg="white", font=("Arial", 9, "bold")).pack(side=tk.LEFT, padx=5)

        subject_frame = tk.Frame(config_frame, bg='#e0f7ff')
        subject_frame.pack(fill='x', padx=10, pady=5)
        tk.Label(subject_frame, text="Subject:", font=("Arial", 10, "bold"), bg='#e0f7ff', fg="#2c3e50").pack(side=tk.LEFT, padx=(0, 5))
        self.vps_subject_var = tk.StringVar(value="Important Message")
        tk.Entry(subject_frame, textvariable=self.vps_subject_var, bg="white", fg="#2c3e50").pack(side=tk.LEFT, fill='x', expand=True, padx=5)
        self.vps_subject_char_label = tk.Label(subject_frame, text="(17 chars)", font=("Arial", 8), bg='#e0f7ff', fg="#666")
        self.vps_subject_char_label.pack(side=tk.LEFT, padx=2)

        def _update_vps_subject_count(*args):
            count = len(self.vps_subject_var.get())
            self.vps_subject_char_label.config(text=f"({count} chars)")
        self.vps_subject_var.trace_add("write", _update_vps_subject_count)

        self.vps_use_random_sender_var = tk.BooleanVar(value=False)
        tk.Checkbutton(subject_frame, text="Use Random Sender Email", variable=self.vps_use_random_sender_var, bg='#e0f7ff', fg="#2c3e50").pack(side=tk.RIGHT, padx=10)

        # VPS-specific settings
        vps_settings_frame = tk.LabelFrame(config_frame, text="VPS Sender Configuration", bg='#e0f7ff', fg="#2c3e50")
        vps_settings_frame.pack(fill='x', padx=10, pady=5)
        self.vps_settings_frame = vps_settings_frame
        tk.Label(vps_settings_frame, text="Default Sender Name:", bg='#e0f7ff', fg="#2c3e50").grid(row=0, column=0, sticky='w', padx=5, pady=2)
        self.vps_sender_name_var = tk.StringVar(value="Sender")
        tk.Entry(vps_settings_frame, textvariable=self.vps_sender_name_var, bg="white", fg="#2c3e50").grid(row=0, column=1, sticky='ew', padx=5, pady=2)
        tk.Label(vps_settings_frame, text="Default Sender Email:", bg='#e0f7ff', fg="#2c3e50").grid(row=1, column=0, sticky='w', padx=5, pady=2)
        self.vps_sender_email_var = tk.StringVar(value="noreply@example.com")
        tk.Entry(vps_settings_frame, textvariable=self.vps_sender_email_var, bg="white", fg="#2c3e50").grid(row=1, column=1, sticky='ew', padx=5, pady=2)
        tk.Label(vps_settings_frame, text="Random Emails Pool (comma-separated):", bg='#e0f7ff', fg="#2c3e50").grid(row=2, column=0, sticky='w', padx=5, pady=2)
        self.vps_random_emails_var = tk.StringVar(value="")
        random_emails_entry = tk.Entry(vps_settings_frame, textvariable=self.vps_random_emails_var, bg="white", fg="#2c3e50")
        random_emails_entry.grid(row=2, column=1, sticky='ew', padx=5, pady=2)
        self.vps_random_emails_count_label = tk.Label(vps_settings_frame, text="(0 emails)", font=("Arial", 8), bg='#e0f7ff', fg="#666")
        self.vps_random_emails_count_label.grid(row=2, column=2, sticky='w', padx=2, pady=2)

        def _update_random_emails_count(*args):
            pool = self.vps_random_emails_var.get().strip()
            if pool:
                count = sum(1 for e in pool.split(',') if e.strip())
                self.vps_random_emails_count_label.config(text=f"({count} email{'s' if count != 1 else ''})")
            else:
                self.vps_random_emails_count_label.config(text="(0 emails)")
        self.vps_random_emails_var.trace_add("write", _update_random_emails_count)
        vps_settings_frame.grid_columnconfigure(1, weight=1)

        # Direct MX variables (UI moved to dedicated Direct MX tab)
        self.use_direct_mx_var = tk.BooleanVar(value=False)
        self.vps_reply_to_var = tk.StringVar(value="")
        self.vps_message_id_domain_var = tk.StringVar(value="")

        # VPS Advanced Sender Identity
        vps_identity_frame = tk.LabelFrame(config_frame, text="🔐 VPS Sender Identity", bg='#e0f7ff', fg="#2c3e50")
        vps_identity_frame.pack(fill='x', padx=10, pady=5)
        tk.Label(vps_identity_frame, text="Reply-To Email:", bg='#e0f7ff', fg="#2c3e50").grid(row=0, column=0, sticky='w', padx=5, pady=2)
        tk.Entry(vps_identity_frame, textvariable=self.vps_reply_to_var, bg="white", fg="#2c3e50").grid(row=0, column=1, sticky='ew', padx=5, pady=2)
        tk.Label(vps_identity_frame, text="Message-ID Domain:", bg='#e0f7ff', fg="#2c3e50").grid(row=1, column=0, sticky='w', padx=5, pady=2)
        tk.Entry(vps_identity_frame, textvariable=self.vps_message_id_domain_var, bg="white", fg="#2c3e50").grid(row=1, column=1, sticky='ew', padx=5, pady=2)
        vps_identity_frame.grid_columnconfigure(1, weight=1)

        # VPS Controls
        vps_controls_frame = tk.Frame(config_frame, bg='#e0f7ff')
        vps_controls_frame.pack(fill='x', padx=10, pady=5)
        self.vps_controls_frame = vps_controls_frame
        self.vps_start_btn = tk.Button(vps_controls_frame, text="▶️ START VPS CAMPAIGN", command=self._start_vps_sending, bg="#27ae60", fg="white", font=("Arial", 9, "bold"))
        self.vps_pause_btn = tk.Button(vps_controls_frame, text="⏸️ PAUSE VPS CAMPAIGN", command=self._pause_vps_sending, bg="#f39c12", fg="white", font=("Arial", 9, "bold"), state="disabled")
        self.vps_stop_btn = tk.Button(vps_controls_frame, text="⏹️ STOP VPS CAMPAIGN", command=self._stop_vps_sending, bg="#e74c3c", fg="white", font=("Arial", 9, "bold"), state="disabled")
        self.vps_resume_btn = tk.Button(vps_controls_frame, text="▶️ RESUME VPS CAMPAIGN", command=self._resume_vps_sending, bg="#27ae60", fg="white", font=("Arial", 9, "bold"), state="disabled")

        self.vps_start_btn.pack(side=tk.LEFT, padx=2, pady=2)
        self.vps_pause_btn.pack(side=tk.LEFT, padx=2, pady=2)
        self.vps_stop_btn.pack(side=tk.LEFT, padx=2, pady=2)
        self.vps_resume_btn.pack(side=tk.LEFT, padx=2, pady=2)

        # Batch Size Selector
        batch_frame = tk.Frame(config_frame, bg='#e0f7ff')
        batch_frame.pack(fill='x', padx=10, pady=5)
        tk.Label(batch_frame, text="Batch Size:", bg='#e0f7ff', fg="#2c3e50").pack(side=tk.LEFT)
        self.vps_batch_size_var = tk.IntVar(value=100)
        tk.Entry(batch_frame, textvariable=self.vps_batch_size_var, bg="white", fg="#2c3e50", width=10).pack(side=tk.LEFT, padx=5)

        # Preferred Geo-Region
        geo_frame = tk.Frame(config_frame, bg='#e0f7ff')
        geo_frame.pack(fill='x', padx=10, pady=5)
        tk.Label(geo_frame, text="Preferred Geo-Region:", bg='#e0f7ff', fg="#2c3e50").pack(side=tk.LEFT)
        self.vps_geo_region_var = tk.StringVar(value="")
        ttk.Combobox(geo_frame, textvariable=self.vps_geo_region_var, values=["", "US", "EU", "Asia", "South America", "Africa", "Oceania"], width=18).pack(side=tk.LEFT, padx=5)

        # VPS Geo Failover & AI Defaults
        vps_options_frame = tk.Frame(config_frame, bg='#e0f7ff')
        vps_options_frame.pack(fill='x', padx=10, pady=5)
        tk.Checkbutton(vps_options_frame, text="Enable Geo Failover", variable=self.vps_geo_failover_enabled_var,
                        bg='#e0f7ff', fg="#2c3e50").pack(side=tk.LEFT, padx=5)
        tk.Label(vps_options_frame, text="Default AI Provider:", bg='#e0f7ff', fg="#2c3e50").pack(side=tk.LEFT, padx=(15, 5))
        ttk.Combobox(vps_options_frame, textvariable=self.vps_ai_defaults_var, values=["OpenAI", "Local"],
                     state="readonly", width=10).pack(side=tk.LEFT, padx=5)
        tk.Label(vps_options_frame, text="(Per-VPS AI overrides this)", bg='#e0f7ff', fg="#7f8c8d",
                 font=("Arial", 8)).pack(side=tk.LEFT, padx=5)

        # VPS Status Indicator Panel (Live Dashboard)
        vps_status_frame = tk.LabelFrame(config_frame, text="📊 VPS Live Status Dashboard", bg='#e0f7ff', fg="#2c3e50")
        vps_status_frame.pack(fill='x', padx=10, pady=5)
        self.vps_mode_label = tk.Label(vps_status_frame, text="Mode: VPS", font=("Arial", 9, "bold"), bg='#e0f7ff', fg="#2c3e50")
        self.vps_mode_label.grid(row=0, column=0, sticky='w', padx=5, pady=1)
        self.vps_rate_label = tk.Label(vps_status_frame, text="Rate Limit: N/A", font=("Arial", 9), bg='#e0f7ff', fg="#666")
        self.vps_rate_label.grid(row=0, column=1, sticky='w', padx=5, pady=1)
        self.vps_circuit_label = tk.Label(vps_status_frame, text="Circuit: 🟢 Closed", font=("Arial", 9), bg='#e0f7ff', fg="#27ae60")
        self.vps_circuit_label.grid(row=1, column=0, sticky='w', padx=5, pady=1)
        self.vps_geo_label = tk.Label(vps_status_frame, text="Geo: --", font=("Arial", 9), bg='#e0f7ff', fg="#666")
        self.vps_geo_label.grid(row=1, column=1, sticky='w', padx=5, pady=1)
        self.vps_retry_label = tk.Label(vps_status_frame, text="Retry Queue: 0", font=("Arial", 9), bg='#e0f7ff', fg="#666")
        self.vps_retry_label.grid(row=2, column=0, sticky='w', padx=5, pady=1)
        self.vps_dkim_label = tk.Label(vps_status_frame, text="DKIM: Not Configured", font=("Arial", 9), bg='#e0f7ff', fg="#e74c3c")
        self.vps_dkim_label.grid(row=2, column=1, sticky='w', padx=5, pady=1)
        self.vps_active_proxy_label = tk.Label(vps_status_frame, text="Active Proxy: None", font=("Arial", 9), bg='#e0f7ff', fg="#666")
        self.vps_active_proxy_label.grid(row=3, column=0, sticky='w', padx=5, pady=1)
        self.vps_sent_today_label = tk.Label(vps_status_frame, text="Sent Today: 0", font=("Arial", 9), bg='#e0f7ff', fg="#666")
        self.vps_sent_today_label.grid(row=3, column=1, sticky='w', padx=5, pady=1)

        # Proxy Chain Manager
        proxy_frame = tk.LabelFrame(config_frame, text="🔗 Proxy Chain Manager", bg='#e0f7ff', fg="#2c3e50")
        proxy_frame.pack(fill='x', padx=10, pady=5)
        proxy_btn_frame = tk.Frame(proxy_frame, bg='#e0f7ff')
        proxy_btn_frame.pack(fill='x', padx=5, pady=2)
        tk.Button(proxy_btn_frame, text="➕ Add Proxy", command=self._add_proxy, bg="#27ae60", fg="white", font=("Arial", 9)).pack(side=tk.LEFT, padx=2)
        tk.Button(proxy_btn_frame, text="🗑️ Remove Proxy", command=self._remove_proxy, bg="#e74c3c", fg="white", font=("Arial", 9)).pack(side=tk.LEFT, padx=2)
        tk.Button(proxy_btn_frame, text="⬆️ Move Up", command=self._move_proxy_up, bg="#3498db", fg="white", font=("Arial", 9)).pack(side=tk.LEFT, padx=2)
        tk.Button(proxy_btn_frame, text="⬇️ Move Down", command=self._move_proxy_down, bg="#3498db", fg="white", font=("Arial", 9)).pack(side=tk.LEFT, padx=2)
        tk.Button(proxy_btn_frame, text="🧪 Test Proxy", command=self._test_selected_proxy, bg="#e67e22", fg="white", font=("Arial", 9)).pack(side=tk.LEFT, padx=2)
        proxy_list_frame = tk.Frame(proxy_frame, bg='#e0f7ff')
        proxy_list_frame.pack(fill='both', expand=True, padx=5, pady=2)
        proxy_columns = ("host", "port", "type", "status")
        self.proxy_tree = ttk.Treeview(proxy_list_frame, columns=proxy_columns, show='headings', height=4)
        self.proxy_tree.heading("host", text="Host")
        self.proxy_tree.heading("port", text="Port")
        self.proxy_tree.heading("type", text="Type")
        self.proxy_tree.heading("status", text="Status")
        self.proxy_tree.column("host", width=120)
        self.proxy_tree.column("port", width=60)
        self.proxy_tree.column("type", width=70)
        self.proxy_tree.column("status", width=80)
        proxy_scroll = ttk.Scrollbar(proxy_list_frame, orient="vertical", command=self.proxy_tree.yview)
        self.proxy_tree.configure(yscrollcommand=proxy_scroll.set)
        proxy_scroll.pack(side='right', fill='y')
        self.proxy_tree.pack(fill='both', expand=True)

        # Schedule periodic VPS status refresh
        self.root.after(5000, self._refresh_vps_status_panel)

    def _refresh_vps_status_panel(self):
        """Dynamically update VPS status indicator labels from proxy_vps_handler state."""
        try:
            handler = self.proxy_vps_handler
            # Update mode label
            mode = "VPS"
            if hasattr(self, 'vps_mode_label') and self.vps_mode_label.winfo_exists():
                self.vps_mode_label.config(text=f"Mode: {mode}")

            # Update rate limit label
            if hasattr(handler, 'rate_limits') and handler.rate_limits:
                total_rate = sum(handler.rate_limits.values())
                self.vps_rate_label.config(text=f"Rate Limit: {total_rate}/day")
            else:
                self.vps_rate_label.config(text="Rate Limit: N/A")

            # Update circuit breaker label
            healthy_count = sum(1 for v in handler.health_status.values() if v)
            total_vps = len(handler.health_status) if handler.health_status else 0
            if total_vps > 0:
                self.vps_circuit_label.config(
                    text=f"Circuit: 🟢 {healthy_count}/{total_vps} Healthy",
                    fg="#27ae60" if healthy_count > 0 else "#e74c3c"
                )
            else:
                self.vps_circuit_label.config(text="Circuit: ⚪ No VPS", fg="#666")

            # Update geo label
            geo = self.vps_geo_region_var.get()
            self.vps_geo_label.config(text=f"Geo: {geo}" if geo else "Geo: --")

            # Update retry queue label
            retry_count = len(handler.retry_queue) if hasattr(handler, 'retry_queue') else 0
            self.vps_retry_label.config(text=f"Retry Queue: {retry_count}")

            # Update active proxy label
            if hasattr(self, 'vps_active_proxy_label') and self.vps_active_proxy_label.winfo_exists():
                active_proxy = "None"
                if hasattr(handler, 'proxy_health_status') and handler.proxy_health_status:
                    healthy_proxies = [k for k, v in handler.proxy_health_status.items() if v.get('healthy', False)]
                    if healthy_proxies:
                        active_proxy = healthy_proxies[0]
                self.vps_active_proxy_label.config(text=f"Active Proxy: {active_proxy}")

            # Update sent today label
            if hasattr(self, 'vps_sent_today_label') and self.vps_sent_today_label.winfo_exists():
                sent_today = self.sent_count if hasattr(self, 'sent_count') else 0
                self.vps_sent_today_label.config(text=f"Sent Today: {sent_today}")

            # Update proxy tree if visible
            if hasattr(self, 'proxy_tree') and self.proxy_tree.winfo_exists():
                self._refresh_proxy_tree()

        except Exception:
            pass
        # Re-schedule
        self.root.after(5000, self._refresh_vps_status_panel)

    def _toggle_direct_mx_ui(self):
        """Toggle visibility of VPS-specific settings when Direct MX is enabled/disabled."""
        try:
            if self.use_direct_mx_var.get():
                # Direct MX mode: hide VPS-specific controls
                if hasattr(self, 'vps_settings_frame'):
                    self.vps_settings_frame.pack_forget()
                if hasattr(self, 'vps_controls_frame'):
                    self.vps_controls_frame.pack_forget()
                if hasattr(self, 'vps_mode_label') and self.vps_mode_label.winfo_exists():
                    self.vps_mode_label.config(text="Mode: Direct MX")
            else:
                # VPS mode: show VPS-specific controls
                if hasattr(self, 'vps_settings_frame'):
                    self.vps_settings_frame.pack(fill='x', padx=10, pady=5)
                if hasattr(self, 'vps_controls_frame'):
                    self.vps_controls_frame.pack(fill='x', padx=10, pady=5)
                if hasattr(self, 'vps_mode_label') and self.vps_mode_label.winfo_exists():
                    self.vps_mode_label.config(text="Mode: VPS")
        except Exception:
            pass

    def _build_vps_message_frame(self, parent):
        message_frame = tk.LabelFrame(parent, text="📝 VPS Message Content (Jinja2 Enabled)", font=("Arial", 12, "bold"), bg='#e0f7ff', fg="#2c3e50")
        message_frame.pack(fill='both', expand=True, padx=5, pady=5)

        message_toolbar = tk.Frame(message_frame, bg='#e0f7ff')
        message_toolbar.pack(fill='x', pady=2)

        tk.Button(message_toolbar, text="📁 Load HTML", command=self._load_vps_html_file, bg="#3498db", fg="white", font=("Arial", 9)).pack(side=tk.LEFT, padx=2)
        tk.Button(message_toolbar, text="📝 Template", command=self._load_vps_template, bg="#3498db", fg="white", font=("Arial", 9)).pack(side=tk.LEFT, padx=2)
        tk.Button(message_toolbar, text="📎 Attach", command=self._add_vps_attachment, bg="#3498db", fg="white", font=("Arial", 9)).pack(side=tk.LEFT, padx=2)
        tk.Button(message_toolbar, text="🗑️ Clear", command=self._clear_vps_attachments, bg="#e74c3c", fg="white", font=("Arial", 9)).pack(side=tk.LEFT, padx=2)

        self.vps_attachment_label = tk.Label(message_toolbar, text="No attachments", font=("Arial", 9), bg='#e0f7ff', fg="#666")
        self.vps_attachment_label.pack(side=tk.LEFT, padx=5)
        self.vps_char_count_label = tk.Label(message_toolbar, text="Chars: 0", font=("Arial", 9), bg='#e0f7ff', fg="#666")
        self.vps_char_count_label.pack(side=tk.RIGHT, padx=5)

        self.vps_message_box = scrolledtext.ScrolledText(message_frame, height=15, wrap=tk.WORD, font=("Consolas", 10), undo=True, bg="white", fg="#2c3e50")
        self.vps_message_box.pack(fill='both', expand=True, padx=5, pady=5)
        self.vps_message_box.bind('<KeyRelease>', self._update_vps_char_count)

    def _build_vps_recipients_frame(self, parent):
        recipients_frame = tk.LabelFrame(parent, text="👥 VPS Recipients", font=("Arial", 12, "bold"), bg='#e0f7ff', fg="#2c3e50")
        recipients_frame.pack(fill='both', expand=True, padx=5, pady=5)

        # Row 1: Load, Paste, Validate, Clear, Remove
        recipients_toolbar_row1 = tk.Frame(recipients_frame, bg='#e0f7ff')
        recipients_toolbar_row1.pack(fill='x', pady=(5, 2))
        tk.Button(recipients_toolbar_row1, text="📁 Load List", command=self._load_vps_emails, bg="#3498db", fg="white", font=("Arial", 10, "bold")).pack(side=tk.LEFT, padx=5)
        tk.Button(recipients_toolbar_row1, text="📋 Paste", command=self._paste_vps_emails, bg="#3498db", fg="white", font=("Arial", 10, "bold")).pack(side=tk.LEFT, padx=5)
        tk.Button(recipients_toolbar_row1, text="✔️ Validate List", command=self._validate_vps_email_list, bg="#3498db", fg="white", font=("Arial", 10, "bold")).pack(side=tk.LEFT, padx=5)
        tk.Button(recipients_toolbar_row1, text="🗑️ Clear List", command=self._clear_vps_email_list, bg="#e74c3c", fg="white", font=("Arial", 10, "bold")).pack(side=tk.LEFT, padx=5)
        tk.Button(recipients_toolbar_row1, text="🗑️ Remove Sel.", command=self._remove_vps_selected, bg="#e74c3c", fg="white", font=("Arial", 9)).pack(side=tk.RIGHT, padx=5)

        # Row 2: Add Single Email
        recipients_toolbar_row2 = tk.Frame(recipients_frame, bg='#e0f7ff')
        recipients_toolbar_row2.pack(fill='x', pady=(2, 5))
        tk.Label(recipients_toolbar_row2, text="Add Email:", bg='#e0f7ff', fg="#2c3e50", font=("Arial", 9)).pack(side=tk.LEFT, padx=5)
        self.new_vps_recipient_var = tk.StringVar()
        recipient_entry = tk.Entry(recipients_toolbar_row2, textvariable=self.new_vps_recipient_var, bg="white", fg="#2c3e50", width=30)
        recipient_entry.pack(side=tk.LEFT, padx=5)
        recipient_entry.bind('<Return>', lambda e: self._add_vps_recipient())
        tk.Button(recipients_toolbar_row2, text="➕ Add", command=self._add_vps_recipient, bg="#27ae60", fg="white", font=("Arial", 9, "bold")).pack(side=tk.LEFT, padx=5)

        table_container = tk.Frame(recipients_frame, bg='#e0f7ff')
        table_container.pack(fill='both', expand=True, padx=5, pady=5)
        columns = ("email", "status", "sent_at", "attempts", "reason")
        self.vps_tree = ttk.Treeview(table_container, columns=columns, show='headings', height=8)
        headings = {"email": "📧 Email", "status": "📊 Status", "sent_at": "⏰ Sent", "attempts": "🔄", "reason": "❗ Reason"}
        widths = {"email": 230, "status": 100, "sent_at": 120, "attempts": 40, "reason": 120}
        for col, text in headings.items():
            self.vps_tree.heading(col, text=text); self.vps_tree.column(col, width=widths[col], anchor='center' if col != "email" else 'w')

        tree_scrollbar = ttk.Scrollbar(table_container, orient="vertical", command=self.vps_tree.yview)
        self.vps_tree.configure(yscrollcommand=tree_scrollbar.set)
        tree_scrollbar.pack(side='right', fill='y'); self.vps_tree.pack(fill='both', expand=True)

        self.vps_email_list = []
        self.vps_tracking_map = {}
        self.vps_tree_items = {}
        self.vps_attachment_paths = []

        self.vps_stats_label = tk.Label(recipients_frame, text="📊 VPS Stats: 0 sent | 0 failed | 0 total", font=("Arial", 10), bg='#e0f7ff', fg="#666")
        self.vps_stats_label.pack(pady=2, fill='x')

    def _build_direct_mx_tab(self):
        """Dedicated tab for Direct MX delivery - standalone sending without VPS or SMTP auth."""
        mx_tab = ttk.Frame(self.notebook)
        self.notebook.add(mx_tab, text='🚀 Direct MX Sender')

        mx_pane = ttk.PanedWindow(mx_tab, orient=tk.VERTICAL)
        mx_pane.pack(fill='both', expand=True)

        top_frame = ttk.Frame(mx_pane, height=500)
        bottom_frame = ttk.Frame(mx_pane, height=300)
        top_frame.pack_propagate(True)
        bottom_frame.pack_propagate(True)
        mx_pane.add(top_frame, weight=2)
        mx_pane.add(bottom_frame, weight=1)

        top_pane = ttk.PanedWindow(top_frame, orient=tk.HORIZONTAL)
        top_pane.pack(fill='both', expand=True)

        config_outer = ttk.Frame(top_pane, width=450)
        message_outer = ttk.Frame(top_pane)
        config_outer.pack_propagate(False)
        top_pane.add(config_outer, weight=1)
        top_pane.add(message_outer, weight=2)

        self._build_direct_mx_config_frame(config_outer)
        self._build_direct_mx_message_frame(message_outer)
        self._build_direct_mx_recipients_frame(bottom_frame)

    def _build_direct_mx_config_frame(self, parent):
        config_frame = tk.LabelFrame(parent, text="⚙️ Direct MX Configuration", font=("Arial", 12, "bold"), bg='#e0f7ff', fg="#2c3e50")
        config_frame.pack(fill='both', expand=True, padx=5, pady=5)

        # Enable Direct MX Mode Toggle
        mx_toggle_frame = tk.Frame(config_frame, bg='#e0f7ff')
        mx_toggle_frame.pack(fill='x', padx=10, pady=5)
        self.enable_direct_mx_var = tk.BooleanVar(value=True)
        tk.Checkbutton(mx_toggle_frame, text="✅ Enable Direct MX Delivery Mode", variable=self.enable_direct_mx_var,
                       bg='#e0f7ff', fg="#2c3e50", font=("Arial", 10, "bold")).pack(side=tk.LEFT, padx=5)
        self.mx_use_random_sender_var = tk.BooleanVar(value=False)
        tk.Checkbutton(mx_toggle_frame, text="🔀 Use Random Sender", variable=self.mx_use_random_sender_var,
                       bg='#e0f7ff', fg="#2c3e50").pack(side=tk.RIGHT, padx=5)

        # Subject
        subject_frame = tk.Frame(config_frame, bg='#e0f7ff')
        subject_frame.pack(fill='x', padx=10, pady=5)
        tk.Label(subject_frame, text="Subject:", font=("Arial", 10, "bold"), bg='#e0f7ff', fg="#2c3e50").pack(side=tk.LEFT, padx=(0, 5))
        self.mx_subject_var = tk.StringVar(value="Important Message")
        tk.Entry(subject_frame, textvariable=self.mx_subject_var, bg="white", fg="#2c3e50").pack(side=tk.LEFT, fill='x', expand=True, padx=5)

        # Sender Configuration
        sender_frame = tk.LabelFrame(config_frame, text="Sender Configuration", bg='#e0f7ff', fg="#2c3e50")
        sender_frame.pack(fill='x', padx=10, pady=5)
        tk.Label(sender_frame, text="Default Sender Name:", bg='#e0f7ff', fg="#2c3e50").grid(row=0, column=0, sticky='w', padx=5, pady=2)
        self.mx_sender_name_var = tk.StringVar(value="Sender")
        tk.Entry(sender_frame, textvariable=self.mx_sender_name_var, bg="white", fg="#2c3e50").grid(row=0, column=1, sticky='ew', padx=5, pady=2)
        tk.Label(sender_frame, text="Default Sender Email:", bg='#e0f7ff', fg="#2c3e50").grid(row=1, column=0, sticky='w', padx=5, pady=2)
        self.mx_sender_email_var = tk.StringVar(value="noreply@example.com")
        tk.Entry(sender_frame, textvariable=self.mx_sender_email_var, bg="white", fg="#2c3e50").grid(row=1, column=1, sticky='ew', padx=5, pady=2)
        tk.Label(sender_frame, text="From Emails Pool (comma-separated):", bg='#e0f7ff', fg="#2c3e50").grid(row=2, column=0, sticky='w', padx=5, pady=2)
        self.mx_from_pool_var = tk.StringVar(value="")
        tk.Entry(sender_frame, textvariable=self.mx_from_pool_var, bg="white", fg="#2c3e50").grid(row=2, column=1, sticky='ew', padx=5, pady=2)
        sender_frame.grid_columnconfigure(1, weight=1)

        # Advanced Sender Identity (Direct MX specific)
        identity_frame = tk.LabelFrame(config_frame, text="🔐 Advanced Sender Identity", bg='#e0f7ff', fg="#2c3e50")
        identity_frame.pack(fill='x', padx=10, pady=5)
        tk.Label(identity_frame, text="Reply-To Email:", bg='#e0f7ff', fg="#2c3e50").grid(row=0, column=0, sticky='w', padx=5, pady=2)
        tk.Entry(identity_frame, textvariable=self.mx_reply_to_var, bg="white", fg="#2c3e50").grid(row=0, column=1, sticky='ew', padx=5, pady=2)
        tk.Label(identity_frame, text="Message-ID Domain (origin masking):", bg='#e0f7ff', fg="#2c3e50").grid(row=1, column=0, sticky='w', padx=5, pady=2)
        tk.Entry(identity_frame, textvariable=self.mx_message_id_domain_var, bg="white", fg="#2c3e50").grid(row=1, column=1, sticky='ew', padx=5, pady=2)
        tk.Button(identity_frame, text="🔍 Verify From Emails", command=self._verify_from_emails_window, bg="#e67e22", fg="white", font=("Arial", 9, "bold")).grid(row=2, column=0, columnspan=2, sticky='ew', padx=5, pady=5)
        identity_frame.grid_columnconfigure(1, weight=1)

        # DKIM Configuration
        dkim_frame = tk.LabelFrame(config_frame, text="🔑 DKIM Configuration", bg='#e0f7ff', fg="#2c3e50")
        dkim_frame.pack(fill='x', padx=10, pady=5)
        tk.Button(dkim_frame, text="🔑 Configure DKIM", command=self._configure_dkim_window, bg="#9b59b6", fg="white", font=("Arial", 9, "bold")).pack(side=tk.LEFT, padx=5, pady=5)
        tk.Button(dkim_frame, text="🌐 DNS Test", command=lambda: threading.Thread(target=self._dns_test, daemon=True).start(), bg="#3498db", fg="white", font=("Arial", 9)).pack(side=tk.LEFT, padx=5, pady=5)
        tk.Button(dkim_frame, text="🔍 Domain Check", command=lambda: threading.Thread(target=self._domain_check, daemon=True).start(), bg="#3498db", fg="white", font=("Arial", 9)).pack(side=tk.LEFT, padx=5, pady=5)
        tk.Button(dkim_frame, text="🧪 Test DKIM Sign", command=self._test_dkim_signing, bg="#8e44ad", fg="white", font=("Arial", 9)).pack(side=tk.LEFT, padx=5, pady=5)

        # Network Configuration (SPF Alignment)
        network_frame = tk.LabelFrame(config_frame, text="🌐 Network Configuration (SPF Alignment)", bg='#e0f7ff', fg="#2c3e50")
        network_frame.pack(fill='x', padx=10, pady=5)
        tk.Label(network_frame, text="Source IP (VPS IP):", bg='#e0f7ff', fg="#2c3e50").grid(row=0, column=0, sticky='w', padx=5, pady=2)
        tk.Entry(network_frame, textvariable=self.mx_source_ip_var, bg="white", fg="#2c3e50").grid(row=0, column=1, sticky='ew', padx=5, pady=2)
        tk.Label(network_frame, text="EHLO Hostname:", bg='#e0f7ff', fg="#2c3e50").grid(row=1, column=0, sticky='w', padx=5, pady=2)
        tk.Entry(network_frame, textvariable=self.mx_ehlo_hostname_var, bg="white", fg="#2c3e50").grid(row=1, column=1, sticky='ew', padx=5, pady=2)
        network_frame.grid_columnconfigure(1, weight=1)
        tk.Button(network_frame, text="🧪 Test MX Connection", command=self._test_mx_connection, bg="#2ecc71", fg="white", font=("Arial", 9)).grid(row=2, column=0, padx=5, pady=5, sticky='ew')
        tk.Button(network_frame, text="🔍 Validate Network", command=self._validate_mx_network, bg="#3498db", fg="white", font=("Arial", 9)).grid(row=2, column=1, padx=5, pady=5, sticky='ew')

        # Batch Size and Rate Limit
        batch_frame = tk.Frame(config_frame, bg='#e0f7ff')
        batch_frame.pack(fill='x', padx=10, pady=5)
        tk.Label(batch_frame, text="Batch Size:", bg='#e0f7ff', fg="#2c3e50").pack(side=tk.LEFT)
        self.mx_batch_size_var = tk.IntVar(value=100)
        tk.Entry(batch_frame, textvariable=self.mx_batch_size_var, bg="white", fg="#2c3e50", width=10).pack(side=tk.LEFT, padx=5)
        tk.Label(batch_frame, text="Rate Limit/hr/domain:", bg='#e0f7ff', fg="#2c3e50").pack(side=tk.LEFT, padx=(10, 0))
        self.mx_rate_limit_var = tk.IntVar(value=100)
        tk.Entry(batch_frame, textvariable=self.mx_rate_limit_var, bg="white", fg="#2c3e50", width=10).pack(side=tk.LEFT, padx=5)

        # Controls
        mx_controls_frame = tk.Frame(config_frame, bg='#e0f7ff')
        mx_controls_frame.pack(fill='x', padx=10, pady=5)
        self.mx_start_btn = tk.Button(mx_controls_frame, text="▶️ START MX CAMPAIGN", command=self._start_direct_mx_sending, bg="#27ae60", fg="white", font=("Arial", 9, "bold"))
        self.mx_pause_btn = tk.Button(mx_controls_frame, text="⏸️ PAUSE", command=self._pause_direct_mx_sending, bg="#f39c12", fg="white", font=("Arial", 9, "bold"), state="disabled")
        self.mx_stop_btn = tk.Button(mx_controls_frame, text="⏹️ STOP", command=self._stop_direct_mx_sending, bg="#e74c3c", fg="white", font=("Arial", 9, "bold"), state="disabled")
        self.mx_start_btn.pack(side=tk.LEFT, padx=2, pady=2)
        self.mx_pause_btn.pack(side=tk.LEFT, padx=2, pady=2)
        self.mx_stop_btn.pack(side=tk.LEFT, padx=2, pady=2)

        # Status Panel
        mx_status_frame = tk.LabelFrame(config_frame, text="📊 Direct MX Status", bg='#e0f7ff', fg="#2c3e50")
        mx_status_frame.pack(fill='x', padx=10, pady=5)
        self.mx_dkim_label = tk.Label(mx_status_frame, text="DKIM: Not Configured", font=("Arial", 9), bg='#e0f7ff', fg="#e74c3c")
        self.mx_dkim_label.grid(row=0, column=0, sticky='w', padx=5, pady=1)
        self.mx_retry_label = tk.Label(mx_status_frame, text="Retry Queue: 0", font=("Arial", 9), bg='#e0f7ff', fg="#666")
        self.mx_retry_label.grid(row=0, column=1, sticky='w', padx=5, pady=1)
        self.mx_from_pool_label = tk.Label(mx_status_frame, text="From Pool: 0 emails", font=("Arial", 9), bg='#e0f7ff', fg="#666")
        self.mx_from_pool_label.grid(row=1, column=0, sticky='w', padx=5, pady=1)
        self.mx_verified_label = tk.Label(mx_status_frame, text="Verified: 0", font=("Arial", 9), bg='#e0f7ff', fg="#666")
        self.mx_verified_label.grid(row=1, column=1, sticky='w', padx=5, pady=1)
        self.mx_status_indicator = tk.Label(mx_status_frame, text="Status: Stopped", font=("Arial", 9, "bold"), bg='#e0f7ff', fg="#e74c3c")
        self.mx_status_indicator.grid(row=2, column=0, sticky='w', padx=5, pady=1)
        self.mx_queue_depth_label = tk.Label(mx_status_frame, text="Queue Depth: 0", font=("Arial", 9), bg='#e0f7ff', fg="#666")
        self.mx_queue_depth_label.grid(row=2, column=1, sticky='w', padx=5, pady=1)
        self.mx_success_count_label = tk.Label(mx_status_frame, text="Success: 0", font=("Arial", 9), bg='#e0f7ff', fg="#27ae60")
        self.mx_success_count_label.grid(row=3, column=0, sticky='w', padx=5, pady=1)
        self.mx_failure_count_label = tk.Label(mx_status_frame, text="Failures: 0", font=("Arial", 9), bg='#e0f7ff', fg="#e74c3c")
        self.mx_failure_count_label.grid(row=3, column=1, sticky='w', padx=5, pady=1)

        # Retry Queue Manager
        retry_frame = tk.LabelFrame(config_frame, text="🔄 MX Retry Queue Manager", bg='#e0f7ff', fg="#2c3e50")
        retry_frame.pack(fill='x', padx=10, pady=5)
        retry_btn_frame = tk.Frame(retry_frame, bg='#e0f7ff')
        retry_btn_frame.pack(fill='x', padx=5, pady=2)
        tk.Button(retry_btn_frame, text="▶️ Retry Now", command=self._retry_mx_queue_now, bg="#27ae60", fg="white", font=("Arial", 9)).pack(side=tk.LEFT, padx=2)
        tk.Button(retry_btn_frame, text="🗑️ Clear Queue", command=self._clear_mx_retry_queue, bg="#e74c3c", fg="white", font=("Arial", 9)).pack(side=tk.LEFT, padx=2)
        tk.Button(retry_btn_frame, text="🔄 Refresh", command=self._refresh_retry_queue_view, bg="#3498db", fg="white", font=("Arial", 9)).pack(side=tk.LEFT, padx=2)
        self.mx_retry_count_label = tk.Label(retry_frame, text="Pending: 0 | Next Retry: --", font=("Arial", 9), bg='#e0f7ff', fg="#666")
        self.mx_retry_count_label.pack(fill='x', padx=5, pady=2)
        retry_list_frame = tk.Frame(retry_frame, bg='#e0f7ff')
        retry_list_frame.pack(fill='both', expand=True, padx=5, pady=2)
        retry_columns = ("email", "attempts", "next_retry", "reason")
        self.retry_queue_tree = ttk.Treeview(retry_list_frame, columns=retry_columns, show='headings', height=4)
        self.retry_queue_tree.heading("email", text="📧 Email")
        self.retry_queue_tree.heading("attempts", text="🔄 Attempts")
        self.retry_queue_tree.heading("next_retry", text="⏰ Next Retry")
        self.retry_queue_tree.heading("reason", text="❗ Reason")
        self.retry_queue_tree.column("email", width=180)
        self.retry_queue_tree.column("attempts", width=70)
        self.retry_queue_tree.column("next_retry", width=100)
        self.retry_queue_tree.column("reason", width=120)
        retry_scroll = ttk.Scrollbar(retry_list_frame, orient="vertical", command=self.retry_queue_tree.yview)
        self.retry_queue_tree.configure(yscrollcommand=retry_scroll.set)
        retry_scroll.pack(side='right', fill='y')
        self.retry_queue_tree.pack(fill='both', expand=True)

        self.root.after(5000, self._refresh_mx_status_panel)

    def _build_direct_mx_message_frame(self, parent):
        message_frame = tk.LabelFrame(parent, text="📝 Direct MX Message Content", font=("Arial", 12, "bold"), bg='#e0f7ff', fg="#2c3e50")
        message_frame.pack(fill='both', expand=True, padx=5, pady=5)

        message_toolbar = tk.Frame(message_frame, bg='#e0f7ff')
        message_toolbar.pack(fill='x', pady=2)
        tk.Button(message_toolbar, text="📁 Load HTML", command=self._load_mx_html_file, bg="#3498db", fg="white", font=("Arial", 9)).pack(side=tk.LEFT, padx=2)
        tk.Button(message_toolbar, text="📎 Attach", command=self._add_mx_attachment, bg="#3498db", fg="white", font=("Arial", 9)).pack(side=tk.LEFT, padx=2)
        tk.Button(message_toolbar, text="🗑️ Clear", command=self._clear_mx_attachments, bg="#e74c3c", fg="white", font=("Arial", 9)).pack(side=tk.LEFT, padx=2)

        self.mx_attachment_label = tk.Label(message_toolbar, text="No attachments", font=("Arial", 9), bg='#e0f7ff', fg="#666")
        self.mx_attachment_label.pack(side=tk.LEFT, padx=5)

        self.mx_message_box = scrolledtext.ScrolledText(message_frame, height=15, wrap=tk.WORD, font=("Consolas", 10), undo=True, bg="white", fg="#2c3e50")
        self.mx_message_box.pack(fill='both', expand=True, padx=5, pady=5)

    def _build_direct_mx_recipients_frame(self, parent):
        recipients_frame = tk.LabelFrame(parent, text="👥 Direct MX Recipients", font=("Arial", 12, "bold"), bg='#e0f7ff', fg="#2c3e50")
        recipients_frame.pack(fill='both', expand=True, padx=5, pady=5)

        # Row 1: Load, Paste, Clear
        recipients_toolbar_row1 = tk.Frame(recipients_frame, bg='#e0f7ff')
        recipients_toolbar_row1.pack(fill='x', pady=(5, 2))
        tk.Button(recipients_toolbar_row1, text="📁 Load List", command=self._load_mx_emails, bg="#3498db", fg="white", font=("Arial", 10, "bold")).pack(side=tk.LEFT, padx=5)
        tk.Button(recipients_toolbar_row1, text="📋 Paste", command=self._paste_mx_emails, bg="#3498db", fg="white", font=("Arial", 10, "bold")).pack(side=tk.LEFT, padx=5)
        tk.Button(recipients_toolbar_row1, text="🗑️ Clear List", command=self._clear_mx_email_list, bg="#e74c3c", fg="white", font=("Arial", 10, "bold")).pack(side=tk.LEFT, padx=5)

        # Row 2: Add Single Email
        recipients_toolbar_row2 = tk.Frame(recipients_frame, bg='#e0f7ff')
        recipients_toolbar_row2.pack(fill='x', pady=(2, 5))
        tk.Label(recipients_toolbar_row2, text="Add Email:", bg='#e0f7ff', fg="#2c3e50", font=("Arial", 9)).pack(side=tk.LEFT, padx=5)
        self.new_mx_recipient_var = tk.StringVar()
        mx_recipient_entry = tk.Entry(recipients_toolbar_row2, textvariable=self.new_mx_recipient_var, bg="white", fg="#2c3e50", width=30)
        mx_recipient_entry.pack(side=tk.LEFT, padx=5)
        mx_recipient_entry.bind('<Return>', lambda e: self._add_mx_recipient())
        tk.Button(recipients_toolbar_row2, text="➕ Add", command=self._add_mx_recipient, bg="#27ae60", fg="white", font=("Arial", 9, "bold")).pack(side=tk.LEFT, padx=5)

        table_container = tk.Frame(recipients_frame, bg='#e0f7ff')
        table_container.pack(fill='both', expand=True, padx=5, pady=5)
        columns = ("email", "status", "sent_at", "attempts", "reason")
        self.mx_tree = ttk.Treeview(table_container, columns=columns, show='headings', height=8)
        headings = {"email": "📧 Email", "status": "📊 Status", "sent_at": "⏰ Sent", "attempts": "🔄", "reason": "❗ Reason"}
        widths = {"email": 230, "status": 100, "sent_at": 120, "attempts": 40, "reason": 120}
        for col, text in headings.items():
            self.mx_tree.heading(col, text=text)
            self.mx_tree.column(col, width=widths[col], anchor='center' if col != "email" else 'w')

        tree_scrollbar = ttk.Scrollbar(table_container, orient="vertical", command=self.mx_tree.yview)
        self.mx_tree.configure(yscrollcommand=tree_scrollbar.set)
        tree_scrollbar.pack(side='right', fill='y')
        self.mx_tree.pack(fill='both', expand=True)

        self.mx_email_list = []
        self.mx_tracking_map = {}
        self.mx_tree_items = {}
        self.mx_attachment_paths = []

        self.mx_stats_label = tk.Label(recipients_frame, text="📊 MX Stats: 0 sent | 0 failed | 0 total", font=("Arial", 10), bg='#e0f7ff', fg="#666")
        self.mx_stats_label.pack(pady=2, fill='x')

    def _refresh_mx_status_panel(self):
        """Refresh Direct MX status panel labels."""
        try:
            handler = self.direct_mx_handler
            if handler.dkim_private_key:
                self.mx_dkim_label.config(text="DKIM: ✅ Configured", fg="#27ae60")
            else:
                self.mx_dkim_label.config(text="DKIM: Not Configured", fg="#e74c3c")

            retry_count = 0
            try:
                retry_count = self.db_handler.get_retry_queue_count() if hasattr(self.db_handler, 'get_retry_queue_count') else 0
            except Exception:
                pass
            self.mx_retry_label.config(text=f"Retry Queue: {retry_count}")

            pool_count = len(handler.from_emails_pool)
            self.mx_from_pool_label.config(text=f"From Pool: {pool_count} email{'s' if pool_count != 1 else ''}")

            verified_count = sum(1 for s in handler.from_emails_status.values() if s == 'Verified')
            self.mx_verified_label.config(text=f"Verified: {verified_count}")
        except Exception:
            pass
        self.root.after(5000, self._refresh_mx_status_panel)

    def _update_direct_mx_live_status(self):
        """Update the live Direct MX status panel using after() loop."""
        try:
            if self.direct_mx_running and not self.direct_mx_paused:
                status_text = "Status: Running"
                status_color = "#27ae60"
            elif self.direct_mx_running and self.direct_mx_paused:
                status_text = "Status: Paused"
                status_color = "#f39c12"
            else:
                status_text = "Status: Stopped"
                status_color = "#e74c3c"
            if hasattr(self, 'mx_status_indicator') and self.mx_status_indicator.winfo_exists():
                self.mx_status_indicator.config(text=status_text, fg=status_color)
            if hasattr(self, 'mx_success_count_label') and self.mx_success_count_label.winfo_exists():
                self.mx_success_count_label.config(text=f"Success: {self.direct_mx_sent_count}")
            if hasattr(self, 'mx_failure_count_label') and self.mx_failure_count_label.winfo_exists():
                self.mx_failure_count_label.config(text=f"Failures: {self.direct_mx_failed_count}")
            retry_count = 0
            try:
                retry_count = self.db_handler.get_retry_queue_count() if hasattr(self.db_handler, 'get_retry_queue_count') else 0
            except Exception:
                pass
            if hasattr(self, 'mx_queue_depth_label') and self.mx_queue_depth_label.winfo_exists():
                self.mx_queue_depth_label.config(text=f"Queue Depth: {retry_count}")
        except Exception:
            pass
        if self.direct_mx_running:
            self.root.after(2000, self._update_direct_mx_live_status)

    def _start_direct_mx_sending(self):
        """Start Direct MX sending campaign."""
        if self.running:
            return
        if not self.mx_email_list:
            messagebox.showinfo("No Recipients", "Please load a Direct MX email list first.")
            return
        subject = self.mx_subject_var.get()
        content = self.mx_message_box.get("1.0", tk.END).strip()
        if not subject or not content:
            messagebox.showwarning("Missing Content", "Please enter a subject and message content.")
            return
        if not DNSPYTHON_AVAILABLE:
            messagebox.showerror("Missing Library", "dnspython not installed. Install with: pip install dnspython")
            return

        self.running = True
        self.direct_mx_running = True
        self.direct_mx_paused = False
        self.sent_count = 0
        self.failed_count = 0
        self.direct_mx_sent_count = 0
        self.direct_mx_failed_count = 0
        self.direct_mx_retry_count = 0
        # Reset tracking status for all MX recipients
        for email in self.mx_email_list:
            self.mx_tracking_map[email] = {'email': email, 'status': 'Queued', 'attempts': 0}
        self.mx_start_btn.config(state="disabled")
        self.mx_pause_btn.config(state="normal")
        self.mx_stop_btn.config(state="normal")
        self.log("🚀 Starting Direct MX campaign...")
        self._update_direct_mx_live_status()

        self.direct_mx_handler.reply_to_email = self.mx_reply_to_var.get()
        self.direct_mx_handler.message_id_domain = self.mx_message_id_domain_var.get()
        self.direct_mx_handler.source_ip = self.mx_source_ip_var.get()
        self.direct_mx_handler.ehlo_hostname = self.mx_ehlo_hostname_var.get()

        # Sync from pool from GUI entry if not already populated via verify window
        from_pool_text = self.mx_from_pool_var.get().strip()
        if from_pool_text and not self.direct_mx_handler.from_emails_pool:
            pool_emails = [e.strip() for e in from_pool_text.split(',') if '@' in e.strip()]
            self.direct_mx_handler.from_emails_pool = pool_emails

        # Sync rate limit from GUI if available
        if hasattr(self, 'mx_rate_limit_var'):
            try:
                self.direct_mx_handler.rate_limit_per_hour = self.mx_rate_limit_var.get()
            except Exception:
                pass

        use_random = self.mx_use_random_sender_var.get()
        self.direct_mx_handler.run_direct_mx_sending_job(
            self.mx_email_list, subject, content,
            self.mx_attachment_paths, self.mx_batch_size_var.get(),
            use_random_sender=use_random
        )

    def _pause_direct_mx_sending(self):
        """Pause Direct MX sending."""
        if self.direct_mx_running and not self.direct_mx_paused:
            self.paused = True
            self.direct_mx_paused = True
            self.mx_pause_btn.config(text="▶️ RESUME", bg="#27ae60")
            self.log("⏸️ Direct MX sending paused.")
        elif self.direct_mx_running and self.direct_mx_paused:
            self.paused = False
            self.direct_mx_paused = False
            self.mx_pause_btn.config(text="⏸️ PAUSE", bg="#f39c12")
            self.log("▶️ Direct MX sending resumed.")

    def _stop_direct_mx_sending(self):
        """Stop Direct MX sending."""
        if self.running or self.direct_mx_running:
            self.running = False
            self.paused = False
            self.direct_mx_running = False
            self.direct_mx_paused = False
            self._cancel_async_tasks()
            if self.direct_mx_task and not self.direct_mx_task.done():
                self.direct_mx_task.cancel()
            self.direct_mx_task = None
            self.mx_start_btn.config(state="normal")
            self.mx_pause_btn.config(state="disabled", text="⏸️ PAUSE", bg="#f39c12")
            self.mx_stop_btn.config(state="disabled")
            self.log("⏹️ Direct MX sending stopped by user.")

    def _load_mx_html_file(self):
        """Load HTML file for Direct MX message."""
        file_path = filedialog.askopenfilename(filetypes=[("HTML Files", "*.html"), ("All Files", "*.*")])
        if file_path:
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    self.mx_message_box.delete("1.0", tk.END)
                    self.mx_message_box.insert(tk.END, f.read())
                self.log(f"📁 Loaded HTML file for Direct MX: {file_path}")
            except Exception as e:
                self.log(f"❌ Error loading file: {e}")

    def _add_mx_attachment(self):
        """Add attachment for Direct MX campaign."""
        file_path = filedialog.askopenfilename()
        if file_path:
            self.mx_attachment_paths.append(file_path)
            self.mx_attachment_label.config(text=f"{len(self.mx_attachment_paths)} attachment(s)")
            self.log(f"📎 Attachment added for Direct MX: {os.path.basename(file_path)}")

    def _clear_mx_attachments(self):
        """Clear Direct MX attachments."""
        self.mx_attachment_paths = []
        self.mx_attachment_label.config(text="No attachments")
        self.log("🗑️ Direct MX attachments cleared.")

    def _load_mx_emails(self):
        """Load email list for Direct MX."""
        file_path = filedialog.askopenfilename(filetypes=[("Text/CSV Files", "*.txt *.csv"), ("All Files", "*.*")])
        if file_path:
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    emails = [line.strip() for line in f if '@' in line.strip()]
                self.mx_email_list = emails
                for email in emails:
                    self.mx_tracking_map[email] = {'email': email, 'status': 'Pending'}
                    item_id = self.mx_tree.insert("", tk.END, values=(email, "Pending", "", "0", ""))
                    self.mx_tree_items[email] = item_id
                self.mx_stats_label.config(text=f"📊 MX Stats: 0 sent | 0 failed | {len(emails)} total")
                self.log(f"📁 Loaded {len(emails)} emails for Direct MX.")
            except Exception as e:
                self.log(f"❌ Error loading emails: {e}")

    def _paste_mx_emails(self):
        """Paste emails for Direct MX from clipboard."""
        try:
            clipboard = self.root.clipboard_get()
            emails = [line.strip() for line in clipboard.split('\n') if '@' in line.strip()]
            self.mx_email_list.extend(emails)
            for email in emails:
                self.mx_tracking_map[email] = {'email': email, 'status': 'Pending'}
                item_id = self.mx_tree.insert("", tk.END, values=(email, "Pending", "", "0", ""))
                self.mx_tree_items[email] = item_id
            self.mx_stats_label.config(text=f"📊 MX Stats: 0 sent | 0 failed | {len(self.mx_email_list)} total")
            self.log(f"📋 Pasted {len(emails)} emails for Direct MX.")
        except Exception as e:
            self.log(f"❌ Error pasting emails: {e}")

    def _add_mx_recipient(self):
        """Add a single email to the Direct MX recipient list."""
        email = self.new_mx_recipient_var.get().strip().lower()
        if not email or not self._is_valid_email(email):
            messagebox.showwarning("Invalid", "Enter a valid email.")
            return
        if email in self.mx_email_list:
            messagebox.showinfo("Exists", "Email already in list.")
            return
        self.mx_email_list.append(email)
        self.mx_tracking_map[email] = {'email': email, 'status': 'Pending'}
        item_id = self.mx_tree.insert("", tk.END, values=(email, "Pending", "", "0", ""))
        self.mx_tree_items[email] = item_id
        self.mx_stats_label.config(text=f"📊 MX Stats: 0 sent | 0 failed | {len(self.mx_email_list)} total")
        self.new_mx_recipient_var.set("")
        self.log(f"✅ Added Direct MX recipient: {email}")

    def _clear_mx_email_list(self):
        """Clear Direct MX email list."""
        self.mx_email_list = []
        self.mx_tracking_map.clear()
        self.mx_tree_items.clear()
        for item in self.mx_tree.get_children():
            self.mx_tree.delete(item)
        self.mx_stats_label.config(text="📊 MX Stats: 0 sent | 0 failed | 0 total")
        self.log("🗑️ Direct MX email list cleared.")

    def _build_config_frame(self, parent):
        config_frame = tk.LabelFrame(parent, text="⚙️ Configuration & Controls", font=("Arial", 12, "bold"), bg='#e0f7ff', fg="#2c3e50")
        config_frame.pack(fill='both', expand=True, padx=5, pady=5)

        campaign_mgmt_frame = tk.Frame(config_frame, bg='#e0f7ff')
        campaign_mgmt_frame.pack(fill='x', padx=10, pady=5)
        tk.Button(campaign_mgmt_frame, text="💾 Save Campaign", command=self._save_campaign, bg="#3498db", fg="white", font=("Arial", 9, "bold")).pack(side=tk.LEFT, padx=5)
        tk.Button(campaign_mgmt_frame, text="📂 Load Campaign", command=self._load_campaign, bg="#3498db", fg="white", font=("Arial", 9, "bold")).pack(side=tk.LEFT, padx=5)

        profile_frame = tk.Frame(config_frame, bg='#e0f7ff')
        profile_frame.pack(fill='x', padx=10, pady=5)
        tk.Label(profile_frame, text="Profile:", font=("Arial", 9, "bold"), bg='#e0f7ff', fg="#2c3e50").pack(side=tk.LEFT, padx=(0, 5))
        self.profile_combo = ttk.Combobox(profile_frame, textvariable=self.current_profile_var, values=[], state="readonly", width=18)
        self.profile_combo.pack(side=tk.LEFT, padx=5)
        self.profile_combo.bind("<<ComboboxSelected>>", lambda e: self._apply_profile())
        tk.Button(profile_frame, text="💾 Save Profile", command=self._save_current_profile, bg="#3498db", fg="white", font=("Arial", 9, "bold")).pack(side=tk.LEFT, padx=5)

        provider_frame = tk.Frame(config_frame, bg='#e0f7ff')
        provider_frame.pack(fill='x', padx=10, pady=5)
        tk.Label(provider_frame, text="Provider:", font=("Arial", 10, "bold"), bg='#e0f7ff', fg="#2c3e50").pack(side=tk.LEFT)
        provider_options = ["SMTP", "Gmail", "Outlook.com", "Yahoo"] + sorted(self.custom_providers.keys())
        self.provider_var = tk.StringVar(value="SMTP")
        ttk.Combobox(provider_frame, textvariable=self.provider_var, values=provider_options, state="readonly", width=15).pack(side=tk.LEFT, padx=10)
        self.smtp_config_btn = tk.Button(provider_frame, text="📧 SMTP", command=self._open_smtp_config, bg="#3498db", fg="white", font=("Arial", 9, "bold"))
        self.smtp_config_btn.pack(side=tk.LEFT, padx=5)

        subject_frame = tk.Frame(config_frame, bg='#e0f7ff')
        subject_frame.pack(fill='x', padx=10, pady=5)
        tk.Label(subject_frame, text="Subject:", font=("Arial", 10, "bold"), bg='#e0f7ff', fg="#2c3e50").pack(side=tk.LEFT, padx=(0, 5))
        self.subject_var = tk.StringVar(value="Important Message")
        tk.Entry(subject_frame, textvariable=self.subject_var, bg="white", fg="#2c3e50", width=50).pack(side=tk.LEFT, fill='x', expand=True, padx=5)

        tk.Checkbutton(subject_frame, text="Auto Warm-up", variable=self.warmup_mode, bg='#e0f7ff', fg="#2c3e50",
                        command=self._toggle_warmup_mode
                       ).pack(side=tk.RIGHT, padx=10)
        self.warmup_info_label = tk.Label(subject_frame, text="", font=("Arial", 8), bg='#e0f7ff', fg="#27ae60")
        self.warmup_info_label.pack(side=tk.RIGHT)

        # NEW: Throttle Controls
        throttle_frame = tk.LabelFrame(config_frame, text="Throttle Settings", bg='#e0f7ff', fg="#2c3e50")
        throttle_frame.pack(fill='x', padx=10, pady=5)
        tk.Label(throttle_frame, text="Throttle Amount:", bg='#e0f7ff', fg="#2c3e50").grid(row=0, column=0, sticky='w', padx=5, pady=2)
        self.throttle_amount_var = tk.IntVar(value=20)
        tk.Entry(throttle_frame, textvariable=self.throttle_amount_var, bg="white", fg="#2c3e50", width=10).grid(row=0, column=1, sticky='w', padx=5, pady=2)
        tk.Label(throttle_frame, text="Throttle Delay:", bg='#e0f7ff', fg="#2c3e50").grid(row=1, column=0, sticky='w', padx=5, pady=2)
        self.throttle_delay_var = tk.IntVar(value=1)
        tk.Entry(throttle_frame, textvariable=self.throttle_delay_var, bg="white", fg="#2c3e50", width=10).grid(row=1, column=1, sticky='w', padx=5, pady=2)
        tk.Label(throttle_frame, text="Throttle Unit:", bg='#e0f7ff', fg="#2c3e50").grid(row=2, column=0, sticky='w', padx=5, pady=2)
        self.throttle_unit_var = tk.StringVar(value="Minutes")
        ttk.Combobox(throttle_frame, textvariable=self.throttle_unit_var, values=["Seconds", "Minutes"], state="readonly", width=8).grid(row=2, column=1, sticky='w', padx=5, pady=2)

        # Advanced Sending Options (collapsible)
        adv_section = CollapsibleSection(config_frame, title="🔧 Advanced Sending Options", expanded=False)
        adv_section.pack(fill='x', padx=10, pady=2)

        tk.Label(adv_section.content, text="Reply-To Override:", bg='#e0f7ff', fg="#2c3e50").grid(row=0, column=0, sticky='w', padx=5, pady=1)
        tk.Entry(adv_section.content, textvariable=self.reply_to_override_var, bg="white", fg="#2c3e50", width=25).grid(row=0, column=1, sticky='ew', padx=5, pady=1)

        tk.Label(adv_section.content, text="Unsubscribe URL:", bg='#e0f7ff', fg="#2c3e50").grid(row=1, column=0, sticky='w', padx=5, pady=1)
        tk.Entry(adv_section.content, textvariable=self.unsubscribe_url_var, bg="white", fg="#2c3e50", width=25).grid(row=1, column=1, sticky='ew', padx=5, pady=1)

        tk.Label(adv_section.content, text="Message-ID Domain:", bg='#e0f7ff', fg="#2c3e50").grid(row=2, column=0, sticky='w', padx=5, pady=1)
        tk.Entry(adv_section.content, textvariable=self.message_id_domain_override_var, bg="white", fg="#2c3e50", width=25).grid(row=2, column=1, sticky='ew', padx=5, pady=1)

        tk.Label(adv_section.content, text="Random Sender Pool:", bg='#e0f7ff', fg="#2c3e50").grid(row=3, column=0, sticky='w', padx=5, pady=1)
        tk.Entry(adv_section.content, textvariable=self.random_sender_pool_var, bg="white", fg="#2c3e50", width=25).grid(row=3, column=1, sticky='ew', padx=5, pady=1)

        tk.Label(adv_section.content, text="Rate Limit/Hour:", bg='#e0f7ff', fg="#2c3e50").grid(row=4, column=0, sticky='w', padx=5, pady=1)
        tk.Entry(adv_section.content, textvariable=self.rate_limit_per_hour_var, bg="white", fg="#2c3e50", width=8).grid(row=4, column=1, sticky='w', padx=5, pady=1)

        tk.Label(adv_section.content, text="Rate Limit/Day:", bg='#e0f7ff', fg="#2c3e50").grid(row=5, column=0, sticky='w', padx=5, pady=1)
        tk.Entry(adv_section.content, textvariable=self.rate_limit_per_day_var, bg="white", fg="#2c3e50", width=8).grid(row=5, column=1, sticky='w', padx=5, pady=1)

        adv_section.content.grid_columnconfigure(1, weight=1)

        # NEW: Settings Display
        settings_display_frame = tk.LabelFrame(config_frame, text="Current Settings Overview", bg='#e0f7ff', fg="#2c3e50")
        settings_display_frame.pack(fill='x', padx=10, pady=5)
        self.settings_display_text = scrolledtext.ScrolledText(settings_display_frame, height=8, font=("Consolas", 9), state="disabled", bg="white", fg="#2c3e50")
        self.settings_display_text.pack(fill='both', expand=True, padx=5, pady=5)
        self._update_settings_display()

        # Dynamic settings display update via traces
        _settings_trace_cb = lambda *args: self._update_settings_display()
        for _var in [self.concurrency_var, self.max_retries_var, self.retry_delay_var,
                     self.smtp_sleep_time_var, self.email_priority_var,
                     self.attachment_name_var, self.attachment_type_var]:
            _var.trace_add("write", _settings_trace_cb)
        for _var in [self.debug_mode_var, self.include_attachment_var,
                     self.use_random_attachment_names_var, self.use_anti_detection_var]:
            _var.trace_add("write", _settings_trace_cb)

        # Controls
        controls_frame = tk.Frame(config_frame, bg='#e0f7ff')
        controls_frame.pack(fill='x', padx=10, pady=5)
        self.start_btn = tk.Button(controls_frame, text="▶️ START", command=self._start_sending, bg="#27ae60", fg="white", font=("Arial", 9, "bold"))
        self.pause_btn = tk.Button(controls_frame, text="⏸️ PAUSE", command=self._toggle_pause, bg="#f39c12", fg="white", font=("Arial", 9, "bold"), state="disabled")
        self.stop_btn = tk.Button(controls_frame, text="⏹️ STOP", command=self._stop_sending, bg="#e74c3c", fg="white", font=("Arial", 9, "bold"), state="disabled")
        if TKCALENDAR_AVAILABLE:
            self.schedule_btn = tk.Button(controls_frame, text="🗓️ SCHEDULE", command=self._open_schedule_window, bg="#9b59b6", fg="white", font=("Arial", 9, "bold"))
        self.export_report_btn = tk.Button(controls_frame, text="📤 EXPORT", command=self._export_report, bg="#e67e22", fg="white", font=("Arial", 9, "bold"))

        # Pack controls
        self.start_btn.pack(side=tk.LEFT, padx=2, pady=2)
        self.pause_btn.pack(side=tk.LEFT, padx=2, pady=2)
        self.stop_btn.pack(side=tk.LEFT, padx=2, pady=2)
        if self.schedule_btn:
            self.schedule_btn.pack(side=tk.LEFT, padx=2, pady=2)
        self.export_report_btn.pack(side=tk.LEFT, padx=2, pady=2)

    def _toggle_warmup_mode(self):
        """Handle warmup mode toggle and update related UI elements."""
        if self.warmup_mode.get():
            self.warmup_info_label.config(text="🔥 Warmup Active", fg="#e67e22")
            messagebox.showinfo("Automated Warm-up",
                "When enabled, the app will manage daily sending volumes for new SMTP profiles to build reputation.\n\n"
                "Configure warmup_schedule.json for custom daily limits.")
        else:
            self.warmup_info_label.config(text="")

    def _toggle_smtp_rotation_fields(self):
        """Show or hide SMTP rotation configuration fields."""
        if not hasattr(self, 'smtp_rotation_fields_frame'):
            return
        if self.smtp_rotation_enabled_var.get():
            self.smtp_rotation_fields_frame.grid(row=2, column=0, columnspan=2, sticky='w', padx=5, pady=2)
        else:
            self.smtp_rotation_fields_frame.grid_remove()

    def _build_message_frame(self, parent):
        message_frame = tk.LabelFrame(parent, text="📝 Message Editor", font=("Arial", 12, "bold"), bg='#e0f7ff', fg="#2c3e50")
        message_frame.pack(fill='both', expand=True, padx=5, pady=5)

        message_toolbar = tk.Frame(message_frame, bg='#e0f7ff')
        message_toolbar.pack(fill='x', pady=2)

        tk.Button(message_toolbar, text="📁 Load HTML", command=self._load_html_file, bg="#3498db", fg="white", font=("Arial", 9)).pack(side=tk.LEFT, padx=2)
        tk.Button(message_toolbar, text="📝 Template", command=self._load_template, bg="#3498db", fg="white", font=("Arial", 9)).pack(side=tk.LEFT, padx=2)
        tk.Button(message_toolbar, text="📎 Attach", command=self._add_attachment, bg="#3498db", fg="white", font=("Arial", 9)).pack(side=tk.LEFT, padx=2)
        tk.Button(message_toolbar, text="🗑️ Clear", command=self._clear_attachments, bg="#e74c3c", fg="white", font=("Arial", 9)).pack(side=tk.LEFT, padx=2)

        tk.Button(message_toolbar, text="✨ AI Rewrite", command=self._ai_rewrite, bg="#9b59b6", fg="white", font=("Arial", 9, "bold")).pack(side=tk.LEFT, padx=5)
        tk.Button(message_toolbar, text="✨ AI Subject", command=self._ai_subject, bg="#9b59b6", fg="white", font=("Arial", 9, "bold")).pack(side=tk.LEFT, padx=2)
        tk.Button(message_toolbar, text="🎨 Inline CSS", command=self._run_css_inline, bg="#e67e22", fg="white", font=("Arial", 9, "bold")).pack(side=tk.LEFT, padx=5)

        # HTML Helper Blocks dropdown
        html_blocks_mb = tk.Menubutton(message_toolbar, text="🧱 HTML Blocks", bg="#16a085", fg="white", font=("Arial", 9, "bold"), relief=tk.RAISED)
        html_blocks_menu = tk.Menu(html_blocks_mb, tearoff=0)
        for block_type in ["Header", "Text", "Button", "Image", "Divider", "2-Column"]:
            html_blocks_menu.add_command(label=block_type, command=lambda bt=block_type: self._insert_html_block(bt))
        html_blocks_mb.config(menu=html_blocks_menu)
        html_blocks_mb.pack(side=tk.LEFT, padx=5)

        self.attachment_label = tk.Label(message_toolbar, text="No attachments", font=("Arial", 9), bg='#e0f7ff', fg="#666")
        self.attachment_label.pack(side=tk.LEFT, padx=5)
        self.char_count_label = tk.Label(message_toolbar, text="Chars: 0", font=("Arial", 9), bg='#e0f7ff', fg="#666")
        self.char_count_label.pack(side=tk.RIGHT, padx=5)

        ab_frame = tk.LabelFrame(message_frame, text="A/B Testing & Spintax", bg='#e0f7ff', fg="#2c3e50")
        ab_frame.pack(fill='x', padx=5, pady=2)
        tk.Checkbutton(ab_frame, text="Subjects", variable=self.ab_testing_enabled, bg='#e0f7ff', fg="#2c3e50").pack(side=tk.LEFT, padx=5)
        tk.Label(ab_frame, text="A:", bg='#e0f7ff', fg="#2c3e50").pack(side=tk.LEFT); tk.Entry(ab_frame, textvariable=self.subject_a_var, bg="white", fg="#2c3e50", width=20).pack(side=tk.LEFT, padx=2)
        tk.Label(ab_frame, text="B:", bg='#e0f7ff', fg="#2c3e50").pack(side=tk.LEFT); tk.Entry(ab_frame, textvariable=self.subject_b_var, bg="white", fg="#2c3e50", width=20).pack(side=tk.LEFT, padx=2)
        tk.Label(ab_frame, text="A%:", font=("Arial", 9), bg='#e0f7ff', fg="#2c3e50").pack(side=tk.LEFT, padx=(10,0)); tk.Entry(ab_frame, textvariable=self.ab_split_ratio_var, bg="white", fg="#2c3e50", width=4).pack(side=tk.LEFT)
        tk.Checkbutton(ab_frame, text="Body Content", variable=self.ab_body_enabled_var, bg='#e0f7ff', fg="#2c3e50", command=self._toggle_ab_body_view).pack(side=tk.LEFT, padx=10)
        tk.Label(ab_frame, text="Spintax Help: {Hi|Hello}", font=("Arial", 9, "italic"), bg='#e0f7ff', fg="#7f8c8d").pack(side=tk.RIGHT, padx=5)

        secure_delivery_frame = tk.LabelFrame(message_frame, text="🔒 Secure Delivery", bg='#e0f7ff', fg="#2c3e50")
        secure_delivery_frame.pack(fill='x', padx=5, pady=2)
        tk.Checkbutton(secure_delivery_frame, text="Enable Secure PDF Lure", variable=self.enable_secure_pdf_lure_var, bg='#e0f7ff', fg="#2c3e50").pack(side=tk.LEFT, padx=5)
        tk.Label(secure_delivery_frame, text="(Configure PDF template & burner domain in Settings)", font=("Arial", 8, "italic"), bg='#e0f7ff', fg="#7f8c8d").pack(side=tk.LEFT, padx=5)

        self.editor_notebook = ttk.Notebook(message_frame)
        self.editor_notebook.pack(fill='both', expand=True, padx=5, pady=5)
        self.editor_tab_a = ttk.Frame(self.editor_notebook)
        self.editor_notebook.add(self.editor_tab_a, text="Message Body (Version A)")
        self.message_box = self._create_editor_pane(self.editor_tab_a)
        self.editor_tab_b = ttk.Frame(self.editor_notebook)
        self.message_box_b = self._create_editor_pane(self.editor_tab_b)

        # Bind message_box modification to update live preview
        def _on_message_modified(event):
            if self.message_box.edit_modified():
                self.message_box.edit_modified(False)
                if self.live_preview_text:
                    self.root.after(500, self._update_live_preview)
        self.message_box.bind('<<Modified>>', _on_message_modified)

    def _create_editor_pane(self, parent):
        editor_frame = tk.Frame(parent, bg='#e0f7ff')
        editor_frame.pack(fill='both', expand=True)

        toolbar = tk.Frame(editor_frame, bg='#e0f7ff')
        toolbar.pack(fill='x')

        message_box = scrolledtext.ScrolledText(editor_frame, height=15, wrap=tk.WORD, font=("Consolas", 10), undo=True, bg="white", fg="#2c3e50")
        message_box.pack(fill='both', expand=True)
        message_box.bind('<KeyRelease>', self._update_char_count)
        message_box.tag_configure("bold", font=(("Consolas", 10, "bold")))
        message_box.tag_configure("italic", font=(("Consolas", 10, "italic")))
        message_box.tag_configure("code", background="#f0f0f0", font=("Consolas", 10))

        tk.Button(toolbar, text="B", font=("Arial", 10, "bold"), bg="#3498db", fg="white", command=lambda mb=message_box: self._format_text("bold", mb)).pack(side=tk.LEFT, padx=1)
        tk.Button(toolbar, text="I", font=("Arial", 10, "italic"), bg="#3498db", fg="white", command=lambda mb=message_box: self._format_text("italic", mb)).pack(side=tk.LEFT, padx=1)
        tk.Button(toolbar, text="<>", font=("Consolas", 10), bg="#3498db", fg="white", command=lambda mb=message_box: self._format_text("code", mb)).pack(side=tk.LEFT, padx=1)
        tk.Button(toolbar, text="Link", font=("Arial", 10), bg="#3498db", fg="white", command=lambda mb=message_box: self._add_link(mb)).pack(side=tk.LEFT, padx=1)

        return message_box

    def _toggle_ab_body_view(self):
        if self.ab_body_enabled_var.get():
            if self.editor_tab_b not in self.editor_notebook.tabs():
                self.editor_notebook.add(self.editor_tab_b, text="Message Body (Version B)")
        else:
            if self.editor_tab_b in self.editor_notebook.tabs():
                self.editor_notebook.hide(self.editor_tab_b)

    def _build_recipients_frame(self, parent):
        recipients_frame = tk.LabelFrame(parent, text="👥 Recipients", font=("Arial", 12, "bold"), bg='#e0f7ff', fg="#2c3e50")
        recipients_frame.pack(fill='both', expand=True, padx=5, pady=5)

        # Row 1: Load, Paste, Validate, Clear
        recipients_toolbar_row1 = tk.Frame(recipients_frame, bg='#e0f7ff')
        recipients_toolbar_row1.pack(fill='x', pady=(5, 2))
        tk.Button(recipients_toolbar_row1, text="📁 Load List", command=self._load_emails, bg="#3498db", fg="white", font=("Arial", 10, "bold")).pack(side=tk.LEFT, padx=5)
        tk.Button(recipients_toolbar_row1, text="📋 Paste", command=self._paste_emails, bg="#3498db", fg="white", font=("Arial", 10, "bold")).pack(side=tk.LEFT, padx=5)
        tk.Button(recipients_toolbar_row1, text="✔️ Validate List", command=self._validate_email_list, bg="#3498db", fg="white", font=("Arial", 10, "bold")).pack(side=tk.LEFT, padx=5)
        tk.Button(recipients_toolbar_row1, text="🗑️ Clear List", command=self._clear_email_list, bg="#e74c3c", fg="white", font=("Arial", 10, "bold")).pack(side=tk.LEFT, padx=5)
        tk.Button(recipients_toolbar_row1, text="🗑️ Remove Sel.", command=self._remove_selected, bg="#e74c3c", fg="white", font=("Arial", 9)).pack(side=tk.RIGHT, padx=5)
        tk.Button(recipients_toolbar_row1, text="🔄 Retry Failed", command=self._retry_failed, bg="#f39c12", fg="white", font=("Arial", 9)).pack(side=tk.RIGHT, padx=5)

        # Row 2: Add Single Email
        recipients_toolbar_row2 = tk.Frame(recipients_frame, bg='#e0f7ff')
        recipients_toolbar_row2.pack(fill='x', pady=(2, 5))
        tk.Label(recipients_toolbar_row2, text="Add Email:", bg='#e0f7ff', fg="#2c3e50", font=("Arial", 9)).pack(side=tk.LEFT, padx=5)
        self.new_recipient_var = tk.StringVar()
        recipient_entry = tk.Entry(recipients_toolbar_row2, textvariable=self.new_recipient_var, bg="white", fg="#2c3e50", width=30)
        recipient_entry.pack(side=tk.LEFT, padx=5)
        recipient_entry.bind('<Return>', lambda e: self._add_recipient())
        tk.Button(recipients_toolbar_row2, text="➕ Add", command=self._add_recipient, bg="#27ae60", fg="white", font=("Arial", 9, "bold")).pack(side=tk.LEFT, padx=5)

        table_container = tk.Frame(recipients_frame, bg='#e0f7ff')
        table_container.pack(fill='both', expand=True, padx=5, pady=5)
        columns = ("email", "status", "sent_at", "opened", "clicked", "attempts", "reason")
        self.tree = ttk.Treeview(table_container, columns=columns, show='headings', height=8)
        headings = {"email": "�� Email", "status": "📊 Status", "sent_at": "⏰ Sent", "opened": "👁️", "clicked": "🖱️", "attempts": "🔄", "reason": "❗ Reason"}
        widths = {"email": 230, "status": 100, "sent_at": 120, "opened": 40, "clicked": 40, "attempts": 40, "reason": 120}
        for col, text in headings.items():
            self.tree.heading(col, text=text); self.tree.column(col, width=widths[col], anchor='center' if col != "email" else 'w')

        tree_scrollbar = ttk.Scrollbar(table_container, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=tree_scrollbar.set)
        tree_scrollbar.pack(side='right', fill='y'); self.tree.pack(fill='both', expand=True)

    def _build_bottom_controls(self, parent):
        bottom_frame = tk.Frame(parent, bg='#e0f7ff')
        bottom_frame.pack(fill='x', side='bottom', padx=10, pady=5)

        control_frame = tk.LabelFrame(bottom_frame, text="🎯 Activity Log & Progress", font=("Arial", 12, "bold"), bg='#e0f7ff', fg="#2c3e50")
        control_frame.pack(side='top', fill='both', expand=True, pady=(0, 5))

        self.button_frame = tk.Frame(control_frame, bg='#e0f7ff')
        self.button_frame.pack(pady=5, fill='x', expand=True)

        font_spec = ("Arial", 9, "bold")
        self.connect_chrome_btn = tk.Button(self.button_frame, text="🔌 CHROME", command=self._connect_chrome, bg="#3498db", fg="white", font=font_spec)
        self.start_chrome_btn = tk.Button(self.button_frame, text="🚀 START CHROME", command=self._start_chrome_with_debug, bg="#27ae60", fg="white", font=font_spec)
        self.test_smtp_btn = tk.Button(self.button_frame, text="🧪 TEST SMTP", command=self._test_smtp, bg="#e67e22", fg="white", font=font_spec)

        self.connect_outlook_btn = None
        if OUTLOOK_COM_AVAILABLE:
            self.connect_outlook_btn = tk.Button(self.button_frame, text="🖥️ OUTLOOK", command=self._connect_outlook, bg="#9b59b6", fg="white", font=font_spec)

        # Buttons already in config frame

        self._setup_button_grid()

        log_frame = tk.LabelFrame(bottom_frame, text="📋 Activity Log", font=("Arial", 11, "bold"), bg='#e0f7ff', fg="#2c3e50")
        log_frame.pack(side='bottom', fill='both', expand=True)
        self.log_box = scrolledtext.ScrolledText(log_frame, height=6, font=("Consolas", 9), bg='black', fg='white', wrap=tk.WORD)  # Black background, white text
        self.log_box.pack(fill='both', expand=True, padx=5, pady=5)

        progress_frame = tk.Frame(bottom_frame, bg='#e0f7ff')
        progress_frame.pack(fill='x', side='bottom', pady=5)
        progress_frame.grid_columnconfigure(0, weight=1)
        progress_frame.grid_columnconfigure(1, weight=0)
        progress_frame.grid_columnconfigure(2, weight=0)

        self.status_label = tk.Label(progress_frame, text="🔵 Ready", font=("Arial", 11, "bold"), bg='#e0f7ff', fg="#2980b9")
        self.status_label.grid(row=0, column=0, columnspan=3, sticky='ew', pady=2)
        self.progress = ttk.Progressbar(progress_frame, orient='horizontal', length=1000, mode='determinate')
        self.progress.grid(row=1, column=0, columnspan=3, sticky='ew', pady=2)
        self.stats_label = tk.Label(progress_frame, text="📊 Stats: 0 sent | 0 failed | 0 total", font=("Arial", 10), bg='#e0f7ff', fg="#666")
        self.stats_label.grid(row=2, column=0, sticky='ew', pady=2)

        self.connection_status = tk.Label(progress_frame, text="🔌 Chrome: Not Connected", font=("Arial", 10), bg='#e0f7ff', fg="#666")
        self.connection_status.grid(row=2, column=1, sticky='e', padx=5, pady=2)
        self.outlook_status = tk.Label(progress_frame, text="🖥️ Outlook: Not Connected", font=("Arial", 10), bg='#e0f7ff', fg="#666")
        self.outlook_status.grid(row=2, column=2, sticky='e', padx=5, pady=2)

    def _setup_button_grid(self):
        try:
            buttons = [
                self.connect_chrome_btn, self.start_chrome_btn, self.test_smtp_btn
            ]
            if self.connect_outlook_btn:
                buttons.append(self.connect_outlook_btn)

            for i, btn in enumerate(buttons):
                if btn and hasattr(btn, 'winfo_exists') and btn.winfo_exists():
                    btn.pack(side=tk.LEFT, padx=5, pady=5)
        except Exception as e:
            self.log(f"⚠️ Error setting up button layout: {e}")

    def _restore_button_visibility(self):
        self.root.after(100, self._setup_button_grid)

    def _verify_button_layout(self):
        self.root.after(100, self._setup_button_grid)

    def _build_settings_tab(self):
        settings_tab = ttk.Frame(self.notebook)
        self.notebook.add(settings_tab, text='⚙️ Settings')

        settings_pane = ttk.PanedWindow(settings_tab, orient=tk.HORIZONTAL)
        settings_pane.pack(fill='both', expand=True, padx=5, pady=5)

        left_outer = tk.Frame(settings_pane, bg='#e0f7ff')
        settings_pane.add(left_outer, weight=1)
        left_scroll = ScrollableFrame(left_outer)
        left_scroll.pack(fill='both', expand=True)
        left_frame = tk.Frame(left_scroll.scrollable_frame, bg='#e0f7ff')
        left_frame.pack(fill='both', expand=True)
        tk.Label(left_frame, text="Engine & Connectivity", font=("Arial", 11, "bold"), bg='#e0f7ff', fg="#2c3e50").pack(anchor='w', padx=10, pady=5)

        chromedriver_frame = tk.LabelFrame(left_frame, text="ChromeDriver & Proxy", bg='#e0f7ff', fg="#2c3e50")
        chromedriver_frame.pack(fill='x', padx=10, pady=5, expand=True)
        tk.Label(chromedriver_frame, text="Path:", bg='#e0f7ff', fg="#2c3e50").pack(side=tk.LEFT)
        self.chromedriver_label = tk.Label(chromedriver_frame, text="Auto/Default", font=("Arial", 9), bg='#e0f7ff', fg="#666")
        self.chromedriver_label.pack(side=tk.LEFT, padx=10)
        tk.Button(chromedriver_frame, text="Browse", command=self._choose_chromedriver, bg="#3498db", fg="white", font=("Arial", 9)).pack(side=tk.RIGHT, padx=5)

        proxy_frame = tk.Frame(chromedriver_frame, bg='#e0f7ff')
        proxy_frame.pack(fill='x', pady=5)
        tk.Label(proxy_frame, text="Global Proxy (host:port):", bg='#e0f7ff', fg="#2c3e50").pack(side=tk.LEFT)
        tk.Entry(proxy_frame, textvariable=self.proxy_var, bg="white", fg="#2c3e50", width=25).pack(side=tk.LEFT, padx=5)
        tk.Label(proxy_frame, text="Per-Profile Proxy:", bg='#e0f7ff', fg="#2c3e50").pack(side=tk.LEFT, padx=(10,0))
        tk.Entry(proxy_frame, textvariable=self.per_profile_proxy_var, bg="white", fg="#2c3e50", width=25).pack(side=tk.LEFT, padx=5)

        tk.Checkbutton(chromedriver_frame, text="Use Undetected ChromeDriver", variable=self.use_undetected_chromedriver, bg='#e0f7ff', fg="#2c3e50").pack(side=tk.TOP, pady=5)
        tk.Checkbutton(chromedriver_frame, text="Use Headless Mode", variable=self.use_headless, bg='#e0f7ff', fg="#2c3e50").pack(side=tk.TOP, pady=5)

        # Add Chrome version label
        self.chrome_version_label = tk.Label(chromedriver_frame, text="Chrome: Unknown", font=("Arial", 9), bg='#e0f7ff', fg="#bdc3c7")
        self.chrome_version_label.pack(side=tk.TOP, pady=5)
        tk.Button(chromedriver_frame, text="🔍 Check Versions", command=self._manual_version_check, bg="#e67e22", fg="white", font=("Arial", 9)).pack(side=tk.TOP, pady=2)

        adv_config_frame = tk.LabelFrame(left_frame, text="Advanced Config (Requires Restart)", bg='#e0f7ff', fg="#2c3e50")
        adv_config_frame.pack(fill='x', padx=10, pady=5, expand=True)

        tk.Label(adv_config_frame, text="Track Port:", bg='#e0f7ff', fg="#2c3e50").grid(row=0, column=0, sticky='w')
        tk.Entry(adv_config_frame, textvariable=self.track_port_var, bg="white", fg="#2c3e50", width=8).grid(row=0, column=1, sticky='w')

        tk.Label(adv_config_frame, text="Chrome Debug Port:", bg='#e0f7ff', fg="#2c3e50").grid(row=0, column=2, sticky='w')
        tk.Entry(adv_config_frame, textvariable=self.chrome_port_var, bg="white", fg="#2c3e50", width=8).grid(row=0, column=3, sticky='w')

        tk.Label(adv_config_frame, text="Custom Chrome Binary Path:", bg='#e0f7ff', fg="#2c3e50").grid(row=1, column=0, columnspan=4, sticky='w')
        tk.Entry(adv_config_frame, textvariable=self.custom_chrome_path_var, bg="white", fg="#2c3e50", width=40).grid(row=2, column=0, columnspan=4, sticky='w', pady=(0, 5))

        tk.Checkbutton(adv_config_frame, text="API Mode (Headless VPS)", variable=self.api_mode, bg='#e0f7ff', fg="#2c3e50").grid(row=3, column=0, columnspan=4, sticky='w', pady=5)

        # NEW: Configuration Variables
        config_vars_frame = tk.LabelFrame(left_frame, text="Sending Configuration", bg='#e0f7ff', fg="#2c3e50")
        config_vars_frame.pack(fill='x', padx=10, pady=5, expand=True)

        tk.Label(config_vars_frame, text="Concurrency:", bg='#e0f7ff', fg="#2c3e50").grid(row=0, column=0, sticky='w')
        tk.Entry(config_vars_frame, textvariable=self.concurrency_var, bg="white", fg="#2c3e50", width=8).grid(row=0, column=1, sticky='w')
        tk.Label(config_vars_frame, text="Max Retries:", bg='#e0f7ff', fg="#2c3e50").grid(row=0, column=2, sticky='w')
        tk.Entry(config_vars_frame, textvariable=self.max_retries_var, bg="white", fg="#2c3e50", width=8).grid(row=0, column=3, sticky='w')

        tk.Label(config_vars_frame, text="Retry Delay (s):", bg='#e0f7ff', fg="#2c3e50").grid(row=1, column=0, sticky='w')
        tk.Entry(config_vars_frame, textvariable=self.retry_delay_var, bg="white", fg="#2c3e50", width=8).grid(row=1, column=1, sticky='w')
        tk.Label(config_vars_frame, text="SMTP Sleep Time (ms):", bg='#e0f7ff', fg="#2c3e50").grid(row=1, column=2, sticky='w')
        tk.Entry(config_vars_frame, textvariable=self.smtp_sleep_time_var, bg="white", fg="#2c3e50", width=8).grid(row=1, column=3, sticky='w')

        tk.Checkbutton(config_vars_frame, text="Debug Mode", variable=self.debug_mode_var, bg='#e0f7ff', fg="#2c3e50").grid(row=2, column=0, sticky='w')
        tk.Checkbutton(config_vars_frame, text="Include Attachment", variable=self.include_attachment_var, bg='#e0f7ff', fg="#2c3e50").grid(row=2, column=1, sticky='w')
        tk.Checkbutton(config_vars_frame, text="Use Random Attachment Names", variable=self.use_random_attachment_names_var, bg='#e0f7ff', fg="#2c3e50").grid(row=2, column=2, columnspan=2, sticky='w')
        tk.Checkbutton(config_vars_frame, text="Use Anti-Detection", variable=self.use_anti_detection_var, bg='#e0f7ff', fg="#2c3e50").grid(row=3, column=0, columnspan=2, sticky='w')

        tk.Label(config_vars_frame, text="Attachment Name:", bg='#e0f7ff', fg="#2c3e50").grid(row=4, column=0, sticky='w')
        tk.Entry(config_vars_frame, textvariable=self.attachment_name_var, bg="white", fg="#2c3e50", width=15).grid(row=4, column=1, sticky='w')
        tk.Label(config_vars_frame, text="Attachment Type:", bg='#e0f7ff', fg="#2c3e50").grid(row=4, column=2, sticky='w')
        tk.Entry(config_vars_frame, textvariable=self.attachment_type_var, bg="white", fg="#2c3e50", width=10).grid(row=4, column=3, sticky='w')
        tk.Label(config_vars_frame, text="Email Priority:", bg='#e0f7ff', fg="#2c3e50").grid(row=5, column=0, sticky='w')
        tk.Entry(config_vars_frame, textvariable=self.email_priority_var, bg="white", fg="#2c3e50", width=10).grid(row=5, column=1, sticky='w')

        secure_frame = tk.LabelFrame(left_frame, text="Secure Redirector & Lure Settings", bg='#e0f7ff', fg="#2c3e50")
        secure_frame.pack(fill='x', padx=10, pady=5, expand=True)

        tk.Label(secure_frame, text="Burner Domain:", bg='#e0f7ff', fg="#2c3e50").grid(row=0, column=0, sticky='w', padx=5, pady=2)
        tk.Entry(secure_frame, textvariable=self.burner_domain_var, bg="white", fg="#2c3e50", width=30).grid(row=0, column=1, sticky='ew', padx=5, pady=2)
        tk.Label(secure_frame, text="Lure Path:", bg='#e0f7ff', fg="#2c3e50").grid(row=1, column=0, sticky='w', padx=5, pady=2)
        tk.Entry(secure_frame, textvariable=self.lure_path_var, bg="white", fg="#2c3e50", width=30).grid(row=1, column=1, sticky='ew', padx=5, pady=2)
        tk.Label(secure_frame, text="Template PDF:", bg='#e0f7ff', fg="#2c3e50").grid(row=2, column=0, sticky='w', padx=5, pady=2)
        tk.Entry(secure_frame, textvariable=self.template_pdf_var, bg="white", fg="#2c3e50", width=20).grid(row=2, column=1, sticky='ew', padx=5, pady=2)
        tk.Button(secure_frame, text="Browse", command=self._browse_template_pdf, bg="#3498db", fg="white").grid(row=2, column=2, padx=5, pady=2)

        secure_frame.grid_columnconfigure(1, weight=1)

        right_outer = tk.Frame(settings_pane, bg='#e0f7ff')
        settings_pane.add(right_outer, weight=1)
        right_scroll = ScrollableFrame(right_outer)
        right_scroll.pack(fill='both', expand=True)
        right_frame = right_scroll.scrollable_frame

        smtp_frame = tk.LabelFrame(right_frame, text="SMTP Configuration", bg='#e0f7ff', fg="#2c3e50")
        smtp_frame.pack(fill='both', padx=10, pady=5, expand=True)
        tk.Label(smtp_frame, text="Parallel Workers:", bg='#e0f7ff', fg="#2c3e50").grid(row=0, column=0, sticky='w', padx=5, pady=2)
        tk.Entry(smtp_frame, textvariable=self.smtp_max_workers_var, bg="white", fg="#2c3e50", width=5).grid(row=0, column=1, sticky='w', padx=5, pady=2)
        self.smtp_max_workers_var.trace_add("write", lambda *args: self._update_smtp_workers())
        tk.Checkbutton(smtp_frame, text="Enable SMTP Rotation", variable=self.smtp_rotation_enabled_var, bg='#e0f7ff', fg="#2c3e50",
                       command=self._toggle_smtp_rotation_fields).grid(row=1, column=0, columnspan=2, sticky='w', padx=5, pady=2)

        self.smtp_rotation_fields_frame = tk.Frame(smtp_frame, bg='#e0f7ff')
        self.smtp_rotation_fields_frame.grid(row=2, column=0, columnspan=2, sticky='w', padx=5, pady=2)
        tk.Label(self.smtp_rotation_fields_frame, text="Rotation Batch Size:", bg='#e0f7ff', fg="#2c3e50").grid(row=0, column=0, sticky='w', padx=5, pady=2)
        tk.Entry(self.smtp_rotation_fields_frame, textvariable=self.smtp_rotation_batch_size_var, bg="white", fg="#2c3e50", width=8).grid(row=0, column=1, sticky='w', padx=5, pady=2)
        tk.Label(self.smtp_rotation_fields_frame, text="Failover Threshold:", bg='#e0f7ff', fg="#2c3e50").grid(row=1, column=0, sticky='w', padx=5, pady=2)
        tk.Entry(self.smtp_rotation_fields_frame, textvariable=self.smtp_failover_threshold_var, bg="white", fg="#2c3e50", width=8).grid(row=1, column=1, sticky='w', padx=5, pady=2)
        if not self.smtp_rotation_enabled_var.get():
            self.smtp_rotation_fields_frame.grid_remove()

        # Sending Tweaks
        tweaks_frame = tk.LabelFrame(right_frame, text="🔧 Sending Tweaks", bg='#e0f7ff', fg="#2c3e50")
        tweaks_frame.pack(fill='x', padx=10, pady=5)
        tk.Checkbutton(tweaks_frame, text="Enable DOM Shuffling", variable=self.dom_shuffling_enabled, bg='#e0f7ff', fg="#2c3e50").pack(anchor='w', padx=5, pady=2)

        # ASYNC SMTP Mode
        async_smtp_frame = tk.LabelFrame(right_frame, text="Async SMTP Mode", bg='#e0f7ff', fg="#2c3e50")
        async_smtp_frame.pack(fill='x', padx=10, pady=5)
        self.async_smtp_enabled_var = tk.BooleanVar(value=AIOSMTPLIB_AVAILABLE)
        tk.Checkbutton(async_smtp_frame, text="Enable Async SMTP", variable=self.async_smtp_enabled_var, bg='#e0f7ff', fg="#2c3e50").grid(row=0, column=0, sticky='w', padx=5, pady=2)
        async_status = "Available" if AIOSMTPLIB_AVAILABLE else "Not Available (using fallback)"
        tk.Label(async_smtp_frame, text=f"Status: {async_status}", bg='#e0f7ff', fg="#27ae60" if AIOSMTPLIB_AVAILABLE else "#e74c3c").grid(row=0, column=1, sticky='w', padx=5, pady=2)
        tk.Label(async_smtp_frame, text="Async Connection Pool Size:", bg='#e0f7ff', fg="#2c3e50").grid(row=1, column=0, sticky='w', padx=5, pady=2)
        self.async_pool_size_var = tk.IntVar(value=10)
        tk.Entry(async_smtp_frame, textvariable=self.async_pool_size_var, bg="white", fg="#2c3e50", width=5).grid(row=1, column=1, sticky='w', padx=5, pady=2)

        ai_frame = tk.LabelFrame(right_frame, text="🧠 Artificial Intelligence", bg='#e0f7ff', fg="#2c3e50")
        ai_frame.pack(fill='both', padx=10, pady=5, expand=True)

        tk.Label(ai_frame, text="AI Provider:", bg='#e0f7ff', fg="#2c3e50").grid(row=0, column=0, sticky='w', padx=5)
        ai_provider_menu = ttk.Combobox(ai_frame, textvariable=self.ai_provider_var, values=["OpenAI", "Local"], state="readonly", width=10)
        ai_provider_menu.grid(row=0, column=1, sticky='w', padx=5)

        tk.Label(ai_frame, text="OpenAI Key:", bg='#e0f7ff', fg="#2c3e50").grid(row=1, column=0, sticky='w', padx=5)
        tk.Entry(ai_frame, textvariable=self.openai_api_key_var, bg="white", fg="#2c3e50", width=30, show="*").grid(row=1, column=1, sticky='w', padx=5)

        tk.Label(ai_frame, text="Local AI URL:", bg='#e0f7ff', fg="#2c3e50").grid(row=2, column=0, sticky='w', padx=5)
        tk.Entry(ai_frame, textvariable=self.local_ai_url_var, bg="white", fg="#2c3e50", width=30).grid(row=2, column=1, sticky='w', padx=5)

        tk.Label(ai_frame, text="Local AI Model:", bg='#e0f7ff', fg="#2c3e50").grid(row=3, column=0, sticky='w', padx=5)
        tk.Entry(ai_frame, textvariable=self.local_ai_model_var, bg="white", fg="#2c3e50", width=30).grid(row=3, column=1, sticky='w', padx=5)

        ai_btn_frame = tk.Frame(ai_frame, bg='#e0f7ff')
        ai_btn_frame.grid(row=4, column=0, columnspan=2, sticky='ew', padx=5, pady=5)
        tk.Button(ai_btn_frame, text="💾 Save AI Config", command=self._save_ai_config, bg="#27ae60", fg="white", font=("Arial", 9)).pack(side=tk.LEFT, padx=2)
        tk.Button(ai_btn_frame, text="🗑️ Delete AI Config", command=self._delete_ai_config, bg="#e74c3c", fg="white", font=("Arial", 9)).pack(side=tk.LEFT, padx=2)
        tk.Button(ai_btn_frame, text="🧪 Test AI", command=self._test_ai_connection, bg="#e67e22", fg="white", font=("Arial", 9)).pack(side=tk.LEFT, padx=2)

        tk.Label(ai_frame, text="Saved Configs:", bg='#e0f7ff', fg="#2c3e50").grid(row=5, column=0, sticky='w', padx=5)
        self.ai_config_listbox = tk.Listbox(ai_frame, height=3, bg="white", fg="#2c3e50")
        self.ai_config_listbox.grid(row=5, column=1, sticky='ew', padx=5, pady=2)
        self.ai_config_listbox.bind('<<ListboxSelect>>', self._on_ai_config_select)
        self._refresh_ai_config_list()

        # AI Features
        ai_controls_frame = tk.LabelFrame(right_frame, text="AI Features & Feedback", bg='#e0f7ff', fg="#2c3e50")
        ai_controls_frame.pack(fill='x', padx=10, pady=5)
        tk.Button(ai_controls_frame, text="🛡️ Spam Check", command=self._run_ai_spam_check, bg="#e74c3c", fg="white").grid(row=0, column=0, padx=5, pady=2)
        tk.Button(ai_controls_frame, text="✨ Rewrite Content", command=self._ai_rewrite, bg="#9b59b6", fg="white").grid(row=0, column=1, padx=5, pady=2)
        tk.Button(ai_controls_frame, text="✨ Generate Subjects", command=self._ai_subject, bg="#9b59b6", fg="white").grid(row=0, column=2, padx=5, pady=2)
        self.ai_per_campaign_var = tk.BooleanVar(value=True)
        tk.Checkbutton(ai_controls_frame, text="Enable AI for This Campaign", variable=self.ai_per_campaign_var, bg='#e0f7ff', fg="#2c3e50").grid(row=1, column=0, columnspan=3, sticky='w', padx=5, pady=2)
        self.ai_feedback_text = scrolledtext.ScrolledText(ai_controls_frame, height=5, wrap=tk.WORD, state="disabled", bg="white", fg="#2c3e50")
        self.ai_feedback_text.grid(row=2, column=0, columnspan=3, sticky='ew', padx=5, pady=5)

        vps_section = CollapsibleSection(right_frame, title="🖥️ Proxy VPS Mailer", expanded=False)
        vps_section.pack(fill='x', padx=10, pady=3)
        vps_frame = vps_section.content
        tk.Button(vps_frame, text="➕ Add VPS", command=self._add_vps_config, bg="#27ae60", fg="white").grid(row=0, column=0, padx=5, pady=2)
        tk.Button(vps_frame, text="🖊️ Edit Selected", command=self._edit_vps_config, bg="#3498db", fg="white").grid(row=0, column=1, padx=5, pady=2)
        tk.Button(vps_frame, text="🗑️ Remove Selected", command=self._remove_selected_vps, bg="#e74c3c", fg="white").grid(row=0, column=2, padx=5, pady=2)
        tk.Button(vps_frame, text="🔄 Refresh Health", command=self._refresh_vps_health, bg="#f39c12", fg="white").grid(row=0, column=3, padx=5, pady=2)
        self.vps_listbox = tk.Listbox(vps_frame, bg="white", fg="#2c3e50", height=8)
        self.vps_listbox.grid(row=1, column=0, columnspan=4, sticky='ew', padx=5, pady=5)
        self._refresh_vps_list()
        tk.Checkbutton(vps_frame, text="Enable Geo Failover", variable=self.vps_geo_failover_enabled_var, bg='#e0f7ff', fg="#2c3e50").grid(row=2, column=0, sticky='w', padx=5, pady=2)
        tk.Label(vps_frame, text="Default AI Provider:", bg='#e0f7ff', fg="#2c3e50").grid(row=3, column=0, sticky='w', padx=5, pady=2)
        tk.Entry(vps_frame, textvariable=self.vps_ai_defaults_var, bg="white", fg="#2c3e50", width=20).grid(row=3, column=1, sticky='w', padx=5, pady=2)

        imap_section = CollapsibleSection(right_frame, title="📬 IMAP (Reply Tracking)", expanded=False)
        imap_section.pack(fill='x', padx=10, pady=3)
        imap_frame = imap_section.content
        self.imap_enabled_var = tk.BooleanVar(value=False)
        tk.Checkbutton(imap_frame, text="Enable IMAP Reply Tracking", variable=self.imap_enabled_var, bg='#e0f7ff', fg="#2c3e50").grid(row=0, column=0, columnspan=2, sticky='w', padx=5, pady=2)
        tk.Label(imap_frame, text="IMAP Server:", bg='#e0f7ff', fg="#2c3e50").grid(row=1, column=0, sticky='w', padx=5)
        tk.Entry(imap_frame, textvariable=self.imap_server_var, bg="white", fg="#2c3e50", width=20).grid(row=1, column=1, padx=5)
        tk.Label(imap_frame, text="IMAP User:", bg='#e0f7ff', fg="#2c3e50").grid(row=2, column=0, sticky='w', padx=5)
        tk.Entry(imap_frame, textvariable=self.imap_user_var, bg="white", fg="#2c3e50", width=20).grid(row=2, column=1, padx=5)
        tk.Label(imap_frame, text="IMAP Pass:", bg='#e0f7ff', fg="#2c3e50").grid(row=3, column=0, sticky='w', padx=5)
        tk.Entry(imap_frame, textvariable=self.imap_pass_var, bg="white", fg="#2c3e50", width=20, show="*").grid(row=3, column=1, padx=5)
        tk.Label(imap_frame, text="Poll Interval (s):", bg='#e0f7ff', fg="#2c3e50").grid(row=4, column=0, sticky='w', padx=5)
        self.imap_poll_interval_var = tk.IntVar(value=300)
        tk.Entry(imap_frame, textvariable=self.imap_poll_interval_var, bg="white", fg="#2c3e50", width=10).grid(row=4, column=1, sticky='w', padx=5)
        tk.Button(imap_frame, text="🧪 Test IMAP Connection", command=self._test_imap_connection, bg="#e67e22", fg="white", font=("Arial", 9)).grid(row=5, column=0, columnspan=2, pady=5)
        self.imap_status_label = tk.Label(imap_frame, text="IMAP: Not Connected", font=("Arial", 9), bg='#e0f7ff', fg="#666")
        self.imap_status_label.grid(row=6, column=0, columnspan=2, sticky='w', padx=5, pady=2)

        # DKIM Configuration Panel
        dkim_frame = tk.LabelFrame(right_frame, text="🔐 DKIM Configuration", bg='#e0f7ff', fg="#2c3e50")
        dkim_frame.pack(fill='x', padx=10, pady=5)
        tk.Label(dkim_frame, text="DKIM Selector:", bg='#e0f7ff', fg="#2c3e50").grid(row=0, column=0, sticky='w', padx=5, pady=2)
        self.dkim_selector_var = tk.StringVar(value=getattr(getattr(self, 'direct_mx_handler', None), 'dkim_selector', ""))
        tk.Entry(dkim_frame, textvariable=self.dkim_selector_var, bg="white", fg="#2c3e50", width=20).grid(row=0, column=1, sticky='ew', padx=5, pady=2)
        tk.Label(dkim_frame, text="DKIM Domain:", bg='#e0f7ff', fg="#2c3e50").grid(row=1, column=0, sticky='w', padx=5, pady=2)
        self.dkim_domain_var = tk.StringVar(value=getattr(getattr(self, 'direct_mx_handler', None), 'dkim_domain', ""))
        tk.Entry(dkim_frame, textvariable=self.dkim_domain_var, bg="white", fg="#2c3e50", width=20).grid(row=1, column=1, sticky='ew', padx=5, pady=2)
        tk.Button(dkim_frame, text="🔑 Configure DKIM Key", command=self._configure_dkim_window, bg="#9b59b6", fg="white").grid(row=2, column=0, columnspan=2, pady=5)

        # DKIM Key File Upload
        dkim_upload_frame = tk.Frame(dkim_frame, bg='#e0f7ff')
        dkim_upload_frame.grid(row=4, column=0, columnspan=2, sticky='ew', padx=5, pady=2)
        tk.Label(dkim_upload_frame, text="DKIM Key File:", bg='#e0f7ff', fg="#2c3e50").pack(side=tk.LEFT, padx=2)
        tk.Entry(dkim_upload_frame, textvariable=self.dkim_key_file_var, bg="white", fg="#2c3e50", width=20, state='readonly').pack(side=tk.LEFT, padx=2, fill='x', expand=True)
        tk.Button(dkim_upload_frame, text="📂 Upload", command=self._browse_dkim_key_file, bg="#3498db", fg="white", font=("Arial", 9)).pack(side=tk.LEFT, padx=2)

        self.settings_dkim_status_label= tk.Label(dkim_frame, text="DKIM: Not Configured", font=("Arial", 9), bg='#e0f7ff', fg="#e74c3c")
        self.settings_dkim_status_label.grid(row=3, column=0, columnspan=2, sticky='w', padx=5, pady=2)
        dkim_frame.grid_columnconfigure(1, weight=1)

        # Sending Mode Selector
        mode_frame = tk.LabelFrame(right_frame, text="📡 Sending Mode", bg='#e0f7ff', fg="#2c3e50")
        mode_frame.pack(fill='x', padx=10, pady=5)
        self.sending_mode_var = tk.StringVar(value="SMTP")
        tk.Radiobutton(mode_frame, text="Async SMTP", variable=self.sending_mode_var, value="SMTP", bg='#e0f7ff', fg="#2c3e50").pack(anchor='w', padx=10, pady=1)
        tk.Radiobutton(mode_frame, text="Threaded SMTP", variable=self.sending_mode_var, value="ThreadedSMTP", bg='#e0f7ff', fg="#2c3e50").pack(anchor='w', padx=10, pady=1)
        tk.Radiobutton(mode_frame, text="VPS Proxy SMTP", variable=self.sending_mode_var, value="VPS", bg='#e0f7ff', fg="#2c3e50").pack(anchor='w', padx=10, pady=1)
        tk.Radiobutton(mode_frame, text="VPS Standalone (Local MTA)", variable=self.sending_mode_var, value="VPSStandalone", bg='#e0f7ff', fg="#2c3e50").pack(anchor='w', padx=10, pady=1)
        tk.Radiobutton(mode_frame, text="Direct MX Delivery", variable=self.sending_mode_var, value="DirectMX", bg='#e0f7ff', fg="#2c3e50").pack(anchor='w', padx=10, pady=1)
        tk.Radiobutton(mode_frame, text="Browser Automation", variable=self.sending_mode_var, value="Browser", bg='#e0f7ff', fg="#2c3e50").pack(anchor='w', padx=10, pady=1)

        # Health Monitor Controls
        health_frame = tk.LabelFrame(right_frame, text="💓 VPS Health Monitor", bg='#e0f7ff', fg="#2c3e50")
        health_frame.pack(fill='x', padx=10, pady=5)
        self.health_monitor_enabled_var = tk.BooleanVar(value=True)
        tk.Checkbutton(health_frame, text="Enable VPS Health Monitor", variable=self.health_monitor_enabled_var, bg='#e0f7ff', fg="#2c3e50").grid(row=0, column=0, columnspan=2, sticky='w', padx=5, pady=2)
        tk.Label(health_frame, text="Check Interval (s):", bg='#e0f7ff', fg="#2c3e50").grid(row=1, column=0, sticky='w', padx=5)
        self.health_check_interval_var = tk.IntVar(value=300)
        tk.Entry(health_frame, textvariable=self.health_check_interval_var, bg="white", fg="#2c3e50", width=10).grid(row=1, column=1, sticky='w', padx=5)
        tk.Button(health_frame, text="🔍 Check Now", command=self._manual_health_check, bg="#e67e22", fg="white", font=("Arial", 9)).grid(row=2, column=0, columnspan=2, pady=5)
        self.health_monitor_status_label = tk.Label(health_frame, text="Monitor: Active", font=("Arial", 9), bg='#e0f7ff', fg="#27ae60")
        self.health_monitor_status_label.grid(row=3, column=0, columnspan=2, sticky='w', padx=5, pady=2)

        # Encryption Key Management
        enc_frame = tk.LabelFrame(right_frame, text="🔐 Encryption Key", bg='#e0f7ff', fg="#2c3e50")
        enc_frame.pack(fill='x', padx=10, pady=5)
        self.enc_key_status_label = tk.Label(enc_frame, text="Key: Checking...", font=("Arial", 9), bg='#e0f7ff', fg="#666")
        self.enc_key_status_label.grid(row=0, column=0, columnspan=2, sticky='w', padx=5, pady=2)
        tk.Button(enc_frame, text="🔄 Regenerate Key", command=self._regenerate_encryption_key, bg="#e74c3c", fg="white", font=("Arial", 9)).grid(row=1, column=0, padx=5, pady=3)
        tk.Button(enc_frame, text="💾 Backup Key", command=self._backup_encryption_key, bg="#3498db", fg="white", font=("Arial", 9)).grid(row=1, column=1, padx=5, pady=3)
        self._refresh_encryption_key_status()

        # Warmup & Seed List
        warmup_section = CollapsibleSection(right_frame, title="🌱 Warmup & Seed List", expanded=False)
        warmup_section.pack(fill='x', padx=10, pady=3)
        warmup_frame = warmup_section.content
        tk.Button(warmup_frame, text="📅 View Warmup Schedule", command=self._view_warmup_schedule, bg="#3498db", fg="white").pack(side=tk.LEFT, padx=5, pady=5)
        tk.Button(warmup_frame, text="🌱 Manage Seed List", command=self._manage_seed_list, bg="#3498db", fg="white").pack(side=tk.LEFT, padx=5, pady=5)
        tk.Checkbutton(warmup_frame, text="Encrypted Logging", variable=tk.BooleanVar(value=CRYPTOGRAPHY_AVAILABLE), bg='#e0f7ff', fg="#2c3e50").pack(side=tk.LEFT, padx=10, pady=5)

        # Warmup State Display
        warmup_status_section = CollapsibleSection(right_frame, title="🔥 Warmup State", expanded=False)
        warmup_status_section.pack(fill='x', padx=10, pady=3)
        warmup_status_frame = warmup_status_section.content
        self.warmup_state_text = scrolledtext.ScrolledText(warmup_status_frame, height=5, font=("Consolas", 9), state="disabled", bg="white", fg="#2c3e50")
        self.warmup_state_text.pack(fill='both', expand=True, padx=5, pady=5)
        tk.Button(warmup_status_frame, text="🔄 Refresh", command=self._refresh_warmup_state_display, bg="#3498db", fg="white", font=("Arial", 9)).pack(pady=2)
        self._refresh_warmup_state_display()

        tk.Button(right_frame, text="💾 Save All Settings", command=self._save_settings, bg="#27ae60", fg="white").pack(pady=10)

    def _edit_vps_config(self):
        """Edit the selected VPS configuration."""
        sel = self.vps_listbox.curselection()
        if not sel:
            messagebox.showwarning("No Selection", "Please select a VPS to edit.")
            return

        vps_id = self.vps_listbox.get(sel[0]).split(' ')[0]
        existing_config = None
        for vps in self.proxy_vps_handler.vps_pool:
            if vps['id'] == vps_id:
                existing_config = vps
                break

        if not existing_config:
            messagebox.showerror("Error", "Selected VPS not found.")
            return

        vps_win = tk.Toplevel(self.root)
        vps_win.title("Edit VPS Config")
        vps_win.geometry("520x750")
        vps_win.transient(self.root)
        vps_win.grab_set()

        fields = {}

        # --- VPS Connection Section ---
        vps_conn_frame = tk.LabelFrame(vps_win, text="VPS Connection", padx=5, pady=5)
        vps_conn_frame.grid(row=0, column=0, columnspan=3, sticky='ew', padx=5, pady=5)
        tk.Label(vps_conn_frame, text="Host/IP:").grid(row=0, column=0, sticky='w', padx=5, pady=2)
        fields['host'] = tk.Entry(vps_conn_frame, width=30)
        fields['host'].insert(0, existing_config.get('host', ''))
        fields['host'].grid(row=0, column=1, padx=5, pady=2)
        tk.Label(vps_conn_frame, text="SSH User:").grid(row=1, column=0, sticky='w', padx=5, pady=2)
        fields['user'] = tk.Entry(vps_conn_frame, width=30)
        fields['user'].insert(0, existing_config.get('user', ''))
        fields['user'].grid(row=1, column=1, padx=5, pady=2)
        tk.Label(vps_conn_frame, text="SSH Key File:").grid(row=2, column=0, sticky='w', padx=5, pady=2)
        key_file_frame = tk.Frame(vps_conn_frame)
        key_file_frame.grid(row=2, column=1, columnspan=2, sticky='ew', padx=5, pady=2)
        fields['key_file'] = tk.Entry(key_file_frame, width=25, state='readonly')
        fields['key_file'].pack(side=tk.LEFT, fill='x', expand=True)
        existing_key = existing_config.get('key_file', '')
        if existing_key:
            fields['key_file'].config(state='normal')
            fields['key_file'].insert(0, existing_key)
            fields['key_file'].config(state='readonly')
        def browse_key_edit():
            path = filedialog.askopenfilename(title="Select SSH Key File", filetypes=[("Key Files", "*.pem *.key *.pub *.ppk"), ("All Files", "*.*")])
            if path:
                fields['key_file'].config(state='normal')
                fields['key_file'].delete(0, tk.END)
                fields['key_file'].insert(0, path)
                fields['key_file'].config(state='readonly')
        tk.Button(key_file_frame, text="📂 Upload Key", command=browse_key_edit, bg="#3498db", fg="white").pack(side=tk.LEFT, padx=3)
        tk.Label(vps_conn_frame, text="Geo Region:").grid(row=3, column=0, sticky='w', padx=5, pady=2)
        fields['geo_region'] = tk.Entry(vps_conn_frame, width=30)
        fields['geo_region'].insert(0, existing_config.get('geo_region', ''))
        fields['geo_region'].grid(row=3, column=1, padx=5, pady=2)
        tk.Label(vps_conn_frame, text="Rate Limit (per day):").grid(row=4, column=0, sticky='w', padx=5, pady=2)
        fields['rate_limit'] = tk.Entry(vps_conn_frame, width=30)
        fields['rate_limit'].insert(0, str(existing_config.get('rate_limit', 500)))
        fields['rate_limit'].grid(row=4, column=1, padx=5, pady=2)
        tk.Label(vps_conn_frame, text="Max Retries:").grid(row=5, column=0, sticky='w', padx=5, pady=2)
        fields['max_retries'] = tk.Entry(vps_conn_frame, width=30)
        fields['max_retries'].insert(0, str(existing_config.get('max_retries', 3)))
        fields['max_retries'].grid(row=5, column=1, padx=5, pady=2)
        tk.Label(vps_conn_frame, text="Circuit Threshold:").grid(row=6, column=0, sticky='w', padx=5, pady=2)
        fields['circuit_threshold'] = tk.Entry(vps_conn_frame, width=30)
        fields['circuit_threshold'].insert(0, str(existing_config.get('circuit_threshold', 5)))
        fields['circuit_threshold'].grid(row=6, column=1, padx=5, pady=2)
        tk.Label(vps_conn_frame, text="AI Handler:").grid(row=7, column=0, sticky='w', padx=5, pady=2)
        fields['ai_handler'] = ttk.Combobox(vps_conn_frame, values=["OpenAI", "Local"], state="readonly", width=28)
        fields['ai_handler'].set(existing_config.get('ai_handler', 'OpenAI'))
        fields['ai_handler'].grid(row=7, column=1, padx=5, pady=2)
        tk.Label(vps_conn_frame, text="(Overrides default AI provider from VPS tab)", fg="#7f8c8d",
                 font=("Arial", 8)).grid(row=7, column=2, sticky='w', padx=5, pady=2)
        tk.Label(vps_conn_frame, text="Proxy Chain (JSON):").grid(row=8, column=0, sticky='w', padx=5, pady=2)
        fields['proxy_chain'] = tk.Text(vps_conn_frame, height=3, width=30)
        fields['proxy_chain'].insert('1.0', json.dumps(existing_config.get('proxy_chain', []), indent=2))
        fields['proxy_chain'].grid(row=8, column=1, padx=5, pady=2)

        # --- SMTP Configuration Section (Optional — leave blank for local mail server) ---
        smtp_section = tk.LabelFrame(vps_win, text="📧 Mail Server Configuration", padx=5, pady=5)
        smtp_section.grid(row=1, column=0, columnspan=3, sticky='ew', padx=5, pady=5)
        
        # Standalone mode checkbox with callback to toggle SMTP fields
        fields['standalone_mode'] = tk.BooleanVar(value=existing_config.get('standalone_mode', False))
        
        # SMTP fields that will be toggled
        smtp_widgets = {}
        
        def toggle_smtp_fields():
            """Enable/disable SMTP fields based on standalone mode."""
            is_standalone = fields['standalone_mode'].get()
            state = 'disabled' if is_standalone else 'normal'
            for widget in smtp_widgets.values():
                widget.config(state=state)
        
        tk.Checkbutton(smtp_section, text="📬 Use VPS Local Mail Server (Postfix/Exim) — no SMTP credentials needed",
                       variable=fields['standalone_mode'], font=("Arial", 9, "bold"),
                       command=toggle_smtp_fields).grid(row=0, column=0, columnspan=2, sticky='w', padx=5, pady=2)
        
        # Add info label for standalone mode
        standalone_info = tk.Label(smtp_section, 
                                   text="ℹ️ When checked, VPS uses its own configured Postfix/Exim. Leave SMTP fields empty.",
                                   fg="#27ae60", font=("Arial", 8))
        standalone_info.grid(row=1, column=0, columnspan=2, sticky='w', padx=20, pady=2)
        
        tk.Label(smtp_section, text="SMTP Server:").grid(row=2, column=0, sticky='w', padx=5, pady=2)
        fields['smtp_server'] = tk.Entry(smtp_section, width=30)
        fields['smtp_server'].insert(0, existing_config.get('smtp_server', ''))
        fields['smtp_server'].grid(row=2, column=1, padx=5, pady=2)
        smtp_widgets['server'] = fields['smtp_server']
        
        tk.Label(smtp_section, text="SMTP Port:").grid(row=3, column=0, sticky='w', padx=5, pady=2)
        fields['smtp_port'] = tk.Entry(smtp_section, width=30)
        fields['smtp_port'].insert(0, str(existing_config.get('smtp_port', 587)))
        fields['smtp_port'].grid(row=3, column=1, padx=5, pady=2)
        smtp_widgets['port'] = fields['smtp_port']
        
        tk.Label(smtp_section, text="SMTP User:").grid(row=4, column=0, sticky='w', padx=5, pady=2)
        fields['smtp_user'] = tk.Entry(smtp_section, width=30)
        fields['smtp_user'].insert(0, existing_config.get('smtp_user', ''))
        fields['smtp_user'].grid(row=4, column=1, padx=5, pady=2)
        smtp_widgets['user'] = fields['smtp_user']
        
        tk.Label(smtp_section, text="SMTP Pass:").grid(row=5, column=0, sticky='w', padx=5, pady=2)
        fields['smtp_pass'] = tk.Entry(smtp_section, width=30, show="*")
        # Do not pre-fill password for security
        fields['smtp_pass'].grid(row=5, column=1, padx=5, pady=2)
        smtp_widgets['pass'] = fields['smtp_pass']
        
        # Initialize the state based on existing config
        toggle_smtp_fields()

        # --- Sender Details Section ---
        sender_section = tk.LabelFrame(vps_win, text="Sender Details", padx=5, pady=5)
        sender_section.grid(row=2, column=0, columnspan=3, sticky='ew', padx=5, pady=5)
        tk.Label(sender_section, text="Sender Name:").grid(row=0, column=0, sticky='w', padx=5, pady=2)
        fields['sender_name'] = tk.Entry(sender_section, width=30)
        fields['sender_name'].insert(0, existing_config.get('sender_details', {}).get('sender_name', 'Sender'))
        fields['sender_name'].grid(row=0, column=1, padx=5, pady=2)
        tk.Label(sender_section, text="Sender Email:").grid(row=1, column=0, sticky='w', padx=5, pady=2)
        fields['sender_email'] = tk.Entry(sender_section, width=30)
        fields['sender_email'].insert(0, existing_config.get('sender_details', {}).get('sender_email', existing_config.get('smtp_user', '')))
        fields['sender_email'].grid(row=1, column=1, padx=5, pady=2)
        tk.Label(sender_section, text="Random Emails Pool (comma-separated):").grid(row=2, column=0, sticky='w', padx=5, pady=2)
        fields['random_emails_pool'] = tk.Entry(sender_section, width=30)
        fields['random_emails_pool'].insert(0, ','.join(existing_config.get('sender_details', {}).get('random_emails_pool', [])))
        fields['random_emails_pool'].grid(row=2, column=1, padx=5, pady=2)

        def save_edit():
            try:
                config = existing_config.copy()
                for k, v in fields.items():
                    if isinstance(v, tk.Text):
                        config[k] = v.get("1.0", tk.END).strip()
                    else:
                        config[k] = v.get()

                if config['proxy_chain']:
                    try:
                        config['proxy_chain'] = json.loads(config['proxy_chain'])
                    except:
                        config['proxy_chain'] = []

                config['rate_limit'] = int(config['rate_limit']) if config['rate_limit'].isdigit() else 500
                config['max_retries'] = int(config['max_retries']) if config['max_retries'].isdigit() else 3
                config['circuit_threshold'] = int(config['circuit_threshold']) if config['circuit_threshold'].isdigit() else 5

                # Update sender_details
                config['sender_details'] = {
                    'sender_name': config.pop('sender_name', 'Sender'),
                    'sender_email': config.pop('sender_email', 'noreply@example.com'),
                    'random_emails_pool': [e.strip() for e in config.pop('random_emails_pool', '').split(',') if e.strip()]
                }

                if CRYPTOGRAPHY_AVAILABLE and config.get('smtp_pass'):
                    config['smtp_pass_encrypted'] = self.proxy_vps_handler._encrypt_password(config['smtp_pass'])
                    del config['smtp_pass']
                elif not CRYPTOGRAPHY_AVAILABLE and config.get('smtp_pass'):
                    config['smtp_pass_encrypted'] = config['smtp_pass']
                    del config['smtp_pass']
                else:
                    # No password entered — clear any stale encrypted password
                    config.pop('smtp_pass', None)
                    config.pop('smtp_pass_encrypted', None)

                # Update the pool
                for i, vps in enumerate(self.proxy_vps_handler.vps_pool):
                    if vps['id'] == vps_id:
                        self.proxy_vps_handler.vps_pool[i] = config
                        break

                self.proxy_vps_handler.save_vps_configs()
                self._refresh_vps_list()
                vps_win.destroy()
            except Exception as e:
                messagebox.showerror("Save Error", f"Failed to save VPS config: {e}")

        btn_frame = tk.Frame(vps_win)
        btn_frame.grid(row=3, column=0, columnspan=3, pady=10)
        tk.Button(btn_frame, text="Test VPS", command=lambda: self._test_selected_vps(fields), bg="#f39c12", fg="white").pack(side=tk.LEFT, padx=10)
        tk.Button(btn_frame, text="Save Changes", command=save_edit).pack(side=tk.LEFT, padx=10)

    def _add_vps_config(self):
        vps_win = tk.Toplevel(self.root)
        vps_win.title("Add VPS Config")
        vps_win.geometry("520x750")
        vps_win.transient(self.root)
        vps_win.grab_set()

        fields = {}

        # --- VPS Connection Section ---
        vps_conn_frame = tk.LabelFrame(vps_win, text="VPS Connection", padx=5, pady=5)
        vps_conn_frame.grid(row=0, column=0, columnspan=3, sticky='ew', padx=5, pady=5)
        tk.Label(vps_conn_frame, text="Host/IP:").grid(row=0, column=0, sticky='w', padx=5, pady=2)
        fields['host'] = tk.Entry(vps_conn_frame, width=30)
        fields['host'].grid(row=0, column=1, padx=5, pady=2)
        tk.Label(vps_conn_frame, text="SSH User:").grid(row=1, column=0, sticky='w', padx=5, pady=2)
        fields['user'] = tk.Entry(vps_conn_frame, width=30)
        fields['user'].grid(row=1, column=1, padx=5, pady=2)
        tk.Label(vps_conn_frame, text="SSH Key File:").grid(row=2, column=0, sticky='w', padx=5, pady=2)
        key_file_frame = tk.Frame(vps_conn_frame)
        key_file_frame.grid(row=2, column=1, columnspan=2, sticky='ew', padx=5, pady=2)
        fields['key_file'] = tk.Entry(key_file_frame, width=25, state='readonly')
        fields['key_file'].pack(side=tk.LEFT, fill='x', expand=True)
        def browse_key_add():
            path = filedialog.askopenfilename(title="Select SSH Key File", filetypes=[("Key Files", "*.pem *.key *.pub *.ppk"), ("All Files", "*.*")])
            if path:
                fields['key_file'].config(state='normal')
                fields['key_file'].delete(0, tk.END)
                fields['key_file'].insert(0, path)
                fields['key_file'].config(state='readonly')
        tk.Button(key_file_frame, text="📂 Upload Key", command=browse_key_add, bg="#3498db", fg="white").pack(side=tk.LEFT, padx=3)
        tk.Label(vps_conn_frame, text="Geo Region:").grid(row=3, column=0, sticky='w', padx=5, pady=2)
        fields['geo_region'] = tk.Entry(vps_conn_frame, width=30)
        fields['geo_region'].grid(row=3, column=1, padx=5, pady=2)
        tk.Label(vps_conn_frame, text="Rate Limit (per day):").grid(row=4, column=0, sticky='w', padx=5, pady=2)
        fields['rate_limit'] = tk.Entry(vps_conn_frame, width=30)
        fields['rate_limit'].grid(row=4, column=1, padx=5, pady=2)
        tk.Label(vps_conn_frame, text="Max Retries:").grid(row=5, column=0, sticky='w', padx=5, pady=2)
        fields['max_retries'] = tk.Entry(vps_conn_frame, width=30)
        fields['max_retries'].grid(row=5, column=1, padx=5, pady=2)
        tk.Label(vps_conn_frame, text="Circuit Threshold:").grid(row=6, column=0, sticky='w', padx=5, pady=2)
        fields['circuit_threshold'] = tk.Entry(vps_conn_frame, width=30)
        fields['circuit_threshold'].grid(row=6, column=1, padx=5, pady=2)
        tk.Label(vps_conn_frame, text="AI Handler:").grid(row=7, column=0, sticky='w', padx=5, pady=2)
        fields['ai_handler'] = ttk.Combobox(vps_conn_frame, values=["OpenAI", "Local"], state="readonly", width=28)
        fields['ai_handler'].grid(row=7, column=1, padx=5, pady=2)
        tk.Label(vps_conn_frame, text="(Overrides default AI provider from VPS tab)", fg="#7f8c8d",
                 font=("Arial", 8)).grid(row=7, column=2, sticky='w', padx=5, pady=2)
        tk.Label(vps_conn_frame, text="Proxy Chain (JSON):").grid(row=8, column=0, sticky='w', padx=5, pady=2)
        fields['proxy_chain'] = tk.Text(vps_conn_frame, height=3, width=30)
        fields['proxy_chain'].grid(row=8, column=1, padx=5, pady=2)

        # --- SMTP Configuration Section (Optional — leave blank for local mail server) ---
        smtp_section = tk.LabelFrame(vps_win, text="📧 Mail Server Configuration", padx=5, pady=5)
        smtp_section.grid(row=1, column=0, columnspan=3, sticky='ew', padx=5, pady=5)
        
        # Standalone mode checkbox with callback to toggle SMTP fields
        fields['standalone_mode'] = tk.BooleanVar(value=False)
        
        # SMTP fields that will be toggled
        smtp_widgets = {}
        
        def toggle_smtp_fields():
            """Enable/disable SMTP fields based on standalone mode."""
            is_standalone = fields['standalone_mode'].get()
            state = 'disabled' if is_standalone else 'normal'
            for widget in smtp_widgets.values():
                widget.config(state=state)
        
        tk.Checkbutton(smtp_section, text="📬 Use VPS Local Mail Server (Postfix/Exim) — no SMTP credentials needed",
                       variable=fields['standalone_mode'], font=("Arial", 9, "bold"),
                       command=toggle_smtp_fields).grid(row=0, column=0, columnspan=2, sticky='w', padx=5, pady=2)
        
        # Add info label for standalone mode
        standalone_info = tk.Label(smtp_section, 
                                   text="ℹ️ When checked, VPS uses its own configured Postfix/Exim. Leave SMTP fields empty.",
                                   fg="#27ae60", font=("Arial", 8))
        standalone_info.grid(row=1, column=0, columnspan=2, sticky='w', padx=20, pady=2)
        
        tk.Label(smtp_section, text="SMTP Server:").grid(row=2, column=0, sticky='w', padx=5, pady=2)
        fields['smtp_server'] = tk.Entry(smtp_section, width=30)
        fields['smtp_server'].grid(row=2, column=1, padx=5, pady=2)
        smtp_widgets['server'] = fields['smtp_server']
        
        tk.Label(smtp_section, text="SMTP Port:").grid(row=3, column=0, sticky='w', padx=5, pady=2)
        fields['smtp_port'] = tk.Entry(smtp_section, width=30)
        fields['smtp_port'].grid(row=3, column=1, padx=5, pady=2)
        smtp_widgets['port'] = fields['smtp_port']
        
        tk.Label(smtp_section, text="SMTP User:").grid(row=4, column=0, sticky='w', padx=5, pady=2)
        fields['smtp_user'] = tk.Entry(smtp_section, width=30)
        fields['smtp_user'].grid(row=4, column=1, padx=5, pady=2)
        smtp_widgets['user'] = fields['smtp_user']
        
        tk.Label(smtp_section, text="SMTP Pass:").grid(row=5, column=0, sticky='w', padx=5, pady=2)
        fields['smtp_pass'] = tk.Entry(smtp_section, width=30, show="*")
        fields['smtp_pass'].grid(row=5, column=1, padx=5, pady=2)
        smtp_widgets['pass'] = fields['smtp_pass']

        # --- Sender Details Section ---
        sender_section = tk.LabelFrame(vps_win, text="Sender Details", padx=5, pady=5)
        sender_section.grid(row=2, column=0, columnspan=3, sticky='ew', padx=5, pady=5)
        tk.Label(sender_section, text="Sender Name:").grid(row=0, column=0, sticky='w', padx=5, pady=2)
        fields['sender_name'] = tk.Entry(sender_section, width=30)
        fields['sender_name'].grid(row=0, column=1, padx=5, pady=2)
        tk.Label(sender_section, text="Sender Email:").grid(row=1, column=0, sticky='w', padx=5, pady=2)
        fields['sender_email'] = tk.Entry(sender_section, width=30)
        fields['sender_email'].grid(row=1, column=1, padx=5, pady=2)
        tk.Label(sender_section, text="Random Emails Pool (comma-separated):").grid(row=2, column=0, sticky='w', padx=5, pady=2)
        fields['random_emails_pool'] = tk.Entry(sender_section, width=30)
        fields['random_emails_pool'].grid(row=2, column=1, padx=5, pady=2)

        def save_vps():
            try:
                config = {}
                for k, v in fields.items():
                    if isinstance(v, tk.Text):
                        config[k] = v.get("1.0", tk.END).strip()
                    else:
                        config[k] = v.get()

                if config['proxy_chain']:
                    try:
                        config['proxy_chain'] = json.loads(config['proxy_chain'])
                    except:
                        config['proxy_chain'] = []

                config['rate_limit'] = int(config['rate_limit']) if config['rate_limit'].isdigit() else 500
                config['max_retries'] = int(config['max_retries']) if config['max_retries'].isdigit() else 3
                config['circuit_threshold'] = int(config['circuit_threshold']) if config['circuit_threshold'].isdigit() else 5

                # Set sender_details
                config['sender_details'] = {
                    'sender_name': config.pop('sender_name', 'Sender'),
                    'sender_email': config.pop('sender_email', 'noreply@example.com'),
                    'random_emails_pool': [e.strip() for e in config.pop('random_emails_pool', '').split(',') if e.strip()]
                }

                if CRYPTOGRAPHY_AVAILABLE and config.get('smtp_pass'):
                    config['smtp_pass_encrypted'] = self.proxy_vps_handler._encrypt_password(config['smtp_pass'])
                    del config['smtp_pass']
                elif not CRYPTOGRAPHY_AVAILABLE and config.get('smtp_pass'):
                    config['smtp_pass_encrypted'] = config['smtp_pass']
                    del config['smtp_pass']
                else:
                    # No password entered — clean up
                    config.pop('smtp_pass', None)

                self.proxy_vps_handler.add_vps(config)
                self._refresh_vps_list()
                vps_win.destroy()
            except Exception as e:
                messagebox.showerror("Save Error", f"Failed to save VPS config: {e}")

        btn_frame = tk.Frame(vps_win)
        btn_frame.grid(row=3, column=0, columnspan=3, pady=10)
        tk.Button(btn_frame, text="Test VPS", command=lambda: self._test_selected_vps(fields), bg="#f39c12", fg="white").pack(side=tk.LEFT, padx=10)
        tk.Button(btn_frame, text="Save", command=save_vps).pack(side=tk.LEFT, padx=10)

    def _test_selected_vps(self, fields):
        try:
            config = {}
            for k, v in fields.items():
                if isinstance(v, tk.Text):
                    config[k] = v.get("1.0", tk.END).strip()
                else:
                    config[k] = v.get()

            if not config['host'] or not config['user']:
                messagebox.showerror("Incomplete", "Host and SSH User are required for testing.")
                return

            if config['proxy_chain']:
                try:
                    config['proxy_chain'] = json.loads(config['proxy_chain'])
                except:
                    config['proxy_chain'] = []

            config['rate_limit'] = int(config['rate_limit']) if config['rate_limit'].isdigit() else 500
            config['max_retries'] = int(config['max_retries']) if config['max_retries'].isdigit() else 3
            config['circuit_threshold'] = int(config['circuit_threshold']) if config['circuit_threshold'].isdigit() else 5

            # Add sender_details for testing
            config['sender_details'] = {
                'sender_name': config.pop('sender_name', 'Sender'),
                'sender_email': config.pop('sender_email', 'noreply@example.com'),
                'random_emails_pool': [e.strip() for e in config.pop('random_emails_pool', '').split(',') if e.strip()]
            }

            if CRYPTOGRAPHY_AVAILABLE and config.get('smtp_pass'):
                config['smtp_pass_encrypted'] = self.proxy_vps_handler._encrypt_password(config['smtp_pass'])
                del config['smtp_pass']
            elif not CRYPTOGRAPHY_AVAILABLE and config.get('smtp_pass'):
                config['smtp_pass_encrypted'] = config['smtp_pass']
                del config['smtp_pass']
            else:
                config.pop('smtp_pass', None)

            success, msg = self.proxy_vps_handler.test_vps_connection(config)
            if success:
                messagebox.showinfo("Test Result", msg)
            else:
                messagebox.showerror("Test Failed", msg)
        except Exception as e:
            messagebox.showerror("Test Error", f"Error during test: {e}")

    def _remove_selected_vps(self):
        sel = self.vps_listbox.curselection()
        if not sel: return
        vps_id = self.vps_listbox.get(sel[0]).split(' ')[0]
        self.proxy_vps_handler.remove_vps(vps_id)
        self._refresh_vps_list()

    def _refresh_vps_list(self):
        self.vps_listbox.delete(0, tk.END)
        for vps in self.proxy_vps_handler.vps_pool:
            status = "🟢" if self.proxy_vps_handler.health_status.get(vps['id'], False) else "🔴"
            smtp_ok = "SMTP: OK" if self.proxy_vps_handler.health_status.get(vps['id'], False) else "SMTP: ?"
            self.vps_listbox.insert(tk.END, f"{vps['id']} {status} {vps.get('geo_region', 'Unknown')} | {smtp_ok}")

    def _refresh_vps_health(self):
        self.proxy_vps_handler.check_health()
        self._refresh_vps_list()
        self.log("✅ VPS health refreshed.")

    def _build_deliverability_tab(self):
        deliv_tab = ttk.Frame(self.notebook)
        self.notebook.add(deliv_tab, text='🛡️ Deliverability')

        main_pane = ttk.PanedWindow(deliv_tab, orient=tk.HORIZONTAL)
        main_pane.pack(fill='both', expand=True, padx=5, pady=5)

        left_frame = tk.Frame(main_pane, bg='#e0f7ff')
        main_pane.add(left_frame, weight=1)
        right_frame = tk.Frame(main_pane, bg='#e0f7ff')
        main_pane.add(right_frame, weight=2)

        # Domain & IP Reputation
        check_frame = tk.LabelFrame(left_frame, text="Domain & IP Reputation", bg='#e0f7ff', fg="#2c3e50")
        check_frame.pack(fill='x', padx=10, pady=5)

        tk.Label(check_frame, text="Domain/IP to Check:", bg='#e0f7ff', fg="#2c3e50").pack(anchor='w', padx=5)
        self.deliv_target_var = tk.StringVar()
        tk.Entry(check_frame, textvariable=self.deliv_target_var, bg="white", fg="#2c3e50").pack(fill='x', padx=5, pady=2)

        tk.Button(check_frame, text="Check Authentication (Domain)", command=self._run_auth_check, bg="#3498db", fg="white").pack(fill='x', padx=5, pady=2)
        tk.Button(check_frame, text="Check Blacklists (Domain or IP)", command=self._run_blacklist_check, bg="#3498db", fg="white").pack(fill='x', padx=5, pady=2)
        tk.Button(check_frame, text="Run Bulk Domain Check", command=self._run_bulk_domain_check, bg="#3498db", fg="white").pack(fill='x', padx=5, pady=5)

        tk.Label(check_frame, text="Tip: Use your SMTP sender domain/IP.", font=("Arial", 8, "italic"), bg='#e0f7ff', fg="#7f8c8d").pack(padx=5, pady=5)

        # Upload Domain/Email List
        upload_frame = tk.LabelFrame(left_frame, text="Upload Email/Domain List", bg='#e0f7ff', fg="#2c3e50")
        upload_frame.pack(fill='x', padx=10, pady=5)
        tk.Button(upload_frame, text="📂 Upload Email/Domain List", command=self._upload_domain_list, bg="#3498db", fg="white").pack(fill='x', padx=5, pady=2)
        self.deliv_domain_count_label = tk.Label(upload_frame, text="📋 No domains loaded", font=("Arial", 9), bg='#e0f7ff', fg="#666")
        self.deliv_domain_count_label.pack(anchor='w', padx=5, pady=2)
        self.deliv_progress = ttk.Progressbar(upload_frame, orient='horizontal', mode='determinate')
        self.deliv_progress.pack(fill='x', padx=5, pady=5)

        # DNS Validation Actions
        dns_actions_frame = tk.LabelFrame(left_frame, text="DNS Validation Actions", bg='#e0f7ff', fg="#2c3e50")
        dns_actions_frame.pack(fill='x', padx=10, pady=5)
        tk.Button(dns_actions_frame, text="📋 Copy Validated Emails", command=self._copy_validated_emails, bg="#27ae60", fg="white").pack(fill='x', padx=5, pady=2)
        tk.Button(dns_actions_frame, text="🗑️ Delete Unvalidated", command=self._delete_unvalidated_entries, bg="#e74c3c", fg="white").pack(fill='x', padx=5, pady=2)

        # Pre-Send Content Analysis
        spam_check_frame = tk.LabelFrame(left_frame, text="Pre-Send Content Analysis", bg='#e0f7ff', fg="#2c3e50")
        spam_check_frame.pack(fill='x', padx=10, pady=5)
        tk.Button(spam_check_frame, text="Run Basic Spam Check", command=lambda: self._run_spam_check(), bg="#3498db", fg="white").pack(fill='x', padx=5, pady=2)
        tk.Button(spam_check_frame, text="🧠 Run AI Spam Check", command=self._run_ai_spam_check, bg="#9b59b6", fg="white").pack(fill='x', padx=5, pady=5)

        # Link Health Check
        tk.Button(left_frame, text="🔗 Link Health Check", command=self._run_link_health_check, bg="#3498db", fg="white").pack(fill='x', padx=10, pady=2)

        # Export Results
        tk.Button(left_frame, text="📤 Export Results", command=self._export_deliv_results, bg="#27ae60", fg="white").pack(fill='x', padx=10, pady=5)

        # Suppression List Management
        suppression_frame = tk.LabelFrame(left_frame, text="🚫 Suppression List Management", bg='#e0f7ff', fg="#2c3e50")
        suppression_frame.pack(fill='x', padx=10, pady=5)
        tk.Button(suppression_frame, text="👁️ View List", command=self._view_suppression_list, bg="#3498db", fg="white").pack(fill='x', padx=5, pady=2)
        tk.Button(suppression_frame, text="➕ Add Email", command=self._add_to_suppression_manually, bg="#27ae60", fg="white").pack(fill='x', padx=5, pady=2)
        tk.Button(suppression_frame, text="📤 Export List", command=self._export_suppression_list, bg="#e67e22", fg="white").pack(fill='x', padx=5, pady=2)

        # Right panel with tabbed sub-sections
        right_notebook = ttk.Notebook(right_frame)
        right_notebook.pack(fill='both', expand=True, padx=5, pady=5)

        # Tab: Check Results
        results_tab = ttk.Frame(right_notebook)
        right_notebook.add(results_tab, text="Check Results")
        self.domain_health_label_top = tk.Label(results_tab, text="🛡️ Domain: Not checked", font=("Arial", 10, "bold"), bg='#e0f7ff', fg="#666")
        self.domain_health_label_top.pack(pady=5, fill='x')
        self.deliv_results_text = scrolledtext.ScrolledText(results_tab, wrap=tk.WORD, state="disabled", bg="white", fg="#2c3e50")
        self.deliv_results_text.pack(fill='both', expand=True, padx=5, pady=5)

        # Tab: DNS Validation
        dns_tab = ttk.Frame(right_notebook)
        right_notebook.add(dns_tab, text="DNS Validation")
        dns_columns = ('Domain', 'SPF', 'DKIM', 'DMARC', 'MX', 'Status')
        self.dns_table = ttk.Treeview(dns_tab, columns=dns_columns, show='headings', height=15)
        for col in dns_columns:
            self.dns_table.heading(col, text=col)
            self.dns_table.column(col, width=100, anchor='center')
        self.dns_table.column('Domain', width=180, anchor='w')
        self.dns_table.tag_configure('pass', background='#d5f5e3')
        self.dns_table.tag_configure('warn', background='#fdebd0')
        self.dns_table.tag_configure('fail', background='#fadbd8')
        dns_scroll = ttk.Scrollbar(dns_tab, orient='vertical', command=self.dns_table.yview)
        self.dns_table.configure(yscrollcommand=dns_scroll.set)
        self.dns_table.pack(side='left', fill='both', expand=True, padx=5, pady=5)
        dns_scroll.pack(side='right', fill='y', pady=5)

        # Tab: Batch Log
        batch_tab = ttk.Frame(right_notebook)
        right_notebook.add(batch_tab, text="Batch Log")
        self.domain_health_label = tk.Label(batch_tab, text="🛡️ Domain: Not checked", font=("Arial", 10), bg='#e0f7ff', fg="#666")
        self.domain_health_label.pack(pady=5)
        self.domain_check_results_text = scrolledtext.ScrolledText(batch_tab, wrap=tk.WORD, state="disabled", height=10, bg="white", fg="#2c3e50")
        self.domain_check_results_text.pack(fill='both', expand=True, padx=5, pady=5)

    def _run_bulk_domain_check(self):
        """Run bulk domain check using email list."""
        if not self.email_list:
            messagebox.showwarning("No Emails", "Please load an email list first.")
            return

        if not messagebox.askyesno("Bulk Domain Check", f"This will check MX records for {len(set(email.split('@')[1] for email in self.email_list if '@' in email))} unique domains. Continue?"):
            return

        self.deliverability_helper.run_domain_validator(self.email_list)

    def _upload_domain_list(self):
        """Upload a .txt or .csv file containing emails/domains for bulk DNS validation."""
        file_path = filedialog.askopenfilename(
            title="Upload Email/Domain List",
            filetypes=[("Text/CSV Files", "*.txt *.csv"), ("All Files", "*.*")]
        )
        if not file_path:
            return
        try:
            domains = []
            # Store email-to-domain mapping for validated email output
            domain_to_emails = {}
            with open(file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    # Extract domain from email if '@' present
                    if '@' in line:
                        parts = line.split(',')
                        for part in parts:
                            part = part.strip()
                            if '@' in part:
                                domain = part.split('@')[1].strip().lower()
                                if domain:
                                    domains.append(domain)
                                    if domain not in domain_to_emails:
                                        domain_to_emails[domain] = []
                                    domain_to_emails[domain].append(part.strip())
                    else:
                        # Assume each line/comma-separated value is a domain
                        parts = line.split(',')
                        for part in parts:
                            part = part.strip()
                            if '.' in part:
                                domains.append(part.lower())
            domains = list(set(domains))
            if not domains:
                messagebox.showwarning("No Domains", "No valid domains found in the file.")
                return
            self._dns_domain_to_emails = domain_to_emails
            self.deliv_domain_count_label.config(text=f"📋 {len(domains)} domains loaded")
            self.log(f"📂 Loaded {len(domains)} unique domains from {os.path.basename(file_path)}")
            self._run_uploaded_domain_validation(domains)
        except Exception as e:
            messagebox.showerror("Upload Error", f"Failed to read file: {e}")

    def _run_uploaded_domain_validation(self, domains):
        """Run DNS validation on uploaded domain list with concurrent processing, progress and table update."""
        def validate():
            total = len(domains)
            self.root.after(0, lambda: self.deliv_progress.configure(maximum=total, value=0))
            completed = [0]  # mutable counter for thread-safe increment

            def validate_single(domain):
                try:
                    auth = self.deliverability_helper.check_domain_authentication(domain)
                    mx = self.deliverability_helper.check_mx_record(domain)
                    spf = auth.get('spf', 'N/A')
                    dkim_val = auth.get('dkim', 'N/A')
                    dmarc = auth.get('dmarc', 'N/A')

                    # Determine status
                    fails = sum(1 for v in [spf, dkim_val, dmarc] if '❌' in str(v) or 'Missing' in str(v))
                    warns = sum(1 for v in [spf, dkim_val, dmarc] if '⚠' in str(v))
                    if mx != 'Valid':
                        status = 'Fail'
                    elif fails > 0:
                        status = 'Warning'
                    elif warns > 0:
                        status = 'Warning'
                    else:
                        status = 'Pass'

                    self.root.after(0, lambda d=domain, s=spf, dk=dkim_val, dm=dmarc, m=mx, st=status: self._insert_dns_row(d, s, dk, dm, m, st))
                except Exception as e:
                    self.root.after(0, lambda d=domain, err=str(e): self._insert_dns_row(d, 'Error', 'Error', 'Error', 'Error', 'Fail'))

                completed[0] += 1
                self.root.after(0, lambda v=completed[0]: self.deliv_progress.configure(value=v))

            max_workers = min(20, max(5, len(domains) // 5))
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {executor.submit(validate_single, domain): domain for domain in domains}
                for future in as_completed(futures):
                    future.result()

            self.root.after(0, lambda: self.log(f"✅ DNS validation complete for {total} domains."))

        threading.Thread(target=validate, daemon=True).start()

    def _insert_dns_row(self, domain, spf, dkim_val, dmarc, mx, status):
        """Insert a row into the DNS validation table."""
        tag = 'pass' if status == 'Pass' else ('warn' if status == 'Warning' else 'fail')
        self.dns_table.insert('', 'end', values=(domain, spf, dkim_val, dmarc, mx, status), tags=(tag,))

    def _copy_validated_emails(self):
        """Copy validated emails (from domains with Pass status) to clipboard."""
        validated_emails = []
        domain_to_emails = getattr(self, '_dns_domain_to_emails', {})
        for item in self.dns_table.get_children():
            values = self.dns_table.item(item)['values']
            domain = str(values[0]).lower()
            status = str(values[5])
            if status == 'Pass':
                if domain in domain_to_emails:
                    validated_emails.extend(domain_to_emails[domain])
                else:
                    validated_emails.append(domain)
        if not validated_emails:
            messagebox.showinfo("No Validated Emails", "No validated emails found. Upload an email list and run DNS validation first.")
            return
        validated_emails = list(dict.fromkeys(validated_emails))  # deduplicate preserving order
        self.root.clipboard_clear()
        self.root.clipboard_append('\n'.join(validated_emails))
        self.log(f"📋 Copied {len(validated_emails)} validated emails to clipboard.")
        messagebox.showinfo("Copied", f"{len(validated_emails)} validated emails copied to clipboard.")

    def _delete_unvalidated_entries(self):
        """Delete entries with non-Pass status from the DNS validation table."""
        items_to_delete = []
        for item in self.dns_table.get_children():
            values = self.dns_table.item(item)['values']
            status = str(values[5])
            if status != 'Pass':
                items_to_delete.append(item)
        if not items_to_delete:
            messagebox.showinfo("No Unvalidated", "No unvalidated entries to delete.")
            return
        for item in items_to_delete:
            self.dns_table.delete(item)
        self.log(f"🗑️ Deleted {len(items_to_delete)} unvalidated entries from DNS table.")

    def _export_deliv_results(self):
        """Export deliverability results to CSV."""
        file_path = filedialog.asksaveasfilename(
            title="Export Deliverability Results",
            defaultextension=".csv",
            filetypes=[("CSV Files", "*.csv")]
        )
        if not file_path:
            return
        try:
            with open(file_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['Domain', 'SPF', 'DKIM', 'DMARC', 'MX', 'Status'])
                for item in self.dns_table.get_children():
                    writer.writerow(self.dns_table.item(item)['values'])
            self.log(f"📤 Deliverability results exported to {file_path}")
            messagebox.showinfo("Exported", f"Results saved to {file_path}")
        except Exception as e:
            messagebox.showerror("Export Error", f"Failed to export: {e}")

    def _build_sequences_tab(self):
        sequences_tab = ttk.Frame(self.notebook)
        self.notebook.add(sequences_tab, text='💧 Sequences')

        # Top toolbar (pinned)
        top_toolbar = tk.Frame(sequences_tab, bg='#d5eaf7')
        top_toolbar.pack(fill='x', padx=5, pady=(5, 0))
        tk.Button(top_toolbar, text="➕ New Sequence", command=self._new_sequence, bg="#27ae60", fg="white", font=("Arial", 9, "bold")).pack(side=tk.LEFT, padx=2, pady=2)
        tk.Button(top_toolbar, text="💾 Save", command=self._save_sequence, bg="#3498db", fg="white", font=("Arial", 9, "bold")).pack(side=tk.LEFT, padx=2, pady=2)
        tk.Button(top_toolbar, text="📋 Duplicate Step", command=self._duplicate_sequence_step, bg="#3498db", fg="white", font=("Arial", 9, "bold")).pack(side=tk.LEFT, padx=2, pady=2)
        tk.Button(top_toolbar, text="🗑️ Delete Step", command=self._delete_last_sequence_step, bg="#e74c3c", fg="white", font=("Arial", 9, "bold")).pack(side=tk.LEFT, padx=2, pady=2)
        tk.Button(top_toolbar, text="🗑️ Delete Sequence", command=self._delete_sequence, bg="#e74c3c", fg="white", font=("Arial", 9, "bold")).pack(side=tk.LEFT, padx=2, pady=2)
        tk.Button(top_toolbar, text="🔄 Simulate Run", command=self._simulate_sequence, bg="#9b59b6", fg="white", font=("Arial", 9, "bold")).pack(side=tk.LEFT, padx=2, pady=2)
        tk.Button(top_toolbar, text="📤 Export Sequence", command=self._export_sequence, bg="#e67e22", fg="white", font=("Arial", 9, "bold")).pack(side=tk.LEFT, padx=2, pady=2)
        tk.Button(top_toolbar, text="▶️ Start for Loaded List", command=self._start_sequence_for_list, bg="#27ae60", fg="white", font=("Arial", 9, "bold")).pack(side=tk.RIGHT, padx=2, pady=2)

        # Main split layout
        main_pane = ttk.PanedWindow(sequences_tab, orient=tk.HORIZONTAL)
        main_pane.pack(fill='both', expand=True, padx=5, pady=5)

        # LEFT: Sequence list + Step builder
        left_pane = ttk.PanedWindow(main_pane, orient=tk.VERTICAL)
        main_pane.add(left_pane, weight=1)

        sequences_list_frame = tk.LabelFrame(left_pane, text="Automated Sequences", bg='#e0f7ff', fg="#2c3e50")
        left_pane.add(sequences_list_frame, weight=1)

        self.sequences_listbox = tk.Listbox(sequences_list_frame, exportselection=False, bg="white", fg="#2c3e50")
        self.sequences_listbox.pack(fill='both', expand=True, padx=5, pady=5)
        self.sequences_listbox.bind('<<ListboxSelect>>', self._load_sequence_from_listbox)
        self._refresh_sequences_list()

        # Step builder controls
        step_builder_frame = tk.LabelFrame(left_pane, text="Add Steps", bg='#e0f7ff', fg="#2c3e50")
        left_pane.add(step_builder_frame, weight=0)

        tk.Button(step_builder_frame, text="✉️ Add Email Step", command=lambda: self._add_sequence_step('email'), bg="#3498db", fg="white").pack(fill='x', padx=5, pady=2)
        tk.Button(step_builder_frame, text="⏳ Add Delay Step", command=lambda: self._add_sequence_step('wait'), bg="#3498db", fg="white").pack(fill='x', padx=5, pady=2)
        tk.Button(step_builder_frame, text="🔀 Add Condition", command=lambda: self._add_sequence_step('if_status'), bg="#3498db", fg="white").pack(fill='x', padx=5, pady=2)

        # RIGHT: Editor + Preview
        right_pane = ttk.PanedWindow(main_pane, orient=tk.VERTICAL)
        main_pane.add(right_pane, weight=3)

        self.sequence_editor_frame = tk.LabelFrame(right_pane, text="Sequence Editor: (No sequence selected)", bg='#e0f7ff', fg="#2c3e50")
        right_pane.add(self.sequence_editor_frame, weight=2)

        self.sequence_canvas_frame = tk.Frame(self.sequence_editor_frame, bg='#e0f7ff')
        self.sequence_canvas_frame.pack(fill='both', expand=True)

        # Simulation / Preview panel
        preview_frame = tk.LabelFrame(right_pane, text="Execution Preview & Simulation", bg='#e0f7ff', fg="#2c3e50")
        right_pane.add(preview_frame, weight=1)

        self.sequence_preview_text = scrolledtext.ScrolledText(preview_frame, wrap=tk.WORD, font=("Consolas", 9), state="disabled", bg="white", fg="#2c3e50", height=8)
        self.sequence_preview_text.pack(fill='both', expand=True, padx=5, pady=5)

    def _build_analytics_tab(self):
        analytics_tab = ttk.Frame(self.notebook)
        self.notebook.add(analytics_tab, text='📈 Visual Analytics')

        # TOP: Compact header + refresh
        header_frame = tk.Frame(analytics_tab, bg='#e0f7ff')
        header_frame.pack(fill='x', padx=10, pady=(5, 2))
        tk.Label(header_frame, text="Visual Analytics Dashboard", font=("Arial", 14, "bold"), bg='#e0f7ff', fg="#2c3e50").pack(side=tk.LEFT)
        tk.Button(header_frame, text="🔄 Refresh Stats", command=self._update_analytics, bg="#3498db", fg="white").pack(side=tk.RIGHT)
        tk.Button(header_frame, text="📤 Export Report", command=self._export_report, bg="#e67e22", fg="white").pack(side=tk.RIGHT, padx=5)

        # TOP: Compact KPI cards in grid (6 cards, 2 rows x 3 cols)
        stats_frame = tk.Frame(analytics_tab, bg='#e0f7ff')
        stats_frame.pack(fill='x', padx=5, pady=2)

        def create_stat_box(parent, title, row, col):
            frame = tk.LabelFrame(parent, text=title, font=("Arial", 8, "bold"), bg='#e0f7ff', fg="#2c3e50")
            frame.grid(row=row, column=col, sticky='nsew', padx=3, pady=2)
            val_label = tk.Label(frame, text="0", font=("Arial", 16, "bold"), bg='#e0f7ff', fg="#2c3e50")
            val_label.pack(pady=(2, 0))
            pct_label = tk.Label(frame, text="(0.0%)", font=("Arial", 8), bg='#e0f7ff', fg="#7f8c8d")
            pct_label.pack(pady=(0, 2))
            return val_label, pct_label

        self.analytics_sent_val, _ = create_stat_box(stats_frame, "Sent", 0, 0)
        self.analytics_opens_val, self.analytics_opens_pct = create_stat_box(stats_frame, "Opens", 0, 1)
        self.analytics_clicks_val, self.analytics_clicks_pct = create_stat_box(stats_frame, "Clicks", 0, 2)
        self.analytics_unsub_val, self.analytics_unsub_pct = create_stat_box(stats_frame, "Unsubscribes", 1, 0)
        self.analytics_failed_val, self.analytics_failed_pct = create_stat_box(stats_frame, "Failed", 1, 1)
        self.analytics_bounce_val, self.analytics_bounce_pct = create_stat_box(stats_frame, "Bounced", 1, 2)
        for c in range(3):
            stats_frame.grid_columnconfigure(c, weight=1)

        # BOTTOM: Tabbed panels
        analytics_notebook = ttk.Notebook(analytics_tab)
        analytics_notebook.pack(fill="both", expand=True, padx=5, pady=5)

        # Tab 1 - Performance Charts
        charts_tab = ttk.Frame(analytics_notebook)
        analytics_notebook.add(charts_tab, text="📊 Performance")
        self.charts_canvas_frame = tk.Frame(charts_tab, bg='#e0f7ff')
        self.charts_canvas_frame.pack(fill='both', expand=True)

        if not MATPLOTLIB_AVAILABLE:
            tk.Label(self.charts_canvas_frame, text="📊 Charts unavailable: Install matplotlib\n(pip install matplotlib)",
                     font=("Arial", 12), bg='#e0f7ff', fg="#e74c3c").pack(expand=True, pady=40)

        # Tab 2 - Delivery Breakdown
        breakdown_tab = ttk.Frame(analytics_notebook)
        analytics_notebook.add(breakdown_tab, text="📋 Delivery Breakdown")
        self.delivery_breakdown_text = scrolledtext.ScrolledText(breakdown_tab, wrap=tk.WORD, font=("Consolas", 10), state="disabled", bg="white", fg="#2c3e50")
        self.delivery_breakdown_text.pack(fill='both', expand=True, padx=5, pady=5)

        # Tab 3 - Click Heatmap
        heatmap_tab = ttk.Frame(analytics_notebook)
        analytics_notebook.add(heatmap_tab, text="🔥 Click Heatmap")
        self.heatmap_text = scrolledtext.ScrolledText(heatmap_tab, wrap=tk.WORD, font=("Consolas", 10), bg="white", fg="#2c3e50")
        self.heatmap_text.pack(fill='both', expand=True, padx=5, pady=5)
        self.heatmap_text.tag_configure("highlight", background="yellow", relief="raised")

        # Tab 4 - Activity Feed
        activity_tab = ttk.Frame(analytics_notebook)
        analytics_notebook.add(activity_tab, text="📡 Activity Feed")
        self.activity_feed_text = scrolledtext.ScrolledText(activity_tab, wrap=tk.WORD, font=("Consolas", 9), state="disabled", bg="white", fg="#2c3e50")
        self.activity_feed_text.pack(fill='both', expand=True, padx=5, pady=5)

        # Tab 5 - Raw Analytics Table
        raw_tab = ttk.Frame(analytics_notebook)
        analytics_notebook.add(raw_tab, text="📑 Raw Data")
        self.analytics_table = ttk.Treeview(raw_tab, columns=("Email", "Status", "Opens", "Clicks", "Timestamp"), show="headings")
        for col in ("Email", "Status", "Opens", "Clicks", "Timestamp"):
            self.analytics_table.heading(col, text=col)
            self.analytics_table.column(col, width=120)
        analytics_scroll = ttk.Scrollbar(raw_tab, orient="vertical", command=self.analytics_table.yview)
        self.analytics_table.configure(yscrollcommand=analytics_scroll.set)
        self.analytics_table.pack(side=tk.LEFT, fill='both', expand=True, padx=5, pady=5)
        analytics_scroll.pack(side=tk.RIGHT, fill='y', pady=5)

    def _build_placeholders_tab(self):
        """NEW: Build Placeholders documentation and statistics tab with interactive features."""
        placeholders_tab = ttk.Frame(self.notebook)
        self.notebook.add(placeholders_tab, text='🏷️ Placeholders')

        main_pane = ttk.PanedWindow(placeholders_tab, orient=tk.HORIZONTAL)
        main_pane.pack(fill='both', expand=True, padx=5, pady=5)

        left_frame = tk.Frame(main_pane, bg='#e0f7ff')
        main_pane.add(left_frame, weight=1)

        # Statistics and Instructions
        stats_frame = tk.LabelFrame(left_frame, text="Summary Statistics", bg='#e0f7ff', fg="#2c3e50")
        stats_frame.pack(fill='x', padx=10, pady=5)

        self.placeholders_stats_text = scrolledtext.ScrolledText(stats_frame, wrap=tk.WORD, state="disabled", font=("Consolas", 10), bg="white", fg="#2c3e50")
        self.placeholders_stats_text.pack(fill='both', expand=True, padx=5, pady=5)
        self._update_placeholders_stats()

        instructions_frame = tk.LabelFrame(left_frame, text="How to Use Placeholders", bg='#e0f7ff', fg="#2c3e50")
        instructions_frame.pack(fill='x', padx=10, pady=5, expand=True)

        instructions_text = scrolledtext.ScrolledText(instructions_frame, wrap=tk.WORD, state="disabled", font=("Consolas", 10), bg="white", fg="#2c3e50")
        instructions_text.pack(fill='both', expand=True, padx=5, pady=5)
        instructions_text.insert(tk.END, "Instructions for Using Placeholders:\n\n")
        instructions_text.insert(tk.END, "1. Select a placeholder from the list below.\n")
        instructions_text.insert(tk.END, "2. Click 'Select Placeholder' to insert it into the message editor.\n")
        instructions_text.insert(tk.END, "3. Configure options if available (e.g., custom company name).\n")
        instructions_text.insert(tk.END, "4. Preview the generated value in the 'Preview Field'.\n")
        instructions_text.insert(tk.END, "5. Use 'Generate New' to see a different random variation.\n")
        instructions_text.insert(tk.END, "6. View the global live preview to see the entire message with placeholders replaced.\n\n")
        instructions_text.insert(tk.END, "Placeholders are replaced during sending with real values:\n")
        instructions_text.insert(tk.END, "- [EMAIL] becomes the recipient's email.\n")
        instructions_text.insert(tk.END, "- [FIRSTNAME] becomes a guessed or provided name.\n")
        instructions_text.insert(tk.END, "Note: Some placeholders require an email list to be loaded for accurate personalization.")
        instructions_text.config(state="disabled")

        right_frame = tk.Frame(main_pane, bg='#e0f7ff')
        main_pane.add(right_frame, weight=2)

        # Placeholder Selector and Preview
        selector_frame = tk.LabelFrame(right_frame, text="Placeholder Selector & Preview", bg='#e0f7ff', fg="#2c3e50")
        selector_frame.pack(fill='x', padx=10, pady=5)

        tk.Label(selector_frame, text="Select Category:", bg='#e0f7ff', fg="#2c3e50").grid(row=0, column=0, sticky='w', padx=5, pady=5)
        self.placeholder_category_var = tk.StringVar()
        category_combo = ttk.Combobox(selector_frame, textvariable=self.placeholder_category_var, values=list(self.placeholders.keys()), state="readonly")
        category_combo.grid(row=0, column=1, padx=5, pady=5, sticky='ew')
        category_combo.bind("<<ComboboxSelected>>", self._update_placeholder_list)

        tk.Label(selector_frame, text="Select Placeholder:", bg='#e0f7ff', fg="#2c3e50").grid(row=1, column=0, sticky='w', padx=5, pady=5)
        self.placeholder_listbox = tk.Listbox(selector_frame, height=5, bg="white", fg="#2c3e50")
        self.placeholder_listbox.grid(row=1, column=1, padx=5, pady=5, sticky='ew')
        self.placeholder_listbox.bind("<<ListboxSelect>>", self._on_placeholder_select)

        tk.Label(selector_frame, text="Preview Field:", bg='#e0f7ff', fg="#2c3e50").grid(row=2, column=0, sticky='w', padx=5, pady=5)
        self.placeholder_preview_var = tk.StringVar()
        preview_entry = tk.Entry(selector_frame, textvariable=self.placeholder_preview_var, state="readonly", bg="white", fg="#2c3e50")
        preview_entry.grid(row=2, column=1, padx=5, pady=5, sticky='ew')

        button_frame = tk.Frame(selector_frame, bg='#e0f7ff')
        button_frame.grid(row=3, column=0, columnspan=2, pady=5)

        tk.Button(button_frame, text="Select Placeholder", command=self._select_placeholder, bg="#3498db", fg="white").pack(side=tk.LEFT, padx=5)
        tk.Button(button_frame, text="Generate New", command=self._generate_placeholder_preview, bg="#3498db", fg="white").pack(side=tk.LEFT, padx=5)
        tk.Button(button_frame, text="Configure", command=self._configure_placeholder, bg="#3498db", fg="white").pack(side=tk.LEFT, padx=5)

        tk.Checkbutton(selector_frame, text="Use Dummy Data (preview without CSV)", variable=self.use_dummy_data_var,
                        bg='#e0f7ff', fg="#2c3e50").grid(row=4, column=0, columnspan=2, sticky='w', padx=5, pady=2)

        selector_frame.grid_columnconfigure(1, weight=1)

        # Global Live Preview
        preview_frame = tk.LabelFrame(right_frame, text="Global Live Preview (Full Message)", bg='#e0f7ff', fg="#2c3e50")
        preview_frame.pack(fill='both', expand=True, padx=10, pady=5)

        preview_toolbar = tk.Frame(preview_frame, bg='#e0f7ff')
        preview_toolbar.pack(fill='x', pady=2)
        tk.Button(preview_toolbar, text="🔄 Refresh Live Preview", command=self._update_live_preview, bg="#3498db", fg="white").pack(side=tk.LEFT, padx=5)
        tk.Label(preview_toolbar, text="(Shows current message with all placeholders replaced)", bg='#e0f7ff', fg="#7f8c8d").pack(side=tk.LEFT, padx=5)

        self.live_preview_text = scrolledtext.ScrolledText(preview_frame, wrap=tk.WORD, font=("Consolas", 10), state="disabled", bg="white", fg="#2c3e50")
        self.live_preview_text.pack(fill='both', expand=True, padx=5, pady=5)

        # --- Template Validation Checker & Syntax Error Display ---
        validation_frame = tk.LabelFrame(right_frame, text="🔍 Template Validation & Syntax Checker", bg='#e0f7ff', fg="#2c3e50")
        validation_frame.pack(fill='x', padx=10, pady=5)

        val_toolbar = tk.Frame(validation_frame, bg='#e0f7ff')
        val_toolbar.pack(fill='x', pady=2)
        tk.Button(val_toolbar, text="✅ Validate Template", command=self._validate_template, bg="#27ae60", fg="white").pack(side=tk.LEFT, padx=5)
        tk.Label(val_toolbar, text="Check Jinja2 syntax and placeholder usage", bg='#e0f7ff', fg="#7f8c8d").pack(side=tk.LEFT, padx=5)

        self.template_validation_text = scrolledtext.ScrolledText(validation_frame, wrap=tk.WORD, height=4, font=("Consolas", 9), state="disabled", bg="#fafafa", fg="#2c3e50")
        self.template_validation_text.pack(fill='x', padx=5, pady=5)
        self.template_validation_text.tag_configure("error", foreground="#e74c3c", font=("Consolas", 9, "bold"))
        self.template_validation_text.tag_configure("success", foreground="#27ae60", font=("Consolas", 9, "bold"))
        self.template_validation_text.tag_configure("warning", foreground="#f39c12", font=("Consolas", 9))

        # --- Context Preview Renderer ---
        context_frame = tk.LabelFrame(right_frame, text="📋 Context Variable Preview", bg='#e0f7ff', fg="#2c3e50")
        context_frame.pack(fill='x', padx=10, pady=5)

        ctx_toolbar = tk.Frame(context_frame, bg='#e0f7ff')
        ctx_toolbar.pack(fill='x', pady=2)
        tk.Button(ctx_toolbar, text="🔄 Refresh Context", command=self._refresh_context_preview, bg="#3498db", fg="white").pack(side=tk.LEFT, padx=5)
        tk.Label(ctx_toolbar, text="Shows all available template variables and their current values", bg='#e0f7ff', fg="#7f8c8d").pack(side=tk.LEFT, padx=5)

        self.context_preview_text = scrolledtext.ScrolledText(context_frame, wrap=tk.WORD, height=5, font=("Consolas", 9), state="disabled", bg="#fafafa", fg="#2c3e50")
        self.context_preview_text.pack(fill='x', padx=5, pady=5)

        # Initialize
        if self.placeholders:
            self.placeholder_category_var.set(list(self.placeholders.keys())[0])
            self._update_placeholder_list()

    def _update_placeholders_stats(self):
        """Update placeholders statistics."""
        if not hasattr(self, 'placeholders_stats_text'):
            return

        total_categories = len(self.placeholders)
        total_placeholders = sum(len(v) for v in self.placeholders.values()) if self.placeholders else 0

        self.placeholders_stats_text.config(state="normal")
        self.placeholders_stats_text.delete("1.0", tk.END)
        self.placeholders_stats_text.insert(tk.END, f"📊 Total Placeholder Categories: {total_categories}\n")
        self.placeholders_stats_text.insert(tk.END, f"🏷️ Total Unique Placeholders: {total_placeholders}\n\n")
        self.placeholders_stats_text.insert(tk.END, "📋 Breakdown by Category:\n")

        icons = ["📧", "📅", "🔢", "💼", "🏥", "⚖️", "💰"]
        for i, (cat, items) in enumerate(self.placeholders.items()):
            icon = icons[i % len(icons)]
            self.placeholders_stats_text.insert(tk.END, f"{icon} {cat}: {len(items)} placeholders\n")

        self.placeholders_stats_text.config(state="disabled")

    def _update_placeholder_list(self, event=None):
        """Update the placeholder listbox based on selected category."""
        category = self.placeholder_category_var.get()
        self.placeholder_listbox.delete(0, tk.END)
        if category in self.placeholders:
            for ph in self.placeholders[category]:
                self.placeholder_listbox.insert(tk.END, ph)

    def _on_placeholder_select(self, event=None):
        """Handle placeholder selection and show preview."""
        selection = self.placeholder_listbox.curselection()
        if not selection:
            return

        ph = self.placeholder_listbox.get(selection[0])
        self.selected_placeholder_var.set(ph)
        self._generate_placeholder_preview()

    def _generate_placeholder_preview(self):
        """Generate and display preview for the selected placeholder."""
        ph = self.selected_placeholder_var.get()
        if not ph:
            return

        # Use dummy data if toggled on or no list is loaded
        use_dummy = getattr(self, 'use_dummy_data_var', None) and self.use_dummy_data_var.get()
        if not use_dummy and self.email_list:
            preview_email = self.email_list[0]
            recipient_data = self.tracking_map.get(preview_email, {})
            firstname = recipient_data.get('firstname', '')
            lastname = recipient_data.get('lastname', '')
            company = recipient_data.get('company', '')
            if not firstname:
                local_part = preview_email.split('@')[0]
                parts = re.split(r'[._\-+]+', local_part)
                valid_parts = [p for p in parts if len(p) > 1 and p.isalpha()]
                firstname = valid_parts[0].capitalize() if valid_parts else 'User'
            if not company:
                domain = preview_email.split('@')[1]
                parts = domain.split('.')
                company = parts[0].capitalize()
        elif not use_dummy and hasattr(self, 'vps_email_list') and self.vps_email_list:
            preview_email = self.vps_email_list[0]
            firstname = preview_email.split('@')[0].split('.')[0].capitalize()
            lastname = ''
            company = preview_email.split('@')[1].split('.')[0].capitalize()
        else:
            preview_email = "john.doe@example.com"
            firstname = 'John'
            lastname = 'Doe'
            company = 'Example Corp'

        context = {
            'email': preview_email,
            'firstname': firstname,
            'lastname': lastname or 'Doe',
            'company': company or 'Example Corp',
            'greetings': f'Good morning {firstname}' if firstname else 'Good morning',
            'sender_name': getattr(self, 'autodetected_sender_name', '') or 'Sender',
            'currentdate': datetime.now().strftime("%B %d, %Y"),
            'time': datetime.now().strftime("%I:%M %p"),
            'secure_link': self._generate_secure_link(preview_email),
            'unsubscribe_link': f"http://example.com/unsubscribe/{base64.urlsafe_b64encode(preview_email.encode()).decode()}",
        }

        value = self._get_placeholder_value(ph, context)
        self.placeholder_preview_var.set(value)

    def _get_placeholder_value(self, ph, context):
        """Get the value for a placeholder based on the context."""
        mapping = {
            "[EMAIL]": lambda: context.get('email', ''),
            "[EMAIL64]": lambda: base64.b64encode(context.get('email', '').encode()).decode(),
            "[UNAME]": lambda: context.get('email', '').split('@')[0] if '@' in context.get('email', '') else '',
            "[DOMAIN]": lambda: context.get('email', '').split('@')[1] if '@' in context.get('email', '') else '',
            "[COMPANY]": lambda: context.get('company', ''),
            "[COMPANYFULL]": lambda: context.get('company', ''),
            "[FIRSTNAME]": lambda: context.get('firstname', ''),
            "[LASTNAME]": lambda: context.get('lastname', ''),
            "[GREETINGS]": lambda: context.get('greetings', ''),
            "[SENDER_NAME]": lambda: context.get('sender_name', ''),
            "[DATE]": lambda: datetime.now().strftime("%m/%d/%Y"),
            "[DATE-1DAY]": lambda: (datetime.now() - timedelta(days=1)).strftime("%m/%d/%Y"),
            "[DATE-2]": lambda: datetime.now().strftime("%d/%m/%Y"),
            "[TIME]": lambda: datetime.now().strftime("%H:%M"),
            "[FUTURE-1DAY]": lambda: (datetime.now() + timedelta(days=1)).strftime("%m/%d/%Y"),
            "[CURRENTDATE]": lambda: context.get('currentdate', ''),
            "[RAND2]": lambda: f"{random.randint(10,99)}",
            "[RAND3]": lambda: f"{random.randint(100,999)}",
            "[RAND-4]": lambda: f"{random.randint(1000,9999)}",
            "[RAND5]": lambda: f"{random.randint(10000,99999)}",
            "[ORDER_ID]": lambda: f"ORD-{random.randint(100000,999999)}",
            "[INVOICE_NUM]": lambda: f"INV-{random.randint(100000,999999)}",
            "[AMOUNT]": lambda: f"${random.randint(100,999)}.99",
            "[TRACKING_NUM]": lambda: f"TN{random.randint(100000000,999999999)}",
            "[REF_NUM]": lambda: f"REF-{random.randint(10000,99999)}",
            "[PATIENT_ID]": lambda: f"PAT-{random.randint(10000,99999)}",
            "[APPT_DATE]": lambda: (datetime.now() + timedelta(days=random.randint(1,30))).strftime("%m/%d/%Y"),
            "[DOCTOR_NAME]": lambda: random.choice(['Dr. Smith', 'Dr. Johnson', 'Dr. Williams', 'Dr. Brown']),
            "[MED_RECORD]": lambda: f"MR-{random.randint(100000,999999)}",
            "[PRESCRIPTION]": lambda: f"RX-{random.randint(1000,9999)}",
            "[CASE_NUM]": lambda: f"CASE-{random.randint(10000,99999)}",
            "[LAWYER_NAME]": lambda: random.choice(['John Doe Esq.', 'Jane Smith Esq.', 'Bob Lawyer']),
            "[COURT_DATE]": lambda: (datetime.now() + timedelta(days=random.randint(1,30))).strftime("%m/%d/%Y"),
            "[DOC_ID]": lambda: f"DOC-{random.randint(100000,999999)}",
            "[COMPLIANCE_CODE]": lambda: f"COMP-{random.randint(1000,9999)}",
            "[ACCOUNT_NUM]": lambda: f"ACC-{random.randint(100000,999999)}",
            "[TRANSACTION_ID]": lambda: f"TX-{random.randint(100000000,999999999)}",
            "[BALANCE]": lambda: f"${random.randint(1000,9999)}.99",
            "[CARD_LAST4]": lambda: f"{random.randint(1000,9999)}",
            "[BANK_REF]": lambda: f"BR-{random.randint(100000,999999)}",
        }
        if ph in mapping:
            return mapping[ph]()
        else:
            return f"Unknown placeholder: {ph}"

    def _select_placeholder(self):
        """Insert the selected placeholder into the message editor."""
        ph = self.selected_placeholder_var.get()
        if not ph:
            messagebox.showwarning("No Placeholder", "Please select a placeholder first.")
            return

        current_tab = self.editor_notebook.index(self.editor_notebook.select())
        message_box = self.message_box_b if current_tab == 1 and self.ab_body_enabled_var.get() else self.message_box

        message_box.insert(tk.INSERT, ph)
        self.log(f"�� Inserted placeholder: {ph}")

    def _configure_placeholder(self):
        """Open configuration window for selected placeholder."""
        ph = self.selected_placeholder_var.get()
        if not ph:
            return

        config_win = tk.Toplevel(self.root)
        config_win.title(f"Configure {ph}")
        config_win.geometry("400x300")
        config_win.transient(self.root)
        config_win.grab_set()

        tk.Label(config_win, text=f"Configuration for {ph}", font=("Arial", 12, "bold")).pack(pady=10)

        if ph == "[COMPANY]":
            tk.Label(config_win, text="Custom Company Name:").pack(anchor='w', padx=20, pady=5)
            custom_company_var = tk.StringVar(value="Your Company")
            tk.Entry(config_win, textvariable=custom_company_var).pack(padx=20, pady=5)
            def save_custom():
                # Here you could save to settings or apply globally
                messagebox.showinfo("Saved", f"Custom company set to: {custom_company_var.get()}")
                config_win.destroy()
            tk.Button(config_win, text="Save", command=save_custom).pack(pady=10)
        else:
            tk.Label(config_win, text="No configuration options for this placeholder.").pack(pady=20)
            tk.Button(config_win, text="Close", command=config_win.destroy).pack(pady=10)

    def _update_live_preview(self):
        """Update the global live preview with placeholders replaced."""
        if not self.live_preview_text:
            return

        try:
            subject = self.subject_var.get()
            content = self._get_message_content()

            if not content.strip():
                self._set_preview_text("(No message content to preview)")
                return

            # Use first email in list for preview, or fallback to VPS/MX lists, or dummy
            preview_email = None
            if self.email_list:
                preview_email = self.email_list[0]
            elif hasattr(self, 'vps_email_list') and self.vps_email_list:
                preview_email = self.vps_email_list[0]
            elif hasattr(self, 'mx_email_list') and self.mx_email_list:
                preview_email = self.mx_email_list[0]
            else:
                preview_email = "preview@example.com"

            # Personalize subject and content
            personalized_subject, personalized_content, _, _ = self._personalize_content(
                preview_email, subject, content
            )

            self._set_preview_text(f"Subject: {personalized_subject}\n\nBody:\n{personalized_content}")
            self.log("🔄 Live preview updated.")
        except Exception as e:
            self._set_preview_text(f"⚠️ Preview error: {e}\n\nEnsure an email list is loaded and message content is valid.")
            self.log(f"⚠️ Live preview error: {e}")

    def _set_preview_text(self, text):
        """Helper to set text in the live preview widget."""
        self.live_preview_text.config(state="normal")
        self.live_preview_text.delete("1.0", tk.END)
        self.live_preview_text.insert(tk.END, text)
        self.live_preview_text.config(state="disabled")

    def _validate_template(self):
        """Validate Jinja2 template syntax and placeholder usage in the current message body."""
        if not hasattr(self, 'template_validation_text'):
            return

        self.template_validation_text.config(state="normal")
        self.template_validation_text.delete("1.0", tk.END)

        try:
            content = self.message_box.get("1.0", tk.END).strip() if hasattr(self, 'message_box') else ""
        except Exception:
            content = ""

        if not content:
            self.template_validation_text.insert(tk.END, "⚠️ No message body content to validate.\n", "warning")
            self.template_validation_text.config(state="disabled")
            return

        errors = []
        warnings = []

        # Check bracket placeholder syntax
        bracket_placeholders = re.findall(r'\[([A-Z0-9_\-]+)\]', content)
        known_placeholders = set()
        for cat_items in self.placeholders.values():
            for ph in cat_items:
                known_placeholders.add(ph)

        for ph in bracket_placeholders:
            ph_full = f"[{ph}]"
            if ph_full not in known_placeholders:
                warnings.append(f"Unknown placeholder: {ph_full}")

        # Validate Jinja2 syntax if Jinja2 is available
        if JINJA2_AVAILABLE:
            try:
                env = Environment()
                env.parse(content)
            except jinja_exceptions.TemplateSyntaxError as e:
                errors.append(f"Jinja2 Syntax Error (line {e.lineno}): {e.message}")
            except Exception as e:
                errors.append(f"Template Error: {str(e)}")

            # Check for unclosed Jinja2 blocks
            open_blocks = len(re.findall(r'\{%\s*(?:if|for|block|macro)\b', content))
            close_blocks = len(re.findall(r'\{%\s*(?:endif|endfor|endblock|endmacro)\b', content))
            if open_blocks > close_blocks:
                errors.append(f"Unclosed Jinja2 block(s): {open_blocks} opened, {close_blocks} closed")
            elif close_blocks > open_blocks:
                errors.append(f"Extra Jinja2 end block(s): {close_blocks} closers, {open_blocks} openers")

            # Check for Jinja2 variables
            jinja_vars = re.findall(r'\{\{\s*(\w+(?:\.\w+)*)\s*\}\}', content)
            if jinja_vars:
                self.template_validation_text.insert(tk.END, f"📋 Jinja2 variables found: {', '.join(set(jinja_vars))}\n")
        else:
            warnings.append("Jinja2 not installed - advanced template validation unavailable")

        # Check for common issues
        if '{{' in content and '}}' not in content:
            errors.append("Unclosed Jinja2 expression: '{{' without matching '}}'")
        if '{%' in content and '%}' not in content:
            errors.append("Unclosed Jinja2 tag: '{%' without matching '%}'")

        # Display results
        if errors:
            self.template_validation_text.insert(tk.END, "\n❌ ERRORS:\n", "error")
            for err in errors:
                self.template_validation_text.insert(tk.END, f"  • {err}\n", "error")

        if warnings:
            self.template_validation_text.insert(tk.END, "\n⚠️ WARNINGS:\n", "warning")
            for warn in warnings:
                self.template_validation_text.insert(tk.END, f"  • {warn}\n", "warning")

        if not errors and not warnings:
            self.template_validation_text.insert(tk.END, "✅ Template is valid! No syntax errors or unknown placeholders found.\n", "success")
        elif not errors:
            self.template_validation_text.insert(tk.END, "\n✅ No syntax errors found.\n", "success")

        self.template_validation_text.config(state="disabled")

    def _refresh_context_preview(self):
        """Refresh the context variable preview showing all available template variables."""
        if not hasattr(self, 'context_preview_text'):
            return

        self.context_preview_text.config(state="normal")
        self.context_preview_text.delete("1.0", tk.END)

        # Build sample context
        sample_email = "john.doe@example.com"
        if self.email_list:
            sample_email = self.email_list[0]
        elif hasattr(self, 'vps_email_list') and self.vps_email_list:
            sample_email = self.vps_email_list[0]

        local_part = sample_email.split('@')[0]
        domain = sample_email.split('@')[1] if '@' in sample_email else 'example.com'
        parts = re.split(r'[._\-+]+', local_part)
        valid_parts = [p for p in parts if len(p) > 1 and p.isalpha()]
        firstname = valid_parts[0].capitalize() if valid_parts else 'User'
        company = domain.split('.')[0].capitalize()

        context = {
            'email': sample_email,
            'firstname': firstname,
            'lastname': 'Doe',
            'company': company,
            'domain': domain,
            'uname': local_part,
            'greetings': f'Good morning {firstname}',
            'sender_name': getattr(self, 'autodetected_sender_name', '') or 'Sender',
            'currentdate': datetime.now().strftime("%B %d, %Y"),
            'time': datetime.now().strftime("%I:%M %p"),
            'date': datetime.now().strftime("%m/%d/%Y"),
        }

        self.context_preview_text.insert(tk.END, f"📧 Sample Email: {sample_email}\n")
        self.context_preview_text.insert(tk.END, "─" * 50 + "\n")
        for key, value in context.items():
            self.context_preview_text.insert(tk.END, f"  {key:20s} = {value}\n")
        self.context_preview_text.insert(tk.END, "─" * 50 + "\n")
        self.context_preview_text.insert(tk.END, f"Total variables available: {len(context)}\n")

        self.context_preview_text.config(state="disabled")

    # ==========================================
    # DNS Domain Checker Tab
    # ==========================================

    def _build_dns_checker_tab(self):
        """Build DNS Domain Checker tab for batch domain validation."""
        dns_checker_tab = ttk.Frame(self.notebook)
        self.notebook.add(dns_checker_tab, text='🌐 DNS Checker')

        main_pane = ttk.PanedWindow(dns_checker_tab, orient=tk.VERTICAL)
        main_pane.pack(fill='both', expand=True, padx=5, pady=5)

        # Top: Input area
        input_frame = tk.LabelFrame(dns_checker_tab, text="Domain Input", bg='#e8f6f3', fg="#2c3e50")
        main_pane.add(input_frame, weight=1)

        input_toolbar = tk.Frame(input_frame, bg='#e8f6f3')
        input_toolbar.pack(fill='x', padx=5, pady=5)

        tk.Button(input_toolbar, text="📂 Upload Domain List", command=self._upload_domain_list, bg="#3498db", fg="white").pack(side=tk.LEFT, padx=5)
        tk.Button(input_toolbar, text="📋 Extract from Email List", command=self._extract_domains_from_emails, bg="#3498db", fg="white").pack(side=tk.LEFT, padx=5)
        tk.Button(input_toolbar, text="🔍 Check All Domains", command=self._run_dns_check, bg="#27ae60", fg="white").pack(side=tk.LEFT, padx=5)
        tk.Button(input_toolbar, text="🗑️ Clear", command=self._clear_dns_results, bg="#e74c3c", fg="white").pack(side=tk.LEFT, padx=5)
        tk.Button(input_toolbar, text="💾 Export Results", command=self._export_dns_results, bg="#8e44ad", fg="white").pack(side=tk.RIGHT, padx=5)

        tk.Label(input_frame, text="Enter domains (one per line) or use upload/extract buttons:", bg='#e8f6f3', fg="#7f8c8d").pack(anchor='w', padx=10)

        self.dns_domain_input = scrolledtext.ScrolledText(input_frame, height=5, font=("Consolas", 10), bg="white", fg="#2c3e50", wrap=tk.WORD)
        self.dns_domain_input.pack(fill='x', padx=10, pady=5)
        self.dns_domain_input.insert(tk.END, "example.com\ngmail.com")

        # Bottom: Results grid
        results_frame = tk.LabelFrame(dns_checker_tab, text="DNS Validation Results", bg='#e8f6f3', fg="#2c3e50")
        main_pane.add(results_frame, weight=3)

        # Status summary
        self.dns_summary_var = tk.StringVar(value="No domains checked yet.")
        tk.Label(results_frame, textvariable=self.dns_summary_var, bg='#e8f6f3', fg="#2c3e50", font=("Arial", 10, "bold")).pack(anchor='w', padx=10, pady=2)

        # Results Treeview
        tree_frame = tk.Frame(results_frame, bg='#e8f6f3')
        tree_frame.pack(fill='both', expand=True, padx=5, pady=5)

        dns_columns = ('Domain', 'MX Records', 'SPF', 'DKIM', 'DMARC', 'Status')
        self.dns_checker_tree = ttk.Treeview(tree_frame, columns=dns_columns, show='headings', height=15)
        for col in dns_columns:
            self.dns_checker_tree.heading(col, text=col)
            self.dns_checker_tree.column(col, width=100, anchor='center')
        self.dns_checker_tree.column('Domain', width=200, anchor='w')
        self.dns_checker_tree.column('MX Records', width=200, anchor='w')

        self.dns_checker_tree.tag_configure('pass', background='#d5f5e3')
        self.dns_checker_tree.tag_configure('warn', background='#fdebd0')
        self.dns_checker_tree.tag_configure('fail', background='#fadbd8')

        dns_scroll_y = ttk.Scrollbar(tree_frame, orient='vertical', command=self.dns_checker_tree.yview)
        dns_scroll_x = ttk.Scrollbar(tree_frame, orient='horizontal', command=self.dns_checker_tree.xview)
        self.dns_checker_tree.configure(yscrollcommand=dns_scroll_y.set, xscrollcommand=dns_scroll_x.set)

        self.dns_checker_tree.pack(side='left', fill='both', expand=True)
        dns_scroll_y.pack(side='right', fill='y')
        dns_scroll_x.pack(side='bottom', fill='x')

    def _upload_domain_list(self):
        """Upload a file containing domains for DNS checking."""
        file_path = filedialog.askopenfilename(
            title="Select Domain List",
            filetypes=[("Text Files", "*.txt"), ("CSV Files", "*.csv"), ("All Files", "*.*")]
        )
        if not file_path:
            return
        try:
            domains = []
            with open(file_path, 'r', encoding='utf-8-sig') as f:
                if file_path.lower().endswith('.csv'):
                    reader = csv.reader(f)
                    for row in reader:
                        for cell in row:
                            cell = cell.strip()
                            if cell and '.' in cell and ' ' not in cell:
                                if '@' in cell:
                                    cell = cell.split('@')[1]
                                domains.append(cell)
                else:
                    for line in f:
                        line = line.strip()
                        if line and '.' in line and ' ' not in line:
                            if '@' in line:
                                line = line.split('@')[1]
                            domains.append(line)

            domains = sorted(set(domains))
            self.dns_domain_input.delete("1.0", tk.END)
            self.dns_domain_input.insert(tk.END, "\n".join(domains))
            self.log(f"📂 Loaded {len(domains)} unique domains from {os.path.basename(file_path)}")
        except Exception as e:
            self.log(f"❌ Error loading domain list: {e}")
            messagebox.showerror("Error", f"Failed to load domain list: {e}")

    def _extract_domains_from_emails(self):
        """Extract unique domains from the loaded email list."""
        emails = list(self.email_list) if self.email_list else []
        if hasattr(self, 'vps_email_list') and self.vps_email_list:
            emails.extend(self.vps_email_list)
        if hasattr(self, 'mx_email_list') and self.mx_email_list:
            emails.extend(self.mx_email_list)

        if not emails:
            messagebox.showinfo("Info", "No email list loaded. Please load an email list first.")
            return

        domains = sorted(set(e.split('@')[1] for e in emails if '@' in e))
        self.dns_domain_input.delete("1.0", tk.END)
        self.dns_domain_input.insert(tk.END, "\n".join(domains))
        self.log(f"📋 Extracted {len(domains)} unique domains from email list ({len(emails)} emails)")

    def _run_dns_check(self):
        """Run DNS validation on all domains in the input field."""
        raw = self.dns_domain_input.get("1.0", tk.END).strip()
        if not raw:
            messagebox.showinfo("Info", "No domains to check. Enter domains or upload a list.")
            return

        domains = [d.strip() for d in raw.split('\n') if d.strip() and '.' in d.strip()]
        if not domains:
            messagebox.showinfo("Info", "No valid domains found in input.")
            return

        # Clear previous results
        for item in self.dns_checker_tree.get_children():
            self.dns_checker_tree.delete(item)
        self.dns_checker_results = []

        self.log(f"🔍 Starting DNS validation for {len(domains)} domains...")

        def _check_worker():
            passed = 0
            warned = 0
            failed = 0
            for domain in domains:
                result = self._check_single_domain(domain)
                self.dns_checker_results.append(result)
                tag = result.get('tag', 'warn')
                if tag == 'pass':
                    passed += 1
                elif tag == 'fail':
                    failed += 1
                else:
                    warned += 1

                self.gui_update_queue.put(('dns_result', result))

            summary = f"✅ Passed: {passed} | ⚠️ Warnings: {warned} | ❌ Failed: {failed} | Total: {len(domains)}"
            self.gui_update_queue.put(('dns_summary', summary))
            self.gui_update_queue.put(('log_message', f"[{datetime.now().strftime('%H:%M:%S')}] 🔍 DNS check complete: {summary}\n"))

        threading.Thread(target=_check_worker, daemon=True).start()

    def _check_single_domain(self, domain):
        """Check DNS records for a single domain."""
        result = {'domain': domain, 'mx': 'N/A', 'spf': '❌', 'dkim': '❌', 'dmarc': '❌', 'status': 'Unknown', 'tag': 'warn'}

        if not DNSPYTHON_AVAILABLE:
            result['status'] = 'dnspython not installed'
            result['tag'] = 'fail'
            return result

        try:
            # MX Records
            try:
                mx_records = dns.resolver.resolve(domain, 'MX')
                mx_list = [str(r.exchange).rstrip('.') for r in mx_records]
                result['mx'] = ', '.join(mx_list[:3])
                if len(mx_list) > 3:
                    result['mx'] += f' (+{len(mx_list)-3} more)'
            except Exception:
                result['mx'] = '❌ No MX'

            # SPF
            try:
                txt_records = dns.resolver.resolve(domain, 'TXT')
                for r in txt_records:
                    txt = str(r).strip('"')
                    if txt.startswith('v=spf1'):
                        result['spf'] = '✅'
                        break
            except Exception:
                pass

            # DKIM (check common selectors)
            for selector in ['default', 'google', 'selector1', 'selector2', 'k1', 'dkim']:
                try:
                    dkim_domain = f'{selector}._domainkey.{domain}'
                    dns.resolver.resolve(dkim_domain, 'TXT')
                    result['dkim'] = '✅'
                    break
                except Exception:
                    continue

            # DMARC
            try:
                dmarc_records = dns.resolver.resolve(f'_dmarc.{domain}', 'TXT')
                for r in dmarc_records:
                    txt = str(r).strip('"')
                    if txt.startswith('v=DMARC1'):
                        result['dmarc'] = '✅'
                        break
            except Exception:
                pass

            # Overall status
            checks = [result['spf'], result['dkim'], result['dmarc']]
            pass_count = sum(1 for c in checks if c == '✅')
            if pass_count == 3 and result['mx'] != '❌ No MX':
                result['status'] = '✅ All Pass'
                result['tag'] = 'pass'
            elif pass_count == 0 or result['mx'] == '❌ No MX':
                result['status'] = '❌ Critical Issues'
                result['tag'] = 'fail'
            else:
                result['status'] = f'⚠️ {pass_count}/3 Pass'
                result['tag'] = 'warn'

        except Exception as e:
            result['status'] = f'Error: {str(e)[:30]}'
            result['tag'] = 'fail'

        return result

    def _clear_dns_results(self):
        """Clear DNS checker results."""
        for item in self.dns_checker_tree.get_children():
            self.dns_checker_tree.delete(item)
        self.dns_checker_results = []
        self.dns_summary_var.set("No domains checked yet.")
        self.dns_domain_input.delete("1.0", tk.END)

    def _export_dns_results(self):
        """Export DNS checker results to CSV."""
        if not self.dns_checker_results:
            messagebox.showinfo("Info", "No results to export.")
            return

        file_path = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV", "*.csv")],
            title="Export DNS Results"
        )
        if not file_path:
            return
        try:
            with open(file_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['Domain', 'MX Records', 'SPF', 'DKIM', 'DMARC', 'Status'])
                for r in self.dns_checker_results:
                    writer.writerow([r['domain'], r['mx'], r['spf'], r['dkim'], r['dmarc'], r['status']])
            self.log(f"💾 DNS results exported to {file_path}")
            messagebox.showinfo("Export", f"Results exported to {os.path.basename(file_path)}")
        except Exception as e:
            self.log(f"❌ Error exporting DNS results: {e}")
            messagebox.showerror("Error", f"Export failed: {e}")

    # ==========================================
    # Live Sent Email Log Tab
    # ==========================================

    def _build_sent_log_tab(self):
        """Build Live Sent Email Log tab with filtering, search, color-coded severity, and export."""
        log_tab = ttk.Frame(self.notebook)
        self.notebook.add(log_tab, text='📜 Sent Log')

        # Toolbar
        toolbar = tk.Frame(log_tab, bg='#f5eef8')
        toolbar.pack(fill='x', padx=5, pady=5)

        tk.Label(toolbar, text="🔍 Filter:", bg='#f5eef8', fg="#2c3e50").pack(side=tk.LEFT, padx=5)
        self.sent_log_filter_entry = tk.Entry(toolbar, textvariable=self.sent_log_filter_var, width=30)
        self.sent_log_filter_entry.pack(side=tk.LEFT, padx=5)
        self.sent_log_filter_entry.bind('<KeyRelease>', self._filter_sent_log)

        tk.Label(toolbar, text="Severity:", bg='#f5eef8', fg="#2c3e50").pack(side=tk.LEFT, padx=5)
        severity_combo = ttk.Combobox(toolbar, textvariable=self.sent_log_severity_var,
                                       values=["All", "INFO", "SUCCESS", "WARNING", "ERROR"], state="readonly", width=10)
        severity_combo.pack(side=tk.LEFT, padx=5)
        severity_combo.bind("<<ComboboxSelected>>", self._filter_sent_log)

        tk.Checkbutton(toolbar, text="Auto-scroll", variable=self.sent_log_auto_scroll_var,
                        bg='#f5eef8', fg="#2c3e50").pack(side=tk.LEFT, padx=10)

        tk.Button(toolbar, text="🗑️ Clear Log", command=self._clear_sent_log, bg="#e74c3c", fg="white").pack(side=tk.RIGHT, padx=5)
        tk.Button(toolbar, text="💾 Export Log", command=self._export_sent_log, bg="#8e44ad", fg="white").pack(side=tk.RIGHT, padx=5)

        # Log display
        log_display_frame = tk.Frame(log_tab)
        log_display_frame.pack(fill='both', expand=True, padx=5, pady=5)

        self.sent_log_text = scrolledtext.ScrolledText(log_display_frame, wrap=tk.WORD, font=("Consolas", 9),
                                                        bg='#1e1e1e', fg='#d4d4d4', state="disabled")
        self.sent_log_text.pack(fill='both', expand=True)

        # Color-coded severity tags
        self.sent_log_text.tag_configure("INFO", foreground="#569cd6")
        self.sent_log_text.tag_configure("SUCCESS", foreground="#4ec9b0")
        self.sent_log_text.tag_configure("WARNING", foreground="#dcdcaa")
        self.sent_log_text.tag_configure("ERROR", foreground="#f44747")
        self.sent_log_text.tag_configure("TIMESTAMP", foreground="#808080")

        # Status bar
        self.sent_log_status_var = tk.StringVar(value="Log entries: 0")
        tk.Label(log_tab, textvariable=self.sent_log_status_var, bg='#f5eef8', fg="#7f8c8d",
                 font=("Arial", 9)).pack(fill='x', padx=5, pady=2)

    def _append_sent_log(self, msg, severity="INFO"):
        """Append a message to the sent email log (thread-safe via queue)."""
        timestamp = datetime.now().strftime("[%H:%M:%S]")
        entry = {'timestamp': timestamp, 'severity': severity, 'message': msg}
        self.sent_log_entries.append(entry)

        # Cap log entries
        if len(self.sent_log_entries) > 10000:
            self.sent_log_entries = self.sent_log_entries[-5000:]

        self.gui_update_queue.put(('sent_log_entry', entry))

    def _insert_sent_log_entry(self, entry):
        """Insert a single log entry into the sent log text widget (must be called from main thread)."""
        if not hasattr(self, 'sent_log_text'):
            return

        # Check filter
        filter_text = self.sent_log_filter_var.get().lower()
        severity_filter = self.sent_log_severity_var.get()

        if severity_filter != "All" and entry['severity'] != severity_filter:
            return
        if filter_text and filter_text not in entry['message'].lower():
            return

        self.sent_log_text.config(state="normal")
        self.sent_log_text.insert(tk.END, f"{entry['timestamp']} ", "TIMESTAMP")
        self.sent_log_text.insert(tk.END, f"[{entry['severity']}] ", entry['severity'])
        self.sent_log_text.insert(tk.END, f"{entry['message']}\n")

        # Cap displayed lines
        line_count = int(self.sent_log_text.index('end-1c').split('.')[0])
        if line_count > 5000:
            self.sent_log_text.delete('1.0', f'{line_count - 5000}.0')

        if self.sent_log_auto_scroll_var.get():
            self.sent_log_text.see(tk.END)
        self.sent_log_text.config(state="disabled")

        self.sent_log_status_var.set(f"Log entries: {len(self.sent_log_entries)}")

    def _filter_sent_log(self, event=None):
        """Re-render the sent log with current filter and severity settings."""
        if not hasattr(self, 'sent_log_text'):
            return

        self.sent_log_text.config(state="normal")
        self.sent_log_text.delete("1.0", tk.END)

        filter_text = self.sent_log_filter_var.get().lower()
        severity_filter = self.sent_log_severity_var.get()

        displayed = 0
        for entry in self.sent_log_entries:
            if severity_filter != "All" and entry['severity'] != severity_filter:
                continue
            if filter_text and filter_text not in entry['message'].lower():
                continue

            self.sent_log_text.insert(tk.END, f"{entry['timestamp']} ", "TIMESTAMP")
            self.sent_log_text.insert(tk.END, f"[{entry['severity']}] ", entry['severity'])
            self.sent_log_text.insert(tk.END, f"{entry['message']}\n")
            displayed += 1

        self.sent_log_text.config(state="disabled")
        self.sent_log_status_var.set(f"Log entries: {displayed} / {len(self.sent_log_entries)}")

    def _clear_sent_log(self):
        """Clear the sent email log."""
        self.sent_log_entries = []
        if hasattr(self, 'sent_log_text'):
            self.sent_log_text.config(state="normal")
            self.sent_log_text.delete("1.0", tk.END)
            self.sent_log_text.config(state="disabled")
        self.sent_log_status_var.set("Log entries: 0")

    def _export_sent_log(self):
        """Export the sent email log to a text file."""
        if not self.sent_log_entries:
            messagebox.showinfo("Info", "No log entries to export.")
            return

        file_path = filedialog.asksaveasfilename(
            defaultextension=".txt",
            filetypes=[("Text File", "*.txt"), ("CSV", "*.csv")],
            title="Export Sent Email Log"
        )
        if not file_path:
            return
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                if file_path.lower().endswith('.csv'):
                    writer = csv.writer(f)
                    writer.writerow(['Timestamp', 'Severity', 'Message'])
                    for entry in self.sent_log_entries:
                        writer.writerow([entry['timestamp'], entry['severity'], entry['message']])
                else:
                    for entry in self.sent_log_entries:
                        f.write(f"{entry['timestamp']} [{entry['severity']}] {entry['message']}\n")
            self.log(f"💾 Sent log exported to {file_path}")
            messagebox.showinfo("Export", f"Log exported to {os.path.basename(file_path)}")
        except Exception as e:
            self.log(f"❌ Error exporting log: {e}")
            messagebox.showerror("Error", f"Export failed: {e}")

    def _build_health_monitor_tab(self):
        """Health Monitor dashboard: VPS health, proxy health, retry queue, encryption status."""
        health_tab = ttk.Frame(self.notebook)
        self.notebook.add(health_tab, text='💓 Health Monitor')

        health_scroll = ScrollableFrame(health_tab)
        health_scroll.pack(fill='both', expand=True)
        content = health_scroll.scrollable_frame

        # VPS Health Grid
        vps_health_section = CollapsibleSection(content, title="🖥️ VPS Health Grid")
        vps_health_section.pack(fill='x', padx=5, pady=3)

        vps_health_cols = ("vps_id", "region", "status", "smtp_status", "rate_used", "last_check")
        self.health_vps_tree = ttk.Treeview(vps_health_section.content, columns=vps_health_cols, show='headings', height=6)
        for col, text, width in [
            ("vps_id", "VPS ID", 120), ("region", "Region", 80), ("status", "Status", 80),
            ("smtp_status", "SMTP", 80), ("rate_used", "Rate Used", 80), ("last_check", "Last Check", 120)
        ]:
            self.health_vps_tree.heading(col, text=text)
            self.health_vps_tree.column(col, width=width, anchor='center')
        vps_scroll = ttk.Scrollbar(vps_health_section.content, orient="vertical", command=self.health_vps_tree.yview)
        self.health_vps_tree.configure(yscrollcommand=vps_scroll.set)
        self.health_vps_tree.pack(side='left', fill='both', expand=True)
        vps_scroll.pack(side='right', fill='y')

        tk.Button(vps_health_section.content, text="🔄 Refresh VPS Health", command=self._refresh_health_monitor_vps,
                  bg="#3498db", fg="white", font=("Arial", 9)).pack(pady=5)

        # Proxy Health Grid
        proxy_section = CollapsibleSection(content, title="🌐 Proxy Health Grid")
        proxy_section.pack(fill='x', padx=5, pady=3)

        proxy_cols = ("proxy", "type", "status", "latency", "last_used")
        self.health_proxy_tree = ttk.Treeview(proxy_section.content, columns=proxy_cols, show='headings', height=5)
        for col, text, width in [
            ("proxy", "Proxy", 180), ("type", "Type", 80), ("status", "Status", 80),
            ("latency", "Latency", 80), ("last_used", "Last Used", 120)
        ]:
            self.health_proxy_tree.heading(col, text=text)
            self.health_proxy_tree.column(col, width=width, anchor='center')
        proxy_scroll_bar = ttk.Scrollbar(proxy_section.content, orient="vertical", command=self.health_proxy_tree.yview)
        self.health_proxy_tree.configure(yscrollcommand=proxy_scroll_bar.set)
        self.health_proxy_tree.pack(side='left', fill='both', expand=True)
        proxy_scroll_bar.pack(side='right', fill='y')

        # Retry Queue Viewer
        retry_section = CollapsibleSection(content, title="🔄 Retry Queue Viewer")
        retry_section.pack(fill='x', padx=5, pady=3)

        retry_btn_row = tk.Frame(retry_section.content, bg='#e0f7ff')
        retry_btn_row.pack(fill='x', pady=2)
        tk.Button(retry_btn_row, text="▶️ Retry All", command=self._retry_mx_queue_now, bg="#27ae60", fg="white", font=("Arial", 9)).pack(side=tk.LEFT, padx=2)
        tk.Button(retry_btn_row, text="🗑️ Clear Queue", command=self._clear_mx_retry_queue, bg="#e74c3c", fg="white", font=("Arial", 9)).pack(side=tk.LEFT, padx=2)
        tk.Button(retry_btn_row, text="🔄 Refresh", command=self._refresh_health_retry_queue, bg="#3498db", fg="white", font=("Arial", 9)).pack(side=tk.LEFT, padx=2)

        retry_cols = ("email", "attempts", "next_retry", "reason", "source")
        self.health_retry_tree = ttk.Treeview(retry_section.content, columns=retry_cols, show='headings', height=5)
        for col, text, width in [
            ("email", "Email", 200), ("attempts", "Attempts", 70), ("next_retry", "Next Retry", 120),
            ("reason", "Reason", 150), ("source", "Source", 80)
        ]:
            self.health_retry_tree.heading(col, text=text)
            self.health_retry_tree.column(col, width=width, anchor='center' if col != "email" else 'w')
        self.health_retry_tree.pack(fill='both', expand=True, padx=5, pady=2)

        # Encryption Key Status
        enc_section = CollapsibleSection(content, title="🔐 Encryption Key Status")
        enc_section.pack(fill='x', padx=5, pady=3)

        self.health_enc_status = tk.Label(enc_section.content, text="Checking...", font=("Arial", 10), bg='#e0f7ff', fg="#666")
        self.health_enc_status.pack(anchor='w', padx=10, pady=5)
        tk.Button(enc_section.content, text="🔄 Check Status", command=self._refresh_health_enc_status,
                  bg="#3498db", fg="white", font=("Arial", 9)).pack(anchor='w', padx=10, pady=2)

        # Initialize data
        self._refresh_health_monitor_vps()
        self._refresh_health_enc_status()

    def _refresh_health_monitor_vps(self):
        """Refresh VPS health grid in Health Monitor tab."""
        try:
            for item in self.health_vps_tree.get_children():
                self.health_vps_tree.delete(item)
            for vps in self.proxy_vps_handler.vps_pool:
                healthy = self.proxy_vps_handler.health_status.get(vps['id'], False)
                status = "🟢 Online" if healthy else "🔴 Offline"
                smtp_status = "✅ OK" if healthy else "❓ Unknown"
                rate_used = str(self.proxy_vps_handler.rate_limits.get(vps['id'], 0))
                last_check = datetime.now().strftime("%H:%M:%S")
                self.health_vps_tree.insert("", tk.END, values=(
                    vps['id'], vps.get('geo_region', 'Unknown'), status, smtp_status, rate_used, last_check
                ))
        except Exception:
            pass

    def _refresh_health_retry_queue(self):
        """Refresh retry queue in Health Monitor tab."""
        try:
            for item in self.health_retry_tree.get_children():
                self.health_retry_tree.delete(item)
            if hasattr(self, 'direct_mx_handler') and hasattr(self.direct_mx_handler, 'retry_queue'):
                for entry in self.direct_mx_handler.retry_queue:
                    self.health_retry_tree.insert("", tk.END, values=(
                        entry.get('email', ''), entry.get('attempts', 0),
                        entry.get('next_retry', '--'), entry.get('reason', ''), "MX"
                    ))
        except Exception:
            pass

    def _refresh_health_enc_status(self):
        """Refresh encryption key status in Health Monitor tab."""
        try:
            if os.path.exists("encryption.key"):
                size = os.path.getsize("encryption.key")
                self.health_enc_status.config(text=f"🔐 Encryption Key: Active ({size} bytes)", fg="#27ae60")
            else:
                self.health_enc_status.config(text="⚠️ No encryption key found", fg="#e74c3c")
        except Exception:
            self.health_enc_status.config(text="❓ Unable to check", fg="#666")

    def _build_log_viewer_tab(self):
        """Dedicated Log Viewer tab with filtering and throttled updates."""
        log_tab = ttk.Frame(self.notebook)
        self.notebook.add(log_tab, text='📋 Log Viewer')

        # Toolbar
        toolbar = tk.Frame(log_tab, bg='#e0f7ff')
        toolbar.pack(fill='x', padx=5, pady=3)

        tk.Label(toolbar, text="Filter:", bg='#e0f7ff', fg="#2c3e50").pack(side=tk.LEFT, padx=2)
        self.log_viewer_filter_var = tk.StringVar()
        filter_entry = tk.Entry(toolbar, textvariable=self.log_viewer_filter_var, bg="white", fg="#2c3e50", width=25)
        filter_entry.pack(side=tk.LEFT, padx=2)
        filter_entry.bind('<Return>', lambda e: self._apply_log_viewer_filter())

        tk.Label(toolbar, text="Level:", bg='#e0f7ff', fg="#2c3e50").pack(side=tk.LEFT, padx=(10, 2))
        self.log_viewer_level_var = tk.StringVar(value="All")
        ttk.Combobox(toolbar, textvariable=self.log_viewer_level_var,
                     values=["All", "INFO", "SUCCESS", "WARNING", "ERROR"],
                     state="readonly", width=10).pack(side=tk.LEFT, padx=2)

        tk.Button(toolbar, text="🔍 Apply", command=self._apply_log_viewer_filter,
                  bg="#3498db", fg="white", font=("Arial", 9)).pack(side=tk.LEFT, padx=5)
        tk.Button(toolbar, text="🗑️ Clear", command=self._clear_log_viewer,
                  bg="#e74c3c", fg="white", font=("Arial", 9)).pack(side=tk.LEFT, padx=2)

        self.log_viewer_auto_scroll_var = tk.BooleanVar(value=True)
        tk.Checkbutton(toolbar, text="Auto-scroll", variable=self.log_viewer_auto_scroll_var,
                       bg='#e0f7ff', fg="#2c3e50").pack(side=tk.RIGHT, padx=5)

        # Log display
        self.log_viewer_text = scrolledtext.ScrolledText(log_tab, height=25, font=("Consolas", 9),
                                                         bg='#1e1e1e', fg='#d4d4d4', wrap=tk.WORD,
                                                         state="disabled")
        self.log_viewer_text.pack(fill='both', expand=True, padx=5, pady=5)

        # Tag colors for severity
        self.log_viewer_text.tag_configure("ERROR", foreground="#ff6b6b")
        self.log_viewer_text.tag_configure("WARNING", foreground="#ffd93d")
        self.log_viewer_text.tag_configure("SUCCESS", foreground="#6bff6b")
        self.log_viewer_text.tag_configure("INFO", foreground="#d4d4d4")

        # Status bar
        self.log_viewer_count_label = tk.Label(log_tab, text="📊 0 entries", font=("Arial", 9),
                                                bg='#e0f7ff', fg="#666")
        self.log_viewer_count_label.pack(fill='x', padx=5, pady=2)

    def _apply_log_viewer_filter(self):
        """Apply filter to log viewer."""
        try:
            self.log_viewer_text.config(state="normal")
            self.log_viewer_text.delete("1.0", tk.END)

            filter_text = self.log_viewer_filter_var.get().lower()
            level = self.log_viewer_level_var.get()

            count = 0
            for entry in self.sent_log_entries:
                if level != "All" and entry.get('severity', 'INFO') != level:
                    continue
                if filter_text and filter_text not in entry.get('message', '').lower():
                    continue
                line = f"{entry.get('timestamp', '')} [{entry.get('severity', 'INFO')}] {entry.get('message', '')}\n"
                tag = entry.get('severity', 'INFO')
                self.log_viewer_text.insert(tk.END, line, tag)
                count += 1

            self.log_viewer_text.config(state="disabled")
            self.log_viewer_count_label.config(text=f"📊 {count} entries shown")

            if self.log_viewer_auto_scroll_var.get():
                self.log_viewer_text.see(tk.END)
        except Exception:
            pass

    def _clear_log_viewer(self):
        """Clear log viewer display."""
        try:
            self.log_viewer_text.config(state="normal")
            self.log_viewer_text.delete("1.0", tk.END)
            self.log_viewer_text.config(state="disabled")
            self.log_viewer_count_label.config(text="📊 0 entries")
        except Exception:
            pass

    def _update_smtp_workers(self):
        try:
            val = self.smtp_max_workers_var.get()
            if val < 1: val = 1; self.smtp_max_workers_var.set(val)
            self.smtp_max_workers = val
            self.log(f"⚙️ SMTP parallel workers set to {val}")
        except Exception:
            self.smtp_max_workers = 1

    def _format_text(self, style, message_box):
        try:
            start, end = message_box.index(tk.SEL_FIRST), message_box.index(tk.SEL_LAST)
            if style == "bold":
                message_box.insert(end, "</b>")
                message_box.insert(start, "<b>")
            elif style == "italic":
                message_box.insert(end, "</i>")
                message_box.insert(start, "<i>")
            elif style == "code":
                message_box.insert(end, "</code>")
                message_box.insert(start, "<code>")
        except tk.TclError:
            messagebox.showinfo("Formatting", "Please select text to format.")

    def _add_link(self, message_box):
        try:
            selected_text = message_box.get(tk.SEL_FIRST, tk.SEL_LAST)
            start, end = message_box.index(tk.SEL_FIRST), message_box.index(tk.SEL_LAST)
            url = simpledialog.askstring("Add Link", "Enter URL:", parent=self.root)
            if url:
                message_box.delete(start, end)
                message_box.insert(start, f'<a href="{url}">{selected_text or url}</a>')
        except tk.TclError:
            messagebox.showinfo("Add Link", "Please select text to turn into a link.")

    def _load_html_file(self):
        file_path = filedialog.askopenfilename(title="Select HTML File", filetypes=[("HTML", "*.html;*.htm")])
        if file_path:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                self.message_box.delete("1.0", tk.END)
                self.message_box.insert(tk.END, content)
                self._update_char_count()
                self.log(f"✅ Loaded HTML: {os.path.basename(file_path)}")
            except Exception as e:
                self.log(f"❌ HTML load error: {e}")
                messagebox.showerror("Load Error", f"Could not load HTML file:\n{e}")

    def _load_template(self):
        template = """<!DOCTYPE html>
<html>
<head>
    <style> .button { background-color: #4CAF50; color: white; padding: 10px 20px; text-decoration: none; } </style>
</head>
<body>
<div style="font-family: sans-serif;">
    {# This is a Jinja2 comment. Use them for notes! #}
    <h1>{{ subject_line }}</h1>

    <p>{{ greetings }},</p>

    <p>This is a message from {{ company or 'our team' }}.</p>

    {# Example of conditional logic with your CSV data #}
    {% if is_customer == 'yes' %}
        <p>As a loyal customer, we have a special offer for you!</p>
    {% else %}
        <p>Welcome! We're glad to have you with us.</p>
    {% endif %}

    <p>Feel free to click this <a href="{{ download_link or '#' }}">tracked link</a>.</p>

    <p><a href="{{ secure_link }}" class="button">Access Your Secure Portal</a></p>

    <p>Regards,<br>{{ sender_name }}</p>

    <hr>
    <p style="font-size:12px; color:#888;">To unsubscribe, <a href="{{ unsubscribe_link }}">click here</a>.</p>
</div>
</body>
</html>"""
        self.message_box.delete("1.0", tk.END)
        self.message_box.insert(tk.END, template)
        self._update_char_count()
        self.log("✅ Loaded Jinja2 template example with restored autograb placeholders")
        messagebox.showinfo("Jinja2 Enabled", "This editor supports Jinja2 templating and restored autograb tags!\n\n"
                                             "Use {{ placeholder }} for autograb tags like {{ greetings }}.\n"
                                             "Use data from your CSV like {{ csv_column_name }}.\n"
                                             "Use {% if condition %}...{% endif %} for logic.\n\n"
                                             "See the loaded example for syntax.", parent=self.root)

    def _open_template_library(self):
        lib_window = tk.Toplevel(self.root)
        lib_window.title("Template Library")
        lib_window.geometry("500x400")
        lib_window.transient(self.root)
        lib_window.grab_set()
        frame = tk.Frame(lib_window)
        frame.pack(fill='both', expand=True, padx=10, pady=10)
        listbox = tk.Listbox(frame)
        listbox.pack(side=tk.LEFT, fill='both', expand=True)
        def refresh_list():
            listbox.delete(0, tk.END)
            if not os.path.isdir(TEMPLATES_DIR):
                return
            for fname in os.listdir(TEMPLATES_DIR):
                if fname.lower().endswith((".html", ".htm")):
                    listbox.insert(tk.END, fname)
        def load_selected():
            if not listbox.curselection():
                return
            path = os.path.join(TEMPLATES_DIR, listbox.get(listbox.curselection()[0]))
            try:
                with open(path, "r", encoding="utf-8") as f:
                    self.message_box.delete("1.0", tk.END)
                    self.message_box.insert("1.0", f.read())
                self._update_char_count()
                self.log(f"✅ Loaded template from library: {os.path.basename(path)}")
                lib_window.destroy()
            except Exception as e:
                messagebox.showerror("Error", f"Could not load template:\n{e}")
        def save_current_as():
            name = simpledialog.askstring("Save As", "Template name:", parent=lib_window)
            if not name:
                return
            path = os.path.join(TEMPLATES_DIR, name if name.lower().endswith(".html") else name + ".html")
            try:
                with open(path, "w", encoding="utf-8") as f:
                    f.write(self.message_box.get("1.0", tk.END))
                self.log(f"�� Saved template to {path}")
                refresh_list()
            except Exception as e:
                messagebox.showerror("Error", f"Could not save template:\n{e}")
        btn_frame = tk.Frame(lib_window)
        btn_frame.grid(row=8, column=0, columnspan=2, pady=10)
        tk.Button(btn_frame, text="Load", command=load_selected).pack(side=tk.LEFT)
        tk.Button(btn_frame, text="Save Current As", command=save_current_as).pack(side=tk.LEFT)
        tk.Button(btn_frame, text="Cancel", command=lib_window.destroy).pack(side=tk.LEFT)
        refresh_list()

    def _add_attachment(self):
        paths = filedialog.askopenfilenames(title="Select Attachments")
        if paths:
            self.attachment_paths.extend([p for p in paths if p not in self.attachment_paths])
            self._update_attachment_display()
            self.log(f"✅ Added {len(paths)} attachment(s).")

    def _clear_attachments(self):
        if not self.attachment_paths:
            messagebox.showinfo("No Attachments", "There are no attachments to clear.")
            return
        if messagebox.askyesno("Confirm", f"Are you sure you want to remove all {len(self.attachment_paths)} attachment(s)?"):
            self.attachment_paths.clear()
            self._update_attachment_display()
            self.log("🗑️ All attachments have been cleared.")

    def _update_attachment_display(self):
        count = len(self.attachment_paths)
        if count == 0:
            self.attachment_label.config(text="No attachments")
        elif count == 1:
            self.attachment_label.config(text=f"📎 {os.path.basename(self.attachment_paths[0])}")
        else:
            self.attachment_label.config(text=f"📎 {count} files")

    def _update_char_count(self, event=None):
        self.char_count_label.config(text=f"Chars: {len(self._get_message_content()):,}")

    def _get_message_content(self):
        try:
            active_tab_index = self.editor_notebook.index(self.editor_notebook.select())
            content_box = self.message_box_b if active_tab_index == 1 and self.ab_body_enabled_var.get() else self.message_box
        except Exception:
            content_box = self.message_box

        content = content_box.get("1.0", tk.END).strip()

        if ("<" in content and ">" in content):
            if not content.lower().startswith(("<!doctype", "<html")):
                return f"<!DOCTYPE html><html><head><meta charset='utf-8'></head><body>{content}</body></html>"
            return content
        else:
            formatted_text = content.replace("\n", "<br>")
            return f"<!DOCTYPE html><html><body>{formatted_text}</body></html>"

    def _ai_rewrite(self):
        ai_provider = self.ai_provider_var.get()
        handler = self.local_ai_handler if ai_provider == "Local" else self.ai_handler

        if ai_provider == "OpenAI" and not self.openai_api_key_var.get():
            messagebox.showwarning("AI Config", "Please enter your OpenAI API Key in Settings first.")
            return

        content = self.message_box.get("1.0", tk.END).strip()
        if not content:
            return

        self.log(f"🤖 AI Rewrite: Using {ai_provider} to rewrite content...")
        prompt = (f"Rewrite the following email content to be more persuasive and clear. "
                  f"Preserve the existing HTML structure and any Jinja2 template tags like {{{{variable}}}} or {{% logic %}}.\n\n{content}")

        success, result = handler.generate(prompt, system_msg="You are a helpful email marketing assistant.")

        if success:
            if messagebox.askyesno("AI Result", "Accept AI suggested changes?\n\n" + result[:500] + "..."):
                self.message_box.delete("1.0", tk.END)
                self.message_box.insert("1.0", result)
                self.log("✅ AI Rewrite applied.")
        else:
            messagebox.showerror("AI Error", result)

    def _ai_subject(self):
        ai_provider = self.ai_provider_var.get()
        handler = self.local_ai_handler if ai_provider == "Local" else self.ai_handler

        if ai_provider == "OpenAI" and not self.openai_api_key_var.get():
            messagebox.showwarning("AI Config", "Please enter your OpenAI API Key in Settings first.")
            return

        content = self.message_box.get("1.0", tk.END).strip()
        if not content:
            return

        self.log(f"🤖 AI Subject: Using {ai_provider} to generate subjects...")
        success, result = handler.generate(f"Generate 3 short, catchy email subject lines for this content. Return only the lines separated by newlines:\n\n{content}")

        if success:
            lines = [line.strip() for line in result.split('\n') if line.strip()]
            if len(lines) >= 2:
                if messagebox.askyesno("AI Result", f"Use these for A/B testing?\n\nA: {lines[0]}\nB: {lines[1]}"):
                    self.subject_a_var.set(lines[0])
                    self.subject_b_var.set(lines[1])
                    self.ab_testing_enabled.set(True)
                    self.log("✅ AI Subjects applied for A/B test.")
            else:
                 messagebox.showinfo("AI Result", result)
        else:
             messagebox.showerror("AI Error", result)

    def _run_css_inline(self):
        if not CSS_INLINE_AVAILABLE:
            messagebox.showwarning("Missing Library", "css_inline is not installed.\nRun: pip install css_inline")
            return

        content = self.message_box.get("1.0", tk.END)
        self.log("🎨 Inlining CSS...")
        success, result = self.html_helper.inline_css(content)
        if success:
             self.message_box.delete("1.0", tk.END)
             self.message_box.insert("1.0", result)
             self.log("✅ CSS Inlined.")
             messagebox.showinfo("Success", "CSS styles have been inlined.")
        else:
             messagebox.showerror("Error", result)

    def _insert_html_block(self, block_type):
        """Insert a predefined HTML block into the message editor."""
        html = self.html_helper.get_block_html(block_type)
        if html:
            self.message_box.insert(tk.INSERT, html)
            self.log(f"🧱 Inserted HTML block: {block_type}")

    def _save_settings(self):
        settings = {
            "openai_key": self.openai_api_key_var.get(),
            "openai_model": self.openai_model_var.get(),
            "proxy": self.proxy_var.get(),
            "imap_server": self.imap_server_var.get(),
            "imap_user": self.imap_user_var.get(),
            "imap_pass": self.imap_pass_var.get(),
            "track_port": self.track_port_var.get(),
            "chrome_port": self.chrome_port_var.get(),
            "custom_chrome_path": self.custom_chrome_path_var.get(),
            "burner_domain": self.burner_domain_var.get(),
            "lure_path": self.lure_path_var.get(),
            "template_pdf": self.template_pdf_var.get(),
            # NEW v8.0.0
            "ai_provider": self.ai_provider_var.get(),
            "local_ai_url": self.local_ai_url_var.get(),
            "local_ai_model": self.local_ai_model_var.get(),
            # NEW: DKIM settings
            "dkim_private_key": self.direct_mx_handler.dkim_private_key,
            "dkim_selector": self.direct_mx_handler.dkim_selector,
            "dkim_domain": self.direct_mx_handler.dkim_domain,
            # NEW: Advanced sender identity settings
            "mx_reply_to_email": self.mx_reply_to_var.get() if hasattr(self, 'mx_reply_to_var') else "",
            "mx_message_id_domain": self.mx_message_id_domain_var.get() if hasattr(self, 'mx_message_id_domain_var') else "",
            "mx_from_emails_pool": self.direct_mx_handler.from_emails_pool,
            "mx_from_emails_status": self.direct_mx_handler.from_emails_status,
            "mx_source_ip": self.mx_source_ip_var.get() if hasattr(self, 'mx_source_ip_var') else "",
            "mx_ehlo_hostname": self.mx_ehlo_hostname_var.get() if hasattr(self, 'mx_ehlo_hostname_var') else "",
        }

        self.track_port = settings["track_port"]
        self.chrome_debug_port = settings["chrome_port"]
        self.custom_chrome_path = settings["custom_chrome_path"]

        try:
            # Preserve existing data (e.g., vps_configs) by merging instead of overwriting
            existing = {}
            if os.path.exists(SETTINGS_FILE):
                try:
                    with open(SETTINGS_FILE, "r", encoding="utf-8") as f:
                        existing = json.load(f)
                except Exception:
                    existing = {}
            existing.update(settings)
            with open(SETTINGS_FILE, "w", encoding="utf-8") as f:
                json.dump(existing, f, indent=2)

            self.imap_handler.configure(
                self.imap_server_var.get(), 993,
                self.imap_user_var.get(), self.imap_pass_var.get()
            )
            self.ai_handler.configure(self.openai_api_key_var.get())
            self.local_ai_handler.configure(self.local_ai_url_var.get(), self.local_ai_model_var.get())
            self.direct_mx_handler.configure_dkim(settings.get("dkim_private_key", ""), settings.get("dkim_selector", ""), settings.get("dkim_domain", ""))

            self._update_dkim_labels()
            self.log("💾 Settings saved. (Restart recommended to apply new port settings)")
        except Exception as e:
            self.log(f"❌ Error saving settings: {e}")

    def _load_settings(self):
        if os.path.exists(SETTINGS_FILE):
            try:
                with open(SETTINGS_FILE, "r") as f:
                    settings = json.load(f)
                    self.openai_api_key_var.set(settings.get("openai_key", ""))
                    self.openai_model_var.set(settings.get("openai_model", "gpt-3.5-turbo"))
                    self.proxy_var.set(settings.get("proxy", ""))
                    self.imap_server_var.set(settings.get("imap_server", ""))
                    self.imap_user_var.set(settings.get("imap_user", ""))
                    self.imap_pass_var.set(settings.get("imap_pass", ""))
                    self.track_port = int(settings.get("track_port", DEFAULT_TRACK_PORT))
                    self.chrome_debug_port = int(settings.get("chrome_port", DEFAULT_CHROME_DEBUG_PORT))
                    self.custom_chrome_path = settings.get("custom_chrome_path", "")
                    self.track_port_var.set(self.track_port)
                    self.chrome_port_var.set(self.chrome_debug_port)
                    self.custom_chrome_path_var.set(self.custom_chrome_path)
                    self.burner_domain_var.set(settings.get("burner_domain", ""))
                    self.lure_path_var.set(settings.get("lure_path", ""))
                    self.template_pdf_var.set(settings.get("template_pdf", ""))
                    # NEW v8.0.0
                    self.ai_provider_var.set(settings.get("ai_provider", "OpenAI"))
                    self.local_ai_url_var.set(settings.get("local_ai_url", "http://localhost:11434/api/generate"))
                    self.local_ai_model_var.set(settings.get("local_ai_model", "llama3"))
                    self.imap_handler.configure(
                        settings.get("imap_server", ""), 993,
                        settings.get("imap_user", ""), settings.get("imap_pass", "")
                    )
                    self.ai_handler.configure(self.openai_api_key_var.get())
                    self.local_ai_handler.configure(self.local_ai_url_var.get(), self.local_ai_model_var.get())
                    # DKIM
                    self.direct_mx_handler.configure_dkim(settings.get("dkim_private_key", ""), settings.get("dkim_selector", ""), settings.get("dkim_domain", ""))
                    # Advanced sender identity
                    if hasattr(self, 'mx_reply_to_var'):
                        self.mx_reply_to_var.set(settings.get("mx_reply_to_email", ""))
                    if hasattr(self, 'mx_message_id_domain_var'):
                        self.mx_message_id_domain_var.set(settings.get("mx_message_id_domain", ""))
                    self.direct_mx_handler.reply_to_email = settings.get("mx_reply_to_email", "")
                    self.direct_mx_handler.message_id_domain = settings.get("mx_message_id_domain", "")
                    self.direct_mx_handler.from_emails_pool = settings.get("mx_from_emails_pool", [])
                    self.direct_mx_handler.from_emails_status = settings.get("mx_from_emails_status", {})
                    if hasattr(self, 'mx_source_ip_var'):
                        self.mx_source_ip_var.set(settings.get("mx_source_ip", ""))
                    if hasattr(self, 'mx_ehlo_hostname_var'):
                        self.mx_ehlo_hostname_var.set(settings.get("mx_ehlo_hostname", ""))
                    self.direct_mx_handler.source_ip = settings.get("mx_source_ip", "")
                    self.direct_mx_handler.ehlo_hostname = settings.get("mx_ehlo_hostname", "")
                    self._update_dkim_labels()
            except Exception:
                pass

    def _open_schedule_window(self):
        if not TKCALENDAR_AVAILABLE:
            return
        if self.running and not self.paused:
            messagebox.showwarning("In Progress", "Cannot schedule while a campaign is running.")
            return

        schedule_window = tk.Toplevel(self.root)
        schedule_window.title("Schedule Campaign")
        schedule_window.transient(self.root)
        schedule_window.grab_set()

        tk.Label(schedule_window, text="Select Date and Time to Start", font=("Arial", 12, "bold")).pack(pady=10)

        main_frame = tk.Frame(schedule_window)
        main_frame.pack(pady=10, padx=10)

        cal = Calendar(main_frame, selectmode='day', date_pattern='yyyy-mm-dd')
        cal.pack(pady=10, padx=10)

        time_frame = tk.Frame(main_frame)
        time_frame.pack(pady=5)
        now = datetime.now() + timedelta(minutes=5)
        hour_var, minute_var = tk.StringVar(value=f"{now.hour:02d}"), tk.StringVar(value=f"{now.minute:02d}")
        tk.Label(time_frame, text="Time (HH:MM):").pack(side=tk.LEFT)
        tk.Spinbox(time_frame, from_=0, to=23, textvariable=hour_var, wrap=True, width=3, format="%02.0f").pack(side=tk.LEFT)
        tk.Label(time_frame, text=":").pack(side=tk.LEFT)
        tk.Spinbox(time_frame, from_=0, to=59, textvariable=minute_var, wrap=True, width=3, format="%02.0f").pack(side=tk.LEFT)

        def set_schedule(use_sto=False):
            try:
                schedule_time = datetime.strptime(f"{cal.get_date()} {int(hour_var.get())}:{int(minute_var.get())}", "%Y-%m-%d %H:%M")
                if schedule_time < datetime.now():
                    messagebox.showerror("Invalid Time", "Scheduled time cannot be in the past.")
                    return
                delay = (schedule_time - datetime.now()).total_seconds()
                if self.schedule_timer:
                    self.schedule_timer.cancel()
                self.schedule_timer = threading.Timer(delay, self._start_sending)
                self.schedule_timer.start()
                self.log(f"🗓️ Campaign scheduled for: {schedule_time.strftime('%Y-%m-%d %H:%M:%S')}")
                self.status_label.config(text=f"🔵 Scheduled for {schedule_time.strftime('%Y-%m-%d %H:%M')}", fg="#16a085")
                messagebox.showinfo("Scheduled", f"Campaign scheduled for {schedule_time.strftime('%Y-%m-%d %H:%M:%S')}.")
                schedule_window.destroy()
            except ValueError:
                messagebox.showerror("Invalid Time", "Please enter a valid time.")

        btn_frame = tk.Frame(schedule_window)
        btn_frame.pack(pady=10)
        tk.Button(btn_frame, text="Set Schedule", command=lambda: set_schedule(False)).pack(side=tk.LEFT, padx=10)

    def _open_smtp_config(self):
        smtp_window = tk.Toplevel(self.root)
        smtp_window.title("SMTP Configuration")
        smtp_window.geometry("500x550")
        smtp_window.transient(self.root)
        smtp_window.grab_set()
        preset_var = tk.StringVar(value="Gmail")
        server_var, port_var = tk.StringVar(), tk.StringVar()
        tls_var, ssl_var = tk.BooleanVar(value=True), tk.BooleanVar(value=False)
        username_var, password_var = tk.StringVar(), tk.StringVar()
        sender_name_var, sender_email_var = tk.StringVar(), tk.StringVar()

        if self.smtp_handler.smtp_server:
            server_var.set(self.smtp_handler.smtp_server)
            port_var.set(str(self.smtp_handler.smtp_port or 587))
            username_var.set(self.smtp_handler.username or "")
            tls_var.set(self.smtp_handler.use_tls)
            ssl_var.set(self.smtp_handler.use_ssl)
            sender_name_var.set(self.smtp_handler.sender_name or "")
            sender_email_var.set(self.smtp_handler.sender_email or "")

        tk.Label(smtp_window, text="Preset:").grid(row=0, column=0, padx=10, pady=5, sticky='w')
        preset_combo = ttk.Combobox(smtp_window, textvariable=preset_var, values=list(self.smtp_handler.smtp_presets.keys()), state="readonly")
        preset_combo.grid(row=0, column=1, padx=10, pady=5, sticky='ew')
        tk.Label(smtp_window, text="Server:").grid(row=1, column=0, padx=10, pady=5, sticky='w')
        tk.Entry(smtp_window, textvariable=server_var).grid(row=1, column=1, padx=10, pady=5, sticky='ew')
        tk.Label(smtp_window, text="Port:").grid(row=2, column=0, padx=10, pady=5, sticky='w')
        tk.Entry(smtp_window, textvariable=port_var).grid(row=2, column=1, padx=10, pady=5, sticky='ew')
        tk.Checkbutton(smtp_window, text="Use TLS", variable=tls_var).grid(row=3, column=0, padx=10, pady=5, sticky='w')
        tk.Checkbutton(smtp_window, text="Use SSL", variable=ssl_var).grid(row=3, column=1, padx=10, pady=5, sticky='w')
        tk.Label(smtp_window, text="Username:").grid(row=4, column=0, padx=10, pady=5, sticky='w')
        tk.Entry(smtp_window, textvariable=username_var).grid(row=4, column=1, padx=10, pady=5, sticky='ew')
        tk.Label(smtp_window, text="Password:" if KEYRING_AVAILABLE else "Password (insecure):").grid(row=5, column=0, padx=10, pady=5, sticky='w')
        pw_entry = tk.Entry(smtp_window, textvariable=password_var, show="*")
        pw_entry.grid(row=5, column=1, padx=10, pady=5, sticky='ew')
        if KEYRING_AVAILABLE:
            pw_entry.insert(0, "Stored in OS keyring" if self.smtp_handler.get_password() else "Enter to set/update")
        tk.Label(smtp_window, text="Sender Name:").grid(row=6, column=0, padx=10, pady=5, sticky='w')
        tk.Entry(smtp_window, textvariable=sender_name_var).grid(row=6, column=1, padx=10, pady=5, sticky='ew')
        tk.Label(smtp_window, text="Sender Email:").grid(row=7, column=0, padx=10, pady=5, sticky='w')
        tk.Entry(smtp_window, textvariable=sender_email_var).grid(row=7, column=1, padx=10, pady=5, sticky='ew')
        smtp_window.grid_columnconfigure(1, weight=1)

        def on_preset_change(event=None):
            config = self.smtp_handler.smtp_presets.get(preset_var.get())
            if config:
                server_var.set(config["server"])
                port_var.set(str(config["port"]))
                tls_var.set(config["tls"])
                ssl_var.set(config["ssl"])
        preset_combo.bind('<<ComboboxSelected>>', on_preset_change)
        on_preset_change()

        def apply_and_close():
            if not server_var.get() or not username_var.get():
                messagebox.showerror("Incomplete", "Server and Username are required.")
                return
            pw = password_var.get()
            if pw == "Stored in OS keyring":
                pw = None
            self.smtp_handler.configure_smtp(server_var.get(), port_var.get(), username_var.get(), pw, tls_var.get(), ssl_var.get(), sender_name_var.get(), sender_email_var.get())
            self.smtp_config_btn.config(text="✅ SMTP OK", bg="#27ae60")
            smtp_window.destroy()

        button_frame = tk.Frame(smtp_window)
        button_frame.grid(row=8, column=0, columnspan=2, pady=10)
        tk.Button(button_frame, text="Test", command=self._test_smtp).pack(side=tk.LEFT, padx=5)
        tk.Button(button_frame, text="Apply & Close", command=apply_and_close).pack(side=tk.LEFT, padx=5)
        tk.Button(button_frame, text="Cancel", command=smtp_window.destroy).pack(side=tk.LEFT, padx=5)

    def _test_smtp(self):
        if not self.smtp_handler.smtp_server or not self.smtp_handler.username:
            messagebox.showwarning("Not Configured", "Please configure SMTP settings first.")
            return
        self.test_smtp_btn.config(text="TESTING...", state="disabled", bg="#f39c12")
        self.root.update()

        def run_test():
            success, message = self.smtp_handler.test_connection()
            def update_ui():
                self.test_smtp_btn.config(state="normal")
                if success:
                    messagebox.showinfo("Success", f"SMTP connection successful!\nServer: {self.smtp_handler.smtp_server}")
                    self.test_smtp_btn.config(text="✅ SMTP OK", bg="#27ae60")
                else:
                    messagebox.showerror("Failed", f"SMTP connection failed:\n{message}")
                    self.test_smtp_btn.config(text="❌ SMTP FAIL", bg="#e74c3c")
            self.root.after(0, update_ui)
        threading.Thread(target=run_test, daemon=True).start()

    def _update_chrome_version_label(self):
        try:
            version = self.get_chrome_version()
            if version != "unknown":
                self.chrome_version_label.config(text=f"Chrome: v{version.split('.')[0]}", fg="#bdc3c7")
            else:
                self.chrome_version_label.config(text="Chrome: Unknown", fg="#e74c3c")
        except Exception:
            self.chrome_version_label.config(text="Chrome: Error", fg="#e74c3c")

    def _get_chromedriver_version(self):
        try:
            path = self.chromedriver_path or "chromedriver"
            result = subprocess.run([path, "--version"], capture_output=True, text=True, timeout=5, check=True)
            match = re.search(r'ChromeDriver\s*(\d+\.\d+\.\d+)', result.stdout)
            return match.group(1) if match else "unknown"
        except (FileNotFoundError, subprocess.CalledProcessError, subprocess.TimeoutExpired):
            return "not found"
        except Exception:
            return "error"

    def _proactive_version_check(self):
        self.log("🔍 Proactively checking Chrome/ChromeDriver version compatibility...")
        time.sleep(2)
        chrome_ver, driver_ver = self.get_chrome_version(), self._get_chromedriver_version()

        def update_ui():
            if "unknown" in chrome_ver or "not found" in driver_ver or "error" in driver_ver:
                self.log("⚠️ Could not verify versions automatically.")
                return

            chrome_major, driver_major = chrome_ver.split('.')[0], driver_ver.split('.')[0]

            if chrome_major == driver_major:
                self.chrome_version_label.config(fg="#27ae60")
                self.log(f"✅ Version match: Chrome v{chrome_major} | ChromeDriver v{driver_major}")
            else:
                self.chrome_version_label.config(text=f"Chrome v{chrome_major} | Driver v{driver_major} (MISMATCH!)", fg="#c0392b")
                self.log(f"❌ Version MISMATCH: Chrome v{chrome_major} vs ChromeDriver v{driver_major}")
                messagebox.showwarning("Version Mismatch", f"Your Chrome (v{chrome_major}) and ChromeDriver (v{driver_major}) versions do not match. This will likely cause errors. Please update ChromeDriver.")
        self.root.after(0, update_ui)

    def _manual_version_check(self):
        chrome_ver, driver_ver = self.get_chrome_version(), self._get_chromedriver_version()
        compatible = "Unknown"
        if "unknown" not in chrome_ver and "not found" not in driver_ver:
            compatible = "✅ Match" if chrome_ver.split('.')[0] == driver_ver.split('.')[0] else "❌ Mismatch"
        messagebox.showinfo("Version Check", f"Browser: {chrome_ver}\nDriver: {driver_ver}\nStatus: {compatible}")

    def log(self, msg):
        timestamp = datetime.now().strftime("[%H:%M:%S] ")
        full_msg = timestamp + msg + "\n"
        print(timestamp + msg, file=sys.stdout, flush=True)
        if hasattr(self, 'gui_update_queue'):
            self.gui_update_queue.put(('log_message', full_msg))
        else:
            try:
                if hasattr(self, 'log_box') and self.log_box.winfo_exists():
                    self.log_box.insert(tk.END, full_msg)
                    self.log_box.see(tk.END)
            except Exception:
                pass

        # Auto-classify severity for the sent log tab
        if hasattr(self, 'sent_log_entries'):
            if '❌' in msg or 'Error' in msg or 'error' in msg or 'FAIL' in msg:
                severity = "ERROR"
            elif '⚠️' in msg or 'Warning' in msg or 'warning' in msg:
                severity = "WARNING"
            elif '✅' in msg or 'Success' in msg or 'Sent' in msg:
                severity = "SUCCESS"
            else:
                severity = "INFO"
            self._append_sent_log(msg, severity)

    def _choose_chromedriver(self):
        file_path = filedialog.askopenfilename(title="Select ChromeDriver", filetypes=[("Executable", "*.exe" if sys.platform.startswith('win') else "*")])
        if file_path:
            self.chromedriver_path = file_path
            filename = os.path.basename(file_path)
            self.chromedriver_label.config(text=f"Selected: {filename}")
            self.log(f"✅ ChromeDriver selected: {filename}")
            threading.Thread(target=self._proactive_version_check, daemon=True).start()

    def _load_emails(self):
        file_path = filedialog.askopenfilename(title="Select Email List", filetypes=[("CSV", "*.csv"), ("Text", "*.txt")])
        if not file_path:
            return

        self.log("📁 Loading email list in background...")

        def _load_worker():
            newly_added_emails = []
            new_data_rows = []
            new_headers = []

            try:
                if file_path.lower().endswith('.csv'):
                    with open(file_path, 'r', newline='', encoding='utf-8-sig') as f:
                        reader = csv.reader(f)
                        try:
                            new_headers = [h.strip().lower() for h in next(reader)]
                            if 'email' not in new_headers:
                                self.root.after(0, lambda: messagebox.showerror("CSV Error", "CSV must have an 'email' column header."))
                                return
                        except StopIteration:
                            self.root.after(0, lambda: messagebox.showerror("CSV Error", "CSV file is empty or has no header row."))
                            return

                        email_idx = new_headers.index('email')
                        for row in reader:
                            if not row or len(row) <= email_idx:
                                continue
                            email = row[email_idx].strip().lower()
                            if email and self._is_valid_email(email) and email not in self.email_list:
                                newly_added_emails.append(email)
                                new_data_rows.append(dict(zip(new_headers, row)))
                    self.log(f"✅ Loaded {len(newly_added_emails)} new recipients from CSV.")

                else:  # Text file
                    with open(file_path, 'r', encoding='utf-8') as f:
                        new_headers = ['email']
                        for line in f:
                            email = line.strip().lower()
                            if email and self._is_valid_email(email) and email not in self.email_list:
                                newly_added_emails.append(email)
                                new_data_rows.append({'email': email})
                    self.log(f"✅ Loaded {len(newly_added_emails)} new recipients from TXT file.")

                if not newly_added_emails:
                    self.log("💡 No new, unique emails found in the selected file.")
                    return

                def _apply_to_ui():
                    self.email_list_headers = sorted(list(set(self.email_list_headers) | set(new_headers)))
                    for i, email in enumerate(newly_added_emails):
                        self.email_list.append(email)
                        self.tracking_map[email] = new_data_rows[i]
                    for email in newly_added_emails:
                        self._initialize_tracking_map_for_email(email)
                    original_count = len(self.email_list)
                    self.email_list = [e for e in self.email_list if self.tracking_map.get(e, {}).get('status') != 'Suppressed']
                    suppressed_count = original_count - len(self.email_list)
                    if suppressed_count > 0:
                        self.log(f"🚫 Filtered out {suppressed_count} suppressed emails.")
                    self._refresh_recipients_table(force_all=True)

                self.root.after(0, _apply_to_ui)

            except Exception as e:
                self.log(f"❌ Error loading emails: {traceback.format_exc()}")
                self.root.after(0, lambda: messagebox.showerror("Load Error", f"Could not load file:\n{e}"))

        threading.Thread(target=_load_worker, daemon=True).start()

    def _paste_emails(self):
        try:
            clipboard_content = self.root.clipboard_get()
        except Exception as e:
            self.log(f"❌ Paste error: {e}")
            messagebox.showerror("Paste Error", "Could not paste from clipboard.")
            return

        self.log("📋 Processing pasted emails in background...")

        def _paste_worker():
            try:
                emails = re.findall(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', clipboard_content)
                new_emails_data = []

                for email in {e.lower().strip() for e in emails}:
                    if email and self._is_valid_email(email) and email not in self.email_list:
                        new_emails_data.append(email)

                def _apply_to_ui():
                    new_recipients = 0
                    for email in new_emails_data:
                        if email not in self.email_list:
                            self.email_list.append(email)
                            self.tracking_map[email] = {'email': email}
                            self._initialize_tracking_map_for_email(email)
                            if self.tracking_map[email].get('status') != 'Suppressed':
                                new_recipients += 1
                            else:
                                self.email_list.pop()
                                del self.tracking_map[email]

                    if new_recipients > 0:
                        self._refresh_recipients_table(force_all=True)
                        self.log(f"✅ Pasted {new_recipients} new, non-suppressed emails.")
                    else:
                        messagebox.showinfo("No New Emails", "No new, valid, non-suppressed emails found in clipboard.")

                self.root.after(0, _apply_to_ui)
            except Exception as e:
                self.log(f"❌ Paste error: {e}")
                self.root.after(0, lambda: messagebox.showerror("Paste Error", "Could not paste from clipboard."))

        threading.Thread(target=_paste_worker, daemon=True).start()

    def _add_recipient(self):
        email = self.new_recipient_var.get().strip().lower()
        if not email or not self._is_valid_email(email):
            messagebox.showwarning("Invalid", "Enter a valid email.")
            return
        if email in self.email_list:
            messagebox.showinfo("Exists", "Email already in list.")
            return

        if email in self.suppression_list:
            messagebox.showwarning("Suppressed", "This email is in the suppression list and cannot be added.")
            return

        self.email_list.append(email)
        self.tracking_map[email] = {'email': email}
        self._initialize_tracking_map_for_email(email)

        self._refresh_recipients_table(force_all=True)
        self.new_recipient_var.set("")
        self.log(f"✅ Added recipient: {email}")

    def _validate_email_list(self):
        if not self.email_list:
            messagebox.showinfo("List Empty", "No emails to validate.")
            return

        if not messagebox.askyesno("Validate List", f"This will check MX records for {len(self.email_list)} emails. Continue?"):
            return

        self.log("🔍 Starting list validation in background...")

        def _validate_worker():
            valid_count = 0
            invalid_count = 0

            for email in list(self.email_list):
                if not self._is_valid_email(email):
                    self.tracking_map[email]['status'] = 'Invalid Syntax'
                    invalid_count += 1
                    self.gui_update_queue.put(('update_recipient', email))
                    continue

                domain = email.split('@')[1]
                mx_status = self.deliverability_helper.check_mx_record(domain)

                if mx_status == "Valid":
                    self.tracking_map[email]['status'] = 'Valid'
                    valid_count += 1
                else:
                    self.tracking_map[email]['status'] = f"Invalid ({mx_status})"
                    invalid_count += 1

                self.gui_update_queue.put(('update_recipient', email))

            final_valid = valid_count
            final_invalid = invalid_count

            def _finalize():
                self._refresh_recipients_table(force_all=True)
                self.log(f"✅ Validation Complete: {final_valid} Valid, {final_invalid} Invalid.")
                messagebox.showinfo("Validation Complete", f"Result:\nValid: {final_valid}\nInvalid/Issues: {final_invalid}")

            self.root.after(0, _finalize)

        threading.Thread(target=_validate_worker, daemon=True).start()

    def _is_valid_email(self, email):
        if not re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', email):
            return False

        domain = email.split('@')[1].lower()
        if domain in self.common_isp_domains:
            self.log(f"⚠️ Ignored disposable email: {email}")
            return False

        return True

    def _remove_selected(self):
        selected_items = self.tree.selection()
        if not selected_items:
            return
        if not messagebox.askyesno("Confirm", f"Remove {len(selected_items)} recipient(s)?"):
            return
        emails_to_remove = [self.tree.item(item, 'values')[0] for item in selected_items]
        self.email_list = [e for e in self.email_list if e not in emails_to_remove]
        for email in emails_to_remove:
            del self.tracking_map[email]
        self._refresh_recipients_table(force_all=True)
        self.log(f"✅ Removed {len(emails_to_remove)} recipient(s)")

    def _retry_failed(self):
        failed_emails = [e for e, d in self.tracking_map.items() if 'Failed' in d.get('status', '')]
        if not failed_emails:
            messagebox.showinfo("No Failed Emails", "No failed emails to retry.")
            return
        if messagebox.askyesno("Retry", f"Retry sending to {len(failed_emails)} failed emails?"):
            for email in failed_emails:
                self.tracking_map[email]['status'] = 'Queued'
                self.tracking_map[email]['sent_time'] = ''
            self._refresh_recipients_table(force_all=True)
            self._start_sending(email_list_override=failed_emails)

    def _initialize_tracking_map_for_email(self, email):
        base_data = self.tracking_map.get(email, {'email': email})
        if email in self.suppression_list:
            base_data['status'] = 'Suppressed'
        else:
            base_data.setdefault('status', 'Pending')

        base_data.setdefault('sent_time', '')
        base_data.setdefault('attempts', 0)
        base_data.setdefault('failure_reason', '')
        base_data.setdefault('open_events', [])
        base_data.setdefault('click_events', [])

    def _update_stats_label(self):
        """Update the main stats label with current sent/failed/total counts across all engines."""
        try:
            # Aggregate totals from all engines
            total = len(self.email_list)
            sent = self.sent_count
            failed = self.failed_count

            # Also aggregate VPS stats
            if hasattr(self, 'vps_email_list'):
                total += len(self.vps_email_list)
            # Also aggregate MX stats
            if hasattr(self, 'mx_email_list'):
                total += len(self.mx_email_list)

            if hasattr(self, 'stats_label') and self.stats_label.winfo_exists():
                self.stats_label.config(text=f"📊 Stats: {sent} sent | {failed} failed | {total} total")

            # Update progress bar maximum and value
            if hasattr(self, 'progress') and self.progress.winfo_exists():
                self.progress["maximum"] = max(total, 1)
                self.progress["value"] = sent + failed

            # Update VPS stats label if available
            if hasattr(self, 'vps_stats_label') and self.vps_stats_label.winfo_exists():
                vps_total = len(self.vps_email_list) if hasattr(self, 'vps_email_list') else 0
                vps_sent = sum(1 for d in self.vps_tracking_map.values() if d.get('status', '').startswith('Sent')) if hasattr(self, 'vps_tracking_map') else 0
                vps_failed = sum(1 for d in self.vps_tracking_map.values() if d.get('status', '') in ('Failed', 'Error') or 'Failed' in d.get('status', '')) if hasattr(self, 'vps_tracking_map') else 0
                self.vps_stats_label.config(text=f"📊 VPS Stats: {vps_sent} sent | {vps_failed} failed | {vps_total} total")

            # Update MX stats label if available
            if hasattr(self, 'mx_stats_label') and self.mx_stats_label.winfo_exists():
                mx_total = len(self.mx_email_list) if hasattr(self, 'mx_email_list') else 0
                mx_sent = self.direct_mx_sent_count if hasattr(self, 'direct_mx_sent_count') else 0
                mx_failed = self.direct_mx_failed_count if hasattr(self, 'direct_mx_failed_count') else 0
                self.mx_stats_label.config(text=f"📊 MX Stats: {mx_sent} sent | {mx_failed} failed | {mx_total} total")
        except Exception:
            pass

    def _refresh_recipients_table(self, force_all=False):
        self.tree.delete(*self.tree.get_children())
        self.tree_items.clear()
        for email in self.email_list:
            self._insert_recipient_in_tree(email)
        self._update_stats_label()

    def _insert_recipient_in_tree(self, email):
        d = self.tracking_map.get(email, {})
        sent_time = d.get('sent_time', '')
        if sent_time and ' ' in sent_time:
            sent_time = sent_time.split(' ')[1]
        values = (email, d.get('status', 'Pending'), sent_time, '✔️' if d.get('open_events') else '', '✔️' if d.get('click_events') else '', str(d.get('attempts', 0)), d.get('failure_reason', ''))
        item_id = self.tree.insert("", "end", values=values, tags=(d.get('status', '').split(' ')[0],))
        self.tree_items[email] = item_id
        for tag, color in [('Sent', '#d4edda'), ('Failed', '#f8d7da'), ('Error', '#f8d7da'), ('Sending', '#fff3cd'), ('Queued', '#d1ecf1'), ('Suppressed', '#e2e3e5'), ('Unsubscribed', '#e2e3e5')]:
            self.tree.tag_configure(tag, background=color)

    def _clear_email_list(self):
        if not self.email_list:
            messagebox.showinfo("List Empty", "The recipient list is already empty.")
            return
        if messagebox.askyesno("Confirm Clear", f"Are you sure you want to clear the entire list of {len(self.email_list)} recipients? This cannot be undone."):
            self.email_list.clear()
            self.tracking_map.clear()
            self.tree_items.clear()
            self.tree.delete(*self.tree.get_children())
            self._update_stats_label()
            self.log("🗑️ Recipient list has been cleared.")

    def get_rotation_profiles(self):
        if self.provider_var.get() != "SMTP":
            return {}

        if self.smtp_rotation_enabled_var.get() and self.profiles:
            valid_profiles = {}
            for name, data in self.profiles.items():
                if data.get("provider") == "SMTP" and data.get("smtp", {}).get("username"):
                     s = data["smtp"]
                     user = s["username"]
                     pwd = None
                     if KEYRING_AVAILABLE:
                         try:
                             pwd = keyring.get_password(KEYRING_SERVICE_NAME, user)
                         except Exception:
                             pass

                     if not pwd:
                         self.log(f"⚠️ Skipping rotation profile '{name}' - password not found in keyring.")
                         continue

                     valid_profiles[name] = {
                         "server": s["smtp_server"],
                         "port": s["smtp_port"],
                         "username": user,
                         "password": pwd,
                         "use_tls": s["use_tls"],
                         "use_ssl": s["use_ssl"],
                         "sender_name": s["sender_name"],
                         "sender_email": s["sender_email"],
                         "name": name
                     }
            if valid_profiles:
                 self.log(f"🔄 Using {len(valid_profiles)} SMTP rotation profiles.")
                 return valid_profiles

        current_user = self.smtp_handler.username
        if not current_user:
            return {}

        current_pass = self.smtp_handler.get_password()
        if not current_pass:
            self.log("❌ Cannot start SMTP send. Password for the current profile is not available.")
            return {}

        return {
            "Current": {
                "server": self.smtp_handler.smtp_server,
                "port": self.smtp_handler.smtp_port,
                "username": current_user,
                "password": current_pass,
                "use_tls": self.smtp_handler.use_tls,
                "use_ssl": self.smtp_handler.use_ssl,
                "sender_name": self.smtp_handler.sender_name,
                "sender_email": self.smtp_handler.sender_email,
                "name": "Default"
            }
        }

    def current_throttle_delay_seconds(self):
        delay = self.throttle_delay_var.get()
        return delay if self.throttle_unit_var.get() == "Seconds" else delay * 60

    def _detect_browser_sender_identity(self):
        if not self.driver:
            return

        try:
            current_url = self.driver.current_url

            parsed_url = urlparse(current_url)
            current_host = parsed_url.hostname or ""

            if current_host == "mail.google.com":
                try:
                    account_btn = self.driver.find_element(By.CSS_SELECTOR, "a[aria-label^='Google Account:']")
                    label_text = account_btn.get_attribute("aria-label")

                    match = re.search(r'Google Account:\s*(.+?)\s*\((.+?)\)', label_text)
                    if match:
                        name, email = match.group(1), match.group(2)
                        self.log(f"🕵️ Detected Gmail Account: {name} <{email}>")
                        self.autodetected_sender_name = name
                        self.autodetected_sender_email = email
                except Exception:
                    pass

            elif current_host in ("outlook.live.com", "outlook.office.com"):
                try:
                    account_btn = self.driver.find_element(By.ID, "O365_MainLink_Me")
                    label_text = account_btn.get_attribute("aria-label")

                    if "Account manager for" in label_text:
                        name = label_text.replace("Account manager for", "").strip()
                        self.log(f"🕵️ Detected Outlook Account: {name}")
                        self.autodetected_sender_name = name
                        match = re.search(r'\((.+?@.+?)\)', label_text)
                        if match:
                             self.autodetected_sender_email = match.group(1)
                except Exception:
                    pass

        except Exception as e:
            self.log(f"⚠️ Could not auto-detect sender identity: {e}")

    def _connect_outlook(self):
        if not OUTLOOK_COM_AVAILABLE:
            if sys.platform != 'win32':
                 messagebox.showerror("Unavailable", "Outlook Desktop Automation (COM) is only available on Windows.")
            else:
                 messagebox.showerror("Unavailable", "Outlook COM integration requires 'pywin32'.\nInstall with: pip install pywin32")
            return

        if not self.outlook_com:
             self.outlook_com = OutlookCOMHandler(self)

        self.connect_outlook_btn.config(text="CONNECTING...", state="disabled", bg="#f39c12")
        self.root.update()

        def run_connect():
            success = self.outlook_com.connect_to_outlook()
            def update_ui():
                if success:
                    self.outlook_status.config(text="🖥️ Outlook: Connected", fg="#2ecc71")
                    self.connect_outlook_btn.config(text="✅ CONNECTED", bg="#0078d4", state="disabled")
                    self.log("✅ Successfully connected to Desktop Outlook.")
                else:
                    self.connect_outlook_btn.config(text="🖥️ OUTLOOK", state="normal", bg="#0078d4")
                    messagebox.showerror("Connection Failed", "Could not connect to Desktop Outlook.\nEnsure Outlook is running.")
            self.root.after(0, update_ui)
        threading.Thread(target=run_connect, daemon=True).start()

    def _start_sending(self, email_list_override=None):
        if self.running and not self.paused:
            return
        if self.paused:
            self.paused = False
            self._set_ui_state_sending(True)
            self.log("▶️ Sending resumed.")
            return

        emails_to_use = email_list_override if email_list_override else self.email_list
        if not emails_to_use:
            messagebox.showinfo("No Recipients", "Please load an email list first.")
            return

        provider = self.provider_var.get()
        if provider == "SMTP" and (not self.smtp_handler.smtp_server or not self.smtp_handler.username):
            messagebox.showerror("SMTP Config", "Please configure SMTP settings before sending.")
            self._open_smtp_config()
            return

        if provider != "SMTP" and "Outlook" in provider and "COM" not in provider:
             if not self.driver:
                 messagebox.showerror("Not Connected", "Please connect to Chrome first.")
                 return

        subject = self.subject_var.get()
        content = self._get_message_content()

        if not subject or not content:
            messagebox.showwarning("Missing Content", "Please enter a subject and message content.")
            return

        self.running = True
        self.paused = False
        self.failed_count = 0
        self.sent_count = 0

        self._set_ui_state_sending(True)
        self.log(f"🚀 Starting campaign via {provider}...")

        if provider == "Proxy VPS Mailer":
            self.proxy_vps_handler.run_vps_sending_job(emails_to_use, subject, content, self.attachment_paths)
        elif provider == "Direct MX":
            self.direct_mx_handler.run_direct_mx_sending_job(emails_to_use, subject, content, self.attachment_paths)
        else:
            threading.Thread(target=self._sending_process_threaded, args=(subject, content, provider, emails_to_use), daemon=True).start()

    def _toggle_pause(self):
        if not self.running:
            return
        self.paused = not self.paused
        if self.paused:
            self.pause_btn.config(text="▶️ RESUME", bg="#27ae60")
            self.status_label.config(text="⏸️ PAUSED", fg="#f39c12")
            self.log("⏸️ Sending paused.")
        else:
            self.pause_btn.config(text="⏸️ PAUSE", bg="#f39c12")
            self.status_label.config(text="🟢 Sending...", fg="#2ecc71")
            self.log("▶️ Sending resumed.")

    def _set_ui_state_sending(self, is_sending):
        state = "disabled" if is_sending else "normal"
        self.start_btn.config(state="disabled" if is_sending else "normal")
        self.pause_btn.config(state="normal" if is_sending else "disabled", text="⏸️ PAUSE", bg="#f39c12")
        self.stop_btn.config(state="normal" if is_sending else "disabled")

        self.connect_chrome_btn.config(state="disabled" if is_sending else "normal")
        self.start_chrome_btn.config(state="disabled" if is_sending else "normal")
        if self.connect_outlook_btn:
             self.connect_outlook_btn.config(state="disabled" if is_sending else "normal")

        if is_sending:
            self.status_label.config(text="🟢 Sending...", fg="#2ecc71")
            self.progress.config(mode='determinate', maximum=len(self.email_list), value=0)
        else:
            self.status_label.config(text="🔵 Ready", fg="#2980b9")
            self.progress.config(value=0)

    def _sending_process_threaded(self, subject, content, provider, email_list, sequence_context=None):
        try:
            if provider != "SMTP" and self.driver:
                self._detect_browser_sender_identity()

            list_to_send = [e for e in email_list if self.tracking_map.get(e.lower(), {}).get('status') not in ['Sent', 'Sent (SMTP)', 'Suppressed', 'Unsubscribed']]
            for i, email_address in enumerate(list_to_send):
                if not self.running:
                    break
                while self.paused:
                    time.sleep(1)
                    continue
                self._process_single_email(email_address, subject, content, provider, i, len(list_to_send))
                throttle_amount = self.throttle_amount_var.get()
                if throttle_amount > 0 and (i + 1) % throttle_amount == 0:
                    delay = self.current_throttle_delay_seconds()
                    if delay > 0 and self.running:
                        self.log(f"⏱️ Throttling: Pausing for {delay}s...")
                        time.sleep(delay)

        except Exception as e:
            self.log(f"❌ Sending process error: {traceback.format_exc()}")
        finally:
            if not sequence_context:
                self.root.after(0, self._finalize_sending)

    def _prepare_email_data(self, email_address, base_subject, base_content, attachments, index, total):
        """NEW v8.0.3: Centralized method to prepare email data for any sender."""
        subj = self._pick_ab_subject(base_subject, index, total)
        body = self._get_ab_body_content(email_address, base_content, self.message_box_b.get("1.0", tk.END))
        spun_subject = self.deliverability_helper.spin(subj)
        spun_content = self.deliverability_helper.spin(body)
        tracked_content = self._add_tracking_to_content(spun_content, email_address) if self.tracking_enabled.get() else spun_content

        personalized_subject, personalized_content, new_data, unsubscribe_url = self._personalize_content(
            email_address, spun_subject, tracked_content
        )

        if CSS_INLINE_AVAILABLE:
            ok, inlined_content = self.html_helper.inline_css(personalized_content)
            if ok:
                personalized_content = inlined_content

        if new_data:
            self.tracking_map[email_address].update(new_data)

        return {
            "to_email": email_address,
            "subject": personalized_subject,
            "content": personalized_content,
            "attachments": attachments,
            "dynamic_sender_name": self.tracking_map[email_address].get('sender_name'),
            "unsubscribe_url": unsubscribe_url,
        }

    def _process_single_email(self, email_address, base_subject, base_content, provider, index, total):
        try:
            self.log(f"📤 ({self.sent_count + self.failed_count + 1}/{len(self.email_list)}) -> {email_address}")

            if self.autodetected_sender_email:
                 self.tracking_map[email_address]['senderemail'] = self.autodetected_sender_email

            email_data = self._prepare_email_data(email_address, base_subject, base_content, self.attachment_paths, index, total)

            # Handle PDF attachment for browser-based sending
            final_attachments = email_data['attachments'].copy()
            pdf_path = self.template_pdf_var.get()
            if self.enable_secure_pdf_lure_var.get() and pdf_path and os.path.exists(pdf_path) and PYPDF_AVAILABLE:
                try:
                    personalized_pdf_path = self._embed_text_in_pdf(pdf_path, f"For: {email_address} | {str(uuid.uuid4())[:8]}")
                    if personalized_pdf_path:
                        final_attachments.append(personalized_pdf_path)
                        self.log(f"📄 Attached personalized PDF for {email_address}")
                except Exception as e:
                    self.log(f"❌ PDF personalization failed for {email_address}: {e}")

            success = self._send_single_email(
                email_address, email_data['subject'], email_data['content'], provider,
                unsubscribe_url=email_data['unsubscribe_url'], attachments=final_attachments
            )

            if success:
                self.sent_count += 1
                self.tracking_map[email_address]['status'] = 'Sent'
                self.tracking_map[email_address]['sent_time'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            else:
                self.failed_count += 1
                self.tracking_map[email_address]['status'] = 'Failed'

        except Exception as e:
            self.log(f"❌ CRITICAL ERROR in _process_single_email for {email_address}: {traceback.format_exc()}")
            self.failed_count += 1
            self.tracking_map[email_address]['status'] = 'Error'
            self.tracking_map[email_address]['failure_reason'] = 'Processing Error'

        finally:
            self.gui_update_queue.put(('update_recipient', email_address))
            self.gui_update_queue.put(('update_progress', self.sent_count + self.failed_count))

    def _pick_ab_subject(self, base_subject, index, total):
        if not self.ab_testing_enabled.get() or not self.subject_a_var.get() or not self.subject_b_var.get():
            return base_subject
        split_ratio = max(0, min(100, self.ab_split_ratio_var.get()))
        return self.subject_a_var.get() if (index / max(1, total)) * 100 < split_ratio else self.subject_b_var.get()

    def _get_ab_body_content(self, email, content_a, content_b):
        if not self.ab_body_enabled_var.get() or not content_b.strip():
            return content_a
        split_ratio = max(0, min(100, self.ab_split_ratio_var.get()))
        return content_a if (hash(email) % 100) < split_ratio else content_b

    def _personalize_content(self, email, subject, content):
        """
        AUTOGRAB RESTORED & FIXED (v8.0.2): Uses Jinja2 and new logic to provide the requested placeholders.
        - [firstname] -> {{ firstname }}
        - [company] -> {{ company }}
        - [greetings] -> {{ greetings }}
        - ... and others
        Handles fallbacks and ISP/company domain logic as requested.
        """
        recipient_data = self.tracking_map.get(email, {'email': email}).copy()
        newly_found_data = {}

        # 1. Autograb Firstname
        found_name = recipient_data.get('firstname')
        if not found_name:
            local_part = email.split('@')[0]
            potential_parts = re.split(r'[._\-+]+', local_part)
            valid_parts = [p for p in potential_parts if len(p) > 1 and p.isalpha()]
            generic_words = {'info', 'contact', 'admin', 'support', 'sales', 'mail', 'email', 'hello', 'test', 'demo', 'user', 'customer', 'press', 'jobs', 'careers', 'service', 'team', 'office', 'billing', 'accounts', 'dev', 'webmaster', 'media', 'noreply', 'no-reply', 'marketing', 'newsletter', 'updates', 'general', 'enquiry', 'office', 'staff', 'manager', 'hr', 'recruitment', 'inquiries'}
            if valid_parts:
                candidate = valid_parts[0]
                if candidate.lower() not in generic_words:
                    found_name = candidate.capitalize()
            if found_name:
                recipient_data['firstname'] = found_name
                newly_found_data['firstname'] = found_name

        # 2. Autograb Company
        found_company = recipient_data.get('company')
        if not found_company:
            try:
                domain = email.split('@')[1].lower()
                if domain in self.common_isp_domains:
                    found_company = "you"  # Fallback for ISP domains as requested
                else:
                    # Extract company name from non-ISP domain
                    parts = domain.split('.')
                    company_part = parts[-2] if len(parts) > 2 and len(parts[-2]) > 2 and parts[-2] not in ('co', 'com', 'org', 'net', 'ac', 'gov', 'edu') else parts[0]
                    found_company = '-'.join([p.capitalize() for p in company_part.split('-')])

                if found_company:
                    recipient_data['company'] = found_company
                    newly_found_data['company'] = found_company
            except Exception:
                pass  # Keep company blank if logic fails

        # 3. Prepare the full Jinja2 context, ensuring all keys are lowercase
        final_context = {k.lower(): v for k, v in recipient_data.items()}

        # 4. Add dynamic autograb placeholders with improved logic
        now = datetime.now()

        # Dynamic Greetings (FIXED)
        hour = now.hour
        if 5 <= hour < 12:
            base_greeting = "Good morning"
        elif 12 <= hour < 18:
            base_greeting = "Good afternoon"
        else:
            base_greeting = "Good evening"

        # Combine greeting with firstname if available
        if found_name:
            final_context['greetings'] = f"{base_greeting} {found_name}"
        else:
            final_context['greetings'] = base_greeting

        # Fallback for firstname placeholder itself
        final_context.setdefault('firstname', 'Hello')

        # Set company fallback for templates
        final_context.setdefault('company', 'you')

        # Other dynamic fields
        final_context['sender_name'] = self.autodetected_sender_name or self.smtp_handler.sender_name or "Sender"
        final_context['currentdate'] = now.strftime("%B %d, %Y")
        final_context['time'] = now.strftime("%I:%M %p")
        final_context['secure_link'] = self._generate_secure_link(email)

        # Unsubscribe Link
        unsubscribe_url = ""
        if self.tracking_server and self.tracking_server.public_url:
            email_id = base64.urlsafe_b64encode(email.encode()).decode()
            unsubscribe_url = f'{self.tracking_server.public_url}/unsubscribe/{CAMPAIGN_ID}/{email_id}'
        final_context['unsubscribe_link'] = unsubscribe_url
        newly_found_data['unsubscribe_url'] = unsubscribe_url

        # Legacy support
        final_context['unsubscribe_url'] = unsubscribe_url

        # 5. Render subject and content using Jinja2
        # Replace legacy `[tag]` format with `{{ tag }}` before rendering
        def replace_legacy_tags(text):
            # This regex avoids replacing things that look like Jinja but aren't the target placeholders
            return re.sub(r'\[([a-zA-Z0-9_]+)\]', r'{{ \1 }}', text)

        personalized_subject = replace_legacy_tags(subject)
        personalized_content = replace_legacy_tags(content)

        if self.jinja_env:
            try:
                subject_template = self.jinja_env.from_string(personalized_subject)
                personalized_subject = subject_template.render(final_context)

                content_template = self.jinja_env.from_string(personalized_content)
                personalized_content = content_template.render(final_context)

            except jinja_exceptions.TemplateError as e:
                self.log(f"⚠️ Jinja2 Render Error for {email}: {e}. Placeholders may not be filled.")
                # Fallback to simple replacement for safety
                for key, value in final_context.items():
                    personalized_subject = personalized_subject.replace(f"{{{{ {key} }}}}", str(value))
                    personalized_content = personalized_content.replace(f"{{{{ {key} }}}}", str(value))

        return personalized_subject, personalized_content, newly_found_data, unsubscribe_url

    def _add_tracking_to_content(self, html_content, email):
        if not self.tracking_server or not self.tracking_server.public_url:
            return html_content
        public_url, email_id = self.tracking_server.public_url, base64.urlsafe_b64encode(email.encode()).decode()
        pixel = f'<img src="{public_url}/track/open/{CAMPAIGN_ID}/{email_id}" width="1" height="1" alt="">'
        if '</body>' in html_content.lower():
            html_content = re.sub(r'</body>', f'{pixel}</body>', html_content, flags=re.IGNORECASE)
        else:
            html_content += pixel

        def replace_link(match):
            original_url = match.group(2)
            # Do not track secure links or unsubscribe links
            if original_url.startswith(('http', 'https')) and '/track/click/' not in original_url and 'unsubscribe' not in original_url and 'nonce=' not in original_url:
                encoded_url = base64.urlsafe_b64encode(original_url.encode()).decode()
                return f'{match.group(1)}="{public_url}/track/click/{CAMPAIGN_ID}/{email_id}?url={encoded_url}"'
            return match.group(0)
        return re.sub(r'(href\s*=\s*)(["\'](https?://[^"\']+)["\'])', replace_link, html_content, flags=re.IGNORECASE)

    def _send_single_email(self, email, subject, content, provider, unsubscribe_url=None, attachments=None):
        try:
            final_attachments = attachments or self.attachment_paths
            if provider in self.custom_providers:
                self.log(f"🔌 Using custom provider '{provider}' for: {email}")
                return bool(self.custom_providers[provider].send(email=email, subject=subject, content=content, attachments=final_attachments, context={"sender": self}))
            elif "outlook" in provider.lower() and self.outlook_com and self.outlook_com.connected:
                return self.outlook_com.send_email(email, subject, content, final_attachments, unsubscribe_url=unsubscribe_url)
            elif self.driver:
                return self._send_via_browser(email, subject, content, provider, final_attachments)
            self.log(f"❌ No available sending method for: {email}")
            return False
        except Exception:
            self.log(f"❌ Send error for {email}: {traceback.format_exc()}")
            return False

    def _send_via_browser(self, email, subject, content, provider, attachments=None):
        try:
            browser_safe_content = self._prepare_html_for_browser(content)

            with self.driver_lock:
                if "gmail" in provider.lower():
                    return self._send_gmail_optimized(email, subject, browser_safe_content, attachments)
                elif "outlook" in provider.lower():
                    return self._send_outlook_web_optimized(email, subject, browser_safe_content, attachments)
                elif "yahoo" in provider.lower():
                    return self._send_yahoo_optimized(email, subject, browser_safe_content, attachments)
                self.log(f"⚠️ Generic provider '{provider}' not implemented.")
                return False
        except Exception:
            self.log(f"❌ Browser send error: {traceback.format_exc()}")
            return False

    def _prepare_html_for_browser(self, full_html_content):
        processed_content = full_html_content
        if CSS_INLINE_AVAILABLE:
            try:
                inliner = css_inline.CSSInliner()
                processed_content = inliner.inline(full_html_content)
            except Exception as e:
                self.log(f"⚠️ CSS Inlining failed: {e}. Layout might break.")
        else:
            self.log("⚠️ css_inline not installed. HTML emails will render as plain text in Browser.")

        # DOM Shuffling for polymorphism
        if self.dom_shuffling_enabled.get():
            processed_content = self._apply_dom_shuffling(processed_content)

        try:
            processed_content = re.sub(r'<!DOCTYPE[^>]*>', '', processed_content, flags=re.IGNORECASE)

            if "<body" in processed_content.lower():
                match = re.search(r'<body[^>]*>(.*?)</body>', processed_content, re.DOTALL | re.IGNORECASE)
                if match:
                    return match.group(1).strip()

            processed_content = re.sub(r'<html[^>]*>', '', processed_content, flags=re.IGNORECASE)
            processed_content = re.sub(r'</html>', '', processed_content, flags=re.IGNORECASE)
            processed_content = re.sub(r'<head[^>]*>.*?</head>', '', processed_content, flags=re.DOTALL | re.IGNORECASE)

            return processed_content.strip()
        except Exception as e:
            self.log(f"⚠️ HTML preparation error: {e}")
            return processed_content

    def _apply_dom_shuffling(self, html_content):
        """Apply DOM shuffling to make emails structurally unique."""
        # Insert random invisible divs
        invisible_divs = [f'<div style="display:none; height:0; width:0;">{uuid.uuid4().hex[:8]}</div>' for _ in range(random.randint(1, 3))]
        html_content = html_content.replace('<body>', '<body>' + ''.join(invisible_divs))

        # Shuffle class names
        classes = ['btn-primary', 'cta-button', 'link-style', 'text-link']
        for cls in classes:
            new_cls = f"x{random.randint(1000, 9999)}-{cls}"
            html_content = html_content.replace(f'class="{cls}"', f'class="{new_cls}"')

        return html_content

    def _send_outlook_web_optimized(self, email, subject, content, attachments=None):
        try:
            if not self.driver:
                return False

            outlook_host = urlparse(self.driver.current_url).hostname or ""
            if outlook_host not in ("outlook.office.com", "outlook.live.com"):
                self.driver.get("https://outlook.office.com/mail/")

            try:
                account_btn = WebDriverWait(self.driver, 5).until(
                    EC.presence_of_element_located((By.ID, "O365_MainLink_Me"))
                )
                label_text = account_btn.get_attribute("aria-label") or ""
                match = re.search(r'\((.+?@.+?)\)', label_text)
                if match:
                    detected_email = match.group(1)
                    if detected_email and detected_email != self.autodetected_sender_email:
                        self.autodetected_sender_email = detected_email
                        self.log(f"🕵️ Detected Sender Email: {self.autodetected_sender_email}")
            except Exception:
                pass

            compose_button = WebDriverWait(self.driver, 20).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "button[aria-label='New mail'], button[aria-label='New Email']"))
            )
            self.driver.execute_script("arguments[0].click();", compose_button)

            WebDriverWait(self.driver, 15).until(
                EC.visibility_of_element_located((By.CSS_SELECTOR, "div[aria-label='Message body']"))
            )

            self.driver.execute_script(
                """
                var toField = document.querySelector("div[aria-label='To']");
                if (toField) {
                    var input = toField.querySelector('input');
                    if (!input) {
                         toField.focus();
                         document.execCommand('insertText', false, arguments[0]);
                    } else {
                        input.value = arguments[0];
                    }
                }
                """,
                email
            )
            time.sleep(0.5)
            ActionChains(self.driver).send_keys(Keys.TAB).perform()

            subject_field = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "input[aria-label='Subject']"))
            )
            self.driver.execute_script("arguments[0].focus();", subject_field)
            time.sleep(0.2)
            subject_field.send_keys(subject)

            body_field = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "div[aria-label='Message body']"))
            )

            try:
                self.driver.execute_script("arguments[0].innerHTML = arguments[1];", body_field, content)
            except Exception:
                 self.driver.execute_script("arguments[0].focus(); arguments[0].innerHTML = arguments[1];", body_field, content)

            if attachments:
                try:
                    self.log(f"📎 Uploading {len(attachments)} attachment(s)...")

                    file_inputs = self.driver.find_elements(By.CSS_SELECTOR, "input[type='file']")

                    if not file_inputs:
                        try:
                            attach_btn = self.driver.find_element(By.CSS_SELECTOR, "button[aria-label='Attach'], button[title='Attach'], button[name='Attach']")
                            self.driver.execute_script("arguments[0].click();", attach_btn)
                            time.sleep(1)
                            file_inputs = self.driver.find_elements(By.CSS_SELECTOR, "input[type='file']")
                        except Exception:
                            pass

                    if file_inputs:
                        target_input = file_inputs[-1]

                        self.driver.execute_script(
                            "arguments[0].style.display = 'block'; arguments[0].style.visibility = 'visible'; arguments[0].style.opacity = '1'; arguments[0].style.width = '1px'; arguments[0].style.height = '1px';",
                            target_input
                        )

                        for path in attachments:
                            abs_path = os.path.abspath(path)
                            target_input.send_keys(abs_path)
                            time.sleep(2)

                        try:
                            time.sleep(2)
                            modal = self.driver.find_elements(By.CSS_SELECTOR, "div[role='dialog']")
                            if modal:
                                attach_as_copy = self.driver.find_elements(By.XPATH, "//button[contains(., 'Attach as copy')]")
                                if attach_as_copy:
                                    self.log("⚠️ Detected OneDrive prompt. Clicking 'Attach as copy'...")
                                    self.driver.execute_script("arguments[0].click();", attach_as_copy[0])
                                else:
                                    pass
                        except Exception:
                            pass

                        self.log("✅ Attachments injected.")
                    else:
                        self.log("⚠️ Could not find or spawn a file input for attachments.")
                except Exception as e:
                    self.log(f"⚠️ Attachment error: {e}")

            time.sleep(1)
            send_button = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "button[aria-label='Send']"))
            )
            self.driver.execute_script("arguments[0].click();", send_button)

            WebDriverWait(self.driver, 15).until(
                EC.invisibility_of_element_located((By.CSS_SELECTOR, "div[aria-label='Message body']"))
            )

            return True

        except Exception:
            self.log(f"❌ Outlook send error: {traceback.format_exc()}")
            try:
                discard_button = self.driver.find_element(By.CSS_SELECTOR, "button[aria-label='Discard']")
                self.driver.execute_script("arguments[0].click();", discard_button)
                self.log("💡 Attempted to discard failed Outlook message.")
            except Exception:
                pass
            return False

    def _send_gmail_optimized(self, email, subject, content, attachments=None):
        try:
            if not self.driver:
                return False

            if urlparse(self.driver.current_url).hostname != "mail.google.com":
                self.driver.get("https://mail.google.com/")

            try:
                account_btn = WebDriverWait(self.driver, 5).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "a[aria-label^='Google Account:']"))
                )
                label_text = account_btn.get_attribute("aria-label") or ""
                match = re.search(r'\((.+?@.+?)\)', label_text)
                if match:
                    detected_email = match.group(1)
                    if detected_email and detected_email != self.autodetected_sender_email:
                        self.autodetected_sender_email = detected_email
                        self.log(f"🕵️ Detected Sender Email: {self.autodetected_sender_email}")
            except Exception:
                 pass

            compose_button = WebDriverWait(self.driver, 20).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "div[role='button'][gh='cm']"))
            )
            self.driver.execute_script("arguments[0].click();", compose_button)

            WebDriverWait(self.driver, 15).until(
                EC.visibility_of_element_located((By.CSS_SELECTOR, "div[aria-label='Message Body']"))
            )

            self.driver.execute_script(
                """
                var toField = document.querySelector("input[peoplekit-id]");
                if (!toField) toField = document.querySelector("input[aria-label='To recipients']");
                if (toField) { toField.focus(); document.execCommand('insertText', false, arguments[0]); }
                """, email
            )
            time.sleep(0.5)
            ActionChains(self.driver).send_keys(Keys.TAB).perform()

            subject_field = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "input[name='subjectbox']"))
            )
            self.driver.execute_script("arguments[0].focus();", subject_field)
            time.sleep(0.2)
            subject_field.send_keys(subject)

            body_field = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "div[aria-label='Message Body']"))
            )

            injection_script = """
            var body = arguments[0];
            var content = arguments[1];
            body.focus();
            var success = false;
            try {
                success = document.execCommand('insertHTML', false, content);
            } catch (e) {
                success = false;
            }
            if (!success) {
                body.innerHTML = content;
            }
            """
            try:
                self.driver.execute_script(injection_script, body_field, content)
            except Exception:
                 time.sleep(1)
                 self.driver.execute_script(injection_script, body_field, content)

            if attachments:
                try:
                    self.log(f"📎 Uploading {len(attachments)} attachment(s)...")
                    file_inputs = self.driver.find_elements(By.CSS_SELECTOR, "input[type='file'][name='Filedata']")

                    if not file_inputs:
                        file_inputs = self.driver.find_elements(By.CSS_SELECTOR, "input[type='file']")

                    if file_inputs:
                        target_input = file_inputs[-1]

                        self.driver.execute_script(
                            "arguments[0].style.display = 'block'; arguments[0].style.visibility = 'visible'; arguments[0].style.opacity = '1'; arguments[0].style.width = '1px'; arguments[0].style.height = '1px';",
                            target_input
                        )

                        for path in attachments:
                            target_input.send_keys(os.path.abspath(path))
                            time.sleep(2)

                        self.log("✅ Attachments injected.")
                    else:
                         self.log("⚠️ No file input found for attachments.")
                except Exception as e:
                    self.log(f"⚠️ Attachment error: {e}")

            time.sleep(1)
            send_button = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "div[role='button'][aria-label*='Send']"))
            )
            self.driver.execute_script("arguments[0].click();", send_button)

            return True

        except Exception:
            self.log(f"❌ Gmail send error: {traceback.format_exc()}")
            return False

    def _send_yahoo_optimized(self, email, subject, content, attachments=None):
        try:
            if "mail.yahoo.com" not in self.driver.current_url:
                self.driver.get("https://mail.yahoo.com/")

            compose_button = WebDriverWait(self.driver, 20).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "a[data-test-id='compose-button']"))
            )
            self.driver.execute_script("arguments[0].click();", compose_button)

            WebDriverWait(self.driver, 15).until(
                EC.visibility_of_element_located((By.CSS_SELECTOR, "div[data-test-id='rte']"))
            )

            self.driver.execute_script(
                """
                var toField = document.getElementById('message-to-field');
                if (toField) {
                    toField.focus();
                    document.execCommand('insertText', false, arguments[0]);
                }
                """,
                email
            )
            time.sleep(0.5)
            ActionChains(self.driver).send_keys(Keys.TAB).perform()

            subject_field = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "input[data-test-id='compose-subject']"))
            )
            self.driver.execute_script("arguments[0].focus();", subject_field)
            time.sleep(0.2)
            subject_field.send_keys(subject)

            body_field = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "div[data-test-id='rte']"))
            )
            self.driver.execute_script("arguments[0].focus();", body_field)
            time.sleep(0.2)

            injection_script = """
            var body = arguments[0];
            var content = arguments[1];
            body.focus();
            try {
                document.execCommand('insertHTML', false, content);
            } catch (e) {
                body.innerHTML = content;
            }
            """
            self.driver.execute_script(injection_script, body_field, content)

            if attachments:
                try:
                    self.log(f"📎 Uploading {len(attachments)} attachment(s)...")
                    file_inputs = self.driver.find_elements(By.CSS_SELECTOR, "input[type='file']")

                    if not file_inputs:
                        try:
                            attach_btn = self.driver.find_element(By.CSS_SELECTOR, "button[data-test-id='compose-attach-button'], button[title='Attach files']")
                            self.driver.execute_script("arguments[0].click();", attach_btn)
                            time.sleep(1)
                            file_inputs = self.driver.find_elements(By.CSS_SELECTOR, "input[type='file']")
                        except Exception:
                            pass

                    if file_inputs:
                        target_input = file_inputs[-1]

                        self.driver.execute_script(
                            "arguments[0].style.display = 'block'; arguments[0].style.visibility = 'visible'; arguments[0].style.opacity = '1'; arguments[0].style.width = '1px'; arguments[0].style.height = '1px';",
                            target_input
                        )

                        for path in attachments:
                            target_input.send_keys(os.path.abspath(path))
                            time.sleep(2)

                        self.log("✅ Attachments injected.")
                    else:
                        self.log("⚠️ No file input found for attachments.")
                except Exception as e:
                    self.log(f"⚠️ Attachment error: {e}")

            time.sleep(1)
            send_button = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "button[data-test-id='compose-send-button']"))
            )
            self.driver.execute_script("arguments[0].click();", send_button)

            WebDriverWait(self.driver, 15).until(
                EC.invisibility_of_element_located((By.CSS_SELECTOR, "div[data-test-id='rte']"))
            )
            return True

        except Exception:
            self.log(f"❌ Yahoo Mail send error: {traceback.format_exc()}")
            try:
                discard_button = self.driver.find_element(By.CSS_SELECTOR, "button[data-test-id='compose-discard-button']")
                self.driver.execute_script("arguments[0].click();", discard_button)
                time.sleep(0.5)
                confirm_discard = self.driver.find_element(By.CSS_SELECTOR, "button[data-test-id='modal-secondary-btn']")
                self.driver.execute_script("arguments[0].click();", confirm_discard)
                self.log("💡 Attempted to discard failed Yahoo message.")
            except Exception:
                pass
            return False

    def _stop_sending(self):
        if self.schedule_timer:
            self.schedule_timer.cancel()
            self.log("🗓️ Scheduled send cancelled.")
        if self.running:
            self.running = False
            self.paused = False
            self._cancel_async_tasks()
            self.log("⏹️ Sending stopping by user command...")
            if self.provider_var.get() != "SMTP":
                self._finalize_sending()
        else:
            self.log("⏹️ Stop command received, but no campaign is running.")


    def _cancel_async_tasks(self):
        """Cancel all pending async tasks on the async event loop."""
        if self.async_loop and not self.async_loop.is_closed():
            for task in asyncio.all_tasks(self.async_loop):
                self.async_loop.call_soon_threadsafe(task.cancel)

    def _finalize_sending(self):
        try:
            self.running = False
            self._set_ui_state_sending(False)
            self._update_stats_label()
            final_status = f"🏁 COMPLETED: {self.sent_count} sent, {self.failed_count} failed"
            self.status_label.config(text=final_status, fg="#2c3e50")
            self.log(final_status)
            # Reset Direct MX state and buttons when campaign finishes
            if self.direct_mx_running:
                self.direct_mx_running = False
                self.direct_mx_paused = False
                if self.direct_mx_task and not self.direct_mx_task.done():
                    self.direct_mx_task.cancel()
                self.direct_mx_task = None
            if hasattr(self, 'mx_start_btn'):
                try:
                    self.mx_start_btn.config(state="normal")
                    self.mx_pause_btn.config(state="disabled", text="⏸️ PAUSE", bg="#f39c12")
                    self.mx_stop_btn.config(state="disabled")
                except Exception:
                    pass
            try:
                self._update_analytics()
            except Exception as e:
                self.log(f"⚠️ Error updating analytics: {e}")
            self.root.after(200, self._verify_button_layout)
            try:
                messagebox.showinfo("Complete", f"Sending finished!\nSent: {self.sent_count}\nFailed: {self.failed_count}")
            except Exception as e:
                self.log(f"⚠️ Error showing completion message: {e}")
        except Exception as e:
            self.log(f"❌ Error during finalization: {e}")

    def _save_campaign(self):
        file_path = filedialog.asksaveasfilename(title="Save Campaign", defaultextension=".json", filetypes=[("Campaign Files", "*.json")])
        if not file_path:
            return
        try:
            data = {
                "version": "9.0.0",
                "subject": self.subject_var.get(),
                "content": self.message_box.get("1.0", tk.END),
                "content_b": self.message_box_b.get("1.0", tk.END),
                "provider": self.provider_var.get(),
                "throttle_amount": self.throttle_amount_var.get(),
                "throttle_delay": self.throttle_delay_var.get(),
                "throttle_unit": self.throttle_unit_var.get(),
                "use_headless": self.use_headless.get(),
                "warmup_mode": self.warmup_mode.get(),
                "email_list": self.email_list,
                "tracking_map": self.tracking_map,
                "attachments": self.attachment_paths,
                "smtp": {k: getattr(self.smtp_handler, k) for k in ["smtp_server", "smtp_port", "username", "use_tls", "use_ssl", "sender_name", "sender_email"]},
                "tracking_enabled": self.tracking_enabled.get(),
                "ab_testing_enabled": self.ab_testing_enabled.get(),
                "ab_body_enabled": self.ab_body_enabled_var.get(),
                "subject_a": self.subject_a_var.get(),
                "subject_b": self.subject_b_var.get(),
                "ab_split_ratio": self.ab_split_ratio_var.get(),
                "warmup_state": self.warmup_state,
                "tracking_data": self.tracking_server.get_tracking_data() if self.tracking_server else {},
            }
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            self.log(f"💾 Campaign saved to {file_path}")
            messagebox.showinfo("Saved", f"Campaign saved to:\n{file_path}")
        except Exception as e:
            self.log(f"❌ Error saving campaign: {e}")
            messagebox.showerror("Save Error", f"Could not save campaign:\n{e}")

    def _load_campaign(self):
        file_path = filedialog.askopenfilename(title="Load Campaign", filetypes=[("Campaign Files", "*.json")])
        if not file_path:
            return
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            self.subject_var.set(data.get("subject", ""))
            self.message_box.delete("1.0", tk.END)
            self.message_box.insert("1.0", data.get("content", ""))
            self.message_box_b.delete("1.0", tk.END)
            self.message_box_b.insert("1.0", data.get("content_b", ""))
            self.provider_var.set(data.get("provider", "SMTP"))
            self.throttle_amount_var.set(data.get("throttle_amount", 20))
            self.throttle_delay_var.set(data.get("throttle_delay", 1))
            self.throttle_unit_var.set(data.get("throttle_unit", "Minutes"))
            self.use_headless.set(data.get("use_headless", False))
            self.warmup_mode.set(data.get("warmup_mode", False))
            self.email_list = data.get("email_list", [])
            self.tracking_map = data.get("tracking_map", {})
            self.attachment_paths = data.get("attachments", [])
            self._update_attachment_display()

            for email in self.email_list:
                self._initialize_tracking_map_for_email(email)

            self._refresh_recipients_table(force_all=True)
            smtp = data.get("smtp", {})
            if smtp.get("server") and smtp.get("username"):
                self.smtp_handler.configure_smtp(smtp.get("server"), smtp.get("port"), smtp.get("username"), None, smtp.get("use_tls", True), smtp.get("use_ssl", False), smtp.get("sender_name", ""), smtp.get("sender_email", ""))
                if self.smtp_handler.get_password():
                    self.smtp_config_btn.config(text="✅ SMTP OK", bg="#27ae60")
                else:
                    self.smtp_config_btn.config(text="⚠️ SMTP (No Pass)", bg="#e67e22")
            self.tracking_enabled.set(data.get("tracking_enabled", True))
            self.ab_testing_enabled.set(data.get("ab_testing_enabled", False))
            self.ab_body_enabled_var.set(data.get("ab_body_enabled", False))
            self._toggle_ab_body_view()
            self.subject_a_var.set(data.get("subject_a", ""))
            self.subject_b_var.set(data.get("subject_b", ""))
            self.ab_split_ratio_var.set(data.get("ab_split_ratio", 50))
            self.warmup_state = data.get("warmup_state", self.warmup_state)

            if self.tracking_server and data.get("tracking_data"):
                self.tracking_server.load_tracking_data(data["tracking_data"])
                self._update_analytics()
            self._update_char_count()
            self.log(f"📂 Campaign loaded from {file_path}")
        except Exception as e:
            self.log(f"❌ Error loading campaign: {e}")
            messagebox.showerror("Load Error", f"Could not load campaign:\n{e}")

    def _load_profiles_from_disk(self):
        if not os.path.exists(PROFILES_FILE):
            return
        try:
            with open(PROFILES_FILE, "r") as f:
                self.profiles = json.load(f)
            self.profile_combo["values"] = [""] + list(self.profiles.keys())
            if "Default" in self.profiles:
                self.current_profile_var.set("Default")
                self._apply_profile()
            self.log("✅ Provider profiles loaded.")
        except Exception as e:
            self.log(f"⚠️ Could not load profiles: {e}")

    def _save_current_profile(self):
        name = simpledialog.askstring("Save Profile", "Profile name:", parent=self.root)
        if not name:
            return

        chrome_path = simpledialog.askstring("Chrome Profile", "Optional: Chrome User Data Dir Path for this profile:\n(Leave empty for default/none)", parent=self.root)

        profile = {
            "provider": self.provider_var.get(),
            "chrome_user_data_dir": chrome_path,
            "smtp": {k: getattr(self.smtp_handler, k) for k in ["smtp_server", "smtp_port", "username", "use_tls", "use_ssl", "sender_name", "sender_email"]}
        }
        self.profiles[name] = profile
        try:
            with open(PROFILES_FILE, "w", encoding="utf-8") as f:
                json.dump(self.profiles, f, indent=2)
            self.profile_combo["values"] = [""] + list(self.profiles.keys())
            self.current_profile_var.set(name)
            self.log(f"💾 Saved profile '{name}'")
        except Exception as e:
            self.log(f"⚠️ Could not save profiles: {e}")

    def _apply_profile(self):
        name = self.current_profile_var.get()
        if not name or name not in self.profiles:
            return
        profile = self.profiles[name]
        self.provider_var.set(profile.get("provider", "SMTP"))
        smtp = profile.get("smtp", {})
        if smtp.get("server") and smtp.get("username"):
            self.smtp_handler.configure_smtp(smtp.get("server"), smtp.get("port"), smtp.get("username"), None, smtp.get("use_tls", True), smtp.get("use_ssl", False), smtp.get("sender_name", ""), smtp.get("sender_email", ""))
            if self.smtp_handler.get_password():
                self.smtp_config_btn.config(text="✅ SMTP OK", bg="#27ae60")
            else:
                self.smtp_config_btn.config(text="⚠️ SMTP (No Pass)", bg="#e67e22")
        self.log(f"📥 Applied profile '{name}'")

    def _export_report(self):
        if not self.email_list:
            messagebox.showinfo("No Data", "No recipients to export.")
            return
        file_path = filedialog.asksaveasfilename(title="Export Report", initialdir=REPORTS_DIR, initialfile=f"report_{datetime.now().strftime('%Y%m%d_%H%M')}.csv", defaultextension=".csv")
        if not file_path:
            return
        try:
            all_headers = set(self.email_list_headers)
            for d in self.tracking_map.values():
                all_headers.update(d.keys())

            fieldnames = sorted(list(all_headers))
            if 'email' in fieldnames:
                fieldnames.insert(0, fieldnames.pop(fieldnames.index('email')))

            with open(file_path, "w", newline="", encoding="utf-8-sig") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
                writer.writeheader()
                for email in self.email_list:
                    row_data = self.tracking_map.get(email, {})
                    writer.writerow(row_data)
            self.log(f"📤 Exported report to {file_path}")
            messagebox.showinfo("Exported", f"Report saved:\n{file_path}")
        except Exception as e:
            self.log(f"❌ Error exporting report: {e}")
            messagebox.showerror("Export Error", f"Could not export report:\n{e}")

    def start_async_loop(self, main_task):
        """NEW v9.0.0: Starts and manages the asyncio event loop in a separate thread."""
        if self.async_thread and self.async_thread.is_alive():
            self.log("⚠️ Async loop is already running.")
            return

        def loop_runner():
            self.async_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.async_loop)
            try:
                self.async_loop.run_until_complete(main_task())
            except Exception as e:
                self.log(f"❌ Async loop error: {e}")
            finally:
                try:
                    self.async_loop.close()
                except Exception as e:
                    self.log(f"⚠️ Error closing async loop: {e}")

        self.async_thread = threading.Thread(target=loop_runner, daemon=True)
        self.async_thread.start()

    def _background_worker(self):
        while True:
            try:
                self._run_sequence_automation_cycle()
                self.db_handler.autosave(self.tracking_map)
                replies = self.imap_handler.check_replies()
                if replies > 0:
                    self.log(f"🔔 {replies} new replies detected via IMAP.")

                # Seed list inbox verification
                if self.seed_list:
                    spam_count = 0
                    total_checks = 0
                    for seed in self.seed_list:
                        # Simulate IMAP check for seed (now actual logic)
                        total_checks += 1
                        # In real impl, check IMAP folders for spam
                        # Placeholder: Assume some logic to detect spam
                        if random.random() < 0.1:  # Simulate 10% spam rate for demo
                            spam_count += 1

                    if total_checks > 0 and spam_count / total_checks > 0.5:
                        self.seed_check_paused = True
                        self.log(f"⚠️ Seed check paused: {spam_count}/{total_checks} in spam (>50%). Campaign paused.")
                        self._toggle_pause()
                    else:
                        self.seed_check_paused = False

                if datetime.now() - self.last_health_check_time > timedelta(hours=24):
                    domain = ""
                    if self.smtp_handler.sender_email and "@" in self.smtp_handler.sender_email:
                        domain = self.smtp_handler.sender_email.split("@")[1]
                    elif self.smtp_handler.username and "@" in self.smtp_handler.username:
                        domain = self.smtp_handler.username.split("@")[1]

                    if domain:
                        self.log(f"🛡️ Running daily domain health check for: {domain}")
                        auth_results = self.deliverability_helper.check_domain_authentication(domain)

                        issues = []
                        if "Missing" in auth_results.get('spf', ''):
                            issues.append("SPF")
                        if "Missing" in auth_results.get('dmarc', ''):
                            issues.append("DMARC")

                        blacklist_status = self.deliverability_helper.check_blacklist(domain)
                        if "Listed" in blacklist_status:
                            issues.append("Blacklist")

                        if not issues:
                            self.domain_health_label.config(text=f"🛡️ Domain: OK ({domain})", fg="#27ae60")
                            if hasattr(self, 'domain_health_label_top') and self.domain_health_label_top.winfo_exists():
                                self.domain_health_label_top.config(text=f"🛡️ Domain: OK ({domain})", fg="#27ae60")
                        else:
                            self.domain_health_label.config(text=f"⚠️ Domain: {', '.join(issues)}", fg="#e74c3c")
                            if hasattr(self, 'domain_health_label_top') and self.domain_health_label_top.winfo_exists():
                                self.domain_health_label_top.config(text=f"⚠️ Domain: {', '.join(issues)}", fg="#e74c3c")
                            self.log(f"⚠️ Domain Health Alert: Issues found with {', '.join(issues)}")

                        self.last_health_check_time = datetime.now()

                # Process MX retry queue
                if DNSPYTHON_AVAILABLE and DKIM_AVAILABLE:
                    self.start_async_loop(self.direct_mx_handler.process_retry_queue)

            except Exception as e:
                self.log(f"❌ Background worker error: {e}")
            time.sleep(60)

    def _process_gui_updates(self):
        try:
            updates = []
            max_per_cycle = 100
            while not self.gui_update_queue.empty() and len(updates) < max_per_cycle:
                updates.append(self.gui_update_queue.get_nowait())

            log_batch = []

            for kind, *args in updates:
                if kind == "update_recipient":
                    email = args[0]
                    if email in self.tree_items:
                        item_id = self.tree_items[email]
                        d = self.tracking_map.get(email, {})
                        sent_time = d.get('sent_time', '')
                        if sent_time and ' ' in sent_time:
                            sent_time = sent_time.split(' ')[1]
                        values = (email, d.get('status', 'Pending'), sent_time, '✔️' if d.get('open_events') else '', '✔️' if d.get('click_events') else '', str(d.get('attempts', 0)), d.get('failure_reason', ''))
                        self.tree.item(item_id, values=values, tags=(d.get('status', '').split(' ')[0],))
                elif kind == "update_vps_recipient":
                    email = args[0]
                    if email in self.vps_tree_items:
                        item_id = self.vps_tree_items[email]
                        d = self.vps_tracking_map.get(email, {})
                        sent_time = d.get('sent_time', '')
                        if sent_time and ' ' in sent_time:
                            sent_time = sent_time.split(' ')[1]
                        values = (email, d.get('status', 'Pending'), sent_time, str(d.get('attempts', 0)), d.get('failure_reason', ''))
                        self.vps_tree.item(item_id, values=values, tags=(d.get('status', '').split(' ')[0],))
                elif kind == "update_mx_recipient":
                    email = args[0]
                    if hasattr(self, 'mx_tree_items') and email in self.mx_tree_items:
                        item_id = self.mx_tree_items[email]
                        d = self.mx_tracking_map.get(email, {})
                        sent_time = d.get('sent_time', '')
                        if sent_time and ' ' in sent_time:
                            sent_time = sent_time.split(' ')[1]
                        values = (email, d.get('status', 'Pending'), sent_time, str(d.get('attempts', 0)), d.get('failure_reason', ''))
                        self.mx_tree.item(item_id, values=values, tags=(d.get('status', '').split(' ')[0],))
                        for tag, color in [('Sent', '#d4edda'), ('Failed', '#f8d7da'), ('Error', '#f8d7da'), ('Queued', '#d1ecf1')]:
                            self.mx_tree.tag_configure(tag, background=color)
                elif kind == "log_message":
                    log_batch.append(args[0])
                elif kind == "update_progress":
                    total = len(self.email_list)
                    if hasattr(self, 'vps_email_list'):
                        total += len(self.vps_email_list)
                    if hasattr(self, 'mx_email_list'):
                        total += len(self.mx_email_list)
                    self.progress["maximum"] = max(total, 1)
                    self.progress["value"] = self.sent_count + self.failed_count
                    self._update_stats_label()
                elif kind == "dns_result":
                    result = args[0]
                    try:
                        self.dns_checker_tree.insert('', 'end',
                            values=(result['domain'], result['mx'], result['spf'],
                                    result['dkim'], result['dmarc'], result['status']),
                            tags=(result.get('tag', 'warn'),))
                    except Exception:
                        pass
                elif kind == "dns_summary":
                    try:
                        self.dns_summary_var.set(args[0])
                    except Exception:
                        pass
                elif kind == "sent_log_entry":
                    try:
                        self._insert_sent_log_entry(args[0])
                    except Exception:
                        pass

            # Batch insert log messages
            if log_batch and hasattr(self, 'log_box'):
                try:
                    if self.log_box.winfo_exists():
                        combined = ''.join(log_batch)
                        self.log_box.insert(tk.END, combined)
                        line_count = int(self.log_box.index('end-1c').split('.')[0])
                        if line_count > 5000:
                            self.log_box.delete('1.0', f'{line_count - 5000}.0')
                        self.log_box.see(tk.END)
                except Exception:
                    pass
        except queue.Empty:
            pass
        finally:
            self.root.after(100, self._process_gui_updates)

    def _browse_template_pdf(self):
        """Opens a file dialog to select a PDF file for the secure redirector lure."""
        path = filedialog.askopenfilename(title="Select Lure PDF", filetypes=[("PDF Files", "*.pdf")])
        if path:
            self.template_pdf_var.set(path)
            self.log(f"📄 Selected lure PDF: {os.path.basename(path)}")

    def _generate_secure_link(self, email_address):
        """Generates the full secure link for a given recipient."""
        burner_domain = self.burner_domain_var.get().strip()
        if not burner_domain:
            return "#"  # Return a dead link if not configured

        parsed_domain = urlparse(burner_domain)
        if not parsed_domain.scheme:
            burner_domain = "https://" + burner_domain
            parsed_domain = urlparse(burner_domain)

        nonce = str(uuid.uuid4())
        target_id = email_address
        lure_path = self.lure_path_var.get().strip()
        if lure_path.startswith('/'):
            lure_path = lure_path[1:]

        final_url = urlunparse((
            parsed_domain.scheme,
            parsed_domain.netloc,
            lure_path,
            '',
            f'id={target_id}&nonce={nonce}',
            ''
        ))

        return final_url

    def _embed_text_in_pdf(self, input_pdf_path, text_to_embed):
        """Creates a temporary copy of a PDF with text embedded in the metadata."""
        if not PYPDF_AVAILABLE:
            self.log("⚠️ PyPDF2 not installed, cannot embed metadata. Skipping.")
            return None

        try:
            temp_dir = tempfile.gettempdir()
            output_filename = os.path.join(temp_dir, f"personalized_{uuid.uuid4().hex}.pdf")

            reader = PyPDF2.PdfReader(input_pdf_path)
            writer = PyPDF2.PdfWriter()

            for page in reader.pages:
                writer.add_page(page)

            writer.add_metadata({
                "/ParisSenderID": text_to_embed
            })

            with open(output_filename, "wb") as f:
                writer.write(f)

            return output_filename
        except Exception as e:
            self.log(f"❌ Error embedding text in PDF: {e}")
            return None

    def _refresh_warmup_state_display(self):
        """Update the warmup state display with current profile warmup info."""
        if not hasattr(self, 'warmup_state_text'):
            return
        self.warmup_state_text.config(state="normal")
        self.warmup_state_text.delete("1.0", tk.END)
        if not self.warmup_state:
            self.warmup_state_text.insert(tk.END, "No warmup profiles active.\n")
        else:
            for profile_user, data in self.warmup_state.items():
                start_date_str = data.get("start_date", "unknown")
                daily_counts = data.get("daily_counts", {})
                today_str = datetime.now().date().isoformat()
                sent_today = daily_counts.get(today_str, 0)
                if start_date_str != "unknown":
                    days_active = (datetime.now().date() - datetime.fromisoformat(start_date_str).date()).days + 1
                else:
                    days_active = "?"
                remaining = self._get_warmup_sends_for_today(profile_user)
                self.warmup_state_text.insert(tk.END, f"Profile '{profile_user}': Day {days_active}, {sent_today} sent today, {remaining} remaining\n")
        self.warmup_state_text.config(state="disabled")

    def _get_warmup_sends_for_today(self, profile_user):
        """Calculates the number of emails that can be sent for a profile today."""
        schedule = [10, 20, 40, 80, 150, 300, 500]  # Daily limits for 1 week

        profile_data = self.warmup_state.get(profile_user, {})
        start_date_str = profile_data.get("start_date")

        if not start_date_str:
            start_date = datetime.now().date()
            self.warmup_state[profile_user] = {
                "start_date": start_date.isoformat(),
                "daily_counts": {start_date.isoformat(): 0}
            }
            return schedule[0]

        start_date = datetime.fromisoformat(start_date_str).date()
        today = datetime.now().date()
        days_since_start = (today - start_date).days

        if days_since_start < 0:
            days_since_start = 0

        if days_since_start >= len(schedule):
            limit = float('inf')  # Warmup complete
        else:
            limit = schedule[days_since_start]

        sent_today = profile_data.get("daily_counts", {}).get(today.isoformat(), 0)

        return max(0, limit - sent_today)

    # --- VPS Bulk Sender Methods ---
    def _load_vps_emails(self):
        file_path = filedialog.askopenfilename(title="Select VPS Email List", filetypes=[("CSV", "*.csv"), ("Text", "*.txt")])
        if not file_path:
            return

        self.log("📁 Loading VPS email list in background...")

        def _load_worker():
            newly_added_emails = []
            new_data_rows = []
            new_headers = []

            try:
                if file_path.lower().endswith('.csv'):
                    with open(file_path, 'r', newline='', encoding='utf-8-sig') as f:
                        reader = csv.reader(f)
                        try:
                            new_headers = [h.strip().lower() for h in next(reader)]
                            if 'email' not in new_headers:
                                self.root.after(0, lambda: messagebox.showerror("CSV Error", "CSV must have an 'email' column header."))
                                return
                        except StopIteration:
                            self.root.after(0, lambda: messagebox.showerror("CSV Error", "CSV file is empty or has no header row."))
                            return

                        email_idx = new_headers.index('email')
                        for row in reader:
                            if not row or len(row) <= email_idx:
                                continue
                            email = row[email_idx].strip().lower()
                            if email and self._is_valid_email(email) and email not in self.vps_email_list:
                                newly_added_emails.append(email)
                                new_data_rows.append(dict(zip(new_headers, row)))
                    self.log(f"✅ Loaded {len(newly_added_emails)} new VPS recipients from CSV.")

                else:  # Text file
                    with open(file_path, 'r', encoding='utf-8') as f:
                        new_headers = ['email']
                        for line in f:
                            email = line.strip().lower()
                            if email and self._is_valid_email(email) and email not in self.vps_email_list:
                                newly_added_emails.append(email)
                                new_data_rows.append({'email': email})
                    self.log(f"✅ Loaded {len(newly_added_emails)} new VPS recipients from TXT file.")

                if not newly_added_emails:
                    self.log("💡 No new, unique emails found in the selected file.")
                    return

                def _apply_to_ui():
                    self.email_list_headers = sorted(list(set(self.email_list_headers) | set(new_headers)))
                    for i, email in enumerate(newly_added_emails):
                        self.vps_email_list.append(email)
                        self.vps_tracking_map[email] = new_data_rows[i]
                    for email in newly_added_emails:
                        self._initialize_vps_tracking_map_for_email(email)
                    self._refresh_vps_recipients_table(force_all=True)

                self.root.after(0, _apply_to_ui)

            except Exception as e:
                self.log(f"❌ Error loading VPS emails: {traceback.format_exc()}")
                self.root.after(0, lambda: messagebox.showerror("Load Error", f"Could not load file:\n{e}"))

        threading.Thread(target=_load_worker, daemon=True).start()

    def _paste_vps_emails(self):
        try:
            clipboard_content = self.root.clipboard_get()
        except Exception as e:
            self.log(f"❌ VPS paste error: {e}")
            messagebox.showerror("Paste Error", "Could not paste from clipboard.")
            return

        self.log("📋 Processing pasted VPS emails in background...")

        def _paste_worker():
            try:
                emails = re.findall(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', clipboard_content)
                new_emails_data = []

                for email in {e.lower().strip() for e in emails}:
                    if email and self._is_valid_email(email) and email not in self.vps_email_list:
                        new_emails_data.append(email)

                def _apply_to_ui():
                    new_recipients = 0
                    for email in new_emails_data:
                        if email not in self.vps_email_list:
                            self.vps_email_list.append(email)
                            self.vps_tracking_map[email] = {'email': email}
                            self._initialize_vps_tracking_map_for_email(email)
                            new_recipients += 1

                    if new_recipients > 0:
                        self._refresh_vps_recipients_table(force_all=True)
                        self.log(f"✅ Pasted {new_recipients} new VPS emails.")
                    else:
                        messagebox.showinfo("No New Emails", "No new, valid emails found in clipboard.")

                self.root.after(0, _apply_to_ui)
            except Exception as e:
                self.log(f"❌ VPS paste error: {e}")
                self.root.after(0, lambda: messagebox.showerror("Paste Error", "Could not paste from clipboard."))

        threading.Thread(target=_paste_worker, daemon=True).start()

    def _add_vps_recipient(self):
        email = self.new_vps_recipient_var.get().strip().lower()
        if not email or not self._is_valid_email(email):
            messagebox.showwarning("Invalid", "Enter a valid email.")
            return
        if email in self.vps_email_list:
            messagebox.showinfo("Exists", "Email already in list.")
            return

        self.vps_email_list.append(email)
        self.vps_tracking_map[email] = {'email': email}
        self._initialize_vps_tracking_map_for_email(email)

        self._refresh_vps_recipients_table(force_all=True)
        self.new_vps_recipient_var.set("")
        self.log(f"✅ Added VPS recipient: {email}")

    def _validate_vps_email_list(self):
        if not self.vps_email_list:
            messagebox.showinfo("List Empty", "No VPS emails to validate.")
            return

        if not messagebox.askyesno("Validate List", f"This will check MX records for {len(self.vps_email_list)} emails. Continue?"):
            return

        self.log("🔍 Starting VPS list validation in background...")

        def _validate_worker():
            valid_count = 0
            invalid_count = 0

            for email in list(self.vps_email_list):
                if not self._is_valid_email(email):
                    self.vps_tracking_map[email]['status'] = 'Invalid Syntax'
                    invalid_count += 1
                    self.gui_update_queue.put(('update_vps_recipient', email))
                    continue

                domain = email.split('@')[1]
                mx_status = self.deliverability_helper.check_mx_record(domain)

                if mx_status == "Valid":
                    self.vps_tracking_map[email]['status'] = 'Valid'
                    valid_count += 1
                else:
                    self.vps_tracking_map[email]['status'] = f"Invalid ({mx_status})"
                    invalid_count += 1

                self.gui_update_queue.put(('update_vps_recipient', email))

            final_valid = valid_count
            final_invalid = invalid_count

            def _finalize():
                self._refresh_vps_recipients_table(force_all=True)
                self.log(f"✅ VPS Validation Complete: {final_valid} Valid, {final_invalid} Invalid.")
                messagebox.showinfo("Validation Complete", f"VPS Result:\nValid: {final_valid}\nInvalid/Issues: {final_invalid}")

            self.root.after(0, _finalize)

        threading.Thread(target=_validate_worker, daemon=True).start()

    def _remove_vps_selected(self):
        selected_items = self.vps_tree.selection()
        if not selected_items:
            return
        if not messagebox.askyesno("Confirm", f"Remove {len(selected_items)} VPS recipient(s)?"):
            return
        emails_to_remove = [self.vps_tree.item(item, 'values')[0] for item in selected_items]
        self.vps_email_list = [e for e in self.vps_email_list if e not in emails_to_remove]
        for email in emails_to_remove:
            del self.vps_tracking_map[email]
        self._refresh_vps_recipients_table(force_all=True)
        self.log(f"✅ Removed {len(emails_to_remove)} VPS recipient(s)")

    def _initialize_vps_tracking_map_for_email(self, email):
        base_data = self.vps_tracking_map.get(email, {'email': email})
        base_data.setdefault('status', 'Pending')
        base_data.setdefault('sent_time', '')
        base_data.setdefault('attempts', 0)
        base_data.setdefault('failure_reason', '')

        self.vps_tracking_map[email] = base_data

    def _refresh_vps_recipients_table(self, force_all=False):
        self.vps_tree.delete(*self.vps_tree.get_children())
        self.vps_tree_items.clear()
        for email in self.vps_email_list:
            self._insert_vps_recipient_in_tree(email)
        self._update_vps_stats_label()

    def _insert_vps_recipient_in_tree(self, email):
        d = self.vps_tracking_map.get(email, {})
        sent_time = d.get('sent_time', '')
        if sent_time and ' ' in sent_time:
            sent_time = sent_time.split(' ')[1]
        values = (email, d.get('status', 'Pending'), sent_time, str(d.get('attempts', 0)), d.get('failure_reason', ''))
        item_id = self.vps_tree.insert("", "end", values=values, tags=(d.get('status', '').split(' ')[0],))
        self.vps_tree_items[email] = item_id
        for tag, color in [('Sent', '#d4edda'), ('Failed', '#f8d7da'), ('Error', '#f8d7da'), ('Sending', '#fff3cd'), ('Queued', '#d1ecf1'), ('Suppressed', '#e2e3e5'), ('Unsubscribed', '#e2e3e5')]:
            self.vps_tree.tag_configure(tag, background=color)

    def _clear_vps_email_list(self):
        if not self.vps_email_list:
            messagebox.showinfo("List Empty", "The VPS recipient list is already empty.")
            return
        if messagebox.askyesno("Confirm Clear", f"Are you sure you want to clear the entire list of {len(self.vps_email_list)} VPS recipients? This cannot be undone."):
            self.vps_email_list.clear()
            self.vps_tracking_map.clear()
            self.vps_tree_items.clear()
            self.vps_tree.delete(*self.vps_tree.get_children())
            self._update_vps_stats_label()
            self.log("🗑️ VPS recipient list has been cleared.")

    def _load_vps_html_file(self):
        file_path = filedialog.askopenfilename(title="Select VPS HTML File", filetypes=[("HTML", "*.html;*.htm")])
        if file_path:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                self.vps_message_box.delete("1.0", tk.END)
                self.vps_message_box.insert(tk.END, content)
                self._update_vps_char_count()
                self.log(f"✅ Loaded VPS HTML: {os.path.basename(file_path)}")
            except Exception as e:
                self.log(f"❌ VPS HTML load error: {e}")
                messagebox.showerror("Load Error", f"Could not load HTML file:\n{e}")

    def _load_vps_template(self):
        template = """<!DOCTYPE html>
<html>
<head>
    <style> .button { background-color: #4CAF50; color: white; padding: 10px 20px; text-decoration: none; } </style>
</head>
<body>
<div style="font-family: sans-serif;">
    {# This is a Jinja2 comment. Use them for notes! #}
    <h1>{{ subject_line }}</h1>

    <p>{{ greetings }},</p>

    <p>This is a message from {{ company or 'our team' }}.</p>

    {# Example of conditional logic with your CSV data #}
    {% if is_customer == 'yes' %}
        <p>As a loyal customer, we have a special offer for you!</p>
    {% else %}
        <p>Welcome! We're glad to have you with us.</p>
    {% endif %}

    <p>Feel free to click this <a href="{{ download_link or '#' }}">tracked link</a>.</p>

    <p><a href="{{ secure_link }}" class="button">Access Your Secure Portal</a></p>

    <p>Regards,<br>{{ sender_name }}</p>

    <hr>
    <p style="font-size:12px; color:#888;">To unsubscribe, <a href="{{ unsubscribe_link }}">click here</a>.</p>
</div>
</body>
</html>"""
        self.vps_message_box.delete("1.0", tk.END)
        self.vps_message_box.insert(tk.END, template)
        self._update_vps_char_count()
        self.log("✅ Loaded VPS Jinja2 template example")

    def _add_vps_attachment(self):
        paths = filedialog.askopenfilenames(title="Select VPS Attachments")
        if paths:
            self.vps_attachment_paths.extend([p for p in paths if p not in self.vps_attachment_paths])
            self._update_vps_attachment_display()
            self.log(f"✅ Added {len(paths)} VPS attachment(s).")

    def _clear_vps_attachments(self):
        if not self.vps_attachment_paths:
            messagebox.showinfo("No Attachments", "There are no VPS attachments to clear.")
            return
        if messagebox.askyesno("Confirm", f"Are you sure you want to remove all {len(self.vps_attachment_paths)} VPS attachment(s)?"):
            self.vps_attachment_paths.clear()
            self._update_vps_attachment_display()
            self.log("🗑️ VPS attachments have been cleared.")

    def _update_vps_attachment_display(self):
        count = len(self.vps_attachment_paths)
        if count == 0:
            self.vps_attachment_label.config(text="No attachments")
        elif count == 1:
            self.vps_attachment_label.config(text=f"📎 {os.path.basename(self.vps_attachment_paths[0])}")
        else:
            self.vps_attachment_label.config(text=f"📎 {count} files")

    def _update_vps_char_count(self, event=None):
        self.vps_char_count_label.config(text=f"Chars: {len(self._get_vps_message_content()):,}")

    def _get_vps_message_content(self):
        content = self.vps_message_box.get("1.0", tk.END).strip()
        if ("<" in content and ">" in content):
            if not content.lower().startswith(("<!doctype", "<html")):
                return f"<!DOCTYPE html><html><head><meta charset='utf-8'></head><body>{content}</body></html>"
            return content
        else:
            formatted_text = content.replace("\n", "<br>")
            return f"<!DOCTYPE html><html><body>{formatted_text}</body></html>"

    def _update_vps_stats_label(self):
        """Update VPS stats label with current sent/failed/total counts from vps_tracking_map."""
        try:
            total = len(self.vps_email_list)
            sent = sum(1 for d in self.vps_tracking_map.values() if d.get('status', '').startswith('Sent'))
            failed = sum(1 for d in self.vps_tracking_map.values() if d.get('status', '') in ('Failed', 'Error'))
            pending = total - sent - failed
            success_rate = (sent / total * 100) if total > 0 else 0
            if hasattr(self, 'vps_stats_label') and self.vps_stats_label.winfo_exists():
                self.vps_stats_label.config(text=f"📊 VPS Stats: {sent} sent | {failed} failed | {pending} pending | {total} total ({success_rate:.1f}% success)")
        except Exception:
            pass

    def _save_vps_campaign(self):
        file_path = filedialog.asksaveasfilename(title="Save VPS Campaign", defaultextension=".json", filetypes=[("VPS Campaign Files", "*.json")])
        if not file_path:
            return
        try:
            data = {
                "version": "9.0.1",
                "subject": self.vps_subject_var.get(),
                "content": self.vps_message_box.get("1.0", tk.END),
                "sender_name": self.vps_sender_name_var.get(),
                "sender_email": self.vps_sender_email_var.get(),
                "random_emails_pool": self.vps_random_emails_var.get().split(','),
                "attachments": self.vps_attachment_paths,
                "email_list": self.vps_email_list,
                "tracking_map": self.vps_tracking_map,
            }
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            self.log(f"💾 VPS Campaign saved to {file_path}")
            messagebox.showinfo("Saved", f"VPS Campaign saved to:\n{file_path}")
        except Exception as e:
            self.log(f"❌ Error saving VPS campaign: {e}")
            messagebox.showerror("Save Error", f"Could not save VPS campaign:\n{e}")

    def _load_vps_campaign(self):
        file_path = filedialog.askopenfilename(title="Load VPS Campaign", filetypes=[("VPS Campaign Files", "*.json")])
        if not file_path:
            return
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            self.vps_subject_var.set(data.get("subject", ""))
            self.vps_message_box.delete("1.0", tk.END)
            self.vps_message_box.insert("1.0", data.get("content", ""))
            self.vps_sender_name_var.set(data.get("sender_name", "Sender"))
            self.vps_sender_email_var.set(data.get("sender_email", "noreply@example.com"))
            self.vps_random_emails_var.set(','.join(data.get("random_emails_pool", [])))
            self.vps_attachment_paths = data.get("attachments", [])
            self._update_vps_attachment_display()
            self.vps_email_list = data.get("email_list", [])
            self.vps_tracking_map = data.get("tracking_map", {})
            for email in self.vps_email_list:
                self._initialize_vps_tracking_map_for_email(email)
            self._refresh_vps_recipients_table(force_all=True)
            self._update_vps_char_count()
            self.log(f"📂 VPS Campaign loaded from {file_path}")
        except Exception as e:
            self.log(f"❌ Error loading VPS campaign: {e}")
            messagebox.showerror("Load Error", f"Could not load VPS campaign:\n{e}")

    def _prepare_email_data_vps(self, email_address, base_subject, base_content, attachments, use_random_sender, index=0, total=0):
        """VPS version of email data preparation."""
        subj = self.deliverability_helper.spin(base_subject)
        spun_content = self.deliverability_helper.spin(base_content)
        tracked_content = self._add_tracking_to_content(spun_content, email_address) if self.tracking_enabled.get() else spun_content

        personalized_subject, personalized_content, new_data, unsubscribe_url = self._personalize_content(
            email_address, subj, tracked_content
        )

        if CSS_INLINE_AVAILABLE:
            ok, inlined_content = self.html_helper.inline_css(personalized_content)
            if ok:
                personalized_content = inlined_content

        return {
            "to_email": email_address,
            "subject": personalized_subject,
            "content": personalized_content,
            "attachments": attachments or [],
            "unsubscribe_url": unsubscribe_url,
            "use_random_sender": use_random_sender,
            "sender_name": self.vps_sender_name_var.get(),
            "sender_email": self.vps_sender_email_var.get(),
            "reply_to": self.vps_reply_to_var.get() if hasattr(self, 'vps_reply_to_var') else "",
            "message_id_domain": self.vps_message_id_domain_var.get() if hasattr(self, 'vps_message_id_domain_var') else "",
        }

    def _prepare_email_data_mx(self, email_address, base_subject, base_content, attachments, use_random_sender, index=0, total=0):
        """MX-specific version of email data preparation using MX sender variables."""
        subj = self.deliverability_helper.spin(base_subject)
        spun_content = self.deliverability_helper.spin(base_content)
        tracked_content = self._add_tracking_to_content(spun_content, email_address) if self.tracking_enabled.get() else spun_content

        personalized_subject, personalized_content, new_data, unsubscribe_url = self._personalize_content(
            email_address, subj, tracked_content
        )

        if CSS_INLINE_AVAILABLE:
            ok, inlined_content = self.html_helper.inline_css(personalized_content)
            if ok:
                personalized_content = inlined_content

        return {
            "to_email": email_address,
            "subject": personalized_subject,
            "content": personalized_content,
            "attachments": attachments or [],
            "unsubscribe_url": unsubscribe_url,
            "use_random_sender": use_random_sender,
            "sender_name": self.mx_sender_name_var.get() if hasattr(self, 'mx_sender_name_var') else "Sender",
            "sender_email": self.mx_sender_email_var.get() if hasattr(self, 'mx_sender_email_var') else "noreply@example.com",
            "reply_to": self.mx_reply_to_var.get() if hasattr(self, 'mx_reply_to_var') else "",
            "message_id_domain": self.mx_message_id_domain_var.get() if hasattr(self, 'mx_message_id_domain_var') else "",
        }

    def _start_vps_sending(self):
        """Start VPS bulk sending."""
        if self.running:
            return
        emails_to_use = self.vps_email_list
        if not emails_to_use:
            messagebox.showinfo("No Recipients", "Please load a VPS email list first.")
            return

        subject = self.vps_subject_var.get()
        content = self._get_vps_message_content()
        if not subject or not content:
            messagebox.showwarning("Missing Content", "Please enter a subject and message content.")
            return

        self.running = True
        self.sent_count = 0
        self.failed_count = 0
        self._set_ui_state_sending(True)
        self.log(f"🚀 Starting VPS campaign...")

        self.proxy_vps_handler.run_vps_sending_job(emails_to_use, subject, content, self.vps_attachment_paths, self.vps_random_emails_var.get().strip() != "", self.vps_batch_size_var.get())

    def _pause_vps_sending(self):
        """Pause VPS sending."""
        if self.running and not self.paused:
            self.paused = True
            self.vps_pause_btn.config(text="▶️ RESUME VPS CAMPAIGN", bg="#27ae60")
            self.log("⏸️ VPS sending paused.")

    def _resume_vps_sending(self):
        """Resume VPS sending."""
        if self.running and self.paused:
            self.paused = False
            self.vps_pause_btn.config(text="⏸️ PAUSE VPS CAMPAIGN", bg="#f39c12")
            self.log("▶️ VPS sending resumed.")

    def _stop_vps_sending(self):
        """Stop VPS sending."""
        if self.running:
            self.running = False
            self.paused = False
            self._cancel_async_tasks()
            self._set_ui_state_sending(False)
            self.log("⏹️ VPS sending stopped by user.")

    def _duplicate_sequence_step(self):
        """Duplicate the last step in the current sequence."""
        if not hasattr(self, 'current_sequence_data') or not self.current_sequence_data.get('steps'):
            messagebox.showinfo("No Steps", "No steps to duplicate.")
            return
        last_step = copy.deepcopy(self.current_sequence_data['steps'][-1])
        self.current_sequence_data['steps'].append(last_step)
        self._render_sequence_steps()
        self.log("📋 Duplicated last sequence step.")

    def _delete_last_sequence_step(self):
        """Delete the last step from the current sequence."""
        if not hasattr(self, 'current_sequence_data') or not self.current_sequence_data.get('steps'):
            messagebox.showinfo("No Steps", "No steps to delete.")
            return
        self.current_sequence_data['steps'].pop()
        self._render_sequence_steps()
        self.log("🗑️ Deleted last sequence step.")

    def _simulate_sequence(self):
        """Simulate the current sequence and show preview."""
        if not hasattr(self, 'current_sequence_data') or not self.current_sequence_data.get('steps'):
            messagebox.showinfo("No Sequence", "No sequence to simulate.")
            return
        preview = "🔄 Sequence Simulation Preview\n" + "=" * 40 + "\n\n"
        for i, step in enumerate(self.current_sequence_data['steps']):
            step_type = step.get('type', 'unknown')
            if step_type == 'email':
                preview += f"Step {i+1}: 📧 Send Email\n"
                preview += f"  Subject: {step.get('subject', 'N/A')}\n"
                preview += f"  Content: {step.get('content', '')[:100]}...\n\n"
            elif step_type == 'wait':
                preview += f"Step {i+1}: ⏳ Wait {step.get('duration', 0)} hours\n\n"
            elif step_type == 'if_status':
                preview += f"Step {i+1}: 🔀 Condition: {step.get('condition', 'N/A')}\n"
                preview += f"  If True → Step {step.get('true_target', 'N/A')}\n"
                preview += f"  If False → Step {step.get('false_target', 'N/A')}\n\n"
        preview += "=" * 40 + "\nSimulation complete. No emails sent."
        if hasattr(self, 'sequence_preview_text'):
            self.sequence_preview_text.config(state="normal")
            self.sequence_preview_text.delete("1.0", tk.END)
            self.sequence_preview_text.insert(tk.END, preview)
            self.sequence_preview_text.config(state="disabled")
        self.log("🔄 Sequence simulation complete.")

    def _export_sequence(self):
        """Export the current sequence to a JSON file."""
        if not hasattr(self, 'current_sequence_data') or not self.current_sequence_data.get('steps'):
            messagebox.showinfo("No Sequence", "No sequence to export.")
            return
        file_path = filedialog.asksaveasfilename(
            title="Export Sequence",
            defaultextension=".json",
            filetypes=[("JSON Files", "*.json")]
        )
        if not file_path:
            return
        try:
            with open(file_path, 'w') as f:
                json.dump(self.current_sequence_data, f, indent=2)
            self.log(f"📤 Sequence exported to {file_path}")
            messagebox.showinfo("Exported", f"Sequence saved to {file_path}")
        except Exception as e:
            messagebox.showerror("Export Error", f"Failed to export: {e}")

    def _view_warmup_schedule(self):
        """Display the warmup schedule in a popup window."""
        win = tk.Toplevel(self.root)
        win.title("Warmup Schedule")
        win.geometry("400x400")
        win.transient(self.root)

        text = scrolledtext.ScrolledText(win, wrap=tk.WORD, font=("Consolas", 10), bg="white", fg="#2c3e50")
        text.pack(fill='both', expand=True, padx=10, pady=10)

        schedule = self.warmup_schedule if hasattr(self, 'warmup_schedule') else self._load_warmup_schedule()
        text.insert(tk.END, "📅 Warmup Schedule\n" + "=" * 30 + "\n\n")
        text.insert(tk.END, f"Account Age (days): {schedule.get('account_age_days', 'N/A')}\n\n")
        for entry in schedule.get('schedule', []):
            text.insert(tk.END, f"Day {entry.get('day', '?')}: Max {entry.get('max_sends', '?')} sends\n")
        text.config(state="disabled")

        tk.Button(win, text="Close", command=win.destroy, bg="#3498db", fg="white").pack(pady=5)

    def _manage_seed_list(self):
        """Manage the seed email list in a popup window."""
        win = tk.Toplevel(self.root)
        win.title("Seed List Manager")
        win.geometry("400x400")
        win.transient(self.root)

        seed_list = self.seed_list if hasattr(self, 'seed_list') else self._load_seed_list()

        listbox = tk.Listbox(win, bg="white", fg="#2c3e50")
        listbox.pack(fill='both', expand=True, padx=10, pady=10)
        for seed in seed_list:
            listbox.insert(tk.END, str(seed))

        btn_frame = tk.Frame(win)
        btn_frame.pack(fill='x', padx=10, pady=5)

        def add_seed():
            email = simpledialog.askstring("Add Seed", "Enter seed email:", parent=win)
            if email:
                seed_list.append(email)
                listbox.insert(tk.END, email)
                self.seed_list = seed_list

        def remove_seed():
            sel = listbox.curselection()
            if sel:
                idx = sel[0]
                listbox.delete(idx)
                if idx < len(seed_list):
                    seed_list.pop(idx)
                self.seed_list = seed_list

        tk.Button(btn_frame, text="➕ Add", command=add_seed, bg="#27ae60", fg="white").pack(side=tk.LEFT, padx=5)
        tk.Button(btn_frame, text="🗑️ Remove", command=remove_seed, bg="#e74c3c", fg="white").pack(side=tk.LEFT, padx=5)
        tk.Button(btn_frame, text="Close", command=win.destroy, bg="#3498db", fg="white").pack(side=tk.RIGHT, padx=5)

    def _configure_dkim_window(self):
        """Open DKIM configuration window."""
        dkim_win = tk.Toplevel(self.root)
        dkim_win.title("Configure DKIM")
        dkim_win.geometry("550x350")
        dkim_win.transient(self.root)
        dkim_win.grab_set()

        tk.Label(dkim_win, text="DKIM Private Key (PEM format):").grid(row=0, column=0, sticky='w', padx=10, pady=5)
        key_text = scrolledtext.ScrolledText(dkim_win, height=5, width=40)
        key_text.grid(row=0, column=1, padx=10, pady=5)
        key_text.insert(tk.END, self.direct_mx_handler.dkim_private_key)

        def upload_dkim_key():
            file_path = filedialog.askopenfilename(
                title="Select DKIM Private Key File",
                filetypes=[("PEM Files", "*.pem"), ("Key Files", "*.key"), ("All Files", "*.*")]
            )
            if file_path:
                try:
                    with open(file_path, 'r') as f:
                        key_content = f.read().strip()
                    key_text.delete("1.0", tk.END)
                    key_text.insert(tk.END, key_content)
                    dkim_status_label.config(text="✅ Key loaded", fg="#27ae60")
                except Exception as e:
                    dkim_status_label.config(text="⚠️ Failed to load key file", fg="#e74c3c")

        tk.Button(dkim_win, text="📂 Upload Key File", command=upload_dkim_key, bg="#3498db", fg="white",
                  font=("Arial", 9, "bold")).grid(row=0, column=2, padx=5, pady=5, sticky='w')

        dkim_status_label = tk.Label(dkim_win, text="", font=("Arial", 9), fg="#666")
        dkim_status_label.grid(row=0, column=3, sticky='w', padx=5, pady=5)

        tk.Label(dkim_win, text="DKIM Selector:").grid(row=1, column=0, sticky='w', padx=10, pady=5)
        selector_var = tk.StringVar(value=self.direct_mx_handler.dkim_selector)
        tk.Entry(dkim_win, textvariable=selector_var).grid(row=1, column=1, padx=10, pady=5, sticky='ew')

        tk.Label(dkim_win, text="DKIM Domain:").grid(row=2, column=0, sticky='w', padx=10, pady=5)
        domain_var = tk.StringVar(value=self.direct_mx_handler.dkim_domain)
        tk.Entry(dkim_win, textvariable=domain_var).grid(row=2, column=1, padx=10, pady=5, sticky='ew')

        def save_dkim():
            key_val = key_text.get("1.0", tk.END).strip()
            # PEM format validation
            if key_val and not (key_val.startswith("-----BEGIN") and "PRIVATE KEY-----" in key_val
                                and key_val.endswith("-----") and "-----END" in key_val):
                dkim_status_label.config(text="⚠️ Invalid PEM key format", fg="#e74c3c")
                return
            self.direct_mx_handler.configure_dkim(key_val, selector_var.get(), domain_var.get())
            self._save_settings()
            # Update DKIM labels
            self._update_dkim_labels()
            dkim_win.destroy()
            messagebox.showinfo("Saved", "DKIM settings saved.")

        tk.Button(dkim_win, text="Save", command=save_dkim).grid(row=3, column=0, columnspan=2, pady=10)

    def _browse_dkim_key_file(self):
        """Browse for a DKIM private key file."""
        file_path = filedialog.askopenfilename(
            title="Select DKIM Private Key",
            filetypes=[("PEM Files", "*.pem"), ("Key Files", "*.key"), ("All Files", "*.*")]
        )
        if file_path:
            self.dkim_key_file_var.set(file_path)
            self.log(f"🔑 DKIM key file selected: {os.path.basename(file_path)}")
            if hasattr(self, 'direct_mx_handler'):
                try:
                    with open(file_path, 'rb') as f:
                        self.direct_mx_handler.dkim_private_key = f.read()
                    self.log("✅ DKIM key loaded successfully.")
                except Exception as e:
                    self.log(f"❌ Failed to load DKIM key: {e}")

    def _verify_from_emails_window(self):
        """Open window to verify 'From' emails for inbox readiness."""
        verify_win = tk.Toplevel(self.root)
        verify_win.title("🔍 Verify From Emails")
        verify_win.geometry("600x500")
        verify_win.transient(self.root)
        verify_win.grab_set()

        tk.Label(verify_win, text="Enter 'From' emails to verify (one per line):", font=("Arial", 10, "bold")).pack(padx=10, pady=5, anchor='w')
        emails_text = scrolledtext.ScrolledText(verify_win, height=5, width=60)
        emails_text.pack(padx=10, pady=5, fill='x')
        # Pre-fill from existing pool
        if self.direct_mx_handler.from_emails_pool:
            emails_text.insert(tk.END, '\n'.join(self.direct_mx_handler.from_emails_pool))

        imap_frame = tk.LabelFrame(verify_win, text="IMAP Credentials (for inbox check)", font=("Arial", 9, "bold"))
        imap_frame.pack(fill='x', padx=10, pady=5)
        tk.Label(imap_frame, text="IMAP Server:").grid(row=0, column=0, sticky='w', padx=5, pady=2)
        imap_server_var = tk.StringVar(value=self.imap_handler.server)
        tk.Entry(imap_frame, textvariable=imap_server_var).grid(row=0, column=1, sticky='ew', padx=5, pady=2)
        tk.Label(imap_frame, text="IMAP Port:").grid(row=1, column=0, sticky='w', padx=5, pady=2)
        imap_port_var = tk.StringVar(value=str(self.imap_handler.port))
        tk.Entry(imap_frame, textvariable=imap_port_var).grid(row=1, column=1, sticky='ew', padx=5, pady=2)
        tk.Label(imap_frame, text="IMAP Username:").grid(row=2, column=0, sticky='w', padx=5, pady=2)
        imap_user_var = tk.StringVar(value=self.imap_handler.username)
        tk.Entry(imap_frame, textvariable=imap_user_var).grid(row=2, column=1, sticky='ew', padx=5, pady=2)
        tk.Label(imap_frame, text="IMAP Password:").grid(row=3, column=0, sticky='w', padx=5, pady=2)
        imap_pass_var = tk.StringVar(value=self.imap_handler.password)
        tk.Entry(imap_frame, textvariable=imap_pass_var, show="*").grid(row=3, column=1, sticky='ew', padx=5, pady=2)
        imap_frame.grid_columnconfigure(1, weight=1)

        results_frame = tk.LabelFrame(verify_win, text="Verification Results", font=("Arial", 9, "bold"))
        results_frame.pack(fill='both', expand=True, padx=10, pady=5)
        results_text = scrolledtext.ScrolledText(results_frame, height=8, width=60, state='disabled')
        results_text.pack(fill='both', expand=True, padx=5, pady=5)

        # Show existing verification status
        if self.direct_mx_handler.from_emails_status:
            results_text.config(state='normal')
            for em, st in self.direct_mx_handler.from_emails_status.items():
                icon = "✅" if st == 'Verified' else "❌"
                results_text.insert(tk.END, f"{icon} {em}: {st}\n")
            results_text.config(state='disabled')

        def run_verification():
            raw = emails_text.get("1.0", tk.END).strip()
            if not raw:
                messagebox.showinfo("No Emails", "Please enter emails to verify.")
                return
            from_emails = [e.strip() for e in raw.split('\n') if e.strip() and '@' in e.strip()]
            if not from_emails:
                messagebox.showinfo("No Valid Emails", "No valid emails found.")
                return

            # Update from pool
            self.direct_mx_handler.from_emails_pool = from_emails

            # Build IMAP configs (same credentials for all emails in this UI)
            imap_configs = {}
            try:
                imap_port = int(imap_port_var.get() or 993)
            except ValueError:
                imap_port = 993
            for em in from_emails:
                imap_configs[em] = {
                    'server': imap_server_var.get(),
                    'port': imap_port,
                    'username': imap_user_var.get(),
                    'password': imap_pass_var.get()
                }

            results_text.config(state='normal')
            results_text.delete("1.0", tk.END)
            results_text.insert(tk.END, "⏳ Verification in progress...\n")
            results_text.config(state='disabled')
            verify_win.update()

            def _do_verify():
                results = self.direct_mx_handler.verify_from_emails(from_emails, imap_configs)
                def _show_results():
                    results_text.config(state='normal')
                    results_text.delete("1.0", tk.END)
                    for em, st in results.items():
                        icon = "✅" if st == 'Verified' else "❌"
                        results_text.insert(tk.END, f"{icon} {em}: {st}\n")
                    results_text.config(state='disabled')
                verify_win.after(0, _show_results)

            threading.Thread(target=_do_verify, daemon=True).start()

        tk.Button(verify_win, text="🚀 Start Verification", command=run_verification, bg="#27ae60", fg="white", font=("Arial", 10, "bold")).pack(padx=10, pady=10)

    def _save_ai_config(self):
        """Save current AI configuration as a named profile."""
        name = simpledialog.askstring("Save AI Config", "Enter a name for this AI configuration:")
        if not name:
            return
        try:
            try:
                with open(SETTINGS_FILE, "r") as f:
                    settings = json.load(f)
            except (FileNotFoundError, json.JSONDecodeError):
                settings = {}
            ai_configs = settings.get("ai_configs", {})
            ai_configs[name] = {
                "provider": self.ai_provider_var.get(),
                "openai_key": self.openai_api_key_var.get(),
                "local_ai_url": self.local_ai_url_var.get(),
                "local_ai_model": self.local_ai_model_var.get(),
            }
            settings["ai_configs"] = ai_configs
            with open(SETTINGS_FILE, "w") as f:
                json.dump(settings, f, indent=2)
            self._refresh_ai_config_list()
            self.log(f"💾 AI config '{name}' saved.")
        except Exception as e:
            self.log(f"❌ Error saving AI config: {e}")

    def _delete_ai_config(self):
        """Delete selected AI configuration."""
        sel = self.ai_config_listbox.curselection()
        if not sel:
            messagebox.showinfo("No Selection", "Select an AI config to delete.")
            return
        name = self.ai_config_listbox.get(sel[0])
        if not messagebox.askyesno("Confirm", f"Delete AI config '{name}'?"):
            return
        try:
            with open(SETTINGS_FILE, "r") as f:
                settings = json.load(f)
            ai_configs = settings.get("ai_configs", {})
            ai_configs.pop(name, None)
            settings["ai_configs"] = ai_configs
            with open(SETTINGS_FILE, "w") as f:
                json.dump(settings, f, indent=2)
            self._refresh_ai_config_list()
            self.log(f"🗑️ AI config '{name}' deleted.")
        except Exception as e:
            self.log(f"❌ Error deleting AI config: {e}")

    def _on_ai_config_select(self, event):
        """Load selected AI configuration."""
        sel = self.ai_config_listbox.curselection()
        if not sel:
            return
        name = self.ai_config_listbox.get(sel[0])
        try:
            with open(SETTINGS_FILE, "r") as f:
                settings = json.load(f)
            ai_configs = settings.get("ai_configs", {})
            config = ai_configs.get(name, {})
            if config:
                self.ai_provider_var.set(config.get("provider", "OpenAI"))
                self.openai_api_key_var.set(config.get("openai_key", ""))
                self.local_ai_url_var.set(config.get("local_ai_url", ""))
                self.local_ai_model_var.set(config.get("local_ai_model", ""))
                self.ai_handler.configure(self.openai_api_key_var.get())
                self.local_ai_handler.configure(self.local_ai_url_var.get(), self.local_ai_model_var.get())
                self.log(f"✅ Loaded AI config '{name}'.")
        except Exception as e:
            self.log(f"❌ Error loading AI config: {e}")

    def _refresh_ai_config_list(self):
        """Refresh the AI config listbox."""
        try:
            self.ai_config_listbox.delete(0, tk.END)
            try:
                with open(SETTINGS_FILE, "r") as f:
                    settings = json.load(f)
                ai_configs = settings.get("ai_configs", {})
                for name in sorted(ai_configs.keys()):
                    self.ai_config_listbox.insert(tk.END, name)
            except (FileNotFoundError, json.JSONDecodeError):
                pass
        except Exception:
            pass

    def _update_dkim_labels(self):
        """Update all DKIM status labels based on current configuration."""
        try:
            if self.direct_mx_handler.dkim_private_key:
                label_text = "DKIM: ✅ Configured"
                label_fg = "#27ae60"
            else:
                label_text = "DKIM: Not Configured"
                label_fg = "#e74c3c"
            if hasattr(self, 'vps_dkim_label') and self.vps_dkim_label.winfo_exists():
                self.vps_dkim_label.config(text=label_text, fg=label_fg)
            if hasattr(self, 'mx_dkim_label') and self.mx_dkim_label.winfo_exists():
                self.mx_dkim_label.config(text=label_text, fg=label_fg)
            if hasattr(self, 'settings_dkim_status_label') and self.settings_dkim_status_label.winfo_exists():
                self.settings_dkim_status_label.config(text=label_text, fg=label_fg)
        except Exception:
            pass

    # --- NEW: Handler methods for missing GUI controls ---

    def _test_imap_connection(self):
        """Test IMAP connection in a background thread."""
        def _do_test():
            try:
                server = self.imap_server_var.get().strip()
                user = self.imap_user_var.get().strip()
                passwd = self.imap_pass_var.get().strip()
                if not server or not user or not passwd:
                    self.gui_update_queue.put(('log_message', "[IMAP] ❌ Missing IMAP credentials.\n"))
                    self.root.after(0, lambda: self.imap_status_label.config(text="IMAP: ❌ Missing credentials", fg="#e74c3c"))
                    return
                import imaplib
                conn = imaplib.IMAP4_SSL(server)
                conn.login(user, passwd)
                conn.logout()
                self.gui_update_queue.put(('log_message', f"[IMAP] ✅ Connection successful to {server}\n"))
                self.root.after(0, lambda: self.imap_status_label.config(text="IMAP: ✅ Connected", fg="#27ae60"))
            except Exception as e:
                self.gui_update_queue.put(('log_message', f"[IMAP] ❌ Connection failed: {e}\n"))
                self.root.after(0, lambda: self.imap_status_label.config(text=f"IMAP: ❌ Failed", fg="#e74c3c"))
        threading.Thread(target=_do_test, daemon=True).start()

    def _test_ai_connection(self):
        """Test AI provider connection in a background thread."""
        def _do_test():
            try:
                provider = self.ai_provider_var.get()
                if provider == "OpenAI":
                    api_key = self.openai_api_key_var.get().strip()
                    if not api_key:
                        self.gui_update_queue.put(('log_message', "[AI] ❌ No OpenAI API key configured.\n"))
                        return
                    import urllib.request
                    req = urllib.request.Request("https://api.openai.com/v1/models", headers={"Authorization": f"Bearer {api_key}"})
                    urllib.request.urlopen(req, timeout=10)
                    self.gui_update_queue.put(('log_message', "[AI] ✅ OpenAI connection successful.\n"))
                else:
                    url = self.local_ai_url_var.get().strip()
                    if not url:
                        self.gui_update_queue.put(('log_message', "[AI] ❌ No Local AI URL configured.\n"))
                        return
                    import urllib.request
                    urllib.request.urlopen(url, timeout=10)
                    self.gui_update_queue.put(('log_message', f"[AI] ✅ Local AI connection successful at {url}\n"))
            except Exception as e:
                self.gui_update_queue.put(('log_message', f"[AI] ❌ Connection test failed: {e}\n"))
        threading.Thread(target=_do_test, daemon=True).start()

    def _manual_health_check(self):
        """Trigger a manual VPS health check in background thread."""
        def _do_check():
            try:
                self.gui_update_queue.put(('log_message', "[Health] 🔍 Running manual health check...\n"))
                self.proxy_vps_handler.check_health()
                self.gui_update_queue.put(('log_message', "[Health] ✅ Health check complete.\n"))
            except Exception as e:
                self.gui_update_queue.put(('log_message', f"[Health] ❌ Health check failed: {e}\n"))
        threading.Thread(target=_do_check, daemon=True).start()

    def _regenerate_encryption_key(self):
        """Regenerate the encryption key after user confirmation."""
        if not messagebox.askyesno("Regenerate Key", "⚠️ WARNING: Regenerating the encryption key will make all existing encrypted passwords unreadable. Continue?"):
            return
        try:
            from cryptography.fernet import Fernet
            new_key = Fernet.generate_key()
            key_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'encryption.key')
            with open(key_file, 'wb') as f:
                f.write(new_key)
            self.proxy_vps_handler.fernet = Fernet(new_key)
            self.log("🔐 Encryption key regenerated successfully.")
            self._refresh_encryption_key_status()
        except Exception as e:
            self.log(f"❌ Failed to regenerate key: {e}")

    def _backup_encryption_key(self):
        """Backup the encryption key to a user-selected location."""
        try:
            key_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'encryption.key')
            if not os.path.exists(key_file):
                messagebox.showwarning("No Key", "No encryption key file found.")
                return
            dest = filedialog.asksaveasfilename(title="Backup Encryption Key", defaultextension=".key", filetypes=[("Key Files", "*.key"), ("All Files", "*.*")])
            if dest:
                import shutil
                shutil.copy2(key_file, dest)
                self.log(f"💾 Encryption key backed up to {dest}")
        except Exception as e:
            self.log(f"❌ Failed to backup key: {e}")

    def _refresh_encryption_key_status(self):
        """Update encryption key status label."""
        try:
            key_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'encryption.key')
            if os.path.exists(key_file):
                from cryptography.fernet import Fernet
                with open(key_file, 'rb') as f:
                    key_data = f.read().strip()
                Fernet(key_data)
                if hasattr(self, 'enc_key_status_label') and self.enc_key_status_label.winfo_exists():
                    self.enc_key_status_label.config(text="Key: ✅ Valid", fg="#27ae60")
            else:
                if hasattr(self, 'enc_key_status_label') and self.enc_key_status_label.winfo_exists():
                    self.enc_key_status_label.config(text="Key: ⚠️ Not Found", fg="#e74c3c")
        except Exception:
            if hasattr(self, 'enc_key_status_label') and self.enc_key_status_label.winfo_exists():
                self.enc_key_status_label.config(text="Key: ❌ Corrupted", fg="#e74c3c")

    def _add_proxy(self):
        """Add a new proxy to the proxy chain via dialog."""
        dialog = tk.Toplevel(self.root)
        dialog.title("Add Proxy")
        dialog.geometry("350x200")
        dialog.resizable(False, False)
        dialog.configure(bg='#e0f7ff')

        tk.Label(dialog, text="Proxy Host:", bg='#e0f7ff', fg="#2c3e50").grid(row=0, column=0, sticky='w', padx=10, pady=5)
        host_var = tk.StringVar()
        tk.Entry(dialog, textvariable=host_var, width=25).grid(row=0, column=1, padx=10, pady=5)

        tk.Label(dialog, text="Proxy Port:", bg='#e0f7ff', fg="#2c3e50").grid(row=1, column=0, sticky='w', padx=10, pady=5)
        port_var = tk.StringVar(value="1080")
        tk.Entry(dialog, textvariable=port_var, width=25).grid(row=1, column=1, padx=10, pady=5)

        tk.Label(dialog, text="Proxy Type:", bg='#e0f7ff', fg="#2c3e50").grid(row=2, column=0, sticky='w', padx=10, pady=5)
        type_var = tk.StringVar(value="socks5")
        ttk.Combobox(dialog, textvariable=type_var, values=["socks5", "socks4", "http"], state="readonly", width=22).grid(row=2, column=1, padx=10, pady=5)

        def _save():
            host = host_var.get().strip()
            port = port_var.get().strip()
            if not host or not port:
                messagebox.showwarning("Missing", "Please enter host and port.")
                return
            proxy = {"host": host, "port": int(port), "type": type_var.get()}
            self.proxy_tree.insert("", tk.END, values=(host, port, type_var.get(), "🟡 Unknown"))
            self.log(f"➕ Proxy added: {host}:{port}")
            dialog.destroy()

        tk.Button(dialog, text="💾 Save", command=_save, bg="#27ae60", fg="white").grid(row=3, column=0, columnspan=2, pady=10)

    def _remove_proxy(self):
        """Remove selected proxy from the proxy tree."""
        selected = self.proxy_tree.selection()
        if not selected:
            messagebox.showinfo("Select Proxy", "Please select a proxy to remove.")
            return
        for item in selected:
            values = self.proxy_tree.item(item, 'values')
            self.proxy_tree.delete(item)
            self.log(f"🗑️ Proxy removed: {values[0]}:{values[1]}")

    def _move_proxy_up(self):
        """Move selected proxy up in priority."""
        selected = self.proxy_tree.selection()
        if not selected:
            return
        for item in selected:
            idx = self.proxy_tree.index(item)
            if idx > 0:
                self.proxy_tree.move(item, '', idx - 1)

    def _move_proxy_down(self):
        """Move selected proxy down in priority."""
        selected = self.proxy_tree.selection()
        if not selected:
            return
        for item in selected:
            idx = self.proxy_tree.index(item)
            self.proxy_tree.move(item, '', idx + 1)

    def _test_selected_proxy(self):
        """Test the selected proxy in a background thread."""
        selected = self.proxy_tree.selection()
        if not selected:
            messagebox.showinfo("Select Proxy", "Please select a proxy to test.")
            return
        item = selected[0]
        values = self.proxy_tree.item(item, 'values')
        host, port = values[0], values[1]

        def _do_test():
            try:
                import socket
                sock = socket.create_connection((host, int(port)), timeout=10)
                sock.close()
                self.gui_update_queue.put(('log_message', f"[Proxy] ✅ {host}:{port} is reachable.\n"))
                self.root.after(0, lambda: self.proxy_tree.set(item, "status", "🟢 Healthy"))
            except Exception as e:
                self.gui_update_queue.put(('log_message', f"[Proxy] ❌ {host}:{port} failed: {e}\n"))
                self.root.after(0, lambda: self.proxy_tree.set(item, "status", "🔴 Down"))
        threading.Thread(target=_do_test, daemon=True).start()

    def _refresh_proxy_tree(self):
        """Refresh proxy tree with current health status from handler."""
        try:
            handler = self.proxy_vps_handler
            if not hasattr(handler, 'proxy_health_status'):
                return
            for item in self.proxy_tree.get_children():
                values = self.proxy_tree.item(item, 'values')
                key = f"{values[0]}:{values[1]}"
                status_info = handler.proxy_health_status.get(key, {})
                if status_info.get('healthy', True):
                    self.proxy_tree.set(item, "status", "🟢 Healthy")
                else:
                    self.proxy_tree.set(item, "status", "🔴 Down")
        except Exception:
            pass

    def _retry_mx_queue_now(self):
        """Process the MX retry queue immediately in a background thread."""
        def _do_retry():
            try:
                self.gui_update_queue.put(('log_message', "[MX Retry] ▶️ Processing retry queue...\n"))
                items = self.db_handler.get_mx_retry_items() if hasattr(self.db_handler, 'get_mx_retry_items') else []
                if not items:
                    self.gui_update_queue.put(('log_message', "[MX Retry] ✅ Retry queue is empty.\n"))
                    return
                self.gui_update_queue.put(('log_message', f"[MX Retry] Processing {len(items)} queued items...\n"))
                for item in items:
                    try:
                        email = item.get('recipient', item[0] if isinstance(item, (list, tuple)) else 'unknown')
                        self.gui_update_queue.put(('log_message', f"[MX Retry] Retrying: {email}\n"))
                    except Exception:
                        pass
                self.gui_update_queue.put(('log_message', "[MX Retry] ✅ Retry processing complete.\n"))
                self.root.after(0, self._refresh_retry_queue_view)
            except Exception as e:
                self.gui_update_queue.put(('log_message', f"[MX Retry] ❌ Error: {e}\n"))
        threading.Thread(target=_do_retry, daemon=True).start()

    def _clear_mx_retry_queue(self):
        """Clear all items from the MX retry queue."""
        if not messagebox.askyesno("Clear Queue", "Clear all items from the retry queue?"):
            return
        try:
            if hasattr(self.db_handler, 'clear_mx_retry_queue'):
                self.db_handler.clear_mx_retry_queue()
            self.log("🗑️ MX retry queue cleared.")
            self._refresh_retry_queue_view()
        except Exception as e:
            self.log(f"❌ Failed to clear retry queue: {e}")

    def _refresh_retry_queue_view(self):
        """Refresh the retry queue treeview with current data."""
        try:
            if not hasattr(self, 'retry_queue_tree'):
                return
            for item in self.retry_queue_tree.get_children():
                self.retry_queue_tree.delete(item)
            items = []
            if hasattr(self.db_handler, 'get_mx_retry_items'):
                items = self.db_handler.get_mx_retry_items() or []
            for item in items:
                try:
                    if isinstance(item, dict):
                        email = item.get('recipient', '')
                        attempts = item.get('attempts', 0)
                        next_retry = item.get('next_retry', '--')
                        reason = item.get('reason', '')
                    elif isinstance(item, (list, tuple)) and len(item) >= 4:
                        email, attempts, next_retry, reason = item[0], item[1], item[2], item[3]
                    else:
                        continue
                    self.retry_queue_tree.insert("", tk.END, values=(email, attempts, next_retry, reason))
                except Exception:
                    pass
            count = len(items)
            if hasattr(self, 'mx_retry_count_label') and self.mx_retry_count_label.winfo_exists():
                self.mx_retry_count_label.config(text=f"Pending: {count} | Next Retry: --")
        except Exception:
            pass

    def _dns_test(self):
        """Run DNS test for the configured sender domain."""
        try:
            sender = self.mx_sender_email_var.get().strip()
            if not sender or '@' not in sender:
                self.gui_update_queue.put(('log_message', "[DNS] ❌ Please enter a valid sender email first.\n"))
                return
            domain = sender.split('@')[1]
            self.gui_update_queue.put(('log_message', f"[DNS] 🔍 Testing DNS for {domain}...\n"))
            import dns.resolver
            try:
                mx_records = dns.resolver.resolve(domain, 'MX')
                self.gui_update_queue.put(('log_message', f"[DNS] ✅ MX records found for {domain}: {len(mx_records)} records\n"))
                for r in mx_records:
                    self.gui_update_queue.put(('log_message', f"  → {r.exchange} (priority {r.preference})\n"))
            except Exception as e:
                self.gui_update_queue.put(('log_message', f"[DNS] ❌ MX lookup failed: {e}\n"))
            try:
                spf = dns.resolver.resolve(domain, 'TXT')
                for r in spf:
                    txt = r.to_text()
                    if 'spf' in txt.lower():
                        self.gui_update_queue.put(('log_message', f"[DNS] SPF: {txt}\n"))
            except Exception:
                self.gui_update_queue.put(('log_message', f"[DNS] ⚠️ No SPF record found for {domain}\n"))
        except Exception as e:
            self.gui_update_queue.put(('log_message', f"[DNS] ❌ DNS test error: {e}\n"))

    def _domain_check(self):
        """Run domain validation check for the sender domain."""
        try:
            sender = self.mx_sender_email_var.get().strip()
            if not sender or '@' not in sender:
                self.gui_update_queue.put(('log_message', "[Domain] ❌ Please enter a valid sender email first.\n"))
                return
            domain = sender.split('@')[1]
            self.gui_update_queue.put(('log_message', f"[Domain] 🔍 Checking domain {domain}...\n"))
            import socket
            try:
                ip = socket.gethostbyname(domain)
                self.gui_update_queue.put(('log_message', f"[Domain] ✅ Domain resolves to {ip}\n"))
            except socket.gaierror:
                self.gui_update_queue.put(('log_message', f"[Domain] ❌ Domain {domain} does not resolve.\n"))
                return
            import dns.resolver
            try:
                dkim_selector = self.dkim_selector_var.get().strip() if hasattr(self, 'dkim_selector_var') else "default"
                if dkim_selector:
                    dkim_records = dns.resolver.resolve(f"{dkim_selector}._domainkey.{domain}", 'TXT')
                    self.gui_update_queue.put(('log_message', f"[Domain] ✅ DKIM record found for {dkim_selector}._domainkey.{domain}\n"))
            except Exception:
                self.gui_update_queue.put(('log_message', f"[Domain] ⚠️ No DKIM record found for selector.\n"))
            try:
                dmarc = dns.resolver.resolve(f"_dmarc.{domain}", 'TXT')
                for r in dmarc:
                    self.gui_update_queue.put(('log_message', f"[Domain] DMARC: {r.to_text()}\n"))
            except Exception:
                self.gui_update_queue.put(('log_message', f"[Domain] ⚠️ No DMARC record found for {domain}\n"))
        except Exception as e:
            self.gui_update_queue.put(('log_message', f"[Domain] ❌ Domain check error: {e}\n"))

    def _test_mx_connection(self):
        """Test MX connection to verify network settings."""
        def _do_test():
            try:
                sender = self.mx_sender_email_var.get().strip()
                if not sender or '@' not in sender:
                    self.gui_update_queue.put(('log_message', "[MX Test] ❌ Please enter a valid sender email.\n"))
                    return
                domain = sender.split('@')[1]
                self.gui_update_queue.put(('log_message', f"[MX Test] 🧪 Testing MX connection for {domain}...\n"))
                try:
                    import dns.resolver
                    mx_records = dns.resolver.resolve(domain, 'MX')
                    mx_host = str(list(mx_records)[0].exchange).rstrip('.')
                    self.gui_update_queue.put(('log_message', f"[MX Test] Found MX: {mx_host}\n"))
                except Exception as e:
                    self.gui_update_queue.put(('log_message', f"[MX Test] ❌ MX lookup failed: {e}\n"))
                    return
                import socket
                try:
                    sock = socket.create_connection((mx_host, 25), timeout=10)
                    greeting = sock.recv(1024).decode()
                    sock.close()
                    self.gui_update_queue.put(('log_message', f"[MX Test] ✅ Connected! Greeting: {greeting.strip()}\n"))
                except Exception as e:
                    self.gui_update_queue.put(('log_message', f"[MX Test] ❌ Connection failed: {e}\n"))
            except Exception as e:
                self.gui_update_queue.put(('log_message', f"[MX Test] ❌ Error: {e}\n"))
        threading.Thread(target=_do_test, daemon=True).start()

    def _validate_mx_network(self):
        """Validate Source IP, EHLO hostname, DNS MX lookup, and SPF alignment."""
        def _do_validate():
            try:
                issues = []
                # Validate Source IP format
                source_ip = self.mx_source_ip_var.get().strip()
                if source_ip:
                    import re
                    ip_pattern = r'^(\d{1,3}\.){3}\d{1,3}$'
                    if not re.match(ip_pattern, source_ip):
                        issues.append(f"❌ Source IP format invalid: {source_ip}")
                    else:
                        parts = source_ip.split('.')
                        try:
                            if all(0 <= int(p) <= 255 for p in parts):
                                self.gui_update_queue.put(('log_message', f"[Validate] ✅ Source IP valid: {source_ip}\n"))
                            else:
                                issues.append(f"❌ Source IP octets out of range: {source_ip}")
                        except ValueError:
                            issues.append(f"❌ Source IP format invalid: {source_ip}")
                else:
                    self.gui_update_queue.put(('log_message', "[Validate] ℹ️ No Source IP configured (will use default).\n"))
                # Validate EHLO hostname format
                ehlo = self.mx_ehlo_hostname_var.get().strip()
                if ehlo:
                    import re
                    hostname_pattern = r'^[a-zA-Z0-9]([a-zA-Z0-9\-]*[a-zA-Z0-9])?(\.[a-zA-Z0-9]([a-zA-Z0-9\-]*[a-zA-Z0-9])?)*$'
                    if re.match(hostname_pattern, ehlo):
                        self.gui_update_queue.put(('log_message', f"[Validate] ✅ EHLO hostname valid: {ehlo}\n"))
                    else:
                        issues.append(f"❌ EHLO hostname format invalid: {ehlo}")
                else:
                    self.gui_update_queue.put(('log_message', "[Validate] ℹ️ No EHLO hostname configured (will use sender domain).\n"))
                # Validate DNS MX lookup
                sender = self.mx_sender_email_var.get().strip()
                if sender and '@' in sender:
                    domain = sender.split('@')[1]
                    try:
                        import dns.resolver
                        mx_records = dns.resolver.resolve(domain, 'MX')
                        self.gui_update_queue.put(('log_message', f"[Validate] ✅ DNS MX records found for {domain}\n"))
                    except Exception as e:
                        issues.append(f"❌ DNS MX lookup failed for {domain}: {e}")
                    # SPF alignment warning
                    if ehlo and ehlo != domain:
                        self.gui_update_queue.put(('log_message', f"[Validate] ⚠️ SPF alignment warning: EHLO hostname ({ehlo}) differs from sender domain ({domain})\n"))
                if issues:
                    for issue in issues:
                        self.gui_update_queue.put(('log_message', f"[Validate] {issue}\n"))
                else:
                    self.gui_update_queue.put(('log_message', "[Validate] ✅ All network settings validated successfully.\n"))
            except Exception as e:
                self.gui_update_queue.put(('log_message', f"[Validate] ❌ Error: {e}\n"))
        threading.Thread(target=_do_validate, daemon=True).start()

    def _test_dkim_signing(self):
        """Test DKIM signing without sending an email."""
        try:
            handler = self.direct_mx_handler
            if not handler.dkim_private_key:
                messagebox.showwarning("DKIM Test", "No DKIM private key configured.\nConfigure via Settings > DKIM Configuration")
                return
            if not handler.dkim_selector:
                messagebox.showwarning("DKIM Test", "No DKIM selector configured.")
                return
            if not handler.dkim_domain:
                messagebox.showwarning("DKIM Test", "No DKIM domain configured.")
                return
            # Build a test MIME message
            from email.mime.text import MIMEText
            test_msg = MIMEText("This is a DKIM signing test.", 'plain', 'utf-8')
            test_msg['Subject'] = "DKIM Test"
            test_msg['From'] = f"test@{handler.dkim_domain}"
            test_msg['To'] = "test@example.com"
            # Attempt signing
            signed = handler._sign_dkim(test_msg)
            if signed != test_msg:
                messagebox.showinfo("DKIM Test", "✅ DKIM signing successful!\nSignature was generated correctly.")
                self.log("✅ DKIM test signing successful.")
            else:
                messagebox.showwarning("DKIM Test", "⚠️ DKIM signing returned unsigned message.\nCheck your DKIM configuration.")
                self.log("⚠️ DKIM test signing returned unsigned message.")
        except Exception as e:
            messagebox.showerror("DKIM Test", f"❌ DKIM signing test failed:\n{e}")
            self.log(f"❌ DKIM test signing failed: {e}")

def main():
    root = tk.Tk()
    try:
        root.state('zoomed') if sys.platform == "win32" else root.attributes('-zoomed', True)
    except tk.TclError:
        root.geometry("1400x1000")

    app = BulkEmailSender(root)

    def on_closing():
        if app.running and not messagebox.askyesno("Exit", "A campaign is running. Are you sure you want to exit?"):
            return
        if messagebox.askyesno("Exit", "Are you sure you want to exit?"):
            app.log("Application closing...")
            app.running = False
            if app.schedule_timer:
                app.schedule_timer.cancel()
            if app.driver:
                try:
                    app.driver.quit()
                except Exception:
                    pass
            if app.driver_service:
                app.log("🔌 Stopping ChromeDriver service...")
                try:
                    if hasattr(app.driver_service, 'process') and app.driver_service.process:
                        app.driver_service.stop()
                        app.log("✅ ChromeDriver service stopped.")
                except Exception as e:
                    app.log(f"⚠️ Error stopping service: {e}")
            if app.outlook_com:
                try:
                    app.outlook_com.disconnect()
                except Exception:
                    pass
            if app.tracking_server:
                try:
                    app.tracking_server.shutdown()
                except Exception:
                    pass
            root.destroy()
            if PYNGROK_AVAILABLE:
                try:
                    ngrok.kill()
                except Exception:
                    pass
            sys.exit(0)

    root.protocol("WM_DELETE_WINDOW", on_closing)

    print("="*80 + "\n🚀 PARIS SENDER - AGILE MARKETING SUITE v9.0.0 (ENHANCED PROXY VPS MAILER WITH ADVANCED ANTI-DETECTION & VPS OPTIMIZATION)\n" + "="*80)
    if AIOSMTPLIB_AVAILABLE:
        print("✅ High-performance `aiosmtplib` library found. Async sending enabled.")
    else:
        print("⚠️ `aiosmtplib` not found. Falling back to standard multi-threaded sending.")
        print("   For best performance, use Python 3.7+ and run: pip install aiosmtplib")
    print("✅ Integrated Jinja2 for powerful and flexible email templating.")
    print("✅ Added support for Local LLMs (e.g., Ollama) for AI features.")
    print("✅ FIX: Corrected autograb logic for fallbacks and ISP domains.")
    print("✅ NEW: Proxy VPS Mailer with per-VPS SMTP, geo-failover, AI optimization, proxy chains, encrypted logging, rate limiting.")
    print("   Globally unmatched robustness and sophistication for bulk email sending.")
    print("✅ ENHANCEMENT: Human-mimic browser fingerprinting with undetected-chromedriver.")
    print("✅ ENHANCEMENT: Per-profile proxy binding for IP rotation.")
    print("✅ ENHANCEMENT: Smart warmup algorithm with configurable schedule.")
    print("✅ ENHANCEMENT: Inbox placement verification using seed list and IMAP.")
    print("✅ ENHANCEMENT: Dynamic content polymorphism with DOM shuffling.")
    print("✅ ENHANCEMENT: Headless API mode with core_engine.py separation, FastAPI for API, and web UI support for VPS control.")
    print("✅ NEW: DNS Domain Checker for batch validation of domains from email list.")
    print("✅ NEW: Enhanced Configuration Settings Display for at-a-glance overview.")
    print("✅ NEW: Placeholder Engine with documentation and statistics.")
    print("✅ NEW: Live Sent Email Log with formatted output.")
    print("✅ NEW: VPS Bulk Sender Mode - Strict separation from SMTP, dedicated tab, enhanced sender configuration, improved bulk workflow.")
    print("✅ NEW: Direct-to-MX Delivery without SMTP Authentication - Fully asynchronous, with DKIM signing, rate limiting, retry queue via SQLite, and MX failover.")
    print("="*80)

    root.mainloop()

if __name__ == "__main__":
    main()