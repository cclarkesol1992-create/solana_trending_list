import sys
import os
import time
import json
import random
import re
import logging
import requests 
import aiohttp
from datetime import datetime
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton, ForceReply
from telegram.ext import (
    Application, CommandHandler, MessageHandler, ConversationHandler,
    CallbackQueryHandler, TypeHandler, filters, CallbackContext, ContextTypes
)
from telegram.error import NetworkError, TimedOut, RetryAfter, BadRequest
from telegram.request import HTTPXRequest

import websockets


try:
    import asyncio
    import nest_asyncio
    nest_asyncio.apply()
except ImportError:
    pass


from collections import defaultdict, deque

# --- Early debug shims (ensure names exist before first use) ---
if 'DEEP_DEBUG' not in globals():
    DEEP_DEBUG = False  # default; later code may override
if 'logger' not in globals():
    logger = logging.getLogger(__name__)

# --- Early globals so linters don't complain & early refs are safe ---
# Pylance flagged DEEP_DEBUG/logger as undefined because we reference them
# above their full configuration. Define safe defaults here; later config
# (logging.basicConfig + DEEP_DEBUG assignment) will override as needed.
if 'DEEP_DEBUG' not in globals():
    DEEP_DEBUG = False
try:
    logger  # type: ignore[name-defined]
except Exception:
    import logging as _logging  # local alias to avoid shadowing later imports
    logger = _logging.getLogger(__name__)


HELIUS_API_KEY = "e60ecfcc-f4c7-4f79-b796-903ceea4076e"

# --- RPC HTTPS resolver (avoid 429s on public mainnet-beta) ---
def _resolve_rpc_https() -> str:
    """
    Resolve the HTTPS RPC endpoint. Prefer SOL_RPC override, then Helius with API key.
    Never returns the public mainnet-beta URL.
    """
    # 1) Explicit override
    ov = (os.environ.get("SOL_RPC") or "").strip()
    if ov:
        return ov

    # 2) Helius with key
    key = (globals().get("HELIUS_API_KEY") or os.environ.get("HELIUS_API_KEY") or "").strip()
    if key:
        return f"https://mainnet.helius-rpc.com/?api-key={key}"

    # 3) Helius without key (still avoids public RPC)
    return "https://mainnet.helius-rpc.com/"

# Set a single source of truth for HTTPS RPC usage across the file
RPC_HTTPS = _resolve_rpc_https()
if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
    try:
        _key = (globals().get("HELIUS_API_KEY") or os.environ.get("HELIUS_API_KEY") or "").strip()
        def _mask(k: str) -> str:
            return ("***" + k[-6:]) if (isinstance(k, str) and len(k) >= 6) else "***"
        _dbg_url = str(RPC_HTTPS)
        if _key:
            _dbg_url = _dbg_url.replace(_key, _mask(_key))
        logger.debug("[RPC][CONFIG] RPC_HTTPS resolved -> %s (source=%s)",
                     _dbg_url,
                     "ENV" if os.environ.get("RPC_HTTPS") else ("HELIUS" if _key else "PUBLIC"))
    except Exception:
        pass

# --- Startup snapshot for credentials wiring (masked) ---
try:
    if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
        _k = (globals().get("HELIUS_API_KEY") or os.environ.get("HELIUS_API_KEY") or "").strip()
        _masked_k = ("***" + _k[-6:]) if isinstance(_k, str) and len(_k) >= 6 else ("present" if _k else "missing")
        _recv = str(globals().get("SOL_RECEIVE_ADDRESS") or "").strip()
        _recv_disp = (_recv[:6] + "…" + _recv[-6:]) if len(_recv) > 14 else (_recv or "missing")
        logger.debug("[BOOT][CREDS] HELIUS_API_KEY=%s SOL_RECEIVE_ADDRESS=%s", _masked_k, _recv_disp)
except Exception:
    pass

# --- Robust JSON-RPC POST with 429-aware backoff ---
def _post_json_with_backoff(url: str,
                            payload: dict,
                            *,
                            max_retries: int = 6,
                            base_sleep: float = 0.5,
                            timeout: float = 12.0) -> dict | None:
    """
    POST JSON to `url` with 429 + 5xx handling.
    - Honors `Retry-After` header on 429 if present.
    - Exponential backoff with jitter.
    - Returns parsed JSON dict on HTTP 200; otherwise None.
    """
    import math
    for attempt in range(max_retries):
        try:
            r = requests.post(url, json=payload, timeout=timeout, headers={"Content-Type": "application/json"})
            # Happy path
            if r.status_code == 200:
                try:
                    return r.json()
                except Exception:
                    return None

            # Rate limited: respect Retry-After if present
            if r.status_code == 429:
                retry_after_hdr = r.headers.get("Retry-After")
                try:
                    retry_after = float(retry_after_hdr) if retry_after_hdr else None
                except Exception:
                    retry_after = None
                # Fallback to exp backoff if header not present
                sleep_s = retry_after if (retry_after is not None) else (base_sleep * (2 ** attempt))
                # jitter (±20%)
                jitter = 0.2 * sleep_s
                sleep_s = max(0.1, sleep_s + (random.random() * 2 * jitter - jitter))
                if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                    logger.debug("[RPC][BACKOFF] 429 on %s; sleeping %.2fs (Retry-After=%s, attempt=%s/%s)",
                                 url.split("?")[0], sleep_s, retry_after_hdr, attempt + 1, max_retries)
                time.sleep(sleep_s)
                continue

            # Transient server errors: 5xx -> backoff
            if 500 <= r.status_code < 600:
                sleep_s = base_sleep * (2 ** attempt)
                jitter = 0.2 * sleep_s
                sleep_s = max(0.1, sleep_s + (random.random() * 2 * jitter - jitter))
                if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                    logger.debug("[RPC][BACKOFF] %s on %s; sleeping %.2fs (attempt=%s/%s)",
                                 r.status_code, url.split("?")[0], sleep_s, attempt + 1, max_retries)
                time.sleep(sleep_s)
                continue

            # Unexpected status: do not retry aggressively
            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                logger.debug("[RPC][BACKOFF] Non-retryable status %s from %s; aborting.",
                             r.status_code, url.split("?")[0])
            return None

        except Exception as e:
            # Network errors: backoff and retry
            sleep_s = base_sleep * (2 ** attempt)
            jitter = 0.2 * sleep_s
            sleep_s = max(0.1, sleep_s + (random.random() * 2 * jitter - jitter))
            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                logger.debug("[RPC][BACKOFF][ERR] %s on %s; sleeping %.2fs (attempt=%s/%s)",
                             e, url.split("?")[0], sleep_s, attempt + 1, max_retries)
            time.sleep(sleep_s)
            continue
    return None

# --- Base58-ish public key validator (lightweight, avoids Pylance undefined) ---
def _is_valid_pubkey(s: str) -> bool:
    """Best-effort check for a Solana pubkey string.
    Accepts 32–44 chars of base58 alphabet (no 0,O,I,l). Format-only; no RPC.
    """
    try:
        if not isinstance(s, str):
            return False
        s = s.strip()
        if not (32 <= len(s) <= 44):
            return False
        base58 = set("123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz")
        return all(c in base58 for c in s)
    except Exception:
        return False

 # Deep debug toggle: default OFF in production.
# Set environment variable DEEP_DEBUG=1 to re-enable very verbose logging.
DEEP_DEBUG = False
try:
    _env_dd = os.environ.get("DEEP_DEBUG")
    if _env_dd is not None:
        DEEP_DEBUG = bool(int(str(_env_dd).strip() or "0"))
except Exception:
    # If env parsing fails, keep the safe default (False)
    DEEP_DEBUG = False

logging.basicConfig(
    level=logging.DEBUG if DEEP_DEBUG else logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler("solana_trending.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
promo_lock = asyncio.Lock()

# ====== Error Handler ======
async def error_handler(update: object, context: CallbackContext) -> None:
    """Centralized PTB error handler with deep debug breadcrumbs."""
    err = context.error
    # Deep debug: capture chat & user if available
    chat_id_dbg = None
    user_id_dbg = None
    try:
        if getattr(update, "effective_chat", None):
            chat_id_dbg = getattr(update.effective_chat, "id", None)
        if getattr(update, "effective_user", None):
            user_id_dbg = getattr(update.effective_user, "id", None)
    except Exception:
        pass

    logger.warning(f"[TG ERROR] {type(err).__name__}: {err}")
    if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
        try:
            logger.debug("[TG ERROR][CTX] chat_id=%s user_id=%s update_has_msg=%s update_has_cb=%s",
                         chat_id_dbg, user_id_dbg,
                         bool(getattr(update, "message", None)),
                         bool(getattr(update, "callback_query", None)))
        except Exception:
            pass

    # Gracefully swallow common transient errors
    if isinstance(err, (NetworkError, TimedOut)):
        return
    if isinstance(err, RetryAfter):
        await asyncio.sleep(getattr(err, "retry_after", 5))
        return

    # For anything else, don't crash the app — try to notify the user
    try:
        if update and hasattr(context, "bot"):
            chat_id = None
            if hasattr(update, "effective_chat") and update.effective_chat:
                chat_id = update.effective_chat.id
            if chat_id:
                await context.bot.send_message(chat_id, "⚠️ Temporary bot error. Please try again.")
    except Exception:
        pass

BOT_TOKEN = "7934906127:AAHM43poc3k2TCEOXHjttczf50D00ll9Mek"
GROUP_CHAT_IDS = ["-1002758844913"]

# --- Chat id normalization / expansion ---------------------------------------
def _expand_chat_targets(targets):
    """
    Yield a de-duplicated sequence of chat identifiers to try.
    Handles:
      - raw ints / numeric strings (groups)
      - supergroups/channels that require the -100 prefix
      - @usernames for channels
    Strategy:
      - Try given id as-is
      - If purely numeric and |id| < 1e11, also try "-100<abs(id)>" (Telegram channel/supergroup form)
    """
    seen = set()
    def _emit(x):
        k = str(x)
        if k not in seen:
            seen.add(k)
            yield x

    if not isinstance(targets, (list, tuple, set)):
        targets = [targets]

    for raw in targets:
        # Pass through @username form directly
        if isinstance(raw, str) and raw.startswith("@"):
            for v in _emit(raw):
                yield v
            continue

        # Always try the provided value first
        try_first = None
        try:
            try_first = int(str(raw))
        except Exception:
            try_first = raw
        for v in _emit(try_first):
            yield v

        # If numeric and likely missing the -100 prefix, try with it as well
        try:
            s = str(raw)
            neg = s.startswith("-")
            digits = s[1:] if neg else s
            if digits.isdigit():
                n = int(s)
                # Telegram channels/supergroups typically use -100<id>
                if abs(n) < 10**11:
                    prefixed = int(f"-100{abs(n)}")
                    for v in _emit(prefixed):
                        yield v
        except Exception:
            pass
ADMIN_USER_IDS = [6920754307]

# --- Robust admin/user id helpers ---
def is_admin_user(user_id) -> bool:
    """
    Normalize user_id to int and check against ADMIN_USER_IDS.
    Accepts ints, strings, or dicts with an 'id' field.
    """
    try:
        if isinstance(user_id, dict):
            user_id = user_id.get("id") or user_id.get("user_id")
        return int(str(user_id)) in {int(x) for x in ADMIN_USER_IDS}
    except Exception:
        return False

def get_user_id_from_update(update: Update) -> int:
    """
    Robustly extract the Telegram user id from an Update.
    Works for message and callback_query updates; falls back to 0.
    """
    try:
        if getattr(update, "effective_user", None) and getattr(update.effective_user, "id", None) is not None:
            return int(update.effective_user.id)
        if getattr(update, "message", None) and getattr(update.message, "from_user", None):
            return int(update.message.from_user.id)
        if getattr(update, "callback_query", None) and getattr(update.callback_query, "from_user", None):
            fu = update.callback_query.from_user
            return int(fu["id"] if isinstance(fu, dict) else fu.id)
    except Exception:
        pass
    return 0

def get_user_id_from_callback(query) -> int:
    """
    Robustly extract the Telegram user id from a CallbackQuery object (or dict).
    """
    try:
        if hasattr(query, "from_user") and query.from_user is not None:
            fu = query.from_user
            return int(fu["id"] if isinstance(fu, dict) else fu.id)
        if isinstance(query, dict):
            fu = query.get("from") or query.get("from_user") or {}
            return int(fu.get("id"))
    except Exception:
        pass
    return 0

SOL_RECEIVE_ADDRESS = "HtEZi6eUFJnZKfLi2PPdYEPKhYYYyGBBqcsUJAUbwgkk"
PROMO_JSON_PATH = "active_promotions.json"
BUY_CHECK_INTERVAL = 45
BUYER_MEMORY = 10

# --- Global USD thresholds for LIVE BUY alerts (apply everywhere) ---
# If set to 0 or <= 0, that bound is disabled.
MIN_ALERT_AMOUNT_USD = 0.50
MAX_ALERT_AMOUNT_USD = 15000.0
if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
    try:
        logger.debug("[ALERT][USD-THRESHOLDS] MIN=%.2f MAX=%.2f", MIN_ALERT_AMOUNT_USD, MAX_ALERT_AMOUNT_USD)
    except Exception:
        pass
# Increase per-minute alert cap so live-buy posts are not suppressed during bursts.
# Set to a higher value (or 0 to disable rate limiting) if you expect many buys.
MAX_ALERTS_PER_MINUTE = 5  # live alert rate limit (per minute)

# --- Live buy separator (format-only; safe for manual tweaking) ---
# This only controls the visual separator line used in LIVE BUY trending alerts.
# You can freely edit the characters in LIVE_BUY_SEPARATOR_LINE without affecting
# buttons or any other logic.
LIVE_BUY_SEPARATOR_LINE = "━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# --- Alert rate limiter (wired to MAX_ALERTS_PER_MINUTE) ---
from collections import deque as _deque_alerts
_ALERT_TS_Q: _deque_alerts[float] = _deque_alerts(maxlen=600)  # keep ~ last 10 minutes if needed

def _allow_alert_now() -> bool:
    """
    True if an alert may be sent now based on MAX_ALERTS_PER_MINUTE; otherwise False.
    MAX_ALERTS_PER_MINUTE <= 0 disables alerts.
    """
    try:
        max_per_min = int(globals().get("MAX_ALERTS_PER_MINUTE", 0) or 0)
    except Exception:
        max_per_min = 0
    if max_per_min <= 0:
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[ALERT][RATE] MAX_ALERTS_PER_MINUTE=%s => suppress all alerts.", max_per_min)
        return False
    now = time.time()
    try:
        while _ALERT_TS_Q and (now - float(_ALERT_TS_Q[0])) > 60.0:
            _ALERT_TS_Q.popleft()
    except Exception:
        pass
    if len(_ALERT_TS_Q) >= max_per_min:
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[ALERT][RATE] Suppressing alert (%s in last 60s, max=%s).", len(_ALERT_TS_Q), max_per_min)
        return False
    _ALERT_TS_Q.append(now)
    return True

HELIUS_PROJECT_ID = "af47f4d3-787e-4b70-a0b6-c0ea4509d03e" 
DEXSCREENER_MIN_BUYS_DELTA = 1  # alert when buys in last 5m increases by ≥ this
DS_LAST = {}  # {mint: {"buys_m5": int, "pair": str}}

# --- Auto payment detection state (WS account balance deltas) ---
if 'PENDING_BOOKINGS' not in globals():
    PENDING_BOOKINGS: dict[str, dict] = {}
if '_PAY_ADDR_LAST_LAMPORTS' not in globals():
    _PAY_ADDR_LAST_LAMPORTS: int | None = None
if 'AMOUNT_EPSILON' not in globals():
    AMOUNT_EPSILON: float = 0.0005  # acceptable SOL delta when matching payments

async def _payment_poll_job(context: CallbackContext) -> None:
    """
    Periodic safety net for auto payment detection.

    This job:
      - Fetches current lamports for SOL_RECEIVE_ADDRESS via RPC
      - Computes positive deltas vs _PAY_ADDR_LAST_LAMPORTS
      - Matches delta against PENDING_BOOKINGS within AMOUNT_EPSILON
      - Finalizes matching booking exactly as the WS accountNotification path

    It shares the same global state (PENDING_BOOKINGS, _PAY_ADDR_LAST_LAMPORTS,
    AMOUNT_EPSILON), so there is no second queue or duplicated booking flow.
    """
    try:
        addr = str(globals().get("SOL_RECEIVE_ADDRESS") or "").strip()
        if not addr:
            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                logger.debug("[PAY][POLL] SOL_RECEIVE_ADDRESS not set; skipping.")
            return

        # Fetch current lamports via cheap getAccountInfo RPC helper
        lam = _rpc_get_account_lamports(addr)
        if lam is None:
            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                logger.debug("[PAY][POLL] _rpc_get_account_lamports(%s) -> None; skipping.", addr[:6])
            return

        global _PAY_ADDR_LAST_LAMPORTS
        prev = _PAY_ADDR_LAST_LAMPORTS
        _PAY_ADDR_LAST_LAMPORTS = int(lam)

        # First observation becomes baseline
        if prev is None:
            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                logger.debug("[PAY][POLL][BASELINE] lamports=%s (%.9f SOL)", lam, lam / 1_000_000_000.0)
            return

        delta_lam = int(lam) - int(prev)
        if delta_lam <= 0:
            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                logger.debug("[PAY][POLL][NO-INCR] prev=%s now=%s delta=%s", prev, lam, delta_lam)
            return

        delta_sol = float(delta_lam) / 1_000_000_000.0
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[PAY][POLL][DELTA] +%s lamports (%.9f SOL)", delta_lam, delta_sol)

        # Ignore dust (< epsilon/5) to avoid false positives
        _eps = float(globals().get("AMOUNT_EPSILON", 0.0005))
        if delta_sol < max(0.00001, _eps / 5.0):
            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                logger.debug("[PAY][POLL][DUST] %.9f SOL < threshold; ignoring.", delta_sol)
            return

        # Match against pending bookings (within epsilon), SAME behaviour as WS path
        pending = dict(PENDING_BOOKINGS) if 'PENDING_BOOKINGS' in globals() else {}
        if not pending:
            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                logger.debug("[PAY][POLL][UNMATCHED] No PENDING_BOOKINGS present.")
            return

        matched_pid = None
        matched_pb = None
        try:
            candidates = []
            for pid, pb in pending.items():
                want = float(pb.get("amount_sol") or 0.0)
                if abs(delta_sol - want) <= _eps:
                    candidates.append((int(pb.get("created") or 0), pid, pb))
            if candidates:
                candidates.sort(reverse=True)  # newest first
                _, matched_pid, matched_pb = candidates[0]
        except Exception:
            matched_pid, matched_pb = None, None

        if not matched_pid:
            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                logger.debug("[PAY][POLL][UNMATCHED] No pending booking matched %.9f SOL (eps=%.6f)", delta_sol, _eps)
            return

        # Finalize booking (identical to WS accountNotification finalize block)
        try:
            PENDING_BOOKINGS.pop(matched_pid, None)
        except Exception:
            pass
        pb = matched_pb or {}

        try:
            slot = int(pb.get("slot"))
        except Exception:
            slot = None
        if slot is None:
            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                logger.debug("[PAY][POLL][FINALIZE][MISS] no slot in pb=%s", pb)
            return

        promo = {
            "token_name": pb.get("token_name"),
            "token_address": pb.get("token_address"),
            "pair_address": pb.get("pair_address"),
            "booked_by": int(pb.get("user_id") or 0),
            "start_time": int(time.time()),
            "duration": float((TRENDING_SLOTS.get(slot) or {}).get("duration") or 0.0),
        }
        promo["end_time"] = int(promo["start_time"] + promo["duration"] * 3600)

        promos = clear_expired_promotions() or {}
        promos[str(slot)] = promo
        save_promotions(promos)

        # Release slot held by the payer now that payment is confirmed
        try:
            release_slot(slot, int(pb.get("user_id") or 0), force=True)
            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                logger.debug("[HOLD][RELEASE][POLL] payment-confirmed -> slot=%s uid=%s",
                             slot, int(pb.get("user_id") or 0))
        except Exception:
            pass

        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[PAY][POLL][FINALIZE] slot=%s promo=%s",
                         slot, {k: promo.get(k) for k in ("token_name","token_address","pair_address","duration")})

        # Update pin & broadcast, using context.bot
        try:
            await update_pinned_trending_message(context.bot, int(pb.get("user_id") or 0))
        except Exception as e:
            logger.warning("[PAY][POLL][PIN][ERR] %s", e)
        try:
            asyncio.create_task(_broadcast_trending_announcement(slot, promo, context.bot))
        except Exception as e:
            logger.warning("[PAY][POLL][BROADCAST][ERR] %s", e)

        # Notify payer
        try:
            uid = int(pb.get("user_id") or 0)
            if uid:
                await context.bot.send_message(
                    chat_id=uid,
                    text="✅ Payment received. Your token is now trending!",
                    parse_mode="HTML",
                    disable_web_page_preview=True
                )
        except Exception as e:
            logger.warning("[PAY][POLL][USER-NOTIFY][ERR] %s", e)

        logger.info("[PAY][POLL] Auto-confirmed booking for slot %s via %.9f SOL deposit.", slot, delta_sol)
    except Exception as e:
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[PAY][POLL][ERR] %s", e)


# --- Duplicate booking guards (prevent the same token being booked twice concurrently) ---
def _norm_mint(addr: str | None) -> str:
    """Normalize a token address/mint to a comparable lowercase string."""
    try:
        return str(addr or "").strip().lower()
    except Exception:
        return ""

def is_token_trending_or_pending(token_address: str | None) -> tuple[bool, str, int | None]:
    """
    Return (True, reason, slot) if the supplied token is *already* trending or pending payment,
    otherwise (False, "", None).

    - Checks active promotions (after clearing expired ones)
    - Checks PENDING_BOOKINGS awaiting payment
    """
    try:
        mint_lc = _norm_mint(token_address)
        if not mint_lc:
            return False, "", None

        # 1) Active promos (remove expired first)
        promos = {}
        try:
            promos = clear_expired_promotions() or {}
        except Exception:
            # fallback if clear function is unavailable in this scope
            try:
                promos = load_promotions() or {}
            except Exception:
                promos = {}

        for _slot_str, _p in (promos or {}).items():
            try:
                _slot = int(_slot_str)
            except Exception:
                _slot = None
            if _norm_mint((_p or {}).get("token_address")) == mint_lc:
                if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                    try:
                        logger.debug("[BOOK][DUP-GUARD] token already trending in slot=%s", _slot)
                    except Exception:
                        pass
                return True, "already trending", _slot

        # 2) Pending payments (someone started booking and hasn't finished paying yet)
        pend = dict(globals().get("PENDING_BOOKINGS") or {})
        for _pid, _pb in (pend or {}).items():
            if _norm_mint((_pb or {}).get("token_address")) == mint_lc:
                if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                    try:
                        logger.debug("[BOOK][DUP-GUARD] token has a pending booking id=%s slot=%s", _pid, (_pb or {}).get("slot"))
                    except Exception:
                        pass
                try:
                    s = int((_pb or {}).get("slot"))
                except Exception:
                    s = None
                return True, "pending payment exists", s

        return False, "", None
    except Exception as e:
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            try:
                logger.debug("[BOOK][DUP-GUARD][ERR] %s", e)
            except Exception:
                pass
        # On error, do not block (fail open)
        return False, "", None

def assert_not_duplicate_booking(token_address: str | None) -> None:
    """
    Raise a ValueError if the token is already trending or has a pending booking.
    This is designed to be called at the start of any booking flow *before* holding a slot
    or creating an entry in PENDING_BOOKINGS.

    Example (inside your booking flow):
        try:
            assert_not_duplicate_booking(user_supplied_token_address)
        except ValueError as e:
            await bot.send_message(chat_id=user_id, text=f"⚠️ {e}")
            return
    """
    ok, reason, slot = is_token_trending_or_pending(token_address)
    if ok:
        slot_msg = f" (slot {slot})" if slot is not None else ""
        raise ValueError(f"This token is {reason}{slot_msg}. You can't book the same token twice at the same time.")

async def _abort_if_duplicate_token(update: Update, context: CallbackContext, token_address: str, *, state: dict | None = None) -> bool:
    """
    If the token is already trending or has a pending booking, stop the flow,
    release any held slot, notify the user, and return True (blocked).
    """
    try:
        assert_not_duplicate_booking(token_address)
        return False  # OK: not a duplicate
    except ValueError as e:
        # Release any held slot for this user
        try:
            s = None
            if state and state.get("slot") is not None:
                s = int(state["slot"])
            elif context and getattr(context, "user_data", None):
                s = int(context.user_data.get("booking_slot")) if context.user_data.get("booking_slot") is not None else None
            if s is not None:
                release_slot(s, get_user_id_from_update(update), force=True)
                if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                    logger.debug("[BOOK][DUP-BLOCK] Released held slot=%s due to duplicate token.", s)
        except Exception as _e:
            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                logger.debug("[BOOK][DUP-BLOCK][RELEASE-ERR] %s", _e)

        # Tell the user and stop this conversation path
        try:
            chat_id = update.effective_chat.id if getattr(update, "effective_chat", None) else None
            if chat_id:
                await context.bot.send_message(chat_id=chat_id, text=f"⚠️ {e}")
        except Exception as _e:
            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                logger.debug("[BOOK][DUP-BLOCK][MSG-ERR] %s", _e)
        return True

# --- Slot Hold (no timers; NOT "booked", just unavailable to others) ---
# Maps slot -> user_id
SLOT_HELD_BY: dict[int, int] = {}

def is_slot_held(slot: int) -> bool:
    try:
        return int(slot) in SLOT_HELD_BY
    except Exception:
        return False

def slot_held_by_other(slot: int, user_id: int) -> bool:
    try:
        uid = SLOT_HELD_BY.get(int(slot))
        return (uid is not None) and (int(uid) != int(user_id))
    except Exception:
        return False

def hold_slot(slot: int, user_id: int) -> None:
    """Mark a slot as temporarily held by a user.

    NOTE: This helper only updates the in-memory hold state. Any timers
    (e.g. 5-minute auto-release for '⏳ Being booked') must be scheduled
    by the async booking handler that calls this function, where `context`
    and the full user scope are available.
    """
    try:
        SLOT_HELD_BY[int(slot)] = int(user_id)
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[HOLD][SET] slot=%s by user=%s", slot, user_id)
    except Exception as e:
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[HOLD][SET][ERR] %s", e)

def release_slot(slot: int, user_id: int | None = None, *, force: bool = False) -> None:
    try:
        s = int(slot)
        if s not in SLOT_HELD_BY:
            return
        if force or (user_id is None) or (int(SLOT_HELD_BY.get(s)) == int(user_id)):
            SLOT_HELD_BY.pop(s, None)
            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                logger.debug("[HOLD][DEL] slot=%s by user=%s force=%s", slot, user_id, force)
    except Exception as e:
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[HOLD][DEL][ERR] %s", e)

async def _cb_booking_info_timeout(context: CallbackContext) -> None:
    """Auto-release slot if user does not finish booking info within 4 minutes.

    This function is designed to be scheduled via JobQueue. It assumes
    `job.data = {"user_id": <int>, "slot": <int>, "chat_id": <int-optional>}`.
    """
    job = getattr(context, "job", None)
    data = getattr(job, "data", {}) if job else {}
    uid = int(data.get("user_id") or 0)
    slot = int(data.get("slot") or 0)
    chat_id = int(data.get("chat_id") or 0) if data.get("chat_id") else uid

    if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
        try:
            logger.debug("[BOOK][TIMEOUT][JOB] fired uid=%s slot=%s chat_id=%s", uid, slot, chat_id)
        except Exception:
            pass

    if not uid or not slot:
        return

    # If booking already moved into payment waiting, do nothing
    try:
        for pid, pb in (PENDING_BOOKINGS or {}).items():
            if int(pb.get("user_id") or 0) == uid and int(pb.get("slot") or 0) == slot:
                if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                    logger.debug("[BOOK][TIMEOUT][SKIP] uid=%s slot=%s already in PENDING_BOOKINGS (pid=%s)", uid, slot, pid)
                return
    except Exception:
        pass

    # Release slot
    try:
        release_slot(slot, uid, force=True)
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[BOOK][TIMEOUT][RELEASE] slot=%s uid=%s", slot, uid)
    except Exception as e:
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[BOOK][TIMEOUT][RELEASE-ERR] %s", e)

    # Clear any booking deadline state
    try:
        if '_clear_booking_deadline' in globals():
            _clear_booking_deadline(context)
    except Exception:
        pass

    # Notify user with ADMIN clickable link
    try:
        await context.bot.send_message(
            chat_id=chat_id,
            text=(
                "⌛ Your booking expired because it wasn't completed within 4 minutes. "
                "Please /start again.\n\n"
                "If you are having any problems, contact <a href=\"https://t.me/CryptoBoss0011\">ADMIN</a>."
            ),
            parse_mode="HTML",
        )
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[BOOK][TIMEOUT][NOTIFY] uid=%s slot=%s", uid, slot)
    except Exception as e:
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[BOOK][TIMEOUT][NOTIFY-ERR] %s", e)

# --- Booking-info deadline (4 minutes) helpers --------------------------------
BOOKING_INFO_DEADLINE_SECONDS = 4 * 60


def _set_booking_deadline(context: CallbackContext, slot: int, user_id: int, chat_id: int | None = None) -> None:
    """Start (or reset) the 4-minute booking-info deadline for a user/slot.

    - Stores an absolute expiry timestamp in context.user_data
    - Schedules a JobQueue callback to `_cb_booking_info_timeout` after 4 minutes
    - Ensures no duplicate jobs with the same name remain active
    """
    try:
        uid = int(user_id)
        s = int(slot)
        cid = int(chat_id) if chat_id is not None else uid

        # Persist deadline metadata on this context
        ud = getattr(context, "user_data", {})
        expires_at = time.time() + BOOKING_INFO_DEADLINE_SECONDS
        ud["_booking_deadline_ts"] = float(expires_at)
        ud["_booking_deadline_slot"] = s
        ud["_booking_deadline_user"] = uid
        ud["_booking_deadline_chat"] = cid

        # Cancel any existing job with the same name
        jq = getattr(context, "job_queue", None)
        job_name = f"bookinfo:{uid}:{s}"
        if jq is not None:
            try:
                for j in jq.get_jobs_by_name(job_name) or []:
                    try:
                        j.schedule_removal()
                    except Exception:
                        pass
            except Exception:
                pass
            # Schedule fresh 4-minute timeout
            try:
                j = jq.run_once(
                    _cb_booking_info_timeout,
                    when=BOOKING_INFO_DEADLINE_SECONDS,
                    name=job_name,
                    data={"user_id": uid, "slot": s, "chat_id": cid},
                )
                ud["_booking_deadline_job_name"] = job_name
                if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                    logger.debug(
                        "[BOOK][DEADLINE][SET] uid=%s slot=%s chat_id=%s expires_in=%ss",
                        uid, s, cid, BOOKING_INFO_DEADLINE_SECONDS,
                    )
            except Exception as e:
                if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                    logger.debug("[BOOK][DEADLINE][SET-ERR] %s", e)
    except Exception as e:
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[BOOK][DEADLINE][SET-ERR-OUTER] %s", e)

def _booking_deadline_expired(context: CallbackContext) -> bool:
    """Return True if the current context's 4-minute booking deadline has passed."""
    try:
        ud = getattr(context, "user_data", {})
        ts = float(ud.get("_booking_deadline_ts") or 0.0)
        if not ts:
            return False
        expired = (time.time() > ts)
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG and expired:
            try:
                logger.debug("[BOOK][DEADLINE][CHK] expired ts=%s now=%s", ts, time.time())
            except Exception:
                pass
        return expired
    except Exception:
        return False


def _clear_booking_deadline(context: CallbackContext) -> None:
    """Cancel any scheduled booking-info deadline job and clear local markers."""
    try:
        ud = getattr(context, "user_data", {})
        job_name = ud.pop("_booking_deadline_job_name", None)
        jq = getattr(context, "job_queue", None)
        if job_name and jq is not None:
            try:
                for j in jq.get_jobs_by_name(job_name) or []:
                    try:
                        j.schedule_removal()
                    except Exception:
                        pass
            except Exception:
                pass
        for k in ("_booking_deadline_ts", "_booking_deadline_slot", "_booking_deadline_user", "_booking_deadline_chat"):
            try:
                ud.pop(k, None)
            except Exception:
                pass
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            try:
                logger.debug("[BOOK][DEADLINE][CLEAR] job_name=%s", job_name)
            except Exception:
                pass
    except Exception as e:
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[BOOK][DEADLINE][CLEAR-ERR] %s", e)


async def _handle_booking_timeout(update: Update, context: CallbackContext) -> None:
    """Shared timeout handler when user tries to continue after 4 minutes.

    - Releases the held slot
    - Clears deadline + booking state
    - Notifies the user in the current chat
    """
    try:
        uid = get_user_id_from_update(update)
        ud = getattr(context, "user_data", {})
        booking = ud.get("booking") or {}
        slot = booking.get("slot") or ud.get("booking_slot")
        try:
            s = int(slot) if slot is not None else 0
        except Exception:
            s = 0

        # Release slot if any
        if s:
            try:
                release_slot(s, uid, force=True)
                if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                    logger.debug("[BOOK][DEADLINE][HANDLE] release slot=%s uid=%s", s, uid)
            except Exception as e:
                if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                    logger.debug("[BOOK][DEADLINE][HANDLE-RELEASE-ERR] %s", e)

        # Clear deadline and booking state
        try:
            _clear_booking_deadline(context)
        except Exception:
            pass
        try:
            ud.pop("booking", None)
        except Exception:
            pass

        # Notify the user (with ADMIN link)
        try:
            chat_id = update.effective_chat.id if getattr(update, "effective_chat", None) else uid
            await context.bot.send_message(
                chat_id=chat_id,
                text=(
                    "⌛ Your booking expired because it wasn't completed within 4 minutes. "
                    "Please /start again.\n\n"
                    "If you are having any problems, contact "
                    "<a href=\"https://t.me/CryptoBoss0011\">ADMIN</a>."
                ),
                parse_mode="HTML",
            )
        except Exception as e:
            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                logger.debug("[BOOK][DEADLINE][HANDLE-NOTIFY-ERR] %s", e)

    except Exception as e:
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[BOOK][DEADLINE][HANDLE-ERR] %s", e)

# === Helius / WS (guarded so we don't duplicate if already present) ===
if 'USE_HELIUS_WS' not in globals():
    USE_HELIUS_WS = True  # master toggle for WebSocket listener

if 'HELIUS_WS_COMMITMENT' not in globals():
    HELIUS_WS_COMMITMENT = "confirmed"  # or 'finalized'

# Configure Helius WS URL (Standard mainnet) if not already set; avoid referencing undefined names for linters
if not globals().get('HELIUS_WS_URL'):
    HELIUS_WS_URL = f"wss://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
    if globals().get('DEEP_DEBUG'):
        try:
            _masked_key = None
            try:
                _masked_key = ("***" + str(HELIUS_API_KEY)[-6:]) if isinstance(HELIUS_API_KEY, str) and len(HELIUS_API_KEY) >= 6 else "present"
            except Exception:
                _masked_key = "present"
            _masked_url = str(HELIUS_WS_URL).replace(str(HELIUS_API_KEY), _masked_key) if isinstance(HELIUS_WS_URL, str) else HELIUS_WS_URL
            logger.debug("[WS][CONFIG] HELIUS_WS_URL set to Standard endpoint: %s", _masked_url)
        except Exception:
            pass

# --- Pump.fun program id (from helius.dev docs) ---
PUMP_FUN_PROGRAM_ID = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"

if 'DEX_PROGRAM_IDS' not in globals():
    # --- All major Solana DEX program IDs (as of 2025) ---
    # Includes Raydium (AMM & CLMM), Orca, Meteora (DLMM), Pump.fun, etc.
    DEX_PROGRAM_IDS = {
        # --- Raydium (core) ---
        "AMM55ShsF7hW8o6rT1b7X9qS3V5oqpQhYgGxZ4m6B5g": "Raydium AMM",
        "CLMMuWJzFhQdHh4FqF2P8N1pVjoAf8cqvud5Qw9H1oB": "Raydium CLMM",

        # --- Orca (whirlpools) ---
        "whirLbMiicVdio4qv9QDUZrZbYSEZ4Zb7bZ6RkB8iFs": "Orca Whirlpool",

        # --- Meteora (DLMM) ---
        "DLMMvZP8wjHuT8nThUZQYzGhHb5A9Dp9SRi6T8E7Q7j": "Meteora DLMM",

        # --- Pump.fun (from helius.dev docs) ---
        PUMP_FUN_PROGRAM_ID: "Pump.fun",
    }

    # Deep-debug: immediately sanitize obvious bad pubkeys at config-time (format-only check).
    try:
        _raw_ids = list(DEX_PROGRAM_IDS.keys())
        _filtered: dict[str, str] = {}
        for _k, _v in list(DEX_PROGRAM_IDS.items()):
            _ok = True
            try:
                # If a helper exists, use it; otherwise do a light-weight format check (base58-ish length).
                if '_is_valid_pubkey' in globals() and callable(globals().get('_is_valid_pubkey')):
                    _ok = bool(_is_valid_pubkey(_k))
                else:
                    _s = str(_k).strip()
                    _ok = (32 <= len(_s) <= 44) and all(c.isalnum() for c in _s) and ("0" not in set(_s))  # crude but safe
            except Exception:
                _ok = False
            if _ok:
                _filtered[_k] = _v
            else:
                if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                    try:
                        logger.debug("[WS][CONFIG] Dropping invalid-format DEX program id: %s", _k)
                    except Exception:
                        pass

        if len(_filtered) != len(DEX_PROGRAM_IDS):
            DEX_PROGRAM_IDS = _filtered  # type: ignore
            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                try:
                    logger.debug("[WS][CONFIG] Filtered DEX_PROGRAM_IDS: %d -> %d", len(_raw_ids), len(DEX_PROGRAM_IDS))
                except Exception:
                    pass
    except Exception as _e:
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            try:
                logger.debug("[WS][CONFIG] DEX_PROGRAM_IDS sanitize err: %s", _e)
            except Exception:
                pass

# --- Optional RPC-backed validator (drops program ids that are NOT accounts) ---
# Uses public RPC HTTPS (or configured RPC_HTTPS) to call getAccountInfo on each id once at startup.
def _validate_program_ids_via_rpc(ids: list[str] | dict[str, str]) -> list[str]:
    """
    Return only those ids that resolve to an existing account via JSON-RPC getAccountInfo.
    Safe fallback: if RPC call fails, returns the original ids unchanged.
    """
    try:
        # Accept dict or list
        _ids = list(ids.keys()) if isinstance(ids, dict) else list(ids)
        if not _ids:
            return []

        rpc_url = str(
    globals().get("RPC_HTTPS")
    or f"https://mainnet.helius-rpc.com/?api-key={(globals().get('HELIUS_API_KEY') or os.environ.get('HELIUS_API_KEY') or '').strip()}"
).strip()
        headers = {"Content-Type": "application/json"}
        valid: list[str] = []

        for pid in _ids:
            try:
                payload = {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "getAccountInfo",
                    "params": [pid, {"encoding": "base64"}],
                }
                resp = requests.post(rpc_url, headers=headers, json=payload, timeout=5)
                if resp.status_code == 200:
                    data = resp.json()
                    value = ((data or {}).get("result") or {}).get("value")
                    if value:
                        valid.append(pid)
                    else:
                        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                            logger.debug("[WS][BUILD] Dropping program id with no account: %s", pid)
                else:
                    if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                        logger.debug("[WS][BUILD] RPC %s for %s returned %s", "getAccountInfo", pid, resp.status_code)
            except Exception as e:
                if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                    logger.debug("[WS][BUILD] Dropping invalid program id %s (%s)", pid, e)
        return valid if valid else _ids
    except Exception as e:
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[WS][BUILD] _validate_program_ids_via_rpc err: %s (leaving ids unchanged)", e)
        # On any error, do not mutate the configured ids.
        return list(ids.keys()) if isinstance(ids, dict) else list(ids)

# One-time hardening: refine DEX_PROGRAM_IDS using RPC existence check (non-fatal).
try:
    _pre = list((globals().get("DEX_PROGRAM_IDS") or {}).keys())
    _post = _validate_program_ids_via_rpc(DEX_PROGRAM_IDS)
    if set(_post) != set(_pre):
        # Rebuild dict preserving names for those that remain.
        DEX_PROGRAM_IDS = {k: v for k, v in (globals().get("DEX_PROGRAM_IDS") or {}).items() if k in set(_post)}  # type: ignore
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[WS][CONFIG] RPC-validated DEX_PROGRAM_IDS shrank: %d -> %d", len(_pre), len(_post))
    else:
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[WS][CONFIG] RPC-validated DEX_PROGRAM_IDS unchanged: %d ids", len(_pre))
except Exception as _e:
    if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
        logger.debug("[WS][CONFIG] DEX_PROGRAM_IDS RPC validation err: %s", _e)

# --- Override: keep Pump.fun in DEX_PROGRAM_IDS so we can always subscribe to it via logsSubscribe(mentions) if needed ---
DEX_PROGRAM_IDS = {
    PUMP_FUN_PROGRAM_ID: "Pump.fun",
}
# Ensure Token Program is explicitly watched via account/program subscribe flow if needed.
if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
    logger.debug("[WS][CONFIG] DEX_PROGRAM_IDS left empty; full logs subscription will be used for SPL transfer detection.")

# --- WS subscription/endpoint hardening ---
# Single source of truth (do NOT redeclare later)
# Temporarily enable full logs subscription so LIVE BUY detection sees Token Program events.
# WARNING: this will increase WS events volume. Set back to False if you need to reduce traffic.
SUBSCRIBE_ALL_LOGS = True
HELIUS_WS_COMMITMENT = "finalized"    # reduces duplicate events
# Note: Enhanced REST is fully removed; no _ENH_* variables are used anywhere.
# Keep REQUIRE_DS_TRIGGER, BUY_CHECK_INTERVAL, DEXSCREENER_MIN_BUYS_DELTA updated earlier.

if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
    try:
        logger.debug(
            "[COST][OVERRIDES] SUBSCRIBE_ALL_LOGS=%s, HELIUS_WS_COMMITMENT=%s, "
            "REQUIRE_DS_TRIGGER=%s, BUY_CHECK_INTERVAL=%s, DEXSCREENER_MIN_BUYS_DELTA=%s, WS_PREFETCH_QPS=%s",
            SUBSCRIBE_ALL_LOGS,
            HELIUS_WS_COMMITMENT,
            globals().get("REQUIRE_DS_TRIGGER"),
            BUY_CHECK_INTERVAL,
            DEXSCREENER_MIN_BUYS_DELTA,
            int(globals().get("_WS_PREFETCH_QPS") or 0),
        )
    except Exception:
        pass

# --- Helius WS payload helpers (exact spec-conformant) -----------------------
# Reference payload shape (from helius.dev):
# {
#   "jsonrpc": "2.0",
#   "id": <number>,
#   "method": "<subscription_method>",
#   "params": [
#     "<required_parameter>",
#     { "encoding": "<encoding_option>", "commitment": "<commitment_level>" }
#   ]
# }

def helius_ws_subscribe_payload(method: str,
                                required_param,
                                *,
                                encoding: str | None = "jsonParsed",
                                commitment: str | None = None,
                                request_id: int = 1) -> dict:
    """Build a strict JSON-RPC 2.0 subscription payload for Helius WS.

    Args:
        method: The subscription method name, e.g. "logsSubscribe", "accountSubscribe".
        required_param: The required first param for the method (address string, filter dict, etc.).
        encoding: The encoding option (None for methods that don't support/need it, like logsSubscribe).
        commitment: The commitment level (default: HELIUS_WS_COMMITMENT if set, else "confirmed").
        request_id: Numeric JSON-RPC id.

    Returns:
        A dict ready to json.dumps and send on the websocket.
    """
    if commitment is None:
        commitment = str(globals().get("HELIUS_WS_COMMITMENT") or "confirmed")

    # Build options, always include commitment; include encoding only when applicable
    opts = {"commitment": str(commitment)}
    if method != "logsSubscribe" and encoding is not None:
        opts["encoding"] = str(encoding)

    return {
        "jsonrpc": "2.0",
        "id": int(request_id),
        "method": str(method),
        "params": [
            required_param,
            opts,
        ],
    }


def build_logs_subscribe_messages(program_ids: list[str] | None = None,
                                  *,
                                  subscribe_all: bool | None = None,
                                  encoding: str = "jsonParsed",
                                  commitment: str | None = None,
                                  starting_id: int = 1) -> list[dict]:
    """Create one or more `logsSubscribe` messages.

    - If `subscribe_all` True, uses "all" (or "allWithVotes" when HELIUS_WS_INCLUDE_VOTES=True).
    - Otherwise, builds **one message per program id** using {"mentions": [<program_id>]}.
      (Helius requires a sequence for 'mentions' and only supports a single address per request.)
    - Invalid pubkeys are filtered out to prevent 32602 errors.
    """
    if subscribe_all is None:
        subscribe_all = bool(globals().get("SUBSCRIBE_ALL_LOGS", False))

    msgs: list[dict] = []
    rid = int(starting_id)
    commit = commitment or str(globals().get("HELIUS_WS_COMMITMENT") or "confirmed")

    # Normalize + validate ids
    raw_ids = list(program_ids or [])
    valid_ids: list[str] = []
    skipped: list[str] = []
    for pid in raw_ids:
        s = str(pid).strip()
        if _is_valid_pubkey(s):
            valid_ids.append(s)
        else:
            skipped.append(s)

    if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
        try:
            logger.debug("[WS][BUILD][SANITY] program_ids: raw=%d valid=%d skipped=%d sample_valid=%s skipped_list=%s",
                         len(raw_ids), len(valid_ids), len(skipped), valid_ids[:3], skipped[:3])
        except Exception:
            pass

    if subscribe_all or not valid_ids:
        first_param = "allWithVotes" if bool(globals().get("HELIUS_WS_INCLUDE_VOTES", False)) else "all"
        payload = helius_ws_subscribe_payload(
            "logsSubscribe",
            first_param,
            encoding=None,
            commitment=commit,
            request_id=rid,
        )
        msgs.append(payload)
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[WS][BUILD] logsSubscribe(all=%s) -> id=%s payload=%s", first_param, rid, payload)
        return msgs

    for pid in valid_ids:
        param = {"mentions": [pid]}  # sequence required; one address per request
        payload = helius_ws_subscribe_payload(
            "logsSubscribe",
            param,
            encoding=None,
            commitment=commit,
            request_id=rid,
        )
        msgs.append(payload)
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[WS][BUILD] logsSubscribe(mentions=%s) -> id=%s payload=%s", pid, rid, payload)
        rid += 1
    return msgs

def build_account_subscribe_message(account_pubkey: str,
                                    *,
                                    encoding: str = "jsonParsed",
                                    commitment: str | None = None,
                                    request_id: int = 1) -> dict:
    """
    Strict `accountSubscribe` payload builder for Helius WS.
    Returns a dict suitable for `json.dumps(...)` right before send.
    """
    if commitment is None:
        commitment = str(globals().get("HELIUS_WS_COMMITMENT") or "confirmed")
    return {
        "jsonrpc": "2.0",
        "id": int(request_id),
        "method": "accountSubscribe",
        "params": [
            str(account_pubkey),
            {"encoding": str(encoding), "commitment": str(commitment)}
        ],
    }


def build_signature_subscribe_message(signature: str,
                                      *,
                                      encoding: str = "jsonParsed",
                                      commitment: str | None = None,
                                      request_id: int = 1) -> dict:
    """Create a single `signatureSubscribe` message for a transaction signature."""
    return helius_ws_subscribe_payload(
        "signatureSubscribe",
        str(signature),
        encoding=encoding,
        commitment=commitment or str(globals().get("HELIUS_WS_COMMITMENT") or "confirmed"),
        request_id=request_id,
    )

# --- programSubscribe (Token Program) builder ---
def build_program_subscribe_message(program_id: str,
                                    *,
                                    encoding: str = "jsonParsed",
                                    commitment: str | None = None,
                                    request_id: int = 1) -> dict:
    """Create a strict `programSubscribe` message for a program id (e.g., Token Program)."""
    return helius_ws_subscribe_payload(
        "programSubscribe",
        str(program_id),
        encoding=encoding,
        commitment=commitment or str(globals().get("HELIUS_WS_COMMITMENT") or "confirmed"),
        request_id=request_id,
    )

def _resolve_ws_url():
    """
    Resolve which Helius WebSocket URL to use for the WS listener.

    Priority order:
      1) If the user supplied HELIUS_WS_URL in env / config, use that exactly.
      2) Otherwise build a Standard WebSocket URL pointing at mainnet.helius-rpc.com
         using HELIUS_API_KEY. The "atlas"/"enhanced" endpoints may reject keys
         that don't have the Enhanced WebSocket entitlement (causes HTTP 401), so
         preferring the standard mainnet endpoint avoids that class of 401s on the
         free plan.

    The function logs a masked URL for debugging so the real API key is not printed.
    Returns a string WS URL or None when no key is configured.
    """
    # If the user explicitly set a HELIUS_WS_URL, use it unchanged.
    _raw = globals().get("HELIUS_WS_URL")
    if _raw:
        # Mask the API key for logs if present
        try:
            _k = globals().get("HELIUS_API_KEY") or ""
            _masked = _raw.replace(_k, "***" + (str(_k)[-6:] if _k else ""))
        except Exception:
            _masked = _raw
        logger.debug("[WS][CONFIG] Using configured HELIUS_WS_URL -> %s", _masked)
        return _raw

    # If no explicit WS URL, build a standard mainnet WS URL using the API key.
    _key = globals().get("HELIUS_API_KEY")
    if not _key:
        logger.warning("[WS][AUTH] No HELIUS_API_KEY found in environment/config — WebSocket will be disabled.")
        return None

    # Prefer the standard Mainnet URL (available on free plan). Avoid atlas/enhanced by default
    _url = f"wss://mainnet.helius-rpc.com/?api-key={_key}"

    # Log a masked form so secrets are not printed raw to logs
    _masked = _url.replace(_key, "***" + str(_key)[-6:])
    logger.debug("[WS][CONFIG] Resolved Helius WS URL -> %s", _masked)
    return _url



# Resolve immediately at import time
_resolve_ws_url()

# --- WS keepalive tuning (env-overridable, rate-limit friendly) ---
# Increase ping interval to reduce heartbeat churn on Helius; keep a modest timeout.
WS_PING_INTERVAL = int(str(os.environ.get("WS_PING_INTERVAL", "55")).strip() or "55")
WS_PING_TIMEOUT  = int(str(os.environ.get("WS_PING_TIMEOUT", "25")).strip() or "25")
if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
    try:
        logger.debug("[WS][KEEPALIVE] ping_interval=%s ping_timeout=%s (env %s / %s)",
                     WS_PING_INTERVAL, WS_PING_TIMEOUT,
                     os.environ.get("WS_PING_INTERVAL"), os.environ.get("WS_PING_TIMEOUT"))
    except Exception:
        pass

# === DEX-Priority-only mode (no explicit program IDs) ==========================
# If enabled, we subscribe to `logsSubscribe("all")` and filter events at runtime
# by matching the DEX name against DEX_PRIORITY. This eliminates the need for
# DEX_PROGRAM_IDS entirely.
USE_DEX_PRIORITY_ONLY = False  # disable priority mode; we will not use the 'all' firehose

# Normalize helper
_def_norm = lambda s: str(s or "").strip().lower()

async def _on_ws_message(app: Application, raw: str) -> None:
    """Handle a single WebSocket message from Helius.
    Fast path(s):
      - accountNotification -> payment detection for SOL_RECEIVE_ADDRESS
      - programNotification(Token Program) -> per-account token/WSOL deltas (used for live-buy correlation)
    Fallback:
      - logsNotification -> SPL transfer detection from logs; correlate using slot
    """
    try:
        msg = json.loads(raw)
    except Exception:
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[WS][PARSE] non-JSON frame: %s", (raw if isinstance(raw, str) else str(raw))[:200])
        return

    params = (msg or {}).get("params") or {}
    result = (params or {}).get("result") or {}
    value  = (result or {}).get("value") or {}
    logs   = value.get("logs") or []
    if not isinstance(logs, list):
        logs = []

    # =========================
    # ACCOUNT NOTIFICATION PATH (automatic payment detection)
    # =========================
    try:
        if msg.get("method") == "accountNotification":
            # Optional: which subscription fired (deep debug)
            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                try:
                    logger.debug("[PAY][FRAME] sub_id=%s", (params.get("subscription")))
                except Exception:
                    pass

            acc_val = ((msg.get("params") or {}).get("result") or {}).get("value") or {}
            lam = acc_val.get("lamports")
            if lam is None:
                if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                    logger.debug("[PAY][SKIP] accountNotification missing lamports field.")
                return
            try:
                lam = int(lam)
            except Exception:
                if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                    logger.debug("[PAY][SKIP] lamports not int-coercible: %s", lam)
                return

            global _PAY_ADDR_LAST_LAMPORTS
            prev = _PAY_ADDR_LAST_LAMPORTS
            _PAY_ADDR_LAST_LAMPORTS = lam

            if prev is None:
                if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                    logger.debug("[PAY][BASELINE] lamports=%s (%.9f SOL)", lam, lam / 1_000_000_000.0)
                return

            delta_lam = lam - int(prev)
            if delta_lam <= 0:
                if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                    logger.debug("[PAY][NO-INCR] prev=%s now=%s delta=%s", prev, lam, delta_lam)
                return

            delta_sol = float(delta_lam) / 1_000_000_000.0
            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                logger.debug("[PAY][DELTA] +%s lamports (%.9f SOL)", delta_lam, delta_sol)

            # Ignore dust (< epsilon/5) to avoid false positives from rent/interest
            _eps = float(globals().get("AMOUNT_EPSILON", 0.0005))
            if delta_sol < max(0.00001, _eps / 5.0):
                if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                    logger.debug("[PAY][DUST] %.9f SOL < threshold; ignoring.", delta_sol)
                return

            # Match against pending bookings (within epsilon)
            pending = dict(PENDING_BOOKINGS) if 'PENDING_BOOKINGS' in globals() else {}
            if not pending:
                if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                    logger.debug("[PAY][UNMATCHED] No PENDING_BOOKINGS present.")
                return

            matched_pid = None
            matched_pb = None
            try:
                # Prefer the most-recent created pending that matches the amount within epsilon
                candidates = []
                for pid, pb in pending.items():
                    want = float(pb.get("amount_sol") or 0.0)
                    if abs(delta_sol - want) <= _eps:
                        candidates.append((int(pb.get("created") or 0), pid, pb))
                if candidates:
                    candidates.sort(reverse=True)  # newest first
                    _, matched_pid, matched_pb = candidates[0]
            except Exception:
                matched_pid, matched_pb = None, None

            if not matched_pid:
                if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                    logger.debug("[PAY][UNMATCHED] No pending booking matched %.9f SOL (eps=%.6f)", delta_sol, _eps)
                return

            # Finalize booking
            try:
                PENDING_BOOKINGS.pop(matched_pid, None)
            except Exception:
                pass
            pb = matched_pb or {}

            try:
                slot = int(pb.get("slot"))
            except Exception:
                slot = None
            if slot is None:
                if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                    logger.debug("[PAY][FINALIZE][MISS] no slot in pb=%s", pb)
                return

            promo = {
                "token_name": pb.get("token_name"),
                "token_address": pb.get("token_address"),
                "pair_address": pb.get("pair_address"),
                "booked_by": int(pb.get("user_id") or 0),
                "start_time": int(time.time()),
                "duration": float((TRENDING_SLOTS.get(slot) or {}).get("duration") or 0.0),
            }
            promo["end_time"] = int(promo["start_time"] + promo["duration"] * 3600)

            promos = clear_expired_promotions() or {}
            promos[str(slot)] = promo
            save_promotions(promos)

            # Release slot held by the payer now that payment is confirmed
            try:
                release_slot(slot, int(pb.get("user_id") or 0), force=True)
                if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                    logger.debug("[HOLD][RELEASE] payment-confirmed -> slot=%s uid=%s", slot, int(pb.get("user_id") or 0))
            except Exception:
                pass

            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                logger.debug("[PAY][FINALIZE] slot=%s promo=%s", slot, {k: promo.get(k) for k in ("token_name","token_address","pair_address","duration")})

            # Update pin & broadcast
            try:
                await update_pinned_trending_message(app.bot, int(pb.get("user_id") or 0))
            except Exception as e:
                logger.warning("[PAY][PIN][ERR] %s", e)
            try:
                asyncio.create_task(_broadcast_trending_announcement(slot, promo, app.bot))
            except Exception as e:
                logger.warning("[PAY][BROADCAST][ERR] %s", e)

            # Notify payer (optional)
            try:
                uid = int(pb.get("user_id") or 0)
                if uid:
                    await app.bot.send_message(
                        chat_id=uid,
                        text="✅ Payment received. Your token is now trending!",
                        parse_mode="HTML",
                        disable_web_page_preview=True
                    )
            except Exception as e:
                logger.warning("[PAY][USER-NOTIFY][ERR] %s", e)

            logger.info("[PAY] Auto-confirmed booking for slot %s via %.9f SOL deposit.", slot, delta_sol)
            return
    except Exception as e:
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[PAY][ERR] %s", e)

    # =========================
    # PROGRAM NOTIFICATION PATH (Token Program deltas for live-buy correlation)
    # =========================
    try:
        if (msg.get("method") == "programNotification"):
            v = ((msg.get("params") or {}).get("result") or {}).get("value") or {}
            acc_pub = (v.get("pubkey") or "").strip()
            acc = (v.get("account") or {})
            data = (acc.get("data") or {})
            parsed = (data.get("parsed") or {})
            pinfo = (parsed.get("info") or {})
            mint = str(pinfo.get("mint") or "").lower()
            is_native = bool((pinfo.get("isNative") or False))

            # context slot
            context_slot = (((msg.get("params") or {}).get("result") or {}).get("context") or {}).get("slot")
            try:
                cs = int(context_slot) if context_slot is not None else None
            except Exception:
                cs = None

            # Native (WSOL) accounts -> net WSOL delta per slot
            if is_native:
                w_ui = None
                try:
                    ta = pinfo.get("tokenAmount") or {}
                    if ta.get("uiAmount") is not None:
                        w_ui = float(ta.get("uiAmount"))
                    elif ta.get("uiAmountString") is not None:
                        w_ui = float(ta.get("uiAmountString"))
                    elif ta.get("amount") is not None:
                        dec = int(ta.get("decimals") or 0)
                        w_ui = float(ta.get("amount")) / (10 ** max(dec, 0))
                except Exception:
                    w_ui = None

                prev_w = _TOK_ACC_BAL_LAST.get(acc_pub)
                _TOK_ACC_BAL_LAST[acc_pub] = float(w_ui) if (w_ui is not None) else _TOK_ACC_BAL_LAST.get(acc_pub, 0.0)
                if cs is not None and w_ui is not None and prev_w is not None:
                    d = float(w_ui) - float(prev_w)
                    if d != 0:
                        _prune_slot_caches(cs)
                        _SLOT_WSOL_NET[cs] = float(_SLOT_WSOL_NET.get(cs, 0.0) + d)
                        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                            logger.debug("[WS][DELTA][WSOL] slot=%s net=%.9f (this=%.9f)", cs, _SLOT_WSOL_NET[cs], d)
                return

            # SPL token account deltas for watched mints only
            if not mint:
                return

            promos = load_promotions() or {}
            watched = {}
            for s, p in promos.items():
                try:
                    m = (p.get("token_address") or "").lower()
                    if m:
                        watched[m] = int(s)
                except Exception:
                    continue
            if mint not in watched:
                return

            token_amt_ui = None
            try:
                ta = pinfo.get("tokenAmount") or {}
                if ta.get("uiAmount") is not None:
                    token_amt_ui = float(ta.get("uiAmount"))
                elif ta.get("uiAmountString") is not None:
                    token_amt_ui = float(ta.get("uiAmountString"))
                elif ta.get("amount") is not None:
                    dec = int(ta.get("decimals") or 0)
                    token_amt_ui = float(ta.get("amount")) / (10 ** max(dec, 0))
            except Exception:
                token_amt_ui = None
            if token_amt_ui is None:
                return

            prev = _TOK_ACC_BAL_LAST.get(acc_pub)
            _TOK_ACC_BAL_LAST[acc_pub] = float(token_amt_ui)

            # For watched mints, treat the first observation as a positive delta.
            # This is critical for Pump.fun buys where a brand new ATA is created
            # and we never see a "before" balance in the same session. For non-pump
            # tokens this simply means the first time we ever see an account holding
            # the trending mint, we treat that balance as the initial buy.
            if prev is None:
                delta = float(token_amt_ui)
            else:
                delta = float(token_amt_ui) - float(prev)

            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                try:
                    logger.debug("[WS][PROG][RAW] pubkey=%s mint=%s ui=%.10f", acc_pub[:8], mint, token_amt_ui)
                    logger.debug("[WS][PROG][DELTA] acct=%s mint=%s prev=%s now=%.10f d=%s",
                                 acc_pub[:8], mint,
                                 (f"{prev:.10f}" if isinstance(prev, (int,float)) else None),
                                 token_amt_ui,
                                 (f"{delta:.10f}" if isinstance(delta, (int,float)) else "None"))
                except Exception:
                    pass

            if delta is None or delta <= 0:
                return

            if cs is not None:
                try:
                    _prune_slot_caches(cs)
                    slot_map = _SLOT_TOKEN_DELTAS.setdefault(cs, {})
                    slot_map[mint] = float(slot_map.get(mint, 0.0) + float(delta))
                    if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                        logger.debug("[WS][DELTA] slot=%s mint=%s +%.10f (slot_sum=%.10f)", cs, mint, float(slot_map[mint]))
                except Exception as e:
                    if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                        logger.debug("[WS][DELTA][ERR] %s", e)

            if bool(globals().get("FORCE_SIGNATURE_ONLY", False)):
                return
    except Exception:
        # fall through to logsNotification path if anything goes wrong
        pass

    # ======================
    # LOGS NOTIFICATION PATH
    # ======================
    sig = (
        result.get("signature")
        or result.get("transactionSignature")
        or (value or {}).get("signature")
    )
    context_slot = (((msg.get("params") or {}).get("result") or {}).get("context") or {}).get("slot")

    # Drop duplicates early (prevents repeated posts) — signature only applies to logsNotification
    if not _mark_sig_seen(sig or ""):
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[WS][DEDUP] Skipping already-seen signature %s", (str(sig)[:8] if sig else "?"))
        return

    # Detect SPL Token Program transfers using only logs
    saw_tokenkeg = False
    saw_transfer = False
    is_pump_log = False  # specifically for Pump.fun InitializeMint2 (new token creation)
    try:
        pump_id = globals().get("PUMP_FUN_PROGRAM_ID")
        for line in logs:
            s = str(line)

            # Standard SPL Token transfer breadcrumbs (non-pump tokens)
            if "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA" in s and "invoke" in s:
                saw_tokenkeg = True
            if "Program log: Instruction: Transfer" in s or "Program log: Instruction: TransferChecked" in s:
                saw_transfer = True

            # Pump.fun-specific: standard WebSocket logsSubscribe(mentions=[pump_program])
            # Filter for InitializeMint2 for new token creation, per Helius docs.
            # With a `mentions: [PUMP_FUN_PROGRAM_ID]` subscription, any frame that
            # includes "Instruction: InitializeMint2" corresponds to a new Pump token.
            if pump_id and ("Instruction: InitializeMint2" in s):
                is_pump_log = True
    except Exception:
        pass

    # Pump.fun gets a dedicated hydrate path based on logsSubscribe(mentions=[PUMP_FUN_PROGRAM_ID]).
    # This runs *before* the generic SPL-Transfer handler so Pump tokens don't depend on slot-level
    # token delta correlation, which can miss Pump bonding-curve trades.
    if is_pump_log:
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            try:
                logger.debug(
                    "[WS][PUMP][MATCH] sig=%s -> Pump.fun logs frame detected (logsSubscribe/mentions).",
                    (str(sig)[:8] if sig else "?"),
                )
            except Exception:
                pass
        try:
            await _handle_pump_fun_event(app, sig)
        except Exception as _pe:
            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                try:
                    logger.debug("[WS][PUMP][ERR] %s", _pe)
                except Exception:
                    pass

    # Generic SPL-Transfer path (non-Pump, or any flow that still emits Tokenkeg transfer logs)
    elif saw_tokenkeg and saw_transfer:
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[WS][SPL][XFER] logs-only transfer detected sig=%s", (str(sig)[:8] if sig else "?"))
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            try:
                logger.debug("[WS][SPL][XFER][INFO] sig=%s saw_tokenkeg=%s saw_transfer=%s booked_any=%s",
                             (str(sig)[:8] if sig else "?"),
                             saw_tokenkeg, saw_transfer,
                             bool(clear_expired_promotions() or {}))
            except Exception:
                pass
        try:
            await _handle_spl_transfer_log(app, sig, logs, context_slot)
        except Exception as _hx:
            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                logger.debug("[WS][SPL][XFER][ERR] %s", _hx)
    else:
        if (msg.get("method") != "programNotification") and 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[WS][SKIP] Non-SPL-transfer frame sig=%s", (str(sig)[:8] if sig else "?"))


# --- Deduplicate signature handling (avoid repeated hydration attempts) ---
_SEEN_SIGS_SET = set()
_SEEN_SIGS_Q = deque(maxlen=500)  # remember last 500 txs

def _mark_sig_seen(sig: str) -> bool:
    """
    Returns True if this signature was first-seen (i.e., should be processed),
    or False if we've already handled it recently.
    """
    try:
        s = str(sig or "")
        if not s:
            return False
        if s in _SEEN_SIGS_SET:
            return False
        _SEEN_SIGS_SET.add(s)
        _SEEN_SIGS_Q.append(s)
        # evict old when deque wraps
        if len(_SEEN_SIGS_Q) == _SEEN_SIGS_Q.maxlen:
            # trim set to contents of deque
            _SEEN_SIGS_SET.intersection_update(_SEEN_SIGS_Q)
        return True
    except Exception:
        return True

# --- Post-level dedup: avoid double-posting LIVE BUY for the same signature ---
# Keep a small, time-bounded registry of posted signatures.
# This prevents duplicate LIVE BUY posts on reconnect/replay while avoiding unbounded growth.
from collections import deque as _deque_posted

_LIVE_BUY_POSTED: dict[str, float] = {}          # sig -> ts
_LIVE_BUY_POSTED_Q: _deque_posted[str] = _deque_posted(maxlen=5000)  # order of insertion (bounded)
_LIVE_BUY_TTL_SEC = 2 * 60 * 60  # keep entries for up to 2 hours

def _prune_posted_sigs(max_age_sec: int | None = None) -> None:
    try:
        ttl = int(max_age_sec if max_age_sec is not None else _LIVE_BUY_TTL_SEC)
        now = time.time()
        # Drop expired by timestamp
        for s, ts in list(_LIVE_BUY_POSTED.items()):
            if (now - float(ts)) > ttl:
                _LIVE_BUY_POSTED.pop(s, None)
                try:
                    # best-effort remove from deque (O(n), but small)
                    _LIVE_BUY_POSTED_Q.remove(s)
                except Exception:
                    pass
        # Enforce the deque size bound on the dict (if deque evicted leftmost on append)
        keep = set(_LIVE_BUY_POSTED_Q)
        for k in list(_LIVE_BUY_POSTED.keys()):
            if k not in keep:
                _LIVE_BUY_POSTED.pop(k, None)
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            try:
                logger.debug("[SPL][DEDUP][PRUNE] size=%s ttl=%ss", len(_LIVE_BUY_POSTED), ttl)
            except Exception:
                pass
    except Exception:
        pass

def _already_posted_sig(sig: str) -> bool:
    try:
        _prune_posted_sigs()
        s = str(sig or "")
        if not s:
            return False
        return s in _LIVE_BUY_POSTED
    except Exception:
        return False

def _mark_posted_sig(sig: str) -> None:
    try:
        _prune_posted_sigs()
        s = str(sig or "")
        if not s:
            return
        _LIVE_BUY_POSTED[s] = time.time()
        _LIVE_BUY_POSTED_Q.append(s)
        # If deque reached capacity and evicted an old key, sync dict
        if len(_LIVE_BUY_POSTED_Q) == _LIVE_BUY_POSTED_Q.maxlen:
            keep = set(_LIVE_BUY_POSTED_Q)
            for k in list(_LIVE_BUY_POSTED.keys()):
                if k not in keep:
                    _LIVE_BUY_POSTED.pop(k, None)
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            try:
                logger.debug("[SPL][DEDUP] Marked posted sig=%s size=%s", (s[:8] if s else "?"), len(_LIVE_BUY_POSTED))
            except Exception:
                pass
    except Exception:
        pass

# --- Program-subscribe token-account balance cache (for per-account deltas) ---
if '_TOK_ACC_BAL_LAST' not in globals():
    _TOK_ACC_BAL_LAST: dict[str, float] = {}   # account_pubkey -> last ui amount observed

# --- WS-only per-slot correlation caches ---
# slot -> { mint_lower: cumulative_positive_token_delta_ui }
if '_SLOT_TOKEN_DELTAS' not in globals():
    _SLOT_TOKEN_DELTAS: dict[int, dict[str, float]] = {}
# slot -> net WSOL delta (negative means SOL spent)
if '_SLOT_WSOL_NET' not in globals():
    _SLOT_WSOL_NET: dict[int, float] = {}
# prune after N slots to keep memory bounded
_SLOT_CACHE_TTL_SLOTS = 8

def _prune_slot_caches(current_slot: int | None = None) -> None:
    try:
        if current_slot is None:
            keys = list(_SLOT_TOKEN_DELTAS.keys()) + list(_SLOT_WSOL_NET.keys())
            if not keys:
                return
            current_slot = max(keys)
        min_keep = max(0, int(current_slot) - int(_SLOT_CACHE_TTL_SLOTS))
        for d in list(_SLOT_TOKEN_DELTAS.keys()):
            if d < min_keep:
                _SLOT_TOKEN_DELTAS.pop(d, None)
        for d in list(_SLOT_WSOL_NET.keys()):
            if d < min_keep:
                _SLOT_WSOL_NET.pop(d, None)
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[WS][CACHE][PRUNE] upto_slot<%s kept_tokens=%s kept_wsol=%s",
                         min_keep, len(_SLOT_TOKEN_DELTAS), len(_SLOT_WSOL_NET))
    except Exception:
        pass


def _rpc_get_spl_transfer_amount(signature: str) -> tuple[str | None, float | None]:
    """
    Return (mint, ui_amount) for an SPL transfer, or (None, None) if unknown.

    In WS-only mode, this function is intentionally disabled to avoid
    getTransaction REST calls that can consume large numbers of credits.
    """
    try:
        if globals().get("WS_ONLY_MODE", False):
            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                logger.debug("[RPC][TX] WS_ONLY_MODE=True -> skipping getTransaction hydrate for %s",
                             (signature[:8] if signature else "?"))
            return None, None
    except Exception:
        return None, None

    # If WS_ONLY_MODE is turned off later, keep a *very* conservative fallback.
    try:
        if not signature:
            return None, None

        rpc_url = str(
    globals().get('RPC_HTTPS')
    or f"https://mainnet.helius-rpc.com/?api-key={(globals().get('HELIUS_API_KEY') or os.environ.get('HELIUS_API_KEY') or '').strip()}"
)
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getTransaction",
            "params": [signature, {
                "encoding": "jsonParsed",
                "maxSupportedTransactionVersion": 2,
                "commitment": str(globals().get("HELIUS_WS_COMMITMENT") or "confirmed"),
            }],
        }
        data = _post_json_with_backoff(rpc_url, payload, max_retries=1, base_sleep=0.25, timeout=6.0)
        tx = (data or {}).get("result") or {}
        meta = tx.get("meta") or {}

        instrs = (((tx.get("transaction") or {}).get("message") or {}).get("instructions")) or []
        inner  = []
        for g in (meta.get("innerInstructions") or []):
            inner.extend(g.get("instructions") or [])

        def _as_ui_amount(token_amount):
            try:
                if isinstance(token_amount, dict):
                    if token_amount.get("uiAmount") is not None:
                        return float(token_amount["uiAmount"])
                    if token_amount.get("uiAmountString") is not None:
                        return float(token_amount["uiAmountString"])
                    if token_amount.get("amount") is not None:
                        dec = int(token_amount.get("decimals") or 0)
                        return float(token_amount["amount"]) / (10 ** max(dec, 0))
                elif isinstance(token_amount, (int, float, str)):
                    return float(token_amount)
            except Exception:
                return None
            return None

        def _scan(lst):
            for ins in lst or []:
                parsed = (ins.get("parsed") or {})
                if not parsed:
                    continue
                typ = str(parsed.get("type") or "").lower()
                if typ in ("transfer", "transferchecked"):
                    info = parsed.get("info") or {}
                    mint = info.get("mint") or info.get("token") or info.get("mintAddress")
                    amt  = info.get("tokenAmount") or info.get("amount")
                    ui_amt = _as_ui_amount(amt)
                    if mint and ui_amt is not None:
                        return str(mint), float(ui_amt)
            return None, None

        mint, amt = _scan(instrs)
        if mint and amt is not None:
            return mint, amt
        mint, amt = _scan(inner)
        if mint and amt is not None:
            return mint, amt
    except Exception as e:
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[RPC][TX][FALLBACK-ERR] %s", e)
    return None, None

def _rpc_get_account_lamports(addr: str) -> int | None:
    """
    Cheap baseline fetch for a system account's lamports via getAccountInfo.
    Returns lamports as int, or None on failure.
    """
    try:
        if not addr or not isinstance(addr, str):
            return None
        rpc_url = str(
            globals().get('RPC_HTTPS')
            or f"https://mainnet.helius-rpc.com/?api-key={(globals().get('HELIUS_API_KEY') or os.environ.get('HELIUS_API_KEY') or '').strip()}"
        )
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getAccountInfo",
            "params": [addr, {
                "encoding": "jsonParsed",
                "commitment": str(globals().get("HELIUS_WS_COMMITMENT") or "confirmed"),
            }],
        }
        data = _post_json_with_backoff(rpc_url, payload, max_retries=1, base_sleep=0.25, timeout=6.0)
        lam = (((data or {}).get("result") or {}).get("value") or {}).get("lamports")
        try:
            return int(lam) if lam is not None else None
        except Exception:
            return None
    except Exception as e:
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[RPC][ACCT][ERR] %s", e)
    return None

async def _handle_spl_transfer_log(app: Application, signature: str | None, logs: list[str] | None, slot: int | None):
    """
    WS logs-only handler (WS-only hydration):
      - Resolve watched mint by correlating with programSubscribe deltas stored per-slot
      - Compute SOL spent from aggregated negative WSOL deltas for the same slot
      - Estimate USD using get_sol_usd_price()
      - Emit alert with SOL + optional USD amounts
    """
    if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
        try:
            logger.debug("[WS][SPL][HANDLE][BEGIN] sig=%s slot=%s", (signature or "")[:12], slot)
        except Exception:
            pass

    # Require a proper signature so we can post a link/button
    if not signature or not isinstance(signature, str) or len(signature) < 8:
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[WS][SPL][HANDLE] No usable signature; skipping.")
        return

    # Load watched mints
    promos = load_promotions() or {}
    watched = {}
    for s, p in promos.items():
        try:
            m = (p.get("token_address") or "").lower()
            if m:
                watched[m] = int(s)
        except Exception:
            continue

    if not watched:
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[WS][SPL][HANDLE] No active promos; skipping.")
        return

    # Resolve slot
    try:
        _slot = int(slot) if slot is not None else None
    except Exception:
        _slot = None
    if _slot is None:
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[WS][SPL][HANDLE] Missing context slot; cannot correlate.")
        return

    # Prune + read caches
    try:
        _prune_slot_caches(_slot)
    except Exception:
        pass

    slot_map = (_SLOT_TOKEN_DELTAS.get(_slot) or {})
    if not slot_map:
        # Fallback for cases where we didn't see per-account deltas via programSubscribe
        # (this is common for some Pump.fun flows). In that case, hydrate the transaction
        # once via REST, extract the mint, and proceed if it's a watched token.
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[WS][SPL][HANDLE] No token deltas recorded for slot=%s; trying RPC fallback for sig=%s.",
                         _slot, (signature or "")[:8])
        mint_fb, _amt_fb = _rpc_get_spl_transfer_amount(signature or "")
        if not mint_fb:
            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                logger.debug("[WS][SPL][HANDLE][RPC-FALLBACK] No mint resolved for sig=%s; skipping.",
                             (signature or "")[:8])
            return
        mint_lc = str(mint_fb).lower()
        if mint_lc not in watched:
            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                logger.debug("[WS][SPL][HANDLE][RPC-FALLBACK] Mint %s not in watched promos; skipping.", mint_lc)
            return
        token_delta = float(_amt_fb or 0.0)
    else:
        # Pick watched mint with largest positive delta from WS per-account cache
        mint_lc = None
        token_delta = 0.0
        try:
            for m, d in slot_map.items():
                if m in watched and d > 0 and d > token_delta:
                    mint_lc = m
                    token_delta = float(d)
        except Exception:
            mint_lc = None

        if not mint_lc:
            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                logger.debug("[WS][SPL][HANDLE][SKIP] mint not watched or unresolved in slot=%s signature=%s",
                             _slot, (signature or "")[:8])
            return

    # IMPORTANT: Do NOT derive SOL from aggregated WSOL net per-slot — it mixes multiple signers
    # and often over/under-estimates. Leave amounts as None so `_post_live_buy_direct()` performs a
    # one-shot precise hydrate (getTransaction jsonParsed) to match Solscan.
    sol_amount = None
    usd_amount = None
    if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
        try:
            logger.debug("[WS][SPL][HANDLE] Deferring SOL/USD calc to precise hydrate for sig=%s", (signature or "")[:8])
        except Exception:
            pass

    if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
        try:
            logger.debug("[WS][SPL][HYDRATE] slot=%s sig=%s mint=%s token_delta=%.10f sol=%s usd=%s",
                         _slot, (signature or "")[:8], mint_lc, token_delta, sol_amount,
                         (None if usd_amount is None else f"{usd_amount:.2f}"))
        except Exception:
            pass

    # Post
    try:
        await _post_live_buy_direct(
            app=app,
            slot=watched.get(mint_lc, _slot),  # prefer booking slot id if configured
            mint_lc=mint_lc,
            signature=signature,
            sol_amount=sol_amount,
            usd_amount=usd_amount,
        )
        if signature:
            _mark_posted_sig(signature)
        # prevent duplicate matches on replays
        _SLOT_TOKEN_DELTAS.pop(_slot, None)
        _SLOT_WSOL_NET.pop(_slot, None)
    except Exception as e:
        logger.warning("[WS][SPL][HANDLE] Failed to send live-buy alert: %s", e)

    if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
        logger.debug("[WS][SPL][HANDLE][END] sig=%s", (signature or "")[:12])

# --- Helper: build a transaction URL for explorers (Solscan primary, falls back to explorer.solana.com) ---
def _tx_url(signature: str) -> str:
    try:
        s = str(signature or "").strip()
        if not s:
            return "https://solscan.io"
        return f"https://solscan.io/tx/{s}"
    except Exception:
        try:
            return f"https://explorer.solana.com/tx/{signature}"
        except Exception:
            return "https://explorer.solana.com"

async def _post_live_buy_direct(
    *,
    app: Application,
    slot: int,
    mint_lc: str,
    signature: str,
    token_units: float | None = None,   # exact token units, when known
    sol_amount: float | None = None,    # SOL leg if known
    usd_amount: float | None = None
) -> None:
    """
    Canonical LIVE BUY poster (adapter).

    Routes ALL legacy direct posts through `send_trending_alert_for_slot`, which composes
    via `_compose_trending_text`. This guarantees one format everywhere and removes the
    old “LIVE BUY” block you saw in screenshots.
    """
    # Deep debug breadcrumbs
    if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
        try:
            logger.debug(
                "[LIVE][POST][ADAPTER][BEGIN] slot=%s mint=%s sig=%s tok_units=%s sol=%s usd=%s",
                slot, (mint_lc or "")[:12], (signature[:8] if signature else None),
                (None if token_units is None else f"{float(token_units):.8f}"),
                (None if sol_amount is None else f"{float(sol_amount):.9f}"),
                (None if usd_amount is None else f"{float(usd_amount):.2f}")
            )
        except Exception:
            pass

    if not signature:
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[LIVE][POST][ADAPTER][SKIP] missing signature")
        return

    async def _precise_hydrate_amounts(signature: str, watched_mint_lc: str) -> tuple[float | None, float | None, float | None]:
        """
        Fetch getTransaction(jsonParsed) once and derive:
          - token_units: positive delta of the watched mint for the buyer (from pre/post token balances)
          - sol_amount: absolute SOL (WSOL) delta spent by the buyer (ui units)
          - usd_spent: absolute USD stablecoin delta spent (USDC/USDT) if SOL was not the leg
        Returns (token_units, sol_amount, usd_spent) as UI amounts. On any failure, returns (None, None, None).
        This is credit-cheap: called only once per signature, and only from LIVE BUY posting.
        """
        try:
            if bool(globals().get("DISABLE_PRECISE_HYDRATE", False)):
                return None, None, None

            rpc_url = str(
                globals().get('RPC_HTTPS')
                or f"https://mainnet.helius-rpc.com/?api-key={(globals().get('HELIUS_API_KEY') or os.environ.get('HELIUS_API_KEY') or '').strip()}"
            )
            # Use maxSupportedTransactionVersion=2 for broader coverage (v0 & v0.1 txs)
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getTransaction",
                "params": [signature, {
                    "encoding": "jsonParsed",
                    "maxSupportedTransactionVersion": 2,
                    "commitment": str(globals().get("HELIUS_WS_COMMITMENT") or "confirmed"),
                }],
            }
            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                try:
                    logger.debug(
                        "[HYD][TRIGGER] getTransaction(jsonParsed, maxSupportedTransactionVersion=2) for sig=%s",
                        (signature[:8] if signature else None)
                    )
                except Exception:
                    pass
            data = _post_json_with_backoff(rpc_url, payload, max_retries=1, base_sleep=0.25, timeout=6.0)
            tx = (data or {}).get("result") or {}
            meta = tx.get("meta") or {}

            # Helpers
            def _as_ui_amount(token_amount):
                try:
                    if isinstance(token_amount, dict):
                        if token_amount.get("uiAmount") is not None:
                            return float(token_amount["uiAmount"])
                        if token_amount.get("uiAmountString") is not None:
                            return float(token_amount["uiAmountString"])
                        if token_amount.get("amount") is not None:
                            dec = int(token_amount.get("decimals") or 0)
                            return float(token_amount["amount"]) / (10 ** max(dec, 0))
                    elif isinstance(token_amount, (int, float, str)):
                        return float(token_amount)
                except Exception:
                    return None
                return None

            def _scan(lst):
                # Find first parsed transfer for any mint
                for ins in lst or []:
                    parsed = (ins.get("parsed") or {})
                    if not parsed:
                        continue
                    typ = str(parsed.get("type") or "").lower()
                    if typ in ("transfer", "transferchecked"):
                        info = parsed.get("info") or {}
                        mint = info.get("mint") or info.get("token") or info.get("mintAddress")
                        amt  = info.get("tokenAmount") or info.get("amount")
                        ui_amt = _as_ui_amount(amt)
                        if mint and ui_amt is not None:
                            return str(mint).lower(), float(ui_amt)
                return None, None

            instrs = (((tx.get("transaction") or {}).get("message") or {}).get("instructions")) or []
            inner  = []
            for g in (meta.get("innerInstructions") or []):
                inner.extend(g.get("instructions") or [])

            # Build token balance maps: (mint_lc, owner_lc) -> uiAmount
            def _amount_map(lst):
                m = {}
                for b in lst or []:
                    try:
                        mint = str(b.get("mint") or "").lower()
                        owner = str(b.get("owner") or "").lower()
                        ui = None
                        ui_ta = (b.get("uiTokenAmount") or {})
                        if ui_ta.get("uiAmount") is not None:
                            ui = float(ui_ta.get("uiAmount"))
                        elif ui_ta.get("uiAmountString") is not None:
                            ui = float(ui_ta.get("uiAmountString"))
                        elif ui_ta.get("amount") is not None:
                            dec = int(ui_ta.get("decimals") or 0)
                            ui = float(ui_ta.get("amount")) / (10 ** max(dec, 0))
                        if mint and owner and (ui is not None):
                            m[(mint, owner)] = float(ui)
                    except Exception:
                        continue
                return m

            pre_map  = _amount_map(meta.get("preTokenBalances") or [])
            post_map = _amount_map(meta.get("postTokenBalances") or [])

            watched = str(watched_mint_lc or "").lower()
            sol_mint = "so11111111111111111111111111111111111111112"  # WSOL
            STABLE_MINTS = {
                # Accurate lowercased Solana stablecoin mints
                "epjfwdd5aufqssqem2qnxzybapc8g4weggkzwytdt1": True,  # USDC
                "es9vmfrzacer mjfrf4h2fyd4kconky11mcce8benwnyb".replace(" ", ""): True  # USDT
            }

            # 1) Token units: prefer balance diff; fallback to parsed instruction scan
            buyer = None
            token_units = None
            # Find owner with largest positive delta for watched mint
            try:
                for (mint, owner), pre_amt in pre_map.items():
                    if mint != watched:
                        continue
                    post_amt = float(post_map.get((mint, owner), 0.0))
                    d = post_amt - float(pre_amt)
                    if d > (token_units or 0):
                        token_units = d
                        buyer = owner
                for (mint, owner), post_amt in post_map.items():
                    if mint != watched:
                        continue
                    pre_amt = float(pre_map.get((mint, owner), 0.0))
                    d = float(post_amt) - pre_amt
                    if d > (token_units or 0):
                        token_units = d
                        buyer = owner
            except Exception:
                pass

            if token_units is None:
                m1, a1 = _scan(instrs)
                m2, a2 = _scan(inner)
                if m1 == watched and a1 is not None:
                    token_units = float(a1)
                elif m2 == watched and a2 is not None:
                    token_units = float(a2)

                # If we still don't know buyer, try to infer by matching deltas ~ amount
                if token_units is not None and not buyer:
                    tol = max(1e-12, token_units * 0.001)
                    for (mint, owner), pre_amt in pre_map.items():
                        if mint != watched:
                            continue
                        post_amt = float(post_map.get((mint, owner), 0.0))
                        d = post_amt - float(pre_amt)
                        if abs(d - token_units) <= tol:
                            buyer = owner
                            break

            # 2) SOL leg (WSOL): look for buyer's WSOL negative delta; else aggregate any negative WSOL
            sol_delta = None
            try:
                if buyer:
                    pre_sol  = float(pre_map.get((sol_mint, buyer), 0.0))
                    post_sol = float(post_map.get((sol_mint, buyer), 0.0))
                    d = post_sol - pre_sol
                    if d < 0:
                        sol_delta = abs(d)
                if sol_delta is None:
                    total_wsol_out = 0.0
                    for (mint, owner), pre_amt in pre_map.items():
                        if mint != sol_mint:
                            continue
                        post_amt = float(post_map.get((mint, owner), 0.0))
                        d = post_amt - float(pre_amt)
                        if d < 0:
                            total_wsol_out += abs(d)
                    if total_wsol_out > 0:
                        sol_delta = total_wsol_out
            except Exception:
                pass

            # 3) If not a SOL leg, try stablecoin spend sum
            usd_spent = None
            if sol_delta is None:
                try:
                    total_usd = 0.0
                    for (mint, owner), pre_amt in pre_map.items():
                        if mint not in STABLE_MINTS:
                            continue
                        post_amt = float(post_map.get((mint, owner), 0.0))
                        d = post_amt - float(pre_amt)
                        if d < 0:
                            total_usd += abs(d)
                    if total_usd > 0:
                        usd_spent = total_usd
                except Exception:
                    pass

            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                try:
                    logger.debug("[HYD][PRECISE] sig=%s buyer=%s token_units=%s sol=%s usd_spent=%s",
                                 (signature[:8] if signature else None), (buyer[:6] if buyer else None),
                                 (None if token_units is None else f"{token_units:.8f}"),
                                 (None if sol_delta is None else f"{sol_delta:.9f}"),
                                 (None if usd_spent is None else f"{usd_spent:.2f}"))
                except Exception:
                    pass

            return (None if token_units is None else float(token_units)), \
                   (None if sol_delta   is None else float(sol_delta)), \
                   (None if usd_spent  is None else float(usd_spent))
        except Exception as e:
            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                logger.debug("[HYD][PRECISE][ERR] %s", e)
        return None, None, None

    # Optional precise hydrate to match Solscan (pre/post balances + decimals).
    if signature and mint_lc:
        t_units, s_sol, s_usd = await _precise_hydrate_amounts(signature, str(mint_lc).lower())
        if t_units is not None:
            token_units = t_units
        if s_sol is not None:
            sol_amount = s_sol
        if s_usd is not None:
            usd_amount = s_usd
        # If no SOL leg but we have USD stables spent, derive a SOL-equivalent using current price
        if (sol_amount is None) and (usd_amount is not None):
            try:
                if 'get_sol_usd_price' in globals():
                    px = float(get_sol_usd_price() or 0)
                    if px > 0:
                        sol_amount = float(usd_amount) / px
            except Exception:
                pass

    # Require both SOL and USD amounts to show in the alert. If either is missing or non-positive, skip posting.
    try:
        _sol_ok = (sol_amount is not None) and (float(sol_amount) > 0)
    except Exception:
        _sol_ok = False
    try:
        _usd_ok = (usd_amount is not None) and (float(usd_amount) > 0)
    except Exception:
        _usd_ok = False

    if (not _sol_ok) or (not _usd_ok):
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            try:
                logger.debug("[LIVE][SKIP] Missing SOL/USD amounts -> sol=%s usd=%s sig=%s mint=%s",
                             sol_amount, usd_amount, (signature[:8] if signature else None), (mint_lc or "")[:12])
            except Exception:
                pass
        return

    # --- Global USD min/max filters (non-toggling, apply to all live-buy alerts) ---
    try:
        _min_usd = float(globals().get("MIN_ALERT_AMOUNT_USD", 0) or 0)
    except Exception:
        _min_usd = 0.0
    try:
        _max_usd = float(globals().get("MAX_ALERT_AMOUNT_USD", 0) or 0)
    except Exception:
        _max_usd = 0.0

    # Suppress tiny "dust" buys below MIN_ALERT_AMOUNT_USD (if > 0)
    if (_min_usd > 0) and (float(usd_amount) < _min_usd):
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            try:
                logger.debug("[LIVE][SKIP][USD<MIN] usd=%.2f < min=%.2f sig=%s", float(usd_amount), _min_usd, (signature[:8] if signature else None))
            except Exception:
                pass
        return

    # Suppress whale/oversized buys above MAX_ALERT_AMOUNT_USD (if > 0)
    if (_max_usd > 0) and (float(usd_amount) > _max_usd):
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            try:
                logger.debug("[LIVE][SKIP][USD>MAX] usd=%.2f > max=%.2f sig=%s", float(usd_amount), _max_usd, (signature[:8] if signature else None))
            except Exception:
                pass
        return

    try:
        await send_trending_alert_for_slot(
            application=app,
            slot=slot,
            mint_lc=str(mint_lc or "").lower(),
            signature=signature,
            logs=[],
            tx_amount=sol_amount,
            tx_usd=usd_amount,
            token_amount=token_units,  # <-- forward token units so the text can show '0.0000 TICKER (≈X SOL, $Y)'
        )
    except Exception as e:
        logger.warning("[LIVE][POST][ADAPTER][ERR] slot=%s mint=%s sig=%s err=%s",
                       slot, (mint_lc or "")[:12], (signature[:8] if signature else None), e)
        return

    if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
        logger.debug("[LIVE][POST][ADAPTER][DONE] slot=%s mint=%s sig=%s",
                     slot, (mint_lc or "")[:12], (signature[:8] if signature else None))

 # --- Single-socket guard for Helius WS (prevents duplicate listener tasks) ---
_WS_TASK: asyncio.Task | None = None
_WS_TASK_LOCK = asyncio.Lock()
WS_CONNECTED = False  # live connection status for deep-debug + gating hints


async def ensure_ws_listener_started(app: Application) -> None:
    """
    Start the Helius WebSocket listener exactly once. If a task is already
    running (and not done), do nothing. This prevents multiple concurrent
    connection loops that can cause HTTP 429 errors.
    """
    global _WS_TASK
    async with _WS_TASK_LOCK:
        if _WS_TASK and not _WS_TASK.done():
            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                logger.debug("[WS][GUARD] Listener already running; not starting another.")
            return
        loop = asyncio.get_event_loop()
        _WS_TASK = loop.create_task(start_helius_ws(app))
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[WS][GUARD] Spawned single WS listener task: %s", _WS_TASK)


# --- Periodic watchdog: if WS is not connected, (re)start it. ---
async def _ws_guard_tick(app: Application) -> None:
    """Periodic watchdog: if WS is not connected, (re)start it."""
    try:
        if not globals().get("USE_HELIUS_WS", True):
            return
        if not WS_CONNECTED:
            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                logger.debug("[WS][GUARD][TICK] Not connected -> starting listener …")
            await ensure_ws_listener_started(app)
    except Exception as e:
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[WS][GUARD][ERR] %s", e)

async def _payment_timeout_job(context: CallbackContext) -> None:
    """
    Runs 5 minutes after a paid booking is staged.
    If unpaid, remove it and notify the user with a /start prompt.
    """
    try:
        d = context.job.data or {}
        pid = d.get("payment_id")
        uid = int(d.get("user_id") or 0)
    except Exception:
        pid, uid = None, 0

    if not pid:
        return

    pb = (PENDING_BOOKINGS or {}).get(pid)
    if not pb:
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[BOOK][TIMEOUT] pid=%s already settled.", pid)
        return

    # Remove pending (cancel)
    try:
        PENDING_BOOKINGS.pop(pid, None)
    except Exception:
        pass

    # Release held slot
    try:
        s = (pb or {}).get("slot")
        u = int((pb or {}).get("user_id") or uid or 0)
        if s:
            release_slot(int(s), u, force=True)
            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                logger.debug("[HOLD][RELEASE] timeout -> slot=%s uid=%s", s, u)
    except Exception:
        pass

    # Notify user with ADMIN link
    try:
        if uid and hasattr(context, "bot"):
            await context.bot.send_message(
                chat_id=uid,
                text=(
                    "⌛ You ran out of time. The booking was cancelled. Use /start to begin again.\n\n"
                    "If you are having any problems, contact <a href=\"https://t.me/CryptoBoss0011\">ADMIN</a>."
                ),
                parse_mode="HTML",
                disable_web_page_preview=True
            )
    except Exception as e:
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[BOOK][TIMEOUT][NOTIFY][ERR] %s", e)

async def _cb_cancel_booking(update: Update, context: CallbackContext) -> None:
    """Inline '❌ Cancel' button handler.
    Cancels the pending booking, kills its timeout job, releases the slot,
    and clears any 4-minute booking deadline.
    """
    q = update.callback_query
    try:
        await q.answer()
    except Exception:
        pass

    uid = get_user_id_from_callback(q)
    data = str(getattr(q, "data", "") or "")
    if not data.startswith("cancel:"):
        return
    pid = data.split(":", 1)[-1]

    pb = (PENDING_BOOKINGS or {}).get(pid)
    if (not pb) or (int(pb.get("user_id") or 0) != int(uid)):
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            try:
                logger.debug("[BOOK][CANCEL][MISS] pid=%s uid=%s pb_user=%s", pid, uid, (pb or {}).get("user_id"))
            except Exception:
                pass
        try:
            await q.edit_message_text("Nothing to cancel.")
        except Exception:
            pass
        return

    # remove pending booking
    try:
        PENDING_BOOKINGS.pop(pid, None)
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[BOOK][CANCEL] removed pending pid=%s uid=%s", pid, uid)
    except Exception:
        pass

    # cancel the timeout job if still queued
    try:
        for j in context.job_queue.get_jobs_by_name(f"paytimeout:{pid}"):
            j.schedule_removal()
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[BOOK][CANCEL] cleared timeout jobs for pid=%s", pid)
    except Exception as _e:
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[BOOK][CANCEL][TIMEOUT-ERR] %s", _e)

    # Release the held slot for this pending booking
    try:
        s = (pb or {}).get("slot")
        if s:
            release_slot(int(s), uid, force=True)
            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                logger.debug("[HOLD][RELEASE] user-cancel inline -> slot=%s uid=%s", s, uid)
    except Exception:
        pass

    # Clear any active 4-minute booking deadline for this user/session
    try:
        if '_clear_booking_deadline' in globals():
            _clear_booking_deadline(context)
            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                logger.debug("[BOOK][CANCEL] cleared booking deadline for uid=%s pid=%s", uid, pid)
    except Exception as _e:
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[BOOK][CANCEL][DEADLINE-ERR] %s", _e)

    try:
        await q.edit_message_text("❌ Booking cancelled.")
    except Exception:
        pass

async def cmd_cancel(update: Update, context: CallbackContext):
    """User-facing /cancel for booking flow.

    - Releases any slots currently HELD by this user.
    - Cancels the most recent pending booking for this user (if any).
    - Clears local booking state in context.user_data.
    """
    uid = get_user_id_from_update(update)

    # 1) Clear local booking state for this user
    try:
        context.user_data.pop("booking", None)
        context.user_data.pop("booking_slot", None)
    except Exception:
        pass

    # 2) Release ANY slot holds owned by this user
    released_slots: list[int] = []
    try:
        global SLOT_HELD_BY
        for s, holder in list(SLOT_HELD_BY.items()):
            try:
                if int(holder) == int(uid):
                    release_slot(int(s), uid, force=True)
                    released_slots.append(int(s))
            except Exception:
                continue
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG and released_slots:
            logger.debug("[CANCEL][HOLDS] uid=%s released_slots=%s", uid, released_slots)
    except Exception as e:
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[CANCEL][HOLDS][ERR] uid=%s err=%s", uid, e)

    # 3) Find the most recent pending booking for this user (if any)
    latest_pid = None
    latest_ts = 0
    try:
        for pid, pb in (PENDING_BOOKINGS or {}).items():
            try:
                if int(pb.get("user_id") or 0) != int(uid):
                    continue
            except Exception:
                continue
            ts = int(pb.get("created") or 0)
            if ts > latest_ts:
                latest_ts = ts
                latest_pid = pid
    except Exception as e:
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[CANCEL][PENDING][SCAN-ERR] uid=%s err=%s", uid, e)

    cancelled_slot = None
    if latest_pid is not None:
        # Remove the pending booking and its timeout job, and release that slot as well.
        pb = PENDING_BOOKINGS.pop(latest_pid, None) or {}
        try:
            if hasattr(context, "job_queue") and context.job_queue is not None:
                for j in list(context.job_queue.jobs()):
                    try:
                        if (j.name or "").startswith(f"paytimeout:{latest_pid}"):
                            j.schedule_removal()
                            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                                logger.debug("[CANCEL][PENDING] cancelled timeout job for pid=%s", latest_pid)
                    except Exception:
                        continue
        except Exception as e:
            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                logger.debug("[CANCEL][PENDING][TIMEOUT-ERR] pid=%s err=%s", latest_pid, e)

        try:
            cancelled_slot = int(pb.get("slot") or 0)
        except Exception:
            cancelled_slot = None
        if cancelled_slot:
            try:
                release_slot(cancelled_slot, uid, force=True)
                if cancelled_slot not in (released_slots or []):
                    released_slots.append(cancelled_slot)
            except Exception:
                pass

    # 4) Build a friendly message and, if possible, redraw the slot keyboard
    chat_id = getattr(update.effective_chat, "id", None)

    if latest_pid is None and not released_slots:
        text = "No active booking was found. If you previously held a slot, it may have already expired."
    else:
        text = "✅ Booking cancelled. Your held slot has been released." if released_slots else "✅ Booking cancelled."

    keyboard = None
    if chat_id is not None:
        try:
            async with promo_lock:
                promos = clear_expired_promotions() or {}
                keyboard = build_slot_inline_keyboard(promos, uid)
        except Exception as e:
            keyboard = None
            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                logger.debug("[CANCEL][KB][ERR] uid=%s err=%s", uid, e)

    try:
        if update.message:
            await update.message.reply_text(text, reply_markup=keyboard)
        else:
            # Fallback: send a fresh message to the chat if we don't have a standard Message
            if chat_id is not None:
                await context.bot.send_message(chat_id=chat_id, text=text, reply_markup=keyboard)
    except Exception as e:
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[CANCEL][SEND][ERR] uid=%s err=%s", uid, e)

async def start_helius_ws(app: Application) -> None:
    """Connect to Helius WS, subscribe to DEX logs (and optional Token Program + accountSubscribe for payments), and stream events."""
    try:
        globals()["_APP_REF"] = app
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[WS][BOOT] _APP_REF set for downstream tasks.")
    except Exception:
        pass

    if not globals().get("USE_HELIUS_WS", True):
        logger.info("[WS] USE_HELIUS_WS=False — listener disabled.")
        return

    url = _resolve_ws_url()
    if not url:
        logger.warning("[WS] No Helius WS URL resolved; skipping listener.")
        return

    def _mask_url(u: str) -> str:
        try:
            k = str(globals().get("HELIUS_API_KEY") or "")
            if not k:
                return u
            return u.replace(k, "***" + (k[-6:] if len(k) >= 6 else ""))
        except Exception:
            return u

    backoff = 1
    global WS_CONNECTED

    while True:
        try:
            masked = _mask_url(url)
            logger.info("[WS] Connecting to %s", masked)

            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                try:
                    logger.debug("[WS][OPEN] Using ping_interval=%s ping_timeout=%s", WS_PING_INTERVAL, WS_PING_TIMEOUT)
                except Exception:
                    pass

            async with websockets.connect(
                url,
                ping_interval=WS_PING_INTERVAL,
                ping_timeout=WS_PING_TIMEOUT,
                max_size=None
            ) as ws:
                WS_CONNECTED = True
                if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                    logger.debug("[WS][STATE] Connected successfully to Helius WebSocket.")

                # One-time baseline for SOL_RECEIVE_ADDRESS lamports
                try:
                    recv_addr = str(globals().get("SOL_RECEIVE_ADDRESS") or "").strip()
                    if recv_addr:
                        lam = _rpc_get_account_lamports(recv_addr)
                        if lam is not None:
                            globals()["_PAY_ADDR_LAST_LAMPORTS"] = int(lam)
                            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                                logger.debug("[PAY][BASELINE][RPC] %s lamports=%s (%.9f SOL)", recv_addr[:6]+"…", lam, lam/1_000_000_000.0)
                    else:
                        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                            logger.debug("[PAY][BASELINE][SKIP] No SOL_RECEIVE_ADDRESS set.")
                except Exception as _e:
                    if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                        logger.debug("[PAY][BASELINE][ERR] %s", _e)

                # Build subscription messages (DEX logs + optional Token Program)
                if globals().get("USE_DEX_PRIORITY_ONLY", False):
                    if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                        logger.debug(
                            "[WS][SUB][PLAN] dex_priority_only=True subscribe_all=True prio_sample=%s",
                            (globals().get("DEX_PRIORITY") or [])[:5]
                        )
                    msgs = build_logs_subscribe_messages(None, subscribe_all=True)
                    sub_mode, sub_ids = "all", []
                else:
                    program_ids = list((globals().get("DEX_PROGRAM_IDS") or {}).keys())
                    if not program_ids:
                        program_ids = list(globals().get("LISTEN_PROGRAM_IDS") or [])
                        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                            logger.debug("[WS][SUB][PLAN] using LISTEN_PROGRAM_IDS fallback; ids=%s", program_ids[:3])
                    if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                        logger.debug("[WS][SUB][PLAN] program_ids=%d subscribe_all=%s ids_sample=%s",
                                     len(program_ids),
                                     bool(globals().get("SUBSCRIBE_ALL_LOGS", False)),
                                     list(program_ids)[:3])
                    msgs = build_logs_subscribe_messages(program_ids,
                                                         subscribe_all=globals().get("SUBSCRIBE_ALL_LOGS", False))
                    if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                        logger.debug("[WS][SUB][MODE] using %s",
                                     "all" if (globals().get("SUBSCRIBE_ALL_LOGS", False) or not program_ids) else "mentions")
                    if bool(globals().get("SUBSCRIBE_ALL_LOGS", False)) or not program_ids:
                        sub_mode, sub_ids = "all", []
                    else:
                        sub_mode, sub_ids = "mentions", list(program_ids)

                # programSubscribe to Token Program (jsonParsed) — optional
                try:
                    token_prog = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
                    prog_msg = build_program_subscribe_message(
                        token_prog,
                        encoding="jsonParsed",
                        request_id=(msgs[-1]["id"] + 1) if (msgs and isinstance(msgs[-1], dict) and "id" in msgs[-1]) else 1
                    )
                    msgs.append(prog_msg)
                    if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                        logger.debug("[WS][SUB][ADD] programSubscribe %s -> id=%s", token_prog, prog_msg["id"])
                except Exception as e:
                    if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                        logger.debug("[WS][SUB][ADD][ERR] programSubscribe: %s", e)

                # --- Account subscribe for automatic payment detection (SOL_RECEIVE_ADDRESS) ---
                try:
                    recv_addr = str(globals().get("SOL_RECEIVE_ADDRESS") or "").strip()
                    if recv_addr:
                        acc_msg = {
                            "jsonrpc": "2.0",
                            "id": (msgs[-1]["id"] + 1) if (msgs and isinstance(msgs[-1], dict) and "id" in msgs[-1]) else 1,
                            "method": "accountSubscribe",
                            "params": [
                                recv_addr,
                                {"encoding": "jsonParsed", "commitment": str(globals().get("HELIUS_WS_COMMITMENT") or "confirmed")}
                            ],
                        }
                        msgs.append(acc_msg)
                        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                            logger.debug("[WS][SUB][ADD] accountSubscribe %s (commitment=%s)", recv_addr, acc_msg["params"][1]["commitment"])
                    else:
                        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                            logger.debug("[WS][SUB][ADD] Skipping accountSubscribe (no SOL_RECEIVE_ADDRESS).")
                except Exception as e:
                    if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                        logger.debug("[WS][SUB][ADD][ERR] accountSubscribe: %s", e)

                # Summary
                try:
                    logger.info(
                        "[WS] Subscriptions: mode=%s, commitment=%s, subscribe_all=%s, program_ids=%d",
                        sub_mode if 'sub_mode' in locals() else ("all" if bool(globals().get("SUBSCRIBE_ALL_LOGS", False)) else "mentions"),
                        str(globals().get("HELIUS_WS_COMMITMENT") or "confirmed"),
                        bool(globals().get("SUBSCRIBE_ALL_LOGS", False)),
                        0 if ('sub_mode' in locals() and sub_mode == "all") else len(locals().get("sub_ids", []))
                    )
                    if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                        logger.debug("[WS][SUB][SUMMARY] ids_sample=%s", (locals().get("sub_ids") or [])[:3])
                except Exception:
                    pass

                if not msgs:
                    logger.warning("[WS][SUB] No program IDs and SUBSCRIBE_ALL_LOGS is False. Skipping …")
                    if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                        logger.debug("[WS][SUB][SKIP] Skipping subscription and pausing before reconnect.")
                    await asyncio.sleep(10)
                    continue

                if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                    logger.debug("[WS][SUB] sending %d subscription message(s)", len(msgs))

                for m in msgs:
                    try:
                        if isinstance(m, str):
                            await ws.send(m)
                            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                                logger.debug("[WS][SUB][SENT][str] %s", m[:300])
                        else:
                            await ws.send(json.dumps(m))
                            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                                logger.debug("[WS][SUB][SENT][dict] %s", m)
                    except Exception as e:
                        logger.warning("[WS][SUB][ERR] %s", e)

                # Probe to confirm the accountSubscribe was in the sent batch
                try:
                    if any((isinstance(_m, dict) and _m.get("method") == "accountSubscribe") for _m in msgs):
                        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                            logger.debug("[WS][SUB][CHECK] accountSubscribe present in sent batch.")
                    else:
                        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                            logger.debug("[WS][SUB][CHECK] accountSubscribe NOT present in sent batch.")
                except Exception:
                    pass

                backoff = 1
                if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                    logger.debug("[WS][LOOP] Entering frame stream …")

                async for raw in ws:
                    if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                        try:
                            _preview = raw if isinstance(raw, str) else str(raw)
                            logger.debug("[WS][RAW<=] %s", _preview[:300])
                        except Exception:
                            pass
                    await _on_ws_message(app, raw)

        except Exception as e:
            WS_CONNECTED = False

            # Detect 429s
            _is_429 = False
            try:
                import websockets as _ws_mod
                if isinstance(e, getattr(_ws_mod.exceptions, "InvalidStatusCode", Exception)):
                    if getattr(e, "status_code", None) == 429:
                        _is_429 = True
            except Exception:
                pass
            if (not _is_429) and ("HTTP 429" in str(e) or "429" in str(e)):
                _is_429 = True

            if _is_429:
                cooldown = max(60, backoff, 120)
                logger.warning("[WS] 429 rate limit on connect; cooling down for %ss before retry.", cooldown)
                if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                    logger.debug("[WS][RETRY][429] SUBSCRIBE_ALL_LOGS=%s pids=%d",
                                 bool(globals().get('SUBSCRIBE_ALL_LOGS', False)),
                                 len(list((globals().get('DEX_PROGRAM_IDS') or {}).keys())))
                await asyncio.sleep(cooldown)
                backoff = min(max(cooldown // 2, 30), 300)
                continue

            logger.warning("[WS] Disconnected or connect error: %s", e)
            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                logger.debug("[WS][RETRY] Will reconnect after %.2fs (backoff=%s)", backoff, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)
        finally:
            if WS_CONNECTED:
                WS_CONNECTED = False
                if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                    logger.debug("[WS][STATE] Marked disconnected; preparing to retry.")

# --- Hydration state (edit-in-place) ---
LAST_ALERT_MSGS: dict[str, list[dict]] = {}
HYDRATE_WINDOW_SEC = 120
HYDRATE_DELAY_SEC = 6

# Verbose toggle for extremely detailed matching logs
# Verbose toggle for extremely detailed matching logs



# If deep debug is enabled, raise logger + handler levels to DEBUG so breadcrumbs are visible
try:
    if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
        logger.setLevel(logging.DEBUG)
        for _h in logger.handlers:
            try:
                _h.setLevel(logging.DEBUG)
            except Exception:
                pass
        logger.debug("[DEBUG][INIT] Elevated logging to DEBUG due to DEEP_DEBUG=True")
except Exception:
    pass

def _deep_debug_startup_snapshot():
    """One-time verbose environment + config snapshot for deep diagnostics.
    Safe to run in production; secrets are masked.
    """
    try:
        if not ('DEEP_DEBUG' in globals() and DEEP_DEBUG):
            return
        # Mask helpers
        def _mask(s):
            try:
                if not s:
                    return "(empty)"
                s = str(s)
                if len(s) <= 8:
                    return "***"
                return s[:2] + "***" + s[-4:]
            except Exception:
                return "(err)"

        # Masked config values
        _masked_bot = _mask(globals().get('BOT_TOKEN'))
        _masked_helius = _mask(globals().get('HELIUS_API_KEY'))
        _helius_url = globals().get('HELIUS_WS_URL')
        try:
            _masked_url = str(_helius_url).replace(str(globals().get('HELIUS_API_KEY') or ''), _mask(globals().get('HELIUS_API_KEY')))
        except Exception:
            _masked_url = _helius_url

        # Environment / runtime
        _venv = os.environ.get('VIRTUAL_ENV') or os.environ.get('CONDA_PREFIX') or '(none)'
        _cwd = os.getcwd()
        _pyexe = sys.executable
        _pyprefix = sys.prefix
        _pyver = sys.version.split(" (", 1)[0]

        # WS subscription mode flags
        _has_programs = bool(globals().get('DEX_PROGRAM_IDS'))
        _sub_all = bool(globals().get('SUBSCRIBE_ALL_LOGS'))

        # Group targets (normalized + expanded preview)
        try:
            _targets = GROUP_CHAT_IDS if isinstance(GROUP_CHAT_IDS, (list, tuple, set)) else [GROUP_CHAT_IDS]
        except Exception:
            _targets = []
        try:
            _expanded = list(_expand_chat_targets(_targets))
        except Exception:
            _expanded = []

        # Misc app config
        _buy_check_interval = globals().get('BUY_CHECK_INTERVAL')
        _helius_commit = globals().get('HELIUS_WS_COMMITMENT')
        _ws_qps = int(globals().get('_WS_PREFETCH_QPS') or 0)
        _enh_qps = globals().get('_ENH_RATE_LIMIT_QPS', 0)

        logger.debug("[BOOT][SNAPSHOT] DEEP_DEBUG=%s", DEEP_DEBUG)
        logger.debug("[BOOT][SNAPSHOT] Python=%s exe=%s prefix=%s", _pyver, _pyexe, _pyprefix)
        logger.debug("[BOOT][SNAPSHOT] CWD=%s VENV=%s", _cwd, _venv)
        logger.debug("[BOOT][SNAPSHOT] BOT_TOKEN=%s HELIUS_API_KEY=%s", _masked_bot, _masked_helius)
        logger.debug("[BOOT][SNAPSHOT] HELIUS_WS_URL=%s commitment=%s", _masked_url, _helius_commit)
        try:
            logger.debug("[BOOT][SNAPSHOT] RPC_HTTPS=%s", (str(RPC_HTTPS).replace(str(globals().get('HELIUS_API_KEY') or ''), '***KEY***') if 'RPC_HTTPS' in globals() else '(unset)'))
        except Exception:
            pass
        logger.debug("[BOOT][SNAPSHOT] SUBSCRIBE: programs=%s subscribe_all=%s", _has_programs, _sub_all)
        logger.debug("[BOOT][SNAPSHOT] BUY_CHECK_INTERVAL=%s", _buy_check_interval)
        logger.debug("[BOOT][SNAPSHOT] WS_PREFETCH_QPS=%s ENH_QPS=%s", _ws_qps, _enh_qps)
        logger.debug("[BOOT][SNAPSHOT] GROUP_CHAT_IDS(raw)=%s", list(_targets))
        logger.debug("[BOOT][SNAPSHOT] GROUP_CHAT_IDS(expanded)=%s", list(_expanded))
    except Exception as _e:
        try:
            logger.debug("[BOOT][SNAPSHOT][ERR] %s", _e)
        except Exception:
            pass

try:
    _deep_debug_startup_snapshot()
except Exception:
    pass

# ======= Live Buy gating options =======
# If False: post immediately (no Helius gating)
# If True:  wait for a real Helius SWAP (from WebSocket) to hydrate the alert
USE_HELIUS_GATE = False  # enabled: only hydrate after DS trigger (saves credits)

# === Runtime mode ===
# Enforce WebSocket-only mode. When True, *all* REST getTransaction hydrations
# are disabled to prevent credit burn; we rely solely on WS frames.
WS_ONLY_MODE = True

# When True, suppress signature-less programNotification posts; only logsNotification with a real tx signature may post
FORCE_SIGNATURE_ONLY = True

# --- UI layout toggles ------------------------------------------------------
# When True: inline keyboard buttons are rendered one-per-row (max width),
# and the visual separator line in live-buy messages is lengthened.
if 'WIDE_LIVE_BUY_LAYOUT' not in globals():
    WIDE_LIVE_BUY_LAYOUT = True

# How many seconds to wait for Helius before giving up (used only if USE_HELIUS_GATE is True + fallback enabled)
DS_FALLBACK_TIMEOUT_SEC = 600

# === Dexscreener trigger window (DS sets "armed"; Helius swap hydrates alert) ===
DS_TRIGGERED: dict[str, float] = {}     # mint -> last trigger ts
DS_TRIGGER_WINDOW_SEC = 600             # post only if swap arrives within this window
# Require Dexscreener arming before accepting a Helius swap?
# Set to True to require DS arming before accepting a Helius swap.
REQUIRE_DS_TRIGGER = False  # require DS arming before accepting a Helius swap.

# --- Cheap gate: are there *any* active DS triggers within the window? ---
from collections import deque as _deque_for_ws

def _ds_trigger_active(now_ts: float | None = None) -> bool:
    """Return True if at least one mint is armed within DS_TRIGGER_WINDOW_SEC."""
    try:
        now = now_ts or time.time()
        # prune expired
        for m, ts in list(DS_TRIGGERED.items()):
            if (now - float(ts)) > float(DS_TRIGGER_WINDOW_SEC):
                DS_TRIGGERED.pop(m, None)
        return bool(DS_TRIGGERED)
    except Exception:
        return False

# --- WS-side prefetch rate limiter (drop before Enhanced fetch) -------------
_WS_PREFETCH_QPS = 3        # hard cap of enhanced lookups per second
_WS_PREFETCH_WINDOW = 1.0   # seconds
_WS_PREFETCH_TS = _deque_for_ws()

# --- Per-mint alert throttle (anti-flood) ---
THROTTLE_WINDOW_SEC = 50       # rolling window length in seconds
MAX_ALERTS_PER_TOKEN = 3       # maximum alerts per token within the window
_ALERT_WINDOW = defaultdict(deque)

# --- Global alert throttle (cap total posts across ALL tokens) ---
GLOBAL_WINDOW_SEC = 15        # rolling window length (seconds)
GLOBAL_MAX_ALERTS = 4         # max total trend posts allowed within the window
_GLOBAL_POSTED = deque()      # deque of timestamps

# --- One-time trending announcement guard (slot -> last_end_time posted) ---
LAST_TREND_BROADCAST: dict[int, int] = {}

# --- Per-mint + global anti-flood (rolling gates) ---
_MINT_ALERT_TIMES = {}  # mint_lc -> list of timestamps
_GLOBAL_ALERT_TIMES = []

def allow_alert_for_mint(mint_lc: str, window_sec: int = 120, max_alerts: int = 2) -> bool:
    import time
    now = time.time()
    q = _MINT_ALERT_TIMES.setdefault((mint_lc or "").lower(), [])
    # evict old
    q[:] = [t for t in q if now - t <= window_sec]
    allowed = len(q) < max_alerts
    if allowed:
        q.append(now)
    if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
        try:
            logger.debug("[THROTTLE][MINT] mint=%s window=%ss max=%s now=%s count=%s allowed=%s",
                         mint_lc, window_sec, max_alerts, now, len(q), allowed)
        except Exception:
            pass
    return allowed

def allow_global_alert(now_ts: float, window_sec: int = 60, max_alerts: int = 4) -> bool:
    global _GLOBAL_ALERT_TIMES
    _GLOBAL_ALERT_TIMES[:] = [t for t in _GLOBAL_ALERT_TIMES if now_ts - t <= window_sec]
    allowed = len(_GLOBAL_ALERT_TIMES) < max_alerts
    if allowed:
        _GLOBAL_ALERT_TIMES.append(now_ts)
    if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
        try:
            logger.debug("[THROTTLE][GLOBAL] window=%ss max=%s now=%s count=%s allowed=%s",
                         window_sec, max_alerts, now_ts, len(_GLOBAL_ALERT_TIMES), allowed)
        except Exception:
            pass
    return allowed

# Use larger HTTP timeouts for Telegram API calls to avoid startup timeouts
_tg_request = HTTPXRequest(connect_timeout=20.0, read_timeout=30.0)
application = Application.builder().token(BOT_TOKEN).request(_tg_request).build()

# --- Deep debug tap for ALL incoming updates (runs before any other handler) ---
async def _dbg_incoming(update: object, context: CallbackContext) -> None:
    if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
        try:
            chat_id = getattr(getattr(update, "effective_chat", None), "id", None)
            user_id = getattr(getattr(update, "effective_user", None), "id", None)
            msg = getattr(update, "message", None)
            text = getattr(msg, "text", None)
            logger.debug("[UPD][RAW] type=%s chat_id=%s user_id=%s text=%s",
                         type(update).__name__, chat_id, user_id, (text[:120] if isinstance(text, str) else text))
        except Exception as e:
            logger.debug("[UPD][RAW][ERR] %s", e)

# Early debug tap: log all incoming updates before other handlers
try:
    application.add_handler(TypeHandler(Update, _dbg_incoming), group=-1)
    if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
        logger.debug("[UPD][REG] Added _dbg_incoming TypeHandler at group=-1")
except Exception as _e:
    logger.warning("[UPD][REG][ERR] %s", _e)


# --- /start handler with deep debug + duplicate-safe registration ---

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
        logger.debug("[/start] handler invoked for chat_id=%s user_id=%s", getattr(getattr(update, 'effective_chat', None), 'id', None), getattr(getattr(update, 'effective_user', None), 'id', None))
    # Original logic follows
    try:
        _chat = getattr(update, "effective_chat", None)
        _user = getattr(update, "effective_user", None)
        logger.debug("[START][CALL] chat_id=%s type=%s title=%s user_id=%s username=%s",
                     getattr(_chat, "id", None),
                     getattr(_chat, "type", None),
                     getattr(_chat, "title", None) or getattr(_chat, "username", None),
                     getattr(_user, "id", None),
                     getattr(_user, "username", None))
    except Exception:
        pass

    try:
        async with promo_lock:
            promos = clear_expired_promotions()
            keyboard = build_slot_inline_keyboard(promos, getattr(update.effective_user, 'id', None))
    except Exception as e:
        keyboard = None
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[START][UI][ERR] %s", e)

    header = ["👇 Select a slot to book:", "(You can /cancel at any time to quit)"]

    try:
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="\n".join(header),
            parse_mode="HTML",
            disable_web_page_preview=True,
            reply_markup=keyboard,
        )
    except Exception as e:
        logger.warning("[START][ERR] %s", e)

def register_start_handler_once(app: Application) -> None:
    """Force-register /start (and /help) so it always works, even after hot-reloads.
    Also add a regex-based fallback that catches variations like /start@YourBot.
    """
    try:
        # Primary: explicit CommandHandler
        app.add_handler(CommandHandler(["start", "help"], cmd_start), group=0)
        # Fallback: regex so /start@botname also hits even if command parsing fails
        app.add_handler(MessageHandler(filters.Regex(r"^/start(?:@\w+)?$"), cmd_start), group=0)
        # Simple health check command to verify that commands reach the bot in any chat
        async def _cmd_ping(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
            try:
                await context.bot.send_message(chat_id=update.effective_chat.id, text="pong ✅")
            except Exception as e:
                logger.warning("[PING][ERR] %s", e)
        app.add_handler(CommandHandler("ping", _cmd_ping), group=0)

        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[START][REG] (forced) Added /start,/help + regex fallback + /ping at group=0")
    except Exception as _e:
        logger.warning("[START][REG][ERR] %s", _e)

# Ensure the /start handler is present once.

register_start_handler_once(application)

# --- Register cancel handlers once (avoid duplicate registration) ---
def register_cancel_handlers_once(app: Application) -> None:
    # Prevent duplicate registration across hot-reloads
    if globals().get("_CANCEL_HANDLERS_ADDED"):
        return
    try:
        app.add_handler(CallbackQueryHandler(_cb_cancel_booking, pattern=r"^cancel:"))
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[BOOK][REG] Added cancel CallbackQueryHandler")
    except Exception as _e:
        logger.warning("[BOOK][REG][ERR] cancel cb: %s", _e)
    try:
        app.add_handler(CommandHandler("cancel", cmd_cancel))
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[BOOK][REG] Added /cancel command")
    except Exception as _e:
        logger.warning("[BOOK][REG][ERR] /cancel: %s", _e)
    globals()["_CANCEL_HANDLERS_ADDED"] = True

register_cancel_handlers_once(application)

import re as _re


async def on_slot_select(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Slot tap handler.
    - Extract slot id robustly.
    - If bookings disabled and user is not admin -> block.
    - Atomically HOLD the slot so others see 'Being booked'.
    - Begin booking flow -> ask_token_name (ForceReply to capture replies in groups/DMs).
    - Starts a 4-minute booking deadline so incomplete bookings auto-expire.
    """
    try:
        q = update.callback_query
        data = str(getattr(q, "data", "") or "")
        uid = get_user_id_from_update(update)

        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug(
                "[BOOK][CB] raw_cb=%s uid=%s chat=%s",
                data,
                getattr(getattr(q, 'from_user', None), 'id', None),
                getattr(getattr(update, 'effective_chat', None), 'id', None),
            )

        # Always try to answer the callback to avoid Telegram client spinners
        try:
            await q.answer()
        except Exception:
            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                logger.debug("[BOOK][CB][ANSWER-ERR] raw_cb=%s", data)

        # Robust slot-id extraction from several callback_data shapes
        # Accepts e.g. "slot:1", "book-2", "select 3", "choose_04", etc.
        try:
            # Use the same regex helper alias that is imported as "_re"
            m = _re.search(r"(?:book|slot|select|choose)\s*[:_\-= ]?\s*(\d{1,2})", data, _re.I)
        except Exception as e:
            m = None
            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                logger.debug("[BOOK][CB][REGEX-ERR] data=%s err=%s", data, e)

        if not m:
            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                logger.debug("[BOOK][CB][SKIP] pattern-miss: %s", data)
            return

        try:
            slot = int(m.group(1))
        except Exception:
            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                logger.debug("[BOOK][CB][PARSE-ERR] m.group(1) invalid: %s", getattr(m, "group", lambda *_: None)(1))
            return

        # Gate on global bookings flag for non-admin users
        try:
            on = bool(is_bookings_enabled())
        except Exception:
            on = bool(globals().setdefault("BOOKINGS_ENABLED", True))

        if (not on) and (not is_admin_user(uid)):
            try:
                await context.bot.send_message(update.effective_chat.id, "⛔ Bookings are currently disabled.")
            except Exception as e:
                if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                    logger.debug("[BOOK][CB][BLOCK-SEND-ERR] uid=%s slot=%s err=%s", uid, slot, e)
            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                logger.debug("[BOOK][CB][BLOCK] uid=%s slot=%s -> disabled", uid, slot)
            return

        # Atomic: check -> hold -> re-render keyboard
        async with promo_lock:
            promos_now = clear_expired_promotions() or {}

            # Already booked?
            if (str(slot) in promos_now) or (slot in promos_now):
                if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                    logger.debug("[BOOK][CB] slot=%s already BOOKED; denying.", slot)
                try:
                    await context.bot.send_message(update.effective_chat.id, f"❌ Slot #{slot} is already booked.")
                except Exception as e:
                    if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                        logger.debug("[BOOK][CB][BOOKED-SEND-ERR] slot=%s err=%s", slot, e)
                return

            # Admin-only gate
            try:
                admin_only = set(globals().get("ADMIN_ONLY_SLOTS") or {13, 14, 15, 16, 17})
            except Exception:
                admin_only = {13, 14, 15, 16, 17}

            if slot in admin_only and not is_admin_user(uid):
                if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                    logger.debug("[BOOK][CB] slot=%s admin-only; uid=%s denied.", slot, uid)
                try:
                    await context.bot.send_message(update.effective_chat.id, f"🔒 Slot #{slot} is admin-only.")
                except Exception as e:
                    if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                        logger.debug("[BOOK][CB][ADMIN-SEND-ERR] slot=%s err=%s", slot, e)
                return

            # Held by someone else?
            if slot_held_by_other(slot, uid):
                if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                    logger.debug(
                        "[BOOK][CB] slot=%s held by other user=%s; uid=%s denied.",
                        slot,
                        SLOT_HELD_BY.get(slot),
                        uid,
                    )
                try:
                    await context.bot.send_message(
                        update.effective_chat.id,
                        f"⏳ Slot #{slot} is currently being booked by another user.",
                    )
                except Exception as e:
                    if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                        logger.debug("[BOOK][CB][HELD-SEND-ERR] slot=%s err=%s", slot, e)
                return

            # Place/refresh our hold
            try:
                hold_slot(slot, uid)
                if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                    logger.debug("[BOOK][CB][HOLD] slot=%s uid=%s", slot, uid)
            except Exception as e:
                if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                    logger.debug("[BOOK][CB][HOLD-ERR] slot=%s uid=%s err=%s", slot, uid, e)
                return

            # Re-render keyboard (shows 'Being booked' for others)
            try:
                kb = build_slot_inline_keyboard(promos_now, uid)
                if q and q.message:
                    await q.message.edit_reply_markup(reply_markup=kb)
            except Exception as e:
                if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                    logger.debug("[BOOK][CB][EDIT-KB][ERR] %s", e)

        # Persist the selected slot + step state for this user
        context.user_data["booking"] = {"slot": slot, "step": "ask_token_name"}
        try:
            context.user_data["booking_slot"] = int(slot)
            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                logger.debug("[BOOK][STATE] booking_slot=%s saved for user=%s", slot, uid)
        except Exception as e:
            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                logger.debug("[BOOK][STATE-ERR] slot=%s uid=%s err=%s", slot, uid, e)

        # Start a 4-minute booking deadline tied to this user/slot so incomplete bookings auto-expire.
        try:
            if '_set_booking_deadline' in globals():
                chat_obj = getattr(update, "effective_chat", None)
                chat_id = getattr(chat_obj, "id", None) or uid
                _set_booking_deadline(context, int(slot), int(uid), int(chat_id))
                if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                    logger.debug(
                        "[BOOK][DEADLINE][SET] slot=%s user=%s chat_id=%s",
                        slot,
                        uid,
                        chat_id,
                    )
            elif 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                logger.debug("[BOOK][DEADLINE][SKIP] _set_booking_deadline not in globals()")
        except Exception as e:
            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                logger.debug("[BOOK][DEADLINE][ERR] slot=%s user=%s err=%s", slot, uid, e)

        # Begin flow — ForceReply ensures the user's reply is captured by handle_booking_reply
        try:
            await context.bot.send_message(
                update.effective_chat.id,
                f"📝 Reserving slot #{slot}.\n"
                f"Please reply with the <b>Token Name</b>.\n\n"
                "⏰ You have 4 minutes to complete your booking.\n"
                "Send /cancel to quit.",
                parse_mode="HTML",
                reply_markup=ForceReply(selective=True),
            )
        except Exception as e:
            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                logger.debug("[BOOK][CB][PROMPT-ERR] slot=%s uid=%s err=%s", slot, uid, e)
            # If we cannot send the message, release the hold so we don't lock the slot
            try:
                release_slot(slot, uid, force=True)
                if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                    logger.debug("[BOOK][CB][PROMPT-FAIL-RELEASE] slot=%s uid=%s", slot, uid)
            except Exception:
                pass
            return

        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[BOOK][FLOW] uid=%s slot=%s -> step=ask_token_name (HOLD active)", uid, slot)

    except Exception as e:
        logger.warning("[BOOK][CB][ERR] %s", e)


# --- Handler for pinned "Add Your Token To Trending" button ---
async def _cb_open_bookings(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Opens the slot-selection keyboard from the pinned 'Add Your Token To Trending' button."""
    q = update.callback_query
    try:
        await q.answer()
    except Exception:
        pass
    user_id = get_user_id_from_update(update)
    try:
        async with promo_lock:
            promos = clear_expired_promotions() or {}
            kb = build_slot_inline_keyboard(promos, user_id)
    except Exception as e:
        kb = None
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[BOOK][OPEN][ERR] %s", e)
    intro = "👇 Select a slot to book:"
    try:
        if q and q.message:
            await q.message.edit_text(intro, reply_markup=kb, parse_mode="HTML", disable_web_page_preview=True)
        else:
            await context.bot.send_message(chat_id=update.effective_chat.id, text=intro, reply_markup=kb, parse_mode="HTML", disable_web_page_preview=True)
    except Exception as e:
        logger.warning("[BOOK][OPEN][POST-ERR] %s", e)

def _fmt_price_two_dec(x) -> str:
    try:
        return f"{float(x):.2f}"
    except Exception:
        return "0.00"

def _held_by(slot: int):
    try:
        return (globals().get("SLOT_HELD_BY") or {}).get(int(slot))
    except Exception:
        return None

def build_slot_inline_keyboard(promos: dict, viewer_user_id: int | None) -> InlineKeyboardMarkup | None:
    """
    Builds the slot chooser keyboard:
      - BOOKED -> 🚫 BOOKED
      - Held by others -> ⏳ Being booked
      - If bookings disabled, non-admins see DISABLED on otherwise available slots
      - Admin-only slots (13–17) are hidden from non-admins; admins see them labeled 'Admin'
      - Admins get a bottom toggle button (Enable/Disable Bookings)
      - Admins also see a 'Remove Trending (Admin)' button directly under the toggle
    """
    try:
        is_admin = is_admin_user(viewer_user_id)
    except Exception:
        is_admin = False

    try:
        on = bool(is_bookings_enabled())
    except Exception:
        on = bool(globals().setdefault("BOOKINGS_ENABLED", True))

    rows: list[list[InlineKeyboardButton]] = []
    promos = promos or {}

    def _slot_conf(s: int) -> tuple[float, float]:
        try:
            conf = (globals().get("TRENDING_SLOTS") or {}).get(int(s)) or {}
            hours = float(conf.get("duration") or 0.0)
            price = float(conf.get("price") or 0.0)
            return hours, price
        except Exception:
            return 0.0, 0.0

    try:
        admin_only = set(globals().get("ADMIN_ONLY_SLOTS") or {13, 14, 15, 16, 17})
    except Exception:
        admin_only = {13, 14, 15, 16, 17}

    # Which slots to show to this viewer
    all_slots = sorted((globals().get("TRENDING_SLOTS") or {}).keys()) or list(range(1, 13)) + list(admin_only)
    show_slots: list[int] = []
    for s in all_slots:
        if (s in admin_only) and (not is_admin):
            continue  # hide admin slots from non-admins
        show_slots.append(int(s))

    for s in show_slots:
        hours, price = _slot_conf(s)
        booked = bool(promos.get(str(s)) or promos.get(s))
        held_by_other = slot_held_by_other(int(s), int(viewer_user_id or 0))
        admin_tag = " · Admin" if (s in admin_only and is_admin) else ""

        if booked:
            label = f"🚫 Slot {s}: {hours:g}h - BOOKED{admin_tag}"
            cb = f"noop:booked:{s}"
        elif held_by_other:
            label = f"⏳ Slot {s}: {hours:g}h - Being booked{admin_tag}"
            cb = f"noop:held:{s}"
        else:
            if (not on) and (not is_admin):
                label = f"🚫 Slot {s}: {hours:g}h - DISABLED{admin_tag}"
                cb = f"noop:disabled:{s}"
            else:
                if price > 0:
                    label = f"✅ Slot {s}: {hours:g}h - {price:.2f} SOL{admin_tag}"
                else:
                    label = f"✅ Slot {s}: {hours:g}h - 0.00 SOL{admin_tag}"
                cb = f"slot:{s}"

        rows.append([InlineKeyboardButton(label, callback_data=cb)])

    # Admin toggle row
    try:
        if is_admin and len(rows) > 0:
            # Admin-only price adjust button directly under slot rows
            try:
                rows.append([InlineKeyboardButton("⚙️ Adjust Slot Prices (Admin)", callback_data="ADMIN_PRICES_MENU")])
                if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                    logger.debug("[KB][ADMIN-PRICE][BTN] added under slots; rows=%s", len(rows))
            except Exception as e:
                if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                    logger.debug("[KB][ADMIN-PRICE][ERR] %s", e)

            toggle_label = "🔴 Disable Bookings" if on else "🟢 Enable Bookings"
            toggle_cb = "bookings:disable" if on else "bookings:enable"
            rows.append([InlineKeyboardButton(toggle_label, callback_data=toggle_cb)])

            # Admin-only: place Remove Trending button directly BELOW the toggle
            if promos:
                rows.append([InlineKeyboardButton("🗑 Remove Trending (Admin)", callback_data="ADMIN_REMOVE")])
                if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                    logger.debug("[KB][ADMIN-REMOVE][BTN] added under toggle; promos=%s", len(promos))
    except Exception as e:
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[KB][ADMIN-TOGGLE][ERR] %s", e)

    if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
        try:
            logger.debug("[KB][BUILD] rows=%s viewer=%s admin=%s on=%s", len(rows), viewer_user_id, is_admin, on)
        except Exception:
            pass

    try:
        return InlineKeyboardMarkup(rows)
    except Exception as e:
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[KB][BUILD][ERR] %s", e)
        return None
    
def build_admin_remove_keyboard(promos: dict) -> InlineKeyboardMarkup:
    """
    Build a keyboard listing each currently trending slot so an admin can remove one.
    """
    try:
        items = []
        for slot in sorted(promos, key=lambda x: int(x)):
            p = promos.get(slot) or {}
            name = (p.get("token_name") or p.get("name") or p.get("symbol") or "Token").strip()
            btn_txt = f"❌ Slot {int(slot)} · {name}"
            items.append([InlineKeyboardButton(btn_txt, callback_data=f"ADMIN_REMOVE_SLOT:{int(slot)}")])
        # Back action to return to the main chooser (use your existing upper-case route)
        items.append([InlineKeyboardButton(
            "↩️ Back",
            callback_data=("DISABLE_BOOKINGS" if not is_bookings_enabled() else "ENABLE_BOOKINGS")
        )])
        return InlineKeyboardMarkup(items)
    except Exception as e:
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[ADMIN][REMOVE][KB][ERR] %s", e)
        return InlineKeyboardMarkup([[InlineKeyboardButton("No trending tokens", callback_data="DISABLED")]])


# === Admin price adjust helpers and handlers ===
def _adjust_all_slot_prices(percent: float) -> None:
    """
    Adjust all TRENDING_SLOTS prices by the given percent (e.g. +10 or -25).
    0 or missing prices are left at 0. Values are rounded to 4 decimals.
    """
    try:
        slots = globals().get("TRENDING_SLOTS") or {}
        factor = 1.0 + (float(percent) / 100.0)
    except Exception:
        return

    changed = {}
    for s, conf in list(slots.items()):
        try:
            price = float((conf or {}).get("price") or 0.0)
        except Exception:
            continue
        if price <= 0:
            continue  # keep free/admin slots at 0
        new_price = max(price * factor, 0.0)
        try:
            new_price_rounded = float(f"{new_price:.4f}")
        except Exception:
            new_price_rounded = new_price
        try:
            conf["price"] = new_price_rounded
        except Exception:
            continue
        changed[int(s)] = (price, new_price_rounded)

    if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
        try:
            logger.debug(
                "[PRICES][ADJUST] percent=%s changed=%s",
                percent,
                {k: {"old": v[0], "new": v[1]} for k, v in changed.items()},
            )
        except Exception:
            pass


async def _cb_price_adjust_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Admin-only: show quick buttons to increase/decrease all slot prices by a percentage.
    """
    q = update.callback_query
    try:
        await q.answer()
    except Exception:
        pass

    uid = get_user_id_from_update(update)
    if not is_admin_user(uid):
        try:
            await context.bot.answer_callback_query(
                q.id,
                text="Admins only.",
                show_alert=False,
            )
        except Exception:
            pass
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[PRICES][MENU][DENY] uid=%s is not admin", uid)
        return

    # Build small adjust keyboard
    try:
        kb = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("⬆️ +10%", callback_data="ADMIN_PRICE:+10"),
                InlineKeyboardButton("⬇️ -10%", callback_data="ADMIN_PRICE:-10"),
            ],
            [
                InlineKeyboardButton("⬆️ +25%", callback_data="ADMIN_PRICE:+25"),
                InlineKeyboardButton("⬇️ -25%", callback_data="ADMIN_PRICE:-25"),
            ],
        ])
    except Exception as e:
        kb = None
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[PRICES][MENU][KB-ERR] %s", e)

    text = (
        "Select how much to adjust <b>all slot prices</b>.\n"
        "This affects paid slots only; admin/free slots stay at 0.00 SOL."
    )
    try:
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=text,
            parse_mode="HTML",
            reply_markup=kb,
        )
    except Exception as e:
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[PRICES][MENU][SEND-ERR] uid=%s err=%s", uid, e)


async def _cb_price_adjust_apply(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Admin-only: apply a percentage adjustment to all slot prices.
    """
    q = update.callback_query
    try:
        await q.answer()
    except Exception:
        pass

    uid = get_user_id_from_update(update)
    if not is_admin_user(uid):
        try:
            await context.bot.answer_callback_query(q.id, text="Admins only.", show_alert=False)
        except Exception:
            pass
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[PRICES][APPLY][DENY] uid=%s is not admin", uid)
        return

    data = str(getattr(q, "data", "") or "")
    try:
        _, pct_str = data.split(":", 1)
        pct = float(pct_str)
    except Exception as e:
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[PRICES][APPLY][PARSE-ERR] data=%s err=%s", data, e)
        return

    try:
        _adjust_all_slot_prices(pct)
    except Exception as e:
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[PRICES][APPLY][ERR] pct=%s err=%s", pct, e)
        return

    direction = "increased" if pct > 0 else "decreased" if pct < 0 else "updated"
    pct_abs = abs(pct)

    # Rebuild latest slot keyboard so admin sees new prices
    kb = None
    try:
        async with promo_lock:
            promos = clear_expired_promotions() or {}
            kb = build_slot_inline_keyboard(promos, uid)
    except Exception as e:
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[PRICES][APPLY][KB-ERR] %s", e)

    text = f"✅ All slot prices {direction} by {pct_abs:.1f}%."

    try:
        if q and q.message:
            await q.message.edit_text(
                text,
                parse_mode="HTML",
                reply_markup=kb,
                disable_web_page_preview=True,
            )
        else:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=text,
                parse_mode="HTML",
                reply_markup=kb,
                disable_web_page_preview=True,
            )
    except Exception as e:
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[PRICES][APPLY][SEND-ERR] uid=%s err=%s", uid, e)

async def handle_booking_reply(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Booking flow (admin & paid):
      1) ask_token_name  -> store name -> ask mint
      2) ask_token_mint  -> store mint -> (try resolve pair)
         - for admin-only slots (13–17, admin user): ask_duration -> store custom hours
      3) confirm         -> admins/free: start immediately; paid: show payment + 5m window and auto-confirm on-chain
    """
    text = (getattr(update.message, "text", "") or "").strip()
    uid = get_user_id_from_update(update)
    data = context.user_data.get("booking") or {}
    step = data.get("step")
    slot = data.get("slot")

    # Enforce shared 4-minute booking time limit for this ForceReply flow
    try:
        if '_booking_deadline_expired' in globals() and _booking_deadline_expired(context):
            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                logger.debug("[BOOK][FLOW][DEADLINE] expired for uid=%s slot=%s step=%s", uid, slot, step)
            if '_handle_booking_timeout' in globals():
                await _handle_booking_timeout(update, context)
            return
    except Exception as e:
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[BOOK][FLOW][DEADLINE][ERR] %s", e)

    if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
        try:
            logger.debug("[BOOK][FLOW] uid=%s step=%s slot=%s text=%s", uid, step, slot, text[:80])
        except Exception:
            pass

    # Defensive checks
    if not slot or step not in {"ask_token_name", "ask_token_mint", "ask_duration", "confirm"}:
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[BOOK][GUARD] Invalid state: slot=%s step=%s", slot, step)
        return

    # Step 1: collect token name
    if step == "ask_token_name":
        if not text or len(text) < 2:
            await update.message.reply_text("Please enter a valid token name (at least 2 characters).")
            return
        data["token_name"] = text.strip()
        data["step"] = "ask_token_mint"
        context.user_data["booking"] = data
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[BOOK][STEP] token_name=%s -> ask_token_mint", data["token_name"])
        await update.message.reply_text(
            "Now send the <b>Token <i>Mint</i> Address</b> (the SPL token address).",
            parse_mode="HTML"
        )
        return

    # Step 2: collect token mint and try to infer pair
    if step == "ask_token_mint":
        mint = text.strip()
        mint_lc = (mint or "").strip().lower()

        # --- HARD BLOCK: do not allow the same token to be booked twice at the same time ---
        try:
            # 1) Active promos check (strict, current bookings)
            existing_slot = find_slot_for_mint(mint_lc)
        except Exception:
            existing_slot = None
        try:
            # 2) Pending payment check (someone is already paying for this exact mint)
            pending_for_mint = False
            _pb = globals().get("PENDING_BOOKINGS") or {}
            for _pid, _p in list(_pb.items()):
                try:
                    if str((_p or {}).get("token_address", "")).strip().lower() == mint_lc:
                        pending_for_mint = True
                        break
                except Exception:
                    continue
        except Exception:
            pending_for_mint = False

        if existing_slot or pending_for_mint:
            try:
                # Release our temporary hold since we are aborting the flow
                release_slot(int(slot), uid, force=True)
            except Exception:
                pass
            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                logger.debug(
                    "[BOOK][BLOCK][DUP] mint=%s existing_slot=%s pending=%s",
                    mint_lc, existing_slot, pending_for_mint
                )
            msg = (
                "❌ This token is already trending or pending confirmation.\n"
                + (f"Currently in <b>slot #{existing_slot}</b>.\n" if existing_slot else "")
                + "You can't book the same token twice at the same time."
            )
            await update.message.reply_text(msg, parse_mode="HTML", disable_web_page_preview=True)
            context.user_data.pop("booking", None)
            return

        # Continue normal flow if not duplicate
        data["token_address"] = mint

        # Try dexscreener to guess the best pair
        pair = ""
        try:
            p = fetch_dexscreener_best_pair_for_mint(mint) if "fetch_dexscreener_best_pair_for_mint" in globals() else None
            if isinstance(p, dict):
                pair = (p.get("pairAddress") or p.get("pair") or "") or ""
        except Exception as e:
            pair = ""
            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                logger.debug("[BOOK][PAIR][ERR] mint=%s err=%s", mint, e)

        data["pair_address"] = pair

        # Detect admin-only slot and branch into custom-duration step for admin 13–17
        is_admin = bool(uid in ADMIN_USER_IDS)
        try:
            slot_int = int(slot)
        except Exception:
            slot_int = None
        try:
            admin_only = set(globals().get("ADMIN_ONLY_SLOTS") or {13, 14, 15, 16, 17})
        except Exception:
            admin_only = {13, 14, 15, 16, 17}

        if is_admin and slot_int in admin_only:
            data["step"] = "ask_duration"
            context.user_data["booking"] = data
            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                logger.debug("[BOOK][STEP] admin uid=%s slot=%s -> ask_duration", uid, slot)
            await update.message.reply_text(
                "Enter the duration in <b>hours</b> for this admin slot (e.g. 1, 2.5, 24).",
                parse_mode="HTML"
            )
            return

        # Non-admin (or non-admin-only) -> go straight to confirm as before
        data["step"] = "confirm"
        context.user_data["booking"] = data

        # Show summary and ask to confirm
        price = float((TRENDING_SLOTS.get(slot) or {}).get("price") or 0.0)
        hours = float((TRENDING_SLOTS.get(slot) or {}).get("duration") or 0.0)
        pay_line = "Free (admin)" if (is_admin or price == 0.0) else f"{price:.4f} SOL"
        pair_line = pair or "(will auto-detect)"
        msg = (
            f"Please confirm your booking:\n"
            f"• Slot: #{slot} — {hours:g} hour(s)\n"
            f"• Token: {data['token_name']}\n"
            f"• Mint: <code>{mint}</code>\n"
            f"• Pair: <code>{pair_line}</code>\n"
            f"• Price: <b>{pay_line}</b>\n\n"
            f"Reply with 'yes' to confirm or 'no' to cancel."
        )
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[BOOK][CONFIRM] uid=%s slot=%s price=%s hours=%s pair=%s", uid, slot, price, hours, pair_line)
        await update.message.reply_text(msg, parse_mode="HTML", disable_web_page_preview=True)
        return

    # Step 2b: admin-only slots 13–17 -> collect custom duration in hours
    if step == "ask_duration":
        # Only admins should ever hit this, but guard anyway
        is_admin = bool(uid in ADMIN_USER_IDS)
        if not is_admin:
            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                logger.debug("[BOOK][STEP][ASK-DUR][DENY] non-admin uid=%s slot=%s", uid, slot)
            await update.message.reply_text("Only admins can set custom durations for these slots.")
            context.user_data.pop("booking", None)
            return

        raw = text.replace(",", ".")
        try:
            hours = float(raw)
        except Exception:
            await update.message.reply_text("Please enter a valid number of hours (e.g. 1, 2.5, 24).")
            return
        if hours <= 0:
            await update.message.reply_text("Duration must be greater than 0 hours.")
            return

        data["duration_hours"] = float(hours)
        data["step"] = "confirm"
        context.user_data["booking"] = data

        price = float((TRENDING_SLOTS.get(slot) or {}).get("price") or 0.0)
        pair_line = data.get("pair_address") or "(will auto-detect)"
        pay_line = "Free (admin)"  # admin slots; price is effectively 0 for admin user

        msg = (
            f"Please confirm your booking:\n"
            f"• Slot: #{slot} — {hours:g} hour(s)\n"
            f"• Token: {data.get('token_name')}\n"
            f"• Mint: <code>{data.get('token_address')}</code>\n"
            f"• Pair: <code>{pair_line}</code>\n"
            f"• Price: <b>{pay_line}</b>\n\n"
            f"Reply with 'yes' to confirm or 'no' to cancel."
        )
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[BOOK][CONFIRM][ADMIN-DUR] uid=%s slot=%s price=%s hours=%s pair=%s",
                         uid, slot, price, hours, pair_line)
        await update.message.reply_text(msg, parse_mode="HTML", disable_web_page_preview=True)
        return

    # Step 3: final confirmation
    if step == "confirm":
        # User has reached the confirm step; clear the 4-minute info deadline
        try:
            if '_clear_booking_deadline' in globals():
                _clear_booking_deadline(context)
                if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                    logger.debug(
                        "[BOOK][FLOW] Cleared booking deadline at confirm step uid=%s slot=%s",
                        uid, slot
                    )
        except Exception as e:
            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                logger.debug("[BOOK][FLOW][DEADLINE-CLEAR-ERR] %s", e)

        if text.lower() not in {"yes", "y"}:
            # User cancelled at confirm step -> release hold
            try:
                if slot:
                    release_slot(int(slot), uid, force=True)
                    if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                        logger.debug("[HOLD][RELEASE] user-cancel confirm -> slot=%s uid=%s", slot, uid)
            except Exception:
                pass

            await update.message.reply_text("Booking cancelled.")
            context.user_data.pop("booking", None)
            return

        price = float((TRENDING_SLOTS.get(slot) or {}).get("price") or 0.0)
        is_admin = bool(uid in ADMIN_USER_IDS)

        # Admin & free slots: start immediately
        if is_admin or price == 0.0:
            # Use custom duration for admin-only slots (if provided), otherwise slot default
            try:
                custom_hours = float(data.get("duration_hours"))
            except Exception:
                custom_hours = None
            if custom_hours is not None and custom_hours > 0:
                duration_hours = custom_hours
            else:
                duration_hours = float((TRENDING_SLOTS.get(slot) or {}).get("duration") or 0.0)

            promo = {
                "token_name": data.get("token_name"),
                "token_address": data.get("token_address"),
                "pair_address": data.get("pair_address"),
                "booked_by": uid,
                "start_time": int(time.time()),
                "duration": float(duration_hours),
            }
            promo["end_time"] = int(promo["start_time"] + float(duration_hours) * 3600)
            promos = clear_expired_promotions() or {}
            promos[str(slot)] = promo
            save_promotions(promos)

            # Release hold now that booking is finalized
            try:
                release_slot(slot, uid, force=True)
                if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                    logger.debug("[HOLD][RELEASE] admin/free finalize -> slot=%s uid=%s", slot, uid)
            except Exception:
                pass

            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                logger.debug(
                    "[BOOK][START] immediate slot=%s promo=%s",
                    slot, {k: promo.get(k) for k in ("token_name", "token_address", "pair_address", "duration")}
                )
            try:
                await update_pinned_trending_message(context.bot, uid)
            except Exception as e:
                logger.warning("[BOOK][PIN][ERR] %s", e)
            try:
                asyncio.create_task(_broadcast_trending_announcement(slot, promo, context.bot))
            except Exception as e:
                logger.warning("[BOOK][BROADCAST][ERR] %s", e)
            context.user_data.pop("booking", None)
            await update.message.reply_text("✅ Your token is now trending!")
            return

        # Paid users: stage pending booking and auto-confirm on WS payment
        addr = str(globals().get("SOL_RECEIVE_ADDRESS") or "").strip()
        hours = float((TRENDING_SLOTS.get(slot) or {}).get("duration") or 0.0)

        # Ensure queue exists and stage pending booking for WS settlement
        try:
            globals().setdefault("PENDING_BOOKINGS", {})
        except Exception:
            pass

        import uuid
        payment_id = uuid.uuid4().hex[:12]
        try:
            PENDING_BOOKINGS[payment_id] = {
                "user_id": uid,
                "slot": slot,
                "amount_sol": float(price),
                "created": int(time.time()),
                "token_name": data.get("token_name"),
                "token_address": data.get("token_address"),
                "pair_address": data.get("pair_address"),
            }
            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                logger.debug("[BOOK][PENDING] pid=%s uid=%s slot=%s amount=%.6f", payment_id, uid, slot, float(price))
        except Exception as e:
            logger.warning("[BOOK][PENDING][ERR] %s", e)

        # Schedule a 5-minute timeout to auto-cancel if unpaid
        try:
            if hasattr(context, "job_queue"):
                context.job_queue.run_once(
                    _payment_timeout_job,
                    when=5 * 60,
                    name=f"paytimeout:{payment_id}",
                    data={"payment_id": payment_id, "user_id": uid}
                )
                if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                    logger.debug("[BOOK][TIMEOUT] scheduled 5m timeout for pid=%s", payment_id)
        except Exception as e:
            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                logger.debug("[BOOK][TIMEOUT][ERR] %s", e)

        # Build payment message with /cancel button and 5m note
        from telegram import InlineKeyboardButton, InlineKeyboardMarkup, ForceReply
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("❌ Cancel", callback_data=f"cancel:{payment_id}")]
        ])
        pay_msg = (
            "💳 <b>Payment required to confirm your booking</b>\n"
            f"• Slot: <b>#{slot}</b> — {hours:g} hour(s)\n"
            f"• Amount: <b>{float(price):.4f} SOL</b>\n"
            f"• Pay to: <code>{addr or '(set SOL_RECEIVE_ADDRESS)'}</code>\n\n"
            "✅ <i>Auto-confirmed as soon as on-chain payment is detected.</i>\n"
            "⏳ You have <b>5 minutes</b> to pay, or this booking will auto-cancel. "
            "Use /cancel if you change your mind."
        )
        await update.message.reply_text(
            pay_msg,
            parse_mode="HTML",
            disable_web_page_preview=True,
            reply_markup=kb
        )
        context.user_data.pop("booking", None)
        return

# --- Register slot-selection CallbackQuery handler exactly once ---
_SLOT_CB_REGISTERED = False

def register_slot_selection_handler_once(app: Application) -> None:
    global _SLOT_CB_REGISTERED
    try:
        if _SLOT_CB_REGISTERED:
            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                logger.debug("[BOOK][REG] slot CallbackQuery already registered; skipping.")
            return
        # Accept several callback_data formats: slot_12, slot:12, book:12, select-12
        pattern = r"^(?:book|slot|select|choose)[:_\-=]?\d{1,2}$|^slot_\d{1,2}$|.*slot(?:id)?[:= ]?\d{1,2}.*"
        app.add_handler(CallbackQueryHandler(on_slot_select, pattern=pattern), group=0)
        _SLOT_CB_REGISTERED = True
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[BOOK][REG] Added slot CallbackQueryHandler at group=0 pattern=%s", pattern)
    except Exception as _e:
        logger.warning("[BOOK][REG][ERR] %s", _e)

# Ensure the slot-selection handler is present once.
register_slot_selection_handler_once(application)

# Register the booking reply handler exactly once (no duplicates)

# --- Register booking reply handler (DM or reply in groups), exactly once ---
if not globals().get("_BOOKING_REPLY_H_ADDED", False):
    try:
        booking_reply_handler = MessageHandler(
            (filters.TEXT & (~filters.COMMAND)) & (filters.REPLY | filters.ChatType.PRIVATE),
            handle_booking_reply
        )
        application.add_handler(booking_reply_handler, group=0)
        globals()["_BOOKING_REPLY_H_ADDED"] = True
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[BOOK][REG] Added handle_booking_reply at group=0 (REPLY|PRIVATE)")
    except Exception as _e:
        logger.warning("[BOOK][REG][ERR] %s", _e)

# --- Admin bookings toggle handler (enable/disable/toggle) ---
async def _cb_bookings_toggle(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Handles the admin 'Enable/Disable bookings' button.
    Accepts: bookings:enable | bookings:disable | bookings:toggle
    """
    q = update.callback_query
    try:
        await q.answer()
    except Exception:
        pass

    uid = get_user_id_from_update(update)
    if not is_admin_user(uid):
        try:
            await context.bot.answer_callback_query(q.id, text="Admins only.", show_alert=False)
        except Exception:
            pass
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            try:
                logger.debug("[BOOKINGS][TOGGLE][DENY] uid=%s is not admin", uid)
            except Exception:
                pass
        return

    data = str(getattr(q, "data", "") or "")
    desired: bool | None = None
    if data.endswith(":enable"):
        desired = True
    elif data.endswith(":disable"):
        desired = False
    else:
        try:
            desired = not bool(is_bookings_enabled())
        except Exception:
            desired = True

    try:
        set_bookings_enabled(bool(desired))
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[BOOKINGS][TOGGLE] uid=%s -> %s", uid, "ENABLED" if desired else "DISABLED")
    except Exception as _e:
        logger.warning("[BOOKINGS][TOGGLE][ERR] %s", _e)

    # Toast
    try:
        await context.bot.answer_callback_query(
            q.id,
            text=("Bookings enabled ✅" if desired else "Bookings disabled ⛔"),
            show_alert=False
        )
    except Exception:
        pass

    # Rebuild the same message's keyboard
    try:
        async with promo_lock:
            promos = clear_expired_promotions() or {}
            kb = build_slot_inline_keyboard(promos, uid)
        if q and q.message:
            await q.message.edit_reply_markup(reply_markup=kb)
    except Exception as e:
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[BOOKINGS][TOGGLE][EDIT-ERR] %s", e)

# --- Register bookings-toggle (admin) exactly once ---
if not globals().get("_BOOKINGS_TOGGLE_ADDED"):
    try:
        application.add_handler(
            CallbackQueryHandler(_cb_bookings_toggle, pattern=r"^bookings:(?:enable|disable|toggle)$"), group=0
        )
        globals()["_BOOKINGS_TOGGLE_ADDED"] = True
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[BOOKINGS][REG] Added admin toggle CallbackQueryHandler at group=0")
    except Exception as _e:
        logger.warning("[BOOKINGS][REG][ERR] %s", _e)

# --- Register price-adjust handlers (admin) exactly once ---
if not globals().get("_PRICE_ADJUST_HANDLERS_ADDED"):
    try:
        application.add_handler(
            CallbackQueryHandler(_cb_price_adjust_menu, pattern=r"^ADMIN_PRICES_MENU$"),
            group=0,
        )
        application.add_handler(
            CallbackQueryHandler(_cb_price_adjust_apply, pattern=r"^ADMIN_PRICE:[+-]?\d+$"),
            group=0,
        )
        globals()["_PRICE_ADJUST_HANDLERS_ADDED"] = True
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[PRICES][REG] Added admin price adjust handlers at group=0")
    except Exception as _e:
        logger.warning("[PRICES][REG][ERR] %s", _e)

def _log_cost_mode_now():
    try:
        # Current operating modes (Enhanced REST fully removed)
        dexprio = bool(globals().get('USE_DEX_PRIORITY_ONLY'))
        subs_all = bool(globals().get('SUBSCRIBE_ALL_LOGS'))
        gate = bool(globals().get('USE_HELIUS_GATE'))
        req_ds = bool(globals().get('REQUIRE_DS_TRIGGER'))
        ws_qps = int(globals().get('_WS_PREFETCH_QPS') or 0)

        # High-level cost mode summary
        logger.info(
            "[COST] Mode: USE_DEX_PRIORITY_ONLY=%s, SUBSCRIBE_ALL_LOGS=%s, ENHANCED_REST=DISABLED, USE_HELIUS_GATE=%s, REQUIRE_DS_TRIGGER=%s, WS_QPS=%s",
            dexprio, subs_all, gate, req_ds, ws_qps
        )

        # Deep debug breadcrumbs (no Enhanced variables referenced)
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            try:
                logger.debug(
                    "[COST][TUNING] SPL-logs-only mode. WS_PREFETCH_QPS=%s COMMITMENT=%s LISTEN_PROGRAM_IDS(sample)=%s",
                    ws_qps,
                    str(globals().get('HELIUS_WS_COMMITMENT')),
                    (globals().get('LISTEN_PROGRAM_IDS') or [])[:3]
                )
            except Exception:
                pass
    except Exception as e:
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[COST][ERR] %s", e)

# Start WS once shortly after boot, then keep a small watchdog
try:
    application.job_queue.run_once(lambda ctx: asyncio.create_task(ensure_ws_listener_started(application)), when=1)
    application.job_queue.run_repeating(lambda ctx: asyncio.create_task(_ws_guard_tick(application)), interval=60, first=60)
    if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
        logger.debug("[BOOT][WS] Scheduled ensure_ws_listener_started(+watchdog)")
except Exception as _e:
    logger.warning("[BOOT][WS] Failed to schedule WS starter/watchdog: %s", _e)

TRENDING_SLOTS = {
    12: {"price": 0.65, "duration": 1.5},
    11: {"price": 1.3, "duration": 2.5},
    10: {"price": 1.95, "duration": 3.5},
    9: {"price": 2.6, "duration": 4.5},
    8: {"price": 3.25, "duration": 5.5},
    7: {"price": 3.9, "duration": 6.5},
    6: {"price": 4.55, "duration": 7.5},
    5: {"price": 5.2, "duration": 8.5},
    4: {"price": 5.85, "duration": 9.5},
    3: {"price": 6.5, "duration": 10.5},
    2: {"price": 7.15, "duration": 11.5},
    1: {"price": 7.8, "duration": 12.5},
    13: {"price": 0, "duration": 3},
    14: {"price": 0, "duration": 4},
    15: {"price": 0, "duration": 5},
    16: {"price": 0, "duration": 6},
    17: {"price": 0, "duration": 7},
}
ADMIN_ONLY_SLOTS = {13, 14, 15, 16, 17}
AUTO_ADMIN_SLOTS = [13, 14, 15]
PINNED_MESSAGE_IDS = {}
DEX_PRIORITY = [
    'Raydium', 'Pumpswap', 'Orca', 'Lifinity',
    'Meteora', 'OpenBook', 'Phoenix', 'GooseFX',
    'Crema', 'Saber', 'Saros', 'Cropper',
    'Step', 'Serum', 'Invariant', 'Dradex',
    'DexLab', 'Aldrin', 'Atrix'
]

# --- Minimal program ids when avoiding DEX_PROGRAM_IDS ---
# Subscribe only to the SPL Token Program to catch token transfers/mints/burns
# without enabling the full firehose or maintaining a big DEX list.
if 'LISTEN_PROGRAM_IDS' not in globals():
    LISTEN_PROGRAM_IDS = [
        "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"  # SPL Token Program
    ]

BOOKINGS_STATE_PATH = "bookings_enabled.json"

MAIN_EVENT_LOOP = None  # will be set at startup

_sol_price_usd = 0
_sol_price_usd_last = 0

def format_float(val, decimals=8):
    try:
        return f"{float(val):.{decimals}f}"
    except (ValueError, TypeError):
        return "0"

def format_int(val):
    try:
        return f"{int(round(float(val))):,}"
    except (ValueError, TypeError):
        return "0"

def _now() -> float:
    return time.time()

def _sol_from_lamports(lamports: int | float | str | None) -> float:
    try:
        return float(lamports) / 1_000_000_000.0
    except Exception:
        return 0.0

def _nearly_equal(a: float, b: float, eps: float = 1e-9) -> bool:
    try:
        return abs(float(a) - float(b)) <= float(eps)
    except Exception:
        return False

def _trim_zeros(s: str) -> str:
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return s

def fmt_compact(n, *, decimals: int = 2) -> str:
    try:
        x = float(n)
    except Exception:
        return "0"
    neg = x < 0
    x = abs(x)
    for k, suf in [(1e12,"T"),(1e9,"B"),(1e6,"M"),(1e3,"K")]:
        if x >= k:
            s = f"{x/k:.{decimals}f}{suf}"
            return ("-" if neg else "") + _trim_zeros(s)
    return ("-" if neg else "") + _trim_zeros(f"{x:.{decimals}f}")

def fmt_usd(n) -> str:
    try:
        x = float(n)
    except Exception:
        return "0.00"
    if x >= 1000:   return f"{x:,.0f}"
    if x >= 1:      return f"{x:,.2f}"
    return _trim_zeros(f"{x:.6f}")

def fmt_price_sol(n) -> str:
    try:
        x = float(n)
    except Exception:
        return "0"
    if x == 0:      return "0"
    if x >= 0.0001: return _trim_zeros(f"{x:.6f}")
    return _trim_zeros(f"{x:.10f}")

def build_trending_keyboard(pair_url=None, dextools_url=None, solscan_url=None):
    """One compact row: DEXscreener | DEXTools | Solscan"""
    row = []
    if pair_url:
        row.append(InlineKeyboardButton("DEXscreener", url=pair_url))
    if dextools_url:
        row.append(InlineKeyboardButton("DEXTools", url=dextools_url))
    if solscan_url:
        row.append(InlineKeyboardButton("Solscan", url=solscan_url))
    return InlineKeyboardMarkup([row]) if row else None

# --- Deep debug chat inspection helper ---
async def _debug_chat_inspect(bot, chat_id):
    if not ('DEEP_DEBUG' in globals() and DEEP_DEBUG):
        return
    try:
        c = await bot.get_chat(chat_id)
        title = getattr(c, "title", None) or getattr(c, "username", None) or "?"
        chat_type = getattr(c, "type", None)
        can_post = getattr(getattr(c, "permissions", None), "can_send_messages", None)
        logger.debug("[CHAT][INSPECT] chat_id=%s type=%s title=%s can_post=%s", chat_id, chat_type, title, can_post)
    except Exception as e:
        logger.debug("[CHAT][INSPECT][ERR] chat_id=%s -> %s", chat_id, e)

def build_add_your_token_keyboard():
    """
    One big button that opens the slot-selection keyboard.
    Uses callback_data to keep users inside Telegram (no links required).
    """
    try:
        return InlineKeyboardMarkup(
            [[InlineKeyboardButton("➕ Add Your Token To Trending", callback_data="bookings:open")]]
        )
    except Exception:
        return None


async def _broadcast_trending_announcement(slot: int, promo: dict, bot) -> None:
    """
    Post a fresh trending announcement to GROUP_CHAT_IDS for the given slot/promo.
    - Duplicate-safe per (slot, end_time).
    - Respects per-mint & global throttles.
    - Adds deep debug breadcrumbs throughout, including chat inspection.
    """
    try:
        if not isinstance(promo, dict):
            return

        token_name   = (promo.get("token_name") or "").strip()
        token_mint   = (promo.get("token_address") or "").strip()
        pair_address = (promo.get("pair_address") or "").strip()
        end_time     = int(promo.get("end_time") or 0)

        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            try:
                logger.debug("[TREND][BEGIN] slot=%s name=%s mint=%s pair=%s end=%s",
                             slot, token_name, token_mint, pair_address, end_time)
            except Exception:
                pass

        # Duplicate guard for same slot window
        try:
            last_end = LAST_TREND_BROADCAST.get(int(slot))
            if last_end == end_time and end_time > 0:
                if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                    logger.debug("[TREND][SKIP] Duplicate guard hit for slot=%s end_time=%s", slot, end_time)
                return
        except Exception:
            pass

        # Throttles
        now_ts = time.time()
        if not allow_global_alert(now_ts):
            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                logger.debug("[TREND][SKIP] Blocked by global throttle.")
            return
        # IMPORTANT: do not pass timestamps into allow_alert_for_mint; it maintains its own window
        if not allow_alert_for_mint(token_mint):
            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                logger.debug("[TREND][SKIP] Blocked by mint throttle for %s.", token_mint)
            return

        # Stats
        try:
            price, price_usd, liquidity, mcap, change_24h = get_token_stats_from_pair(fetch_dexscreener_pair(pair_address) or {})
        except Exception as e:
            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                logger.debug("[TREND][STATS][ERR] %s", e)
            price, price_usd, liquidity, mcap, change_24h = 0, 0, None, None, None

        try:
            sol_price = get_sol_price_usd()
        except Exception:
            sol_price = 0
        try:
            price_sol = price if price else ((price_usd / sol_price) if (sol_price and price_usd) else 0)
        except Exception:
            price_sol = 0

        dex_id = ""
        try:
            dex_id = (fetch_dexscreener_pair(pair_address) or {}).get("dexId", "") or ""
        except Exception:
            dex_id = ""
        dex_str = f" ({dex_id})" if dex_id else ""

        liquidity_line = f"💧 Liquidity: ${liquidity:,.0f}" if liquidity is not None else "💧 Liquidity: N/A"
        mcap_line      = f"📈 Market Cap: ${mcap:,.0f}"     if mcap is not None else "📈 Market Cap: N/A"
        change_line    = f"📊 24h Change: {change_24h:+.2f}%" if change_24h is not None else "📊 24h Change: N/A"

        # Links + keyboard
        dexscreener_url = get_dexscreener_link(pair_address)
        dextools_url    = get_dextools_link(pair_address)
        solscan_url     = get_solscan_link(token_mint)
        kb = build_trending_keyboard(dexscreener_url, dextools_url, solscan_url)

        msg = (
            f"<b>{token_name or 'Token'}</b>     <b>💵 BUY!</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"Trending <b>#{slot}</b>{dex_str}\n"
            f"💰 Price: <b>{price_sol:.11f} SOL</b> (${(price_usd or 0):.8f})\n"
            f"{liquidity_line}\n"
            f"{mcap_line}\n"
            f"{change_line}\n"
            f"━━━━━━━━━━━━━━━━━━"
        )

        # Pre-flight: inspect chats
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            try:
                targets = GROUP_CHAT_IDS if isinstance(GROUP_CHAT_IDS, (list, tuple, set)) else [GROUP_CHAT_IDS]
                logger.debug("[TREND][TARGETS] %s (count=%s) msg_len=%s", list(targets), len(list(targets)), len(msg))
                for _cid in targets:
                    await _debug_chat_inspect(bot, _cid)
            except Exception:
                pass

        # Send to all targets (with -100 expansion)
        posted_any = False
        targets = GROUP_CHAT_IDS if isinstance(GROUP_CHAT_IDS, (list, tuple, set)) else [GROUP_CHAT_IDS]
        for original in targets:
            sent_for_original = False
            for candidate in _expand_chat_targets([original]):
                if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                    try:
                        await _debug_chat_inspect(bot, candidate)
                        logger.debug("[TREND][TRY] slot=%s original=%s candidate=%s", slot, original, candidate)
                    except Exception:
                        pass
                try:
                    await bot.send_message(
                        chat_id=candidate,
                        text=msg,
                        reply_markup=kb,
                        parse_mode="HTML",
                        disable_web_page_preview=True
                    )
                    posted_any = True
                    sent_for_original = True
                    if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                        logger.debug("[TREND][POSTED] slot=%s original=%s used=%s", slot, original, candidate)
                    break
                except Exception as e:
                    logger.warning("[TREND][POST-ERR] original=%s candidate=%s error=%s", original, candidate, e)
            if not sent_for_original and ('DEEP_DEBUG' in globals() and DEEP_DEBUG):
                logger.debug("[TREND][MISS] Could not post for original target=%s (tried expanded forms).", original)

        # Update pin
        try:
            await update_pinned_trending_message(bot)
        except Exception as e:
            logger.warning("Failed to update pinned trending message: %s", e)

        if posted_any:
            try:
                LAST_TREND_BROADCAST[int(slot)] = end_time or int(time.time())
            except Exception:
                pass
            logger.info("[TREND] Posted trending announcement for slot %s.", slot)
        else:
            logger.info("[TREND] No trending announcement was posted (all sends failed).")

        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            try:
                logger.debug("[TREND][END] slot=%s posted_any=%s guard_state=%s",
                             slot, posted_any, dict(LAST_TREND_BROADCAST))
            except Exception:
                pass

    except Exception as e:
        logger.warning("[TREND][BROADCAST] Unexpected error: %s", e)

def update_sol_price_loop():
    global _sol_price_usd
    while True:
        try:
            resp = requests.get(
                "https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd", timeout=5)
            _sol_price_usd = float(resp.json()['solana']['usd'])
        except Exception as e:
            print(f"[SOL PRICE ERROR] {e}")
        time.sleep(30)

# === 2. SPL Token Price + Symbol Cache ===
token_price_cache = {}  # {mint: (price_usd, timestamp)}
token_symbol_cache = {}  # {mint: (symbol, timestamp)}

def get_token_price_usd(mint):
    """Fetch USD price for SPL token or SOL, robust against Dexscreener schema."""
    if mint == "So11111111111111111111111111111111111111112":
        return _sol_price_usd
    now = time.time()
    cached = token_price_cache.get(mint)
    if cached and now - cached[1] < 20:
        return cached[0]

    def _safe_float(v, default=None):
        try:
            if isinstance(v, (int, float, str)):
                return float(v)
            # Dexscreener sometimes nests {"usd": ...}
            if isinstance(v, dict):
                # common keys we may want to read
                for k in ("usd", "value", "amount"):
                    if k in v and isinstance(v[k], (int, float, str)):
                        return float(v[k])
        except Exception:
            pass
        return default

    try:
        url = f"https://api.dexscreener.com/latest/dex/tokens/{mint}"
        resp = requests.get(url, timeout=7)
        data = resp.json()
        pairs = data.get("pairs") or []
        if DEEP_DEBUG:
            try:
                logger.debug("[PRICE][TOKENS] mint=%s pairs=%d", mint, len(pairs))
            except Exception:
                pass

        if not pairs:
            return None

        # Choose the pair with the highest visible USD liquidity
        def _pair_liq_usd(p: dict) -> float:
            liq = p.get("liquidity")
            if isinstance(liq, dict):
                return _safe_float(liq.get("usd"), 0.0) or 0.0
            return _safe_float(liq, 0.0) or 0.0

        best = max((p for p in pairs if isinstance(p, dict)), key=_pair_liq_usd, default=None)
        if not isinstance(best, dict):
            return None

        price = _safe_float(best.get("priceUsd"), None)
        if price is not None:
            token_price_cache[mint] = (price, now)
            if DEEP_DEBUG:
                try:
                    logger.debug("[PRICE][OK] mint=%s priceUsd=%s pair=%s liq_usd=%s",
                                 mint, price, best.get("pairAddress"), _pair_liq_usd(best))
                except Exception:
                    pass
        else:
            if DEEP_DEBUG:
                try:
                    logger.debug("[PRICE][MISS] mint=%s raw_price=%s", mint, best.get("priceUsd"))
                except Exception:
                    pass
        return price
    except Exception as e:
        if DEEP_DEBUG:
            logger.debug("[TOKEN PRICE ERROR] %s", e)
        else:
            print(f"[TOKEN PRICE ERROR] {e}")
        return None

def get_token_symbol(mint):
    """Fetch symbol for SPL token or SOL, robust against Dexscreener schema."""
    if mint == "So11111111111111111111111111111111111111112":
        return "SOL"
    now = time.time()
    cached = token_symbol_cache.get(mint)
    if cached and now - cached[1] < 86400:
        return cached[0]

    def _safe_float(v, default=0.0):
        try:
            if isinstance(v, (int, float, str)):
                return float(v)
            if isinstance(v, dict):
                if "usd" in v and isinstance(v["usd"], (int, float, str)):
                    return float(v["usd"])
        except Exception:
            pass
        return default

    try:
        url = f"https://api.dexscreener.com/latest/dex/tokens/{mint}"
        resp = requests.get(url, timeout=7)
        data = resp.json()
        pairs = data.get("pairs") or []
        if not pairs:
            return "TOKEN"

        def _pair_liq_usd(p: dict) -> float:
            liq = p.get("liquidity")
            if isinstance(liq, dict):
                return _safe_float(liq.get("usd"), 0.0)
            return _safe_float(liq, 0.0)

        best = max((p for p in pairs if isinstance(p, dict)), key=_pair_liq_usd, default=None)
        symbol = None
        if isinstance(best, dict):
            bt = best.get("baseToken") or {}
            symbol = bt.get("symbol") or best.get("symbol") or "TOKEN"

        if not symbol:
            symbol = "TOKEN"

        token_symbol_cache[mint] = (symbol, now)
        if DEEP_DEBUG:
            try:
                logger.debug("[SYMBOL][OK] mint=%s symbol=%s pair=%s liq_usd=%s",
                             mint, symbol, (best or {}).get("pairAddress"), _pair_liq_usd(best or {}))
            except Exception:
                pass
        return symbol
    except Exception as e:
        if DEEP_DEBUG:
            logger.debug("[SYMBOL ERROR] %s", e)
        else:
            print(f"[SYMBOL ERROR] {e}")
        return "TOKEN"

def build_alert_message(mint, amount):
    """Return 'X TOKEN (≈Y SOL, $Z)' where X is in base-token units for `mint`."""
    symbol = get_token_symbol(mint) or "TOKEN"
    try:
        amount_f = float(amount or 0)
    except Exception:
        amount_f = 0.0

    price_usd = get_token_price_usd(mint) or 0.0
    sol_usd   = _sol_price_usd or get_sol_price_usd()
    price_sol = (price_usd / sol_usd) if (price_usd and sol_usd) else 0.0

    usd_val = amount_f * price_usd if price_usd else 0.0
    sol_val = amount_f * price_sol if price_sol else 0.0

    amt_str = fmt_compact(amount_f, decimals=3)
    sol_str = fmt_price_sol(sol_val)
    usd_str = fmt_usd(usd_val)

    # Deep debug snapshot
    if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
        try:
            logger.debug(
                "[ALERT][BUILD] mint=%s amount=%s -> sol_val=%s usd_val=%s symbol=%s",
                mint, amount_f, sol_val, usd_val, symbol
            )
        except Exception:
            pass

    return f"{amt_str} {symbol} (≈{sol_str} SOL, ${usd_str})"

# --- Bookings master toggle (single source of truth) ---
BOOKINGS_ENABLED = globals().get("BOOKINGS_ENABLED", True)

def is_bookings_enabled() -> bool:
    try:
        return bool(globals().get("BOOKINGS_ENABLED", True))
    except Exception:
        return True

def set_bookings_enabled(v: bool) -> None:
    try:
        globals()["BOOKINGS_ENABLED"] = bool(v)
    except Exception:
        pass

def find_slot_for_mint(mint):
    promos = load_promotions()
    for slot, promo in promos.items():
        if promo['token_address'].lower() == mint.lower():
            return int(slot)
    return None

def is_trending_token(token_mint: str) -> bool:
    """
    Strict check: only mints that are **currently booked** in active_promotions.json.
    """
    try:
        if not token_mint:
            return False
        tgt = str(token_mint).strip().lower()
        promos = load_promotions()
        if isinstance(promos, dict):
            for _slot, p in promos.items():
                try:
                    if str((p or {}).get("token_address", "")).strip().lower() == tgt:
                        return True
                except Exception:
                    continue
        elif isinstance(promos, list):
            for p in promos:
                try:
                    if str((p or {}).get("token_address", "")).strip().lower() == tgt:
                        return True
                except Exception:
                    continue
        return False
    except Exception as e:
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            try:
                logger.debug("[TREND-CHECK][ERR] %s input=%s", e, token_mint)
            except Exception:
                pass
        return False

def get_booked_promo_by_mint(mint_lc: str) -> tuple[int | None, dict]:
    """
    Return (slot, promo_dict) for a booked mint (case-insensitive). (None, {}) if not found.
    """
    try:
        promos = load_promotions()
        if isinstance(promos, dict):
            for slot, p in promos.items():
                try:
                    if str((p or {}).get("token_address", "")).strip().lower() == (mint_lc or "").lower():
                        return int(slot), dict(p or {})
                except Exception:
                    continue
        elif isinstance(promos, list):
            for p in promos:
                try:
                    if str((p or {}).get("token_address", "")).strip().lower() == (mint_lc or "").lower():
                        return int(p.get("slot")), dict(p or {})
                except Exception:
                    continue
    except Exception as _e:
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[PROMO][LOOKUP][ERR] %s", _e)
    return None, {}

def get_trending_token_mints():
    """
    Return all active promo token mints, normalized to lowercase strings.
    Supports both dict (slot->promo) and list payload shapes.
    """
    t0 = time.time()
    path = os.path.abspath('active_promotions.json')
    try:
        with open(path) as f:
            promos = json.load(f) or {}
            entries = promos.values() if isinstance(promos, dict) else promos
            mints = []
            for entry in entries:
                try:
                    mint = (entry or {}).get('token_address')
                    if mint:
                        mints.append(str(mint).strip().lower())
                except Exception:
                    continue
            # de-dupe
            mints = list(dict.fromkeys(mints))
            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                logger.debug("[TREND][LOAD] path=%s shape=%s count=%d sample=%s elapsed=%.3fs",
                             path,
                             "dict" if isinstance(promos, dict) else "list",
                             len(mints),
                             mints[:5],
                             time.time() - t0)
            return mints
    except Exception as e:
        logging.error(f"Failed to load trending token mints from {path}: {e}")
        return []

async def handle_payment_received(app, from_address: str, sol_amount: float, signature: str):
    """
    Called when a SOL payment to SOL_RECEIVE_ADDRESS is detected (via WebSocket).
    Notifies admins and (optionally) the payer if user id can be matched.
    """
    try:
        # Compute USD value for admin visibility
        try:
            sol_usd_price = float(get_sol_price_usd() or 0)
        except Exception:
            sol_usd_price = 0.0
        try:
            usd_amount = (float(sol_amount) * sol_usd_price) if (sol_amount is not None and sol_usd_price) else 0.0
        except Exception:
            usd_amount = 0.0
        usd_str = f"{usd_amount:,.2f}" if usd_amount else "0.00"

        # Deep debug of inputs & computed values
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            try:
                logger.debug(
                    "[PAYMENT][BEGIN] from=%s sol=%.6f sol_usd=%.4f usd_est=%s sig=%s admins=%d",
                    from_address, (sol_amount or 0.0), sol_usd_price, usd_str, signature, len(ADMIN_USER_IDS)
                )
            except Exception:
                pass

        msg = (
            f"✅ <b>Payment received</b>\n"
            f"From: <code>{from_address or 'unknown'}</code>\n"
            f"Amount: <b>{(sol_amount or 0.0):.4f} SOL</b> (~${usd_str})\n"
            f"Tx: <a href='https://solscan.io/tx/{signature}'>view on Solscan</a>"
        )

        notified_ids = set()

        # Send to all admins
        for admin_id in ADMIN_USER_IDS:
            try:
                await app.bot.send_message(
                    chat_id=int(admin_id),
                    text=msg,
                    parse_mode="HTML",
                    disable_web_page_preview=True
                )
                notified_ids.add(int(admin_id))
                if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                    logger.debug("[PAYMENT][ADMIN] Notified admin_id=%s", admin_id)
            except Exception as err:
                logger.error("[PAYMENT][ADMIN] Failed to notify admin_id=%s: %s", admin_id, err)

        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            try:
                logger.debug("[PAYMENT][ADMIN] Completed admin notifications for sig=%s; notified=%s", signature, sorted(list(notified_ids)))
            except Exception:
                pass

        # Optional: notify payer if you can map wallet -> user_id (placeholder)
        payer_user_id = None  # Replace with actual lookup if available
        if payer_user_id and int(payer_user_id) not in notified_ids:
            try:
                await app.bot.send_message(
                    chat_id=int(payer_user_id),
                    text=msg,
                    parse_mode="HTML",
                    disable_web_page_preview=True
                )
                notified_ids.add(int(payer_user_id))
                if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                    logger.debug("[PAYMENT][PAYER] Notified payer user_id=%s", payer_user_id)
            except Exception as err:
                logger.error("[PAYMENT][PAYER] Failed to notify payer user_id=%s: %s", payer_user_id, err)

        logger.info("[PAYMENT RECEIVED] %.6f SOL from %s (sig %s)", (sol_amount or 0.0), from_address, signature)
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[PAYMENT][END] Notified IDs: %s", sorted(list(notified_ids)))

    except Exception as e:
        logger.error("[PAYMENT] handle_payment_received error: %s", e)


def get_token_symbol_from_pair(pair):
    # Try "baseToken" first (DEXScreener), fallback to "token0" for other APIs, fallback to "?" if missing
    if not pair:
        return "?"
    if "baseToken" in pair and "symbol" in pair["baseToken"]:
        return pair["baseToken"]["symbol"]
    if "token0" in pair and "symbol" in pair["token0"]:
        return pair["token0"]["symbol"]
    if "symbol" in pair:
        return pair["symbol"]
    return "?"

def fetch_dexscreener_best_pair_for_mint(mint: str) -> dict | None:
    """
    Return the best (highest-liquidity) DEXscreener pair for a Solana mint.
    Robust against schema variations; filters to Solana chain when available.
    """
    try:
        mint = (mint or "").strip()
        if not mint:
            return None

        url = f"https://api.dexscreener.com/latest/dex/tokens/{mint}"
        resp = requests.get(url, timeout=8)
        data = resp.json() if getattr(resp, "ok", False) else {}
        pairs = data.get("pairs") or []
        if not isinstance(pairs, list) or not pairs:
            return None

        def _liq_usd(p: dict) -> float:
            liq = p.get("liquidity")
            # allow number or {"usd": ...}
            if isinstance(liq, dict):
                v = liq.get("usd")
            else:
                v = liq
            try:
                return float(v)
            except Exception:
                return 0.0

        # Prefer Solana chain if indicated
        sol_pairs = [p for p in pairs if str(p.get("chainId", "")).lower() in ("solana", "sol")]
        if not sol_pairs:
            sol_pairs = pairs

        best = max((p for p in sol_pairs if isinstance(p, dict)), key=_liq_usd, default=None)
        return best if isinstance(best, dict) else None
    except Exception as e:
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            try:
                logger.debug("[DS][BESTPAIR][ERR] %s", e)
            except Exception:
                pass
        return None

def _get_price_usd_for_mint(mint: str) -> float | None:
    """
    Return current priceUsd (float) for given mint using the existing
    `fetch_dexscreener_best_pair_for_mint` helper; None on failure.
    """
    try:
        fn = globals().get("fetch_dexscreener_best_pair_for_mint")
        if not callable(fn):
            return None
        p = fn(str(mint).strip())
        if isinstance(p, dict):
            v = p.get("priceUsd") or (p.get("price") if isinstance(p.get("price"), (int, float, str)) else None)
            try:
                return float(v)
            except Exception:
                return None
    except Exception:
        return None
    return None

def get_dexscreener_link(pair_address):
    return f"https://dexscreener.com/solana/{pair_address}"

def get_dextools_link(pair_address):
    return f"https://www.dextools.io/app/en/solana/pair-explorer/{pair_address}"

def get_solscan_link(token_address):
    return f"https://solscan.io/token/{token_address}"

def get_solscan_tx_link(signature):
    return f"https://solscan.io/tx/{signature}"

def get_solscan_account_link(address):
    return f"https://solscan.io/account/{address}"

def _is_real_signature(sig: str | None) -> bool:
    """Return True only for valid-looking Solana signatures (base58, ~64 chars).
    Also reject our synthetic DS fallback signatures like "ds:...".
    """
    if not sig or not isinstance(sig, str):
        return False
    if sig.startswith("ds:"):
        return False
    # Rough sanity check - real sigs are base58 and usually 80-90 chars
    return 40 <= len(sig) <= 120 and all(
        c in "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz" for c in sig
    )


def abbreviate_addr(addr):
    if not addr or len(addr) < 8:
        return addr
    return addr[:4] + "..." + addr[-4:]

def load_promotions():
    if not os.path.isfile(PROMO_JSON_PATH):
        return {}
    try:
        with open(PROMO_JSON_PATH, "r") as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Failed to load promotions: {e}")
        return {}

# === Promotions-derived watchlists (built at startup) ===
# Build these once at startup from the promotions file so Pylance and runtime both see them.
PROMOS = load_promotions()  # {slot: {"mint": str, "pair": str, ...}, ...}

# Keep lowercase for case-insensitive comparisons later
active_promos_mint_list = [
    (p.get("mint") or "").lower()
    for p in PROMOS.values()
    if p.get("mint")
]
active_promos_pair_list = [
    (p.get("pair") or "").lower()
    for p in PROMOS.values()
    if p.get("pair")
]

# Sets are faster for membership tests
WATCHED_MINTS = {m for m in active_promos_mint_list}
WATCHED_PAIRS = {p for p in active_promos_pair_list}

#
# Build these from the current promotions and keep them refreshed
def _active_promos_lists():
    """
    Return (mints, pairs) extracted from promotions.json, all lowercased & stripped.
    Supports both dict (slot->promo) and list payload shapes.
    """
    promos = load_promotions() or {}
    entries = promos.values() if isinstance(promos, dict) else promos
    mints, pairs = [], []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        m = (entry.get("token_address") or "").strip().lower()
        p = (entry.get("pair_address") or "").strip().lower()
        if m:
            mints.append(m)
        if p:
            pairs.append(p)
    # de-dupe
    mints = list(dict.fromkeys(mints))
    pairs = list(dict.fromkeys(pairs))
    return mints, pairs

def refresh_watched_sets():
    """Refresh WATCHED_MINTS/WATCHED_PAIRS from promotions.json."""
    global WATCHED_MINTS, WATCHED_PAIRS
    mints, pairs = _active_promos_lists()
    WATCHED_MINTS = set(mints)
    WATCHED_PAIRS = set(pairs)

# Initialize watched sets at startup
WATCHED_MINTS, WATCHED_PAIRS = set(), set()
refresh_watched_sets()

def collect_candidates_from_tx(tx: dict):
    mints, accounts = set(), set()
    meta = (tx.get("meta") or {})
    for b in (meta.get("preTokenBalances") or []):
        mint = (b.get("mint") or "").lower().strip()
        if mint: mints.add(mint)
    for b in (meta.get("postTokenBalances") or []):
        mint = (b.get("mint") or "").lower().strip()
        if mint: mints.add(mint)
    for t in (tx.get("tokenTransfers") or []):
        mint = (t.get("mint") or "").lower().strip()
        if mint: mints.add(mint)
    ev = (tx.get("events") or {})
    swaps = ev.get("swap") or ev.get("swaps") or []
    if isinstance(swaps, dict): swaps = [swaps]
    for s in swaps:
        for k in ("tokenInMint","tokenOutMint","mintIn","mintOut"):
            v = (s.get(k) or "").lower().strip()
            if v: mints.add(v)
    msg = ((tx.get("transaction") or {}).get("message") or {})
    for a in (msg.get("accountKeys") or []):
        if isinstance(a, dict): a = a.get("pubkey") or a.get("pubKey") or ""
        a = (a or "").lower().strip()
        if a: accounts.add(a)
    return mints, accounts

def match_promo_from_tx(tx: dict):
    mints, accounts = collect_candidates_from_tx(tx)
    mint_hits  = WATCHED_MINTS.intersection(mints)
    pair_hits  = WATCHED_PAIRS.intersection(accounts)
    matched = bool(mint_hits or pair_hits)
    return matched, mint_hits, pair_hits

def save_promotions(promos):
    try:
        # Save to disk
        with open(PROMO_JSON_PATH, "w") as f:
            json.dump(promos, f, indent=2)
        with open("payload.json", "w") as pf:
            json.dump(promos, pf, indent=2)
        logger.info("Promotions saved (and payload.json updated).")

        try:
            refresh_watched_sets()
        except Exception as _e:
            logger.warning("Failed to refresh WATCHED_* after save_promotions: %s", _e)

        # Deep snapshot for debugging
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            try:
                logger.info("[PROMOS] Current promos snapshot: %s", json.dumps(promos, indent=2))
            except Exception:
                pass
            # Also log broadcast targets (helps diagnose BadRequest: Chat not found)
            try:
                targets = GROUP_CHAT_IDS if isinstance(GROUP_CHAT_IDS, (list, tuple, set)) else [GROUP_CHAT_IDS]
                logger.debug("[PROMOS] Broadcast targets -> %s (count=%s)", list(targets), len(list(targets)))
            except Exception:
                pass
            # Additionally, verify group ids are integers & log normalization
            try:
                _targets = GROUP_CHAT_IDS if isinstance(GROUP_CHAT_IDS, (list, tuple, set)) else [GROUP_CHAT_IDS]
                _norm = []
                for _t in _targets:
                    try:
                        _norm.append(int(str(_t)))
                    except Exception:
                        _norm.append(str(_t))
                logger.debug("[PROMOS] Normalized GROUP_CHAT_IDS -> %s", _norm)
            except Exception:
                pass
            # Deep-debug: expanded chat ids to be attempted
            try:
                exp = list(_expand_chat_targets(GROUP_CHAT_IDS))
                logger.debug("[PROMOS] Expanded chat targets preview -> %s", exp)
            except Exception:
                pass
    except Exception as e:
        logger.error(f"Failed to save promotions: {e}")

def clean_expired_booking():
    """
    Hold cleanup aligned to the no-timer model:
      - Drop any holds for slots that became actually booked.
      - Keep holds that correspond to an active pending payment.
    """
    try:
        promos = load_promotions() or {}
        booked_slots = {int(s) for s in promos.keys() if str(s).isdigit()}

        pending_slots = set()
        try:
            for pb in (PENDING_BOOKINGS or {}).values():
                try:
                    pending_slots.add(int(pb.get("slot") or 0))
                except Exception:
                    continue
        except Exception:
            pending_slots = set()

        for s, _uid in list(SLOT_HELD_BY.items()):
            try:
                si = int(s)
            except Exception:
                si = None
            if si is None:
                continue
            # If slot is already booked OR there is no pending payment for it, remove the hold.
            if (si in booked_slots) or (si not in pending_slots):
                SLOT_HELD_BY.pop(si, None)
                if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                    logger.debug("[HOLD][CLEAN] removed hold slot=%s (booked=%s pending=%s)",
                                 si, si in booked_slots, si in pending_slots)
    except Exception as _e:
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[HOLD][CLEAN][ERR] %s", _e)

#
# --- /id helper: reply with effective chat id (admin or anyone) --------------
from telegram.ext import CallbackContext, CommandHandler
async def chat_id_cmd(update: Update, context: CallbackContext):
    try:
        cid = getattr(update.effective_chat, "id", None)
        title = getattr(update.effective_chat, "title", None) or getattr(update.effective_chat, "username", None)
        await update.message.reply_text(f"chat_id = {cid}\nname = {title or '(no title)'}")
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[/id] chat_id=%s name=%s", cid, title)
    except Exception as e:
        logger.warning("[/id] error: %s", e)

application.add_handler(CommandHandler("id", chat_id_cmd))
application.add_handler(CallbackQueryHandler(_cb_bookings_toggle, pattern=r"^bookings:toggle$"))
application.add_handler(CallbackQueryHandler(_cb_open_bookings, pattern=r"^bookings:open$"))

def clear_expired_promotions():
    """Remove expired promotions and save changes."""
    promos = load_promotions() or {}
    expired = []
    now = time.time()

    for slot, promo in list(promos.items()):
        start_time = promo.get("start_time")
        duration_h = promo.get("duration")
        end_time = promo.get("end_time")

        # Backfill end_time if it's missing but we have start_time + duration
        if not end_time and isinstance(start_time, (int, float)) and isinstance(duration_h, (int, float)):
            end_time = int(start_time + duration_h * 3600)
            promo["end_time"] = end_time
            promos[slot] = promo  # save backfill to memory

        # If the promo has ended, mark it for removal
        if end_time and now > end_time:
            expired.append(slot)
            promos.pop(slot, None)

    if expired:
        logger.info(f"Expired promotions removed: {expired}")
        save_promotions(promos)
    try:
        refresh_watched_sets()
    except Exception as _e:
        logger.debug("WATCHED_* refresh skipped in clear_expired_promotions: %s", _e)
    return promos

def cleanup_expired_promotions_now():
    """Immediately clear expired promotions from promotions.json."""
    promos = load_promotions() or {}
    expired = []
    now = time.time()

    for slot, promo in list(promos.items()):
        start_time = promo.get("start_time")
        duration_h = promo.get("duration")
        end_time = promo.get("end_time")

        # Backfill end_time if missing
        if not end_time and isinstance(start_time, (int, float)) and isinstance(duration_h, (int, float)):
            end_time = int(start_time + duration_h * 3600)
            promo["end_time"] = end_time
            promos[slot] = promo

        # Mark as expired if end time passed
        if end_time and now > end_time:
            expired.append(slot)
            promos.pop(slot, None)

    if expired:
        save_promotions(promos)
        logger.info(f"[CLEANUP] Expired promos removed immediately: {expired}")
    else:
        logger.info("[CLEANUP] No expired promos found.")

async def clear_expired_promotions_job(context: ContextTypes.DEFAULT_TYPE):
    async with promo_lock:
        clear_expired_promotions()
        # keep the trending pin current
        await update_pinned_trending_message(context.bot)

def get_sol_price_usd(force_update=False):
    global _sol_price_usd, _sol_price_usd_last
    now = time.time()
    if not force_update and (now - _sol_price_usd_last < 20):
        return _sol_price_usd
    try:
        resp = requests.get("https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd", timeout=5)
        _sol_price_usd = float(resp.json()['solana']['usd'])
        _sol_price_usd_last = now
        return _sol_price_usd
    except Exception as e:
        print(f"[SOL PRICE ERROR] {e}")
        return _sol_price_usd or 0

get_sol_usd_price = get_sol_price_usd  # back-compat alias for older call sites

def _liquidity_val(p):
    liq = p.get("liquidity")
    if isinstance(liq, dict):
        try:
            return float(liq.get("usd") or 0)
        except Exception:
            return 0.0
    try:
        return float(liq or 0)
    except Exception:
        return 0.0


def get_best_pair(token_address):
    try:
        url = f"https://api.dexscreener.com/latest/dex/tokens/{token_address}"
        resp = requests.get(url, timeout=7)
        data = resp.json()
        pairs = data.get('pairs', [])
        if not pairs:
            return None, None

        # Whitelist and ranking
        wl = {d.lower() for d in DEX_PRIORITY}
        dex_rank = {d.lower(): i for i, d in enumerate(DEX_PRIORITY)}  # lower is better

        # Minimum visible liquidity to consider, in USD
        MIN_LIQ_USD_PRIMARY = 5000.0
        MIN_LIQ_USD_FALLBACK = 100.0  # don't ever pick dust pools

        # Filter valid candidates: correct base token, on our whitelisted DEXes, with enough liquidity
        def _is_valid_pair(p: dict) -> bool:
            if not isinstance(p, dict):
                return False
            base_ok = str(p.get("baseToken", {}).get("address", "")).lower() == str(token_address).lower()
            dex_ok = (p.get('dexId', '') or '').strip().lower() in wl
            liq = _liquidity_val(p)
            return base_ok and dex_ok and (liq >= MIN_LIQ_USD_PRIMARY)

        candidates = [p for p in pairs if _is_valid_pair(p)]
        if not candidates:
            # If nothing meets the primary threshold, fall back to whitelisted pairs
            tmp = []
            for p in pairs:
                if not isinstance(p, dict):
                    continue
                base_ok = str(p.get("baseToken", {}).get("address", "")).lower() == str(token_address).lower()
                dex_ok = (p.get('dexId', '') or '').strip().lower() in wl
                if not (base_ok and dex_ok):
                    continue
                liq = _liquidity_val(p)
                labels = [str(l).lower() for l in (p.get('labels') or [])]
                is_clmm = 'clmm' in labels
                is_wp = 'wp' in labels
                # hard floor: never consider pools under $100 liq
                if liq < MIN_LIQ_USD_FALLBACK:
                    continue
                # extra rule: if CLMM/Whirlpool and liquidity is very small, skip
                if (is_clmm or is_wp) and liq < 10000:
                    continue
                tmp.append(p)
            candidates = tmp
            if not candidates:
                logger.warning(f"No eligible DEX pair for mint {token_address}. Found pairs: {[p.get('pairAddress') for p in pairs if isinstance(p, dict)]}")
                return None, None

        SOL_MINT = "so11111111111111111111111111111111111111112"

        def score(p: dict):
            # Prefer SOL-quoted pools
            quote = (p.get('quoteToken') or {}).get('address', '')
            sol_quote = str(quote).lower() == SOL_MINT

            # Penalize CLMM and Whirlpool (wp) labels because visible liquidity buckets are tiny
            labels = [str(l).lower() for l in (p.get('labels') or [])]
            is_clmm = 'clmm' in labels
            is_wp = 'wp' in labels

            liq = _liquidity_val(p)
            dex = (p.get('dexId') or '').strip().lower()
            rank = dex_rank.get(dex, 999)

            # Higher tuple is better; prioritize DEX, avoid clmm/wp, prefer SOL, then liquidity
            return (
                -rank,
                0 if (is_clmm or is_wp) else 1,
                1 if sol_quote else 0,
                liq
            )

        best = max(candidates, key=score)
        logger.info(f"Best DEX pair for {token_address}: {best.get('pairAddress')}")
        return best.get('pairAddress'), best

    except Exception as e:
        logger.warning(f"Dexscreener pair lookup failed: {e}")
        return None, None

async def dexscreener_poll_once(context: ContextTypes.DEFAULT_TYPE):
    """Poll Dexscreener for buy deltas; arm hydration via WS-only flow.
    Notes:
      - NEVER posts an alert by itself; only "arms" a mint when m5 buys increase.
      - Hydration occurs when a real swap is seen on the Helius WebSocket stream.
    """
    try:
        promos = load_promotions()
        if not promos:
            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                logger.debug("[DS][SKIP] No promos loaded; skipping poll.")
            return

        # Lowercased map from mint -> slot for quick lookup
        mint_to_slot: dict[str, int] = {}
        for slot, promo in (promos or {}).items():
            m = (promo.get("token_address") or "").lower()
            if m:
                mint_to_slot[m] = int(slot)

        # Iterate promos and compute deltas
        for slot, promo in promos.items():
            try:
                mint = (promo.get("token_address") or "").lower()
                pair_addr = promo.get("pair_address")
                if not mint or not pair_addr:
                    if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                        logger.debug("[DS][SKIP] Missing mint/pair for slot=%s promo=%s", slot, bool(promo))
                    continue

                # Dexscreener stats
                stats = fetch_dexscreener_pair(pair_addr)
                if not isinstance(stats, dict):
                    logger.warning("[DS] Invalid Dexscreener stats for %s / %s: %s", mint, pair_addr, stats)
                    continue

                buys_now = int(((stats.get("txns") or {}).get("m5") or {}).get("buys", 0))
                prev = DS_LAST.get(mint)

                if not prev:
                    DS_LAST[mint] = {"buys_m5": buys_now, "pair": pair_addr}
                    if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                        logger.debug("[DS][INIT] mint=%s slot=%s m5_buys=%s", mint, slot, buys_now)
                    continue

                prev_buys = int(prev.get("buys_m5", 0))
                delta = buys_now - prev_buys
                DS_LAST[mint] = {"buys_m5": buys_now, "pair": pair_addr}

                if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                    try:
                        logger.debug("[DS][DELTA] mint=%s slot=%s prev=%s now=%s delta=%s", mint, slot, prev_buys, buys_now, delta)
                    except Exception:
                        pass

                if delta >= DEXSCREENER_MIN_BUYS_DELTA:
                    logger.info("[DS] Triggered for %s (m5 %s->%s, delta=%s).", mint, prev_buys, buys_now, delta)

                    # NEVER post a DS-based alert. We only arm and wait for a real swap on WS.
                    DS_TRIGGERED[mint] = time.time()
                    logger.info("[DS] Armed by Dexscreener; waiting for real Helius SWAP on WebSocket to hydrate alert…")

            except Exception as e:
                logger.warning("[DS][PROMO] Error in slot=%s: %s", slot, e)

        # Purge DS_TRIGGERED entries older than the configured window (housekeeping)
        now_ts = time.time()
        purged = 0
        for m in list(DS_TRIGGERED.keys()):
            if now_ts - DS_TRIGGERED[m] > DS_TRIGGER_WINDOW_SEC:
                DS_TRIGGERED.pop(m, None)
                purged += 1
        if purged and 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[DS][PURGE] Removed %s stale armed entries (window=%ss)", purged, DS_TRIGGER_WINDOW_SEC)

    except Exception as e:
        logger.warning("[DS] dexscreener_poll_once error: %s", e)

def get_pool_vaults(pair_address, dex_id):
    # Raydium example; add more logic if supporting other DEXes
    if dex_id.lower() == "raydium":
        # Raydium V3 pairs API - has vault info
        url = f"https://api.raydium.io/v2/main/pairs/{pair_address}"
        r = requests.get(url, timeout=7)
        if r.status_code == 200:
            data = r.json()
            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                try:
                    logger.debug("[RAYDIUM][VAULTS] pair=%s base=%s quote=%s", pair_address, data.get("baseVault"), data.get("quoteVault"))
                except Exception:
                    pass
            base_vault = data.get("baseVault")
            quote_vault = data.get("quoteVault")
            return [v for v in [base_vault, quote_vault] if v]
        else:
            logger.warning(f"Raydium API vault fetch failed for {pair_address}")
    # You could add Orca vault fetch logic here, if needed
    return []

def get_token_stats_from_pair(pair: dict | None) -> tuple[float | None, float | None, float | None, float | None, float | None]:
    """
    Extract (price_native, price_usd, liq_usd, mcap_usd, change_24h) from a DEXscreener pair dict.
    - Liquidity is taken *verbatim* from pair['liquidity']['usd'] (no scaling).
    - Market cap prefers 'marketCap'; falls back to 'fdv'.
    - All fields coerced to float when possible; otherwise None.
    """
    def _f(x):
        try:
            if x is None or x == "":
                return None
            return float(x)
        except Exception:
            return None

    p = pair or {}
    price_native = _f(p.get("priceNative"))
    price_usd    = _f(p.get("priceUsd"))
    liq_usd      = _f((p.get("liquidity") or {}).get("usd"))
    mcap_usd     = _f(p.get("marketCap")) or _f(p.get("fdv"))
    chg_24h      = _f((p.get("priceChange") or {}).get("h24"))

    if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
        try:
            logger.debug("[DS][STATS] priceNative=%s priceUsd=%s liq_usd=%s mcap=%s chg24h=%s",
                         price_native, price_usd, liq_usd, mcap_usd, chg_24h)
        except Exception:
            pass
    return price_native, price_usd, liq_usd, mcap_usd, chg_24h

def get_all_pairs(token_address):
    url = f"https://api.dexscreener.com/latest/dex/tokens/{token_address}"
    try:
        resp = requests.get(url, timeout=7)
        data = resp.json()
        raw_pairs = data.get('pairs', [])
        dex_whitelist = ['Raydium', 'Orca']
        pairs = []
        for pair in raw_pairs:
            if isinstance(pair, dict):
                dex_id = str(pair.get('dexId', '')).capitalize()
                if dex_id in dex_whitelist:
                    pairs.append(pair)
            else:
                logger.warning(f"[get_all_pairs] Non-dict in raw_pairs: {pair} (type: {type(pair)})")
        return pairs
    except Exception as e:
        logger.warning(f"Dexscreener all-pairs lookup failed: {e}")
        return []

# Helper to fetch base/quote mints for a given promo (with deep debug)
_pair_mints_cache = {}

def _get_pair_mints_for_promo(promo: dict):
    """Return (base_addr_exact, quote_addr_exact, pair_dict) for a promo.
    Uses a simple in-process cache keyed by pair_address to avoid spamming Dexscreener.
    """
    try:
        pair_addr = (promo or {}).get("pair_address") or ""
        if not isinstance(pair_addr, str) or not pair_addr:
            return None, None, None
        if pair_addr in _pair_mints_cache:
            return _pair_mints_cache[pair_addr]
        pair = fetch_dexscreener_pair(pair_addr)
        if not isinstance(pair, dict):
            return None, None, None
        bt = ((pair.get("baseToken") or {}).get("address") or "").strip() or None
        qt = ((pair.get("quoteToken") or {}).get("address") or "").strip() or None
        _pair_mints_cache[pair_addr] = (bt, qt, pair)
        if DEEP_DEBUG:
            logger.info("[PAIR-INDEX] pair=%s base=%s quote=%s dex=%s",
                        pair_addr, bt, qt, (pair.get("dexId") or ""))
        return bt, qt, pair
    except Exception as e:
        logger.warning("_get_pair_mints_for_promo error: %s", e)
        return None, None, None

# === Dexscreener resolvers (single source of truth; deep-debug) ===============
_DS_HTTP_TIMEOUT = 7.0

def _ds_http_get(url: str) -> dict | None:
    try:
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug('[DS][FETCH] url=%s', url)
        r = requests.get(url, timeout=_DS_HTTP_TIMEOUT)
        if r.status_code != 200:
            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                logger.debug('[DS][HTTP] %s on %s', r.status_code, url)
            return None
        return r.json()
    except Exception as e:
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug('[DS][HTTP][ERR] %s on %s', e, url)
        return None

def _select_best_pair_from_ds(ob: dict | None, *, want_pair: str | None = None) -> dict | None:
    """Pick the best Solana pair object from a Dexscreener response object."""
    try:
        if not ob:
            return None
        pairs = (ob.get('pairs') or (ob.get('data') or {}).get('pairs') or [])
        if not isinstance(pairs, list):
            pairs = []
        want_pair_lc = (want_pair or '').lower()
        sol = [p for p in pairs if str(p.get('chainId') or p.get('chain') or '').lower() == 'solana']
        if want_pair_lc:
            for p in sol:
                pid = str(p.get('pairAddress') or p.get('pair') or '').lower()
                if pid == want_pair_lc:
                    return p
        if not sol and pairs:
            sol = pairs  # fallback when chain field missing
        best, best_liq = None, -1.0
        for p in sol:
            try:
                liq = float(((p.get('liquidity') or {}).get('usd')) or 0)
            except Exception:
                liq = 0.0
            if liq > best_liq:
                best, best_liq = p, liq
        return best
    except Exception:
        return None

def fetch_dexscreener_pair(pair_or_mint: str | None) -> dict | None:
    """
    Resolve a Dexscreener pair object for Solana using several fallbacks:
      1) /latest/dex/pairs/solana/<pair>
      2) /latest/dex/tokens/<addr>
      3) /latest/dex/search?q=<addr>
    Returns the selected pair dict or None.
    """
    s = str(pair_or_mint or '').strip()
    if not s:
        return None
    addr_lc = s.lower()

    # 1) exact pair (fast path)
    data = _ds_http_get(f'https://api.dexscreener.com/latest/dex/pairs/solana/{addr_lc}')
    p = _select_best_pair_from_ds(data, want_pair=addr_lc)
    if p:
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            try:
                liq_usd = ((p.get("liquidity") or {}).get("usd"))
                dex_id = p.get("dexId")
                pair_addr_dbg = p.get("pairAddress") or addr_lc
                url_dbg = f"https://dexscreener.com/solana/{pair_addr_dbg}"
                logger.debug(
                    "[DS][FETCH][SELECT] liq_usd=%s dexId=%s url=%s pair=%s",
                    liq_usd, dex_id, url_dbg, pair_addr_dbg
                )
            except Exception:
                pass
        return p

    # 2) token endpoint (mint/pair sometimes work here)
    data = _ds_http_get(f'https://api.dexscreener.com/latest/dex/tokens/{addr_lc}')
    p = _select_best_pair_from_ds(data)
    if p:
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug('[DS][TOK][SELECT] liq_usd=%s dexId=%s pair=%s',
                         ((p.get('liquidity') or {}).get('usd')), p.get('dexId'), p.get('pairAddress'))
        return p

    # 3) search fallback (great for very new tokens)
    data = _ds_http_get(f'https://api.dexscreener.com/latest/dex/search?q={addr_lc}')
    p = _select_best_pair_from_ds(data)
    if p:
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug('[DS][SEARCH][SELECT] liq_usd=%s dexId=%s pair=%s',
                         ((p.get('liquidity') or {}).get('usd')), p.get('dexId'), p.get('pairAddress'))
        return p

    if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
        logger.debug('[DS] No pair found for %s', addr_lc)
    return None

async def _ws_send_json(ws, payload: dict):
    if DEEP_DEBUG:
        try:
            logger.debug("[WS][->] %s", json.dumps(payload))
        except Exception:
            pass
    await ws.send(json.dumps(payload))

def _ws_subscriptions():
    """Yield JSON-RPC subscription payloads for logs/program notifications only.

    Behavior:
      - Prefer logsSubscribe with {"mentions": [<single_program_id>]} for known DEX program ids.
      - If no program ids and SUBSCRIBE_ALL_LOGS=True, use the *string* param: "all" or "allWithVotes".
      - Optionally also subscribe with programSubscribe for each program id (diagnostic; no signatures)
        when ADD_PROGRAM_SUBS=True (default True).
      - NOTE: No accountSubscribe to payment address — payment flow removed.
    """
    _sub_next_id = 1
    def _next_id():
        nonlocal _sub_next_id
        i = _sub_next_id
        _sub_next_id += 1
        return i

    try:
        _commit = str(globals().get("HELIUS_WS_COMMITMENT") or "confirmed")
    except Exception:
        _commit = "confirmed"
    include_votes = bool(globals().get("HELIUS_WS_INCLUDE_VOTES", False))
    add_program_subs = bool(globals().get("ADD_PROGRAM_SUBS", True))
    subscribe_all = bool(globals().get("SUBSCRIBE_ALL_LOGS", False))

    # Normalize program ids
    prog_ids = []
    try:
        raw = globals().get("DEX_PROGRAM_IDS")
        if isinstance(raw, dict):
            prog_ids = [str(k).strip() for k in raw.keys() if k]
        elif isinstance(raw, (list, tuple, set)):
            prog_ids = [str(k).strip() for k in raw if k]
        seen = set()
        prog_ids = [p for p in prog_ids if not (p in seen or seen.add(p))]
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[WS][SUBSCRIBE] program ids normalized -> %s (count=%d)", prog_ids, len(prog_ids))
    except Exception as e:
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[WS][SUBSCRIBE][ERR] normalizing program ids failed: %s", e)
        prog_ids = []

    # 1) logsSubscribe (primary)
    try:
        if prog_ids:
            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                logger.debug("[WS][SUBSCRIBE] logsSubscribe via mentions; commitment=%s", _commit)
            for pid in prog_ids:
                payload = {
                    "jsonrpc": "2.0",
                    "id": _next_id(),
                    "method": "logsSubscribe",
                    "params": [
                        {"mentions": [pid]},
                        {"commitment": _commit},
                    ],
                }
                if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                    logger.debug("[WS][->] logsSubscribe payload=%s", payload)
                yield payload
        elif subscribe_all:
            first_param = "allWithVotes" if include_votes else "all"
            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                logger.debug("[WS][SUBSCRIBE] logsSubscribe using %s; commitment=%s", first_param, _commit)
            payload = {
                "jsonrpc": "2.0",
                "id": _next_id(),
                "method": "logsSubscribe",
                "params": [
                    first_param,
                    {"commitment": _commit},
                ],
            }
            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                logger.debug("[WS][->] logsSubscribe payload=%s", payload)
            yield payload
        else:
            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                logger.debug("[WS][SUBSCRIBE] Skipping logsSubscribe (no program IDs; SUBSCRIBE_ALL_LOGS=False).")
    except Exception as e:
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[WS][SUBSCRIBE][ERR] logsSubscribe setup failed: %s", e)

    # 2) Optional programSubscribe (diagnostics)
    try:
        if prog_ids and add_program_subs:
            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                logger.debug("[WS][SUBSCRIBE] programSubscribe x%d; commitment=%s", len(prog_ids), _commit)
            for pid in prog_ids:
                payload = {
                    "jsonrpc": "2.0",
                    "id": _next_id(),
                    "method": "programSubscribe",
                    "params": [
                        pid,
                        {"encoding": "jsonParsed", "commitment": _commit},
                    ],
                }
                if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                    logger.debug("[WS][->] programSubscribe payload=%s", payload)
                yield payload
    except Exception as e:
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[WS][SUBSCRIBE][ERR] programSubscribe setup failed: %s", e)

async def _extract_signature_from_ws(msg: dict) -> str | None:
    # logsNotification carries a signature; accountNotification generally does not
    try:
        if (msg.get("method") == "logsNotification"):
            return (((msg.get("params") or {}).get("result") or {}).get("value") or {}).get("signature")
    except Exception:
        return None
    return None

async def run_helius_ws(app: Application):
    """Background task: maintain a Helius/Solana WS connection and process SPL transfers only.
    Notes:
      - Fully credit-free: NO Enhanced REST, NO hydration, NO dexscreener fallback.
      - Listens purely to WS logs to detect SPL token transfers.
      - Deep debug breadcrumbs for every stage.
    """
    # --- Guard rails ---
    try:
        if not USE_HELIUS_WS:
            logger.info("[WS] USE_HELIUS_WS=False; skipping WS loop.")
            return
        if not HELIUS_API_KEY:
            logger.warning("[WS] HELIUS_API_KEY missing; cannot start WS.")
            return
    except Exception:
        pass

    # --- Resolve and normalize the URL (updates HELIUS_WS_URL if missing) ---
    try:
        url = _resolve_ws_url() if callable(globals().get("_resolve_ws_url", None)) else (
            globals().get("HELIUS_WS_URL") or f"wss://atlas-mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
        )
    except Exception:
        url = globals().get("HELIUS_WS_URL") or f"wss://atlas-mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"

    backoff = 2
    max_backoff = 60
    conn_seq = 0

    while True:
        try:
            conn_seq += 1
            _u = str(url)
            # Mask API key in logs
            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                try:
                    mk = ("***" + str(HELIUS_API_KEY)[-6:]) if isinstance(HELIUS_API_KEY, str) and len(HELIUS_API_KEY) >= 6 else "present"
                    um = _u.replace(str(HELIUS_API_KEY), mk)
                    logger.debug("[WS][OPEN][%s] Dialing Helius URL=%s", conn_seq, um)
                except Exception:
                    pass
            logger.info("[WS] Connecting -> %s", _u)

            t0 = time.time()

            async with websockets.connect(
                _u,
                ping_interval=20,
                ping_timeout=20,
                max_queue=1000,
            ) as ws:
                # Connected successfully
                if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                    try:
                        logger.debug(
                            "[WS][OPENED][%s] state=open elapsed=%.2fs",
                            conn_seq, (time.time() - t0)
                        )
                    except Exception:
                        pass

                backoff = 2

                # Prepare subscriptions (Tokenkeg + SOL_RECEIVE_ADDRESS)
                try:
                    subs = list(_ws_subscriptions())
                except Exception:
                    subs = []

                if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                    try:
                        logger.debug("[WS][SUB] count=%s payload_ids=%s", len(subs), [s.get("id") for s in subs if isinstance(s, dict)])
                    except Exception:
                        pass

                for sub in subs:
                    try:
                        await _ws_send_json(ws, sub)
                    except Exception as e:
                        logger.warning("[WS][SUB][ERR] %s", e)

                logger.info("[WS] Subscribed to logs + account feed (credit-free).")

                # Receive loop
                msg_count = 0
                while True:
                    raw = await ws.recv()
                    msg_count += 1
                    if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                        try:
                            s = str(raw)
                            logger.debug("[WS][RAW<=][#%s] %s", msg_count, (s[:240] + ("…" if len(s) > 240 else "")))
                        except Exception:
                            pass
                    try:
                        msg = json.loads(raw)
                    except Exception:
                        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                            try:
                                logger.debug("[WS][PARSE][FAIL] raw=%s", str(raw)[:300])
                            except Exception:
                                pass
                        continue

                    m = msg.get("method")
                    if 'DEEP_DEBUG' in globals() and DEEP_DEBUG and m:
                        logger.debug("[WS][<=] method=%s", m)

                    # Acks
                    if isinstance(msg, dict) and msg.get("result") is not None and msg.get("id") is not None:
                        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                            logger.debug("[WS][ACK] id=%s", msg.get("id"))
                        continue

                    # Try to detect SPL transfer directly from logs (no hydration)
                    try:
                        result = (msg.get("params") or {}).get("result") or {}
                        value = result.get("value") or {}
                        logs = value.get("logs") or []
                        sig = (
                            result.get("signature")
                            or result.get("transactionSignature")
                            or (value or {}).get("signature")
                        )
                        if not isinstance(logs, list):
                            logs = []
                    except Exception:
                        continue

                    saw_tokenkeg = False
                    saw_transfer = False
                    for line in logs:
                        s = str(line)
                        if "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA" in s and "invoke" in s:
                            saw_tokenkeg = True
                        if "Program log: Instruction: Transfer" in s or "Program log: Instruction: TransferChecked" in s:
                            saw_transfer = True

                    if saw_tokenkeg and saw_transfer:
                        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                            logger.debug("[WS][SPL][XFER] sig=%s detected via logs-only", (str(sig)[:8] if sig else "?"))
                        try:
                            await _handle_spl_transfer_log(app, sig, logs)
                        except Exception as e:
                            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                                logger.debug("[WS][SPL][ERR] %s", e)
                    else:
                        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                            logger.debug("[WS][SKIP] Non-SPL-transfer msg #%s", msg_count)

        except Exception as e:
            logger.warning("[WS][ERR] %s", e)
            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                logger.debug("[WS][RETRY] Will reconnect in %ss", backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, max_backoff)
            continue

        except asyncio.CancelledError:
            logger.info("[WS] Cancelled; exiting WS loop.")
            return
        except websockets.exceptions.InvalidStatusCode as e:
            # Specific handling for HTTP status codes during handshake
            status = getattr(e, 'status_code', None)
            msg = f"HTTP {status}" if status else str(e)
            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                logger.debug("[WS][ERR][HANDSHAKE] %s", msg)
            if status == 401:
                try:
                    _u = str(url)
                    mk = ("***" + str(HELIUS_API_KEY)[-6:]) if isinstance(HELIUS_API_KEY, str) and len(HELIUS_API_KEY) >= 6 else "present"
                    logger.warning(
                        "[WS][AUTH] 401 from WS. Check: 1) API key validity  2) Project has WS access  3) Correct Atlas URL. url=%s key=%s",
                        _u.replace(str(HELIUS_API_KEY), mk), mk,
                    )
                except Exception:
                    pass
            # Backoff and retry
            delay = min(max(backoff, 4), max_backoff)
            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                logger.debug("[WS][BACKOFF] handshake error; sleeping %ss (backoff=%s)", delay, backoff)
            await asyncio.sleep(delay)
            backoff = min(backoff * 2, max_backoff)
        except Exception as e:
            msg = str(e)
            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                logger.debug("[WS][ERR] %s", msg)

            if "429" in msg:
                delay = max(backoff, 15)
                if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                    logger.debug("[WS][BACKOFF] 429 detected; sleeping %ss before reconnect", delay)
                await asyncio.sleep(delay)
                backoff = min(delay * 2, 120)
            else:
                if "extra_headers" in msg:
                    logger.warning("[WS][HINT] websockets version lacks extra_headers; connect() now uses only supported args.")
                if 'Unauthorized' in msg or '401' in msg:
                    try:
                        _u = str(url)
                        mk = ("***" + str(HELIUS_API_KEY)[-6:]) if isinstance(HELIUS_API_KEY, str) and len(HELIUS_API_KEY) >= 6 else "present"
                        logger.warning(
                            "[WS][AUTH] 401 from WS. Verify API key, WS entitlement on Helius, and URL format. url=%s key=%s",
                            _u.replace(str(HELIUS_API_KEY), mk), mk,
                        )
                    except Exception:
                        pass
                delay = min(backoff, 30)
                if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                    logger.debug("[WS][BACKOFF] generic error; sleeping %ss (backoff=%s)", delay, backoff)
                await asyncio.sleep(delay)
                backoff = min(backoff * 2, max_backoff)

# --- Helius WebSocket startup (standalone, no PTB hooks) ---
try:
    # Deprecated path; listener is started via JobQueue singleton elsewhere.
    if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
        logger.debug("[BOOT][WS] Legacy thread startup disabled (using JobQueue singleton).")
except Exception as e:
    logger.error("[BOOT][WS] Startup shim error: %s", e)

# ===== Minimal JSON-RPC helpers =====
def jsonrpc_payload(method: str, params: list | None = None, *, _id: int = 1) -> dict:
    """
    Build a minimal JSON-RPC 2.0 payload.
    Examples:
      jsonrpc_payload("getAccountInfo", ["ACCOUNT_ADDRESS"])
      jsonrpc_payload("getHealth")
    """
    try:
        # Ensure params is a list per JSON-RPC conventions
        p = params if isinstance(params, list) else []
    except Exception:
        p = []
    return {"jsonrpc": "2.0", "id": _id, "method": method, "params": p}

def rpc_post(payload: dict, rpc_url: str | None = None, timeout: float = 8.0) -> dict | None:
    """
    POST a JSON-RPC payload to the configured Solana RPC endpoint.
    Uses RPC_HTTPS if no rpc_url is provided. Returns parsed JSON or None on error.
    """
    url = rpc_url or (globals().get("RPC_HTTPS") or "")
    if not isinstance(url, str) or not url.strip():
        logger.warning("[RPC] RPC_HTTPS is not set; cannot make JSON-RPC call.")
        return None
    try:
        r = requests.post(url, json=payload, timeout=timeout)
        if not r.ok:
            logger.warning("[RPC] HTTP %s for %s", r.status_code, payload.get("method"))
        return r.json()
    except Exception as e:
        logger.warning("[RPC] %s failed: %s", payload.get("method"), e)
        return None

def rpc_get_health(rpc_url: str | None = None, timeout: float = 8.0) -> str | None:
    """
    Call `getHealth` (no params). Returns result string (e.g., 'ok') or None.
    """
    resp = rpc_post(jsonrpc_payload("getHealth", []), rpc_url=rpc_url, timeout=timeout)
    if isinstance(resp, dict):
        return resp.get("result")
    return None

def rpc_get_account_info(account_address: str, rpc_url: str | None = None, timeout: float = 8.0) -> dict | None:
    """
    Call `getAccountInfo` for a single public key. Returns the 'result' dict or None.
    """
    if not isinstance(account_address, str) or not account_address.strip():
        logger.warning("[RPC] getAccountInfo requires a non-empty account address.")
        return None
    resp = rpc_post(jsonrpc_payload("getAccountInfo", [account_address]), rpc_url=rpc_url, timeout=timeout)
    if isinstance(resp, dict):
        return resp.get("result")
    return None

def startup_config_report():
    try:
        promos = load_promotions() or {}
        mints = [
            (p.get("token_address") or "").lower()
            for p in promos.values()
            if p.get("token_address")
        ]
        ws_filter_mode = "mentions" if (DEX_PROGRAM_IDS and len(DEX_PROGRAM_IDS) > 0) else "all"

        logger.info("========== STARTUP CONFIG REPORT ==========")
        logger.info("Bot/Group:")
        logger.info("  GROUP_CHAT_IDS=%s", GROUP_CHAT_IDS)
        try:
            logger.info("  GROUP_CHAT_IDS expanded -> %s", list(_expand_chat_targets(GROUP_CHAT_IDS)))
        except Exception:
            pass
        logger.info("  ADMIN_USER_IDS=%s", ADMIN_USER_IDS)
        logger.info("  ADMIN_ONLY_SLOTS=%s", sorted(list(ADMIN_ONLY_SLOTS)) if 'ADMIN_ONLY_SLOTS' in globals() else "N/A")
        logger.info(
            "Alerts: USE_HELIUS_GATE=%s  USE_HELIUS_WS=%s  BUY_CHECK_INTERVAL=%s  "
            "DEXSCREENER_MIN_BUYS_DELTA=%s  DS_TRIGGER_WINDOW_SEC=%s  DS_FALLBACK_TIMEOUT_SEC=%s  REQUIRE_DS_TRIGGER=%s  DEEP_DEBUG=%s",
            USE_HELIUS_GATE, USE_HELIUS_WS, BUY_CHECK_INTERVAL,
            DEXSCREENER_MIN_BUYS_DELTA, DS_TRIGGER_WINDOW_SEC, DS_FALLBACK_TIMEOUT_SEC,
            REQUIRE_DS_TRIGGER if 'REQUIRE_DS_TRIGGER' in globals() else 'N/A',
            DEEP_DEBUG if 'DEEP_DEBUG' in globals() else 'N/A'
        )

        # Promos / trending
        logger.info("Promos loaded: %d slot(s)", len(promos))
        logger.info("  mints(lowercase for display): %s", mints if mints else "[]")
        if DEEP_DEBUG:
            try:
                preview = {
                    int(s): {
                        "name": (p or {}).get("token_name"),
                        "mint": (p or {}).get("token_address"),
                        "pair": (p or {}).get("pair_address"),
                        "ends": (p or {}).get("end_time")
                    }
                    for s, p in list(promos.items())[:8]
                }
                logger.debug("  promos preview: %s%s", preview, " ..." if len(promos) > 8 else "")
            except Exception:
                pass

        # DS trigger window snapshot
        try:
            active_ds = 0
            if 'DS_TRIGGERED' in globals():
                now = time.time()
                for m, ts in list(DS_TRIGGERED.items()):
                    if now - ts <= (DS_TRIGGER_WINDOW_SEC if 'DS_TRIGGER_WINDOW_SEC' in globals() else 0):
                        active_ds += 1
            logger.info("DS trigger window: active=%s window=%ss", active_ds, (DS_TRIGGER_WINDOW_SEC if 'DS_TRIGGER_WINDOW_SEC' in globals() else 'N/A'))
        except Exception:
            pass

        # RPC endpoints & WS status
        logger.info("RPC endpoints:")
        logger.info("  RPC_HTTPS=%s", RPC_HTTPS)
        logger.info("  RPC_WSS=%s", RPC_WSS)
        try:
            health = rpc_get_health()
            logger.info("  RPC getHealth -> %s", health if health is not None else "unavailable")
        except Exception as _e:
            logger.info("  RPC getHealth -> error: %s", _e)
        try:
            acct_probe = rpc_get_account_info(str(SOL_RECEIVE_ADDRESS)) if 'SOL_RECEIVE_ADDRESS' in globals() else None
            state = "ok" if isinstance(acct_probe, dict) else "unavailable"
            logger.info("  RPC getAccountInfo(SOL_RECEIVE_ADDRESS) -> %s", state)
        except Exception as _e:
            logger.info("  RPC getAccountInfo -> error: %s", _e)

        if HELIUS_API_KEY:
            try:
                masked = ("***" + str(HELIUS_API_KEY)[-6:]) if isinstance(HELIUS_API_KEY, str) and len(HELIUS_API_KEY) >= 6 else "present"
            except Exception:
                masked = "present"
            logger.info("Helius API key: %s", masked)
        else:
            logger.info("Helius API key: MISSING")

        if HELIUS_WS_URL and USE_HELIUS_WS:
            logger.info("Helius WS: %s (commitment=%s, filter=%s)", HELIUS_WS_URL, HELIUS_WS_COMMITMENT, ws_filter_mode)
            logger.info("  WS URL: %s", HELIUS_WS_URL)
            if ws_filter_mode == "mentions":
                try:
                    ids_list = list(DEX_PROGRAM_IDS) if isinstance(DEX_PROGRAM_IDS, dict) else DEX_PROGRAM_IDS
                except Exception:
                    ids_list = []
                logger.info("  DEX_PROGRAM_IDS=%s", ids_list)
        else:
            logger.info("Helius WS: disabled (USE_HELIUS_WS=%s or HELIUS_WS_URL missing)", USE_HELIUS_WS)

        logger.info("===========================================")
    except Exception as e:
        logger.warning("startup_config_report failed: %s", e)



def remove_token_from_all_slots(promos, mint, except_slot=None):
    removed = False
    to_delete = []
    for slot, promo in promos.items():
        if promo["token_address"] == mint and (except_slot is None or int(slot) != except_slot):
            to_delete.append(slot)
    for slot in to_delete:
        logger.info(f"Removing token {mint} from slot {slot} due to duplicate trending restriction.")
        del promos[slot]
        removed = True
    if removed:
        save_promotions(promos)
    return removed


(
    CHOOSE_SLOT,
    GET_ADDRESS,
    GET_TOKEN_NAME,
    GET_DURATION,
    CONFIRM,
) = range(5)

# --- Booking time limit (4 minutes) ---
BOOKING_TIMEOUT_SEC = 4 * 60  # 4 minutes to complete booking steps

async def _booking_timeout_job(context: CallbackContext):
    """
    JobQueue callback: called ~4 minutes after a slot is reserved.
    If the booking hasn't been confirmed by then, release the slot and
    notify the user.
    """
    try:
        job = getattr(context, "job", None)
        data = getattr(job, "data", {}) if job is not None else {}
        slot = int(data.get("slot") or 0)
        user_id = int(data.get("user_id") or 0)
        chat_id = int(data.get("chat_id") or 0)
    except Exception:
        slot = user_id = chat_id = 0

    if "DEEP_DEBUG" in globals() and DEEP_DEBUG:
        try:
            logger.debug("[BOOK][TIMEOUT][JOB] slot=%s user=%s chat=%s", slot, user_id, chat_id)
        except Exception:
            pass

    if not slot or not user_id:
        return

    # If the deadline metadata has already been cleared (user confirmed/cancelled),
    # do nothing.
    if not _booking_deadline_expired(context):
        if "DEEP_DEBUG" in globals() and DEEP_DEBUG:
            try:
                logger.debug("[BOOK][TIMEOUT][JOB] deadline not expired or already cleared; skipping")
            except Exception:
                pass
        return

    # Release the hold
    try:
        release_slot(slot, user_id)
    except Exception as e:
        if "DEEP_DEBUG" in globals() and DEEP_DEBUG:
            logger.debug("[BOOK][TIMEOUT][JOB][RELEASE][ERR] %s", e)

    _clear_booking_deadline(context)

    # Notify the user directly (no ConversationHandler needed)
    if chat_id:
        try:
            await context.bot.send_message(
                chat_id=chat_id,
                text=(
                    "⏰ Your 4-minute booking time limit has expired.\n\n"
                    "The slot has been released. If you still want to trend, "
                    "please /start again and re-book a slot."
                ),
            )
        except Exception as e:
            if "DEEP_DEBUG" in globals() and DEEP_DEBUG:
                logger.debug("[BOOK][TIMEOUT][JOB][NOTIFY][ERR] %s", e)

# --- CANCEL HANDLER (admin conv only; does NOT touch slot holds) ---
async def cancel(update: Update, context: CallbackContext):
    """Generic conversation cancel used for admin flows (does NOT touch slot holds)."""
    # This cancel is only meant for admin conversations such as price adjustment.
    # It must NOT release slot holds; booking flows use cmd_cancel/_cb_cancel_booking instead.
    if update.message:
        await update.message.reply_text("❌ Operation cancelled. To start again, use /start.")
    elif update.callback_query:
        try:
            await update.callback_query.edit_message_text("❌ Operation cancelled. To start again, use /start.")
        except Exception:
            # Fallback if we can't edit the original message
            try:
                await update.callback_query.answer("❌ Operation cancelled.", show_alert=False)
            except Exception:
                pass
    return ConversationHandler.END

async def start(update: Update, context: CallbackContext):
    # Fast, lightweight /start: avoid heavy work and render keyboard immediately
    t0 = time.time()
    logger.info(f"[START] Bookings enabled? {is_bookings_enabled()}")
    if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
        logger.debug("[START][BEGIN] user=%s", get_user_id_from_update(update))

    # Only clear expired promos (cheap) and build UI; no network calls here
    async with promo_lock:
        promos = clear_expired_promotions()
        context.user_data["promos"] = promos
        user_id = get_user_id_from_update(update)
        keyboard = build_slot_inline_keyboard(promos, user_id)

    try:
        await update.message.reply_text(
            "👇 *Select a slot to book:*\n(You can /cancel at any time to quit)",
            reply_markup=keyboard,
            parse_mode="Markdown"
        )
    except Exception as e:
        logger.warning("[START][SEND][ERR] %s", e)
        return ConversationHandler.END

    if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
        try:
            logger.debug("[START][END] ui_ms=%d slots=%d", int((time.time()-t0)*1000), len((keyboard.inline_keyboard or [])))
        except Exception:
            pass
    return CHOOSE_SLOT

async def choose_slot_callback(update: Update, context: CallbackContext):
    # Fast, responsive booking with deep debug and an atomic slot hold
    query = update.callback_query
    await query.answer()
    data = query.data
    user_id = get_user_id_from_callback(query)
    chat_id = getattr(getattr(query, "message", None), "chat_id", None) or getattr(
        getattr(update, "effective_chat", None), "id", None
    )

    if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
        try:
            logger.debug(
                "[BOOK][CHOOSE][BEGIN] data=%s user=%s chat_id=%s",
                data,
                user_id,
                chat_id,
            )
        except Exception:
            pass

    bookings_on = is_bookings_enabled()
    if data == "BOOKED":
        await query.answer("Slot already booked.", show_alert=True)
        return ConversationHandler.END
    if data in {"INPROGRESS", "DISABLED"} or not bookings_on:
        await query.answer("Slot booking is currently disabled! 🚧", show_alert=True)
        return ConversationHandler.END
    if data == "ENABLE_BOOKINGS":
        return await enable_bookings_button(update, context)
    if data == "DISABLE_BOOKINGS":
        return await disable_bookings_button(update, context)

    # Parse slot id
    try:
        slot = int(data)
    except Exception:
        await query.answer("Invalid slot.", show_alert=True)
        return ConversationHandler.END

    # Quick UI feedback
    try:
        await query.edit_message_text("⏳ Processing your slot selection…")
    except TimedOut:
        logger.warning("[BOOK][CHOOSE] Timeout sending reserving prompt.")

    # Admin-only restriction
    if slot in ADMIN_ONLY_SLOTS and not is_admin_user(user_id):
        try:
            await query.edit_message_text("That slot is reserved for admins only.")
        except TimedOut:
            logger.warning("[BOOK][CHOOSE] Timeout admin-only notice.")
        return ConversationHandler.END

    # Atomically check-and-hold under the promo_lock to avoid races
    async with promo_lock:
        # If another user already holds it, stop here
        if slot_held_by_other(slot, user_id):
            try:
                await query.edit_message_text(
                    "Sorry, this slot is currently being booked by someone else. Try another slot."
                )
            except TimedOut:
                logger.warning("[BOOK][CHOOSE] Timeout in-progress notice.")
            return ConversationHandler.END

        # Load promos and stop if actually booked
        try:
            promos = load_promotions() or {}
        except Exception as e:
            promos = {}
            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                logger.debug("[BOOK][CHOOSE] load_promotions error: %s", e)

        if str(slot) in promos:
            try:
                await query.edit_message_text("Slot already booked. Try another slot.")
            except TimedOut:
                logger.warning("[BOOK][CHOOSE] Timeout already-booked notice.")
            return ConversationHandler.END

        # Set/refresh the hold for this user now
        hold_slot(slot, user_id)

    # Start the 4-minute booking deadline for this user and slot
    try:
        _set_booking_deadline(context, slot, user_id, chat_id)
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug(
                "[BOOK][DEADLINE][SET] slot=%s user=%s chat_id=%s",
                slot,
                user_id,
                chat_id,
            )
    except Exception as e:
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[BOOK][DEADLINE][ERR] slot=%s user=%s err=%s", slot, user_id, e)

    # Persist chosen slot into the convo data
    context.user_data["slot"] = slot

    # Ask next question
    try:
        if (slot in {16, 17}) and is_admin_user(user_id):
            await query.edit_message_text(
                "Enter custom duration (in hours, e.g. 2.5):\n\n"
                "⏰ You have 4 minutes to complete your booking.\n"
                "Send /cancel to quit"
            )
            next_state = GET_DURATION
        else:
            await query.edit_message_text(
                "Enter your token address (mint):\n\n"
                "⏰ You have 4 minutes to complete your booking.\n"
                "Send /cancel to quit"
            )
            next_state = GET_ADDRESS
    except TimedOut:
        logger.warning("[BOOK][CHOOSE] Timeout editing message for slot selection.")
        next_state = ConversationHandler.END

    if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
        try:
            logger.debug("[BOOK][CHOOSE][END] user=%s slot=%s next=%s", user_id, slot, next_state)
        except Exception:
            pass

    return next_state

async def admin_remove_button(update: Update, context: CallbackContext):
    """Entry point when admin taps 'Remove Trending (Admin)'. Shows a list of current promos."""
    query = update.callback_query
    await query.answer()
    uid = get_user_id_from_callback(query)

    if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
        logger.debug("[ADMIN][REMOVE][BEGIN] user=%s", uid)

    if not is_admin_user(uid):
        await query.answer("Admins only.", show_alert=True)
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[ADMIN][REMOVE][DENY] user=%s", uid)
        return ConversationHandler.END

    promos = load_promotions() or {}
    if not promos:
        try:
            await query.edit_message_text("There are no trending tokens right now.")
        except Exception as e:
            logger.warning("[ADMIN][REMOVE][EMPTY][ERR] %s", e)
        return ConversationHandler.END

    try:
        kb = build_admin_remove_keyboard(promos)
        await query.edit_message_text("Select the trending token to remove:", reply_markup=kb)
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[ADMIN][REMOVE][LIST] count=%s", len(promos))
    except Exception as e:
        logger.warning("[ADMIN][REMOVE][LIST][ERR] %s", e)
    return ConversationHandler.END


async def admin_remove_select(update: Update, context: CallbackContext):
    """Handler for selecting a specific slot to remove from trending."""
    query = update.callback_query
    await query.answer()
    uid = get_user_id_from_callback(query)
    data = str(query.data or "")

    if not is_admin_user(uid):
        await query.answer("Admins only.", show_alert=True)
        return ConversationHandler.END

    try:
        # Expect ADMIN_REMOVE_SLOT:<slot>
        parts = data.split(":")
        slot = int(parts[1]) if len(parts) == 2 else 0
    except Exception:
        slot = 0

    if not slot:
        await query.answer("Bad selection.", show_alert=True)
        return ConversationHandler.END

    # Load, remove, save
    promos = load_promotions() or {}
    removed = False
    try:
        if str(slot) in promos:
            promos.pop(str(slot), None)
            save_promotions(promos)
            removed = True
            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                logger.debug("[ADMIN][REMOVE][OK] slot=%s by user=%s", slot, uid)
    except Exception as e:
        logger.warning("[ADMIN][REMOVE][ERR] %s", e)

    # Update pinned message regardless
    try:
        await update_pinned_trending_message(context.bot, uid)
    except Exception as e:
        logger.warning("[ADMIN][REMOVE][PIN][ERR] %s", e)

    # Notify and redraw the main booking keyboard
    try:
        async with promo_lock:
            current = clear_expired_promotions()
            keyboard = build_slot_inline_keyboard(current, uid)
        msg = ("✅ Removed trending from slot #{slot}." if removed else "Nothing to remove for that slot.").format(slot=slot)
        await query.edit_message_text(msg, reply_markup=keyboard, parse_mode="Markdown")
    except Exception as e:
        logger.warning("[ADMIN][REMOVE][REDRAW][ERR] %s", e)

    return ConversationHandler.END

async def get_duration(update: Update, context: CallbackContext):
    if update.message.text.strip().lower() == '/cancel':
        return await cancel(update, context)
    if _booking_deadline_expired(context):
        return await _handle_booking_timeout(update, context)
    try:
        duration = float(update.message.text.strip())
        if duration <= 0:
            raise ValueError
    except Exception:
        await update.message.reply_text("Invalid input. Please enter duration in hours (e.g. 2.5):\n\nSend /cancel to quit")
        return GET_DURATION
    slot = context.user_data["slot"]
    TRENDING_SLOTS[slot]["duration"] = duration
    await update.message.reply_text("Enter your token address (mint):\n\nSend /cancel to quit")
    return GET_ADDRESS

async def get_address(update: Update, context: CallbackContext):
    if update.message.text.strip().lower() == '/cancel':
        return await cancel(update, context)
    if _booking_deadline_expired(context):
        return await _handle_booking_timeout(update, context)

    context.user_data["token_address"] = update.message.text.strip()
    token_address = context.user_data["token_address"]

    # Try to resolve best LP/pair for the token
    pair_address, pair = get_best_pair(token_address)

    if not pair_address or not pair:
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            try:
                logger.debug("[BOOK][ADDR][NO-LP] token=%s", token_address)
            except Exception:
                pass
        await update.message.reply_text(
            "❌ Could not find LP (pair) for this token on any major Solana DEX. "
            "Please check your mint address or try a different token."
        )
        return ConversationHandler.END

    context.user_data["pair_address"] = pair_address
    context.user_data["pair"] = pair

    await update.message.reply_text("Enter your token name:\n\nSend /cancel to quit")
    return GET_TOKEN_NAME

async def get_token_name(update: Update, context: CallbackContext):
    if update.message.text.strip().lower() == '/cancel':
        return await cancel(update, context)
    if _booking_deadline_expired(context):
        return await _handle_booking_timeout(update, context)
    context.user_data["token_name"] = update.message.text.strip()
    slot = context.user_data["slot"]
    s = TRENDING_SLOTS[slot]
    token_name = context.user_data['token_name']
    token_address = context.user_data['token_address']
    pair_address = context.user_data['pair_address']
    msg = (
    f"Slot {slot} ({'Admin Only' if slot in ADMIN_ONLY_SLOTS else str(s['price']) + ' SOL'})\n"
    f"Duration: {s['duration']} hours\n"
    f"Token: {token_name}\n"
    f"Mint: {token_address}\n"
    f"LP: {pair_address}\n\n"
    "Type 'yes' to confirm booking, or 'no' to cancel. Or send /cancel."
)
    await update.message.reply_text(msg)
    return CONFIRM

async def confirm(update: Update, context: CallbackContext):
    if update.message.text.strip().lower() == '/cancel':
        return await cancel(update, context)
    if _booking_deadline_expired(context):
        return await _handle_booking_timeout(update, context)

    answer = (update.message.text or "").strip().lower()
    slot = int(context.user_data.get("slot", 0) or 0)
    user_id = int(getattr(update.effective_user, "id", 0) or 0)

    if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
        logger.debug("[CONFIRM][BEGIN] user=%s slot=%s answer=%s", user_id, slot, answer)

    # Cancel path
    if answer != "yes":
        release_slot(slot, user_id)
        _clear_booking_deadline(context)
        await update.message.reply_text("Booking cancelled.")
        return ConversationHandler.END

    async with promo_lock:
        promos = clear_expired_promotions() or {}
        if str(slot) in promos:
            release_slot(slot, user_id)
            await update.message.reply_text("Sorry, someone just booked that slot. Try again.")
            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                logger.debug("[CONFIRM][TAKEN] slot=%s already booked.", slot)
            return ConversationHandler.END

        if slot in ADMIN_ONLY_SLOTS and user_id not in ADMIN_USER_IDS:
            release_slot(slot, user_id)
            await update.message.reply_text("That slot is reserved for admins only.")
            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                logger.debug("[CONFIRM][DENIED] user=%s tried admin-only slot=%s", user_id, slot)
            return ConversationHandler.END

        s = TRENDING_SLOTS[slot]
        base_addr = ((context.user_data.get("pair") or {}).get("baseToken") or {}).get("address")
        if isinstance(base_addr, str) and base_addr.strip():
            context.user_data["token_address"] = base_addr.strip()

        promo_data = {
            "token_name": context.user_data.get("token_name"),
            "token_address": context.user_data.get("token_address"),
            "pair_address": context.user_data.get("pair_address"),
            "booked_by": user_id,
            "start_time": int(time.time()),
            "duration": s["duration"],
            "end_time": int(time.time() + s["duration"] * 3600),
        }
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[CONFIRM][SAVED] promo_data=%s", promo_data)

        removed = remove_token_from_all_slots(promos, promo_data["token_address"], except_slot=slot)
        if removed:
            logger.info("Duplicate trending token removed from another slot.")

        promos[str(slot)] = promo_data
        save_promotions(promos)

        try:
            await update_pinned_trending_message(context.bot, user_id)
            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                logger.debug("[CONFIRM][PIN] Updated pinned trending after booking slot=%s", slot)
        except Exception as e:
            logger.warning("[CONFIRM] Failed to update pinned summary for slot %s: %s", slot, e)

        try:
            asyncio.create_task(_broadcast_trending_announcement(slot, promo_data, context.bot))
            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                logger.debug("[CONFIRM][SCHEDULE] Queued broadcast for slot=%s", slot)
        except Exception as e:
            logger.warning("[CONFIRM] Failed to queue broadcast for slot %s: %s", slot, e)

    # Success -> release the hold, clear deadline, and notify the user
    release_slot(slot, user_id)
    _clear_booking_deadline(context)
    await update.message.reply_text("✅ Your token is now trending!")
    if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
        logger.debug("[CONFIRM][SUCCESS] Slot=%s booked successfully.", slot)
    return ConversationHandler.END

async def build_trending_summary_message(user_id=None):
    """
    Build the pinned 'SOLANA TRENDING LIST' message.
    Re-adds the ticker (symbol) next to the token name.
    Emits deep debug breadcrumbs.
    """
    promos = load_promotions() or {}
    trending = []
    for slot in sorted(promos, key=lambda x: int(x)):
        try:
            if user_id is not None and int(slot) in ADMIN_ONLY_SLOTS and user_id not in ADMIN_USER_IDS:
                continue
            promo = promos[slot]
            token_name = promo.get('token_name', '')

            pair_addr = promo.get("pair_address")
            pair = fetch_dexscreener_pair(pair_addr) if pair_addr else None

            ticker = promo.get('symbol')
            if not ticker and isinstance(pair, dict):
                bt = pair.get("baseToken") or {}
                ticker = bt.get("symbol") or pair.get("symbol")

            _, _, _, _, change_24h = get_token_stats_from_pair(pair or {})
            change_str = f"{change_24h:+.2f}%" if change_24h is not None else "N/A"

            if ticker:
                trending.append(f"<b>{token_name} ${str(ticker).upper()}</b> <b>{change_str}</b>")
            else:
                trending.append(f"<b>{token_name}</b> <b>{change_str}</b>")
        except Exception as e:
            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                logger.debug("[PINNED][ROW][ERR] slot=%s err=%s", slot, e)
            continue

    if not trending:
        return None, None

    msg = "<b>📌 SOLANA TRENDING LIST</b>\n\n" + "\n".join(trending)
    if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
        logger.debug("[PINNED][BUILD] rows=%s", len(trending))
    return msg, trending


async def update_pinned_trending_message(bot, user_id: int = 0):
    """
    Build and (re)pin the 'SOLANA TRENDING LIST' summary.
    - Tries every expanded chat id; failure on one does not abort the rest.
    - Unpins the previous pinned message for that chat if we have its id.
    - Uses deep-debug breadcrumbs throughout.
    """
    try:
        msg_text, _rows = await build_trending_summary_message(user_id=user_id)
    except Exception as e:
        logger.warning("[PIN][BUILD][ERR] %s", e)
        return

    if not msg_text:
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[PIN] No trending rows to pin; skipping.")
        return

    targets = list(_expand_chat_targets(GROUP_CHAT_IDS)) if 'GROUP_CHAT_IDS' in globals() else []
    if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
        logger.debug("[PIN][TARGETS] Expanded chat targets -> %s (count=%s)", targets, len(targets))

    for chat_id in targets:
        try:
            # Normalize chat id
            try:
                int_chat = int(str(chat_id).strip())
            except Exception:
                if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                    logger.debug("[PIN][SKIP] Invalid chat id value: %s", chat_id)
                continue

            # Unpin previous if we stored one
            try:
                if 'PINNED_MESSAGE_IDS' in globals() and isinstance(PINNED_MESSAGE_IDS, dict) and int_chat in PINNED_MESSAGE_IDS:
                    prev_id = PINNED_MESSAGE_IDS.get(int_chat)
                    if prev_id:
                        try:
                            await bot.unpin_chat_message(chat_id=int_chat, message_id=prev_id)
                            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                                logger.debug("[PIN][UNPIN] chat=%s message_id=%s", int_chat, prev_id)
                        except Exception as _e:
                            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                                logger.debug("[PIN][UNPIN][ERR] chat=%s err=%s", int_chat, _e)
            except Exception:
                pass

            # Compose keyboard
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("➕ Add Your Token To Trending", url="https://t.me/SolanaTrendingList_Bot?start=addtoken")]
            ])

            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                logger.debug("[PIN][POST] chat=%s len=%s", int_chat, len(msg_text or ""))

            # Post and pin
            sent = await bot.send_message(
                chat_id=int_chat,
                text=msg_text,
                reply_markup=keyboard,
                parse_mode="HTML",
                disable_web_page_preview=True,
                disable_notification=True
            )
            try:
                await bot.pin_chat_message(chat_id=int_chat, message_id=sent.message_id, disable_notification=True)
                if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                    logger.debug("[PIN][PINNED] chat=%s message_id=%s", int_chat, sent.message_id)
            except Exception as _e:
                logger.warning("[PIN][PIN-ERR] chat=%s err=%s", int_chat, _e)

            # Remember for future unpin
            try:
                if 'PINNED_MESSAGE_IDS' not in globals():
                    globals()['PINNED_MESSAGE_IDS'] = {}
                PINNED_MESSAGE_IDS[int_chat] = sent.message_id
            except Exception as _e:
                if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                    logger.debug("[PIN][STORE-ID][ERR] chat=%s err=%s", int_chat, _e)

        except Exception as e:
            logger.warning("[PIN][POST-ERR] chat=%s err=%s", chat_id, e)
            # Continue to next chat even on failure
            continue

# === DEDUPLICATION FOR LIVE ALERTS ===
ALERTED_SIGNATURES = {}
ALERTS_POSTED_THIS_RUN = 0

# Track which mints have been pinned in this trending cycle
ALREADY_PINNED_MINTS = set()


def _compose_trending_text(
    promo,
    slot,
    pair,
    signature,
    tx_amount,
    tx_usd,
    *,
    token_units: float | None = None,
    hydrated: bool = True,
):
    """
    Compose the trending alert text in the requested style:
      <name> <${SYMBOL}>  💵  LIVE BUY!
      ────────────────────────────────────────
      Trending #<slot> (<dex>)
      🟢 <token_units SYMBOL> (≈<SOL> SOL, $<USD>)
      💲 Price: <priceNative> SOL ($<priceUsd>)
      💧 Liquidity: $<liq>
      🏛️ Market Cap: $<mcap>
      📊 24h Change: <chg>
      🔗 Latest Transaction
      ────────────────────────────────────────

    The ONLY part you should edit to change the separator line is the
    global `LIVE_BUY_SEPARATOR_LINE` near the top of the file. All buttons
    and layout stay exactly where they are.
    """
    token_name = promo.get("token_name") or promo.get("name") or promo.get("symbol") or "Token"
    symbol = (
        (promo.get("token_symbol") or promo.get("symbol") or
         ((pair or {}).get("baseToken") or {}).get("symbol") or "").strip()
    )
    if symbol:
        header_title = f"{token_name} &lt;${symbol}&gt;   💵  LIVE BUY!"
    else:
        header_title = f"{token_name}   💵  LIVE BUY!"

    dex_id = (pair or {}).get("dexId")
    dex_hint = f" ({dex_id})" if dex_id else ""
    line_header = f"Trending <b>#{slot}</b>{dex_hint}"

    # Price, liquidity, mcap, 24h change
    price_native, price_usd, liq_usd, mcap, chg_24h = get_token_stats_from_pair(pair or {})

    # Compute USD if missing using SOL/USD
    if (tx_amount is not None) and (tx_usd is None):
        sol_usd = None
        try:
            if 'get_sol_usd_price' in globals():
                sol_usd = float(get_sol_usd_price() or 0)
        except Exception:
            sol_usd = None
        if not sol_usd:
            try:
                if price_usd and price_native:
                    sol_usd = float(price_usd) / float(price_native)
            except Exception:
                sol_usd = None
        if sol_usd:
            try:
                tx_usd = float(tx_amount) * float(sol_usd)
            except Exception:
                tx_usd = None

    # Buy line to match screenshot strictly
    buy_line = None
    try:
        if token_units is not None:
            sym_show = (symbol or token_name).upper()
            units_str = format_float(token_units, 6)
            sol_str = ("—" if tx_amount is None else f"{format_float(tx_amount, 6)} SOL")
            usd_str = ("$—" if tx_usd is None else f"${format_float(tx_usd, 2)}")
            buy_line = f"🟢 {units_str} {sym_show} (≈{sol_str}, {usd_str})"
        elif (tx_amount is not None) or (tx_usd is not None):
            sol_part = f"{format_float(tx_amount, 6)} SOL" if tx_amount is not None else "— SOL"
            usd_part = f"${format_float(tx_usd, 2)}" if tx_usd is not None else "$—"
            buy_line = f"🟢 {sol_part} ({usd_part})"
    except Exception:
        buy_line = None

    # Price line
    try:
        if ('fmt_price_sol' in globals()) and ('fmt_usd' in globals()):
            p_sol_str = fmt_price_sol(price_native or 0)
            p_usd_str = fmt_usd(price_usd or 0)
            price_line = f"💲 Price: <b>{p_sol_str} SOL</b> (${p_usd_str})"
        else:
            price_line = f"💲 Price: <b>{format_float(price_native, 11)} SOL</b> (${format_float(price_usd, 7)})"
    except Exception:
        price_line = f"💲 Price: <b>{format_float(price_native, 11)} SOL</b> (${format_float(price_usd, 7)})"

    liq_line = f"💧 Liquidity: ${format_int(liq_usd)}" if liq_usd is not None else None
    mc_line  = f"🏛️ Market Cap: ${format_int(mcap)}" if mcap is not None else None
    chg_line = f"📊 24h Change: {chg_24h:+.2f}%" if isinstance(chg_24h, (int, float)) else None

    latest_tx_line = (
        f"🔗 <a href=\"https://solscan.io/tx/{signature}\">Latest Transaction</a>"
        if hydrated and _is_real_signature(signature) else None
    )

    # Separator: single configurable line for LIVE BUY alerts.
    # Single source of truth: global LIVE_BUY_SEPARATOR_LINE.
    # Optional env override: LIVEBUY_SEPARATOR_LINE.
    try:
        _sep_env = os.environ.get("LIVEBUY_SEPARATOR_LINE")
    except Exception:
        _sep_env = None
    separator = (_sep_env or LIVE_BUY_SEPARATOR_LINE)

    if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
        try:
            logger.debug("[ALERT][COMPOSE][SEP] width=%s preview=%s", len(separator), separator)
        except Exception:
            pass

    if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
        try:
            logger.debug(
                "[ALERT][COMPOSE] slot=%s sig=%s tok_units=%s amount_sol=%s amount_usd=%s priceNative=%s priceUsd=%s",
                slot,
                (signature[:8] if signature else None),
                token_units,
                tx_amount,
                tx_usd,
                price_native,
                price_usd,
            )
        except Exception:
            pass

    parts = [
        header_title,
        separator,
        line_header,
    ]
    if buy_line:
        parts.append(buy_line)
    parts.append(price_line)
    if liq_line:
        parts.append(liq_line)
    if mc_line:
        parts.append(mc_line)
    if chg_line:
        parts.append(chg_line)
    if latest_tx_line:
        parts.append(latest_tx_line)
    parts.append(separator)
    return "\n".join(parts)

async def _hydrate_and_edit_alert(
    application: Application,
    slot: int,
    mint_lc: str,
    signature: str | None,
):
    """
    After a delay, edit the previously sent alert message(s) to add the tx link.
    """
    await asyncio.sleep(HYDRATE_DELAY_SEC)

    promos = load_promotions() or {}
    promo = promos.get(str(slot)) or {}

    msgs = LAST_ALERT_MSGS.get(mint_lc, [])
    if not msgs:
        return

    # Reuse values from the most recent alert
    tx_amount   = msgs[0].get("tx_amount")
    tx_usd      = msgs[0].get("tx_usd")
    token_units = msgs[0].get("token_units")
    price_native = msgs[0].get("price_native")
    price_usd    = msgs[0].get("price_usd")

    # Fetch pair (for meta), but splice price hints back in so composer can use them
    pair_addr = promo.get("pair_address")
    pair = fetch_dexscreener_pair(pair_addr) if pair_addr else None
    if isinstance(pair, dict):
        try:
            if price_native is not None:
                pair.setdefault('priceNative', float(price_native))
            if price_usd is not None:
                pair.setdefault('priceUsd', float(price_usd))
        except Exception:
            pass

    # Only hydrate if real signature
    if not _is_real_signature(signature):
        return

    text = _compose_trending_text(
        promo, slot, pair, signature, tx_amount, tx_usd,
        token_units=token_units, hydrated=True
    )

    # Edit all sent messages for this alert
    for msg in msgs:
        chat_id = msg.get("chat_id")
        message_id = msg.get("message_id")
        if not chat_id or not message_id:
            continue
        try:
            await application.bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=text,
                parse_mode="HTML",
                disable_web_page_preview=True,
                reply_markup=msg.get("keyboard"),
            )
        except Exception as e:
            logger.warning(f"[HYDRATE] Failed to edit alert message {chat_id}/{message_id}: {e}")

async def send_trending_alert_for_slot(
    application: Application,
    slot: int,
    mint_lc: str,
    signature: str | None,
    logs: list[str] | None,
    tx_amount: float | None,
    tx_usd: float | None,
    *,
    price_native: float | None = None,   # SOL per token
    price_usd: float | None = None,      # USD per token
    token_amount: float | None = None,   # token units bought
) -> None:
    """
    LIVE BUY message (canonical):
      • Text is composed by _compose_trending_text (single source of truth)
      • Buttons: DEXscreener • DEXTools • Solscan(Token page) — single compact row
      • Deep-debug breadcrumbs throughout
    """
    try:
        # --- Load promo + pair -------------------------------------------------
        promos = load_promotions() or {}
        promo = promos.get(str(slot)) or {}
        token_name = promo.get("token_name") or promo.get("name") or "Token"
        pair_addr = promo.get("pair_address") or ""
        token_addr = promo.get("token_address") or (mint_lc or "")

        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            try:
                logger.debug(
                    "[LIVEBUY][BEGIN] slot=%s mint=%s sig=%s amt_sol=%s amt_usd=%s tok_units=%s px_sol=%s px_usd=%s",
                    slot, (mint_lc or '')[:12], (signature[:8] if signature else None),
                    tx_amount, tx_usd, token_amount, price_native, price_usd
                )
            except Exception:
                pass

        # Fetch pair once (for price + stats). Caller can hint prices; we still fetch for meta.
        pair = None
        try:
            if pair_addr or token_addr:
                pair = fetch_dexscreener_pair(pair_addr or token_addr)
        except Exception as e:
            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                logger.debug("[LIVEBUY][PAIR][ERR] slot=%s err=%s", slot, e)

        # If the caller provided price hints, splice them into the pair dict so that
        # _compose_trending_text can reuse them through get_token_stats_from_pair.
        try:
            if isinstance(pair, dict):
                if price_native is not None:
                    pair.setdefault('priceNative', float(price_native))
                if price_usd is not None:
                    pair.setdefault('priceUsd', float(price_usd))
        except Exception:
            pass

        # --- Compose via the canonical helper ---------------------------------
        hydrated_flag = _is_real_signature(signature)
        text = _compose_trending_text(
            promo,
            slot,
            pair,
            signature,
            tx_amount,
            tx_usd,
            token_units=token_amount,   # provide token units to the composer
            hydrated=hydrated_flag,
        )

        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            try:
                logger.debug("[LIVEBUY][COMPOSED][LEN] %s chars", len(text or ""))
            except Exception:
                pass

        # --- Build keyboard (single row, exact labels) -------------------------
        ds_url = f"https://dexscreener.com/solana/{pair_addr}" if pair_addr else None
        dt_url = f"https://www.dextools.io/app/en/solana/pair-explorer/{pair_addr}" if pair_addr else None
        # *** Change: Solscan button -> TOKEN PAGE (not tx) ***
        token_mint_for_btn = (token_addr or mint_lc or "").strip()
        sc_url = f"https://solscan.io/token/{token_mint_for_btn}?cluster=mainnet" if token_mint_for_btn else None

        row: list[InlineKeyboardButton] = []
        if ds_url: row.append(InlineKeyboardButton("DEXscreener ↗", url=ds_url))
        if dt_url: row.append(InlineKeyboardButton("DEXTools ↗", url=dt_url))
        if sc_url: row.append(InlineKeyboardButton("Solscan ↗", url=sc_url))
        kb = InlineKeyboardMarkup([row]) if row else None

        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            try:
                logger.debug("[LIVEBUY][BTN][ROW] count=%s labels=%s urls_sample=%s",
                             len(row), [b.text for b in row], [getattr(b, 'url', None) for b in row])
            except Exception:
                pass

        # --- Broadcast to all targets -----------------------------------------
        sent_any = False
        for chat_id in _expand_chat_targets(GROUP_CHAT_IDS):
            try:
                msg = await application.bot.send_message(
                    chat_id=chat_id,
                    text=text,
                    parse_mode="HTML",
                    disable_web_page_preview=True,
                    reply_markup=kb,
                )
                sent_any = True
                # Remember last alert per mint for later hydration edits
                try:
                    if 'LAST_ALERT_MSGS' not in globals():
                        globals()['LAST_ALERT_MSGS'] = {}
                    LAST_ALERT_MSGS.setdefault(mint_lc, [])
                    LAST_ALERT_MSGS[mint_lc].insert(0, {
                        'chat_id': chat_id,
                        'message_id': msg.message_id,
                        'keyboard': kb,
                        'tx_amount': tx_amount,
                        'tx_usd': tx_usd,
                        'token_units': token_amount,
                        'price_native': price_native,
                        'price_usd': price_usd,
                    })
                    if len(LAST_ALERT_MSGS[mint_lc]) > 5:
                        LAST_ALERT_MSGS[mint_lc] = LAST_ALERT_MSGS[mint_lc][:5]
                except Exception as _e:
                    if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                        logger.debug("[LIVEBUY][STORE][ERR] %s", _e)
                if signature:
                    _mark_posted_sig(signature)
                if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                    logger.debug(
                        "[LIVEBUY][POSTED] chat=%s slot=%s sig=%s text_len=%s",
                        chat_id, slot, (signature[:8] if signature else ''), len(text or "")
                    )
            except Exception as e:
                logger.warning("[LIVEBUY][POST-ERR] chat=%s err=%s", chat_id, e)

        if sent_any and 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[LIVEBUY][END] slot=%s mint=%s sig=%s", slot, (mint_lc or '')[:12], (signature[:8] if signature else None))
    except Exception as e:
        logger.warning("[LIVEBUY][ERR] %s", e)

# --- Deep debug toggle (env-controlled, safe default ON) ---
try:
    DEEP_DEBUG  # already defined somewhere else?
except NameError:
    try:
        import os as _os_dbg  # safe import if missing
    except Exception:
        _os_dbg = None
    _val = str((_os_dbg.getenv("DEEP_DEBUG", "1") if _os_dbg else "1")).strip().lower()
    DEEP_DEBUG = _val not in {"0", "false", "no"}

RPC_HTTPS = os.environ.get("SOL_RPC", f"https://mainnet.helius-rpc.com/?api-key={(globals().get('HELIUS_API_KEY') or os.environ.get('HELIUS_API_KEY') or '').strip()}")
RPC_WSS   = os.environ.get("SOL_WSS",  f"wss://mainnet.helius-rpc.com/?api-key={(globals().get('HELIUS_API_KEY') or os.environ.get('HELIUS_API_KEY') or '').strip()}")


# --- Standard Helius-compatible WS (logsSubscribe). Not Enhanced. ---
# We use Helius' Solana-compatible WS to subscribe to logs. This works on the standard plan.
# If DEX_PROGRAM_IDS is empty, we subscribe to "all" logs (heavier) and filter client-side.
USE_HELIUS_WS = True
HELIUS_WS_URL = (
    f"wss://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
    if HELIUS_API_KEY else None
)
HELIUS_WS_COMMITMENT = "confirmed"

async def _rpc_post(session: aiohttp.ClientSession, method: str, params: list):
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    async with session.post(RPC_HTTPS, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as r:
        return await r.json()

async def _fetch_tx_from_dexscreener(session: aiohttp.ClientSession, signature: str, watched_mints_lc: set[str]):
    """
    Try to resolve a swap by tx signature using Dexscreener's transactions endpoint.
    Returns tuple: (mint_lc | None, sol_amount | None, price_usd | None, price_native | None)
    We only return a mint if it matches one of the watched_mints_lc.
    """
    try:
        endpoints = [
            f"https://api.dexscreener.com/latest/dex/transactions/solana/{signature}",
            f"https://api.dexscreener.com/latest/dex/tx/solana/{signature}",
        ]
        for url in endpoints:
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                    if r.status != 200:
                        continue
                    data = await r.json()
            except Exception:
                continue

            # Dexscreener may return {"transactions":[{...}]} or {"transaction":{...}}
            tx = None
            if isinstance(data, dict):
                if isinstance(data.get("transaction"), dict):
                    tx = data.get("transaction")
                elif isinstance(data.get("transactions"), list) and data["transactions"]:
                    tx = data["transactions"][0]

            if not isinstance(tx, dict):
                continue

            def _lc(x):
                return (x or "").lower()

            base = (tx.get("baseToken") or {})
            quote = (tx.get("quoteToken") or {})
            base_mint = _lc(base.get("address"))
            quote_mint = _lc(quote.get("address"))

            sol_mint_lc = "so11111111111111111111111111111111111111112"

            mint_lc = None
            for m in [base_mint, quote_mint]:
                if m and m in watched_mints_lc:
                    mint_lc = m
                    break

            sol_amount = None
            try:
                def _to_float(v):
                    try:
                        return float(v)
                    except Exception:
                        return None

                if base_mint == sol_mint_lc:
                    sol_amount = _to_float(tx.get("amountBase") or tx.get("amountIn") or tx.get("baseAmount"))
                if sol_amount is None and quote_mint == sol_mint_lc:
                    sol_amount = _to_float(tx.get("amountQuote") or tx.get("amountOut") or tx.get("quoteAmount"))
            except Exception:
                sol_amount = None

            price_usd = None
            price_native = None
            try:
                pu = tx.get("priceUsd")
                pn = tx.get("priceNative")
                price_usd = float(pu) if pu is not None else None
                price_native = float(pn) if pn is not None else None
            except Exception:
                price_usd = price_usd or None
                price_native = price_native or None

            if mint_lc:
                return mint_lc, sol_amount, price_usd, price_native

        return None, None, None, None
    except Exception:
        return None, None, None, None

async def _helius_parse_signature(session: aiohttp.ClientSession, signature: str):
    """
    Resolve a transaction via Helius HTTP and return (token_transfers, signature).
    token_transfers is a list of dicts like {"mint": "...", "tokenAmount": float, "toUserAccount": "..."}.
    Works on Standard plan. Falls back to deriving from accountData if tokenTransfers absent.
    """
    try:
        url = f"https://api.helius.xyz/v0/transactions/?api-key={HELIUS_API_KEY}"
        payload = {"transactions": [signature]}
        async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status != 200:
                raise RuntimeError(f"HTTP {r.status}")
            arr = await r.json()
    except Exception as e:
        raise RuntimeError(f"Helius tx fetch failed: {e}")

    if not isinstance(arr, list) or not arr:
        return [], signature

    tx = arr[0] or {}
    token_transfers = tx.get("tokenTransfers") or []

    # Derive from accountData if tokenTransfers is missing (Standard plan)
    if not token_transfers:
        derived = []
        for acc in (tx.get("accountData") or []):
            for ch in (acc.get("tokenBalanceChanges") or []):
                mint_cc = ch.get("mint")
                raw = ch.get("rawTokenAmount") or {}
                amt = None
                try:
                    dec = int(raw.get("decimals", 0) or 0)
                    raw_amt = raw.get("tokenAmount")
                    if raw_amt is not None:
                        amt = float(raw_amt) / (10 ** dec)
                except Exception:
                    amt = None
                derived.append({
                    "mint": mint_cc,
                    "tokenAmount": amt,
                    "toUserAccount": ch.get("userAccount"),
                })
        token_transfers = derived

    # Normalize to expected shape
    norm = []
    for tt in (token_transfers or []):
        if not isinstance(tt, dict):
            continue
        m = tt.get("mint")
        amt = tt.get("tokenAmount")
        to = tt.get("toUserAccount") or tt.get("toUserAccountOwner") or None
        if amt is None:
            raw = tt.get("rawTokenAmount") or {}
            try:
                dec = int(raw.get("decimals", 0) or 0)
                raw_amt = raw.get("tokenAmount")
                if raw_amt is not None:
                    amt = float(raw_amt) / (10 ** dec)
            except Exception:
                amt = None
        norm.append({"mint": m, "tokenAmount": amt, "toUserAccount": to})
    return norm, signature


async def _fetch_tx_amount(session: aiohttp.ClientSession, signature: str, watched_mints_lc: set[str]):
    """Fallback: query Helius enhanced parse API to resolve (mint, SOL amount) by signature.
    Returns (mint_lc | None, sol_amount | None). Used when Dexscreener tx endpoint has no data.
    """
    try:
        if HELIUS_API_KEY:
            url = f"https://api.helius.xyz/v0/transactions/?api-key={HELIUS_API_KEY}"
            async with session.post(url, json={"transactions": [signature]}, timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status == 200:
                    arr = await r.json()
                    if isinstance(arr, list) and arr:
                        m = None
                        sol_seen = []
                        for tt in (arr[0].get("tokenTransfers") or []):
                            mint = (tt.get("mint") or "").lower()
                            if mint and mint in watched_mints_lc:
                                m = mint
                            if (tt.get("mint") or "").lower() == "so11111111111111111111111111111111111111112":
                                try:
                                    sol_seen.append(abs(float(tt.get("tokenAmount") or 0)))
                                except Exception:
                                    pass
                        if m:
                            return m, (max(sol_seen) if sol_seen else None)
        return None, None
    except Exception:
        return None, None

async def _fetch_tx_amount_via_rpc(session: aiohttp.ClientSession, signature: str, watched_mints_lc: set[str]):
    """
    Fallback #3: plain RPC getTransaction (jsonParsed).
    Attempts to infer which watched mint participated in the swap and the SOL (wSOL) leg size.
    Returns (mint_lc | None, sol_amount | None).
    """
    try:
        params = [
            signature,
            {
                "encoding": "jsonParsed",
                "maxSupportedTransactionVersion": 0,
            },
        ]
        data = await _rpc_post(session, "getTransaction", params)
        result = (data or {}).get("result") or {}
        if not isinstance(result, dict):
            return None, None

        meta = result.get("meta") or {}
        pre_tb = meta.get("preTokenBalances") or []
        post_tb = meta.get("postTokenBalances") or []

        def _ui_amount(tb_item):
            if not isinstance(tb_item, dict):
                return None
            ui = (tb_item.get("uiTokenAmount") or {}).get("uiAmount")
            try:
                return float(ui) if ui is not None else None
            except Exception:
                return None

        pre_by_idx = {}
        for b in pre_tb:
            try:
                pre_by_idx[b.get("accountIndex")] = ((b.get("mint") or "").lower(), _ui_amount(b))
            except Exception:
                continue

        post_by_idx = {}
        for b in post_tb:
            try:
                post_by_idx[b.get("accountIndex")] = ((b.get("mint") or "").lower(), _ui_amount(b))
            except Exception:
                continue

        indices = set(pre_by_idx.keys()) | set(post_by_idx.keys())
        watched_mint_lc = None
        wsol_delta_abs_max = None

        WSOL = "so11111111111111111111111111111111111111112"

        for idx in indices:
            pre_mint, pre_amt = pre_by_idx.get(idx, (None, None))
            post_mint, post_amt = post_by_idx.get(idx, (None, None))
            mint_lc = (post_mint or pre_mint or "").lower()
            if not mint_lc:
                continue

            delta = None
            try:
                if (pre_amt is not None) or (post_amt is not None):
                    pre_val = pre_amt or 0.0
                    post_val = post_amt or 0.0
                    delta = post_val - pre_val
            except Exception:
                delta = None

            if mint_lc in watched_mints_lc and delta is not None and abs(delta) > 0:
                watched_mint_lc = mint_lc

            if mint_lc == WSOL and delta is not None:
                try:
                    mag = abs(float(delta))
                    if (wsol_delta_abs_max is None) or (mag > wsol_delta_abs_max):
                        wsol_delta_abs_max = mag
                except Exception:
                    pass

        if (not watched_mint_lc) or (wsol_delta_abs_max is None):
            acct_data = meta.get("tokenBalanceChanges") or []
            for ch in acct_data:
                try:
                    mint = (ch.get("mint") or "").lower()
                    if mint in watched_mints_lc:
                        watched_mint_lc = mint
                    if mint == WSOL:
                        raw = ch.get("rawTokenAmount") or {}
                        dec = int(raw.get("decimals") or 0)
                        amt = raw.get("tokenAmount")
                        if amt is not None:
                            val = abs(float(amt) / (10 ** dec))
                            if (wsol_delta_abs_max is None) or (val > wsol_delta_abs_max):
                                wsol_delta_abs_max = val
                except Exception:
                    continue

        return watched_mint_lc, wsol_delta_abs_max
    except Exception:
        return None, None

# Dedupe set so each WS signature is processed once
SEEN_WS_SIGS: set[str] = set()

async def watch_swaps_over_ws():
    """
    Public RPC WebSocket watcher (DEX logs only):
      - Subscribes to logs (program mentions or ALL)
      - For each signature, tries (in order):
        1) Dexscreener TX endpoint
        2) Helius enhanced parse API
        3) Plain RPC getTransaction (jsonParsed)
      - If it matches a watched mint, sends an alert *with* tx_amount and token_units
    """
    try:
        import websockets  # type: ignore
    except Exception as e:
        logger.error("[WS] The 'websockets' package is not available: %s", e)
        logger.error("[WS] Install it with: pip install websockets")
        return

    if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
        logger.debug("[WS] watch_swaps_over_ws starting (SUBSCRIBE_ALL_LOGS=%s)", SUBSCRIBE_ALL_LOGS)

    if not SUBSCRIBE_ALL_LOGS and not DEX_PROGRAM_IDS:
        logger.info("[WS] No DEX program IDs configured and not using all-logs; websocket watcher disabled.")
        return

    while True:
        try:
            logger.info("[WS] Connecting to %s ...", RPC_WSS)
            async with websockets.connect(
                RPC_WSS,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=10,
                max_queue=1000,
            ) as ws:
                # --- Guarded subscribe frames (single-key maps; no per-connection dups) ---
                try:
                    _sub_next_id = 1
                    def _next_id():
                        nonlocal _sub_next_id
                        i = _sub_next_id
                        _sub_next_id += 1
                        return i

                    mentions = list({str(k) for k in (DEX_PROGRAM_IDS or {}).keys() if k})
                    if mentions:
                        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                            logger.debug("[WS][SUBSCRIBE] using mentions filter (%d ids)", len(mentions))
                        payload = {
                            "jsonrpc": "2.0",
                            "id": _next_id(),
                            "method": "logsSubscribe",
                            "params": [
                                {"mentions": mentions},
                                {"commitment": HELIUS_WS_COMMITMENT},
                            ],
                        }
                        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                            logger.debug("[WS][->] logsSubscribe payload=%s", payload)
                        await ws.send(json.dumps(payload))
                    elif SUBSCRIBE_ALL_LOGS:
                        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                            logger.debug("[WS][SUBSCRIBE] using filter=all")
                        payload = {
                            "jsonrpc": "2.0",
                            "id": _next_id(),
                            "method": "logsSubscribe",
                            "params": [
                                {"filter": "all"},
                                {"commitment": HELIUS_WS_COMMITMENT},
                            ],
                        }
                        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                            logger.debug("[WS][->] logsSubscribe (all) payload=%s", payload)
                        await ws.send(json.dumps(payload))
                    else:
                        logger.info("[WS] No logsSubscribe sent (SUBSCRIBE_ALL_LOGS=False and no DEX_PROGRAM_IDS).")

                    logger.info("[WS] Subscribed (logs).")
                except Exception as _e:
                    logger.warning("[WS][SUBSCRIBE-ERR] Failed to send subscribe messages: %s", _e)

                async with aiohttp.ClientSession() as session:
                    while True:
                        raw = await ws.recv()
                        try:
                            data = json.loads(raw)
                        except Exception:
                            continue

                        if not isinstance(data, dict):
                            continue
                        params = data.get("params") or {}
                        if not params:
                            continue
                        value = (params.get("result") or {}).get("value") or {}
                        sig = value.get("signature")
                        if not sig:
                            continue

                        # Pump.fun frame detection (standard WS gives logs only)
                        logs_list = value.get("logs") or []
                        is_pump_frame = False
                        try:
                            pump_pid = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
                            for line in logs_list:
                                if not isinstance(line, str):
                                    continue
                                low = line.lower()
                                if (pump_pid in line) or ("pump.fun" in low) or ("initializemint2" in low):
                                    is_pump_frame = True
                                    break
                        except Exception:
                            is_pump_frame = False

                        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG and is_pump_frame:
                            try:
                                logger.debug("[WS][PUMP][FRAME] sig=%s logs_count=%s", sig[:8], len(logs_list))
                            except Exception:
                                pass

                        # Dedupe
                        if sig in SEEN_WS_SIGS:
                            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                                logger.debug("[WS][DEDUP] Skipping already-seen sig=%s", sig[:8])
                            continue
                        SEEN_WS_SIGS.add(sig)
                        if len(SEEN_WS_SIGS) > 10000:
                            SEEN_WS_SIGS.clear()

                        # Gather watched mints
                        try:
                            promos = load_promotions() or {}
                            watched = {
                                (p.get("token_address") or "").lower()
                                for p in promos.values() if p.get("token_address")
                            }
                        except Exception:
                            watched = set()
                        if not watched:
                            continue

                        # (1) Dexscreener TX by signature
                        mint_lc, sol_amt, price_usd_snap, price_native_snap = await _fetch_tx_from_dexscreener(
                            session, sig, watched
                        )

                        # (2) Helius parse
                        if not mint_lc:
                            mint_lc, sol_amt = await _fetch_tx_amount(session, sig, watched)
                            price_usd_snap, price_native_snap = None, None

                        # (3) Plain RPC jsonParsed
                        if not mint_lc:
                            rpc_mint, rpc_sol_amt = await _fetch_tx_amount_via_rpc(session, sig, watched)
                            if rpc_mint:
                                mint_lc = rpc_mint
                                if sol_amt is None:
                                    sol_amt = rpc_sol_amt

                        if not mint_lc:
                            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                                logger.debug("[WS] Signature %s did not match any watched mint after fallbacks.", sig)
                            continue

                        # USD (if we can infer SOL/USD from the snapshot)
                        tx_usd_immediate = None
                        try:
                            if sol_amt is not None and price_usd_snap and price_native_snap and price_native_snap != 0:
                                sol_usd_rate = float(price_usd_snap) / float(price_native_snap)
                                tx_usd_immediate = float(sol_amt) * sol_usd_rate
                        except Exception:
                            tx_usd_immediate = None

                        # Token units (SOL per token -> divide)
                        token_units = None
                        try:
                            if (sol_amt is not None) and price_native_snap and float(price_native_snap) > 0:
                                token_units = float(sol_amt) / float(price_native_snap)
                        except Exception:
                            token_units = None

                        # Map mint -> configured slot
                        try:
                            slot_id = next(
                                (
                                    int(s)
                                    for s, p in (promos or {}).items()
                                    if (p.get("token_address") or "").lower() == mint_lc
                                ),
                                None,
                            )
                        except Exception:
                            slot_id = None
                        if slot_id is None:
                            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                                logger.debug("[WS] Could not map mint %s to a slot.", mint_lc)
                            # Pump.fun fallback: logs-only frame where generic path failed to map mint->slot
                            if is_pump_frame:
                                try:
                                    if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                                        logger.debug("[WS][PUMP][FALLBACK] sig=%s invoking _handle_pump_fun_log()", sig[:8])
                                    asyncio.run_coroutine_threadsafe(
                                        _handle_pump_fun_log(application, sig),
                                        MAIN_EVENT_LOOP,
                                    )
                                except Exception as e:
                                    logger.warning("[WS][PUMP][FALLBACK-ERR] sig=%s err=%s", sig[:8], e)
                            continue

                        logger.info("[WS] Live swap matched mint=%s sig=%s sol=%s tok_units=%s",
                                    mint_lc, sig, sol_amt, token_units)
                        try:
                            asyncio.run_coroutine_threadsafe(
                                send_trending_alert_for_slot(
                                    application,
                                    slot_id,
                                    mint_lc,
                                    signature=sig,
                                    logs=[],
                                    tx_amount=sol_amt,              # drives the Buy Amount line
                                    tx_usd=tx_usd_immediate,
                                    price_native=price_native_snap,
                                    price_usd=price_usd_snap,
                                    token_amount=token_units,
                                ),
                                MAIN_EVENT_LOOP,
                            )
                        except Exception as e:
                            logger.warning("[WS] Could not schedule alert: %s", e)

        except Exception as e:
            logger.warning("[WS] Websocket error: %s. Reconnecting in 5s...", e)
            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                logger.debug("[WS][RETRY] after error; sleeping 5s")
            await asyncio.sleep(5)
        finally:
            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                logger.debug("[WS] loop recycle (finally)")

def _extract_sol_amount_from_token_transfers(token_transfers):
    """
    From a Helius enhanced SWAP 'tokenTransfers' array, return the max absolute SOL amount (float) if present,
    scanning for the wrapped SOL mint. Returns None if not found or not parseable.
    """
    try:
        if not isinstance(token_transfers, list):
            return None
        sol_mint_lc = "so11111111111111111111111111111111111111112"
        seen = []
        for tt in token_transfers:
            try:
                mint = (tt.get("mint") or "").lower()
                if mint != sol_mint_lc:
                    continue
                amt = tt.get("tokenAmount")
                if amt is None:
                    continue
                seen.append(abs(float(amt)))
            except Exception:
                continue
        if seen:
            return max(seen)
        return None
    except Exception:
        return None

def _infer_tx_usd_from_sol(sol_amount: float | None) -> float | None:
    """
    Convert a SOL amount to USD using the current SOL price (if available).
    """
    try:
        if sol_amount is None:
            return None
        sol_price = None
        if 'get_sol_usd_price' in globals():
            sol_price = float(get_sol_usd_price() or 0)
        elif 'get_sol_price_usd' in globals():
            sol_price = float(get_sol_price_usd() or 0)
        if not sol_price:
            return None
        return float(sol_amount) * sol_price
    except Exception:
        return None


# ==== ENABLE/DISABLE VIA BUTTONS ====
async def enable_bookings_button(update: Update, context: CallbackContext):
    user_id = update.effective_user.id
    if user_id not in ADMIN_USER_IDS:
        await update.callback_query.answer("No permission.", show_alert=True)
        return ConversationHandler.END
    set_bookings_enabled(True)
    logger.info(f"Bookings ENABLED by admin {user_id}")
    logger.info(f"After button: is_bookings_enabled() = {is_bookings_enabled()}")
    await update.callback_query.answer("Bookings ENABLED.", show_alert=True)
    async with promo_lock:
        promos = clear_expired_promotions()
        keyboard = build_slot_inline_keyboard(promos, user_id)
        try:
            await update.callback_query.edit_message_reply_markup(reply_markup=keyboard)
        except Exception as e:
            logger.warning(f"Could not edit message reply markup: {e}")
    for chat_id in GROUP_CHAT_IDS:
        async with promo_lock:
            promos = clear_expired_promotions()
            keyboard = build_slot_inline_keyboard(promos, user_id)
            try:
                await context.bot.send_message(
                    chat_id=int(chat_id),
                    text="Slots have been updated!\n\nNon-admin users: please /start again to see bookable slots." if is_bookings_enabled() else "Slots have been updated!\n\nAll slots are unavailable until enabled.",
                    reply_markup=keyboard,
                    parse_mode="Markdown"
                )
            except Exception as err:
                logger.error(f"Failed to send refreshed slots UI after enabling/disabling bookings: {err}")
    return CHOOSE_SLOT

async def disable_bookings_button(update: Update, context: CallbackContext):
    user_id = update.effective_user.id
    if user_id not in ADMIN_USER_IDS:
        await update.callback_query.answer("No permission.", show_alert=True)
        return ConversationHandler.END
    set_bookings_enabled(False)
    logger.info(f"After button: is_bookings_enabled() = {is_bookings_enabled()}")
    await update.callback_query.answer("Bookings DISABLED.", show_alert=True)
    async with promo_lock:
        promos = clear_expired_promotions()
        keyboard = build_slot_inline_keyboard(promos, user_id)
        try:
            await update.callback_query.edit_message_reply_markup(reply_markup=keyboard)
        except Exception as e:
            try:
                await update.callback_query.message.reply_text(
                    "Bookings have been DISABLED.\n\nAll slots are unavailable.",
                    reply_markup=keyboard,
                    parse_mode="Markdown"
                )
            except Exception as err:
                logger.error(f"Failed to send refreshed bookings UI: {err}")
    for chat_id in GROUP_CHAT_IDS:
        async with promo_lock:
            promos = clear_expired_promotions()
            keyboard = build_slot_inline_keyboard(promos, user_id)
            try:
                await context.bot.send_message(
                    chat_id=int(chat_id),
                    text="Slots have been updated!\n\nNon-admin users: please /start again to see bookable slots." if is_bookings_enabled() else "Slots have been updated!\n\nAll slots are unavailable until enabled.",
                    reply_markup=keyboard,
                    parse_mode="Markdown"
                )
            except Exception as err:
                logger.error(f"Failed to send refreshed slots UI after enabling/disabling bookings: {err}")
    return CHOOSE_SLOT

def main():
    """Boot the app, start WS watchers, and start Telegram polling (WS-only).
    - Applies Windows event loop policy for websockets compatibility.
    - Starts WebSocket-only listeners (no HTTP listeners).
    - Schedules periodic jobs (cleanup + Dexscreener polling + payment poll).
    - Starts Telegram bot polling.
    """
    import socket  # noqa: F401
    import sys

    # --- Windows event loop compatibility ---
    try:
        if sys.platform.startswith("win"):
            import asyncio as _asyncio_winfix
            _asyncio_winfix.set_event_loop_policy(_asyncio_winfix.WindowsSelectorEventLoopPolicy())
            logger.info("[BOOT] WindowsSelectorEventLoopPolicy applied.")
    except Exception as e:
        logger.warning(f"[BOOT] Could not apply Windows event-loop policy: {e}")

    # --- Startup configuration report ---
    try:
        startup_config_report()
    except Exception as e:
        logger.warning(f"startup_config_report failed: {e}")

    if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
        try:
            logger.debug(
                "[BOOT] main() starting. USE_HELIUS_WS=%s HELIUS_WS_URL=%s BUY_CHECK_INTERVAL=%s",
                USE_HELIUS_WS,
                HELIUS_WS_URL,
                BUY_CHECK_INTERVAL
            )
        except Exception:
            pass

    # === ConversationHandler ===
    conv = ConversationHandler(
        entry_points=[CommandHandler('start', start)],
        states={
            CHOOSE_SLOT: [
                CallbackQueryHandler(choose_slot_callback, pattern=r"^slot:\d+$"),
                CommandHandler('cancel', cancel),
            ],
            GET_DURATION: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, get_duration),
                CommandHandler('cancel', cancel),
            ],
            GET_ADDRESS: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, get_address),
                CommandHandler('cancel', cancel),
            ],
            GET_TOKEN_NAME: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, get_token_name),
                CommandHandler('cancel', cancel),
            ],
            CONFIRM: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, confirm),
                CommandHandler('cancel', cancel),
            ],
        },
        fallbacks=[CommandHandler('cancel', cancel)],
        allow_reentry=True,
    )

    # === Register handlers ===
    application.add_handler(conv)
    application.add_handler(CommandHandler('cancel', cancel))

    # Admin “Remove Trending”
    application.add_handler(
        CallbackQueryHandler(admin_remove_button, pattern=r"^ADMIN_REMOVE$", block=False),
        group=0,
    )
    application.add_handler(
        CallbackQueryHandler(admin_remove_select, pattern=r"^ADMIN_REMOVE_SLOT:\d+$", block=False),
        group=0,
    )
    if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
        logger.debug("[BOOKINGS][REG] Admin remove handlers registered")

    # === Global error handler ===
    application.add_error_handler(error_handler)

    # === Scheduled jobs ===
    application.job_queue.run_repeating(
        clear_expired_promotions_job,
        interval=3600,
        first=5,
        name="clear_expired_promotions_job",
    )

    application.job_queue.run_repeating(
        dexscreener_poll_once,
        interval=BUY_CHECK_INTERVAL,
        first=10,
        name="dexscreener_poll_once",
    )

    # 🚨 PAYMENT POLL JOB — BACKUP PAYMENT DETECTION
    try:
        application.job_queue.run_repeating(
            _payment_poll_job,
            interval=30,
            first=15,
            name="payment-poll",
        )
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[PAY][POLL][SCHED] _payment_poll_job scheduled (30s interval).")
    except Exception as e:
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[PAY][POLL][SCHED-ERR] %s", e)

    # --- WS scheduling guard ---
    if 'WS_SCHEDULED' not in globals():
        global WS_SCHEDULED
        WS_SCHEDULED = False

    # === post_init loop capture ===
    async def _capture_loop_and_schedule(_ctx):
        global MAIN_EVENT_LOOP, WS_SCHEDULED
        try:
            MAIN_EVENT_LOOP = asyncio.get_running_loop()
            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                logger.debug("[BOOT] Captured PTB running loop: %s", MAIN_EVENT_LOOP)

            if not WS_SCHEDULED:
                if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                    logger.debug("[WS] Skipping public RPC WS watcher (WS-only mode).")
                WS_SCHEDULED = True
            else:
                if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                    logger.debug("[WS] WS watcher already scheduled earlier.")
        except Exception as e:
            logger.warning(f"[BOOT] Failed to capture loop/schedule watchers: {e}")

    try:
        application.post_init = _capture_loop_and_schedule
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[BOOT] post_init hook registered")
    except Exception as e:
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[BOOT] Could not register post_init hook: %s", e)

    # === Start bot ===
    if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
        try:
            logger.debug("[BOOT] Starting application.run_polling()  (WS-only). URL=%s", HELIUS_WS_URL)
        except Exception:
            logger.debug("[BOOT] Starting application.run_polling()  (WS-only).")
    application.run_polling()



if __name__ == "__main__":
    try:
        cleanup_expired_promotions_now()
    except Exception as e:
        logger.warning(f"[BOOT] cleanup_expired_promotions_now failed: {e}")
    main()
# --- Pump.fun-specific log handler (hydration; canonical LIVE BUY path) ---
async def _handle_pump_fun_log(app: Application, signature: str | None) -> None:
    """
    Pump.fun-specific LIVE BUY handler.

    Called when a logsNotification frame mentions the Pump.fun program id but does
    not contain a direct Token Program Transfer log. We:
      - hydrate the transaction once via getTransaction(jsonParsed)
      - find any mint that matches an actively promoted token whose address ends with 'pump'
      - route the event through `_post_live_buy_direct` so formatting and gating stay identical
    """
    if not signature or not isinstance(signature, str) or len(signature) < 8:
        return

    # Build watched mint -> slot map, but only for pump-style token addresses
    promos = load_promotions() or {}
    watched: dict[str, int] = {}
    for s, p in promos.items():
        try:
            mint = str(p.get("token_address") or "").strip().lower()
            if not mint:
                continue
            # Only consider pump-style tokens here to avoid double-processing
            if mint.endswith("pump"):
                watched[mint] = int(s)
        except Exception:
            continue

    if not watched:
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[WS][PUMP][SKIP] No active pump tokens in promotions; signature=%s", (signature[:8] if signature else "?"))
        return

    # Hydrate the transaction once to discover which promoted pump mint (if any) it touches
    try:
        rpc_url = str(
            globals().get("RPC_HTTPS")
            or f"https://mainnet.helius-rpc.com/?api-key={(globals().get('HELIUS_API_KEY') or os.environ.get('HELIUS_API_KEY') or '').strip()}"
        )
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getTransaction",
            "params": [signature, {
                "encoding": "jsonParsed",
                "maxSupportedTransactionVersion": 2,
                "commitment": str(globals().get("HELIUS_WS_COMMITMENT") or "confirmed"),
            }],
        }
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            try:
                logger.debug("[WS][PUMP][HYDRATE] getTransaction(jsonParsed) for sig=%s", (signature[:8] if signature else "?"))
            except Exception:
                pass
        data = _post_json_with_backoff(rpc_url, payload, max_retries=1, base_sleep=0.25, timeout=6.0)
        tx = (data or {}).get("result") or {}
        meta = tx.get("meta") or {}
    except Exception as e:
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            try:
                logger.debug("[WS][PUMP][HYDRATE-ERR] sig=%s err=%s", (signature[:8] if signature else "?"), e)
            except Exception:
                pass
        return

    # Collect all mints touched in this transaction from token balance arrays
    touched_mints: set[str] = set()
    try:
        for b in (meta.get("preTokenBalances") or []):
            m = str((b or {}).get("mint") or "").strip().lower()
            if m:
                touched_mints.add(m)
        for b in (meta.get("postTokenBalances") or []):
            m = str((b or {}).get("mint") or "").strip().lower()
            if m:
                touched_mints.add(m)
    except Exception:
        pass

    # Find the first promoted pump mint involved in this tx
    target_mint = None
    for m in touched_mints:
        if m in watched:
            target_mint = m
            break

    if not target_mint:
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            try:
                logger.debug("[WS][PUMP][MISS] sig=%s touched_mints=%s", (signature[:8] if signature else "?"), list(touched_mints)[:4])
            except Exception:
                pass
        return

    slot = watched.get(target_mint)
    if slot is None:
        return

    if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
        try:
            logger.debug("[WS][PUMP][HIT] sig=%s mint=%s slot=%s", (signature[:8] if signature else "?"), target_mint, slot)
        except Exception:
            pass

    # Route through the canonical LIVE BUY poster so output + gating stay identical
    try:
        await _post_live_buy_direct(
            app=app,
            slot=slot,
            mint_lc=target_mint,
            signature=signature,
            sol_amount=None,
            usd_amount=None,
        )
        if signature:
            _mark_posted_sig(signature)
    except Exception as e:
        logger.warning("[WS][PUMP][HANDLE] Failed to send pump.fun live-buy alert: %s", e)
# --- Pump.fun-specific live-buy handler (hydration path) ---
async def _handle_pump_fun_event(app: Application, signature: str | None) -> None:
    """
    Pump.fun-specific live-buy handler.

    Triggered from the logsSubscribe(mentions=[PUMP_FUN_PROGRAM_ID]) path. For each Pump.fun
    transaction, we:
      - hydrate the transaction once via getTransaction(jsonParsed),
      - extract the first SPL transfer mint,
      - check whether that mint is one of the currently-booked trending tokens,
      - and, if so, route the event through the same `_post_live_buy_direct` adapter used
        everywhere else so formatting and gating stay identical.

    This avoids relying on per-slot Token Program deltas, which can miss Pump bonding-curve
    flows, while keeping the existing non-Pump live-buy logic untouched.
    """
    # Basic sanity and post-level dedup (avoid double Telegram posts for the same tx)
    try:
        if not signature or not isinstance(signature, str) or len(signature) < 8:
            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                logger.debug("[WS][PUMP][SKIP] No usable signature in Pump.fun handler.")
            return
    except Exception:
        return

    # If we've already posted a LIVE BUY for this signature, skip.
    try:
        if _already_posted_sig(signature):
            if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
                logger.debug("[WS][PUMP][SKIP] Already posted LIVE BUY for sig=%s", signature[:8])
            return
    except Exception:
        # If dedup helper is unavailable for any reason, fail open (better duplicate than miss)
        pass

    # Build the set of currently-watched mints (lowercased) from active promotions.
    try:
        promos = load_promotions() or {}
    except Exception:
        promos = {}

    watched: dict[str, int] = {}
    for s, p in (promos or {}).items():
        try:
            mint_lc = str(p.get("token_address") or "").strip().lower()
            if not mint_lc:
                continue
            slot_id = int(s)
            watched[mint_lc] = slot_id
        except Exception:
            continue

    if not watched:
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[WS][PUMP][SKIP] No active promotions when handling Pump.fun tx %s", signature[:8])
        return

    if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
        try:
            logger.debug("[WS][PUMP][WATCHED] %d mint(s) currently trending.", len(watched))
        except Exception:
            pass

    # Hydrate once via RPC to get the SPL transfer mint and amount.
    try:
        mint, amt = _rpc_get_spl_transfer_amount(signature)
    except Exception as e:
        mint, amt = None, None
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[WS][PUMP][HYDRATE-ERR] sig=%s err=%s", signature[:8], e)

    mint_lc = str(mint or "").strip().lower()
    if not mint_lc:
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug("[WS][PUMP][SKIP] getTransaction did not yield a usable SPL mint for sig=%s", signature[:8])
        return

    # Only proceed if this mint is one of the booked/trending tokens.
    if mint_lc not in watched:
        if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
            logger.debug(
                "[WS][PUMP][SKIP] Hydrated mint %s for sig=%s is not in watched promos.",
                mint_lc,
                signature[:8],
            )
        return

    slot_id = watched[mint_lc]
    if 'DEEP_DEBUG' in globals() and DEEP_DEBUG:
        try:
            logger.debug(
                "[WS][PUMP][MATCHED] sig=%s mint=%s slot=%s (amt_ui=%s)",
                signature[:8],
                mint_lc,
                slot_id,
                (None if amt is None else f"{float(amt):.8f}"),
            )
        except Exception:
            pass

    # Route through the canonical LIVE BUY poster so formatting and gating remain consistent.
    try:
        await _post_live_buy_direct(
            app=app,
            slot=slot_id,
            mint_lc=mint_lc,
            signature=signature,
            token_units=None,   # let the precise hydrate inside _post_live_buy_direct compute this
            sol_amount=None,
            usd_amount=None,
        )
        try:
            _mark_posted_sig(signature)
        except Exception:
            # Best-effort post-level dedup; non-fatal if it fails.
            pass
    except Exception as e:
        logger.warning("[WS][PUMP][HANDLE] Failed to send Pump.fun live-buy alert for %s: %s", signature[:8], e)
