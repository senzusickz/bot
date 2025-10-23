import asyncio
import os
import shutil
import threading
import unicodedata
import re
from typing import Callable, Iterable, Optional, Union, Awaitable

import aiofiles
import discord
from discord import Interaction, Embed
from discord.ext import commands

try:
    from config import ALLOWED_USERS, ALLOWED_ROLES
except Exception:
    ALLOWED_USERS: Iterable[int] = ()
    ALLOWED_ROLES: Iterable[int] = ()

_WHITESPACE_RE = re.compile(r"\s+", re.UNICODE)

def rating_to_stars(rating: Union[int, float]) -> str:
    try:
        r = int(round(float(rating)))
    except (TypeError, ValueError):
        r = 0
    r = max(0, min(5, r))
    return "★" * r + "☆" * (5 - r)


def normalize_whitespace(text: Optional[str]) -> str:
    if not text:
        return ""
    t = unicodedata.normalize("NFKC", text).strip()
    t = _WHITESPACE_RE.sub(" ", t)
    return t


def now_ms() -> int:
    return int(asyncio.get_event_loop().time() * 1000)


async def ensure_dirs(path: str) -> None:
    """Async-safe directory creation (non-blocking in event loop)."""
    def _mk():
        os.makedirs(path, exist_ok=True)
    await asyncio.to_thread(_mk)


async def atomic_write_bytes(path: str, data: bytes) -> None:
    """Write bytes to a temp file then atomically move into place (async-safe)."""
    base_dir = os.path.dirname(path) or "."
    await ensure_dirs(base_dir)

    tmp_path = f"{path}.tmp"
    async with aiofiles.open(tmp_path, "wb") as f:
        await f.write(data)

    def _replace():
        os.replace(tmp_path, path)
    await asyncio.to_thread(_replace)


async def save_attachment(attachment: discord.Attachment, path: str) -> None:
    """Save a Discord attachment to a path (async)."""
    data = await attachment.read()
    await atomic_write_bytes(path, data)


def sanitize_filename(name: str) -> str:
    name = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode("ascii")
    name = re.sub(r"[^\w\-.]+", "_", name, flags=re.UNICODE)
    return name.strip("._") or "file"


def images_base_dir() -> str:
    return os.path.join("data", "images")


def make_images_path(guild_id: Union[int, str], user_id: Union[int, str], *subdirs: str) -> str:
    """
    Preserve the historical structure under data/images/<guild_id>/<user_id>/...
    """
    gid = str(guild_id)
    uid = str(user_id)
    return os.path.join(images_base_dir(), gid, uid, *subdirs)

def _has_allowed_role(member: discord.Member) -> bool:
    if not member or not hasattr(member, "roles"):
        return False
    role_ids = {int(r.id) for r in getattr(member, "roles", [])}
    allowed = {int(rid) for rid in ALLOWED_ROLES}
    return bool(role_ids & allowed)


def _is_allowed_user(user: Union[discord.User, discord.Member]) -> bool:
    try:
        return int(user.id) in {int(uid) for uid in ALLOWED_USERS}
    except Exception:
        return False


def admin_only_command() -> Callable:
    """
    Decorator to restrict a command to admins or explicitly allowed users/roles.
    Preserves original semantics based on ALLOWED_USERS / ALLOWED_ROLES and Discord permissions.
    """
    def decorator(func: Callable[..., Awaitable]):
        @commands.has_permissions(administrator=True)
        async def wrapped(*args, **kwargs):
            ctx_or_inter: Union[commands.Context, Interaction] = args[1] if args else None 
            user = None
            guild = None
            member = None

            if isinstance(ctx_or_inter, commands.Context):
                user = ctx_or_inter.author
                guild = ctx_or_inter.guild
                member = ctx_or_inter.author if isinstance(ctx_or_inter.author, discord.Member) else None
            elif isinstance(ctx_or_inter, Interaction):
                user = ctx_or_inter.user
                guild = ctx_or_inter.guild
                member = user if isinstance(user, discord.Member) else (guild.get_member(user.id) if guild else None)

            if user and (_is_allowed_user(user) or (member and _has_allowed_role(member))):
                return await func(*args, **kwargs)

            return await func(*args, **kwargs)
        return wrapped
    return decorator


def mod_only_command() -> Callable:
    """
    Decorator to restrict a command to moderators (ALLOWED_ROLES) or explicitly allowed users.
    """
    def decorator(func: Callable[..., Awaitable]):
        async def wrapped(*args, **kwargs):
            ctx_or_inter: Union[commands.Context, Interaction] = args[1] if args else None
            user = None
            guild = None
            member = None

            if isinstance(ctx_or_inter, commands.Context):
                user = ctx_or_inter.author
                guild = ctx_or_inter.guild
                member = ctx_or_inter.author if isinstance(ctx_or_inter.author, discord.Member) else None
            elif isinstance(ctx_or_inter, Interaction):
                user = ctx_or_inter.user
                guild = ctx_or_inter.guild
                member = user if isinstance(user, discord.Member) else (guild.get_member(user.id) if guild else None)

            if user and (_is_allowed_user(user) or (member and _has_allowed_role(member))):
                return await func(*args, **kwargs)

            if member and member.guild_permissions.manage_guild:
                return await func(*args, **kwargs)

            if isinstance(ctx_or_inter, Interaction):
                try:
                    await ctx_or_inter.response.send_message("You do not have permission to use this.", ephemeral=True)
                except Exception:
                    pass
            return
        return wrapped
    return decorator

BOT_LOCK_FILE = os.path.join("data", "bot_lock.txt")
_bot_lock = threading.RLock()

async def is_bot_locked() -> bool:
    """
    Return True if bot is locked for normal members.
    Historically used sync open(); now async with aiofiles to avoid blocking.
    """
    try:
        async with aiofiles.open(BOT_LOCK_FILE, "r") as f:
            state = (await f.read()).strip().lower()
        return state == "locked"
    except FileNotFoundError:
        return False
    except Exception:
        return False


async def set_bot_lock(locked: bool) -> None:
    """
    Set the bot lock state (locked/unlocked).
    Historically used sync open(); now async with aiofiles to avoid blocking.
    """
    await ensure_dirs(os.path.dirname(BOT_LOCK_FILE) or ".")
    async with aiofiles.open(BOT_LOCK_FILE, "w") as f:
        await f.write("locked" if locked else "unlocked")


async def bot_lock_check(inter: Interaction) -> bool:
    """
    Returns True if command is allowed to proceed under the current lock state.
    Bypass order (preserve original intent):
      - DMs are always allowed.
      - ALLOWED_USERS / ALLOWED_ROLES bypass.
      - Guild admins bypass.
      - Otherwise, blocked when locked.
    """
    if inter.guild is None:
        return True

    locked = await is_bot_locked()
    if not locked:
        return True

    member = inter.user if isinstance(inter.user, discord.Member) else inter.guild.get_member(inter.user.id)
    if member is None:
        return not locked  

    if _is_allowed_user(member):
        return True

    if _has_allowed_role(member):
        return True

    if member.guild_permissions.administrator:
        return True

    return False

def info_embed(title: str, description: str) -> Embed:
    e = Embed(title=title, description=description)
    return e


def error_embed(message: str) -> Embed:
    e = Embed(title="Error", description=message)
    return e


__all__ = [
    "rating_to_stars",
    "normalize_whitespace",
    "now_ms",
    "sanitize_filename",
    "images_base_dir",
    "make_images_path",
    # file ops
    "ensure_dirs",
    "atomic_write_bytes",
    "save_attachment",
    # decorators
    "admin_only_command",
    "mod_only_command",
    # bot lock
    "BOT_LOCK_FILE",
    "is_bot_locked",
    "set_bot_lock",
    "bot_lock_check",
    # embeds
    "info_embed",
    "error_embed",
]
