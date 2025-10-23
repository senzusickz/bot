import asyncio
import os
import re
import hashlib
from typing import Optional, Union

import aiohttp
import aiofiles
import discord
from discord import app_commands, Interaction, Embed, File
from discord.ext import commands

from core.db import VouchDB
from core.utils import (
    ensure_dirs,
    sanitize_filename,
    bot_lock_check,
    rating_to_stars,
)

_whitespace_re = re.compile(r"\s+", re.UNICODE)


def _normalize_desc(text: Optional[str]) -> str:
    if not text:
        return ""
    t = text.strip().lower()
    t = _whitespace_re.sub(" ", t)
    return t


def _hash_desc(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8", errors="ignore")).hexdigest()


def _hash_bytes(data: Optional[bytes]) -> str:
    return hashlib.sha256(data or b"").hexdigest()


async def _download_bytes(url: str, *, timeout: int = 20, max_size: int = 20 * 1024 * 1024) -> bytes:
    t = aiohttp.ClientTimeout(total=timeout)
    async with aiohttp.ClientSession(timeout=t) as session:
        async with session.get(url) as resp:
            resp.raise_for_status()
            total = 0
            chunks = []
            async for chunk in resp.content.iter_chunked(65536):
                total += len(chunk)
                if total > max_size:
                    raise ValueError("image too large")
                chunks.append(chunk)
            return b"".join(chunks)


async def _save_image_bytes(path: str, data: bytes) -> None:
    await ensure_dirs(os.path.dirname(path) or ".")
    tmp = f"{path}.tmp"
    async with aiofiles.open(tmp, "wb") as f:
        await f.write(data)
    await asyncio.to_thread(os.replace, tmp, path)


async def _get_or_create_webhook(channel: discord.TextChannel, name: str) -> discord.Webhook:
    hooks = await channel.webhooks()
    for h in hooks:
        if h.name == name:
            return h
    return await channel.create_webhook(name=name)


class _MuteVouchDMView(discord.ui.View):
    def __init__(self, db: VouchDB, user_id: int, guild_id: int):
        super().__init__(timeout=1800)
        self.db = db
        self.user_id = str(user_id)
        self.guild_id = str(guild_id)

    @discord.ui.button(label="Mute Notifications", style=discord.ButtonStyle.secondary)
    async def mute_button(self, interaction: Interaction, button: discord.ui.Button):
        await self.db.mute_set(user_id=self.user_id, guild_id=self.guild_id, mute_type="vouch", muted=True)
        await interaction.response.send_message("You will no longer receive vouch notifications in this server.", ephemeral=True)


class VouchCog(commands.Cog):
    def __init__(self, bot: commands.Bot, db: VouchDB):
        self.bot = bot
        self.db = db

    async def _seller_allowed(self, guild: discord.Guild, seller_id: int) -> bool:
        enabled = await self.db.is_whitelist_enabled(guild_id=guild.id)
        if not enabled:
            return True
        if await self.db.is_whitelisted(guild_id=guild.id, user_id=seller_id):
            return True
        member: Optional[discord.Member] = guild.get_member(seller_id) or await self._safe_fetch_member(guild, seller_id)
        if not member:
            return False
        role_ids = set(await self.db.get_whitelist_role_ids(guild_id=guild.id))
        return any(str(r.id) in role_ids for r in member.roles)

    async def _safe_fetch_member(self, guild: discord.Guild, user_id: int) -> Optional[discord.Member]:
        try:
            return await guild.fetch_member(user_id)
        except Exception:
            return None

    async def _notify_seller_dm(self, seller: Union[discord.Member, discord.User], guild_id: int, buyer: Union[discord.Member, discord.User], rating: int, text: str, image_path: Optional[str]):
        if await self.db.is_muted(user_id=str(seller.id), guild_id=str(guild_id), mute_type="vouch"):
            return
        if not await self.db.is_dm_enabled(user_id=str(seller.id), guild_id=str(guild_id), kind="vouch"):
            return
        embed = Embed(title="You received a new vouch", description=text or "")
        embed.add_field(name="Rating", value=f"{rating_to_stars(rating)} ({rating}/5)", inline=False)
        embed.add_field(name="Buyer", value=f"{buyer} ({buyer.id})", inline=False)
        embed.add_field(name="Server", value=str(guild_id), inline=False)
        file: Optional[File] = None
        if image_path and os.path.exists(image_path):
            try:
                file = File(image_path, filename=os.path.basename(image_path))
                embed.set_image(url=f"attachment://{os.path.basename(image_path)}")
            except Exception:
                file = None
        view = _MuteVouchDMView(self.db, seller.id, guild_id)
        try:
            if file:
                await seller.send(embed=embed, file=file, view=view)
            else:
                await seller.send(embed=embed, view=view)
        except Exception:
            return

    @app_commands.command(name="vouch", description="Leave a vouch for a seller in this server.")
    @app_commands.describe(
        seller="Who are you vouching for?",
        rating="0 to 5 stars",
        text="Short description",
        shipped_days="How many days until shipped (optional)",
        image_url="Optional image URL",
        image="Optional image attachment"
    )
    async def vouch(
        self,
        interaction: Interaction,
        seller: discord.User,
        rating: app_commands.Range[int, 0, 5],
        text: Optional[str] = None,
        shipped_days: Optional[int] = None,
        image_url: Optional[str] = None,
        image: Optional[discord.Attachment] = None,
    ):
        if not await bot_lock_check(interaction):
            await interaction.response.send_message("The bot is currently locked.", ephemeral=True)
            return

        guild = interaction.guild
        if not guild:
            await interaction.response.send_message("Use this in a server.", ephemeral=True)
            return

        buyer_id = str(interaction.user.id)
        seller_id = str(seller.id)
        guild_id = str(guild.id)

        if await self.db.is_blacklisted(user_id=buyer_id, guild_id=guild_id):
            await interaction.response.send_message("You are blacklisted in this server.", ephemeral=True)
            return
        if await self.db.is_blacklisted(user_id=seller_id, guild_id=guild_id):
            await interaction.response.send_message("That user is blacklisted and cannot receive vouches here.", ephemeral=True)
            return
        if not await self._seller_allowed(guild, int(seller_id)):
            await interaction.response.send_message("Vouching is restricted: the seller is not whitelisted for this server.", ephemeral=True)
            return

        desc_norm = _normalize_desc(text or "")
        desc_hash = _hash_desc(desc_norm)

        img_bytes: Optional[bytes] = None
        if image is not None:
            try:
                img_bytes = await image.read()
            except Exception:
                await interaction.response.send_message("Could not read the attached image.", ephemeral=True)
                return
        elif image_url:
            try:
                img_bytes = await _download_bytes(image_url)
            except Exception:
                await interaction.response.send_message("Could not download the image from the provided URL.", ephemeral=True)
                return

        img_hash = _hash_bytes(img_bytes)
        if await self.db.is_duplicate_vouch(seller_id=seller_id, guild_id=guild_id, img_hash=img_hash, desc_hash=desc_hash):
            await interaction.response.send_message("🚫 Duplicate vouch detected (same seller, image and text).", ephemeral=True)
            return

        image_path = None
        if img_bytes:
            folder = os.path.join("data", "images", guild_id, seller_id, "vouches")
            await ensure_dirs(folder)
            base = sanitize_filename(f"vouch_{interaction.user.id}_{seller.id}")
            filename = f"{base}_{img_hash[:10]}.bin"
            path = os.path.join(folder, filename)
            try:
                await _save_image_bytes(path, img_bytes)
                image_path = path
            except Exception:
                image_path = None

        await interaction.response.defer(ephemeral=False)

        ok, vouch_id, err = await self.db.add_vouch(
            seller_id=seller_id,
            buyer_id=buyer_id,
            guild_id=guild_id,
            rating=int(rating),
            text=text or "",
            img_hash=img_hash,
            desc_hash=desc_hash,
            image_path=image_path,
            image_url=image_url if image_url else None,
            notify_seller=1,
        )
        if not ok:
            await interaction.followup.send(err or "Failed to add vouch.", ephemeral=True)
            return

        embed = Embed(title="New Vouch")
        embed.add_field(name="Seller", value=f"{seller} ({seller.id})", inline=False)
        embed.add_field(name="Buyer", value=f"{interaction.user} ({interaction.user.id})", inline=False)
        embed.add_field(name="Rating", value=f"{rating_to_stars(rating)} ({int(rating)}/5)", inline=True)
        if shipped_days is not None:
            embed.add_field(name="Shipped In", value=f"{int(shipped_days)}d", inline=True)
        embed.add_field(name="Text", value=text or "—", inline=False)

        file_obj: Optional[File] = None
        if image_path and os.path.exists(image_path):
            try:
                file_obj = File(image_path, filename=os.path.basename(image_path))
                embed.set_image(url=f"attachment://{os.path.basename(image_path)}")
            except Exception:
                file_obj = None

        channel = interaction.channel
        if not isinstance(channel, discord.TextChannel):
            await interaction.followup.send("This command must be used in a text channel.", ephemeral=True)
            return

        from config import VOUCH_WEBHOOK_NAME  # keep original config-based webhook naming
        try:
            webhook = await _get_or_create_webhook(channel, VOUCH_WEBHOOK_NAME)
            await webhook.send(
                username=interaction.user.display_name,
                avatar_url=interaction.user.display_avatar.url if interaction.user.display_avatar else None,
                embed=embed,
                file=file_obj,
                allowed_mentions=discord.AllowedMentions(users=[seller, interaction.user]),
            )
        except discord.Forbidden:
            await interaction.followup.send("`❌ I need Manage Webhooks and Attach Files in this channel.`", ephemeral=True)
            return
        except Exception as e:
            await interaction.followup.send(f"`❌ Webhook send failed: `{e}``", ephemeral=True)
            return

        await interaction.followup.send(f"`✅ Vouch recorded for {seller.mention}!`", ephemeral=False)

        try:
            target = guild.get_member(seller.id) or await self._safe_fetch_member(guild, seller.id)
            if target:
                await self._notify_seller_dm(
                    seller=target,
                    guild_id=guild.id,
                    buyer=interaction.user,
                    rating=int(rating),
                    text=text or "",
                    image_path=image_path if image_path and os.path.exists(image_path) else None,
                )
        except Exception:
            pass

async def setup(bot: commands.Bot):
    db_path = os.getenv("DB_PATH", os.path.join("data", "vouches.sqlite3"))
    db = VouchDB(db_path)
    await bot.add_cog(VouchCog(bot, db))
