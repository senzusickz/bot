import os
from typing import List, Optional, Dict, Any, Union

import discord
from discord import app_commands, Interaction, Embed, File
from discord.ext import commands

from core.db import VouchDB
from core.utils import rating_to_stars, bot_lock_check


PAGE_SIZE = 5
VIEW_TIMEOUT_SECONDS = 180


def _fmt_user(u: Union[discord.Member, discord.User, None]) -> str:
    if not u:
        return "Unknown"
    return f"{u} ({u.id})"


def _render_vouch_row(row: Dict[str, Any], seller: Union[discord.Member, discord.User, None], buyer: Union[discord.Member, discord.User, None]) -> Embed:
    rating = int(row.get("rating") or 0)
    text = row.get("text") or "—"

    e = Embed(title="Vouch")
    e.add_field(name="Seller", value=_fmt_user(seller), inline=False)
    e.add_field(name="Buyer", value=_fmt_user(buyer), inline=False)
    e.add_field(name="Rating", value=f"{rating_to_stars(rating)} ({rating}/5)", inline=True)
    e.add_field(name="Text", value=text, inline=False)
    ts = row.get("timestamp")
    if ts:
        e.set_footer(text=f"Posted: {ts}")
    image_path = row.get("image_path")
    if image_path and os.path.exists(image_path):
        e.set_image(url=f"attachment://{os.path.basename(image_path)}")
    return e


class ReviewView(discord.ui.View):
    def __init__(
        self,
        *,
        owner_id: int,
        guild: discord.Guild,
        items: List[Dict[str, Any]],
        mode: str,
        page_size: int = PAGE_SIZE,
        timeout: Optional[float] = VIEW_TIMEOUT_SECONDS,
    ):
        super().__init__(timeout=timeout)
        self.owner_id = owner_id
        self.guild = guild
        self.items = items
        self.mode = mode
        self.page_size = max(1, page_size)
        self.page = 0
        self._recompute_page_state()

    def _slice(self) -> List[Dict[str, Any]]:
        start = self.page * self.page_size
        end = start + self.page_size
        return self.items[start:end]

    def _recompute_page_state(self):
        total_pages = max(1, (len(self.items) + self.page_size - 1) // self.page_size)
        self.page = max(0, min(self.page, total_pages - 1))
        self.prev_button.disabled = (self.page == 0)
        self.next_button.disabled = (self.page >= total_pages - 1)

    def _render_embeds_and_files(self) -> (List[Embed], List[File]):
        embeds: List[Embed] = []
        files: List[File] = []
        chunk = self._slice()
        for row in chunk:
            seller = self.guild.get_member(int(row["seller_id"])) or None
            buyer = self.guild.get_member(int(row["buyer_id"])) or None
            e = _render_vouch_row(row, seller, buyer)
            embeds.append(e)
            image_path = row.get("image_path")
            if image_path and os.path.exists(image_path):
                files.append(File(image_path, filename=os.path.basename(image_path)))
        return embeds, files

    async def interaction_check(self, interaction: Interaction) -> bool:
        return interaction.user and interaction.user.id == self.owner_id

    async def refresh_message(self, interaction: Interaction):
        self._recompute_page_state()
        embeds, files = self._render_embeds_and_files()
        footer = f"Page {self.page + 1} / {max(1, (len(self.items) + self.page_size - 1) // self.page_size)} • Mode: {self.mode.capitalize()}"
        if embeds:
            embeds[-1].set_footer(text=footer)
        await interaction.response.edit_message(embeds=embeds[:10], attachments=files[:10], view=self)

    async def on_timeout(self):
        for child in self.children:
            if isinstance(child, discord.ui.Button):
                child.disabled = True

    @discord.ui.button(label="◀ Back", style=discord.ButtonStyle.secondary, custom_id="review_prev")
    async def prev_button(self, interaction: Interaction, button: discord.ui.Button):
        self.page -= 1
        await self.refresh_message(interaction)

    @discord.ui.button(label="Next ▶", style=discord.ButtonStyle.primary, custom_id="review_next")
    async def next_button(self, interaction: Interaction, button: discord.ui.Button):
        self.page += 1
        await self.refresh_message(interaction)


class ReviewCog(commands.Cog):
    def __init__(self, bot: commands.Bot, db: VouchDB):
        self.bot = bot
        self.db = db

    async def _fetch_seller_history(self, guild_id: int, seller_id: int, limit: int) -> List[Dict[str, Any]]:
        rows = await self.db.get_vouches_for_seller_in_guild(seller_id=seller_id, guild_id=guild_id)
        return rows[:limit]

    async def _fetch_buyer_history(self, guild_id: int, buyer_id: int, limit: int) -> List[Dict[str, Any]]:
        rows = await self.db.history_for_user(user_id=str(buyer_id), guild_id=str(guild_id), limit=limit, offset=0)
        rows = [r for r in rows if str(r.get("buyer_id")) == str(buyer_id)]
        return rows[:limit]

    async def _fetch_recent(self, guild_id: int, limit: int) -> List[Dict[str, Any]]:
        # reuse history_for_user on the guild by aggregating seller or buyer; simpler: fetch seller vouches for all via direct call
        # since db has no direct "recent in guild" method, get by guild index
        # emulate using seller leaderboard retrieval of all sellers, then gather their vouches, but that can be heavy.
        # Instead, a direct query helper would be ideal; fallback to afetchall:
        rows = await self.db.afetchall(
            """
            SELECT id, seller_id, buyer_id, guild_id, rating, text, img_hash, desc_hash, image_path, image_url, timestamp
            FROM vouches
            WHERE guild_id=?
            ORDER BY timestamp DESC
            LIMIT ?
            """,
            (str(guild_id), int(limit)),
        )
        keys = ["id", "seller_id", "buyer_id", "guild_id", "rating", "text", "img_hash", "desc_hash", "image_path", "image_url", "timestamp"]
        return [dict(zip(keys, r)) for r in rows]

    @app_commands.command(name="review", description="Browse vouch history in this server.")
    @app_commands.describe(
        mode="Which history to view",
        user="Target user (defaults to you for seller/buyer modes)",
        limit="How many entries to load (max 50)"
    )
    @app_commands.choices(mode=[
        app_commands.Choice(name="Seller", value="seller"),
        app_commands.Choice(name="Buyer", value="buyer"),
        app_commands.Choice(name="Recent", value="recent"),
    ])
    async def review(self, interaction: Interaction, mode: app_commands.Choice[str], user: Optional[discord.User] = None, limit: Optional[int] = 10):
        if not await bot_lock_check(interaction):
            await interaction.response.send_message("The bot is currently locked.", ephemeral=True)
            return
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message("Use this in a server.", ephemeral=True)
            return

        limit = int(limit or 10)
        limit = max(1, min(50, limit))

        await interaction.response.defer(ephemeral=False)

        rows: List[Dict[str, Any]] = []
        mode_val = mode.value

        if mode_val == "seller":
            target = user or interaction.user
            rows = await self._fetch_seller_history(guild_id=guild.id, seller_id=int(target.id), limit=limit)
        elif mode_val == "buyer":
            target = user or interaction.user
            rows = await self._fetch_buyer_history(guild_id=guild.id, buyer_id=int(target.id), limit=limit)
        else:
            rows = await self._fetch_recent(guild_id=guild.id, limit=limit)

        if not rows:
            await interaction.followup.send("No vouches found for that view.")
            return

        view = ReviewView(
            owner_id=interaction.user.id,
            guild=guild,
            items=rows,
            mode=mode_val,
            page_size=PAGE_SIZE,
        )
        embeds, files = view._render_embeds_and_files()
        footer = f"Page 1 / {max(1, (len(rows) + PAGE_SIZE - 1) // PAGE_SIZE)} • Mode: {mode_val.capitalize()}"
        if embeds:
            embeds[-1].set_footer(text=footer)
        await interaction.followup.send(embeds=embeds[:10], files=files[:10], view=view)


async def setup(bot: commands.Bot):
    db_path = os.getenv("DB_PATH", os.path.join("data", "vouches.sqlite3"))
    db = VouchDB(db_path)
    await bot.add_cog(ReviewCog(bot, db))
