import asyncio
import os
import time
from typing import Dict, List, Optional, Tuple

import discord
from discord import app_commands, Interaction, Embed
from discord.ext import commands

from core.db import VouchDB
from core.utils import rating_to_stars, bot_lock_check

CACHE_TTL_SECONDS = 60
PAGE_SIZE = 10
VIEW_TIMEOUT_SECONDS = 120


class LeaderboardView(discord.ui.View):
    def __init__(
        self,
        *,
        owner_id: int,
        guild: discord.Guild,
        rows: List[dict],
        by_value: str,
        page_size: int = PAGE_SIZE,
        timeout: Optional[float] = VIEW_TIMEOUT_SECONDS,
    ):
        super().__init__(timeout=timeout)
        self.owner_id = owner_id
        self.guild = guild
        self.rows = rows
        self.by_value = by_value
        self.page_size = page_size
        self.page = 0
        self._recompute_page_state()

    def _slice(self) -> List[dict]:
        start = self.page * self.page_size
        end = start + self.page_size
        return self.rows[start:end]

    def _recompute_page_state(self):
        total_pages = max(1, (len(self.rows) + self.page_size - 1) // self.page_size)
        self.page = max(0, min(self.page, total_pages - 1))
        self.prev_button.disabled = (self.page == 0)
        self.next_button.disabled = (self.page >= total_pages - 1)

    def _render_embed(self) -> Embed:
        title = "Leaderboard — Rating" if self.by_value == "rating" else "Leaderboard — Vouches"
        embed = Embed(title=title)
        page_rows = self._slice()
        lines = []
        rank_start = self.page * self.page_size + 1
        for i, row in enumerate(page_rows, start=rank_start):
            seller_id = int(row["seller_id"])
            member = self.guild.get_member(seller_id)
            tag = f"{member}" if member else f"{seller_id}"
            avg = float(row.get("avg_rating") or 0.0)
            total = int(row.get("total_vouches") or 0)
            line = f"**#{i}** — {tag} • {rating_to_stars(avg)} ({round(avg, 2)}/5) • {total} vouches"
            lines.append(line)
        embed.description = "\n".join(lines) if lines else "No sellers found."
        total_pages = max(1, (len(self.rows) + self.page_size - 1) // self.page_size)
        embed.set_footer(text=f"Page {self.page + 1} / {total_pages}")
        return embed

    async def interaction_check(self, interaction: Interaction) -> bool:
        return interaction.user and interaction.user.id == self.owner_id

    async def refresh_message(self, interaction: Interaction):
        self._recompute_page_state()
        await interaction.response.edit_message(embed=self._render_embed(), view=self)

    async def on_timeout(self):
        for child in self.children:
            if isinstance(child, discord.ui.Button):
                child.disabled = True

    @discord.ui.button(label="◀ Back", style=discord.ButtonStyle.secondary, custom_id="ldr_prev")
    async def prev_button(self, interaction: Interaction, button: discord.ui.Button):
        self.page -= 1
        await self.refresh_message(interaction)

    @discord.ui.button(label="Next ▶", style=discord.ButtonStyle.primary, custom_id="ldr_next")
    async def next_button(self, interaction: Interaction, button: discord.ui.Button):
        self.page += 1
        await self.refresh_message(interaction)


class LeaderboardCog(commands.Cog):
    def __init__(self, bot: commands.Bot, db: VouchDB):
        self.bot = bot
        self.db = db
        self._cache: Dict[Tuple[int, str], Tuple[float, List[dict]]] = {}

    async def _load_rows(self, guild_id: int) -> List[dict]:
        rows = await self.db.leaderboard(guild_id=str(guild_id), limit=1000, min_vouches=1)
        return rows

    async def _filter_privacy_and_members(self, guild: discord.Guild, rows: List[dict]) -> List[dict]:
        filtered: List[dict] = []
        for row in rows:
            sid = int(row["seller_id"])
            member = guild.get_member(sid)
            if not member:
                continue
            public = await self.db.get_profile_privacy(user_id=sid, guild_id=int(guild.id))
            if not public:
                continue
            filtered.append(row)
        return filtered

    def _sort_rows(self, rows: List[dict], by_value: str) -> List[dict]:
        if by_value == "vouches":
            rows.sort(key=lambda r: (int(r.get("total_vouches") or 0), float(r.get("avg_rating") or 0.0)), reverse=True)
        else:
            rows.sort(key=lambda r: (float(r.get("avg_rating") or 0.0), int(r.get("total_vouches") or 0)), reverse=True)
        return rows

    def _cache_get(self, guild_id: int, by_value: str) -> Optional[List[dict]]:
        key = (guild_id, by_value)
        entry = self._cache.get(key)
        if not entry:
            return None
        ts, rows = entry
        if (time.time() - ts) > CACHE_TTL_SECONDS:
            self._cache.pop(key, None)
            return None
        return rows

    def _cache_set(self, guild_id: int, by_value: str, rows: List[dict]):
        key = (guild_id, by_value)
        self._cache[key] = (time.time(), list(rows))

    @app_commands.command(name="leaderboard", description="Show top sellers in this server")
    @app_commands.choices(by=[
        app_commands.Choice(name="Rating", value="rating"),
        app_commands.Choice(name="Vouches", value="vouches"),
    ])
    async def leaderboard(self, interaction: Interaction, by: app_commands.Choice[str]):
        if not await bot_lock_check(interaction):
            await interaction.response.send_message("The bot is currently locked.", ephemeral=True)
            return

        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message("Must be used in a server.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=False)

        cached = self._cache_get(guild.id, by.value)
        if cached is None:
            base_rows = await self._load_rows(guild.id)
            filtered = await self._filter_privacy_and_members(guild, base_rows)
            sorted_rows = self._sort_rows(filtered, by.value)
            self._cache_set(guild.id, by.value, sorted_rows)
            rows = sorted_rows
        else:
            rows = cached

        if not rows:
            await interaction.followup.send("No sellers found.")
            return

        view = LeaderboardView(
            owner_id=interaction.user.id,
            guild=guild,
            rows=rows,
            by_value=by.value,
        )
        await interaction.followup.send(embed=view._render_embed(), view=view)


async def setup(bot: commands.Bot):
    db_path = os.getenv("DB_PATH", os.path.join("data", "vouches.sqlite3"))
    db = VouchDB(db_path)
    await bot.add_cog(LeaderboardCog(bot, db))
