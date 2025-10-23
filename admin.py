import os
from typing import Optional, List

import discord
from discord import app_commands, Interaction, Embed
from discord.ext import commands

from core.db import VouchDB
from core.utils import bot_lock_check


async def _ensure_admin(inter: Interaction) -> bool:
    if inter.guild is None:
        return False
    member = inter.user if isinstance(inter.user, discord.Member) else inter.guild.get_member(inter.user.id)
    if member and member.guild_permissions.administrator:
        return True
    return False


async def _display_name(guild: discord.Guild, user_id: int) -> str:
    m = guild.get_member(user_id)
    if m:
        return f"{m} ({user_id})"
    try:
        m = await guild.fetch_member(user_id)
        return f"{m} ({user_id})"
    except Exception:
        return f"{user_id}"


class AdminCog(commands.Cog):
    def __init__(self, bot: commands.Bot, db: VouchDB):
        self.bot = bot
        self.db = db

    @app_commands.command(name="ban_user", description="Blacklist a user in this server (admin only).")
    @app_commands.describe(user="User to blacklist", reason="Optional reason")
    async def ban_user(self, interaction: Interaction, user: discord.User, reason: Optional[str] = None):
        if not await bot_lock_check(interaction):
            await interaction.response.send_message("The bot is currently locked.", ephemeral=True)
            return
        if not await _ensure_admin(interaction):
            await interaction.response.send_message("Admin only.", ephemeral=True)
            return
        guild = interaction.guild
        await self.db.ban_user(user_id=int(user.id), guild_id=int(guild.id), reason=reason)
        await interaction.response.send_message(f"Blacklisted {user} ({user.id}).", ephemeral=True)

    @app_commands.command(name="unban_user", description="Remove a user from blacklist in this server (admin only).")
    @app_commands.describe(user="User to unblacklist")
    async def unban_user(self, interaction: Interaction, user: discord.User):
        if not await bot_lock_check(interaction):
            await interaction.response.send_message("The bot is currently locked.", ephemeral=True)
            return
        if not await _ensure_admin(interaction):
            await interaction.response.send_message("Admin only.", ephemeral=True)
            return
        guild = interaction.guild
        await self.db.unban_user(user_id=int(user.id), guild_id=int(guild.id))
        await interaction.response.send_message(f"Unblacklisted {user} ({user.id}).", ephemeral=True)

    @app_commands.command(name="blacklist_list", description="Show blacklisted users in this server (admin only).")
    async def blacklist_list(self, interaction: Interaction):
        if not await bot_lock_check(interaction):
            await interaction.response.send_message("The bot is currently locked.", ephemeral=True)
            return
        if not await _ensure_admin(interaction):
            await interaction.response.send_message("Admin only.", ephemeral=True)
            return
        guild = interaction.guild
        rows = await self.db.get_banned_users(guild_id=int(guild.id))
        if not rows:
            await interaction.response.send_message("No blacklisted users in this server.", ephemeral=True)
            return
        lines: List[str] = []
        for uid, reason, banned_at in rows:
            name = await _display_name(guild, int(uid))
            if "(" not in name:
                name = f"{name} ({uid})"
            if reason:
                lines.append(f"{name} — {reason}")
            else:
                lines.append(f"{name}")
        embed = Embed(title="Blacklisted Users", description="\n".join(lines))
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @app_commands.command(name="whitelist_toggle", description="Enable or disable whitelist for this server (admin only).")
    @app_commands.describe(enabled="Turn whitelist ON or OFF")
    async def whitelist_toggle(self, interaction: Interaction, enabled: bool):
        if not await bot_lock_check(interaction):
            await interaction.response.send_message("The bot is currently locked.", ephemeral=True)
            return
        if not await _ensure_admin(interaction):
            await interaction.response.send_message("Admin only.", ephemeral=True)
            return
        await self.db.set_whitelist_enabled(guild_id=int(interaction.guild.id), on=enabled)
        await interaction.response.send_message(f"Whitelist is now {'ON' if enabled else 'OFF'}.", ephemeral=True)

    @app_commands.command(name="whitelist_add", description="Add a user to whitelist (admin only).")
    @app_commands.describe(user="User to allow receiving vouches")
    async def whitelist_add(self, interaction: Interaction, user: discord.User):
        if not await bot_lock_check(interaction):
            await interaction.response.send_message("The bot is currently locked.", ephemeral=True)
            return
        if not await _ensure_admin(interaction):
            await interaction.response.send_message("Admin only.", ephemeral=True)
            return
        await self.db.whitelist_add(guild_id=int(interaction.guild.id), user_id=int(user.id))
        await interaction.response.send_message(f"Whitelisted {user} ({user.id}).", ephemeral=True)

    @app_commands.command(name="whitelist_remove", description="Remove a user from whitelist (admin only).")
    @app_commands.describe(user="User to remove from whitelist")
    async def whitelist_remove(self, interaction: Interaction, user: discord.User):
        if not await bot_lock_check(interaction):
            await interaction.response.send_message("The bot is currently locked.", ephemeral=True)
            return
        if not await _ensure_admin(interaction):
            await interaction.response.send_message("Admin only.", ephemeral=True)
            return
        await self.db.whitelist_remove(guild_id=int(interaction.guild.id), user_id=int(user.id))
        await interaction.response.send_message(f"Removed {user} ({user.id}) from whitelist.", ephemeral=True)

    @app_commands.command(name="whitelist_roles_add", description="Allow a role to bypass whitelist (admin only).")
    @app_commands.describe(role="Role to add to whitelist bypass")
    async def whitelist_roles_add(self, interaction: Interaction, role: discord.Role):
        if not await bot_lock_check(interaction):
            await interaction.response.send_message("The bot is currently locked.", ephemeral=True)
            return
        if not await _ensure_admin(interaction):
            await interaction.response.send_message("Admin only.", ephemeral=True)
            return
        await self.db.whitelist_add_role(guild_id=int(interaction.guild.id), role_id=int(role.id))
        await interaction.response.send_message(f"Role added to whitelist bypass: {role.name} ({role.id}).", ephemeral=True)

    @app_commands.command(name="whitelist_roles_remove", description="Remove a role from whitelist bypass (admin only).")
    @app_commands.describe(role="Role to remove from whitelist bypass")
    async def whitelist_roles_remove(self, interaction: Interaction, role: discord.Role):
        if not await bot_lock_check(interaction):
            await interaction.response.send_message("The bot is currently locked.", ephemeral=True)
            return
        if not await _ensure_admin(interaction):
            await interaction.response.send_message("Admin only.", ephemeral=True)
            return
        await self.db.whitelist_remove_role(guild_id=int(interaction.guild.id), role_id=int(role.id))
        await interaction.response.send_message(f"Role removed from whitelist bypass: {role.name} ({role.id}).", ephemeral=True)

    @app_commands.command(name="whitelist_roles_list", description="List roles that bypass whitelist (admin only).")
    async def whitelist_roles_list(self, interaction: Interaction):
        if not await bot_lock_check(interaction):
            await interaction.response.send_message("The bot is currently locked.", ephemeral=True)
            return
        if not await _ensure_admin(interaction):
            await interaction.response.send_message("Admin only.", ephemeral=True)
            return
        role_ids = await self.db.get_whitelist_role_ids(guild_id=int(interaction.guild.id))
        if not role_ids:
            await interaction.response.send_message("No whitelist bypass roles set.", ephemeral=True)
            return
        names: List[str] = []
        for rid in role_ids:
            role = interaction.guild.get_role(int(rid))
            if role:
                names.append(f"{role.name} ({role.id})")
            else:
                names.append(str(rid))
        embed = Embed(title="Whitelist Bypass Roles", description="\n".join(names))
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @app_commands.command(name="guildmerge", description="Merge all data from one guild to another (admin only).")
    @app_commands.describe(from_guild_id="Source guild ID", to_guild_id="Destination guild ID")
    async def guildmerge(self, interaction: Interaction, from_guild_id: str, to_guild_id: str):
        if not await bot_lock_check(interaction):
            await interaction.response.send_message("The bot is currently locked.", ephemeral=True)
            return
        if not await _ensure_admin(interaction):
            await interaction.response.send_message("Admin only.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        try:
            result = await self.db.guild_merge(from_guild_id=str(from_guild_id), to_guild_id=str(to_guild_id))
            desc = "\n".join(f"{k}: {v}" for k, v in result.items())
            await interaction.followup.send(f"Guild merge complete.\n{desc}", ephemeral=True)
        except Exception as e:
            await interaction.followup.send(f"Guild merge failed: {e}", ephemeral=True)

    @app_commands.command(name="sellermerge", description="Move a seller's vouches between guilds (admin only).")
    @app_commands.describe(seller_id="Seller ID", from_guild_id="Source guild ID", to_guild_id="Destination guild ID")
    async def sellermerge(self, interaction: Interaction, seller_id: str, from_guild_id: str, to_guild_id: str):
        if not await bot_lock_check(interaction):
            await interaction.response.send_message("The bot is currently locked.", ephemeral=True)
            return
        if not await _ensure_admin(interaction):
            await interaction.response.send_message("Admin only.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        try:
            result = await self.db.seller_merge(seller_id=str(seller_id), from_guild_id=str(from_guild_id), to_guild_id=str(to_guild_id))
            desc = "\n".join(f"{k}: {v}" for k, v in result.items())
            await interaction.followup.send(f"Seller merge complete.\n{desc}", ephemeral=True)
        except Exception as e:
            await interaction.followup.send(f"Seller merge failed: {e}", ephemeral=True)


async def setup(bot: commands.Bot):
    db_path = os.getenv("DB_PATH", os.path.join("data", "vouches.sqlite3"))
    db = VouchDB(db_path)
    await bot.add_cog(AdminCog(bot, db))
