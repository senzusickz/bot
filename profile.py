from importies import *

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


async def _save_banner_bytes(guild_id: int, user_id: int, data: bytes, filename_hint: Optional[str] = None) -> str:
    base = make_images_path(guild_id, user_id, "banners")
    await ensure_dirs(base)
    base_name = sanitize_filename(filename_hint or "banner")
    path = os.path.join(base, f"{base_name}.bin")
    tmp = f"{path}.tmp"
    async with aiofiles.open(tmp, "wb") as f:
        await f.write(data)
    await asyncio.to_thread(os.replace, tmp, path)
    return path


async def _read_attachment_bytes(att: Optional[discord.Attachment]) -> Optional[bytes]:
    if not att:
        return None
    try:
        return await att.read()
    except Exception:
        return None


async def _safe_fetch_member(guild: discord.Guild, user_id: int) -> Optional[discord.Member]:
    try:
        return await guild.fetch_member(user_id)
    except Exception:
        return None


async def _avg_avatar_color(user: Union[discord.Member, discord.User]) -> Optional[int]:
    try:
        url = user.display_avatar.url
        data = await _download_bytes(str(url))
        def _compute():
            img = Image.open(io.BytesIO(data)).convert("RGB")
            img = img.resize((50, 50))
            pixels = list(img.getdata())
            if not pixels:
                return None
            r = sum(p[0] for p in pixels) // len(pixels)
            g = sum(p[1] for p in pixels) // len(pixels)
            b = sum(p[2] for p in pixels) // len(pixels)
            return (r << 16) + (g << 8) + b
        return await asyncio.to_thread(_compute)
    except Exception:
        return None


class ProfileCog(commands.Cog):
    def __init__(self, bot: commands.Bot, db: VouchDB):
        self.bot = bot
        self.db = db

    async def _build_profile_embed(
        self,
        guild: discord.Guild,
        target: Union[discord.Member, discord.User],
        banner_path: Optional[str],
        privacy_public: bool
    ) -> Tuple[Embed, Optional[File]]:
        count, avg = await self.db.get_aggregates_in_guild(seller_id=int(target.id), guild_id=int(guild.id))
        color_int = await _avg_avatar_color(target)
        embed = Embed(title=f"{target.display_name}'s Profile")
        if color_int is not None:
            embed.color = color_int
        embed.set_thumbnail(url=target.display_avatar.url)
        embed.add_field(name="User", value=f"{target} ({target.id})", inline=False)
        embed.add_field(name="Vouches", value=str(count), inline=True)
        embed.add_field(name="Average", value=f"{rating_to_stars(avg)} ({round(float(avg), 2)}/5)", inline=True)
        embed.add_field(name="Privacy", value=f"👀 Your stats are currently {'Public' if privacy_public else 'Private'}.", inline=False)
        file: Optional[File] = None
        if banner_path and os.path.exists(banner_path):
            try:
                file = File(banner_path, filename=os.path.basename(banner_path))
                embed.set_image(url=f"attachment://{os.path.basename(banner_path)}")
            except Exception:
                file = None
        return embed, file

    @app_commands.command(name="profile", description="Show a user's vouch profile in this server.")
    @app_commands.describe(user="User to view (defaults to you)")
    async def profile(self, interaction: Interaction, user: Optional[discord.User] = None):
        if not await bot_lock_check(interaction):
            await interaction.response.send_message("The bot is currently locked.", ephemeral=True)
            return
        guild = interaction.guild
        if not guild:
            await interaction.response.send_message("Use this in a server.", ephemeral=True)
            return
        if await self.db.is_blacklisted(user_id=str(interaction.user.id), guild_id=str(guild.id)):
            await interaction.response.send_message("You are blacklisted in this server.", ephemeral=True)
            return
        target = user or interaction.user
        member = guild.get_member(target.id) or await _safe_fetch_member(guild, target.id)
        target_obj: Union[discord.Member, discord.User] = member or target
        privacy_public = await self.db.get_profile_privacy(user_id=int(target_obj.id), guild_id=int(guild.id))
        banner_path = await self.db.get_banner_path(user_id=str(target_obj.id), guild_id=str(guild.id))
        embed, file = await self._build_profile_embed(guild, target_obj, banner_path, privacy_public)
        if file:
            await interaction.response.send_message(embed=embed, file=file)
        else:
            await interaction.response.send_message(embed=embed)

    @app_commands.command(name="profile_privacy", description="Show your current profile stats visibility.")
    async def profile_privacy(self, interaction: Interaction):
        if not await bot_lock_check(interaction):
            await interaction.response.send_message("The bot is currently locked.", ephemeral=True)
            return
        guild = interaction.guild
        if not guild:
            await interaction.response.send_message("Use this in a server.", ephemeral=True)
            return
        if await self.db.is_blacklisted(user_id=str(interaction.user.id), guild_id=str(guild.id)):
            await interaction.response.send_message("You are blacklisted in this server.", ephemeral=True)
            return
        public = await self.db.get_profile_privacy(user_id=int(interaction.user.id), guild_id=int(guild.id))
        await interaction.response.send_message(f"👀 Your stats are currently {'Public' if public else 'Private'}.", ephemeral=True)

    @app_commands.command(name="profile_privacy_set", description="Set your profile stats visibility.")
    @app_commands.describe(visibility="Choose who can view your stats (others vs only you/admins).")
    @app_commands.choices(visibility=[
        app_commands.Choice(name="Public", value="public"),
        app_commands.Choice(name="Private", value="private"),
    ])
    async def profile_privacy_set(self, interaction: Interaction, visibility: app_commands.Choice[str]):
        if not await bot_lock_check(interaction):
            await interaction.response.send_message("The bot is currently locked.", ephemeral=True)
            return
        guild = interaction.guild
        if not guild:
            await interaction.response.send_message("Use this in a server.", ephemeral=True)
            return
        if await self.db.is_blacklisted(user_id=str(interaction.user.id), guild_id=str(guild.id)):
            await interaction.response.send_message("You are blacklisted in this server.", ephemeral=True)
            return
        public = visibility.value == "public"
        await self.db.set_profile_privacy(user_id=int(interaction.user.id), guild_id=int(guild.id), public=public)
        await interaction.response.send_message(f"✅ Updated: Your stats are now {'Public' if public else 'Private'}.", ephemeral=True)

    @app_commands.command(name="profile_banner_set", description="Set your profile banner image (attach an image or provide a URL).")
    @app_commands.describe(image_url="Optional image URL if not attaching a file", image="Attach an image")
    async def profile_banner_set(self, interaction: Interaction, image_url: Optional[str] = None, image: Optional[discord.Attachment] = None):
        if not await bot_lock_check(interaction):
            await interaction.response.send_message("The bot is currently locked.", ephemeral=True)
            return
        guild = interaction.guild
        if not guild:
            await interaction.response.send_message("Use this in a server.", ephemeral=True)
            return
        if await self.db.is_blacklisted(user_id=str(interaction.user.id), guild_id=str(guild.id)):
            await interaction.response.send_message("You are blacklisted in this server.", ephemeral=True)
            return
        banner_bytes: Optional[bytes] = None
        if image is not None:
            banner_bytes = await _read_attachment_bytes(image)
            if not banner_bytes:
                await interaction.response.send_message("Could not read the attached image.", ephemeral=True)
                return
        elif image_url:
            try:
                banner_bytes = await _download_bytes(image_url)
            except Exception:
                await interaction.response.send_message("Failed to download image from the URL.", ephemeral=True)
                return
        else:
            await interaction.response.send_message("Please attach an image or provide an image_url.", ephemeral=True)
            return
        try:
            path = await _save_banner_bytes(guild_id=guild.id, user_id=interaction.user.id, data=banner_bytes, filename_hint=(image.filename if image else "banner"))
        except Exception:
            await interaction.response.send_message("Failed to store your banner image.", ephemeral=True)
            return
        await self.db.set_banner_path(user_id=str(interaction.user.id), guild_id=str(guild.id), banner_path=path)
        await interaction.response.send_message("Your profile banner has been updated.", ephemeral=True)

    @app_commands.command(name="profile_banner_clear", description="Clear your profile banner.")
    async def profile_banner_clear(self, interaction: Interaction):
        if not await bot_lock_check(interaction):
            await interaction.response.send_message("The bot is currently locked.", ephemeral=True)
            return
        guild = interaction.guild
        if not guild:
            await interaction.response.send_message("Use this in a server.", ephemeral=True)
            return
        if await self.db.is_blacklisted(user_id=str(interaction.user.id), guild_id=str(guild.id)):
            await interaction.response.send_message("You are blacklisted in this server.", ephemeral=True)
            return
        await self.db.set_banner_path(user_id=str(interaction.user.id), guild_id=str(guild.id), banner_path=None)
        await interaction.response.send_message("Your profile banner has been cleared.", ephemeral=True)


async def setup(bot: commands.Bot):
    db_path = os.getenv("DB_PATH", os.path.join("data", "vouches.sqlite3"))
    db = VouchDB(db_path)
    await bot.add_cog(ProfileCog(bot, db))
