from importies import *

class _MuteReplyDMView(discord.ui.View):
    def __init__(self, db: VouchDB, user_id: int, guild_id: int):
        super().__init__(timeout=1800)
        self.db = db
        self.user_id = str(user_id)
        self.guild_id = str(guild_id)

    @discord.ui.button(label="Mute Notifications", style=discord.ButtonStyle.secondary)
    async def mute_button(self, interaction: Interaction, button: discord.ui.Button):
        await self.db.mute_set(user_id=self.user_id, guild_id=self.guild_id, mute_type="reply", muted=True)
        await interaction.response.send_message("You will no longer receive reply notifications in this server.", ephemeral=True)


async def _safe_fetch_member(guild: discord.Guild, user_id: int) -> Optional[discord.Member]:
    try:
        return await guild.fetch_member(user_id)
    except Exception:
        return None


async def _notify_buyer_dm(
    db: VouchDB,
    buyer: Union[discord.Member, discord.User],
    guild_id: int,
    seller: Union[discord.Member, discord.User],
    vouch_fields: dict,
    reply_text: str,
    image_path: Optional[str],
):
    if await db.is_muted(user_id=str(buyer.id), guild_id=str(guild_id), mute_type="reply"):
        return
    if not await db.is_dm_enabled(user_id=str(buyer.id), guild_id=str(guild_id), kind="reply"):
        return

    embed = Embed(title="Seller replied to your vouch", description=reply_text or "—")
    embed.add_field(name="Seller", value=f"{seller} ({seller.id})", inline=False)
    embed.add_field(name="Server", value=str(guild_id), inline=False)
    embed.add_field(name="Rating", value=f"{rating_to_stars(vouch_fields['rating'])} ({vouch_fields['rating']}/5)", inline=True)
    embed.add_field(name="Original Text", value=vouch_fields.get("text") or "—", inline=False)

    file: Optional[File] = None
    if image_path and os.path.exists(image_path):
        try:
            file = File(image_path, filename=os.path.basename(image_path))
            embed.set_image(url=f"attachment://{os.path.basename(image_path)}")
        except Exception:
            file = None

    view = _MuteReplyDMView(db, buyer.id, guild_id)
    try:
        if file:
            await buyer.send(embed=embed, file=file, view=view)
        else:
            await buyer.send(embed=embed, view=view)
    except Exception:
        return


async def setup(bot: commands.Bot):
    db_path = os.getenv("DB_PATH", os.path.join("data", "vouches.sqlite3"))
    db = VouchDB(db_path)

    group = app_commands.Group(
        name="seller_reply",
        description="Manage your reply on a vouch"
    )

    @group.command(name="set", description="Set or update your reply on a specific vouch")
    @app_commands.describe(
        vouch_id="The vouch ID shown in the review footer (ID#123)",
        text="Your reply text"
    )
    async def seller_reply_set(interaction: Interaction, vouch_id: int, text: str):
        guild = interaction.guild
        if not guild:
            await interaction.response.send_message("Use this in a server.", ephemeral=True)
            return

        user_id = str(interaction.user.id)
        guild_id = str(guild.id)

        if await db.is_blacklisted(user_id=user_id, guild_id=guild_id):
            await interaction.response.send_message("You are blacklisted in this server.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=False)

        row = await db.get_vouch(vouch_id, guild_id)
        if not row:
            await interaction.followup.send("`❌ Vouch not found in this server.`", ephemeral=True)
            return

        if row["seller_id"] != user_id:
            await interaction.followup.send("`❌ Only the seller on this vouch can reply to it.`", ephemeral=True)
            return

        ok, reply_id, err = await db.add_reply(
            vouch_id=vouch_id,
            guild_id=guild_id,
            seller_id=row["seller_id"],
            buyer_id=row["buyer_id"],
            text=text,
        )
        if not ok:
            await interaction.followup.send(err or "`❌ Failed to add reply.`", ephemeral=True)
            return

        embed = Embed(title="Reply Added", description=text or "—")
        embed.add_field(name="Vouch ID", value=str(vouch_id), inline=True)
        embed.add_field(name="Seller", value=f"{interaction.user} ({interaction.user.id})", inline=False)
        embed.add_field(name="Buyer", value=str(row["buyer_id"]), inline=False)
        embed.add_field(name="Rating", value=f"{rating_to_stars(row['rating'])} ({row['rating']}/5)", inline=False)
        embed.add_field(name="Original Text", value=row["text"] or "—", inline=False)

        file: Optional[File] = None
        image_path = row.get("image_path")
        if image_path and os.path.exists(image_path):
            try:
                file = File(image_path, filename=os.path.basename(image_path))
                embed.set_image(url=f"attachment://{os.path.basename(image_path)}")
                await interaction.followup.send(embed=embed, file=file)
            except Exception:
                file = None
                await interaction.followup.send(embed=embed)
        else:
            await interaction.followup.send(embed=embed)

        try:
            buyer_member = guild.get_member(int(row["buyer_id"])) or await _safe_fetch_member(guild, int(row["buyer_id"]))
            if buyer_member:
                await _notify_buyer_dm(
                    db=db,
                    buyer=buyer_member,
                    guild_id=guild.id,
                    seller=interaction.user,
                    vouch_fields={"rating": row["rating"], "text": row["text"]},
                    reply_text=text,
                    image_path=image_path if image_path and os.path.exists(image_path) else None,
                )
        except Exception:
            pass

    bot.tree.add_command(group)
