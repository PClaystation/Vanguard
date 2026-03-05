# vote.py
import discord
from discord.ext import commands
import json
import asyncio
import os
from datetime import datetime, timedelta, timezone

DATA_FILE = 'votes.json'
ACTIVE_VIEWS = {}  # vote_id -> VoteView instance (for cancelling countdowns)

if os.path.exists(DATA_FILE):
    try:
        with open(DATA_FILE, 'r') as f:
            votes = json.load(f)
        if not isinstance(votes, dict):
            votes = {}
    except (json.JSONDecodeError, OSError):
        votes = {}
else:
    votes = {}

def save_votes():
    with open(DATA_FILE, 'w') as f:
        json.dump(votes, f, indent=2)


def _parse_datetime_utc(value):
    if not isinstance(value, str):
        return None
    try:
        dt = datetime.fromisoformat(value)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _get_vote_finish_time(vote_id, vote):
    finish_time = _parse_datetime_utc(vote.get("finish_at"))
    if finish_time:
        return finish_time

    # Backwards compatibility for older saved votes that did not include finish metadata.
    duration_hours = vote.get("duration_hours", 24)
    try:
        duration_hours = int(duration_hours)
    except (TypeError, ValueError):
        duration_hours = 24
    if duration_hours < 1:
        duration_hours = 1

    try:
        started_ts = int(vote_id.rsplit("-", 1)[-1])
    except (TypeError, ValueError):
        return None
    return datetime.fromtimestamp(started_ts, tz=timezone.utc) + timedelta(hours=duration_hours)

class VoteView(discord.ui.View):
    def __init__(self, vote_id, finish_time, bot):
        super().__init__(timeout=None)
        self.vote_id = vote_id
        self.finish_time = finish_time  # aware UTC datetime
        self.bot = bot
        self.update_task = asyncio.create_task(self._countdown_updater())
        self._stopped = False

    @discord.ui.button(label="Vote Against", style=discord.ButtonStyle.danger)
    async def against(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.record_vote(interaction, "against")

    @discord.ui.button(label="Vote Support", style=discord.ButtonStyle.success)
    async def support(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.record_vote(interaction, "support")

    async def record_vote(self, interaction: discord.Interaction, choice):
        vote = votes.get(self.vote_id)
        if not vote:
            return await interaction.response.send_message("This vote has already ended.", ephemeral=True)

        member = interaction.user
        now = datetime.now(timezone.utc)

        # Fraud protection
        account_age = (now - member.created_at).days
        join_age = (now - member.joined_at).days if getattr(member, "joined_at", None) else 0
        min_account_days = vote.get("min_account_days", 7)
        min_join_days = vote.get("min_join_days", 1)
        if account_age < min_account_days:
            return await interaction.response.send_message(f"⚠️ Your account is too new ({account_age}d) to vote.", ephemeral=True)
        if join_age < min_join_days:
            return await interaction.response.send_message(f"⚠️ You joined too recently ({join_age}d) to vote.", ephemeral=True)

        # Record or change vote
        prev = vote["votes"].get(str(member.id))
        vote["votes"][str(member.id)] = choice
        votes[self.vote_id] = vote
        save_votes()

        # Ephemeral confirmation
        if not prev:
            await interaction.response.send_message(f"✅ Your vote ({choice}) has been recorded.", ephemeral=True)
        elif prev == choice:
            await interaction.response.send_message(f"ℹ️ You already voted {choice}.", ephemeral=True)
        else:
            await interaction.response.send_message(f"🔁 Changed vote from {prev} to {choice}.", ephemeral=True)

        # Update embed counts immediately
        channel = interaction.channel or self.bot.get_channel(vote["channel_id"])
        if channel:
            await self.update_message(channel, vote)

    async def update_message(self, channel, vote):
        if channel is None:
            return
        try:
            msg = await channel.fetch_message(vote["message_id"])
        except Exception:
            return
        against_count = sum(1 for v in vote["votes"].values() if v == "against")
        support_count = sum(1 for v in vote["votes"].values() if v == "support")
        remaining = int((self.finish_time - datetime.now(timezone.utc)).total_seconds())
        if remaining < 0:
            remaining = 0
        hours, rem = divmod(remaining, 3600)
        minutes, seconds = divmod(rem, 60)
        time_text = f"{hours}h {minutes}m {seconds}s" if remaining > 0 else "0s"

        embed = discord.Embed(
            title="🚨 EMERGENCY VOTE: NO CONFIDENCE 🚨",
            description=f"A server-wide vote has been started against **{vote['target_name']}**.\nThis is important — please vote responsibly.",
            color=discord.Color.red()
        )
        embed.add_field(name="Against", value=str(against_count), inline=True)
        embed.add_field(name="Support", value=str(support_count), inline=True)
        embed.set_footer(text=f"Time remaining: {time_text} • Votes are one per account (you can change before closing)")

        try:
            await msg.edit(embed=embed)
        except Exception:
            pass

    async def _countdown_updater(self):
        """Update the embed countdown every 30s until the vote ends or this view is stopped."""
        try:
            while True:
                if self._stopped:
                    break
                # Stop if vote no longer exists
                vote = votes.get(self.vote_id)
                if not vote:
                    break
                # If finished, final update and stop
                if datetime.now(timezone.utc) >= self.finish_time:
                    # ensure final update before finish_vote runs
                    try:
                        ch = self.bot.get_channel(vote["channel_id"])
                        if ch:
                            await self.update_message(ch, vote)
                    except Exception:
                        pass
                    break
                # normal update
                try:
                    ch = self.bot.get_channel(vote["channel_id"])
                    if ch:
                        await self.update_message(ch, vote)
                except Exception:
                    pass
                await asyncio.sleep(30)
        except asyncio.CancelledError:
            pass

    async def stop_updater(self):
        self._stopped = True
        try:
            if self.update_task:
                self.update_task.cancel()
        except Exception:
            pass
        super().stop()


def _safe_channel_slug(name: str) -> str:
    slug_chars = []
    for char in name.lower():
        if char.isascii() and char.isalnum():
            slug_chars.append(char)
        else:
            slug_chars.append("-")
    slug = "-".join(part for part in "".join(slug_chars).split("-") if part)
    return slug or "user"

async def finish_vote(bot, vote_id):
    vote = votes.get(vote_id)
    if not vote:
        return
    # cancel updater if present
    view = ACTIVE_VIEWS.pop(vote_id, None)
    if view:
        await view.stop_updater()

    try:
        channel = bot.get_channel(vote["channel_id"])
        if channel:
            msg = await channel.fetch_message(vote["message_id"])
            against_count = sum(1 for v in vote["votes"].values() if v == "against")
            support_count = sum(1 for v in vote["votes"].values() if v == "support")
            embed = discord.Embed(
                title="📣 VOTE CLOSED",
                description=f"Final result for **{vote['target_name']}**",
                color=discord.Color.orange()
            )
            embed.add_field(name="Against", value=str(against_count), inline=True)
            embed.add_field(name="Support", value=str(support_count), inline=True)
            if against_count > support_count:
                result = "Against — majority"
            elif support_count > against_count:
                result = "Support — majority"
            else:
                result = "Tie"
            embed.add_field(name="Result", value=result, inline=False)
            embed.set_footer(text=f"Vote ended • Started by <@{vote['starter_id']}>")
            try:
                await msg.edit(embed=embed, view=None)
            except Exception:
                pass
            await channel.send(f"🔔 **VOTE ENDED** — {result}\nStarter: <@{vote['starter_id']}>, Target: <@{vote['target_id']}>")
            # delete vote channel after 1 hour for cleanliness
            await asyncio.sleep(3600)
            try:
                ch = bot.get_channel(vote["channel_id"])
                bot_member = None
                if ch:
                    bot_member = ch.guild.me
                    if bot_member is None and bot.user:
                        bot_member = ch.guild.get_member(bot.user.id)
                if ch and bot_member and ch.permissions_for(bot_member).manage_channels:
                    await ch.delete(reason="Vote ended")
            except Exception:
                pass
    except Exception as e:
        print("Error finishing vote:", e)
    finally:
        votes.pop(vote_id, None)
        save_votes()


async def schedule_finish(bot, vote_id, delay_seconds):
    await asyncio.sleep(max(0, int(delay_seconds)))
    await finish_vote(bot, vote_id)


async def restore_vote_state(bot):
    """Restore active vote views/tasks after bot restart."""
    now = datetime.now(timezone.utc)
    changed = False

    # Defensive: stop old in-memory updaters before rebuilding.
    for view in list(ACTIVE_VIEWS.values()):
        try:
            await view.stop_updater()
        except Exception:
            pass
    ACTIVE_VIEWS.clear()

    for vote_id, vote in list(votes.items()):
        finish_time = _get_vote_finish_time(vote_id, vote)
        if finish_time is None:
            votes.pop(vote_id, None)
            changed = True
            continue

        if vote.get("finish_at") != finish_time.isoformat():
            vote["finish_at"] = finish_time.isoformat()
            votes[vote_id] = vote
            changed = True

        if finish_time <= now:
            bot.loop.create_task(finish_vote(bot, vote_id))
            continue

        message_id = vote.get("message_id")
        channel_id = vote.get("channel_id")
        if not isinstance(message_id, int) or not isinstance(channel_id, int):
            votes.pop(vote_id, None)
            changed = True
            continue

        view = VoteView(vote_id, finish_time, bot)
        ACTIVE_VIEWS[vote_id] = view
        try:
            bot.add_view(view, message_id=message_id)
        except Exception:
            pass

        delay_seconds = (finish_time - now).total_seconds()
        bot.loop.create_task(schedule_finish(bot, vote_id, delay_seconds))

    if changed:
        save_votes()

def setup_vote_module(bot: commands.Bot):
    @bot.command(name="startvote")
    async def startvote(ctx, target: discord.Member, duration_hours: int = 24):
        """Start a server-wide emergency vote of no confidence."""
        guild = ctx.guild
        if not guild:
            return await ctx.send("This command must be used in a server.")
        if duration_hours < 1 or duration_hours > 168:
            return await ctx.send("⚠️ Duration must be between 1 and 168 hours.")

        # create vote category if it doesn't exist
        category_name = "🚨 VOTES 🚨"
        category = discord.utils.get(guild.categories, name=category_name)
        if not category:
            try:
                category = await guild.create_category(category_name)
            except Exception as e:
                await ctx.send("⚠️ Could not create vote category. Check bot permissions.")
                return

        # create channel name that Discord accepts
        channel_name = f"vote-against-{_safe_channel_slug(target.name)}"[:100]

        try:
            vote_channel = await guild.create_text_channel(channel_name, category=category)
        except Exception as e:
            await ctx.send("⚠️ Could not create vote channel. Check bot permissions.")
            return

        finish_time = datetime.now(timezone.utc) + timedelta(hours=duration_hours)
        vote_id = f"{guild.id}-{vote_channel.id}-{int(datetime.now(timezone.utc).timestamp())}"
        view = VoteView(vote_id, finish_time, bot)
        ACTIVE_VIEWS[vote_id] = view

        # initial embed (big deal tone)
        embed = discord.Embed(
            title="🚨 EMERGENCY VOTE: NO CONFIDENCE 🚨",
            description=f"A server-wide vote has been initiated against **{target}**.\nThis is important — please participate and vote responsibly.",
            color=discord.Color.red()
        )
        embed.add_field(name="Against", value="0", inline=True)
        embed.add_field(name="Support", value="0", inline=True)
        embed.set_footer(text=f"Duration: {duration_hours} hour(s). Votes are saved and you can change before closing.")

        try:
            message = await vote_channel.send(content="@everyone", embed=embed, view=view)
        except Exception:
            await ctx.send("⚠️ Could not post vote message (maybe cannot mention everyone).")
            # still save minimal info and return
            try:
                message = await vote_channel.send(embed=embed, view=view)
            except Exception:
                await ctx.send("⚠️ Failed to post vote message. Aborting.")
                return

        # Save vote meta
        votes[vote_id] = {
            "id": vote_id,
            "starter_id": ctx.author.id,
            "target_id": target.id,
            "target_name": str(target),
            "channel_id": vote_channel.id,
            "message_id": message.id,
            "votes": {},
            "duration_hours": duration_hours,
            "finish_at": finish_time.isoformat(),
            "min_account_days": 7,
            "min_join_days": 1
        }
        save_votes()

        await ctx.send(f"📣 Emergency vote started in {vote_channel.mention}. Make your voice heard!")

        # schedule finish
        bot.loop.create_task(schedule_finish(bot, vote_id, duration_hours * 3600))
