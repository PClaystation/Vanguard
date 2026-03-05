import asyncio
import json
import os
import random
import re
import traceback
from datetime import datetime, timedelta, timezone
from typing import Any

import discord
from discord.ext import commands
from mcstatus import JavaServer
import requests

from vote import restore_vote_state, setup_vote_module, votes as vote_store

BOT_PREFIX = os.getenv("BOT_PREFIX", "!")
MAX_PREFIX_LENGTH = 5
AI_SERVER_URL = os.getenv("AI_SERVER_URL", "http://localhost:3001/ask")
FLAG_USER_URL = os.getenv("FLAG_USER_URL", "http://localhost:3001/fuck")
UNFLAG_USER_URL = os.getenv("UNFLAG_USER_URL", "http://localhost:3001/unfuck")
MC_DEFAULT_HOST = os.getenv("MC_DEFAULT_HOST")
try:
    MC_DEFAULT_PORT = int(os.getenv("MC_DEFAULT_PORT", "25565"))
except ValueError:
    MC_DEFAULT_PORT = 25565

SETTINGS_FILE = "settings.json"
REMINDERS_FILE = "reminders.json"
START_TIME = datetime.now(timezone.utc)
REMINDER_CHECK_SECONDS = 15
MAX_REMINDER_SECONDS = 60 * 60 * 24 * 30
MAX_TIMEOUT_SECONDS = 60 * 60 * 24 * 28

ID_RE = re.compile(r"(\d{17,20})")
DURATION_TOKEN_RE = re.compile(r"(\d+)([smhdw])")


def default_guild_settings() -> dict[str, Any]:
    return {
        "prefix": None,
        "welcome_channel_id": None,
        "welcome_role_id": None,
        "welcome_message": None,
        "lockdown_role_id": None,
        "mod_role_ids": [],
        "mc_host": None,
        "mc_port": 25565,
    }


def default_settings() -> dict[str, Any]:
    return {
        "owner_only": False,
        "guilds": {},
    }


def as_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def parse_datetime_utc(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        dt = datetime.fromisoformat(value)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def normalize_settings(raw: Any) -> dict[str, Any]:
    normalized = default_settings()
    if not isinstance(raw, dict):
        return normalized

    normalized["owner_only"] = bool(raw.get("owner_only", False))
    raw_guilds = raw.get("guilds", {})
    if not isinstance(raw_guilds, dict):
        return normalized

    guilds: dict[str, dict[str, Any]] = {}
    for guild_id, cfg in raw_guilds.items():
        if not isinstance(cfg, dict):
            continue

        guild_cfg = default_guild_settings()

        prefix = cfg.get("prefix")
        if isinstance(prefix, str):
            prefix = prefix.strip()
            guild_cfg["prefix"] = prefix[:MAX_PREFIX_LENGTH] if prefix else None

        guild_cfg["welcome_channel_id"] = as_int(cfg.get("welcome_channel_id"))
        guild_cfg["welcome_role_id"] = as_int(cfg.get("welcome_role_id"))
        guild_cfg["lockdown_role_id"] = as_int(cfg.get("lockdown_role_id"))

        welcome_message = cfg.get("welcome_message")
        if isinstance(welcome_message, str):
            welcome_message = welcome_message.strip()
            guild_cfg["welcome_message"] = welcome_message[:500] if welcome_message else None

        role_ids = cfg.get("mod_role_ids", [])
        if isinstance(role_ids, list):
            guild_cfg["mod_role_ids"] = [
                role_id for role_id in (as_int(value) for value in role_ids) if role_id
            ]

        host = cfg.get("mc_host")
        guild_cfg["mc_host"] = host.strip() if isinstance(host, str) and host.strip() else None

        port = as_int(cfg.get("mc_port"))
        guild_cfg["mc_port"] = port if port and 1 <= port <= 65535 else 25565

        guilds[str(guild_id)] = guild_cfg

    normalized["guilds"] = guilds
    return normalized


def load_settings() -> dict[str, Any]:
    if not os.path.exists(SETTINGS_FILE):
        initial = default_settings()
        with open(SETTINGS_FILE, "w") as file:
            json.dump(initial, file, indent=2)
        return initial

    try:
        with open(SETTINGS_FILE, "r") as file:
            return normalize_settings(json.load(file))
    except (json.JSONDecodeError, OSError):
        reset = default_settings()
        with open(SETTINGS_FILE, "w") as file:
            json.dump(reset, file, indent=2)
        return reset


settings = load_settings()


def save_settings() -> None:
    with open(SETTINGS_FILE, "w") as file:
        json.dump(settings, file, indent=2)


def get_guild_config(guild_id: int) -> dict[str, Any]:
    guilds = settings.setdefault("guilds", {})
    key = str(guild_id)
    cfg = guilds.get(key)
    if not isinstance(cfg, dict):
        cfg = default_guild_settings()
        guilds[key] = cfg
    return cfg


def normalize_reminders(raw: Any) -> list[dict[str, Any]]:
    if not isinstance(raw, list):
        return []
    normalized: list[dict[str, Any]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        reminder_id = as_int(item.get("id"))
        user_id = as_int(item.get("user_id"))
        channel_id = as_int(item.get("channel_id"))
        guild_id = as_int(item.get("guild_id"))
        message = item.get("message", "")
        due_at = parse_datetime_utc(item.get("due_at"))
        if (
            reminder_id is None
            or user_id is None
            or channel_id is None
            or due_at is None
            or not isinstance(message, str)
            or not message.strip()
        ):
            continue
        normalized.append(
            {
                "id": reminder_id,
                "user_id": user_id,
                "channel_id": channel_id,
                "guild_id": guild_id,
                "message": message.strip()[:300],
                "due_at": due_at.isoformat(),
            }
        )
    normalized.sort(key=lambda reminder: reminder["due_at"])
    return normalized


def load_reminders() -> list[dict[str, Any]]:
    if not os.path.exists(REMINDERS_FILE):
        with open(REMINDERS_FILE, "w") as file:
            json.dump([], file, indent=2)
        return []
    try:
        with open(REMINDERS_FILE, "r") as file:
            return normalize_reminders(json.load(file))
    except (json.JSONDecodeError, OSError):
        with open(REMINDERS_FILE, "w") as file:
            json.dump([], file, indent=2)
        return []


reminders = load_reminders()
next_reminder_id = max((reminder["id"] for reminder in reminders), default=0) + 1


def save_reminders() -> None:
    reminders.sort(key=lambda reminder: reminder["due_at"])
    with open(REMINDERS_FILE, "w") as file:
        json.dump(reminders, file, indent=2)


def create_reminder_id() -> int:
    global next_reminder_id
    current_id = next_reminder_id
    next_reminder_id += 1
    return current_id


def parse_duration_to_seconds(duration: str) -> int | None:
    cleaned = duration.strip().lower().replace(" ", "")
    if not cleaned:
        return None
    if cleaned.isdigit():
        # Plain integer defaults to minutes.
        return int(cleaned) * 60

    matches = list(DURATION_TOKEN_RE.finditer(cleaned))
    if not matches:
        return None
    if "".join(match.group(0) for match in matches) != cleaned:
        return None

    unit_seconds = {
        "s": 1,
        "m": 60,
        "h": 3600,
        "d": 86400,
        "w": 604800,
    }
    total_seconds = 0
    for match in matches:
        amount = int(match.group(1))
        unit = match.group(2)
        total_seconds += amount * unit_seconds[unit]
    return total_seconds


def format_duration(seconds: int) -> str:
    seconds = max(0, int(seconds))
    units = [
        ("d", 86400),
        ("h", 3600),
        ("m", 60),
        ("s", 1),
    ]
    parts: list[str] = []
    remaining = seconds
    for label, unit_seconds in units:
        if remaining >= unit_seconds:
            amount, remaining = divmod(remaining, unit_seconds)
            parts.append(f"{amount}{label}")
    return " ".join(parts) if parts else "0s"


def extract_id(target: str) -> str | None:
    if not target:
        return None
    if target.isdigit():
        return target
    match = ID_RE.search(target)
    return match.group(1) if match else None


def get_bot_member(guild: discord.Guild) -> discord.Member | None:
    member = guild.me
    if member:
        return member
    if bot.user:
        return guild.get_member(bot.user.id)
    return None


def resolve_role(guild: discord.Guild, role_id: int | None) -> discord.Role | None:
    if not role_id:
        return None
    return guild.get_role(role_id)


def has_mod_access(member: discord.Member, guild_cfg: dict[str, Any]) -> bool:
    perms = member.guild_permissions
    if perms.administrator or perms.manage_guild:
        return True

    mod_role_ids = set(guild_cfg.get("mod_role_ids", []))
    return any(role.id in mod_role_ids for role in member.roles)


def can_manage_target(actor: discord.Member, target: discord.Member) -> bool:
    if actor.guild.owner_id == actor.id:
        return True
    return actor.top_role > target.top_role


def render_welcome_message(guild_cfg: dict[str, Any], member: discord.Member) -> str:
    template = guild_cfg.get("welcome_message")
    if not isinstance(template, str) or not template.strip():
        return f"Welcome {member.mention} to **{member.guild.name}**."
    return (
        template.replace("{user}", member.mention)
        .replace("{server}", member.guild.name)
        .replace("{username}", member.display_name)
    )


def build_welcome_embed(member: discord.Member, guild_cfg: dict[str, Any]) -> discord.Embed:
    embed = discord.Embed(
        title="Welcome",
        description=render_welcome_message(guild_cfg, member),
        color=discord.Color.red(),
    )
    embed.add_field(
        name="Get Started",
        value="Read the server rules and introduce yourself.",
        inline=False,
    )
    embed.add_field(
        name="Tip",
        value="Use the right channel for the right topic.",
        inline=False,
    )
    embed.set_footer(text="Glad to have you here.")
    try:
        embed.set_thumbnail(url=member.display_avatar.url)
    except Exception:
        pass
    return embed


def resolve_welcome_channel(
    guild: discord.Guild,
    preferred_channel_id: int | None,
) -> discord.TextChannel | None:
    bot_member = get_bot_member(guild)
    if not bot_member:
        return None

    def can_send(channel: discord.abc.GuildChannel) -> bool:
        if not isinstance(channel, discord.TextChannel):
            return False
        perms = channel.permissions_for(bot_member)
        return perms.send_messages and perms.embed_links

    if preferred_channel_id:
        preferred = guild.get_channel(preferred_channel_id)
        if preferred and can_send(preferred):
            return preferred

    if guild.system_channel and can_send(guild.system_channel):
        return guild.system_channel

    for channel in guild.text_channels:
        if can_send(channel):
            return channel
    return None


def get_active_prefix(guild: discord.Guild | None) -> str:
    if guild is None:
        return BOT_PREFIX
    guild_cfg = get_guild_config(guild.id)
    guild_prefix = guild_cfg.get("prefix")
    if isinstance(guild_prefix, str) and guild_prefix:
        return guild_prefix
    return BOT_PREFIX


def dynamic_prefix(bot_instance: commands.Bot, message: discord.Message):
    return commands.when_mentioned_or(get_active_prefix(message.guild))(bot_instance, message)


async def send_backend_user_update(
    ctx: commands.Context,
    target: str,
    endpoint: str,
    success_text: str,
) -> None:
    user_id = extract_id(target)
    if not user_id:
        await ctx.send("⚠️ Provide a mention (`@user`) or the raw numeric ID.")
        return

    payload = {"userId": user_id}
    try:
        response = await asyncio.to_thread(
            requests.post,
            endpoint,
            json=payload,
            timeout=6,
        )
    except requests.exceptions.RequestException as exc:
        await ctx.send(f"❌ Request error: `{exc}`.")
        return

    if response.status_code != 200:
        await ctx.send(
            f"⚠️ Request failed for <@{user_id}>. "
            f"Status: {response.status_code}. Response: ```{response.text}```"
        )
        return

    mention = f"<@{user_id}>"
    if ctx.guild:
        try:
            member = await ctx.guild.fetch_member(int(user_id))
            mention = member.mention
        except Exception:
            pass

    backend_msg = ""
    try:
        backend_msg = response.json().get("message", "")
    except Exception:
        backend_msg = response.text

    await ctx.send(f"✅ {mention} {success_text}. `{backend_msg}`")


async def require_guild_context(
    ctx: commands.Context,
) -> tuple[discord.Guild, dict[str, Any]] | None:
    if ctx.guild is None:
        await ctx.send("⚠️ This command can only be used in a server.")
        return None
    return ctx.guild, get_guild_config(ctx.guild.id)


async def require_mod_context(
    ctx: commands.Context,
) -> tuple[discord.Guild, dict[str, Any]] | None:
    result = await require_guild_context(ctx)
    if not result:
        return None

    guild, guild_cfg = result
    if not isinstance(ctx.author, discord.Member):
        await ctx.send("⚠️ This command can only be used in a server.")
        return None
    if not has_mod_access(ctx.author, guild_cfg):
        await ctx.send("⛔ You do not have permission to run this command.")
        return None
    return guild, guild_cfg


async def set_lockdown_state(ctx: commands.Context, locked: bool) -> None:
    result = await require_mod_context(ctx)
    if not result:
        return

    guild, guild_cfg = result
    target_role_id = guild_cfg.get("lockdown_role_id")
    target_role = resolve_role(guild, target_role_id) if target_role_id else guild.default_role
    if target_role is None:
        await ctx.send("⚠️ Configured lockdown role no longer exists. Set it again with `!setlockdownrole`.")
        return

    updated = 0
    failed = 0
    for channel in guild.text_channels:
        overwrite = channel.overwrites_for(target_role)
        overwrite.send_messages = not locked
        try:
            await channel.set_permissions(target_role, overwrite=overwrite)
            updated += 1
        except Exception:
            failed += 1

    title = "EMERGENCY LOCKDOWN" if locked else "LOCKDOWN LIFTED"
    description = (
        "Member communication has been disabled for the configured role."
        if locked
        else "Members can communicate normally again."
    )
    color = discord.Color.red() if locked else discord.Color.green()
    embed = discord.Embed(title=title, description=description, color=color)
    embed.add_field(name="Channels updated", value=str(updated), inline=True)
    embed.add_field(name="Channels failed", value=str(failed), inline=True)
    embed.set_footer(text=f"Triggered by {ctx.author.display_name}")
    await ctx.send(embed=embed)


async def dispatch_due_reminders() -> None:
    global reminders
    now = datetime.now(timezone.utc)
    due: list[dict[str, Any]] = []
    future: list[dict[str, Any]] = []

    for reminder in reminders:
        due_at = parse_datetime_utc(reminder.get("due_at"))
        if due_at is None or due_at <= now:
            due.append(reminder)
        else:
            future.append(reminder)

    if len(future) != len(reminders):
        reminders = future
        save_reminders()

    for reminder in due:
        user_id = reminder["user_id"]
        channel_id = reminder["channel_id"]
        body = reminder["message"]
        content = f"⏰ Reminder for <@{user_id}>: {body}"

        delivered = False
        channel = bot.get_channel(channel_id)
        if channel and hasattr(channel, "send"):
            try:
                await channel.send(content)
                delivered = True
            except Exception:
                delivered = False

        if delivered:
            continue

        user = bot.get_user(user_id)
        if user is None:
            try:
                user = await bot.fetch_user(user_id)
            except Exception:
                user = None
        if user:
            try:
                await user.send(content)
            except Exception:
                pass


async def reminder_worker() -> None:
    while not bot.is_closed():
        try:
            await dispatch_due_reminders()
        except Exception as exc:
            print(f"[REMINDER] Worker error: {exc}")
        await asyncio.sleep(REMINDER_CHECK_SECONDS)


intents = discord.Intents.default()
intents.message_content = True
intents.guilds = True
intents.members = True

bot = commands.Bot(
    command_prefix=dynamic_prefix,
    intents=intents,
    help_command=None,
    case_insensitive=True,
)
setup_vote_module(bot)

startup_initialized = False
reminder_loop_task: asyncio.Task | None = None


@bot.check
async def global_owner_check(ctx: commands.Context) -> bool:
    if not settings.get("owner_only", False):
        return True
    return await bot.is_owner(ctx.author)


@bot.event
async def on_ready():
    global startup_initialized, reminder_loop_task
    print(f"[READY] Logged in as {bot.user} ({bot.user.id}) in {len(bot.guilds)} guild(s)")

    if startup_initialized:
        return

    try:
        await restore_vote_state(bot)
    except Exception as exc:
        print(f"[VOTE] Restore failed: {exc}")

    if reminder_loop_task is None or reminder_loop_task.done():
        reminder_loop_task = asyncio.create_task(reminder_worker())

    startup_initialized = True


@bot.event
async def on_member_join(member: discord.Member):
    guild_cfg = get_guild_config(member.guild.id)
    channel = resolve_welcome_channel(member.guild, guild_cfg.get("welcome_channel_id"))
    if channel is None:
        return

    try:
        await channel.send(embed=build_welcome_embed(member, guild_cfg))
    except Exception:
        return

    role = resolve_role(member.guild, guild_cfg.get("welcome_role_id"))
    if role:
        try:
            await member.add_roles(role, reason="Auto-assign welcome role")
        except Exception:
            pass

    mentions = []
    for role_id in guild_cfg.get("mod_role_ids", []):
        mod_role = resolve_role(member.guild, role_id)
        if mod_role:
            mentions.append(mod_role.mention)
    if mentions:
        await channel.send(f"New member joined: {member.mention}. Heads up {', '.join(mentions)}.")


@bot.event
async def on_command_error(ctx: commands.Context, error: commands.CommandError):
    if hasattr(ctx.command, "on_error"):
        return

    err = getattr(error, "original", error)

    if isinstance(err, commands.CommandNotFound):
        return
    if isinstance(err, commands.NotOwner):
        await ctx.send("⛔ This command is owner-only.")
        return
    if isinstance(err, commands.MissingPermissions):
        missing = ", ".join(err.missing_permissions)
        await ctx.send(f"⛔ Missing required permissions: `{missing}`.")
        return
    if isinstance(err, commands.BotMissingPermissions):
        missing = ", ".join(err.missing_permissions)
        await ctx.send(f"⛔ I am missing permissions: `{missing}`.")
        return
    if isinstance(err, commands.CommandOnCooldown):
        await ctx.send(f"⏳ Slow down. Try again in `{err.retry_after:.1f}` seconds.")
        return
    if isinstance(err, commands.MissingRequiredArgument):
        usage = f"{ctx.clean_prefix}{ctx.command.qualified_name} {ctx.command.signature}"
        await ctx.send(f"⚠️ Missing argument `{err.param.name}`.\nUsage: `{usage}`")
        return
    if isinstance(
        err,
        (
            commands.BadArgument,
            commands.BadUnionArgument,
            commands.MemberNotFound,
            commands.RoleNotFound,
            commands.ChannelNotFound,
        ),
    ):
        await ctx.send("⚠️ Invalid argument. Check the command format and try again.")
        return

    print("[ERROR] Unhandled command error:")
    traceback.print_exception(type(err), err, err.__traceback__)
    await ctx.send("❌ Unexpected error while running that command.")


@bot.command(name="help")
async def help_command(ctx: commands.Context, *, command_name: str | None = None):
    """Show help for all commands or a specific command."""
    if command_name:
        command = bot.get_command(command_name.lower())
        if command is None or command.hidden:
            await ctx.send("❌ Command not found.")
            return

        usage = f"{ctx.clean_prefix}{command.qualified_name} {command.signature}".strip()
        embed = discord.Embed(title=f"Help: {command.qualified_name}", color=discord.Color.red())
        embed.add_field(name="Description", value=command.help or "No description.", inline=False)
        embed.add_field(name="Usage", value=f"`{usage}`", inline=False)
        if command.aliases:
            embed.add_field(name="Aliases", value=", ".join(f"`{alias}`" for alias in command.aliases), inline=False)
        await ctx.send(embed=embed)
        return

    prefix = get_active_prefix(ctx.guild)
    embed = discord.Embed(title="Vanguard Bot Help", color=discord.Color.red())
    embed.description = (
        f"Prefix: `{prefix}` (or mention the bot)\n"
        f"Use `{prefix}help <command>` for detailed help."
    )
    embed.add_field(
        name="General",
        value="`help` `ping` `uptime` `botstats` `serverinfo` `userinfo` `avatar` `voteinfo` `activevotes`",
        inline=False,
    )
    embed.add_field(
        name="Community",
        value="`rules` `mcstatus` `poll` `choose` `roll` `remindme` `reminders` `cancelreminder`",
        inline=False,
    )
    embed.add_field(
        name="Moderation",
        value="`lockdown` `unlock` `purge` `slowmode` `nick` `timeout` `untimeout`",
        inline=False,
    )
    embed.add_field(
        name="Configuration",
        value=(
            "`showconfig` `prefix` `setwelcomechannel` `setwelcomerole` "
            "`setwelcomemessage` `setlockdownrole` `setmodroles` `setmcserver` `clearmcserver`"
        ),
        inline=False,
    )
    embed.add_field(
        name="Owner",
        value="`owneronly` `flaguser` `unflaguser`",
        inline=False,
    )
    await ctx.send(embed=embed)


@bot.command()
async def ping(ctx: commands.Context):
    """Show bot latency."""
    await ctx.send(f"🏓 Pong: `{round(bot.latency * 1000)}ms`")


@bot.command()
async def uptime(ctx: commands.Context):
    """Show bot uptime."""
    elapsed = int((datetime.now(timezone.utc) - START_TIME).total_seconds())
    await ctx.send(f"⏱️ Uptime: `{format_duration(elapsed)}`")


@bot.command()
async def botstats(ctx: commands.Context):
    """Show overall bot stats."""
    unique_user_count = len({member.id for guild in bot.guilds for member in guild.members})
    embed = discord.Embed(title="Bot Stats", color=discord.Color.red())
    embed.add_field(name="Guilds", value=str(len(bot.guilds)), inline=True)
    embed.add_field(name="Unique Users", value=str(unique_user_count), inline=True)
    embed.add_field(name="Latency", value=f"{round(bot.latency * 1000)}ms", inline=True)
    embed.add_field(name="Uptime", value=format_duration(int((datetime.now(timezone.utc) - START_TIME).total_seconds())), inline=False)
    await ctx.send(embed=embed)


@bot.command()
async def avatar(ctx: commands.Context, member: discord.Member | None = None):
    """Show a user's avatar."""
    target = member or ctx.author
    if not isinstance(target, (discord.Member, discord.User)):
        await ctx.send("❌ Could not resolve user.")
        return
    embed = discord.Embed(title=f"Avatar: {target.display_name if isinstance(target, discord.Member) else target.name}", color=discord.Color.red())
    embed.set_image(url=target.display_avatar.url)
    await ctx.send(embed=embed)


@bot.command()
async def userinfo(ctx: commands.Context, member: discord.Member | None = None):
    """Show information about a server member."""
    if ctx.guild is None:
        await ctx.send("⚠️ This command can only be used in a server.")
        return
    target = member or ctx.author
    if not isinstance(target, discord.Member):
        await ctx.send("❌ Could not resolve member.")
        return
    roles = [role.mention for role in target.roles if role != ctx.guild.default_role]
    embed = discord.Embed(title=f"User Info: {target}", color=discord.Color.red())
    embed.add_field(name="User ID", value=str(target.id), inline=True)
    embed.add_field(name="Joined Server", value=target.joined_at.strftime("%Y-%m-%d %H:%M:%S") if target.joined_at else "Unknown", inline=True)
    embed.add_field(name="Account Created", value=target.created_at.strftime("%Y-%m-%d %H:%M:%S"), inline=True)
    embed.add_field(name="Top Role", value=target.top_role.mention if target.top_role else "None", inline=True)
    embed.add_field(name="Roles", value=", ".join(roles[-10:]) if roles else "None", inline=False)
    embed.set_thumbnail(url=target.display_avatar.url)
    await ctx.send(embed=embed)


@bot.command()
async def choose(ctx: commands.Context, *, options: str):
    """Choose randomly from options separated by |."""
    choices = [choice.strip() for choice in options.split("|") if choice.strip()]
    if len(choices) < 2:
        await ctx.send("⚠️ Provide at least 2 options separated by `|`.")
        return
    await ctx.send(f"🎲 I choose: **{random.choice(choices)}**")


@bot.command()
async def roll(ctx: commands.Context, notation: str = "1d6"):
    """Roll dice using NdS notation, e.g. 2d20."""
    match = re.fullmatch(r"(\d{1,2})d(\d{1,4})", notation.lower().strip())
    if not match:
        await ctx.send("⚠️ Invalid format. Use `NdS`, for example `2d6` or `1d20`.")
        return
    count = int(match.group(1))
    sides = int(match.group(2))
    if count < 1 or count > 20 or sides < 2 or sides > 1000:
        await ctx.send("⚠️ Dice limits: `1-20` dice and `2-1000` sides.")
        return
    rolls = [random.randint(1, sides) for _ in range(count)]
    await ctx.send(f"🎲 Rolled `{notation}`: {', '.join(map(str, rolls))} (total: **{sum(rolls)}**)")


@bot.command()
async def poll(ctx: commands.Context, *, content: str):
    """Create a poll. Format: question | option1 | option2 ... (or yes/no with question only)."""
    parts = [part.strip() for part in content.split("|") if part.strip()]
    if not parts:
        await ctx.send("⚠️ Provide a poll question.")
        return

    if len(parts) == 1:
        question = parts[0]
        embed = discord.Embed(title="Poll", description=question, color=discord.Color.red())
        embed.set_footer(text=f"Poll by {ctx.author.display_name}")
        message = await ctx.send(embed=embed)
        await message.add_reaction("👍")
        await message.add_reaction("👎")
        return

    question = parts[0]
    options = parts[1:]
    if len(options) < 2 or len(options) > 10:
        await ctx.send("⚠️ Polls with options must have between 2 and 10 options.")
        return

    number_emojis = ["1️⃣", "2️⃣", "3️⃣", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣", "🔟"]
    description = "\n".join(f"{number_emojis[i]} {option}" for i, option in enumerate(options))
    embed = discord.Embed(title="Poll", description=f"**{question}**\n\n{description}", color=discord.Color.red())
    embed.set_footer(text=f"Poll by {ctx.author.display_name}")
    message = await ctx.send(embed=embed)
    for i in range(len(options)):
        await message.add_reaction(number_emojis[i])


@bot.command()
async def remindme(ctx: commands.Context, duration: str, *, message: str):
    """Create a reminder. Example: !remindme 2h30m stretch."""
    seconds = parse_duration_to_seconds(duration)
    if seconds is None or seconds <= 0:
        await ctx.send("⚠️ Invalid duration. Example: `10m`, `2h30m`, `1d`.")
        return
    if seconds > MAX_REMINDER_SECONDS:
        await ctx.send("⚠️ Maximum reminder duration is 30 days.")
        return

    reminder_id = create_reminder_id()
    due_at = datetime.now(timezone.utc) + timedelta(seconds=seconds)
    reminders.append(
        {
            "id": reminder_id,
            "user_id": ctx.author.id,
            "channel_id": ctx.channel.id,
            "guild_id": ctx.guild.id if ctx.guild else None,
            "message": message.strip()[:300],
            "due_at": due_at.isoformat(),
        }
    )
    save_reminders()
    await ctx.send(
        f"✅ Reminder `{reminder_id}` set for `{format_duration(seconds)}` from now "
        f"(<t:{int(due_at.timestamp())}:R>)."
    )


@bot.command(name="reminders")
async def list_reminders(ctx: commands.Context):
    """List your active reminders."""
    mine = [reminder for reminder in reminders if reminder["user_id"] == ctx.author.id]
    if not mine:
        await ctx.send("You have no active reminders.")
        return

    lines = []
    now = datetime.now(timezone.utc)
    for reminder in sorted(mine, key=lambda item: item["due_at"])[:10]:
        due_at = parse_datetime_utc(reminder["due_at"]) or now
        remaining_seconds = int((due_at - now).total_seconds())
        lines.append(
            f"`{reminder['id']}` • in `{format_duration(remaining_seconds)}` • {reminder['message']}"
        )
    await ctx.send("**Your reminders:**\n" + "\n".join(lines))


@bot.command(name="cancelreminder", aliases=["delreminder"])
async def cancel_reminder(ctx: commands.Context, reminder_id: int):
    """Cancel one of your reminders by ID."""
    global reminders
    before_count = len(reminders)
    reminders = [
        reminder
        for reminder in reminders
        if not (reminder["id"] == reminder_id and reminder["user_id"] == ctx.author.id)
    ]
    if len(reminders) == before_count:
        await ctx.send("❌ Reminder not found.")
        return
    save_reminders()
    await ctx.send(f"✅ Reminder `{reminder_id}` canceled.")


@bot.command()
@commands.has_permissions(manage_messages=True)
async def purge(ctx: commands.Context, amount: int):
    """Delete recent messages in the current channel."""
    if ctx.guild is None or not isinstance(ctx.channel, discord.TextChannel):
        await ctx.send("⚠️ This command can only be used in a text channel.")
        return
    if amount < 1 or amount > 200:
        await ctx.send("⚠️ Amount must be between 1 and 200.")
        return
    deleted = await ctx.channel.purge(limit=amount + 1)
    confirmation = await ctx.send(f"🧹 Deleted `{max(len(deleted) - 1, 0)}` messages.")
    await asyncio.sleep(4)
    try:
        await confirmation.delete()
    except Exception:
        pass


@bot.command()
@commands.has_permissions(manage_channels=True)
async def slowmode(ctx: commands.Context, seconds: int):
    """Set channel slowmode in seconds (0 disables)."""
    if ctx.guild is None or not isinstance(ctx.channel, discord.TextChannel):
        await ctx.send("⚠️ This command can only be used in a text channel.")
        return
    if seconds < 0 or seconds > 21600:
        await ctx.send("⚠️ Slowmode must be between 0 and 21600 seconds.")
        return
    await ctx.channel.edit(slowmode_delay=seconds)
    await ctx.send(f"✅ Slowmode set to `{seconds}` second(s).")


@bot.command()
@commands.has_permissions(manage_nicknames=True)
async def nick(ctx: commands.Context, member: discord.Member, *, nickname: str | None = None):
    """Change or clear a member nickname."""
    if ctx.guild is None or not isinstance(ctx.author, discord.Member):
        await ctx.send("⚠️ This command can only be used in a server.")
        return
    bot_member = get_bot_member(ctx.guild)
    if bot_member is None or bot_member.top_role <= member.top_role:
        await ctx.send("⛔ I cannot edit that member's nickname.")
        return
    if not can_manage_target(ctx.author, member):
        await ctx.send("⛔ You cannot edit that member's nickname.")
        return
    await member.edit(nick=nickname[:32] if nickname else None, reason=f"Requested by {ctx.author}")
    if nickname:
        await ctx.send(f"✅ Updated nickname for {member.mention} to `{nickname[:32]}`.")
    else:
        await ctx.send(f"✅ Cleared nickname for {member.mention}.")


@bot.command()
@commands.has_permissions(moderate_members=True)
async def timeout(ctx: commands.Context, member: discord.Member, duration: str, *, reason: str | None = None):
    """Timeout a member. Example: !timeout @user 30m spam"""
    if ctx.guild is None or not isinstance(ctx.author, discord.Member):
        await ctx.send("⚠️ This command can only be used in a server.")
        return
    if member.id == ctx.author.id:
        await ctx.send("⚠️ You cannot timeout yourself.")
        return
    seconds = parse_duration_to_seconds(duration)
    if seconds is None or seconds <= 0:
        await ctx.send("⚠️ Invalid duration. Example: `10m`, `2h`, `1d`.")
        return
    if seconds > MAX_TIMEOUT_SECONDS:
        await ctx.send("⚠️ Maximum timeout duration is 28 days.")
        return

    bot_member = get_bot_member(ctx.guild)
    if bot_member is None or bot_member.top_role <= member.top_role:
        await ctx.send("⛔ I cannot timeout that member.")
        return
    if not can_manage_target(ctx.author, member):
        await ctx.send("⛔ You cannot timeout that member.")
        return

    until = datetime.now(timezone.utc) + timedelta(seconds=seconds)
    await member.timeout(until, reason=reason or f"Timed out by {ctx.author}")
    await ctx.send(
        f"✅ {member.mention} timed out for `{format_duration(seconds)}` "
        f"(<t:{int(until.timestamp())}:R>)."
    )


@bot.command()
@commands.has_permissions(moderate_members=True)
async def untimeout(ctx: commands.Context, member: discord.Member, *, reason: str | None = None):
    """Remove a member timeout."""
    if ctx.guild is None or not isinstance(ctx.author, discord.Member):
        await ctx.send("⚠️ This command can only be used in a server.")
        return
    bot_member = get_bot_member(ctx.guild)
    if bot_member is None or bot_member.top_role <= member.top_role:
        await ctx.send("⛔ I cannot modify that member.")
        return
    if not can_manage_target(ctx.author, member):
        await ctx.send("⛔ You cannot modify that member.")
        return

    await member.timeout(None, reason=reason or f"Timeout removed by {ctx.author}")
    await ctx.send(f"✅ Timeout removed for {member.mention}.")


@bot.command()
@commands.cooldown(3, 30, commands.BucketType.user)
async def vanguard(ctx: commands.Context, *, question: str):
    """Ask the AI server a question and get a response."""
    async with ctx.typing():
        payload = {
            "question": question,
            "username": str(ctx.author),
            "userId": str(ctx.author.id),
        }
        title_text = "AI Response"
        answer = ""
        try:
            response = await asyncio.to_thread(
                requests.post,
                AI_SERVER_URL,
                json=payload,
                timeout=20,
            )
            if response.status_code == 200:
                data = response.json()
                answer = data.get("answer", "No response from the AI service.")
            else:
                title_text = "AI service returned an error."
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
            title_text = "AI service is currently unreachable."
        except Exception:
            title_text = "Unexpected error while contacting the AI service."

    embed = discord.Embed(title=title_text, description=answer, color=discord.Color.red())
    embed.set_footer(text="Powered by Vanguard AI")
    await ctx.send(embed=embed)


@bot.command(name="flaguser", aliases=["fuck"])
@commands.is_owner()
async def flaguser(ctx: commands.Context, target: str):
    """Owner-only: mark a user in backend moderation service."""
    await send_backend_user_update(ctx, target, FLAG_USER_URL, "has been flagged")


@bot.command(name="unflaguser", aliases=["unfuck"])
@commands.is_owner()
async def unflaguser(ctx: commands.Context, target: str):
    """Owner-only: remove a backend moderation flag for a user."""
    await send_backend_user_update(ctx, target, UNFLAG_USER_URL, "has been unflagged")


@bot.command()
@commands.is_owner()
async def owneronly(ctx: commands.Context, state: str | None = None):
    """Owner-only: toggle global owner-only mode. Usage: !owneronly on|off"""
    if state is None:
        status = "ON" if settings.get("owner_only", False) else "OFF"
        await ctx.send(f"Owner-only mode is currently `{status}`.")
        return

    normalized = state.lower().strip()
    if normalized in {"on", "enable", "enabled", "true"}:
        settings["owner_only"] = True
    elif normalized in {"off", "disable", "disabled", "false"}:
        settings["owner_only"] = False
    else:
        await ctx.send("⚠️ Use `!owneronly on` or `!owneronly off`.")
        return

    save_settings()
    await ctx.send(f"Owner-only mode set to `{'ON' if settings['owner_only'] else 'OFF'}`.")


@bot.command()
async def testwelcome(ctx: commands.Context, target: discord.Member | None = None):
    """Send a preview welcome embed."""
    result = await require_guild_context(ctx)
    if not result:
        return
    _, guild_cfg = result
    member = target or ctx.author
    if not isinstance(member, discord.Member):
        await ctx.send("⚠️ Could not resolve member.")
        return
    await ctx.send(embed=build_welcome_embed(member, guild_cfg))


@bot.command()
async def rules(ctx: commands.Context):
    embed = discord.Embed(
        title="Server Rules",
        description=(
            "1. Be respectful.\n"
            "2. No spam.\n"
            "3. Keep content in the correct channels.\n"
            "4. No harassment or hate speech.\n"
            "5. Follow moderator instructions."
        ),
        color=discord.Color.red(),
    )
    await ctx.send(embed=embed)


@bot.command()
async def serverinfo(ctx: commands.Context):
    if ctx.guild is None:
        await ctx.send("⚠️ This command can only be used in a server.")
        return
    guild = ctx.guild
    owner = guild.get_member(guild.owner_id)
    if owner is None:
        try:
            owner = await guild.fetch_member(guild.owner_id)
        except Exception:
            owner = None

    embed = discord.Embed(title=f"Server Info: {guild.name}", color=discord.Color.red())
    embed.add_field(name="Owner", value=owner.mention if owner else "Unknown", inline=False)
    embed.add_field(name="Created", value=guild.created_at.strftime("%Y-%m-%d %H:%M:%S"), inline=False)
    embed.add_field(name="Members", value=str(guild.member_count), inline=False)
    embed.add_field(name="Server ID", value=str(guild.id), inline=False)
    if guild.icon:
        embed.set_thumbnail(url=guild.icon.url)
    await ctx.send(embed=embed)


@bot.command()
async def mcstatus(ctx: commands.Context):
    result = await require_guild_context(ctx)
    if not result:
        return
    _, guild_cfg = result
    guild_host = guild_cfg.get("mc_host")
    if guild_host:
        host = guild_host
        port = guild_cfg.get("mc_port", 25565)
    else:
        host = MC_DEFAULT_HOST
        port = MC_DEFAULT_PORT

    if not host:
        await ctx.send("⚠️ Minecraft server is not configured. Use `!setmcserver <host> [port]`.")
        return

    server = JavaServer.lookup(f"{host}:{port}")
    try:
        status = server.status()
        await ctx.send(
            f"✅ Minecraft server `{host}:{port}` is online. "
            f"Players: {status.players.online}/{status.players.max}"
        )
    except Exception:
        await ctx.send(f"❌ Minecraft server `{host}:{port}` is offline or unreachable.")


@bot.command()
async def lockdown(ctx: commands.Context):
    await set_lockdown_state(ctx, True)


@bot.command()
async def unlock(ctx: commands.Context):
    await set_lockdown_state(ctx, False)


@bot.command(name="prefix")
async def prefix_command(ctx: commands.Context, new_prefix: str | None = None):
    """Show or set this server's command prefix."""
    if ctx.guild is None:
        await ctx.send(f"Prefix in DMs is `{BOT_PREFIX}`.")
        return

    guild_cfg = get_guild_config(ctx.guild.id)
    current_prefix = get_active_prefix(ctx.guild)
    if new_prefix is None:
        await ctx.send(f"Current prefix for this server is `{current_prefix}`.")
        return

    if not isinstance(ctx.author, discord.Member) or not has_mod_access(ctx.author, guild_cfg):
        await ctx.send("⛔ You do not have permission to change the prefix.")
        return

    candidate = new_prefix.strip()
    if candidate.lower() in {"reset", "default"}:
        guild_cfg["prefix"] = None
        save_settings()
        await ctx.send(f"✅ Prefix reset to default `{BOT_PREFIX}`.")
        return
    if not candidate:
        await ctx.send("⚠️ Prefix cannot be empty.")
        return
    if len(candidate) > MAX_PREFIX_LENGTH:
        await ctx.send(f"⚠️ Prefix cannot be longer than {MAX_PREFIX_LENGTH} characters.")
        return
    if candidate.startswith("<@"):
        await ctx.send("⚠️ Prefix cannot be a mention.")
        return

    guild_cfg["prefix"] = candidate
    save_settings()
    await ctx.send(f"✅ Prefix updated from `{current_prefix}` to `{candidate}`.")


@bot.command(name="setwelcomechannel")
async def setwelcomechannel(ctx: commands.Context, channel: discord.TextChannel | None = None):
    """Mod/admin: set welcome channel. Omit to clear."""
    result = await require_mod_context(ctx)
    if not result:
        return
    _, guild_cfg = result
    guild_cfg["welcome_channel_id"] = channel.id if channel else None
    save_settings()
    if channel:
        await ctx.send(f"✅ Welcome channel set to {channel.mention}.")
    else:
        await ctx.send("✅ Welcome channel cleared. System/default channel fallback will be used.")


@bot.command(name="setwelcomerole")
async def setwelcomerole(ctx: commands.Context, role: discord.Role | None = None):
    """Mod/admin: set role to auto-assign on join. Omit to clear."""
    result = await require_mod_context(ctx)
    if not result:
        return
    _, guild_cfg = result
    guild_cfg["welcome_role_id"] = role.id if role else None
    save_settings()
    if role:
        await ctx.send(f"✅ Welcome role set to `{role.name}`.")
    else:
        await ctx.send("✅ Welcome role cleared.")


@bot.command(name="setwelcomemessage")
async def setwelcomemessage(ctx: commands.Context, *, message: str | None = None):
    """Mod/admin: set welcome message. Supports {user}, {username}, {server}. Use `clear` to reset."""
    result = await require_mod_context(ctx)
    if not result:
        return
    _, guild_cfg = result
    if message is None or message.strip().lower() in {"clear", "reset", "default"}:
        guild_cfg["welcome_message"] = None
        save_settings()
        await ctx.send("✅ Welcome message reset to default.")
        return
    guild_cfg["welcome_message"] = message.strip()[:500]
    save_settings()
    await ctx.send("✅ Welcome message updated.")


@bot.command(name="setlockdownrole")
async def setlockdownrole(ctx: commands.Context, role: discord.Role | None = None):
    """Mod/admin: set role targeted by lockdown. Omit to use @everyone."""
    result = await require_mod_context(ctx)
    if not result:
        return
    guild, guild_cfg = result
    guild_cfg["lockdown_role_id"] = role.id if role else None
    save_settings()
    if role:
        await ctx.send(f"✅ Lockdown role set to `{role.name}`.")
    else:
        await ctx.send(f"✅ Lockdown role reset to default `{guild.default_role.name}`.")


@bot.command(name="setmodroles")
async def setmodroles(ctx: commands.Context, *roles: discord.Role):
    """Mod/admin: set additional roles allowed to run moderation/config commands."""
    result = await require_mod_context(ctx)
    if not result:
        return
    _, guild_cfg = result
    guild_cfg["mod_role_ids"] = sorted({role.id for role in roles})
    save_settings()
    if roles:
        await ctx.send("✅ Mod roles set to: " + ", ".join(f"`{role.name}`" for role in roles))
    else:
        await ctx.send("✅ Mod role list cleared. Only Manage Server/Admin can run mod commands.")


@bot.command(name="setmcserver")
async def setmcserver(ctx: commands.Context, host: str, port: int = 25565):
    """Mod/admin: configure this guild's Minecraft server host/port."""
    result = await require_mod_context(ctx)
    if not result:
        return
    _, guild_cfg = result
    if not 1 <= port <= 65535:
        await ctx.send("⚠️ Port must be between 1 and 65535.")
        return
    guild_cfg["mc_host"] = host.strip()
    guild_cfg["mc_port"] = port
    save_settings()
    await ctx.send(f"✅ Minecraft server set to `{guild_cfg['mc_host']}:{guild_cfg['mc_port']}`.")


@bot.command(name="clearmcserver")
async def clearmcserver(ctx: commands.Context):
    """Mod/admin: clear this guild's Minecraft server settings."""
    result = await require_mod_context(ctx)
    if not result:
        return
    _, guild_cfg = result
    guild_cfg["mc_host"] = None
    guild_cfg["mc_port"] = 25565
    save_settings()
    await ctx.send("✅ Minecraft server setting cleared for this server.")


@bot.command(name="showconfig")
async def showconfig(ctx: commands.Context):
    """Show active configuration for this server."""
    result = await require_guild_context(ctx)
    if not result:
        return
    guild, guild_cfg = result

    welcome_channel = guild.get_channel(guild_cfg.get("welcome_channel_id")) if guild_cfg.get("welcome_channel_id") else None
    welcome_role = resolve_role(guild, guild_cfg.get("welcome_role_id"))
    lockdown_role = resolve_role(guild, guild_cfg.get("lockdown_role_id")) if guild_cfg.get("lockdown_role_id") else guild.default_role
    mod_roles = [resolve_role(guild, role_id) for role_id in guild_cfg.get("mod_role_ids", [])]
    mod_roles = [role for role in mod_roles if role]

    if guild_cfg.get("mc_host"):
        host = guild_cfg["mc_host"]
        port = guild_cfg.get("mc_port", 25565)
    else:
        host = MC_DEFAULT_HOST
        port = MC_DEFAULT_PORT

    embed = discord.Embed(title="Server Bot Configuration", color=discord.Color.red())
    embed.add_field(name="Prefix", value=f"`{get_active_prefix(guild)}`", inline=False)
    embed.add_field(
        name="Welcome Channel",
        value=welcome_channel.mention if isinstance(welcome_channel, discord.TextChannel) else "Not set (fallback mode)",
        inline=False,
    )
    embed.add_field(name="Welcome Role", value=welcome_role.mention if welcome_role else "Not set", inline=False)
    embed.add_field(
        name="Welcome Message",
        value=guild_cfg.get("welcome_message") or "Default",
        inline=False,
    )
    embed.add_field(
        name="Lockdown Role",
        value=lockdown_role.mention if lockdown_role else "Missing role",
        inline=False,
    )
    embed.add_field(
        name="Mod Roles",
        value=", ".join(role.mention for role in mod_roles) if mod_roles else "Not set (Manage Server/Admin only)",
        inline=False,
    )
    embed.add_field(
        name="Minecraft Server",
        value=f"`{host}:{port}`" if host else "Not set",
        inline=False,
    )
    await ctx.send(embed=embed)


@bot.command(name="voteinfo")
async def voteinfo(ctx: commands.Context, vote_id: str):
    """Show who voted for what in a specific vote."""
    result = await require_guild_context(ctx)
    if not result:
        return
    guild, _ = result

    if not vote_id.startswith(f"{guild.id}-"):
        await ctx.send("❌ This vote ID does not belong to this server.")
        return

    vote = vote_store.get(vote_id)
    if not vote:
        await ctx.send("❌ Vote not found.")
        return

    lines = []
    for user_id, choice in vote.get("votes", {}).items():
        member = guild.get_member(int(user_id))
        username = member.display_name if member else f"Unknown ({user_id})"
        lines.append(f"{username}: {choice}")

    if not lines:
        await ctx.send("No votes yet.")
    else:
        await ctx.send("**Vote results so far:**\n" + "\n".join(lines))


@bot.command(name="activevotes")
async def activevotes(ctx: commands.Context):
    """List active votes in this server."""
    result = await require_guild_context(ctx)
    if not result:
        return
    guild, _ = result

    active = [
        (vote_id, vote)
        for vote_id, vote in vote_store.items()
        if vote_id.startswith(f"{guild.id}-")
    ]
    if not active:
        await ctx.send("No active votes in this server.")
        return

    lines = []
    for vote_id, vote in sorted(active, key=lambda item: item[0])[:10]:
        against_count = sum(1 for value in vote.get("votes", {}).values() if value == "against")
        support_count = sum(1 for value in vote.get("votes", {}).values() if value == "support")
        finish_at = parse_datetime_utc(vote.get("finish_at"))
        finish_text = f"<t:{int(finish_at.timestamp())}:R>" if finish_at else "unknown"
        lines.append(
            f"`{vote_id}`\nTarget: <@{vote.get('target_id', 'unknown')}> • "
            f"Against: {against_count} • Support: {support_count} • Ends: {finish_text}"
        )
    await ctx.send("**Active votes:**\n" + "\n\n".join(lines))


DISCORD_TOKEN = os.getenv("DISCORD_BOT_TOKEN")
if not DISCORD_TOKEN:
    raise RuntimeError("Missing DISCORD_BOT_TOKEN environment variable.")

bot.run(DISCORD_TOKEN)
