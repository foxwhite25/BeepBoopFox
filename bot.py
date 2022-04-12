import datetime
import json
import logging
import sys
import traceback
from collections import Counter, deque, defaultdict
from typing import Optional

import aiohttp
import qq
from qq.ext import commands

import config
from cogs import context
from cogs.utils.config import Config

description = """
你好! 我是一个由狐白写的机器人。
"""

logger = logging.getLogger(__name__)

initial_extensions = (
    "cogs.admin",
    "cogs.stats",
    "cogs.meta",
    "cogs.bilibili"
)


def _prefix_callable(bot, msg):
    user_id = bot.user.id
    base = [f'<@!{user_id}> ', f'<@{user_id}> ']
    if msg.guild is None:
        base.append('/')
    else:
        base.extend(bot.prefixes.get(msg.guild.id, ['/']))
    return base


class BeepBoopFox(commands.AutoShardedBot):
    def __init__(self):
        allowed_mentions = qq.AllowedMentions(roles=False, everyone=False, users=True)
        intents = qq.Intents(
            guilds=True,
            members=True,
            messages=True,
            at_guild_messages=False,
            guild_reactions=True,
            audit=True,
        )
        super().__init__(
            command_prefix=_prefix_callable,
            description=description,
            pm_help=None,
            help_attrs=dict(hidden=True),
            chunk_guilds_at_startup=False,
            heartbeat_timeout=150.0,
            allowed_mentions=allowed_mentions,
            intents=intents,
            enable_debug_events=True,
            owner_id=2229785998145077655,
            shard_count=2
        )

        self.bots_app_id = config.bots_app_id
        self.bots_token = config.bots_token
        self.session = aiohttp.ClientSession(loop=self.loop)

        self._prev_events = deque(maxlen=10)

        # shard_id: List[datetime.datetime]
        # shows the last attempted IDENTIFYs and RESUMEs
        self.resumes = defaultdict(list)
        self.identifies = defaultdict(list)

        # guild_id: list
        self.prefixes = Config('prefixes.json')

        # guild_id and user_id mapped to True
        # these are users and guilds globally blacklisted
        # from using the bot
        self.blacklist = Config('blacklist.json')

        self.bilibili = Config('bilibili.json')

        # in case of even further spam, add a cooldown mapping
        # for people who excessively spam commands
        self.spam_control = commands.CooldownMapping.from_cooldown(3, 6.0, commands.BucketType.user)

        # A counter to auto-ban frequent spammers
        # Triggering the rate limit 5 times in a row will auto-ban the user from the bot.
        self._auto_spam_count = Counter()
        for extension in initial_extensions:
            try:
                self.load_extension(extension)
            except Exception as e:
                print(f'Failed to load extension {extension}.', file=sys.stderr)
                traceback.print_exc()

    def _clear_gateway_data(self):
        one_week_ago = qq.utils.utcnow() - datetime.timedelta(days=7)
        for shard_id, dates in self.identifies.items():
            to_remove = [index for index, dt in enumerate(dates) if dt < one_week_ago]
            for index in reversed(to_remove):
                del dates[index]

        for shard_id, dates in self.resumes.items():
            to_remove = [index for index, dt in enumerate(dates) if dt < one_week_ago]
            for index in reversed(to_remove):
                del dates[index]

    async def on_socket_raw_receive(self, msg):
        self._prev_events.append(msg)

    async def before_identify_hook(self, shard_id: Optional[int], *, initial: bool = False) -> None:
        self._clear_gateway_data()
        self.identifies[shard_id].append(qq.utils.utcnow())
        await super().before_identify_hook(shard_id, initial=initial)

    async def on_command_error(self, ctx: commands.Context, error):
        if isinstance(error, commands.NoPrivateMessage):
            await ctx.author.send('此命令不能在私人消息中使用。', msg_id=ctx.message)
        elif isinstance(error, commands.DisabledCommand):
            await ctx.reply('对不起。此命令已禁用，无法使用。')
        elif isinstance(error, commands.CommandInvokeError):
            original = error.original
            if not isinstance(original, qq.HTTPException):
                print(f'In {ctx.command.qualified_name}:', file=sys.stderr)
                traceback.print_tb(original.__traceback__)
                print(f'{original.__class__.__name__}: {original}', file=sys.stderr)
        elif isinstance(error, commands.ArgumentParsingError):
            await ctx.reply(str(error))

    def get_guild_prefixes(self, guild, *, local_inject=_prefix_callable):
        proxy_msg = qq.Object(id=0)
        proxy_msg.guild = guild
        return local_inject(self, proxy_msg)

    def get_raw_guild_prefixes(self, guild_id):
        return self.prefixes.get(guild_id, ['/'])

    async def set_guild_prefixes(self, guild, prefixes):
        if len(prefixes) == 0:
            await self.prefixes.put(guild.id, [])
        elif len(prefixes) > 10:
            raise RuntimeError('自定义前缀不能超过 10 个。')
        else:
            await self.prefixes.put(guild.id, sorted(set(prefixes), reverse=True))

    async def add_to_blacklist(self, object_id):
        await self.blacklist.put(object_id, True)

    async def remove_from_blacklist(self, object_id):
        try:
            await self.blacklist.remove(object_id)
        except KeyError:
            pass

    async def query_member_named(self, guild: qq.Guild, argument: str):
        """Queries a member by their name, or nickname.

        Parameters
        ------------
        guild: Guild
            The guild to query the member in.
        argument: str
            The name, nickname combo to check.

        Returns
        ---------
        Optional[Member]
            The member matching the query or None if not found.
        """
        async for member in guild.fetch_members(limit=1000):
            if member.nick == argument or member.name == argument:
                return member

    async def get_or_fetch_member(self, guild: qq.Guild, member_id):
        """Looks up a member in cache or fetches if not found.

        Parameters
        -----------
        guild: Guild
            The guild to look in.
        member_id: int
            The member ID to search for.

        Returns
        ---------
        Optional[Member]
            The member or None if not found.
        """

        member = guild.get_member(member_id)
        if member is not None:
            return member

        shard = self.get_shard(guild.shard_id)
        try:
            member = await guild.fetch_member(member_id)
        except qq.HTTPException:
            return None
        else:
            return member

    async def resolve_member_ids(self, guild: qq.Guild, member_ids):
        """Bulk resolves' member IDs to member instances, if possible.

        Members that can't be resolved are discarded from the list.

        This is done lazily using an asynchronous iterator.

        Note that the order of the resolved members is not the same as the input.

        Parameters
        -----------
        guild: Guild
            The guild to resolve from.
        member_ids: Iterable[int]
            An iterable of member IDs.

        Yields
        --------
        Member
            The resolved members.
        """

        needs_resolution = []
        for member_id in member_ids:
            member = guild.get_member(member_id)
            if member is not None:
                yield member
            else:
                needs_resolution.append(member_id)

        for resolver in needs_resolution:
            try:
                member = await guild.fetch_member(resolver)
            except qq.HTTPException:
                pass
            else:
                yield member

    async def on_ready(self):
        if not hasattr(self, 'uptime'):
            self.uptime = qq.utils.utcnow()

        logger.info(f'Ready: {self.user} (ID: {self.user.id})')

    async def on_shard_resumed(self, shard_id):
        logger.info(f'Shard ID {shard_id} has resumed...')
        self.resumes[shard_id].append(qq.utils.utcnow())

    def log_spammer(self, ctx, message, retry_after, *, auto_block=False):
        guild_name = getattr(ctx.guild, 'name', 'No Guild (DMs)')
        guild_id = getattr(ctx.guild, 'id', None)
        fmt = '频道 %r (ID %s) 中的用户 %s (ID %s) 刷屏，retry_after: %.2fs'
        logger.warning(fmt, guild_name, guild_id, message.author, message.author.id, retry_after)
        if not auto_block:
            return

        embed = qq.Embed(title='自动封禁成员', colour=0xDDA453)
        embed.add_field(name=f'成员: {message.author} (ID: {message.author.id})', inline=False)
        embed.add_field(name=f'频道资讯: {guild_name} (ID: {guild_id})', inline=False)
        embed.add_field(name=f'子频道资讯: {message.channel} (ID: {message.channel.id})', inline=False)
        embed.timestamp = qq.utils.utcnow()
        embed.set_thumbnail(url=message.author.avatar.url)
        return self.get_channel(1697291).send(embed=embed, msg_id=ctx.message)

    async def process_commands(self, message):
        ctx = await self.get_context(message, cls=context.Context)

        if ctx.command is None:
            return

        if ctx.author.id in self.blacklist:
            return

        if ctx.guild is not None and ctx.guild.id in self.blacklist:
            return

        bucket = self.spam_control.get_bucket(message)
        current = message.created_at.timestamp()
        retry_after = bucket.update_rate_limit(current)
        author_id = message.author.id
        if retry_after and author_id != self.owner_id:
            self._auto_spam_count[author_id] += 1
            if self._auto_spam_count[author_id] >= 5:
                await self.add_to_blacklist(author_id)
                del self._auto_spam_count[author_id]
                await self.log_spammer(ctx, message, retry_after, auto_block=True)
            else:
                self.log_spammer(ctx, message, retry_after)
            return
        else:
            self._auto_spam_count.pop(author_id, None)

        try:
            await self.invoke(ctx)
        finally:
            # Just in case we have any outstanding DB connections
            await ctx.release()

    async def on_message(self, message):
        if message.author.bot:
            return
        await self.process_commands(message)

    async def on_guild_join(self, guild):
        if guild.id in self.blacklist:
            await guild.leave()

    async def close(self):
        await super().close()
        await self.session.close()

    def run(self):
        try:
            super().run(token=f'{config.bots_app_id}.{config.bots_token}', reconnect=True)
        finally:
            with open('prev_events.log', 'w', encoding='utf-8') as fp:
                for data in self._prev_events:
                    try:
                        x = json.dumps(json.loads(data), ensure_ascii=True, indent=4)
                    except:
                        fp.write(f'{data}\n')
                    else:
                        fp.write(f'{x}\n')

    @property
    def config(self):
        return __import__('config')
