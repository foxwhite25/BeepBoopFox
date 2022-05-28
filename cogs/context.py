from __future__ import annotations

import asyncio

import qq
from asyncpg import Pool
from qq.ext import commands


class _ContextDBAcquire:
    __slots__ = ('ctx', 'timeout')

    def __init__(self, ctx, timeout):
        self.ctx = ctx
        self.timeout = timeout

    def __await__(self):
        return self.ctx._acquire(self.timeout).__await__()

    async def __aenter__(self):
        await self.ctx._acquire(self.timeout)
        return self.ctx.db

    async def __aexit__(self, *args):
        await self.ctx.release()


class Context(commands.Context):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.pool: Pool = self.bot.pool
        self._db = None

    async def entry_to_code(self, entries):
        width = max(len(a) for a, b in entries)
        output = ['```']
        for name, entry in entries:
            output.append(f'{name:<{width}}: {entry}')
        output.append('```')
        await self.send('\n'.join(output))

    async def indented_entry_to_code(self, entries):
        width = max(len(a) for a, b in entries)
        output = ['```']
        for name, entry in entries:
            output.append(f'\u200b{name:>{width}}: {entry}')
        output.append('```')
        await self.send('\n'.join(output))

    def __repr__(self):
        # we need this for our cache key strategy
        return '<Context>'

    @property
    def session(self):
        return self.bot.session

    @qq.utils.cached_property
    def replied_reference(self):
        ref = self.message.reference
        if ref and isinstance(ref.resolved, qq.Message):
            return ref.resolved.to_reference()
        return None

    async def disambiguate(self, matches, entry):
        if len(matches) == 0:
            raise ValueError('No results found.')

        if len(matches) == 1:
            return matches[0]

        await self.send('There are too many matches... Which one did you mean? **Only say the number**.')
        await self.send('\n'.join(f'{index}: {entry(item)}' for index, item in enumerate(matches, 1)))

        def check(m):
            return m.content.isdigit() and m.author.id == self.author.id and m.channel.id == self.channel.id

        await self.release()

        # only give them 3 tries.
        try:
            for i in range(3):
                try:
                    message = await self.bot.wait_for('message', check=check, timeout=30.0)
                except asyncio.TimeoutError:
                    raise ValueError('Took too long. Goodbye.')

                index = int(message.content)
                try:
                    return matches[index - 1]
                except:
                    await self.send(f'Please give me a valid number. {2 - i} tries remaining...')

            raise ValueError('Too many tries. Goodbye.')
        finally:
            await self.acquire()

    @property
    def db(self):
        return self._db if self._db else self.pool

    async def _acquire(self, timeout):
        if self._db is None:
            self._db = await self.pool.acquire(timeout=timeout)
        return self._db

    def tick(self, opt, label=None):
        lookup = {
            True: '✔️',
            False: '❌',
            None: '🚫',
        }
        emoji = lookup.get(opt)
        if label is not None:
            return f'{emoji}: {label}'
        return emoji

    def acquire(self, *, timeout=300.0):
        """Acquires a database connection from the pool. e.g. ::

            async with ctx.acquire():
                await ctx.db.execute(...)

        or: ::

            await ctx.acquire()
            try:
                await ctx.db.execute(...)
            finally:
                await ctx.release()
        """
        return _ContextDBAcquire(self, timeout)

    async def release(self):
        """Releases the database connection from the pool.

        Useful if needed for "long" interactive commands where
        we want to release the connection and re-acquire later.

        Otherwise, this is called automatically by the bot.
        """
        # from source digging asyncpg source, releasing an already
        # released connection does nothing

        if self._db is not None:
            await self.bot.pool.release(self._db)
            self._db = None

    async def show_help(self, command=None):
        """Shows the help command for the specified command if given.

        If no command is given, then it'll show help for the current
        command.
        """
        cmd = self.bot.get_command('help')
        command = command or self.command.qualified_name
        await self.invoke(cmd, command=command)

    async def safe_send(self, content, *, escape_mentions=True, **kwargs):
        """Same as send except with some safe guards.

        1) If the message is too long then it sends a file with the results instead.
        2) If ``escape_mentions`` is ``True`` then it escapes mentions.
        """
        if escape_mentions:
            content = qq.utils.escape_mentions(content)

        return await self.reply(content)
