import asyncio
import copy
import importlib
import inspect
import io
import os
import re
import subprocess
import sys
import textwrap
import time
import traceback
from contextlib import redirect_stdout
from typing import Union, Optional

import qq
from qq.ext import commands


# to expose to the eval command


class PerformanceMocker:
    """A mock object that can also be used in await expressions."""

    def __init__(self):
        self.loop = asyncio.get_event_loop()

    def permissions_for(self, obj):
        # Lie and say we don't have permissions to embed
        # This makes it so pagination sessions just abruptly end on __init__
        # Most checks based on permission have a bypass for the owner anyway
        # So this lie will not affect the actual command invocation.
        perms = qq.Permissions.all()
        perms.administrator = False
        perms.embed_links = False
        perms.add_reactions = False
        return perms

    def __getattr__(self, attr):
        return self

    def __call__(self, *args, **kwargs):
        return self

    def __repr__(self):
        return '<PerformanceMocker>'

    def __await__(self):
        future = self.loop.create_future()
        future.set_result(self)
        return future.__await__()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return self

    def __len__(self):
        return 0

    def __bool__(self):
        return False


class GlobalChannel(commands.Converter):
    async def convert(self, ctx, argument):
        try:
            return await commands.TextChannelConverter().convert(ctx, argument)
        except commands.BadArgument:
            # Not found... so fall back to ID + global lookup
            try:
                channel_id = int(argument, base=10)
            except ValueError:
                raise commands.BadArgument(f'Could not find a channel by ID {argument!r}.')
            else:
                channel = ctx.bot.get_channel(channel_id)
                if channel is None:
                    raise commands.BadArgument(f'Could not find a channel by ID {argument!r}.')
                return channel


class Admin(commands.Cog):
    """Admin-only commands that make the bot dynamic."""

    def __init__(self, bot):
        self.bot = bot
        self._last_result = None
        self.sessions = set()

    @property
    def display_emoji(self) -> qq.PartialEmoji:
        return qq.PartialEmoji(name='stafftools', id=314348604095594498)

    async def run_process(self, command):
        try:
            process = await asyncio.create_subprocess_shell(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            result = await process.communicate()
        except NotImplementedError:
            process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            result = await self.bot.loop.run_in_executor(None, process.communicate)

        return [output.decode() for output in result]

    def cleanup_code(self, content):
        """Automatically removes code blocks from the code."""
        # remove ```py\n```
        if content.startswith('```') and content.endswith('```'):
            return '\n'.join(content.split('\n')[1:-1])

        # remove `foo`
        return content.strip('` \n')

    async def cog_check(self, ctx):
        return await self.bot.is_owner(ctx.author)

    def get_syntax_error(self, e):
        if e.text is None:
            return f'```py\n{e.__class__.__name__}: {e}\n```'
        return f'```py\n{e.text}{"^":>{e.offset}}\n{e.__class__.__name__}: {e}```'

    @commands.command(hidden=True)
    async def load(self, ctx, *, module):
        """加载一个模块。"""
        try:
            self.bot.load_extension(module)
        except commands.ExtensionError as e:
            await ctx.reply(f'{e.__class__.__name__}: {e}')
        else:
            await ctx.reply('\N{OK HAND SIGN}')

    @commands.command(hidden=True)
    async def unload(self, ctx, *, module):
        """卸载一个模块。"""
        try:
            self.bot.unload_extension(module)
        except commands.ExtensionError as e:
            await ctx.reply(f'{e.__class__.__name__}: {e}')
        else:
            await ctx.reply('\N{OK HAND SIGN}')

    @commands.group(name='reload', hidden=True, invoke_without_command=True)
    async def _reload(self, ctx, *, module):
        """重新加载一个模块。"""
        try:
            self.bot.reload_extension(module)
        except commands.ExtensionError as e:
            await ctx.reply(f'{e.__class__.__name__}: {e}')
        else:
            await ctx.reply('\N{OK HAND SIGN}')

    _GIT_PULL_REGEX = re.compile(r'\s*(?P<filename>.+?)\s*\|\s*[0-9]+\s*[+-]+')

    def find_modules_from_git(self, output):
        files = self._GIT_PULL_REGEX.findall(output)
        ret = []
        for file in files:
            root, ext = os.path.splitext(file)
            if ext != '.py':
                continue

            if root.startswith('cogs/'):
                # A submodule is a directory inside the main cog directory for
                # my purposes
                ret.append((root.count('/') - 1, root.replace('/', '.')))

        # For reload order, the submodules should be reloaded first
        ret.sort(reverse=True)
        return ret

    def reload_or_load_extension(self, module):
        try:
            self.bot.reload_extension(module)
        except commands.ExtensionNotLoaded:
            self.bot.load_extension(module)

    @_reload.command(name='all', hidden=True)
    async def _reload_all(self, ctx):
        """重新加载所有模块，同时从 git 中 pull。"""

        stdout, stderr = await self.run_process('git pull')

        # progress and stuff is redirected to stderr in git pull
        # however, things like "fast forward" and files
        # along with the text "already up-to-date" are in stdout

        if stdout.startswith('Already up to date.'):
            return await ctx.reply("全部模块都已经是最新的了。")

        modules = self.find_modules_from_git(stdout)
        mods_text = '\n'.join(f'{index}. `{module}`' for index, (_, module) in enumerate(modules, start=1))
        prompt_text = f'这将更新以下模块\n{mods_text}'
        await ctx.reply(prompt_text)

        statuses = []
        for is_submodule, module in modules:
            if is_submodule:
                try:
                    actual_module = sys.modules[module]
                except KeyError:
                    statuses.append((ctx.tick(None), module))
                else:
                    try:
                        importlib.reload(actual_module)
                    except Exception as e:
                        statuses.append((ctx.tick(False), module))
                    else:
                        statuses.append((ctx.tick(True), module))
            else:
                try:
                    self.reload_or_load_extension(module)
                except commands.ExtensionError:
                    statuses.append((ctx.tick(False), module))
                else:
                    statuses.append((ctx.tick(True), module))

        await ctx.reply('\n'.join(f'{status}: `{module}`' for status, module in statuses))

    @commands.command(pass_context=True, hidden=True, name='eval')
    async def _eval(self, ctx, *, body: str):
        """运行代码"""

        env = {
            'bot': self.bot,
            'ctx': ctx,
            'channel': ctx.channel,
            'author': ctx.author,
            'guild': ctx.guild,
            'message': ctx.message,
            '_': self._last_result
        }

        env.update(globals())

        body = self.cleanup_code(body)
        stdout = io.StringIO()

        to_compile = f'async def func():\n{textwrap.indent(body, "  ")}'

        try:
            exec(to_compile, env)
        except Exception as e:
            return await ctx.reply(f'```py\n{e.__class__.__name__}: {e}\n```')

        func = env['func']
        try:
            with redirect_stdout(stdout):
                ret = await func()
        except Exception as e:
            value = stdout.getvalue()
            await ctx.reply(f'```py\n{value}{traceback.format_exc()}\n```')
        else:
            value = stdout.getvalue()
            try:
                await ctx.message.add_reaction(qq.PartialEmoji(custom=True, id='124'))
            except:
                pass

            if ret is None:
                if value:
                    await ctx.reply(f'```py\n{value}\n```')
            else:
                self._last_result = ret
                await ctx.reply(f'```py\n{value}{ret}\n```')

    @commands.command(pass_context=True, hidden=True)
    async def repl(self, ctx):
        """启动交互式 REPL 会话。"""
        variables = {
            'ctx': ctx,
            'bot': self.bot,
            'message': ctx.message,
            'guild': ctx.guild,
            'channel': ctx.channel,
            'author': ctx.author,
            '_': None,
        }

        if ctx.channel.id in self.sessions:
            await ctx.reply('已在此频道中运行 REPL 会话。使用 `quit` 退出它。')
            return

        self.sessions.add(ctx.channel.id)
        await ctx.reply('输入代码以执行或eval。 `exit()` 或 `quit` 退出。')

        def check(m):
            return m.author.id == ctx.author.id and \
                   m.channel.id == ctx.channel.id

        while True:
            try:
                response = await self.bot.wait_for('message', check=check, timeout=10.0 * 60.0)
            except asyncio.TimeoutError:
                await ctx.reply('退出 REPL 会话。')
                self.sessions.remove(ctx.channel.id)
                break

            cleaned = self.cleanup_code(response.content)

            if cleaned in ('quit', 'exit', 'exit()'):
                await ctx.reply('已退出。')
                self.sessions.remove(ctx.channel.id)
                return

            executor = exec
            if cleaned.count('\n') == 0:
                # single statement, potentially 'eval'
                try:
                    code = compile(cleaned, '<repl session>', 'eval')
                except SyntaxError:
                    pass
                else:
                    executor = eval

            if executor is exec:
                try:
                    code = compile(cleaned, '<repl session>', 'exec')
                except SyntaxError as e:
                    await ctx.reply(self.get_syntax_error(e))
                    continue

            variables['message'] = response

            fmt = None
            stdout = io.StringIO()

            try:
                with redirect_stdout(stdout):
                    result = executor(code, variables)
                    if inspect.isawaitable(result):
                        result = await result
            except Exception as e:
                value = stdout.getvalue()
                fmt = f'```py\n{value}{traceback.format_exc()}\n```'
            else:
                value = stdout.getvalue()
                if result is not None:
                    fmt = f'```py\n{value}{result}\n```'
                    variables['_'] = result
                elif value:
                    fmt = f'```py\n{value}\n```'

            try:
                if fmt is not None:
                    if len(fmt) > 2000:
                        await ctx.reply('内容太大，无法输出。')
                    else:
                        await ctx.reply(fmt)
            except qq.Forbidden:
                pass
            except qq.HTTPException as e:
                await ctx.reply(f'意外错误：`{e}`')

    @commands.command(hidden=True)
    async def sql(self, ctx, *, query: str):
        """运行一些 SQL。"""
        # the imports are here because I imagine some people would want to use
        # this cog as a base for their other cog, and since this one is kinda
        # odd and unnecessary for most people, I will make it easy to remove
        # for those people.
        from utils.formats import TabularData, plural
        import time

        query = self.cleanup_code(query)

        is_multistatement = query.count(';') > 1
        if is_multistatement:
            # fetch does not support multiple statements
            strategy = ctx.db.execute
        else:
            strategy = ctx.db.fetch

        try:
            start = time.perf_counter()
            results = await strategy(query)
            dt = (time.perf_counter() - start) * 1000.0
        except Exception:
            return await ctx.reply(f'```py\n{traceback.format_exc()}\n```')

        rows = len(results)
        if is_multistatement or rows == 0:
            return await ctx.reply(f'`{dt:.2f}ms: {results}`')

        headers = list(results[0].keys())
        table = TabularData()
        table.set_columns(headers)
        table.add_rows(list(r.values()) for r in results)
        render = table.render()

        fmt = f'```\n{render}\n```\n*Returned {plural(rows):row} in {dt:.2f}ms*'
        if len(fmt) > 2000:
            fp = io.BytesIO(fmt.encode('utf-8'))
            await ctx.reply('Too many results...', file=qq.File(fp, 'results.txt'))
        else:
            await ctx.reply(fmt)

    @commands.command(hidden=True)
    async def sql_table(self, ctx, *, table_name: str):
        """运行描述表架构的查询。"""
        from utils.formats import TabularData

        query = """SELECT column_name, data_type, column_default, is_nullable
                   FROM INFORMATION_SCHEMA.COLUMNS
                   WHERE table_name = $1
                """

        results = await ctx.db.fetch(query, table_name)

        headers = list(results[0].keys())
        table = TabularData()
        table.set_columns(headers)
        table.add_rows(list(r.values()) for r in results)
        render = table.render()

        fmt = f'```\n{render}\n```'
        if len(fmt) > 2000:
            fp = io.BytesIO(fmt.encode('utf-8'))
            await ctx.reply('Too many results...', file=qq.File(fp, 'results.txt'))
        else:
            await ctx.reply(fmt)

    @commands.command(hidden=True)
    async def sudo(self, ctx, channel: Optional[GlobalChannel], who: Union[qq.Member, qq.User], *,
                   command: str):
        """可选地在另一个频道中以另一个用户身份运行命令。"""
        msg = copy.copy(ctx.message)
        channel = channel or ctx.channel
        msg.channel = channel
        msg.author = who
        msg.content = ctx.prefix + command
        new_ctx = await self.bot.get_context(msg, cls=type(ctx))
        new_ctx._db = ctx._db
        await self.bot.invoke(new_ctx)

    @commands.command(hidden=True)
    async def do(self, ctx, times: int, *, command):
        """将命令重复指定次数。"""
        msg = copy.copy(ctx.message)
        msg.content = ctx.prefix + command

        new_ctx = await self.bot.get_context(msg, cls=type(ctx))
        new_ctx._db = ctx._db

        for i in range(times):
            await new_ctx.reinvoke()

    @commands.command(hidden=True)
    async def sh(self, ctx, *, command):
        """运行 shell 命令。"""

        async with ctx.typing():
            stdout, stderr = await self.run_process(command)

        if stderr:
            text = f'stdout:\n{stdout}\nstderr:\n{stderr}'
        else:
            text = stdout

        await ctx.reply(text)

    @commands.command(hidden=True)
    async def perf(self, ctx, *, command):
        """检查命令的时间，试图抑制 HTTP 和 DB 调用。"""

        msg = copy.copy(ctx.message)
        msg.content = ctx.prefix + command

        new_ctx = await self.bot.get_context(msg, cls=type(ctx))
        new_ctx._db = PerformanceMocker()

        # Intercepts the Messageable interface a bit
        new_ctx._state = PerformanceMocker()
        new_ctx.channel = PerformanceMocker()

        if new_ctx.command is None:
            return await ctx.reply('No command found')

        start = time.perf_counter()
        try:
            await new_ctx.command.invoke(new_ctx)
        except commands.CommandError:
            end = time.perf_counter()
            success = False
            try:
                await ctx.reply(f'```py\n{traceback.format_exc()}\n```')
            except qq.HTTPException:
                pass
        else:
            end = time.perf_counter()
            success = True

        await ctx.reply(f'Status: {ctx.tick(success)} Time: {(end - start) * 1000:.2f}ms')


def setup(bot):
    bot.add_cog(Admin(bot))
