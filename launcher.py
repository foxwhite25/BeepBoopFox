import asyncio
import contextlib
import importlib
import logging
import sys
import traceback
from logging.handlers import RotatingFileHandler

import click
import colorlog

import config
from bot import initial_extensions, BeepBoopFox
from cogs.utils.db import Table

try:
    import uvloop
except ImportError:
    pass
else:
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


class RemoveNoise(logging.Filter):
    def __init__(self):
        super().__init__(name='qq.state')

    def filter(self, record):
        if record.levelname == 'WARNING' and 'referencing an unknown' in record.msg:
            return False
        return True


@contextlib.contextmanager
def setup_logging():
    try:
        # __enter__
        max_bytes = 32 * 1024 * 1024  # 32 MiB
        logging.getLogger('qq').setLevel(logging.INFO)
        logging.getLogger('qq.http').setLevel(logging.DEBUG)
        logging.getLogger('qq.state').addFilter(RemoveNoise())

        log = logging.getLogger()
        log.setLevel(logging.INFO)
        handler = RotatingFileHandler(filename='bb_fox.log', encoding='utf-8', mode='w', maxBytes=max_bytes,
                                      backupCount=5)
        dt_fmt = '%Y-%m-%d %H:%M:%S'
        fmt = logging.Formatter('[{asctime}] [{levelname:<7}] {name}: {message}', dt_fmt, style='{')
        handler.setFormatter(fmt)
        log.addHandler(handler)

        log = logging.getLogger()
        handler = colorlog.StreamHandler()
        handler.setFormatter(
            colorlog.ColoredFormatter(
                "%(log_color)s[%(asctime)s] [%(name)-15s] [%(levelname)-7s]: %(message)s (%(filename)s:%(lineno)d)",
                "%Y-%m-%d %H:%M:%S")
        )

        log.setLevel(logging.INFO)
        log.addHandler(handler)

        yield
    finally:
        # __exit__
        handlers = log.handlers[:]
        for hdlr in handlers:
            hdlr.close()
            log.removeHandler(hdlr)


def run_bot():
    loop = asyncio.get_event_loop()
    log = logging.getLogger()
    kwargs = {
        'command_timeout': 60,
        'max_size': 20,
        'min_size': 20,
    }
    try:
        pool = loop.run_until_complete(Table.create_pool(config.postgresql, **kwargs))
    except Exception as e:
        click.echo('无法设置 PostgreSQL。 正在退出。', file=sys.stderr)
        log.exception('无法设置 PostgreSQL。 正在退出。')
        return

    bot = BeepBoopFox()
    bot.pool = pool
    bot.run()


@click.group(invoke_without_command=True, options_metavar='[options]')
@click.pass_context
def main(ctx):
    """Launches the bot."""
    if ctx.invoked_subcommand is None:
        loop = asyncio.get_event_loop()
        with setup_logging():
            run_bot()


@main.group(short_help='数据库', options_metavar='[options]')
def db():
    pass


@db.command(short_help='初始化机器人的数据库', options_metavar='[options]')
@click.argument('cogs', nargs=-1, metavar='[cogs]')
@click.option('-q', '--quiet', help='不那么冗长的输出', is_flag=True)
def init(cogs, quiet):
    """This manages the migrations and database creation system for you."""

    run = asyncio.get_event_loop().run_until_complete
    try:
        run(Table.create_pool(config.postgresql))
    except Exception:
        click.echo(f'无法创建 PostgreSQL 连接池。\n{traceback.format_exc()}', err=True)
        return

    if not cogs:
        cogs = initial_extensions
    else:
        cogs = [f'cogs.{e}' if not e.startswith('cogs.') else e for e in cogs]

    for ext in cogs:
        try:
            importlib.import_module(ext)
        except Exception:
            click.echo(f'无法加载 {ext}.\n{traceback.format_exc()}', err=True)
            return

    for table in Table.all_tables():
        try:
            created = run(table.create(verbose=not quiet, run_migrations=False))
        except Exception:
            click.echo(f'无法创建 {table.__tablename__}.\n{traceback.format_exc()}', err=True)
        else:
            if created:
                click.echo(f'[{table.__module__}] 创建 {table.__tablename__}。')
            else:
                click.echo(f'[{table.__module__}] {table.__tablename__} 不需要工作。')


@db.command(short_help='迁移数据库')
@click.argument('cog', nargs=1, metavar='[cog]')
@click.option('-q', '--quiet', help='不那么冗长的输出', is_flag=True)
@click.pass_context
def migrate(ctx, cog, quiet):
    """Update the migration file with the newest schema."""

    if not cog.startswith('cogs.'):
        cog = f'cogs.{cog}'

    try:
        importlib.import_module(cog)
    except Exception:
        click.echo(f'无法加载 {cog}。\n{traceback.format_exc()}', err=True)
        return

    def work(table, *, invoked=False):
        try:
            actually_migrated = table.write_migration()
        except RuntimeError as e:
            click.echo(f'无法迁移 {table.__tablename__}：{e}', err=True)
            if not invoked:
                click.confirm('你想创建表吗？', abort=True)
                ctx.invoke(init, cogs=[cog], quiet=quiet)
                work(table, invoked=True)
            sys.exit(-1)
        else:
            if actually_migrated:
                click.echo(f'已成功更新 {table.__tablename__} 的迁移。')
            else:
                click.echo(f'未发现 {table.__tablename__} 的更改。')

    for table in Table.all_tables():
        work(table)

    click.echo(f'完成迁移 {cog}。')


async def apply_migration(cog, quiet, index, *, downgrade=False):
    try:
        pool = await Table.create_pool(config.postgresql)
    except Exception:
        click.echo(f'无法创建 PostgreSQL 连接池。\n{traceback.format_exc()}', err=True)
        return

    if not cog.startswith('cogs.'):
        cog = f'cogs.{cog}'

    try:
        importlib.import_module(cog)
    except Exception:
        click.echo(f'无法加载 {cog}。\n{traceback.format_exc()}', err=True)
        return

    async with pool.acquire() as con:
        tr = con.transaction()
        await tr.start()
        for table in Table.all_tables():
            try:
                await table.migrate(index=index, downgrade=downgrade, verbose=not quiet, connection=con)
            except RuntimeError as e:
                click.echo(f'无法迁移 {table.__tablename__}：{e}', err=True)
                await tr.rollback()
                break
        else:
            await tr.commit()


@db.command(short_help='迁移升级')
@click.argument('cog', nargs=1, metavar='[cog]')
@click.option('-q', '--quiet', help='不那么冗长的输出', is_flag=True)
@click.option('--index', help='要使用的索引', default=-1)
def upgrade(cog, quiet, index):
    """Runs an upgrade from a migration"""
    run = asyncio.get_event_loop().run_until_complete
    run(apply_migration(cog, quiet, index))


@db.command(short_help='迁移降级')
@click.argument('cog', nargs=1, metavar='[cog]')
@click.option('-q', '--quiet', help='不那么冗长的输出', is_flag=True)
@click.option('--index', help='要使用的索引', default=-1)
def downgrade(cog, quiet, index):
    """Runs an downgrade from a migration"""
    run = asyncio.get_event_loop().run_until_complete
    run(apply_migration(cog, quiet, index, downgrade=True))


async def remove_databases(pool, cog, quiet):
    async with pool.acquire() as con:
        tr = con.transaction()
        await tr.start()
        for table in Table.all_tables():
            try:
                await table.drop(verbose=not quiet, connection=con)
            except RuntimeError as e:
                click.echo(f'Could not drop {table.__tablename__}: {e}', err=True)
                await tr.rollback()
                break
            else:
                click.echo(f'Dropped {table.__tablename__}.')
        else:
            await tr.commit()
            click.echo(f'successfully removed {cog} tables.')


@db.command(short_help="移除一个 cog 的表", options_metavar='[options]')
@click.argument('cog', metavar='<cog>')
@click.option('-q', '--quiet', help='不那么冗长的输出', is_flag=True)
def drop(cog, quiet):
    """This removes a database and all its migrations.

    You must be pretty sure about this before you do it,
    as once you do it there's no coming back.

    Also note that the name must be the database name, not
    the cog name.
    """

    run = asyncio.get_event_loop().run_until_complete
    click.confirm('你真的想这样做吗？', abort=True)

    try:
        pool = run(Table.create_pool(config.postgresql))
    except Exception:
        click.echo(f'无法创建 PostgreSQL 连接池。\n{traceback.format_exc()}', err=True)
        return

    if not cog.startswith('cogs.'):
        cog = f'cogs.{cog}'

    try:
        importlib.import_module(cog)
    except Exception:
        click.echo(f'无法加载 {cog}。\n{traceback.format_exc()}', err=True)
        return

    run(remove_databases(pool, cog, quiet))


if __name__ == '__main__':
    main()
