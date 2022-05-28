import asyncio
import datetime
import io
import logging
from collections import defaultdict
from typing import Optional, List

import asyncpg
import cv2
import numpy
import qq
from asyncpg import Record
from qq.ext import commands, tasks
from qq.ext.commands import BucketType

from bot import BeepBoopFox
from cogs.context import Context
from cogs.pixel_war.enum import Colors
from cogs.utils import db

log = logging.getLogger(__name__)


class Pixels(db.Table):
    painter = db.Column(db.String)
    painter_guild = db.Column(db.String)
    x = db.PrimaryKeyColumn()
    y = db.PrimaryKeyColumn()
    color = db.Column(db.Integer)
    time = db.Column(db.Datetime)


class PixelWar(commands.Cog):
    def __init__(self, bot: BeepBoopFox):
        self.bot = bot
        self.max_x = bot.pixel.get("max_x")
        self.max_y = bot.pixel.get("max_y")
        self.filled_in = False
        self._batch_changes = []
        self._batch_lock = asyncio.Lock(loop=bot.loop)
        self.bulk_insert_loop.add_exception_type(asyncpg.PostgresConnectionError)
        self.bulk_insert_loop.start()

        self.pixels = numpy.zeros([self.max_x, self.max_y, 3], dtype=numpy.uint8)
        self.pixel_data = defaultdict(lambda: defaultdict())
        self.pixels.fill(255)

    def cog_unload(self):
        self.filled_in = False
        self.bulk_insert_loop.stop()

    @tasks.loop(seconds=10.0)
    async def bulk_insert_loop(self):
        if not self.filled_in:
            self.filled_in = True
            await self.fill_in_pixels()
        async with self._batch_lock:
            await self.bulk_insert()

    async def bulk_insert(self):
        query = """INSERT INTO pixels (painter, painter_guild, x, y, color, time) 
                   SELECT k.painter, k.painter_guild, k.x, k.y, k.color, k.time
                   FROM jsonb_to_recordset($1::jsonb) AS
                   k(painter TEXT, painter_guild TEXT, x INT, y INT, color INT, time TIMESTAMP)
                   ON CONFLICT (x, y) DO UPDATE 
                   SET painter = excluded.painter, painter_guild = excluded.painter_guild,
                    color = excluded.color, time = excluded.time
                """

        if self._batch_changes:
            await self.bot.pool.execute(query, self._batch_changes)
            total = len(self._batch_changes)
            if total > 1:
                log.info('已将 %s 像素注册到数据库。', total)
            self._batch_changes.clear()

    async def fill_in_pixels(self):
        query = "SELECT * FROM pixels"
        colors: List[Record] = await self.bot.pool.fetch(query)
        for color in colors:
            self.pixel_data[color['x']][color['y']] = {
                'color': color['color'],
                'painter': color['painter'],
                'painter_guild': color['painter_guild'],
                'time': color['time']
            }
            if color['color'] == 31:
                continue
            self.pixels[color['y'], color['x']] = Colors[color['color']]
        log.info("Fill in completed")

    @commands.command(name="帮助")
    async def help(self, ctx: Context):
        await ctx.send(
            "欢迎参加像素大战，看看你的社区能不能在这个地方占上一块地！\n\n"
            "这个机器人的概念是一个社会实验，每个人每五分钟只能在一个全局画布上改变一个像素。\n"
            "但是当一个频道集中力量可以很容易的创造一些图片或像素画，看看你的社区是否能够集中人员来画出一幅代表你频道的区域。\n\n"
            "首先使用 /色轮 选择你喜欢的色号。\n"
            "然后每个人每五分钟只能使用 '/画图 x y 色号' 画一个像素。\n例子： /画图 9 9 6\n\n"
            "使用 '/看图 中心x 中心y 范围' 来观看附近有什么像素以及你的成果。\n例子： /看图 9 9 10\n\n"
            "你还可以使用 '/查像素 x y' 来看看是哪个频道的混蛋覆盖了你的像素！\n例子： /查像素 9 9\n\n"
            f"目前最大范围为 ({self.max_x}, {self.max_y}) 如果之后位置不够用了还能够扩容。\n"
            f"本机器人使用 QQ.py 制作。"
        )

    @commands.command(name="色轮")
    async def color_wheel(self, ctx: Context):
        file = qq.File("./img.png")
        await ctx.send("从左到右 32 种颜色，编号从0开始根据顺序来排序，例如纯白为 色号31。", file=file)

    @commands.cooldown(rate=1, per=30, type=BucketType.user)
    @commands.command(name="看图")
    async def _view_canvas(self, ctx: Context, x: int, y: int, radius: Optional[int] = 10):
        await ctx.send(f"正在生成图片，中心将会是 ({x},{y})：")
        await self.view_canvas(ctx, x, y, radius)

    async def view_canvas(self, ctx: Context, x: int, y: int, radius: Optional[int] = 10):
        if not (0 <= x <= self.max_x and 0 <= y <= self.max_y):
            return await ctx.send("你这中心都超出最大范围了！")
        if radius > 100:
            return await ctx.send("范围太大了！")
        x_max = min(self.max_x, x + radius + 1)
        x_min = max(0, x - radius)
        y_max = min(self.max_y, y + radius + 1)
        y_min = max(0, y - radius)
        size = (1 + radius * 2) * 20
        half_size = size // 2
        img = self.pixels[y_min:y_max, x_min:x_max]
        img = numpy.pad(
            img,
            [
                (max(0, radius - y), max(0, y + radius - self.max_y - 1)),
                (max(0, radius - x), max(0, x + radius - self.max_x - 1)),
                (0, 0)
            ],
            'constant', constant_values=(0, 0)
        )

        img = cv2.resize(img, dsize=(size, size), interpolation=cv2.INTER_AREA)

        img[half_size-5:half_size+5, half_size-5:half_size+5] = 255 - img[half_size, half_size]

        retval, buffer = cv2.imencode('.jpg', img, [int(cv2.IMWRITE_JPEG_QUALITY), 100])
        file = qq.File(io.BytesIO(buffer.tobytes()))
        await ctx.send(file=file)

    @commands.cooldown(rate=1, per=300, type=BucketType.user)
    @commands.command(name="画图")
    async def draw_canvas(self, ctx: Context, x: int, y: int, color: int):
        now = datetime.datetime.now()
        if 0 >= x >= self.max_x or 0 >= y >= self.max_y:
            return await ctx.send("你想设置的位置超出了上限！")
        try:
            self.pixels[y, x] = Colors[color]
        except IndexError:
            return await ctx.send("不支持该色号！")
        self._batch_changes.append({
            'x': x,
            'y': y,
            'color': color,
            'painter': ctx.author.display_name,
            'painter_guild': ctx.guild.name,
            'time': now.isoformat()
        })
        self.pixel_data[x][y] = {
            'color': color,
            'painter': ctx.author.display_name,
            'painter_guild': ctx.guild.name,
            'time': now
        }
        await ctx.send(f"成功在 ({x},{y}) 画上色号 {color} !")
        await self.view_canvas(ctx, x, y)
        await asyncio.sleep(300)
        await ctx.send(f"{ctx.author.mention} 你的下一笔已经准备好了！")

    async def cog_command_error(self, ctx: Context, error: Exception):
        if isinstance(error, commands.CommandOnCooldown):
            return await ctx.send("当前指令在冷却中，请在%.0f秒后重试！现在先喝杯茶休息一下吧 ☕ 。" % error.retry_after)
        if isinstance(error, commands.ConversionError) or isinstance(error, commands.UserInputError):
            return await ctx.send("参数有误，请使用 /帮助 并检查参数！")

    @commands.command(name="查像素")
    async def check_pixel(self, ctx: Context, x: int, y: int):
        if y not in self.pixel_data[x]:
            return await ctx.send("该位置还没有人画上像素，赶紧来画一下吧。")
        result = self.pixel_data[x][y]
        await ctx.send(
            f"位置 ({x},{y}) 由 {result['painter_guild']} 的 {result['painter']} "
            f"于 {result['time'].strftime('%m/%d/%Y, %H:%M:%S')} 绘制为 色号{result['color']}。"
        )

    @commands.cooldown(rate=1, per=300, type=BucketType.user)
    @commands.command(name="全图")
    async def full_map(self, ctx: Context):
        res = numpy.pad(self.pixels, ((20, 20), (20, 20), (0, 0)), 'constant', constant_values=0)
        retval, buffer = cv2.imencode('.jpg', res, [int(cv2.IMWRITE_JPEG_QUALITY), 100])
        file = qq.File(io.BytesIO(buffer.tobytes()))
        await ctx.send(file=file)


def setup(bot):
    bot.add_cog(PixelWar(bot))
