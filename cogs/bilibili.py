import asyncio
import base64
import json
import logging
import random
import time
import traceback
from io import BytesIO

import qq
from PIL import Image
from qq import TextChannel
from qq.ext import commands, tasks

from cogs.utils.image_upload import upload_to_jd

log = logging.getLogger(__name__)


class Bilibili(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.config = bot.bilibili
        self.messageLengthLimit = self.config.get('messageLengthLimit', 0)
        self.push_uid = self.config.get('push_uid', {})
        self.push_times = self.config.get('push_times', {})
        self.room_states = self.config.get('room_states', {})
        self.all_user_name = self.config.get('all_user_name', {})
        self.check_bili_dynamic_loop.start()
        self._check_lock = asyncio.Lock(loop=bot.loop)

    async def save_config(self):
        await self.config.put('messageLengthLimit', self.messageLengthLimit)
        await self.config.put('push_uid', self.push_uid)
        await self.config.put('push_times', self.push_times)
        await self.config.put('room_states', self.room_states)
        await self.config.put('all_user_name', self.all_user_name)
        await self.config.save()

    @property
    def bilibili_cookie(self):
        return f'LIVE_BUVID=AUTO{"".join([str(random.randint(0, 9)) for _ in range(16)])};'

    async def check_uid_exsist(self, uid):
        headers = {
            'Referer': 'https://space.bilibili.com/{user_uid}/'.format(user_uid=uid)
        }

        async with self.bot.session.get(
                'https://api.bilibili.com/x/space/acc/info',
                params={'mid': uid}, headers=headers
        ) as resp:
            res = await resp.json()
            if res['code'] == 0:
                return True
            return False

    async def get_user_name(self, uid):
        headers = {
            'Referer': 'https://space.bilibili.com/{user_uid}/'.format(user_uid=uid)
        }
        async with self.bot.session.get(
                'https://api.bilibili.com/x/space/acc/info',
                params={'mid': uid}, headers=headers
        ) as resp:
            res = await resp.json()
            return res['data']['name']

    async def load_all_username(self):
        uids = self.push_uid.keys()
        for uid in uids:
            self.all_user_name[uid] = await self.get_user_name(uid)

    async def load_username(self, uid):
        if uid not in self.all_user_name:
            self.all_user_name[uid] = await self.get_user_name(uid)

    def cog_unload(self):
        self.check_bili_dynamic_loop.stop()

    def get_limited_message(self, msg):
        if len(msg) > self.messageLengthLimit > 0:
            return msg[0:self.messageLengthLimit] + '……'
        else:
            return msg

    async def broadcast(self, uid, msg, url):
        for channel_id in self.push_uid[uid]:
            channel_id = int(channel_id)
            channel: TextChannel = self.bot.get_channel(channel_id)
            log.info(msg)
            if not url:
                await self.bot.get_channel(channel_id).send(msg)
            else:
                try:
                    await channel.send(msg, image=url.pop(0))
                    for n in url:
                        await self.bot.get_channel(channel_id).send(msg, image=n)
                except qq.error.Forbidden:
                    await self.bot.get_channel(channel_id).send(msg)

    async def make_big_image(self, image_urls, size, image_num):
        images = []
        for url in image_urls:  # 下载全部图片
            async with self.bot.session.get(url) as image_resp:
                image = await image_resp.content
            images.append(image)
        if image_num == 9:
            new_img = Image.new('RGB', (size[0] * 3, size[1] * 3), 255)
            for y in range(3):
                for x in range(3):
                    img = Image.open(images[y * 3 + x])
                    new_img.paste(img, (x * size[0], y * size[1]))
        elif image_num == 6:
            new_img = Image.new('RGB', (size[0] * 3, size[1] * 2), 255)
            for y in range(2):
                for x in range(3):
                    img = Image.open(images[y * 3 + x])
                    new_img.paste(img, (x * size[0], y * size[1]))
        else:
            return
        output = BytesIO()
        new_img.save(output, format='JPEG')
        im_data = output.getvalue()
        image_data = base64.b64encode(im_data)
        if not isinstance(image_data, str):
            # Python 3, decode from bytes to string
            image_data = image_data.decode()
        await upload_to_jd(image_data)

    async def _check_bili_dynamic(self):
        log.debug('B站动态检查开始')
        for uid in self.push_uid.keys():
            headers = {
                'Referer': 'https://space.bilibili.com/{user_uid}/'.format(user_uid=uid)
            }
            async with self.bot.session.get(
                    'https://api.vc.bilibili.com/dynamic_svr/v1/dynamic_svr/space_history?host_uid={user_uid}',
                    params={'host_uid': uid},
                    headers=headers
            ) as resp:
                res = await resp.json()
                if res is None:
                    log.warning(f'检查{uid}时出错 request response is None')
                    continue
            cards = res['data']['cards']
            # cards=[res['data']['cards'][10]]
            uid_time = self.push_times.get(uid, 0)
            self.push_times[uid] = int(time.time())
            for card in cards:
                msg = ''
                url = []
                uname = self.all_user_name[uid]
                if card['desc']['timestamp'] < uid_time:
                    break
                dynamic_id = card['desc']['dynamic_id']
                dynamic_type = card['desc']['type']
                msg += uname + '发表了'
                log.info(f"Got <Notification author={uname} type={dynamic_type}>")
                content = json.loads(card['card'])
                if dynamic_type == 2:  # 带图片动态
                    msg += '动态：\n'
                    pictures_count = content['item']['pictures_count']
                    true_content = content['item']['description']
                    true_content = self.get_limited_message(true_content)
                    msg += true_content
                    pictures = content['item']['pictures']
                    if pictures_count > 0:
                        is_big_picture = False
                        first_picture_size = [pictures[0]['img_width'], pictures[0]['img_height']]
                        if pictures_count >= 9:
                            is_big_picture = True
                            for i in range(9):
                                if pictures[i]['img_width'] != first_picture_size[0] or pictures[i]['img_height'] != \
                                        first_picture_size[1]:
                                    is_big_picture = False
                            if is_big_picture:
                                picture_srcs = []
                                for i in range(9):
                                    picture_srcs.append(pictures[i]['img_src'])
                                url = [await self.make_big_image(picture_srcs, first_picture_size, 9)]
                        if pictures_count >= 6 and not is_big_picture:
                            is_big_picture = True
                            for i in range(6):
                                if pictures[i]['img_width'] != first_picture_size[0] or pictures[i]['img_height'] != \
                                        first_picture_size[1]:
                                    is_big_picture = False
                            if is_big_picture:
                                picture_srcs = []
                                for i in range(6):
                                    picture_srcs.append(pictures[i]['img_src'])
                                url = [await self.make_big_image(picture_srcs, first_picture_size, 6)]
                                picture_srcs = []
                                if pictures_count > 6:
                                    for i in range(7, pictures_count):
                                        picture_srcs.append(pictures[i]['img_src'])
                        if not is_big_picture:
                            if pictures_count > 0 and pictures_count < 4:
                                url = []
                                for pic in pictures:
                                    url.append(pic['img_src'])
                    msg += f'\nhttps://t.bilibili.com/{dynamic_id}'
                elif dynamic_type == 4:  # 纯文字动态
                    msg += '动态：\n'
                    true_content = content['item']['content']
                    true_content = self.get_limited_message(true_content)
                    msg += true_content
                    msg += f'\nhttps://t.bilibili.com/{dynamic_id}'
                elif dynamic_type == 64:  # 文章
                    msg += '文章：\n'
                    cv_id = str(content['id'])
                    title = content['title']
                    summary = content['summary']
                    url = [content['image_urls'][0]]
                    msg += title + '\n' + summary + '……' + f'\nhttps://www.bilibili.com/read/cv{cv_id}'
                elif dynamic_type == 8:  # 投稿视频
                    msg += '视频：\n'
                    bv_id = card['desc']['bvid']
                    url = [content['pic']]
                    msg += content['title'] + '\n' + self.get_limited_message(content['desc']) + '\n' + \
                           f'\nhttps://www.bilibili.com/video/{bv_id}'
                elif dynamic_type == 1:  # 转发动态
                    msg += '转发动态：\n'
                    msg += content['item']['content'] + '\n'
                    origin_type = content['item']['orig_type']
                    origin_content = json.loads(content['origin'])
                    if origin_type == 2:
                        origin_user = origin_content['user']['name']
                        msg += '>>' + origin_user + ': /n'
                        origin_true_content = origin_content['item']['description']
                        origin_true_content = self.get_limited_message(origin_true_content)
                        msg += origin_true_content
                    elif origin_type == 4:
                        origin_user = origin_content['user']['name']
                        msg += '>>' + origin_user + ': /n'
                        origin_true_content = origin_content['item']['content']
                        origin_true_content = self.get_limited_message(origin_true_content)
                        msg += origin_true_content
                    elif origin_type == 8:
                        bv_id = card['desc']['origin']['bvid']
                        title = origin_content['title']
                        cover_image = origin_content['pic']
                        owner_name = origin_content['owner']['name']
                        msg += '>>' + owner_name + '的视频:' + title + '\n' + '>>bv' + bv_id
                    elif origin_type == 64:
                        title = origin_content['title']
                        cv_id = str(origin_content['id'])
                        owner_name = origin_content['author']['name']
                        msg += '>>' + owner_name + '的文章:' + title + '\n' + '>>cv' + cv_id
                    else:
                        msg += '>>暂不支持的源动态类型，请进入动态查看'
                    msg += f'\nhttps://t.bilibili.com/{dynamic_id}'
                else:
                    msg += uname + f'发表了动态：\n暂不支持该动态类型，请进入原动态查看\nhttps://t.bilibili.com/{dynamic_id}'
                await self.broadcast(uid, msg, url)
                await asyncio.sleep(0.5)
        log.debug('B站动态检查结束')
        log.debug('B站直播状态检查开始')
        for uid in self.push_uid.keys():
            try:
                headers = {
                    'Referer': 'https://space.bilibili.com/{user_uid}/'.format(user_uid=uid),
                    'Cookie': self.bilibili_cookie
                }
                async with self.bot.session.get(
                        'https://api.bilibili.com/x/space/acc/info',
                        params={'mid': uid},
                        headers=headers,
                ) as resp:
                    content = await resp.json()
                if content['data']['live_room'] is None:
                    continue
                if content['data']['live_room']['liveStatus'] == 1 and not self.room_states[uid]:
                    log.info(content['data']['live_room'])
                    self.room_states[uid] = True
                    username = self.all_user_name[uid]
                    msg = username + '开播了：\n' + content['data']['live_room']['title'] + '\n' \
                          + res['data']['live_room']['url']
                    url = [content['data']['live_room']['cover']]
                    await self.broadcast(uid, msg, url)
                elif self.room_states[uid] and content['data']['live_room']['liveStatus'] == 0:
                    self.room_states[uid] = False
                    msg = content['data']['name'] + '下播了'
                    await self.broadcast(uid, msg, url)
                await asyncio.sleep(0.5)
            except Exception as _:
                log.warning(f'B站直播检查发生错误 uid={uid}\n' + traceback.format_exc())
        log.debug('B站直播状态检查结束')
        await self.save_config()

    @tasks.loop(minutes=1)
    async def check_bili_dynamic_loop(self):
        if self._check_lock.locked():
            return
        async with self._check_lock:
            await self._check_bili_dynamic()

    @check_bili_dynamic_loop.before_loop
    async def before_check_bili(self):
        await self.bot.wait_until_ready()

    @commands.group(name='动态', invoke_without_command=True, hidden=True)
    @commands.is_owner()
    async def status(self, ctx: commands.Context):
        await self.load_all_username()
        msg = '当前订阅:'
        for uid, username in self.all_user_name.items():
            msg += f'\n{username} ({uid})'
        if not self.all_user_name:
            msg += '暂无'
        await ctx.reply(msg)

    @status.command(name='订阅', hidden=True)
    @commands.is_owner()
    async def _sub(self, ctx: commands.Context, uids: commands.Greedy[int]):
        for uid in uids:
            if not await self.check_uid_exsist(uid):
                await ctx.reply(f'{uid} 订阅失败：用户不存在')
                continue
            if uid not in self.push_uid:
                self.push_uid[uid] = [str(ctx.channel.id)]
                self.room_states[uid] = False
            else:
                if str(ctx.channel.id) in self.push_uid[uid]:
                    await ctx.reply(f'{uid} 订阅失败：请勿重复订阅')
                    continue
                self.push_uid[uid].append(str(ctx.channel.id))
            await self.load_username(uid)
            self.push_times[uid] = int(time.time())
            await ctx.reply(f'{uid} 订阅成功')
        await self.save_config()

    @status.command(name='取消', hidden=True)
    @commands.is_owner()
    async def _cancel_sub(self, ctx: commands.Context, uids: commands.Greedy[int]):
        for uid in uids:
            if uid in self.push_uid.keys():
                if str(ctx.channel.id) in self.push_uid[uid]:
                    if len(self.push_uid[uid]) == 1:
                        self.push_uid.pop(uid)
                    else:
                        self.push_uid[uid].remove(str(ctx.channel.id))
                else:
                    await ctx.reply(f'{uid} 取消订阅失败：未找到该订阅')
                    continue
            else:
                await ctx.reply(f'{uid} 取消订阅失败：未找到该订阅')
                continue
            await ctx.reply(f'{uid} 取消订阅成功')
        await self.save_config()


def setup(bot):
    bot.add_cog(Bilibili(bot))
