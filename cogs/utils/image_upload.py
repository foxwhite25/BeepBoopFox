import asyncio
import json
import logging
from io import BytesIO

import aiohttp
from aiohttp import FormData

logger = logging.getLogger(__name__)


async def upload(image: BytesIO) -> str:
    formdata = FormData()
    formdata.add_field('fileupload', image, filename='test.png')
    async with aiohttp.ClientSession() as session:
        async with session.post('http://pic.qingchengkg.cn/api/upload/', data=formdata) as response:
            data = json.loads(await response.text())
            logger.debug('Image upload status %d with data %s', response.status, data)
            return data['url']


if __name__ == '__main__':
    asyncio.run(upload(BytesIO(open('./test.png', 'rb').read())))
