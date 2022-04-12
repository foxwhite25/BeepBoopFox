import logging
import json

import aiohttp
import lxml.html
import os

logger = logging.getLogger('upload_image')


async def upload_to_jd(b64):
    url = 'https://imio.jd.com/uploadfile/file/post.do'
    headers = {
        'authority': 'imio.jd.com',
        'pragma': 'no-cache',
        'cache-control': 'no-cache',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36',
        'dnt': '1',
        'accept': 'application/json',
        'origin': 'chrome-extension://dckaeinoeaogebmhijpkpmacifmpgmcb',
        'sec-fetch-site': 'none',
        'sec-fetch-mode': 'cors',
        'sec-fetch-dest': 'empty',
        'accept-language': 'zh-CN,zh;q=0.9,en-CN;q=0.8,en;q=0.7',
        'cookie': 'shshshfpa=8ea1094b-9fa8-de6c-6074-c9106dd7607e-1628380635; shshshfpb=z%2FPs6TIxZfdRxtWgmXeif2g%3D%3D; pinId=PPaUOCiikPQ; pin=sirliu; unick=%E7%89%9B%E6%AD%A3%E6%96%87; _tp=7sn2a8%2F1SK1ezvQshrlpwg%3D%3D; _pst=sirliu; unpl=V2_ZzNtbUtfQEVwAU8Hfh4JVWIAEFhLAERGIlxEVS8cCABgA0ZZclRCFnUUR11nGlUUZwIZXEVcRhRFCENkexhdBWMGEV5EVnMlMEsWBi8FXABnBhtVQlJDF30JRl1yGV4AYh8RXUNVQxZ2CEdQeB5sBmczE21CUEAcdwFHU3kcXAdhAxNcRlVBEXUKR2RLGVQBVwMTXENXShFwC0RSeCmKq%2fPWmuKb5%2bbD36aT2cvM1a6zvZRtQl5DF30JR1R8GmwEVwIiHywUF0UoVBKC1reLvMdOElpBXkEcdA9EUXsbWgVmAhZfQFNDF3Q4R2R4; user-key=bb2545ac-0191-4acf-9722-1b93bf6d1836; cn=63; TrackID=1TxuaKNhgxgBtcr2M21qCh6VWbsfy37XyB2W4cljNpXutoeNlbHkYwaOI-2jc2FD6B5Lnak-gbw7hmta-HO2pCIFGysyaVDRBQ6DajTr3PUY; __jda=76161171.16283806341371005335130.1628380634.1630168669.1630859122.7; retina=1; cid=9; webp=1; visitkey=58375295313190738; TrackerID=HOOxXvvO80cvQXiVumgSsXxCj5j4FStKK8lWVeIjTVNmgGVxyt2APEwM-nX3lvq64dxnev681-bN0t6DBjIwyFoHmoP-LYVTCNujsg3v3m_4LV7zx1eqesUw1hfku-V4; pt_key=AAJhNPAPADBER43E6xvtQAb6JGiydrhu7yJll-q-Etg8Z4NoYH9pHk3sKTSMjW50txLw9Dyy3u0; pt_pin=sirliu; pt_token=l2rl7kn8; pwdt_id=sirliu; __wga=1630859280444.1630859280444.1630859280444.1630859280444.1.1; sc_width=400; shshshfp=5a972470188b5209659db1111a4ca4fa; mobilev=html5; cluster=1_file-dd.jd.local_file-dd.jd.local',
    }
    form_data = {
        'appId': 'im.customer',
        'aid': 'undefined',
        'clientType': 'comet',
        'pin': 'undefined',
        's': b64
    }
    multipartWriter = aiohttp.MultipartWriter('mixed')
    for key, value in form_data.items():
        multipartWriter.append(value).set_content_disposition('form-data', name=key)

    headers['Content-Type'] = 'multipart/form-data; boundary=' + multipartWriter.boundary
    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=headers, data=multipartWriter) as r:
            text = await r.text()
            json_strs = lxml.html.document_fromstring(text).find('body').text
    try:
        json_obj = json.loads(json_strs)
        if json_obj['desc'] == "上传成功":
            logger.debug(json_obj['path'])
            return json_obj['path']
        else:
            logger.debug(b64, json_obj)
    except Exception as e:
        logger.error('遇到错误:', e, '图片文件：', '')
        raise