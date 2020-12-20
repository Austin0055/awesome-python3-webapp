import logging
import asyncio
import os
import json
import time
from datetime import datetime

from aiohttp import web
logging.basicConfig(level=logging.INFO)


def index(request):
    return web.Response(body=b'<h1>Awesome</h1>', content_type='html')


async def init(loop):
    app = web.Application()
    app.router.add_route('GET', '/', index)
    app_runner = web.AppRunner(app)
    await app_runner.setup()
    srv = await loop.create_server(app_runner.server, '127.0.0.1', 9000)
    logging.info('server started at http://127.0.0.1:9000...')
    return srv
    # 之前的写法：
    # srv = await loop.create_server(app.make_handler(), '127.0.0.1', 9000)
    # logging.info('server started at http://127.0.0.1:9000...')
    # return srv
    # 网上的一种解决方法
    # runner = web.AppRunner(app)
    # await runner.setup()
    # site = web.TCPSite(runner, '127.0.0.1', 9000)
    # logging.info('server started at http://127.0.0.1:9000...')
    # await site.start()


loop = asyncio.get_event_loop()
loop.run_until_complete(init(loop))
loop.run_forever()
