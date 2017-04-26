# -*- coding: utf-8 -*-

__author__='Shang Nan'

'''
async web application.
'''
import logging;logging.basicConfig(level=logging.INFO)

import asyncio,os,json,time
import orm
from models import User,Blog,Comment
from datetime import datetime
from aiohttp import web

def index(request):
	return web.Response(body=b'<h1>Awesome</h1>',content_type='text/html',charset='UTF-8')

@asyncio.coroutine
def test():
	yield from orm.create_pool(loop=loop,user='root',password='2291277',db='awesome')
	u=User(name='Test',email='test@example.com',passwd='123456789',image='about:blank')
	yield from u.save()
	r=yield from u.findAll()
	print(r)

async def init(loop):
	app=web.Application(loop=loop)
	app.router.add_route('GET','/',index)
	srv=await loop.create_server(app.make_handler(),'127.0.0.1',9000)
	logging.info('server started at http://127.0.0.1:9000...')
	return srv

loop=asyncio.get_event_loop()
loop.run_until_complete(test())
test()

