# -*- coding: utf-8 -*-

__author__='Shang Nan'

import asyncio,os,inspect,logging,functools

"""urlib
This module defines a standard interface to break Uniform Resource Locator (URL) strings
up in components (addressing scheme, network location, path etc.),
to combine the components back into a URL string, and to convert
a “relative URL” to an absolute URL given a “base URL.”
"""
from urllib import parse

from aiohttp import web

from apis import APIError

def get(path):                               #一个函数通过@get()的装饰就附带了URL信息
	'''Define decorator @get('/path')'''
	def decorator(func):
		@functools.wraps(func)
		def wrapper(*args, **kw):
			return func(*args, **kw)
		wrapper.__method__ = 'GET'
		wrapper.__route__ = path
		return wrapper
	return decorator

def post(path):                              #类似get
	'''Define decorator @post('/path')'''
	def decorator(func):
		@functools.wraps(func)
		def wrapper(*args, **kw):
			return func(*args, **kw)
		wrapper.__method__ = 'POST'
		wrapper.__route__ = path
		return wrapper
	return decorator

def get_required_kw_args(fn):
	args=[]
	params=inspect.signature(fn).parameters   #获得函数fn的签名里的参数
	for name,param in params.items():
		if param.kind==inspect.Parameter.KEYWORD_ONLY and param.default==inspect.Parameter.empty:
			args.append(name)
	return tuple(args)

def get_named_kw_args(fn):
	args=[]
	params=inspect.signature(fn).parameters
	for name,param in params.items():
		if param.kind==inspect.Parameter.KEYWORD_ONLY:
			args.append(name)
	return tuple(args)

def has_named_kw_args(fn):
	params = inspect.signature(fn).parameters
	for name, param in params.items():
		if param.kind == inspect.Parameter.KEYWORD_ONLY:
			return True

def has_var_kw_arg(fn):
	params = inspect.signature(fn).parameters
	for name, param in params.items():
		if param.kind == inspect.Parameter.VAR_KEYWORD:
			return True

def has_request_arg(fn):
	sig = inspect.signature(fn)
	params = sig.parameters
	found = False
	for name, param in params.items():
		if name == 'request':
			found = True
			continue
		if found and (param.kind != inspect.Parameter.VAR_POSITIONAL and param.kind != inspect.Parameter.KEYWORD_ONLY and param.kind != inspect.Parameter.VAR_KEYWORD):
			raise ValueError('request parameter must be the last named parameter in function: %s%s' % (fn.__name__, str(sig)))
	return found

class RequestHandler(object):      #To encapsulate a URL-handling function
	"""
	URL处理函数不一定是一个coroutine，因此我们用RequestHandler()来封装一个URL处理函数
	从URL函数中分析其需要接收的参数，从request中获取必要的参数，调用URL函数，
	然后把结果转换为web.Response对象，这样，就完全符合aiohttp框架的要求
	"""
	def __init__(self, app, fn):
		self._app = app
		self._func = fn
		self._has_request_arg = has_request_arg(fn)          #是否有request参数
		self._has_var_kw_arg = has_var_kw_arg(fn)            #是否有变长字典参数
		self._has_named_kw_args = has_named_kw_args(fn)      #是否存在关键字参数
		self._named_kw_args = get_named_kw_args(fn)          #所有关键字参数
		self._required_kw_args = get_required_kw_args(fn)    #所有没有默认值的关键字参数

	@asyncio.coroutine
	def __call__(self, request):          #由于定义了__call__()方法，因此可以将该类的实例视为函数
		"""URL-handling function"""
		kw = None
		if self._has_var_kw_arg or self._has_named_kw_args or self._required_kw_args:
			if request.method == 'POST':
				if not request.content_type:
					return web.HTTPBadRequest('Missing Content-Type.')
				ct = request.content_type.lower()
				if ct.startswith('application/json'):
					params = yield from request.json()
					if not isinstance(params, dict):
						return web.HTTPBadRequest('JSON body must be object.')
					kw = params
				elif ct.startswith('application/x-www-form-urlencoded') or ct.startswith('multipart/form-data'):
					params = yield from request.post()
					kw = dict(**params)
				else:
					return web.HTTPBadRequest('Unsupported Content-Type: %s' % request.content_type)
			if request.method == 'GET':
				qs = request.query_string
				if qs:
					kw = dict()
					for k, v in parse.parse_qs(qs, True).items():
						kw[k] = v[0]

		if kw is None:                          #如果没有在GET或POST里取得参数，直接把match_info的所有参数提取到kw
			kw = dict(**request.match_info)
		else:
			if not self._has_var_kw_arg and self._named_kw_args:     #如果没有变长字典参数且有关键字参数，把所有关键字参数提取出来，忽略所有变长字典参数
				# remove all unamed kw:
				copy = dict()
				for name in self._named_kw_args:
					if name in kw:
						copy[name] = kw[name]
				kw = copy
			# check named arg:
			for k, v in request.match_info.items():         #把match_info的参数提取到kw，检查URL参数和HTTP方法得到的参数是否又重合
				if k in kw:
					logging.warning('Duplicate arg name in named arg and kw args: %s' % k)
				kw[k] = v
		if self._has_request_arg:            #把request参数提取到kw
			kw['request'] = request
		# check required kw:
		if self._required_kw_args:           #检查没有默认值的关键字参数是否已赋值
			for name in self._required_kw_args:
				if not name in kw:
					return web.HTTPBadRequest('Missing argument: %s' % name)
		logging.info('call with args: %s' % str(kw))
		try:
			r = yield from self._func(**kw)
			return r
		except APIError as e:
			return dict(error=e.error, data=e.data, message=e.message)

def add_static(app):
	path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'static')
	app.router.add_static('/static/', path)
	logging.info('add static %s => %s' % ('/static/', path))

def add_route(app, fn):
	'''注册URL处理函数'''
	method = getattr(fn, '__method__', None)
	path = getattr(fn, '__route__', None)
	if path is None or method is None:
		raise ValueError('@get or @post not defined in %s.' % str(fn))
	if not asyncio.iscoroutinefunction(fn) and not inspect.isgeneratorfunction(fn):
		fn = asyncio.coroutine(fn)
	logging.info('add route %s %s => %s(%s)' % (method, path, fn.__name__, ', '.join(inspect.signature(fn).parameters.keys())))
	app.router.add_route(method, path, RequestHandler(app, fn))

def add_routes(app, module_name):
	'''自动将所有URL处理函数注册'''
	# 首先导入模块
	n = module_name.rfind('.')
	if n == (-1):
		mod = __import__(module_name, globals(), locals())				#动态加载模块
	else:
		name = module_name[n+1:]
		mod = getattr(__import__(module_name[:n], globals(), locals(), [name]), name)
	for attr in dir(mod):
		if attr.startswith('_'):
			continue
		fn = getattr(mod, attr)
		if callable(fn):
			method = getattr(fn, '__method__', None)
			path = getattr(fn, '__route__', None)
			if method and path:
				add_route(app, fn)
