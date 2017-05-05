# -*- coding: utf-8 -*-

__author__='Shang Nan'

import asyncio,logging

import aiomysql

def log(sql, args=()):
	"""打印SQL查询语句"""
	logging.info('SQL: %s' % sql)

async def create_pool(loop,**kw):                #async开头就是一个协程,搭配await使用,**kw为一个dict
	"""创建一个全局的连接池，每个HTTP请求都从池中获得数据库连接"""
	logging.info('create database connection pool...')
	global __pool                          #全局变量用于存储整个连接池
	__pool=await aiomysql.create_pool(
		host=kw.get('host','localhost'),   #默认本机IP,get方法获取host对应的value，若不存在，赋为第二个参数
		port=kw.get('port', 3306),
		user=kw['user'],
		password=kw['password'],
		db=kw['db'],
		charset=kw.get('charset', 'utf8'),
		autocommit=kw.get('autocommit', True),  #默认自动提交事务，不用手动去提交事务
		maxsize=kw.get('maxsize', 10),
		minsize=kw.get('minsize', 1),

		loop=loop                           #接受一个event_loop实例
	)

@asyncio.coroutine
def destroy_pool():
	global __pool
	if __pool is not None:
		__pool.close()                     #关闭进程池,close()不是一个协程，不用yield from
		yield from __pool.wait_close()     #wait_close()是一个协程

"""知识点
yield from将会调用一个子协程，并直接返回调用的结果
yield from从连接池中返回一个连接，这个地方已经创建了进程池并和进程池连接了
进程池的创建被封装到了create_pool(loop,**kw)
with所求值的对象必须有一个__enter__()方法和一个 __exit__()方法
紧跟with后面的语句被求值后，返回对象的__enter__()方法被调用，这个方法的返回值将被赋值给
as后的变量，当with后面的代码块全部执行完之后，调用前面返回对象的__exit()__方法
使用该语句的前提是已经创建了进程池，因为这句话是在函数定义里，所以可以这样用
"""
async def select(sql,args,size=None):
	"""封装SQL SELECT语句为select语句"""
	global __pool
	async with __pool.get() as conn:
		async with conn.cursor(aiomysql.DictCursor) as cur:
			await cur.execute(sql.replace('?','%s'), args or ())
			if size:
				rs=await cur.fetchmany(size)
			else:
				rs=await cur.fetchall()
		logging.info('rows returned: %s' % len(rs))
		return rs

async def execute(sql, args,autocommit=True):
	"""
	封装INSERT,UPDATE,DELETE
	语句操作参数一样，所以定义一个通用的执行函数
	返回操作影响的行号
	"""
	log(sql)
	async with __pool.get() as conn:
		if not autocommit:
			await conn.begin()
		try:
			async with conn.cursor(aiomysql.DictCursor) as cur:
				await cur.execute(sql.replace('?','%s'), args)
				affected=cur.rowcount
			if not autocommit:
				await conn.commit()
		except BaseException as e:
			if not autocommit:
				await conn.rollback()
			raise
		return affected


def create_args_string(num):
	"""
	根据输入的参数生成占位符列表
	这个函数主要是把查询字段计数，替换成sql识别的?
	比如说：insert into 'User' ('password','email','name','id') values (?,?,?,?)
	"""
	L = []
	for n in range(num):
		L.append('?')
	return (', '.join(L))        #以','为分隔符，将列表合成字符串

class Field(object):
	"""
	定义Field类，负责保存（数据库）表的字段名和字段类型
	"""
	def __init__(self, name, column_type, primary_key, default):     #表的字段包含名字、类型、是否为表的主键和默认值
		self.name = name
		self.column_type = column_type
		self.primary_key = primary_key
		self.default = default
	def __str__(self):
		return '<%s, %s:%s>' % (self.__class__.__name__, self.column_type, self.name)

class StringField(Field):
	"""
	定义不同类型的衍生Field
    表的不同列的字段的类型不一样
	"""
	def __init__(self, name=None, primary_key=False, default=None, ddl='varchar(100)'):
		super().__init__(name, ddl, primary_key, default)


class BooleanField(Field):
	def __init__(self, name=None, default=False):     #bool类型不能作为主键
		super().__init__(name, 'boolean', False, default)

class IntegerField(Field):
	def __init__(self, name=None, primary_key=False, default=0):
		super().__init__(name, 'bigint', primary_key, default)

class FloatField(Field):
	def __init__(self, name=None, primary_key=False, default=0.0):
		super().__init__(name, 'real', primary_key, default)

class TextField(Field):
	def __init__(self, name=None, default=None):
		super().__init__(name, 'text', False, default)

"""定义Model的元类
所有的元类都继承自type
ModelMetaclass元类定义了所有Model基类(继承ModelMetaclass)的子类实现的操作
ModelMetaclass的工作主要是为一个数据库表映射成一个封装的类做准备：
读取具体子类(user)的映射信息
创造类的时候，排除对Model类的修改
在当前类中查找所有的类属性(attrs)，如果找到Field属性，就将其保存到__mappings__的dict中，同时从类属性中删除Field(防止实例属性遮住类的同名属性)
将数据库表名保存到__table__中
完成这些工作就可以在Model中定义各种数据库的操作方法
metaclass是类的模版，所以必须从‘type’类型派生
"""
class ModelMetaclass(type):
	# __new__控制__init__的执行，所以在其执行之前
	# cls:代表要__init__的类，此参数在实例化时由Python解释器自动提供(例如下文的User和Model)
	# bases：代表继承父类的集合
	# attrs：类的方法集合
	def __new__(cls, name, bases, attrs):
		if name=='Model':       #排除Model,要排除对model类的修改
			return type.__new__(cls, name, bases, attrs)
		tableName = attrs.get('__table__', None) or name      #获取table名词如果存在表名，则返回表名，否则返回name
		logging.info('found model: %s (table: %s)' % (name, tableName))
		mappings = dict()           #获取Field和主键名
		fields = []                 #field保存的是除主键外的属性名
		primaryKey = None
		for k, v in attrs.items():    #k表示字段名
			if isinstance(v, Field):    #Field 属性
				logging.info('  found mapping: %s ==> %s' % (k, v))      #此处打印的k是类的一个属性，v是这个属性在数据库中对应的Field列表属性
				mappings[k] = v
				if v.primary_key:       #找到了主键
					if primaryKey:      #如果此时类实例的以存在主键，说明主键重复了
						raise RuntimeError('Duplicate primary key for field: %s' % k)
					primaryKey = k      #否则将此列设为列表的主键
				else:
					fields.append(k)
		if not primaryKey:
			raise RuntimeError('Primary key not found.')
		for k in mappings.keys():    #从类属性中删除Field属性
			attrs.pop(k)
		escaped_fields = list(map(lambda f: '`%s`' % f, fields))    #保存除主键外的属性名为``（运算出字符串）列表形式
		attrs['__mappings__'] = mappings      #保存属性和列的映射关系
		attrs['__table__'] = tableName        #保存表名
		attrs['__primary_key__'] = primaryKey     #保存主键属性名
		attrs['__fields__'] = fields           #保存除主键外的属性名

		# 构造默认的SELECT、INSERT、UPDATE、DELETE语句
		# ``反引号功能同repr()
		attrs['__select__'] = 'select `%s`, %s from `%s`' % (primaryKey, ', '.join(escaped_fields), tableName)
		attrs['__insert__'] = 'insert into `%s` (%s, `%s`) values (%s)' % (tableName, ', '.join(escaped_fields), primaryKey, create_args_string(len(escaped_fields) + 1))
		attrs['__update__'] = 'update `%s` set %s where `%s`=?' % (tableName, ', '.join(map(lambda f: '`%s`=?' % (mappings.get(f).name or f), fields)), primaryKey)
		attrs['__delete__'] = 'delete from `%s` where `%s`=?' % (tableName, primaryKey)
		return type.__new__(cls, name, bases, attrs)

"""定义ORM所有映射的基类：Model
Model类的任意子类可以映射一个数据库表
Model类可以看作是对所有数据库表操作的基本定义的映射
基于字典查询形式
Model从dict继承，拥有字典的所有功能，同时实现特殊方法__getattr__和__setattr__，能够实现属性操作
实现数据库操作的所有方法，定义为class方法，所有继承自Model都具有数据库操作方法
"""
class Model(dict, metaclass=ModelMetaclass):
	def __init__(self, **kw):
		super(Model, self).__init__(**kw)
	def __getattr__(self, key):
		try:
			return self[key]
		except KeyError:
			raise AttributeError(r"'Model' object has no attribute '%s'" % key)

	def __setattr__(self, key, value):
		self[key] = value

	def getValue(self, key):
		return getattr(self, key, None)

	def getValueOrDefault(self, key):
		value = getattr(self, key, None)
		if value is None:
			field = self.__mappings__[key]
			if field.default is not None:
				value = field.default() if callable(field.default) else field.default
				logging.debug('using default value for %s: %s' % (key, str(value)))
				setattr(self, key, value)
		return value

	#类方法由类变量cls传入，从而可以用cls做一些相关的处理。并且有子类继承时，调用该方法时
	#传入的类变量cls是子类，而非父类
	@classmethod
	@asyncio.coroutine
	def findAll(cls, where=None, args=None, **kw):
		"""find objects by where clause."""
		sql = [cls.__select__]
		if where:
			sql.append('where')
			sql.append(where)
		if args is None:
			args = []
		orderBy = kw.get('orderBy', None)
		if orderBy:
			sql.append('order by')
			sql.append(orderBy)
		limit = kw.get('limit', None)
		if limit is not None:
			sql.append('limit')
			if isinstance(limit, int):
				sql.append('?')
				args.append(limit)
			elif isinstance(limit, tuple) and len(limit) == 2:
				sql.append('?, ?')
				args.extend(limit)
			else:
				raise ValueError('Invalid limit value: %s' % str(limit))
		rs = yield from select(' '.join(sql), args)
		return [cls(**r) for r in rs]

	@classmethod
	@asyncio.coroutine
	def findnumber(cls,selectField,where=None,args=None):
		"""find number by select and where."""
		sql=['select %s _num_ from `%s`' % (selectField,cls.__table__)]
		if where:
			sql.append('where')
			sql.append(where)
		rs=yield from select(' '.join(sql),args,1)
		if len(rs) == 0:
			return None
		return rs[0]['_num_']

	@classmethod
	async def find(cls, pk):
		"""find object by primary key"""
		rs = await select('%s where `%s`=?' % (cls.__select__, cls.__primary_key__), [pk], 1)
		if len(rs) == 0:
			return None
		return cls(**rs[0])

	@asyncio.coroutine
	def save(self):
		args = list(map(self.getValueOrDefault, self.__fields__))
		args.append(self.getValueOrDefault(self.__primary_key__))
		rows = yield from execute(self.__insert__, args)
		if rows != 1:
			logging.warn('failed to insert record: affected rows: %s' % rows)

	@asyncio.coroutine
	def update(self):
		args = list(map(self.getValue, self.__fields__))
		args.append(self.getValue(self.__primary_key__))
		rows = yield from execute(self.__update__, args)
		if rows != 1:
			logging.warn('failed to update by primary key: affected rows: %s' % rows)

	@asyncio.coroutine
	def remove(self):
		args = [self.getValue(self.__primary_key__)]
		rows = yield from execute(self.__delete__, args)
		if rows != 1:
			logging.warn('failed to remove by primary key: affected rows: %s' % rows)

