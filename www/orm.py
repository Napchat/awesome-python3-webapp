# -*- coding: utf-8 -*-

__author__='Shang Nan'

import asyncio,logging

import aiomysql

#��ӡSQL��ѯ���
def log(sql, args=()):
    logging.info('SQL: %s' % sql)
	
#����һ��ȫ�ֵ����ӳأ�ÿ��HTTP���󶼴ӳ��л�����ݿ�����
@asyncio.coroutine     #��װ�ε�gemerator����һ��Э��
def create_pool(loop,**kw):
	logging.info('create database connection pool...')
	global __pool  #ȫ�ֱ������ڴ洢�������ӳ�
	__pool=yield from aiomysql.create_pool(
		#**kwΪһ��dict,get������ȡhost��Ӧ��value���������ڣ���Ϊ�ڶ�������
		#Ĭ�ϱ���IP
		host=kw.get('host','localhost'),   
		port=kw.get('port', 3306),
        user=kw['user'],
        password=kw['password'],
        db=kw['db'],
        charset=kw.get('charset', 'utf8'),
        autocommit=kw.get('autocommit', True),  #Ĭ���Զ��ύ���񣬲����ֶ�ȥ�ύ����
        maxsize=kw.get('maxsize', 10),
        minsize=kw.get('minsize', 1),
		
		#����һ��event_loopʵ��
        loop=loop
	)
	
@asyncio.coroutine
def destroy_pool():
	global __pool
	if __pool is not None:
		#�رս��̳�,close()����һ��Э�̣�����yield from
		__pool.close()
		#wait_close()��һ��Э��
		yield from __pool.wait_close() 
	
#��װSQL SELECT���Ϊselect���
@asyncio.coroutine
def select(sql,args,size=None):
	log(sql,args)
	global __pool
	
	# -*- yield from �������һ����Э�̣���ֱ�ӷ��ص��õĽ��
    # yield from�����ӳ��з���һ�����ӣ�����ط��Ѿ������˽��̳ز��ͽ��̳�������
	#���̳صĴ�������װ����create_pool(loop,**kw)
	
	#with����ֵ�Ķ��������һ��__enter__()������һ�� __exit__()����
	#����with�������䱻��ֵ�󣬷��ض����__enter__()���������ã���������ķ���ֵ������ֵ��
	#as��ı�������with����Ĵ����ȫ��ִ����֮�󣬵���ǰ�淵�ض����__exit()__����
	
	#ʹ�ø�����ǰ�����Ѿ������˽��̳أ���Ϊ��仰���ں�����������Կ���������
	with (yield from __pool) as conn:
		#DictCursor is a cursor which returns results as a dictionary
		cur=yield from conn.cursor(aiomysql.DictCursor)#A cursor which returns results as a dict
		
		#ִ��SQL���
		#SQL����ռλ��Ϊ����MySQL��ռλ��Ϊ%s
		yield from cur.execute(sql.replace('?','%s'),args or ())
		
		#����ָ�����ص�size�����ز�ѯ�Ľ��,�����һ��list��������tuple
		if size:
			#����size����ѯ���
			rs=yield from cur.fetchmany(size)
		else:
			#�������в�ѯ���
			rs=yield from cur.fetchall()
			
		#�ر��α꣬�����ֶ��ر�conn����Ϊ����with�������Զ��ر�
		yield from cur.close()
		logging.info('rows returned: %s' % len(rs))
	
	return rs
		
#��װINSERT,UPDATE,DELETE
#����������һ�������Զ���һ��ͨ�õ�ִ�к���
#���ز���Ӱ����к�
@asyncio.coroutine
def execute(sql, args):
    log(sql)
    with (yield from __pool) as conn:
        try:
			#execute���͵�SQL�������صĽ��ֻ���кţ����Բ���Ҫ��DictCursor
            cur = yield from conn.cursor()
			
			#args���ܵ��ˣ����˾Ͳ��벻����
            yield from cur.execute(sql.replace('?', '%s'), args)
            affected = cur.rowcount
            yield from cur.close()
        except BaseException as e:
            raise
        return affected
	
#��������Ĳ�������ռλ���б�	
#���������Ҫ�ǰѲ�ѯ�ֶμ������滻��sqlʶ���?
#����˵��insert into 'User' ('password','email','name','id') values (?,?,?,?)
def create_args_string(num):
    L = []
    for n in range(num):
        L.append('?')
	#��','Ϊ�ָ��������б�ϳ��ַ���
    return (', '.join(L))
	
#����Field�࣬���𱣴棨���ݿ⣩����ֶ������ֶ�����	
class Field(object):
	#����ֶΰ������֡����͡��Ƿ�Ϊ���������Ĭ��ֵ
    def __init__(self, name, column_type, primary_key, default):
        self.name = name
        self.column_type = column_type
        self.primary_key = primary_key
        self.default = default

	#����ӡ�����ݿ⣩��ʱ����������ݿ⣩�����Ϣ���������ֶ����ͺ�����
    def __str__(self):
        return '<%s, %s:%s>' % (self.__class__.__name__, self.column_type, self.name)
		
# -*- ���岻ͬ���͵�����Field -*-
# -*- ��Ĳ�ͬ�е��ֶε����Ͳ�һ��
class StringField(Field):

    def __init__(self, name=None, primary_key=False, default=None, ddl='varchar(100)'):
        super().__init__(name, ddl, primary_key, default)
	
#bool���Ͳ�����Ϊ����	
class BooleanField(Field):

    def __init__(self, name=None, default=False):
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
	
# -*-����Model��Ԫ��
 
# ���е�Ԫ�඼�̳���type
# ModelMetaclassԪ�ඨ��������Model����(�̳�ModelMetaclass)������ʵ�ֵĲ���
 
# -*-ModelMetaclass�Ĺ�����Ҫ��Ϊһ�����ݿ��ӳ���һ����װ������׼����
# ***��ȡ��������(user)��ӳ����Ϣ
# �������ʱ���ų���Model����޸�
# �ڵ�ǰ���в������е�������(attrs)������ҵ�Field���ԣ��ͽ��䱣�浽__mappings__��dict�У�ͬʱ����������ɾ��Field(��ֹʵ��������ס���ͬ������)
# �����ݿ�������浽__table__��
 
# �����Щ�����Ϳ�����Model�ж���������ݿ�Ĳ�������
# metaclass�����ģ�棬���Ա���ӡ�type����������
class ModelMetaclass(type):

	# __new__����__init__��ִ�У���������ִ��֮ǰ
    # cls:����Ҫ__init__���࣬�˲�����ʵ����ʱ��Python�������Զ��ṩ(�������ĵ�User��Model)
    # bases������̳и���ļ���
    # attrs����ķ�������
    def __new__(cls, name, bases, attrs):
	
		# �ų�Model,Ҫ�ų���model����޸�
        if name=='Model':
            return type.__new__(cls, name, bases, attrs)
			
		# ��ȡtable����
        tableName = attrs.get('__table__', None) or name #������ڱ������򷵻ر��������򷵻�name
        logging.info('found model: %s (table: %s)' % (name, tableName))
		
		# ��ȡField��������
        mappings = dict()
        fields = []    #field������ǳ��������������
        primaryKey = None
		#k��ʾ�ֶ���
        for k, v in attrs.items():
			# Field ����
            if isinstance(v, Field):
				# �˴���ӡ��k�����һ�����ԣ�v��������������ݿ��ж�Ӧ��Field�б�����
                logging.info('  found mapping: %s ==> %s' % (k, v))
                mappings[k] = v
				# �ҵ�������
                if v.primary_key:
                    # �����ʱ��ʵ�����Դ���������˵�������ظ���
                    if primaryKey:
                        raise StandardError('Duplicate primary key for field: %s' % k)
					# ���򽫴�����Ϊ�б������
                    primaryKey = k	
                else:
                    fields.append(k)
		# end for
		
        if not primaryKey:
            raise StandardError('Primary key not found.')
			
		# ����������ɾ��Field����
        for k in mappings.keys():
            attrs.pop(k)
		
		# ������������������Ϊ``��������ַ������б���ʽ
		# ������������������Ա��`id`,`name`������ʽ
        escaped_fields = list(map(lambda f: '`%s`' % f, fields))
		
		# �������Ժ��е�ӳ���ϵ
        attrs['__mappings__'] = mappings # �������Ժ��е�ӳ���ϵ
		# �������
        attrs['__table__'] = tableName
		# ��������������
        attrs['__primary_key__'] = primaryKey
		# ������������������
        attrs['__fields__'] = fields 
		
		# ����Ĭ�ϵ�SELECT��INSERT��UPDATE��DELETE���
        # ``�����Ź���ͬrepr()
        attrs['__select__'] = 'select `%s`, %s from `%s`' % (primaryKey, ', '.join(escaped_fields), tableName)
        attrs['__insert__'] = 'insert into `%s` (%s, `%s`) values (%s)' % (tableName, ', '.join(escaped_fields), primaryKey, create_args_string(len(escaped_fields) + 1))
        attrs['__update__'] = 'update `%s` set %s where `%s`=?' % (tableName, ', '.join(map(lambda f: '`%s`=?' % (mappings.get(f).name or f), fields)), primaryKey)
        attrs['__delete__'] = 'delete from `%s` where `%s`=?' % (tableName, primaryKey)
        return type.__new__(cls, name, bases, attrs)
		

#����ORM����ӳ��Ļ��ࣺModel
#Model��������������ӳ��һ�����ݿ��
#Model����Կ����Ƕ��������ݿ������Ļ��������ӳ��

#�����ֵ��ѯ��ʽ
#Model��dict�̳У�ӵ���ֵ�����й��ܣ�ͬʱʵ�����ⷽ��__getattr__��__setattr__���ܹ�ʵ�����Բ���
#ʵ�����ݿ���������з���������Ϊclass���������м̳���Model���������ݿ��������
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
		#�����Ĭ�����ú���ʵ�ֵ�
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

	#�෽���������cls���룬�Ӷ�������cls��һЩ��صĴ�������������̳�ʱ�����ø÷���ʱ
	#����������cls�����࣬���Ǹ���
    @classmethod
	@asyncio.coroutine
    def findAll(cls, where=None, args=None, **kw):
        ' find objects by where clause. '
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
        rs = yield from select(' '.join(sql), args) #���ص�rs��һ��Ԫ����tuple��list
        return [cls(**r) for r in rs]   # **r �ǹؼ��ֲ�����������һ��cls����б���ʵ����ÿһ����¼��Ӧ����ʵ��

    @classmethod
	@asyncio.coroutine
    def findNumber(cls, selectField, where=None, args=None):
        ' find number by select and where. '
        sql = ['select %s _num_ from `%s`' % (selectField, cls.__table__)]
        if where:
            sql.append('where')
            sql.append(where)
        rs = yield from select(' '.join(sql), args, 1)
        if len(rs) == 0:
            return None
        return rs[0]['_num_']

    @classmethod
	@asyncio.coroutine
    def find(cls, pk):
        ' find object by primary key. '
        rs = yield from select('%s where `%s`=?' % (cls.__select__, cls.__primary_key__), [pk], 1)
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
	
