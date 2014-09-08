#-*- coding:utf8 -*-

import logging,threading,functools

class Dict(dict):
	"""docstring for Dict"""
	def __init__(self, names=(),values=(),**kw):
		super(Dict, self).__init__(**kw)
		for k , v in zip(names,values):
			self[k]=v

	def __getattr__(self,key):
		try:
			return self[key]
		except KeyError:
			raise AttributeError('Dict has no attribute %s'%key)
		
	def __setattr__(self,key,value):
		self[key]=value


class _LazyConnect(object):
	"""docstring for _LazyConnect"""
	def __init__(self):
		self.connection = None
	
	def cursor(self):
		if self.connection is None:
			connection=engine.connect()
			self.connection=connection
		return self.connection.cursor()

	def rollback(self):
		self.connection.rollback()

	def commit(self):
		self.connection.commit()

	def cleanup(self):
		if self.connection:
			connection=self.connection
			self.connection=None
			connection.close()

class _DbCtx(threading.local):
	def __init__(self):
		self.connection=None
		self.transaction=0

	def is_init(self):
		return not self.connection is None

	def init(self):
		self.connection=_LazyConnect()
		self.transaction=0

	def cursor(self):
		return self.connection.cursor()

	def cleanup(self):
		self.connection.cleanup()

_db_ctx = _DbCtx()

engine=None

class _Engine(object):
	"""docstring for _Engine"""
	def __init__(self, connect):
		self._connect = connect
	def connect(self):
		return self._connect()

def create_engine(user,password,host,database,port,**kw):
	import mysql.connector
	if engine is not None:
		raise DBError('DB engine has already initizilied!')
	params = dict(user=user,password=password,host=host,database=database,port=port)
	defaults = dict(use_unicode=True,charset='utf8',collation='utf8_general_ci',autocommit=False)
	for k,v in defaults.iteritems():
		params[k]=kw.pop(k,v)
	params.update(kw)
	params['buffered']=True
	engine=_Engine(lambda:mysql.connector.connect(**params))

class _ConnectionCtx(object):
	"""docstring for _ConnectionCtx"""
	def __enter__(self):
		global _db_ctx
		self.should_cleanup=False
		if not _db_ctx.is_init():
			_db_ctx.init()
			self.should_cleanup=True
		return self

	def __exit__(self,exctype,excvalue,traceback):
		global _db_ctx
		if self.should_cleanup:
			_db_ctx.cleanup()

def connection():
	return _ConnectionCtx()

def with_connection(func):
	'''
	Decorator for reuse connection
	'''
	@functools.wraps(func)
	def _wrap(*args,**kw):
		with _ConnectionCtx():
			return func(*args,**kw)
	return _wrap

<<<<<<< HEAD
def _select(sql,first,*args):
	global _db_ctx
	cursor=None
	sql = sql.replace('?','%s')
	try:
		cursor = _db_ctx.connection.cursor()
		cursor.execute(sql,args)
		if curor.description:
			names = [x[0] for x in cursor.description]
		if first:
			values = cursor.fetchone()
=======
class _TransactonCtx(object):
	"""docstring for _TransactonCtx"""
	def __enter__(self):
		global _db_ctx
		self.should_close_conn=False
		if not _db_ctx.is_init():
			_db_ctx.init()
			self.should_close_conn=True
		_db_ctx.transaction=_db_ctx.transaction+1
		return self

	def __exit__(self,exctype,excvalue,traceback):
		global _db_ctx
		_db_ctx.transaction=_db_ctx.transaction-1
		try:
			if _db_ctx.transaction==0:
				if exctype is None:
					self.commit()
				else:
					self.rollback()
		finally:
			if self.should_close_conn:
				_db_ctx.cleanup()

	def commit(self):
		global _db_ctx
		try:
			_db_ctx.connection.commit()
		except:
			raise

	def rollback(self):
		global _db_ctx
		_db_ctx.connection.rollback()

def transaction():
	return _TransactonCtx()

def with_transaction(func):
	@functools.wraps(func)
	def _wrapper(*args,**kw):
		with _TransactionCtx():
			return func(*args,**kw)
	return _wrapper



def _select(sql,first,*args):
	global _db_ctx
	cursor=None
	sql=sql.replace('?','%s')
	try:
		cursor=_db_ctx.connection.cursor()
		cursor.execute(sql,*args)
		if cursor.description:
			names=[x[0] for x in cursor.description]
		if first:
			values=cursor.fetchone()
>>>>>>> origin/master
			if not values:
				return None
			return Dict(names,values)
		return [Dict(names,x) for x in cursor.fetchall()]
	finally:
		if cursor:
			cursor.close()

@with_connection
def select_one(sql,*args):
	return _select(sql,True,*args)

@with_connection
def select_int(sql,*args):
	d=_select(sql,True,*args)
	if len(d) !=1:
<<<<<<< HEAD
		raise MultiConlumnsError()
	return d.values()[0]
=======
		raise MultiColumnsError()
	return d.values()[0]

@with_connection
def select(sql,*args):
	return _select(sql,False,*args)


@with_connection
def _update(sql,*args):
	global _db_ctx
	cursor=None
	sql=sql.replace('?','%s')
	try:
		cursor=_db_ctx.connection.cursor()
		cursor.excute(sql,*args)
		r=cursor.rawcount
		if _db_ctx.transaction==0:
			_db_ctx.connection.commit()
		return r
	finally:
		if cursor:
			cursor.close()

def insert(table,*kwargs):
	cols,args = zip(*kwargs.iteritems())
	sql = "insert into %s (%s) values(%s)"%(table,','.join(cols),','.join(['?' for i in range(len(cols))]))
	return _update(sql,*args)

def update(sql,*args):
	return _update(sql,*args)
>>>>>>> origin/master
