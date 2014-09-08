# coding:utf8

__author__ = 'hunter'

import logging,time
from transwarp import db
try:
	from cStringIO import cStringIO
except:
	from StringIO import StringIO


class Field(object):
	"""docstring for Field"""
	_count = 0
	def __init__(self, **kw):
		self.name= kw.get('name',None)
		self._default=kw.get('default',None)
		self.primary_key = kw.get('primary_key',False)
		self.nullable = kw.get('nullable',False)
		self.updateable=kw.get('updateable',True)
		self.inertable=kw.get('insertable',True)
		self.ddl=kw.get('ddl','')
		self._order=Field._count
		Field._count=Field._count+1

	@property
	def default(self):
		d=self._default
	    return d() if callable(d) else d

	def __str__(self):
		s=['<%s:%s,%s,default(%s),'%(self.__class__.__name__,self.name,self.ddl,self._default)]
		self.nullable and s.append('N')
		self.updateable and s.append('U')
		self.inertable and s.append('I')
		s.append('>')
		return ''.join(s)



class StringField(Field):
	"""docstring for StringField"""
	def __init__(self, **kw):
		if not 'default' in kw:
			kw['default']=''
		if not 'ddl' in kw:
			kw['ddl']='varchar(255)'
		super(StringField, self).__init__(**kw)

class IntegerField(Field):
	"""docstring for IntegerField"""
	def __init__(self, **kw):
		if not 'default' in kw:
			kw['default'] =0
		if not 'ddl' in kw:
			kw['ddl']='bigint'
		super(IntegerField, self).__init__(**kw)

class FloatField(Field):
	"""docstring for FloatField"""
	def __init__(self, **kw):
		if not 'default' in kw:
			kw['default'] =0.0
		if not 'ddl' in kw:
			kw['ddl']='real'
		super(FloatField, self).__init__(**kw)

class DateField(Field):
	"""docstring for DateField"""
	def __init__(self, **kw):
		if not 'default' in kw:
			kw['default'] =
		if not 'ddl' in kw:
			kw['ddl']=
		super(DateField, self).__init__(**kw)
class Boolean(Field):
	"""docstring for Boolean"""
	def __init__(self, **kw):
		if not 'default' in kw:
			kw['default'] =False
		if not 'ddl' in kw:
			kw['ddl']='bool'
		super(Boolean, self).__init__(**kw)

class BlobField(Field):
	"""docstring for BlobField"""
	def __init__(self, **kw):
		if not 'default' in kw:
			kw['default'] = ''
		if not 'ddl' in kw:
			kw['ddl'] = 'blob'
		super(BlobField, self).__init__(**kw)
		
class VersionField(Field):
	"""docstring for ClassName"""
	def __init__(self, name=None):
		super(VersionField, self).__init__(name=name,default=0,ddl='bigint')

_triggers = frozenset(['pre_insert','pre_update','pre_delete'])

def _gen_sql(table_name,mappings):
	pk=None
	sql=['--generating SQL for %s:' %table_name,'create table `%s`('%table_name]
    for f in sorted(mappings.values(),lambda x,y:cmp(x._order,y._order)):
        if not hasattr(f,'ddl'):
            raise StandardError('no ddl in field:%s'%f)
        ddl=f.ddl
        nullable=f.nullable
        if f.primary_key:
            pk=f.primary_key
        sql.append(nullable and '%s %s'%(f.name,ddl) or '%s %s not null'%(f.name,ddl))
    sql.append(' primary key(%s)'%pk)
    sql.append(');')
    return '\n'.join(sql)


#MetaClass starts here

class ModelMetaClass(type):
	"""docstring for MetaClass"""
	def __new__(cls,name,bases,attrs):
        if name=='Model':
            return type.__new__(cls,name,bases,attrs)
        if not hasattr(cls,'subclasses'):
            cls.subclasses={}
        if not name in subclasses:
            cls.subclasses['name']=name

        mappings=dict()
        primary_key=None
        #Scaning ORMappings...
        for k,v in attrs.iteritems():
            if isinstance(v,Field):
                if not v.name:
                    v.name=k
                if v.primary_key:
                    raise StandardError('Cant defined more than 1 primary key in class %s'%name)
                if v.updateable:
                    v.updateable=False
                if v.inertable:
                    v.inertable=False
                primary_key=v
            mappings[k]=v
        if not primary_key:
            raise StandardError('None primary_key defined in class %s'%name)
        for k in mappings.keys():
            attrs.pop(k)
        if not '__table__' in attrs:
            attrs['__table__'] = name.lower()
        attrs['__mapping__'] = mappings
        attrs['__primary_key__']=primary_key
        attrs['__sql__']=lambda self:_gen_sql(attrs['__table__'],mappings)
        for trigger in _triggers:
            if not trigger in attrs:
                attrs['trigger']=None
        return type.__new__(cls,name,bases,attrs)



class Model(dict):
	__metaclass__ = ModelMetaClass
	
    def __init__(self,**kw):
        super(Model,self).__init__(**kw)

    def __getattr__(self,key):
        try:
            return self[key]
        except:
            raise AttributeError(r"Dict object has no attribute '%s'"%key)

    def __setattr__(self,key,value):
        self[key]=value

#query data by primary key 
    @classmethod
    def get(cls,pk):
        d=db.select_one('select * from %s where %s=?'%(cls.__table__,cls.__primary_key__.name),pk)
        return cls(**d) if d else None

    @classmethod
    def find_first(cls,where,*args):
        d=db.select_one('select * from %s %'%(cls.__table__,where),*args)
        return cls(**d) if d else None

    @classmethod
    def find_by(cls,where,*args):
        L=db.select('select * from %s %s'%(cls.__table__,where),*args)
        return [cls(**d) for d in L]

    @classmethod
    def find_all(cls,*args):
        l=db.select('select * from %s'%(cls.__table__))
        return[cls(**d) for d in l]

    @classmethod
    def count_all(cls):
        return db.select_int('select count(%s) from %s'%(cls.__primary_key__,cls.__table__))

    @classmethod
    def count_by(cls,where,*args):
        return db.select_int('select count(%s) from %s %s'%(cls.__primary_key__,cls.__table__,where),*args)

    def update(self):
        pass

    def insert(self):
        self.pre_insert and self.pre_insert()
        params={}
        for k,v in self.__mapping__.iteritems():
            if v.insertable:
                if not hasattr(self,k):
                    setattr(self,k,v.default)
                params[v.name]=getattr(self,k)
        db.insert('%s'%self.__table__,**params)
        return self

    def delete():
        pass
