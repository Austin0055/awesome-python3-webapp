# -*- coding: utf-8 -*-

__author__ = 'Austin Yuan'

import asyncio, logging

import aiomysql


# 创建连接池：
async def create_pool(loop, **kw):
    logging.info('create database connection pool...')
    global __pool
    __pool = await aiomysql.create_pool(
        host=kw.get('host', 'localhost'),  # 默认定义host名字为localhost
        port=kw.get('port', 3306),  # 默认定义mysql的默认端口是3306
        user=kw['user'],  # user是通过关键字参数传进来的
        password=kw['password'],  # 密码也是通过关键字参数传进来的
        db=kw['db'],  # 数据库名字
        charset=kw.get('charset', 'utf8'),  # 默认数据库字符集是utf8
        autocommit=kw.get('autocommit', True),  # 默认自动提交事务
        maxsize=kw.get('maxsize', 10),  # 连接池最多同时处理10个请求
        minsize=kw.get('minsize', 1),  # 连接池最少1个请求
        loop=loop  # 传递消息循环对象loop用于异步执行
    )


def log(sql, args=()):
    logging.info('SQL: %s' % sql)


# 该函数在ModelMetaclass中调用
def create_args_string(num):
    L = []
    for n in range(num):
        L.append('?')
    return ', '.join(L)


# Select：
async def select(sql, args, size=None):
    log(sql, args)
    global __pool
    # 异步等待连接池对象返回可以连接的线程，with保证了出现异常时能够关闭conn
    async with __pool.get() as conn:
        # 等待连接对象返回游标，这里定义了aiomysql.DictCursor，是为了能够让数据库访问时返回dict结构的列表结果
        async with conn.cursor(aiomysql.DictCursor) as cur:
            # SQL语句的占位符是?，而MySQL的占位符是%s（我们要用的），让这个select()函数在内部自动替换。
            await cur.execute(sql.replace('?', '%s'), args or ())
            # 如果传入size参数，就通过fetchmany()获取最多指定数量的记录
            if size:
                rs = await cur.fetchmany(size)
            # 否则，通过fetchall()获取所有记录。
            else:
                rs = await cur.fetchall()
        logging.info('rows returned: %s' % len(rs))
        return rs


# INSERT, UPDATE, DELETE:
async def execute(sql, args, autocommit=True):
    log(sql)
    async with __pool.get() as conn:
        # 如果设置不自动提交，则手动开启
        if not autocommit:
            await conn.begin()
        try:
            # 同select（）函数中的返回dict样式的结果
            async with conn.cursor(aiomysql.DictCursor) as cur:
                # 将sql形式的占位符换成mysql形式
                await cur.execute(sql.replace('?', '%s'), args)
                # 返回受影响的行数
                affected = cur.rowcount
            # 如果设置不自动提交，则手动提交
            if not autocommit:
                await conn.commit()
        except BaseException as e:
            # 如果设置不自动提交时出错，则回滚到这些INSERT，UPDATE,DELETE操作之前
            if not autocommit:
                await conn.rollback()
            raise e
        return affected


class Field:

    def __init__(self, name, column_type, primary_key, default):
        self.name = name
        self.column_type = column_type
        self.primary_key = primary_key
        self.default = default

    def __str__(self):
        # return '<%s, %s:%s>' % (self.__class__.__name__, self.column_type, self.name)
        return '<{0.__class__.__name__}, {0.column_type}: {0.name}'.format(self)


# 映射varchar的StringField：
class StringField(Field):

    # String一般不作为主键，所以默认False, DDL是数据定义语言，为了配合mysql，所以默认设定为100的长度
    def __init__(self, name=None, primary_key=False, default=None, ddl='varchar(100)'):
        super().__init__(name, ddl, primary_key, default)


class BooleanField(Field):

    def __init__(self, name=None, default=Field):
        super().__init__(name, 'boolean', False, default)


class IntegerField(Field):

    def __init__(self, name=None, primary_key=False, default=0):
        super().__init__(name, 'bigint', primary_key, default)


class FloatField(Field):

    def __init__(self, name=None, primary_key=False, default=0.0):
        super().__init__(name, 'real', primary_key, default)


class TextField(Field):

    def __init__(self, name=None, default=None):
        # 不能作为主键的对象，所以这里直接就设定成False了
        super().__init__(name, 'text', False, default)


# 任何继承自Model的类，都会自动通过ModelMetaclass扫描映射关系，并存储到自身的类属性
class ModelMetaclass(type):

    def __new__(cls, name, bases, attrs):
        # cls: 当前准备创建的类对象,相当于self
        # name: 类名,比如User继承自Model,当使用该元类创建User类时,name=User
        # bases: 父类的元组
        # attrs: 属性(方法)的字典,比如User有__table__,id,等,就作为attrs的keys
        # 排除Model类本身,因为Model类主要就是用来被继承的,其不存在与数据库表的映射
        if name == 'Model':
            return type.__new__(cls, name, bases, attrs)
        # 找到表名，若没有定义__table__属性,将类名作为表名
        tableName = attrs.get('__table__', None) or name
        logging.info('found model: %s (table: %s)' % (name, tableName))
        # 建立映射关系表和找到主键
        mappings = {}  # 用于保存映射关系
        fields = []  # 用于保存所有字段名
        primaryKey = None  # 保存主键
        # 遍历类的属性,找出定义的域(如StringField,字符串域)内的值,建立映射关系
        # k是属性名,v其实是定义域!请看name=StringField(ddl="varchar50")
        for k, v in attrs.copy().items():
            if isinstance(v, Field):
                logging.info('found mapping: %s => %s' % (k, v))
                mappings[k] = attrs.pop(k)
                # 查找并检验主键是否唯一，主键初始值为None，找到一个主键后会被设置为key
                if v.primary_key:
                    # 若if val.primary_key: 再次为真，则会报错
                    if primaryKey:
                        raise KeyError('Duplicate primary key for field: %s' % k)
                    primaryKey = k
                else:
                    # 将非主键的属性名都保存到fields
                    fields.append(k)
        if not primaryKey:  # 没有找到主键也将报错
            raise KeyError('Primary key not found.')
        escaped_fields = list(map(lambda f: '`%s`' % f, fields))
        # 创建新的类属性：
        attrs['__table__'] = tableName  # 保存表名
        attrs['__mappings__'] = mappings  # 保存属性和列的映射关系
        attrs['__primary_key__'] = primaryKey  # 主键属性名
        attrs['__fields__'] = fields  # 除主键外的属性名
        # -----------------------默认SQL语句--------------------------
        attrs['__select__'] = 'select `%s`, %s from `%s`' % (primaryKey, ', '.join(escaped_fields), tableName)
        attrs['__insert__'] = 'insert into `%s` (%s, `%s`) values (%s)' % (
        tableName, ', '.join(escaped_fields), primaryKey, create_args_string(len(escaped_fields) + 1))
        attrs['__update__'] = 'update `%s` set %s where `%s`=?' % (
        tableName, ', '.join(map(lambda f: '`%s`=?' % (mappings.get(f).name or f), fields)), primaryKey)
        attrs['__delete__'] = 'delete from `%s` where `%s`=?' % (tableName, primaryKey)
        return type.__new__(cls, name, bases, attrs)


# ORM映射基类，继承自dict，通过ModelMetaclass元类来构造类
class Model(dict, metaclass=ModelMetaclass):

    def __init__(self, **kw):
        super().__init__(**kw)

    # 增加__getattr__方法，使获取属性更加简单，即可通过"a.b"的形式
    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Model' object has no attribute '%s'" % key)

    # 增加__setattr__方法，使设置属性更方便，可通过"a.b=c"的形式
    def __setattr__(self, key, value):
        self[key] = value

    def getValue(self, key):
        return getattr(self, key, None)

    # 通过键取值,若值不存在,则取默认值
    def getValueOrDefault(self, key):
        value = getattr(self, key, None)
        if value is None:
            # 注意这个__mappings__方法在元类中定义，且在Model类中没有，但是在以Model为父类的类中是有的
            field = self.__mappings__[key]
            if field is not None:
                # 尚不明白这个default方法在哪里
                # 如果field.default可被调用，则返回field.default()，否则返回field.default
                value = field.default() if callable(field.default) else field.default
                logging.debug('using default value for %s: %s' % (key, str(value)))
                # 通过default取到值之后再将其作为当前值
                setattr(self, key, value)
        return value

    # 对于查询相关的操作，我们都定义为类方法，就可以方便查询，而不必先创建实例再查询
    # 查找所有合乎条件的信息
    @classmethod
    async def findAll(cls, where=None, args=None, **kw):
        """ find object by where clause."""
        # 初始化SQL语句和参数列表：
        sql = [cls.__select__]
        if args is None:
            args = []
        # WHERE 查找条件的关键字
        if where:
            sql.append('where')
            sql.append(where)
        # ORDER BY，按照啥关键字排序：
        if kw.get('orderBy', None) is not None:
            sql.append('order by')
            sql.append(kw['orderBy'])
        # LIMIT 要来对结果集进行筛选：
        limit = kw.get('limit', None)
        if limit is not None:
            sql.append('limit')
            if isinstance(limit, int):  # 如果是int类型，则增加占位符
                sql.append('?')
                args.append(limit)
            elif isinstance(limit, tuple) and len(limit) == 2:  # limit可以取2个参数，表示一个范围
                sql.append('?, ?')
                args.append(limit)
            else:
                raise ValueError('Invalid limit value: %s' % str(limit))
        # 调用前面定义的select函数，没有指定size,因此会fetchall,即返回所有结果
        rs = await select(' '.join(sql), args)
        # 从Model的初始化可以得知，以Model为父类的实例也是会得到字典结果，即返回一个元素为字典的列表：
        return [cls(**r) for r in rs]

    # 根据列名和条件查看数据库有多少条信息
    @classmethod
    async def findNumber(cls, selectField, where=None, args=None):
        """ find number by select and where. """
        sql = ['select %s _num_ from `%s`' % (selectField, cls.__table__)]
        if where:
            sql.append('where')
            sql.append(where)
        rs = await select(' '.join(sql), args, 1)  # size = 1
        return rs[0]['_num_'] if len(rs) != 0 else None

    # 根据主键查找一个实例的信息
    @classmethod
    async def find(cls, pk):
        """find object by primary key"""
        rs = await select('%s where `%s`=?' % (cls.__select__, cls.__primary_key__), [pk], 1)
        return cls(**rs[0]) if len(rs) != 0 else None

    # 把一个实例保存到数据库
    async def save(self):
        args = list(map(self.getValueOrDefault, self.__fields))
        args.append(self.getValueOrDefault(self.__primary_key__))
        rows = await execute(self.__insert__, args)
        if rows != 1:
            logging.warning('failed to insert record: affected rows: %s' % rows)

    # 更改一个实例在数据库的信息
    async def update(self):
        args = list(map(self.getValue, self.__fields__))
        args.append(self.getValue(self.__primary_key__))
        rows = await execute(self.__update__, args)
        if rows != 1:
            logging.warning('failed to update by primary key: affected rows: %s' % rows)

    # 把一个实例从数据库中删除
    async def remove(self):
        args = [self.getValue(self.__primary_key__)]
        rows = await execute(self.__delete__,args)
        if rows != 1:
            logging.warning('failded to remove by primary key: affected rows: %s' % rows)

    # 参考中的一个函数：
    # def to_json(self, **kw):
    #     return self.copy()




