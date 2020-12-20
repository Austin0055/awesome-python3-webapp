# -*- coding: utf-8 -*-

__author__ = 'Austin Yuan'

import asyncio, logging

import aiomysql


# 创建连接池：
async def create_pool(loop, **kw):
    logging.info('create database connection pool...')
    global __pool
    __pool = await aiomysql.create_pool(
        host=kw.get('host', 'localhost'),
        port=kw.get('port', 3306),
        user=kw['user'],
        password=kw['password'],
        db=kw['db'],
        charset=kw.get('charset', 'utf8'),
        autocommit=kw.get('autocommit', True),
        maxsize=kw.get('maxsize', 10),
        minsize=kw.get('minsize', 1),
        loop=loop
    )


def log(sql, args=()):
    logging.info('SQL: %s' % sql)


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
        return '<%s, %s:%s>' % (self.__class__.__name__, self.column_type, self.name)


class StringField(Field):

    

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
