# -*- coding: utf-8 -*-

'''
Models for user, blog, comment.
'''

__author__ = 'Austin Yuan'

import time, uuid

form orm import Model, StringField, BoolenField, FloatField, TextField

class User(Model):
    __table__ = 'users'

    id = IntegerField(primary_key=True)
    name = StringField()
