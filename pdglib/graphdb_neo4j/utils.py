# -*- coding:utf8 -*-
import json
from py2neo import Node, Relationship, Graph, authenticate
import py2neo
import py2neo.cypher.error.schema as SchemaErrors
import py2neo.cypher.error.core as Errors
import graphdb_interface as I
from uuid import uuid4
import time
import unicodedata as ud
import re


def uuid():
    return uuid4().hex

def get_time():
    """
    return a timestamp as (long) seconds since Epoch
    """
    return long(time.time())

