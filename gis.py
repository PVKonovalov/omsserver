"""
    pathfinder gis.py
    :copyright: (c) 2019 by Pavel Konovalov
    pasha@nettlecom.com
    Created on 2019-03-18
"""

import json
import time

from helper_database import cursor_to_json, MysqlType


def layer_group_get_list(db):
    """
    Return layer group list with subgroup
    :param db:
    :return:
    """
    r = {}
    cursor = db.cursor()

    sql = 'SELECT id, name ' \
          'FROM gis_layer_group ' \
          'WHERE enabled = 1 ' \
          'ORDER BY sortorder, name'
    cursor.execute(sql)

    items = cursor_to_json(cursor)

    for item in items:
        sql = 'SELECT id, name, group_id ' \
              'FROM gis_layer_subgroup ' \
              'WHERE enabled = 1 and group_id = %s ' \
              'ORDER BY sortorder, name'

        cursor.execute(sql, item['id'])
        subgroups = cursor_to_json(cursor)
        item['subgroups'] = subgroups
        item['count'] = len(subgroups)

    r['count'] = len(items)
    r['items'] = items

    return r


def layer_subgroup_get_list(db):
    """
    Return layer subgroup list
    :param db:
    :return:
    """
    r = {}
    cursor = db.cursor()

    sql = 'select id, name, group_id from gis_layer_subgroup where enabled = 1 order by sortorder, name'
    cursor.execute(sql)

    items = cursor_to_json(cursor)

    r['count'] = len(items)
    r['items'] = items

    return r


def object_state_style_get_list(db):
    """
    Return list of styles of objects
    :param db:
    :return:
    """
    r = {}
    cursor = db.cursor()

    sql = 'select * from object_state_style order by id'
    cursor.execute(sql)

    items = cursor_to_json(cursor)

    r['count'] = len(items)
    r['items'] = items

    return r


def map_provider_get_list(db):
    """
    Return the list of map's enabled providers
    :param db:
    :return:
    """
    r = {}
    cursor = db.cursor()

    sql = 'select * from gis_map_provider where enabled = 1 order by sortorder, name'
    cursor.execute(sql)

    items = cursor_to_json(cursor)

    r['count'] = len(items)
    r['items'] = items

    return r

def layer_get_list(db):
    """
    Return the list of enabled layers
    :param db:
    :return:
    """
    start = time.time()
    r = {}
    cursor = db.cursor()

    sql = 'select * from gis_layer where enabled = 1 order by sortorder, name'
    cursor.execute(sql)

    items = []
    for row in cursor.fetchall():
        item = {}
        for i, value in enumerate(row):
            if value is not None:
                if cursor.description[i][1] == MysqlType.JSON.value:
                    item[cursor.description[i][0]] = json.loads(value)
                else:
                    item[cursor.description[i][0]] = value
        items.append(item)

    r['count'] = len(items)
    r['items'] = items
    r['elapsed'] = time.time() - start

    return r


def layer_get_item(db, layer_id):
    """
    Return the layer for selected index
    :param db:
    :param layer_id:
    :return:
    """
    if layer_id is None or layer_id <= 0:
        return {'status': 'Error', 'message': 'Layer id {} is not acceptable'.format(layer_id)}

    sql = 'select * from gis_layer where id = %s'

    cursor = db.cursor()
    cursor.execute(sql, layer_id)

    item = {}

    row = cursor.fetchone()
    if row is not None:
        for i, value in enumerate(row):
            if value is not None:
                if cursor.description[i][1] == MysqlType.JSON.value:
                    item[cursor.description[i][0]] = json.loads(value)
                else:
                    item[cursor.description[i][0]] = value

    return item



