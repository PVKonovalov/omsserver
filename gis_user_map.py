"""
    omsserver gis_user_map.py
    :copyright: (c) 2019 by Pavel Konovalov
    pasha@nettlecom.com
    Created on 2019-05-08
"""
import json

from helper_session import get_user_id_by_session
from flask import current_app as app
from helper_database import MysqlType
from helper_json import item_from_json


def get_list(db, session_key):
    """
    Return the list of user maps for current user or default from config.ini
    :param session_key:
    :param db:
    :return:
    """
    user_owner_id = get_user_id_by_session(db, session_key)
    r = {}
    cursor = db.cursor()

    sql = 'select id, name, selected, provider_id, scale, sortorder, user_id, layers, ST_X(coordinates) as lng, ' \
          'ST_Y(coordinates) as lat from gis_user_map where user_id = %s order by sortorder, name'
    cursor.execute(sql, user_owner_id)

    lng = 0.0
    lat = 0.0

    if 'GIS_DEFAULT_LNG' in app.config and 'GIS_DEFAULT_LAT' in app.config:
        lng = app.config['GIS_DEFAULT_LNG']
        lat = app.config['GIS_DEFAULT_LAT']

    items = [{'name': app.config['GIS_DEFAULT_MAP_NAME'],
              'coordinates': [lng, lat],
              'provider_id': app.config['GIS_DEFAULT_PROVIDER_ID'],
              'scale': app.config['GIS_DEFAULT_SCALE'],
              'selected': 1,
              'user_id': user_owner_id}]

    cursor_items = cursor.fetchall()

    if len(cursor_items) != 0:
        items = []

    for row in cursor_items:
        item = {}
        for i, value in enumerate(row):
            if value is not None:
                if cursor.description[i][0] == 'lng':
                    lng = value
                elif cursor.description[i][0] == 'lat':
                    lat = value
                elif cursor.description[i][1] == MysqlType.JSON.value:
                    item[cursor.description[i][0]] = json.loads(value)
                else:
                    item[cursor.description[i][0]] = value

        item['coordinates'] = [lng, lat]
        items.append(item)

    r['count'] = len(items)
    r['items'] = items

    return r


def insert(db, session_key, user_map_json):
    """
    Insert user map settings from json for selected user
    :param db:
    :param session_key:
    :param user_map_json:
    :return:
    """
    user_owner_id = get_user_id_by_session(db, session_key)

    name = item_from_json(user_map_json, 'name')
    selected = item_from_json(user_map_json, 'selected', 0)
    provider_id = item_from_json(user_map_json, 'provider_id')
    scale = item_from_json(user_map_json, 'scale')
    sortorder = item_from_json(user_map_json, 'sortorder', 100)
    user_id = item_from_json(user_map_json, 'user_id', user_owner_id)
    layers = json.dumps(item_from_json(user_map_json, 'layers'))
    coordinates = item_from_json(user_map_json, 'coordinates')

    sql_insert = 'insert into gis_user_map (name, selected, provider_id, scale, sortorder, user_id, ' \
                 'layers, coordinates) values(%s,%s,%s,%s,%s,%s,%s,Point(%s,%s))'
    cursor = db.cursor()

    try:
        cursor.execute(sql_insert,
                       (name, selected, provider_id, scale, sortorder, user_id, layers, coordinates[0], coordinates[1]))
        db.commit()

        return {'status': 'Ok', 'id': cursor.lastrowid}

    except Exception:
        return {'status': 'Error', 'code': 200, 'message': 'Error while insert user map. Check your parameters.'}


def update(db, session_key, user_map_json, user_map_id=None):
    """
    Update or insert (if id not present) user map settings from json for selected user. For update id must present
    in parameters or in user_map_json.
    :param user_map_id:
    :param db:
    :param session_key:
    :param user_map_json:
    :return:
    """
    user_owner_id = get_user_id_by_session(db, session_key)

    user_map_id = item_from_json(user_map_json, 'id', user_map_id)

    if user_map_id is None:
        return insert(db, session_key, user_map_json)

    name = item_from_json(user_map_json, 'name')
    selected = item_from_json(user_map_json, 'selected', 0)
    provider_id = item_from_json(user_map_json, 'provider_id')
    scale = item_from_json(user_map_json, 'scale')
    sortorder = item_from_json(user_map_json, 'sortorder', 100)
    user_id = item_from_json(user_map_json, 'user_id', user_owner_id)
    layers = json.dumps(item_from_json(user_map_json, 'layers'))
    coordinates = item_from_json(user_map_json, 'coordinates')

    sql = 'update gis_user_map set name = %s, selected = %s, provider_id = %s, scale = %s, sortorder = %s, ' \
          'user_id = %s, layers = %s , coordinates = Point(%s,%s) where id = %s'
    cursor = db.cursor()

    try:
        cursor.execute(sql, (name, selected, provider_id, scale, sortorder, user_id, layers,
                             coordinates[0], coordinates[1], user_map_id))
        db.commit()

        return {'status': 'Ok'}

    except Exception:
        return {'status': 'Error', 'code': 200, 'message': 'Error while updating user map. Check your parameters.'}


def delete(db, session_key, user_map_id):
    """
    Delete item from user map settings for selected user and id item.
    :param db:
    :param session_key:
    :param user_map_id:
    :return:
    """
    user_owner_id = get_user_id_by_session(db, session_key)

    sql = 'delete from gis_user_map where user_id = %s and id = %s'

    cursor = db.cursor()

    try:
        cursor.execute(sql, (user_owner_id, user_map_id))
        db.commit()

        return {'status': 'Ok'}

    except Exception:
        return {'status': 'Error',
                'code': 200,
                'message': 'Error while deleting from user map for id = {} and user = {}. '
                           'Check your parameters.'.format(user_map_id, user_owner_id)}


def set_current(db, session_key, user_map_id):
    """
    Set selected user map configuration to select state and clear select state from other items for current user
    :param db:
    :param session_key:
    :param user_map_id:
    :return:
    """
    user_owner_id = get_user_id_by_session(db, session_key)
    db.autocommit = False
    cursor = db.cursor()

    try:
        sql = 'select id from gis_user_map where user_id = %s and id = %s'
        cursor.execute(sql, (user_owner_id, user_map_id))
        if len(cursor.fetchall()) == 0:
            cursor.close()
            return {'status': 'Error',
                    'message': 'Error while set selected for user map for id = {} and user = {}. '
                               'Check your parameters.'.format(user_map_id, user_owner_id)}

        sql = 'update gis_user_map set selected = 0 where user_id = %s'
        cursor.execute(sql, user_owner_id)

        sql = 'update gis_user_map set selected = 1 where user_id = %s and id = %s'
        cursor.execute(sql, (user_owner_id, user_map_id))

        db.commit()
        cursor.close()

        return {'status': 'Ok'}

    except Exception:
        db.rollback()
        cursor.close()

        return {'status': 'Error',
                'code': 200,
                'message': 'Error while set selected for user map for id = {} and user = {}. '
                           'Check your parameters.'.format(user_map_id, user_owner_id)}
