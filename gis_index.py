"""
    omsserver gis_index.py
    :copyright: (c) 2019 by Pavel Konovalov
    pasha@nettlecom.com
    Created on 2019-05-08
"""

from helper_database import cursor_to_json
from helper_uuid import is_valid_uuid


def search_in_index(db, name, layers=None):
    """
    Searching in index for name and layer
    :param layers: 1,2,3...
    :param db:
    :param name:
    :return:
    """
    r = {}
    items = []

    if name is not None and name != '':
        cursor = db.cursor()

        if layers is not None:
            params = layers.strip(',').split(',')
            format_strings = ','.join(['%s'] * len(params))
            params.append('%' + name + '%')

            sql = 'select BIN_TO_UUID(guid) as guid, name, ST_X(coordinates) as lng, ST_Y(coordinates) as lat, ' \
                  'gis_layer_id as layer from gis_index where gis_layer_id ' \
                  'in (' + format_strings + ') and upper(name) like upper(%s) order by name'
            cursor.execute(sql, params)
        else:
            cursor.execute(
                'select BIN_TO_UUID(guid) as guid, name, ST_X(coordinates) as lng, ST_Y(coordinates) as lat, '
                'gis_layer_id as layer from gis_index where upper(name) like upper(%s) order by name',
                '%' + name + '%')

        items = cursor_to_json(cursor)

    r['count'] = len(items)
    r['items'] = items

    return r


def fulltext_search_in_index(db, search):
    """
    Searching in index for name and layer
    :param search:
    :param db:
    :return:
    """

    r = {}
    items = []

    name = search.get('q')
    layers = search.get('layers')

    if name is not None and name != '':
        cursor = db.cursor()

        if layers is not None:
            params = [name, name]
            params += layers.strip(',').split(',')
            format_strings = ','.join(['%s'] * (len(params) - 2))

            sql = 'SELECT ' \
                  'BIN_TO_UUID(gis_index.guid) AS guid,' \
                  'gis_index.name as name,' \
                  'ST_X(gis_index.coordinates) AS lng,' \
                  'ST_Y(gis_index.coordinates) AS lat,' \
                  'gis_layer_id AS layer,' \
                  'gis_layer.alias as layer_alias,' \
                  'MATCH (gis_index.name) AGAINST ( %s IN BOOLEAN MODE) AS score ' \
                  'FROM ' \
                  'gis_index ' \
                  'LEFT JOIN gis_layer ON gis_index.gis_layer_id = gis_layer.id ' \
                  'WHERE ' \
                  'MATCH (gis_index.name) AGAINST (%s IN BOOLEAN MODE) ' \
                  'AND gis_layer_id IN (' + format_strings + ')'

            cursor.execute(sql, params)
        else:
            sql = 'SELECT ' \
                  'BIN_TO_UUID(gis_index.guid) AS guid,' \
                  'gis_index.name as name,' \
                  'ST_X(gis_index.coordinates) AS lng,' \
                  'ST_Y(gis_index.coordinates) AS lat,' \
                  'gis_layer_id AS layer,' \
                  'gis_layer.alias as layer_alias,' \
                  'MATCH (gis_index.name) AGAINST ( %s IN BOOLEAN MODE) AS score ' \
                  'FROM ' \
                  'gis_index ' \
                  'LEFT JOIN gis_layer ON gis_index.gis_layer_id = gis_layer.id ' \
                  'WHERE ' \
                  'MATCH (gis_index.name) AGAINST (%s IN BOOLEAN MODE)'

            cursor.execute(sql, (name, name))

        items = cursor_to_json(cursor)

    r['count'] = len(items)
    r['items'] = items

    return r


def get_object_by_guid(db, guid):
    """
    Return object from gis_index by its guid
    :param db:
    :param guid:
    :return:
    """
    if not is_valid_uuid(guid):
        return {'status': 'Error', 'message': 'Object guid {} is not acceptable'.format(guid)}

    sql = 'select BIN_TO_UUID(guid) as guid, name, ST_X(coordinates) as lng, ST_Y(coordinates) as lat, ' \
          'gis_layer_id as layer from gis_index where guid = UUID_TO_BIN(%s)'

    cursor = db.cursor()
    cursor.execute(sql, guid)

    item = cursor_to_json(cursor)

    if item is None or len(item) == 0:
        return {}

    return item[0]
