"""
    omsserver gis_marker.py
    :copyright: (c) 2019 by Pavel Konovalov
    pasha@nettlecom.com
    Created on 2019-05-15
"""

from helper_database import cursor_to_json
import uuid
import time
import requests


def get_icon(db, type_id):
    icon = None

    if type_id:
        sql = 'select icon from gis_marker_type where id = %s'
        cursor = db.cursor()
        cursor.execute(sql, type_id)
        icon = cursor.fetchone()
        if icon:
            icon = icon[0]

    return icon


def get_list(db):
    """
    Return list of current map markers
    :param db:
    :return:
    """
    sql = 'SELECT BIN_TO_UUID(gis_marker.guid) as guid, ' \
          'gis_marker.name AS name, ' \
          'BIN_TO_UUID(gis_marker.guid_object) as guid_object, ' \
          'ST_X(gis_marker.coordinates) AS lng, ' \
          'ST_Y(gis_marker.coordinates) AS lat, ' \
          'UNIX_TIMESTAMP(gis_marker.time_stamp) AS timestamp,' \
          'gis_marker_type.icon AS icon ' \
          'FROM gis_marker ' \
          'LEFT JOIN gis_marker_type ON gis_marker.marker_type_id = gis_marker_type.id'

    cursor = db.cursor()

    cursor.execute(sql)

    items = cursor_to_json(cursor)

    for item in items:
        if 'icon' in item:
            item['icon'] = '/static/marker/' + item['icon']

    return items


def insert(db, name, type_id, guid_object):
    """
    Создает запись о маркере в таблице маркеров
    :param db:
    :param name:
    :param type_id:
    :param guid:
    :param guid_object:
    :return:
    """

    guid = None
    icon = get_icon(db, type_id)

    if guid_object and icon:
        sql = 'insert into gis_marker ' \
              '(guid,name,marker_type_id,guid_object) ' \
              'values(UUID_TO_BIN(%s),%s,%s,UUID_TO_BIN(%s))'

        guid = str(uuid.uuid4())
        cursor = db.cursor()
        cursor.execute(sql, (guid, name[:254], type_id, guid_object))
        db.commit()

        marker = [{
            "guid": guid,
            "icon": "/static/marker/{}".format(icon),
            "guid_object": guid_object,
            "name": name[0:254],
            "timestamp": int(time.time())
        }]

        try:
            requests.post('http://127.0.0.1/rsdu/oms/api/omsgw/marker/set/', json=marker)

        except requests.exceptions.RequestException:
            return guid

    return guid
