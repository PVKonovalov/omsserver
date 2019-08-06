"""
    omsserver gis_marker.py
    :copyright: (c) 2019 by Pavel Konovalov
    pasha@nettlecom.com
    Created on 2019-05-15
"""

from helper_database import cursor_to_json


def get_list(db):
    """
    Return list of current map markers
    :param db:
    :return:
    """
    sql = 'SELECT BIN_TO_UUID(gis_marker.guid) as guid, ' \
          'gis_marker.name AS name, ' \
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
