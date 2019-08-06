"""
    omsserver car_journal.py
    :copyright: (c) 2019 by Pavel Konovalov
    pasha@nettlecom.com
    Created on 2019-05-28
"""

import json

from helper_database import cursor_to_json


def get_list(db):
    """
    Return list of notifications for deep_days
    :param db:
    :return:
    """
    course = ['С', 'СВ', 'СВ', 'В', 'В', 'ЮВ', 'ЮВ', 'Ю', 'Ю', 'ЮЗ', 'ЮЗ', 'З', 'З', 'СЗ', 'СЗ', 'С']
    car_cache = {}

    sql = 'SELECT wialon_id, BIN_TO_UUID(guid) as guid, car_id FROM car where enabled = 1'
    cursor = db.cursor()

    cursor.execute(sql)

    items = cursor_to_json(cursor)

    for item in items:
        car_cache[int(item['wialon_id'])] = {'guid': item['guid'], 'car_id': item['car_id']}

    sql = 'SELECT ' \
          'id, ' \
          'unix_timestamp(time_stamp) as timestamp, ' \
          'locations ' \
          'FROM car_journal ' \
          'WHERE id = (select max(id) from car_journal)'

    cursor = db.cursor()

    cursor.execute(sql)

    record = cursor_to_json(cursor)

    locations_items_str = record[0].get('locations')

    car_locations = []

    if locations_items_str:
        locations_items_json = json.loads(locations_items_str)
        if locations_items_json:
            locations = locations_items_json.get('items')

            for location in locations:
                id_car = location.get('id')
                if id_car:
                    car_from_cache = car_cache.get(id_car)
                    if car_from_cache:
                        guid = car_from_cache.get('guid')
                        car_id = car_from_cache.get('car_id')
                        pos = location.get('pos')
                        if pos:
                            item = {'guid': guid, 'car_id': car_id, 'lat': pos['y'], 'lng': pos['x'], 'vlct': pos['s'],
                                    'crc': pos['c'], 'timestamp': pos['t'], 'crcs': course[int(pos['c']/22.5)]}
                            car_locations.append(item)
            sorted(car_locations, key=lambda i: i['car_id'])

    return car_locations
