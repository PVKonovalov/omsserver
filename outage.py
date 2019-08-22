"""
    omsserver outage.py
    :copyright: (c) 2019 by Pavel Konovalov
    pasha@nettlecom.com
    Created on 2019-06-13
"""

import customer
from helper_database import cursor_to_json


def get_list(db):
    """
    Return list of current map markers
    :param db:
    :return:
    """
    sql = 'SELECT BIN_TO_UUID(car.guid) as guid, ' \
          'car.name AS name, ' \
          'car_type.icon AS icon, ' \
          'car_type.name AS type_name,' \
          'car.car_id AS car_id, ' \
          'car.wialon_id AS wialon_id, ' \
          'car.phone as phone ' \
          'FROM car ' \
          'LEFT JOIN car_type ON car.car_type_id = car_type.id ' \
          'WHERE car.enabled = 1 order by car.car_id'

    cursor = db.cursor()

    cursor.execute(sql)

    items = cursor_to_json(cursor)

    for item in items:
        if 'icon' in item:
            item['icon'] = '/static/cars/' + item['icon']

    return items


def get_customer_outage_journal(db, limit):

    sql = 'SELECT id,' \
          'UNIX_TIMESTAMP(time_stamp) AS timestamp,' \
          'customers,' \
          'shortage,' \
          'off_line_category1,' \
          'off_line_category2,' \
          'off_line_category3,' \
          'localities,' \
          'socials,' \
          'off_line_localities,' \
          'off_line_socials ' \
          'FROM customer_outage_journal ' \
          'order by id desc limit %s'

    cursor = db.cursor()

    cursor.execute(sql, limit)

    records = cursor_to_json(cursor)

    for item in records:
        item['off_line_customers'] = item.get('off_line_category1', 0) + item.get('off_line_category2', 0) + item.get(
            'off_line_category3', 0)

    if len(records) == 1:
        return records[0]
    elif len(records) > 1:
        return records
    else:
        return []

def calculate_for_customer_outage_journal(db):
    customers = customer.get_list(db)

    customers_num = 0
    shortage = 0
    off_line_category1 = 0
    off_line_category2 = 0
    off_line_category3 = 0
    socials = 0
    off_line_socials = 0

    localities_gist = {}
    off_line_localities_gist = {}

    for item in customers:
        locality_id = item.get('locality_id')
        amount = item.get('amount', 1)

        if locality_id and locality_id > 0:
            localities_gist[locality_id] = 1

        is_social = item.get('is_social')
        if is_social and is_social == 1:
            socials += amount

        usage_points = item.get('usage_point')
        if usage_points:
            for usage_point in usage_points:
                customers_num += amount
                state = usage_point.get('state')
                if state is not None and state != 'UnderVoltage':
                    shortage_item = usage_point.get('power')

                    if is_social and is_social == 1:
                        off_line_socials += amount

                    if shortage_item and shortage_item > 0:
                        shortage += shortage_item

                    if locality_id and locality_id > 0:
                        off_line_localities_gist[locality_id] = 1

                    category_item = usage_point.get('category_id')
                    if category_item:
                        if category_item == 1:
                            off_line_category1 += amount
                        elif category_item == 2:
                            off_line_category2 += amount
                        elif category_item == 3:
                            off_line_category3 += amount

    localities = len(localities_gist)
    off_line_localities = len(off_line_localities_gist)

    sql = 'INSERT INTO customer_outage_journal ' \
          '(customers, shortage, off_line_category1, off_line_category2, off_line_category3, localities, socials, ' \
          'off_line_localities, off_line_socials) ' \
          'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)'

    cursor = db.cursor()

    try:
        cursor.execute(sql, (
            customers_num, shortage, off_line_category1, off_line_category2, off_line_category3, localities, socials,
            off_line_localities, off_line_socials))
        db.commit()

        return cursor.lastrowid

    except Exception as e:
        return None
