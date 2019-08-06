"""
    omsserver customer.py
    :copyright: (c) 2019 by Pavel Konovalov
    pasha@nettlecom.com
    Created on 2019-06-07
"""

import omsgw
from helper_database import cursor_to_json


def get_list(db, customer_id=None):
    """
    Return list of current map markers
    :param db:
    :return:
    """

    cursor = db.cursor()

    if customer_id and customer_id > 0:
        sql = 'SELECT customer.id as id, ' \
              'BIN_TO_UUID(customer.guid) as guid, ' \
              'customer.name as name, ' \
              'customer.amount as amount, ' \
              'customer.email as email, ' \
              'customer.address as address, ' \
              'customer.account as account,' \
              'customer.phone as phone, ' \
              'customer.is_social as is_social, ' \
              'locality.name as locality, ' \
              'locality.id as locality_id, ' \
              'locality.zip as locality_zip, ' \
              'locality.type as locality_type,  ' \
              'street.name as street, ' \
              'street.id as street_id, ' \
              'street.type as street_type,  ' \
              'street.zip as street_zip ' \
              'FROM customer ' \
              'LEFT JOIN locality ON customer.locality_id = locality.id ' \
              'LEFT JOIN street ON customer.street_id = street.id ' \
              'where customer.id = %s ' \
              'ORDER BY customer.name'

        cursor.execute(sql, customer_id)
    else:
        sql = 'SELECT customer.id as id, ' \
              'BIN_TO_UUID(customer.guid) as guid, ' \
              'customer.name as name, ' \
              'customer.amount as amount, ' \
              'customer.email as email, ' \
              'customer.address as address, ' \
              'customer.account as account,' \
              'customer.phone as phone, ' \
              'customer.is_social as is_social, ' \
              'locality.name as locality, ' \
              'locality.id as locality_id, ' \
              'locality.zip as locality_zip, ' \
              'locality.type as locality_type,  ' \
              'street.name as street, ' \
              'street.id as street_id, ' \
              'street.type as street_type,  ' \
              'street.zip as street_zip ' \
              'FROM customer ' \
              'LEFT JOIN locality ON customer.locality_id = locality.id ' \
              'LEFT JOIN street ON customer.street_id = street.id ' \
              'ORDER BY customer.name'

        cursor.execute(sql)

    items = cursor_to_json(cursor)

    for item in items:

        sql_usage_point = 'SELECT usage_point.id as id, ' \
                          'BIN_TO_UUID(usage_point.guid) as guid, ' \
                          'usage_point.is_important as is_important, ' \
                          'usage_point.power as power, ' \
                          'usage_point.pillar_num as pillar_num, ' \
                          'usage_point.meter_num as meter_num, ' \
                          'usage_point.act_num as act_num, ' \
                          'meter_type.name as meter_type, ' \
                          'customer_class.name as class, ' \
                          'customer_category.name as category, ' \
                          'customer_category.id as category_id, ' \
                          'usage_point.cnode_id as cnode_id ' \
                          'FROM usage_point ' \
                          'LEFT JOIN customer_class ON usage_point.customer_class_id = customer_class.id ' \
                          'LEFT JOIN customer_category ON usage_point.customer_category_id = customer_category.id ' \
                          'LEFT JOIN meter_type ON usage_point.meter_type_id = meter_type.id ' \
                          'WHERE customer_id = %s'

        cursor.execute(sql_usage_point, item['id'])
        items_usage_point = cursor_to_json(cursor)

        for item_usage_point in items_usage_point:

            usage_point_state = omsgw.network_objects.get(item_usage_point['guid'])

            if usage_point_state:
                item_usage_point['state'] = usage_point_state

            cnode_id = item_usage_point.get('cnode_id')

            if cnode_id:
                sql_cnode = 'SELECT cnode.id as id, ' \
                            'BIN_TO_UUID(cnode.guid) as guid, ' \
                            'cnode.name as name, ' \
                            'cnode.name_equipment as equipment, ' \
                            'cnode.equipment_id as equipment_id ' \
                            'FROM cnode ' \
                            'WHERE id = %s'

                cursor.execute(sql_cnode, cnode_id)
                item_cnode = cursor_to_json(cursor)[0]
                item_usage_point['cnode'] = item_cnode
                del item_usage_point['cnode_id']

        item['usage_point'] = items_usage_point

    return items


def get(db,  customer_id=None):
    """
    Возвращает описание потребителя по его идентификатору
    :param db:
    :param customer_id:
    :return:
    """
    if customer_id and customer_id > 0:
        customer_item = get_list(db, customer_id)
        if len(customer_item) > 0:
            return customer_item[0]
    return None

def search_in_locality(db, search_json):
    """
    Поиск по населенным пунктам
    :param search_json:
    :param db:
    :return:
    """
    r = {}
    items = []

    locality = search_json.get('q')

    if locality is not None and locality != '':
        cursor = db.cursor()
        cursor.execute(
            'select id, name, zip, type from locality where upper(name) like upper(%s) order by name',
            locality + '%')

        items = cursor_to_json(cursor)

    r['count'] = len(items)
    r['items'] = items

    return r


def search_in_street(db, search_json):
    """
    Поиск по улицам
    :param db:
    :param search_json:
    :return:
    """
    r = {}
    items = []

    street = search_json.get('q')
    locality_id = search_json.get('l')

    if street is not None and street != '':
        cursor = db.cursor()
        if locality_id is not None and locality_id > 0:
            cursor.execute(
                'SELECT street.id as id, '
                'street.name as name, '
                'street.zip as zip, '
                'street.type as type, '
                'locality_id, '
                'locality.name as locality,'
                'locality.type as locality_type '
                'FROM street '
                'LEFT JOIN locality ON street.locality_id = locality.id '
                'WHERE locality_id = %s and upper(street.name) like upper(%s) '
                'ORDER BY street.name',
                (locality_id, street + '%'))
        else:
            cursor.execute(
                'SELECT street.id as id, '
                'street.name as name, '
                'street.zip as zip, '
                'street.type as type, '
                'locality_id, '
                'locality.name as locality,'
                'locality.type as locality_type '
                'FROM street '
                'LEFT JOIN locality ON street.locality_id = locality.id '
                'WHERE upper(street.name) like upper(%s) '
                'ORDER BY street.name',
                street + '%')

        items = cursor_to_json(cursor)

    r['count'] = len(items)
    r['items'] = items

    return r


def search(db, search_json):
    """
    Поиск по потребителям
    :param db:
    :param search_json:
    :return:
    """
    r = {}
    items = []

    customer = search_json.get('q')

    if customer is not None and customer != '':
        cursor = db.cursor()
        cursor.execute('SELECT customer.id as id, '
                       'BIN_TO_UUID(customer.guid) as guid, '
                       'customer.name as name, '
                       'customer.email as email, '
                       'customer.address as address, '
                       'customer.account as account,'
                       'customer.phone as phone, '
                       'customer.is_social as is_social, '
                       'locality.name as locality, '
                       'locality.zip as locality_zip, '
                       'locality.type as locality_type, '
                       'street.name as street, '
                       'street.type as street_type, '
                       'street.zip as street_zip '
                       'FROM customer '
                       'LEFT JOIN locality ON customer.locality_id = locality.id '
                       'LEFT JOIN street ON customer.street_id = street.id '
                       'WHERE upper(customer.name) like upper(%s) '
                       'ORDER by customer.name',
                       customer + '%')

        items = cursor_to_json(cursor)

    r['count'] = len(items)
    r['items'] = items

    return r
