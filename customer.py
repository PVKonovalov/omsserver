"""
    omsserver customer.py
    :copyright: (c) 2019 by Pavel Konovalov
    pasha@nettlecom.com
    Created on 2019-06-07
"""

from helper_database import cursor_to_json
import omsgw
import equipment


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


def get_with_guid(db, guid=None):
    """
    Возвращает описание пользователя по его guid
    :param db:
    :param guid:
    :return: Потребитель или None
    """
    if guid:
        sql = 'select id from customer where guid =  UUID_TO_BIN(%s)'
        cursor = db.cursor()
        cursor.execute(sql, guid)
        customer_id = cursor.fetchone()
        if customer_id:
            customer_id = customer_id[0]
            return get(db, customer_id=customer_id)
    return None


def get_guid(db, customer_id=None):
    """
    Возвражает guid потребителя по его идентификатору
    :param db:
    :param customer_id:
    :return: guid или None
    """
    guid = None

    if customer_id:
        sql = 'select BIN_TO_UUID(guid) from customer where id = %s'
        cursor = db.cursor()
        cursor.execute(sql, customer_id)
        guid = cursor.fetchone()
        if guid:
            guid = guid[0]

    return guid


def get(db, customer_id=None):
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


def search_in_complex_address(db, search_json):
    """
    Поиск по улицам
    :param db:
    :param search_json:
    :return:
    """
    r = {}
    items = []

    locality_id = None
    street_id = None
    items_locality = {}
    items_street = {}
    items_customer = {}

    address = search_json.get('q')
    address = address.split(' ')

    if len(address) > 0:
        sql = 'select id as locality_id, name as locality, type as locality_type  from locality where upper(name) like upper(%s)'

        cursor = db.cursor()
        cursor.execute(sql, '%' + address[0] + '%')
        items_locality = cursor_to_json(cursor)

        if len(items_locality) > 0:
            locality_id = items_locality[0].get('locality_id')
            items = items_locality

    if locality_id and len(address) > 1:
        sql = 'select id, name from street where locality_id = %s and upper(name) like upper(%s)'

        cursor = db.cursor()
        cursor.execute(sql, (locality_id, '%' + address[1] + '%'))
        items_street = cursor_to_json(cursor)

        if len(items_street) > 0:
            street_id = items_street[0].get('id')
            items = items_street

    if street_id and len(address) > 2:
        sql = 'select street.id as id,' \
              'street.name as name, ' \
              'street.zip as zip, ' \
              'street.type as type, ' \
              'customer.locality_id as locality_id, ' \
              'locality.name as locality,' \
              'locality.type as locality_type ' \
              'from customer ' \
              'LEFT JOIN locality ON customer.locality_id = locality.id ' \
              'LEFT JOIN street ON customer.street_id = street.id ' \
              'where customer.locality_id = %s and customer.street_id = %s and upper(customer.address) like upper(%s) ' \
              'order by customer.name'

        cursor = db.cursor()
        cursor.execute(sql, (locality_id, street_id, '%' + address[2] + '%'))
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


def get_extra_state(db, guid):
    """
    Возвращает дополнительное описание потребителя
    :param db:
    :param guid:
    :return:
       {
        "Запитан": "ПС Xxxxxx Л-3 КТП Xxxxxx Л-2 Ввод оп.XX д.XX  Л-2 0,4 кВ КТП Xxxxxxxx",
        "Категория": "3 категория",
        "Потребитель": "Xxxxxxx X.X.",
        }

    """
    extra_state = {}
    customer = get_with_guid(db, guid)
    if customer:
        name = customer.get('name')
        if name:
            extra_state['Потребитель'] = name
        usage_point = customer.get('usage_point')
        if usage_point:
            category = usage_point[0].get('category')
            if category:
                extra_state['Категория'] = category
            cnode = usage_point[0].get('cnode')
            if cnode:
                equipment_item = cnode.get('equipment')
                if equipment_item:
                    extra_state['Запитан'] = equipment_item
    return extra_state


def get_power_center(db, customer_guid):
    """
    Возвращает центр питания для заданного потребителя или None
    :param db:
    :param customer_guid:
    :return: Центр питания в виде equipment или None
    """
    power_center_list = []

    customer = get_with_guid(db, customer_guid)

    if customer:
        usage_point_list = customer.get('usage_point')
        if usage_point_list:
            for usage_point in usage_point_list:
                cnode = usage_point.get('cnode')
                if cnode:
                    equipment_id = cnode.get('equipment_id')
                    if equipment_id:
                        equipment_item = equipment.get_with_id(db, equipment_id)
                        while equipment_item and equipment_item.get('type_id') not in [20]:
                            guid_parent = equipment_item.get('guid_parent')
                            equipment_item = None
                            if guid_parent:
                                equipment_item = equipment.get_with_guid(db, guid_parent)
                        if equipment_item:
                            power_center_list.append(equipment_item)

    return power_center_list
