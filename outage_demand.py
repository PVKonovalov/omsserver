"""
    omsserver outage_demand.py
    :copyright: (c) 2019 by Pavel Konovalov
    pasha@nettlecom.com
    Created on 2019-06-13
"""

from helper_database import cursor_to_json
from helper_json import item_from_json
import customer


def get_list(db, order='timestamp-asc', limit=50, page=1):
    """
    Возвращает список заявок(звонков) потребителей
    :param order:
    :param limit:
    :param page:
    :param db:
    :return:
    """
    sort_parameters = ['timestamp-desc', 'timestamp-asc', 'state-desc', 'state-asc']

    if order not in sort_parameters:
        return {'error': 'Sort parameter must in {}'.format(', '.join(sort_parameters))}

    if limit is not None and page is not None:
        if limit <= 0:
            return {'status': 'Error', 'message': 'Limit is not acceptable'}

        if page <= 0:
            return {'status': 'Error', 'message': 'Page is not acceptable'}

    sql_limit = ''

    if limit is not None and page is not None:
        sql_limit = ' limit {} offset {}'.format(limit, (page - 1) * limit)

    sql_base = 'SELECT outage_demand.id as id, ' \
               'unix_timestamp(outage_demand.time_stamp) as timestamp, ' \
               'outage_demand.address as demand_address, ' \
               'outage_demand.phone as demand_phone, ' \
               'outage_demand.description as description,' \
               'outage_demand.outage_demand_state_id as state_id, ' \
               'outage_demand.customer_id as customer_id ' \
               'FROM outage_demand '

    sql_order = ''

    if order == 'timestamp-desc':
        sql_order = 'order by outage_demand.time_stamp desc'
    elif order == 'timestamp-asc':
        sql_order = 'order by outage_demand.time_stamp asc'
    elif order == 'state-desc':
        sql_order = 'order by outage_demand.outage_demand_state_id desc'
    elif order == 'state-asc':
        sql_order = 'order by outage_demand.outage_demand_state_id asc'

    sql = '{} {} {}'.format(sql_base, sql_order, sql_limit)

    cursor = db.cursor()
    cursor.execute(sql)
    items = cursor_to_json(cursor)

    sql_count = 'select count(*) as count from outage_demand'
    cursor.execute(sql_count)
    count = cursor.fetchone()[0]

    for item in items:
        customer_id = item.get('customer_id')
        customer_item = customer.get(db, customer_id)

        if customer_item:
            item['customer'] = customer_item

    return {'count': count, 'items': items}


def create(db, demand_json):
    """
    Создает новую запись в списке заявок(звонков) потребителей
    :param db:
    :param demand_json:
    {
        "customer_id": 1,
        "description": "Отсутствует электричество",
        "state_id":1
    }
    """
    state_id = item_from_json(demand_json, 'state_id', 1)
    description = item_from_json(demand_json, 'description')
    customer_id = item_from_json(demand_json, 'customer_id')

    sql = 'INSERT INTO outage_demand (outage_demand_state_id, description, customer_id) ' \
          'VALUES (%s, %s, %s)'

    cursor = db.cursor()

    try:
        cursor.execute(sql, (state_id, description, customer_id))
        db.commit()

        return {'status': 'Ok', 'id': cursor.lastrowid}

    except Exception as e:
        return {'status': 'Error', 'code': 300, 'message': e}


def update(db, demand_id, demand_json):
    """
    Обновить информацию ы выбранной заявке
    :param db:
    :param demand_id:
    :param demand_json:
    :return:
    """
    if demand_id > 0:

        state_id = item_from_json(demand_json, 'state_id')
        description = item_from_json(demand_json, 'description')
        customer_id = item_from_json(demand_json, 'customer_id')

        sql = 'UPDATE outage_demand set outage_demand_state_id = %s, description = %s, customer_id = %s ' \
              'WHERE id = %s'

        cursor = db.cursor()

        try:
            cursor.execute(sql, (state_id, description, customer_id, demand_id))
            db.commit()

            return {'status': 'Ok'}

        except Exception as e:
            return {'status': 'Error', 'code': 300, 'message': e}
