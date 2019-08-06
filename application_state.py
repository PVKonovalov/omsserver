"""
    pathfinder application_state.py
    :copyright: (c) 2018 by Pavel Konovalov
    pasha@nettlecom.com
    Created on 16.07.2018
"""
 

from helper_database import cursor_to_json


def get_list(db, order, limit, page):
    sort_parameters = ['name-desc', 'name-asc']
    table_name = 'application_state'

    if order not in sort_parameters:
        return {'error': 'Sort parameter must in {}'.format(', '.join(sort_parameters))}
    if limit is not None and page is not None:
        if limit <= 0:
            return {'status': 'Error', 'message': 'Limit is not acceptable'}

        if page <= 0:
            return {'status': 'Error', 'message': 'Page is not acceptable'}

    r = {}
    cursor = db.cursor()

    sql_count = 'select count(*) as count from {}'.format(table_name)
    cursor.execute(sql_count)

    count = cursor.fetchone()[0]

    sql_limit = ''

    if limit is not None and page is not None:
        sql_limit = ' limit {} offset {}'.format(limit, (page - 1) * limit)

    sql_base = 'select {}.id, {}.name, {}.color ' \
               'from {} '.format(table_name, table_name, table_name, table_name)

    sql_order = ''

    if order == 'name-desc':
        sql_order = 'order by {}.name desc'.format(table_name)
    elif order == 'name-asc':
        sql_order = 'order by {}.name asc'.format(table_name)

    sql = '{} {} {}'.format(sql_base, sql_order, sql_limit)
    cursor.execute(sql)

    items = cursor_to_json(cursor)

    r['count'] = count
    r['items'] = items

    return r


def get_item(db, id_item):
    if id_item is None or id_item <= 0:
        return {'error': 'Id is not acceptable'}

    table_name = 'application_state'

    cursor = db.cursor()
    sql_where = 'where {}.id = {}'.format(table_name, id_item)

    sql_base = 'select {}.id, {}.name, {}.color ' \
               'from {} '.format(table_name, table_name, table_name, table_name)

    sql = '{} {}'.format(sql_base, sql_where)
    cursor.execute(sql)

    item = cursor_to_json(cursor)

    if item is None or len(item) == 0:
        return {}

    return item
