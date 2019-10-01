"""
    pathfinder user.py
    :copyright: (c) 2018 by Pavel Konovalov
    pasha@nettlecom.com
    Created on 02.07.2018
"""

import uuid

import helper_session as session
from helper_database import cursor_to_json


def get_list(db, order='none', limit=None, page=None):
    """
    Return list of users
    :param db:
    :param order:
    :param limit:
    :param page:
    :return:
    """

    sort_parameters = ['name-desc', 'name-asc', 'none']

    if order not in sort_parameters:
        return {'status': 'Error', 'message': 'Sort parameter must in {}'.format(', '.join(sort_parameters))}

    if limit is not None and page is not None:
        if limit <= 0:
            return {'status': 'Error', 'message': 'Limit is not acceptable'}

        if page <= 0:
            return {'status': 'Error', 'message': 'Page is not acceptable'}

    r = {}
    cursor = db.cursor()

    sql_count = 'select count(*) as count from user'
    cursor.execute(sql_count)

    count = cursor.fetchone()[0]

    sql_limit = ''

    if limit is not None and page is not None:
        sql_limit = ' limit {} offset {}'.format(limit, (page - 1) * limit)

    sql_base = 'select user.id, ' \
               'BIN_TO_UUID(user.guid) as guid, ' \
               'user.name, ' \
               'user.phone, ' \
               'user.email, ' \
               'mobile_team_id, ' \
               'mobile_team.name as mobile_team, ' \
               'login, ' \
               'avatar ' \
               'from user ' \
               'left join mobile_team on user.mobile_team_id = mobile_team.id '

    sql_order = ''

    if order == 'name-desc':
        sql_order = 'order by user.name desc'
    elif order == 'name-asc':
        sql_order = 'order by user.name asc'

    sql = '{} {}{}'.format(sql_base, sql_order, sql_limit)
    cursor.execute(sql)

    items = cursor_to_json(cursor)

    for item in items:
        if 'avatar' in item:
            item['avatar'] = '/static/user/' + item['avatar']

    r['count'] = count
    r['items'] = items

    return r


def user(db, user_id):
    """
    Return name of selected user
    :param db:
    :param user_id:
    :return:
    """
    if user_id is None or user_id <= 0:
        return 'Unknown user name'

    cursor = db.cursor()
    cursor.execute('select name from user where id = %s', user_id)
    return cursor.fetchone()[0]


def get_item(db, session_key, user_id):
    """
    Return user information by user_id or session_key
    :param db:
    :param session_key:
    :param user_id:
    :return:
    """
    user_owner_id = session.get_user_id_by_session(db, session_key)

    if user_owner_id is None and (user_id is None or user_id <= 0):
        return {'status': 'Error', 'message': 'User id is not acceptable'}

    if user_id is None:
        user_id = user_owner_id

    cursor = db.cursor()

    sql = 'select user.id, ' \
          'BIN_TO_UUID(user.guid) as guid, ' \
          'user.name, ' \
          'user.phone, ' \
          'user.email, ' \
          'mobile_team_id, ' \
          'mobile_team.name as mobile_team, ' \
          'login, ' \
          'avatar ' \
          'from user ' \
          'left join mobile_team on user.mobile_team_id = mobile_team.id ' \
          'where user.id = %s'

    cursor.execute(sql, user_id)

    item = cursor_to_json(cursor)

    if item is None or len(item) == 0:
        return {}

    if 'avatar' in item[0]:
        item[0]['avatar'] = '/static/user/' + item[0]['avatar']

    return item[0]


def login(db, login_name, password, remote_addr):
    """
    Check user access and return session_key
    :param db:
    :param login_name:
    :param password:
    :param remote_addr:
    :return:
    """
    if login_name is None or password is None:
        return {'status': 'Error', 'message': 'Null login or password are not acceptable'}

    r = {}

    cursor = db.cursor()

    sql = 'select id from user where login = %s and password = %s'
    cursor.execute(sql, (login_name, password))

    user_id = 0

    row = cursor.fetchone()

    if row is not None:
        user_id = row[0]

    if user_id > 0:
        session_key = str(uuid.uuid4())
        session.add(db, session_key, user_id, remote_addr)
        r['session_key'] = session_key
        user_json = get_item(db, session_key, user_id)
        if user_json is not None:
            r['user'] = user_json
    else:
        r = {'status': 'Error', 'message': 'Error while login. Check login and password.'}

    return r


def logout(db, session_key):
    """
    Remove user from session list by session_ley
    :param db:
    :param session_key:
    :return:
    """
    if session_key is None or session_key == '':
        return {'status': 'Error', 'message': 'Strange session key'}
    session.delete(db, session_key)
    return {'status': 'Ok'}


def get_list_for_cars(db):
    sql = 'select user.id as wialon_id, ' \
          'BIN_TO_UUID(user.guid) as guid, ' \
          'user.phone, ' \
          'user.email, ' \
          'mobile_team.name as type_name, ' \
          'user.name as car_id, ' \
          'avatar as icon ' \
          'from user ' \
          'left join mobile_team on user.mobile_team_id = mobile_team.id ' \
          'where user.id in (select up2.user_id from ( select up.user_id, max(up.id) from user_path as up group by user_id) as up2)'

    cursor = db.cursor()
    cursor.execute(sql)

    items = cursor_to_json(cursor)

    for item in items:
        if 'icon' in item:
            item['icon'] = '/static/user/man_32.png'
            item['icon_back'] = '/static/user/' + item['icon']
            item['wialon_id'] = '{}'.format(item['wialon_id'])
            item['type'] = 'user'
    return items


def get_list_for_car_journal(db):
    sql = 'select BIN_TO_UUID(user.guid) as guid, ' \
          'UNIX_TIMESTAMP(user_path.time_stamp) as timestamp, ' \
          'user.name as car_id, ' \
          'user_path.lat as lat, ' \
          'user_path.lng as lng ' \
          'from user_path ' \
          'left join user on user_path.user_id = user.id ' \
          'where user_path.id in (select max(id) from user_path group by user_id)'

    cursor = db.cursor()
    cursor.execute(sql)
    items = cursor_to_json(cursor)

    for item in items:
        item['vlct'] = 0
        item['crc'] = 0
        item['crcs'] = 'С'

    return items
