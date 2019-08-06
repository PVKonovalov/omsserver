"""
    pathfinder message.py
    :copyright: (c) 2018 by Pavel Konovalov
    pasha@nettlecom.com
    Created on 05.07.2018
"""

import uuid
from datetime import datetime

from helper_database import array_to_where_in
from helper_database import cursor_to_json
from helper_json import item_from_json
from helper_rabbit import send_via_rabbit
from helper_session import get_user_id_by_session
from helper_uuid import is_valid_uuid
from user import user


def get_last_messages_for_user(db, user_id=None):
    """
    Return last messages for selected user
    :param db:
    :param user_id:
    :return:
    """
    r = {}
    if user_id is not None:
        sql = 'select max(idm) id, user_id from (( select  max(id) idm, user_from_id user_id  from message  ' \
              'where user_to_id = %s group by user_from_id) ' \
              'union (select  max(id) idm, user_to_id user_id  from message  ' \
              'where user_from_id = %s group by user_to_id)) as msg ' \
              'group by user_id order by id'

        cursor = db.cursor()
        cursor.execute(sql, (user_id, user_id))
        items = cursor_to_json(cursor)
        r['items'] = []
        for item in items:
            r['items'].append(get_item_by_id(db, item['id']))

        r['count'] = len(items)

    return r


def get_last_messages(db, session_key):
    """
    Return last messages for session user
    :param db:
    :param session_key:
    :return:
    """
    user_owner_id = get_user_id_by_session(db, session_key)

    if user_owner_id is None or user_owner_id <= 0:
        return {'status': 'Error', 'message': 'User id has not found in the session'}

    return get_last_messages_for_user(db, user_owner_id)


def get_list(db, order='none', limit=None, page=None, user_from=None, user_to=None, has_received=None):
    """
    Return list of messages
    :param db:
    :param order:
    :param limit:
    :param page:
    :param user_from:
    :param user_to:
    :param has_received:
    :return:
    """
    sort_parameters = ['time-stamp-desc', 'time-stamp-asc', 'none']

    if order not in sort_parameters:
        return {'status': 'Error', 'message': 'Sort parameter must in {}'.format(', '.join(sort_parameters))}

    if limit is not None and page is not None:
        if limit <= 0:
            return {'status': 'Error', 'message': 'Limit is not acceptable'}

        if page <= 0:
            return {'status': 'Error', 'message': 'Page is not acceptable'}

    if user_from is not None and user_from <= 0:
        return {'status': 'Error', 'message': 'Sender is not acceptable'}

    if user_to is not None and user_to <= 0:
        return {'status': 'Error', 'message': 'Originator is not acceptable'}

    if has_received is not None and has_received not in [0, 1]:
        return {'status': 'Error', 'message': 'Has received flag is not acceptable'}

    sql_where = ''

    if user_from is not None or user_to is not None or has_received is not None:

        sql_where = ' where '
        condition = ''

        if user_from is not None:
            sql_where += array_to_where_in('message', 'user_from_id', user_from, condition)
            condition = 'and'

        if user_to is not None:
            sql_where += array_to_where_in('message', 'user_to_id', user_to, condition)
            condition = 'and'

        if has_received is not None:
            sql_where += array_to_where_in('message', 'has_received', has_received, condition)

    sql_count = 'select count(*) as count from message {}'.format(sql_where)
    cursor = db.cursor()
    cursor.execute(sql_count)

    count = cursor.fetchone()[0]

    sql_limit = ''
    r = {}

    if limit is not None and page is not None:
        sql_limit = ' limit {} offset {}'.format(limit, (page - 1) * limit)

    sql_base = 'select BIN_TO_UUID(message.guid) AS guid, message.name, has_received, time_stamp, user_from_id, ' \
               'user_from.name as user_from, user_to.name as user_to, user_to_id ' \
               'from message ' \
               'left join user as user_from on message.user_from_id = user_from.id ' \
               'left join user as user_to on message.user_to_id = user_to.id '

    sql_order = ''

    if order == 'time-stamp-desc':
        sql_order = ' order by message.time_stamp desc'
    elif order == 'time-stamp-asc':
        sql_order = ' order by message.time_stamp asc'

    sql = '{}{}{}{}'.format(sql_base, sql_where, sql_order, sql_limit)
    cursor.execute(sql)

    items = cursor_to_json(cursor)

    r['count'] = count
    r['items'] = items

    return r


def get_conversation(db, session_key, with_user=None, order=None, limit=None, page=None):
    """
    Return conversation with selected user
    :param order:
    :param db:
    :param session_key:
    :param with_user:
    :param limit:
    :param page:
    :return:
    """
    if limit is not None and page is not None:
        if limit <= 0:
            return {'status': 'Error', 'message': 'Limit is not acceptable'}

        if page <= 0:
            return {'status': 'Error', 'message': 'Page is not acceptable'}

    if with_user is None or with_user <= 0:
        return {'status': 'Error', 'message': 'User is not acceptable'}

    user_owner_id = get_user_id_by_session(db, session_key)

    if user_owner_id is None or with_user <= 0:
        return {'status': 'Error', 'message': 'User id has not found in the session'}

    sql_base = 'select id from (select id from message where user_from_id = %s and user_to_id = %s union ' \
               'select id from message where user_from_id = %s and user_to_id = %s) as msg'

    sql_order = ''
    if order == 'time-stamp-desc':
        sql_order = ' order by id desc'
    elif order == 'time-stamp-asc':
        sql_order = ' order by id asc'

    sql_limit = ''
    r = {}

    if limit is not None and page is not None:
        sql_limit = ' limit {} offset {}'.format(limit, (page - 1) * limit)

    sql = '{}{}{}'.format(sql_base, sql_order, sql_limit)

    cursor = db.cursor()
    cursor.execute(sql, (user_owner_id, with_user, with_user, user_owner_id))

    items = cursor_to_json(cursor)
    r['items'] = []
    for item in items:
        r['items'].append(get_item_by_id(db, item['id']))

    r['count'] = len(r['items'])

    return r


def get_item_by_id(db, message_id):
    """
    Finding message item associated with selected id
    :param db:
    :param message_id:
    :return:
    """
    if message_id is None or message_id <= 0:
        return {'status': 'Error', 'message': 'Message id is not acceptable'}

    sql_base = 'select BIN_TO_UUID(message.guid) as guid, message.name, has_received, time_stamp, user_from_id, ' \
               'user_from.name as user_from, user_to.name as user_to, user_to_id ' \
               'from message ' \
               'left join user as user_from on message.user_from_id = user_from.id ' \
               'left join user as user_to on message.user_to_id = user_to.id '

    sql_where = ' where message.id = %s'

    sql = '{}{}'.format(sql_base, sql_where)

    cursor = db.cursor()
    cursor.execute(sql, message_id)

    item = cursor_to_json(cursor)

    if item is None or len(item) == 0:
        return {}

    return item[0]


def get_item_by_guid(db, message_guid):
    """
    Finding message item associated with selected guid
    :param db:
    :param message_guid:
    :return:
    """
    if not is_valid_uuid(message_guid):
        return {'status': 'Error', 'message': 'Message guid is not acceptable'}

    sql_base = 'select BIN_TO_UUID(message.guid) as guid, message.name, has_received, time_stamp, user_from_id, ' \
               'user_from.name as user_from, user_to.name as user_to, user_to_id ' \
               'from message ' \
               'left join user as user_from on message.user_from_id = user_from.id ' \
               'left join user as user_to on message.user_to_id = user_to.id '

    sql_where = ' where message.guid = UUID_TO_BIN(%s)'

    sql = '{}{}'.format(sql_base, sql_where)

    cursor = db.cursor()
    cursor.execute(sql, message_guid)

    item = cursor_to_json(cursor)

    if item is None or len(item) == 0:
        return {}

    return item[0]


def send(db, user_to_id, session_key, message_json):
    """
    Insert message item
    :param db:
    :param user_to_id:
    :param session_key:
    :param message_json:
    :return:
    """
    if user_to_id is None or user_to_id <= 0:
        return {'status': 'Error', 'message': 'Originator id is not acceptable'}

    message_str = item_from_json(message_json, 'message')

    if message_str is None or message_str == '':
        return {'status': 'Error', 'message': 'Message is empty'}

    user_from_id = get_user_id_by_session(db, session_key)
    if user_from_id is None or user_from_id <= 0:
        return {'status': 'Error', 'message': 'Sender id is not acceptable'}

    time_stamp = datetime.now()
    message_guid = str(uuid.uuid4())

    sql_insert = 'insert into message (guid, user_from_id,user_to_id,time_stamp,name) ' \
                 'values(UUID_TO_BIN(%s),%s,%s,%s,%s)'
    cursor = db.cursor()
    try:
        cursor.execute(sql_insert, (message_guid, user_from_id, user_to_id, time_stamp, message_str))
        db.commit()

        send_via_rabbit(user_from_id, user(db, user_from_id), user_to_id, message_guid, message_str)

        return {'status': 'Ok', 'message_guid': message_guid}

    except Exception:
        return {'status': 'Error', 'message': 'Error while sending the message. Check your parameters.'}


def set_has_received(db, message_guid):
    """
    Set message received status as True
    :param db:
    :param message_guid:
    :return:
    """
    if message_guid is None or message_guid <= 0:
        return {'status': 'Error', 'message': 'Message id is not acceptable'}

    sql_update = 'update message set has_received = 1 where guid = UUID_TO_BIN(%s)'
    cursor = db.cursor()
    try:
        cursor.execute(sql_update, message_guid)
        db.commit()
        return {'status': 'Ok'}

    except Exception:
        return {'status': 'Error',
                'message': 'Error while update the message {}. Check your parameters.'.format(message_guid)}


def delete_item(db, message_guid):
    """
    Delete message item selected by guid
    :param db:
    :param message_guid:
    :return:
    """
    if message_guid is None or message_guid <= 0:
        return {'status': 'Error', 'message': 'Message id is not acceptable'}

    sql_update = 'delete from message where guid = UUID_TO_BIN(%s)'
    cursor = db.cursor()
    try:
        cursor.execute(sql_update, message_guid)
        db.commit()
        return {'status': 'Ok'}
    except Exception:
        return {'status': 'Error',
                'message': 'Error while delete the message {}. Check your parameters.'.format(message_guid)}
