"""
    pathfinder user_path.py
    :copyright: (c) 2018 by Pavel Konovalov
    pasha@nettlecom.com
    Created on 03.07.2018
"""

from datetime import datetime

from helper_database import cursor_to_json
import helper_session as session


def add_point(db, session_key, lat, lng, time_stamp):
    user_id = session.get_user_id_by_session(db, session_key)

    if lat is None or type(lat) is not float:
        return {'status': 'Error', 'message': 'Latitude is not acceptable'}

    if lng is None or type(lng) is not float:
        return {'status': 'Error', 'message': 'Longitude is not acceptable'}

    if time_stamp is None:
        time_stamp = datetime.now()

    sql_insert = 'insert into user_path (user_id,lat,lng,time_stamp) values({},{},{},"{}")'. \
        format(user_id, lat, lng, time_stamp)
    cursor = db.cursor()
    try:
        cursor.execute(sql_insert)
        db.commit()
        return {'status': 'Ok'}
    except:
        return {'status': 'Error', 'message': 'Error while insert. Check your parameters.'}


def get_last_point(db, user_id):
    if user_id is None or user_id <= 0:
        return {'status': 'Error', 'message': 'User id is not acceptable'}

    sql = 'select user_id, lat,lng,time_stamp from user_path where user_id = {} order by id desc limit 1'.format(
        user_id)

    cursor = db.cursor()
    cursor.execute(sql)

    item = cursor_to_json(cursor)

    if item is None or len(item) == 0:
        return {'status': 'Error', 'message': 'User not found'}

    return item[0]


def get_last_points(db):
    sql = 'select user_path.id as id, ' \
          'user_path.time_stamp as time_stamp, ' \
          'user.name as user, ' \
          'user_path.user_id as user_id, ' \
          'user_path.lat as lat, ' \
          'user_path.lng as lng ' \
          'from user_path ' \
          'left join user on user_path.user_id = user.id ' \
          'where user_path.id in (select max(id) from user_path group by user_id)'

    cursor = db.cursor()
    cursor.execute(sql)

    return cursor_to_json(cursor)
