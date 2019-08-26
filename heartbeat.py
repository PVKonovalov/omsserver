"""
    omsserver heartbeat.py
    :copyright: (c) 2019 by Pavel Konovalov
    pasha@nettlecom.com
    Created on 26/08/2019
"""
import time
from helper_database import cursor_to_json
from user import get_item as get_user_by_session_key


def status(db, session_key):
    """
    Возвращает статус сервера, валидность ключа авторизации, метки времени создания слоев
    :param db:
    :param session_key:
    :return:
    """
    server_timezone = time.timezone / 3600.0

    session_key_status = 'invalid'

    cursor = db.cursor()

    sql = 'select id, time_stamp as ts from gis_layer where enabled_for_mobile = 1'
    cursor.execute(sql)
    layers = cursor_to_json(cursor)

    server_status = {'status': 'ok',
                     'timezone': server_timezone,
                     'time': time.ctime(),
                     'layers': layers,
                     'session_key': session_key_status
                     }

    if session_key is not None:
        server_status['session_key'] = 'valid'
        server_status['user'] = get_user_by_session_key(db, session_key, None)

    return server_status
