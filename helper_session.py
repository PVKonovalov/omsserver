"""
    pathfinder helper_session.py
    :copyright: (c) 2018 by Pavel Konovalov
    pasha@ema.ru
    Created on 02.07.2018
"""


def is_login(db, session_key):
    """
    Wrapper for is_active function with 'None' checking
    :param db:
    :param session_key:
    :return: Boolean
    """
    if session_key is None:
        return False

    return is_active(db, session_key)


def add(db, session_key, user_id, remote_addr):
    """
    Store session key as active user session
    :param db:
    :param session_key:
    :param user_id:
    :param remote_addr:
    """

    sql = 'insert into user_session (session_key,user_id,remote_addr) values(%s,%s,%s)'
    cursor = db.cursor()
    cursor.execute(sql, (session_key, user_id, remote_addr))
    db.commit()


def is_active(db, session_key):
    """
    Checking that selected session key is active
    :param db:
    :param session_key:
    :return: Boolean
    """

    sql = 'select count(*) from user_session where session_key = %s'
    cursor = db.cursor()
    cursor.execute(sql, session_key)
    if cursor.fetchone()[0] > 0:
        return True
    else:
        return False


def get_user_id_by_session(db, session_key):
    """
    Get the identifier of the user by its session key
    :param db:
    :param session_key:
    :return:
    """

    sql = 'select user_id from user_session where session_key = %s limit 1'
    cursor = db.cursor()
    cursor.execute(sql, session_key)
    result = cursor.fetchone()

    if result is not None:
        return result[0]
    else:
        return None


def delete(db, session_key):
    """
    Delete selected session key
    :param db:
    :param session_key:
    """

    sql = 'delete from user_session where session_key = %s'
    cursor = db.cursor()
    cursor.execute(sql, session_key)
    db.commit()
