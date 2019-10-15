"""
    omsserver notify_journal.py
    :copyright: (c) 2019 by Pavel Konovalov
    pasha@nettlecom.com
    Created on 2019-05-13
"""

from helper_database import cursor_to_json


def get_list(db, deep_days=2):
    """
    Return list of notifications for deep_days
    :param deep_days:
    :param db:
    :return:
    """
    sql = 'SELECT BIN_TO_UUID(guid) as id, ' \
          'unix_timestamp(time_stamp) as timestamp,' \
          'notify, ' \
          'state, ' \
          'equipment_name, ' \
          'BIN_TO_UUID(equipment_guid) as equipment_guid, ' \
          'object_name, ' \
          'BIN_TO_UUID(object_guid) as object_guid ' \
          'FROM notify_journal ' \
          'WHERE date(time_stamp)  >=  curdate() - interval {} day ' \
          'ORDER BY time_stamp DESC'

    cursor = db.cursor()

    cursor.execute(sql.format(deep_days))

    return cursor_to_json(cursor)


def insert(db, guid, equipment_guid, equipment_name, notify, object_guid, object_name, state, timestamp):
    sql = "insert into notify_journal " \
          "(guid, equipment_guid, equipment_name, notify, object_guid, object_name, state, time_stamp) " \
          "values (UUID_TO_BIN(%s),UUID_TO_BIN(%s),%s,%s,UUID_TO_BIN(%s),%s,%s,FROM_UNIXTIME(%s))"

    try:
        cursor = db.cursor()

        cursor.execute(sql, (guid, equipment_guid, equipment_name, notify, object_guid, object_name, state, timestamp))
        db.commit()

    except Exception:
        return
