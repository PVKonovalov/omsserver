"""
    omsserver notify.py
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
    sql = 'SELECT notify.id as id, ' \
          'unix_timestamp(notify.time_stamp) as timestamp,' \
          'notify_type.name as notify, ' \
          'notify_type.alias as state, ' \
          'equipment.name as equipment_name, ' \
          'BIN_TO_UUID(equipment.guid) as equipment_guid, ' \
          'object.name as object_name, ' \
          'BIN_TO_UUID(object.guid) as object_guid ' \
          'FROM notify ' \
          'LEFT JOIN notify_type ON notify.notify_type_id = notify_type.id ' \
          'LEFT JOIN equipment ON notify.equipment_id = equipment.id ' \
          'LEFT JOIN equipment AS object ON notify.object_id = object.id ' \
          'WHERE date(notify.time_stamp)  >=  curdate() - interval {} day ' \
          'ORDER BY notify.time_stamp DESC'

    cursor = db.cursor()

    cursor.execute(sql.format(deep_days))

    return cursor_to_json(cursor)

