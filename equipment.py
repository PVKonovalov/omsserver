from helper_database import cursor_to_json


def get_with_id(db, equipment_id):
    """
    Get equipment characteristic by equipment id
    :param db:
    :param equipment_id:
    :return:
    """
    sql = 'select id, ' \
          'name, ' \
          'lat, ' \
          'lng, ' \
          'equipment_type_id as type_id, ' \
          'class_u, ' \
          'BIN_TO_UUID(guid_parent) as guid_parent, ' \
          'BIN_TO_UUID(guid) as guid ' \
          'from equipment ' \
          'where id = %s'

    cursor = db.cursor()

    cursor.execute(sql, equipment_id)

    items = cursor_to_json(cursor)

    if len(items) > 0:
        return items[0]
    else:
        return None


def get_with_guid(db, guid):
    """
    Get equipment characteristic by equipment guid
    :param db:
    :param guid:
    :return:
    """
    sql = 'select id, ' \
          'name, ' \
          'lat, ' \
          'lng, ' \
          'equipment_type_id as type_id, ' \
          'class_u, ' \
          'BIN_TO_UUID(guid_parent) as guid_parent, ' \
          'BIN_TO_UUID(guid) as guid ' \
          'from equipment ' \
          'where guid = UUID_TO_BIN(%s)'

    cursor = db.cursor()

    cursor.execute(sql, guid)

    items = cursor_to_json(cursor)

    if len(items) > 0:
        return items[0]
    else:
        return None
