from helper_database import cursor_to_json


def get_list(db, parent_id):
    """
    Возвращает список документов
    :param db:
    :param parent_id:
    :return:
    """
    cursor = db.cursor()

    if parent_id is None:
        sql = 'SELECT id, name, url, is_directory ' \
              'FROM document ' \
              'where parent_id is null ' \
              'order by name'
        cursor.execute(sql)
    else:
        sql = 'SELECT id, name, url, is_directory ' \
              'FROM document ' \
              'where parent_id = %s ' \
              'order by name'
        cursor.execute(sql, parent_id)

    return cursor_to_json(cursor)
