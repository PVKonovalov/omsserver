"""
    omsserver outage_demand_state.py
    :copyright: (c) 2019 by Pavel Konovalov
    pasha@nettlecom.com
    Created on 2019-06-13
"""
from helper_database import cursor_to_json


def get_list(db):
    """
    Возвращает список возможных состояний для заявок (звонков) потребителей
    :param db:
    :return:
    [
        {
            "cls": "badge-success",
            "id": 1,
            "name": "Завершено"
        },...
    ]
    """
    sql = 'SELECT id, name, cls ' \
          'FROM outage_demand_state ' \
          'ORDER BY id'

    cursor = db.cursor()

    cursor.execute(sql)

    items = cursor_to_json(cursor)

    return items
