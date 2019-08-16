"""
    omsserver gis_mobile.py
    :copyright: (c) 2019 by Pavel Konovalov
    pasha@nettlecom.com
    Created on 2019-08-13
"""

import json
import time

from helper_database import cursor_to_json, MysqlType

def layer_get_list(db):
    """
    Return the list of enabled layers
    :param db:
    :return:
    """
    start = time.time()
    r = {}
    cursor = db.cursor()

    sql = 'select id, name from gis_layer where enabled_for_mobile = 1 order by sortorder, name'
    cursor.execute(sql)

    items = []
    for row in cursor.fetchall():
        item = {}
        for i, value in enumerate(row):
            if value is not None:
                if cursor.description[i][1] == MysqlType.JSON.value:
                    item[cursor.description[i][0]] = json.loads(value)
                else:
                    item[cursor.description[i][0]] = value
        items.append(item)

    r['count'] = len(items)
    r['items'] = items
    r['elapsed'] = time.time() - start

    return r