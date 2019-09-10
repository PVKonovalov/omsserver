"""
    omsserver gis_mobile.py
    :copyright: (c) 2019 by Pavel Konovalov
    pasha@nettlecom.com
    Created on 2019-08-13
"""

import json
import time

from helper_database import cursor_to_json


def layer_get_list(db):
    """
    Return the list of enabled layers
    :param db:
    :return:
    """
    start = time.time()
    r = {}
    cursor = db.cursor()

    sql = 'select id, ' \
          'name, ' \
          'subgroup_id as sbgrp_id ' \
          'from gis_layer ' \
          'where enabled_for_mobile = 1 ' \
          'order by sortorder, name'

    cursor.execute(sql)

    r['layers'] = cursor_to_json(cursor)

    sql = 'select gis_layer_subgroup.id as id, gis_layer_subgroup.name as name ' \
          'from gis_layer_subgroup ' \
          'where gis_layer_subgroup.id in ' \
          '(select distinct subgroup_id from gis_layer ' \
          'where enabled_for_mobile = 1 ) ' \
          'order by gis_layer_subgroup.sortorder'

    cursor.execute(sql)

    r['sbrgp'] = cursor_to_json(cursor)

    return r
