"""
    pathfinder helper_json.py
    :copyright: (c) 2018 by Pavel Konovalov
    pasha@nettlecom.com
    Created on 02.07.2018
"""

import json

import flask


def resp(code, data, resp_charset='utf-8'):
    return flask.Response(
        status=code,
        mimetype='application/json; charset=' + resp_charset,
        response=json.dumps(data, indent=4, sort_keys=True, default=str, ensure_ascii=False).encode(resp_charset)
    )


def item_from_json(json_string, item, def_value=None):
    """
    Возвращет элемент из JSON описания по его имени
    :param def_value:
    :param json_string:
    :param item:
    :return: List of items or item
    """
    if json_string is None:
        return None

    if item in json_string:
        return json_string[item]
    else:
        return def_value


def string_to_number(str_value):
    """
    Преобразует строку в число (целое или с точкой)
    :param str_value:
    :return: Number
    """
    if str_value is None:
        return None

    try:
        return int(str_value)
    except ValueError:
        try:
            return float(str_value)
        except ValueError:
            return None


def item_number_from_json(json_string, item):
    """
    Возвращает числовой элемент из JSON описания по его имени
    Преобразует элемент из строки в число
    :param json_string:
    :param item:
    :return: List of Numbers or Number
    """
    if json_string is None:
        return None

    values_int = []
    if item in json_string:
        values = json_string[item]
        if isinstance(values, type([])):
            for idx, val in enumerate(values):
                values_int.append(string_to_number(val))
            if len(values_int) == 1:
                return values_int[0]
            else:
                return values_int
        else:
            return string_to_number(values)
    else:
        return None
