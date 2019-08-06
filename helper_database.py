"""
    pathfinder helper_database.py
    :copyright: (c) 2018 by Pavel Konovalov
    pasha@nettlecom.com
    Created on 02.07.2018
"""

from enum import IntEnum


class MysqlType(IntEnum):
    JSON = 245


def cursor_to_json(cursor):
    """
    Преобразует результат запроса к БД в JSON объект
    :param cursor:
    :return: JSON
    """
    return [dict((cursor.description[i][0], value) for i, value in enumerate(row) if value is not None and value != '')
            for row in
            cursor.fetchall()]


def cursor_one_to_json(cursor):
    """
    Преобразует результат запроса к БД в JSON объект
    :param cursor:
    :return: JSON
    """
    return [dict((cursor.description[i][0], value) for i, value in enumerate(row) if value is not None and value != '')
            for row in cursor.fetchall()]


def array_to_where_in(table_name, field_name, value_array, condition='and'):
    """
    Перобразует массив значений в условие включения или одно значение в условие равенства
    :param table_name:
    :param value_array:
    :param field_name:
    :param condition:
    :return: String
    """
    if value_array is not None:
        if isinstance(value_array, type([])):
            if len(value_array) > 0:
                return ' {} {}.{} in ({})'.format(condition, table_name, field_name, ','.join(map(str, value_array)))
        else:
            if value_array is not None:
                return ' {} {}.{} = {}'.format(condition, table_name, field_name, value_array)
            else:
                return ''
    else:
        return ''


def array_to_where_between(table_name, field_name, value_array):
    """
    Преобразует массив значений из двух элементов в условие between
    :param table_name:
    :param value_array:
    :param field_name:
    :return: String
    """
    r = ''

    if value_array is not None:
        if isinstance(value_array, type([])):
            if len(value_array) == 2:
                if isinstance(value_array[0], int) and isinstance(value_array[1], int):
                    if value_array[0] == value_array[1]:
                        r = ' and {}.{} = {}'.format(table_name, field_name, value_array[0])
                    else:
                        r = ' and {}.{} between {} and {}'.format(table_name, field_name, value_array[0],
                                                                  value_array[1])
                else:
                    if isinstance(value_array[0], int) and value_array[1] is None:
                        r = ' and {}.{} >= {}'.format(table_name, field_name, value_array[0])
                    elif isinstance(value_array[1], int) and value_array[0] is None:
                        r = ' and {}.{} <= {}'.format(table_name, field_name, value_array[1])
    return r
