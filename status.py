"""
    pathfinder status.py
    :copyright: (c) 2019 by Pavel Konovalov
    pasha@nettlecom.com
    Created on 2019-04-03
"""
from enum import IntEnum


class Code(IntEnum):
    SessionNotFound = 100
    UserMapInsertFault = 200


messages = {
    Code.SessionNotFound.value: {'status': 'Error', 'code': Code.SessionNotFound.value,
                                 'message': {'ru-RU': 'Пользователь не зарегистрирован',
                                             'en-GB': 'Session key failure'}},
    Code.UserMapInsertFault.value: {'status': 'Error', 'code': Code.UserMapInsertFault.value,
                                    'message': {'ru-RU': 'Невозможно сохранить пользовательскую карту',
                                                'en-GB': 'Error while inserting user map. Check your parameters'}}

}


def message_list():
    """
    Return JSON object with all errors
    :return:
    """
    return messages


def message(code, locale='ru-RU'):
    """
    Return JSON error message object for code and locale
    :param code:
    :param locale:
    :return:
    """

    mess = messages.get(code.value)
    if locale not in mess['message']:
        locale = 'ru-RU'

    return {'status': mess['status'], 'code': mess['code'], 'message': mess['message'].get(locale)}
