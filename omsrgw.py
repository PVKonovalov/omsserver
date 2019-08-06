import requests
import json


oms_login = 'omssrv1'
oms_password = 'rsdu_211'
wap_url = 'http://10.5.165.42:8080'

network_objects = {}


def notify_client(data):
    try:
        r = requests.post('http://127.0.0.1/rsdu/oms/api/omsrgw/notify', json=data)
        #print(data)
        if r.status_code != 200:
            return None

    except requests.exceptions.RequestException as e:
        return None



def network_objects_update(new_network_objects):
    """
    Определение списка измененных состояний объектов
    :param new_network_objects:
    :return: Список состояний объектов
    """
    network_objects_changed = {}
    if network_objects is not None and len(network_objects) == 0:
        for item in new_network_objects:
            network_objects[item['ObjectId']] = item['State']
    else:
        for item in new_network_objects:
            if network_objects.get(item['ObjectId']) is None:
                network_objects_changed[item['ObjectId']] = item['State']
            else:
                if network_objects[item['ObjectId']] != item['State']:
                    network_objects_changed[item['ObjectId']] = item['State']
            network_objects[item['ObjectId']] = item['State']
    return network_objects_changed


def session_logon(login, password):
    """
    Авторизация и получение идентификатора сессии
    :param login:
    :param password:
    :return: Идентификатор сессии или None в случае ошибки
    """
    try:
        r = requests.post(wap_url + '/rsdu/session/logon', json={"Login": login, "Password": password})

        if r.status_code != 200:
            return None

        session = r.json()

    except requests.exceptions.RequestException as e:
        return None

    if session and 'SessionId' in session:
        return session['SessionId']


def session_logoff(session_id):
    """
    Завершение сессии
    :param session_id:
    :return: True | False
    """
    try:
        r = requests.get(wap_url + '/rsdu/session/logoff', headers={'Authorization': 'RSDU ' + session_id})

        if r.status_code == 200:
            return True
        else:
            return False

    except requests.exceptions.RequestException as e:
        return False


def get_all_topology_state(session_id):
    """
    Возвращает состояние сети в виде JSON
    :param session_id:
    :return: JSON состояния сети или None
    """
    try:
        r = requests.get(wap_url + '/rsdu/scada/api/app/topology/raw/network/state',
                         headers={'Authorization': 'RSDU ' + session_id})

        if r.status_code == 200:
            return r.json()
        else:
            return None

    except requests.exceptions.RequestException as e:
        return None


def topology_has_been_changed(db, rgw_request=None):
    """
    Обработка события об изменении топологии сети
    :param db:
    :param rgw_request:
    :return:
    """
    session_id = session_logon(oms_login, oms_password)

    if session_id is None:
        return {'status': 'Error',
                'code': 555,
                'message': 'Error while logon as {}'.format(oms_login)
                }

    new_network_objects = get_all_topology_state(session_id)
    if new_network_objects is not None:
        update_objects = network_objects_update(new_network_objects)
        notify_client(update_objects)


    session_logoff(session_id)

    return {'status': 'Ok'}
