"""
    omsserver scada_gateway.py
    :copyright: (c) 2019 by Pavel Konovalov
    pasha@nettlecom.com
    Created on 2019-05-17
"""

import requests
from datetime import datetime, timezone


class ScadaGateway:

    def __init__(self, scada_url, login, password, cache=None):
        self.login = login
        self.password = password
        self.scada_url = scada_url
        self.session_id = None
        self.cache = cache
        self.user_id = None

    def logon(self):
        """
        Авторизация и получение идентификатора сессии
        :return: True | False
        """
        if self.scada_url is None or self.password is None or self.scada_url is None:
            return False

        try:
            r = requests.post(self.scada_url + '/rsdu/session/logon',
                              json={"Login": self.login, "Password": self.password})

            if r.status_code != 200:
                return False

            session = r.json()

        except requests.exceptions.RequestException:
            return False

        self.session_id = session.get('SessionId')

        if self.session_id is None:
            return False
        else:
            user = session.get('User')
            if user:
                self.user_id = user.get('Id')
            return True

    def logoff(self):
        """
        Завершение сессии
        :return: True | False
        """
        try:
            r = requests.get(self.scada_url + '/rsdu/session/logoff',
                             headers={'Authorization': 'RSDU ' + self.session_id})

            if r.status_code == 200:
                return True
            else:
                return False

        except requests.exceptions.RequestException:
            return False

    def topology_network_state_types(self):
        """
        Возвращает состояние сети в виде JSON для заданных типов
        :return: JSON состояния сети или None
        """
        try:
            r = requests.post(self.scada_url + '/rsdu/scada/api/app/topology/raw/network/state',
                              json={
                                  'ObjectTypes': ['OTYP_IKZ',
                                                  'OTYP_CONSUMER',
                                                  'OTYP_RECLOSER',
                                                  'OTYP_POWER_SWITCH',
                                                  'OTYP_AIR_POWER_LINE',
                                                  'OTYP_AIR_LINE_SEGMENT',
                                                  'OTYP_CABLE_POWER_LINE',
                                                  'OTYP_CABLE_LINE_SEGMENT',
                                                  'OTYP_DISCONNECTING_SWITCH',
                                                  'OTYP_DISTRIBUTION_SUBSTATION'
                                                  ]},
                              headers={'Authorization': 'RSDU ' + self.session_id})

            if r.status_code == 200:
                return r.json()
            else:
                return None

        except requests.exceptions.RequestException:
            return None

    def topology_network_state_all(self):
        """
        Возвращает состояние сети в виде JSON
        :return: JSON состояния сети или None
        """
        try:
            r = requests.get(self.scada_url + '/rsdu/scada/api/app/topology/raw/network/state',
                             headers={'Authorization': 'RSDU ' + self.session_id})

            if r.status_code == 200:
                return r.json()
            else:
                return None

        except requests.exceptions.RequestException:
            return None

    def model_objstruct_object_by_type(self, object_types):
        """
        Возвращает описание объектов по их типу.
        :param object_types:
        :return:
        """
        params = '?types={}'.format(object_types)

        try:
            r = requests.get(self.scada_url + '/rsdu/scada/api/model/objstruct/objects{}'.format(params),
                             headers={'Authorization': 'RSDU ' + self.session_id})
            r_json = r.json()

            if r.status_code == 200:
                return r_json
            else:
                return None

        except requests.exceptions.RequestException:
            return None

    def model_objstruct_objects(self, ids=None, guid=None, placement=False):
        """
        Возвращает описание оборудования или объекта в котором оборудование установлено по его идентификатору или GUID.
        :param ids: Идентификатор
        :param guid: GUID
        :param placement: True, если нужно описание объекта
        :return:
        """
        params = ''
        placement_str = ''
        cache_key = None

        if placement:
            placement_str = '/placement'

        if ids is not None:
            params = '?ids={}'.format(ids)
            cache_key = ids
        else:
            if guid is not None:
                params = '/{}{}'.format(guid, placement_str)
                cache_key = guid

        r_json = None

        if self.cache is not None:
            r_json = self.cache.get(cache_key)

        if r_json is not None:
            return r_json

        try:
            r = requests.get(self.scada_url + '/rsdu/scada/api/model/objstruct/objects{}'.format(params),
                             headers={'Authorization': 'RSDU ' + self.session_id})
            r_json = r.json()

            if r.status_code == 200:
                if self.cache is not None:
                    self.cache[cache_key] = r_json
                return r_json
            else:
                return None

        except requests.exceptions.RequestException:
            return None

    def model_objstruct_cnodes(self, is_consumer=None, object_uid=None, recursively=None):
        """
        Возвращает список соединительных узлов
        :param is_consumer:
        :param object_uid:
        :param recursively:
        :return:
        """
        params = ''

        if object_uid is not None:
            params = '/?object_uid={}'.format(object_uid)

        if is_consumer is True:
            if params != '':
                params += '&'
            else:
                params += '?'
            params += 'isconsumer=true'
        elif is_consumer is False:
            if params != '':
                params += '&'
            else:
                params += '?'
            params += 'isconsumer=false'

        if recursively is True:
            if params != '':
                params += '&'
            else:
                params += '/?'
            params += 'recursively=true'
        elif recursively is False:
            if params != '':
                params += '&'
            else:
                params += '/?'
            params += 'recursively=false'

        try:

            r = requests.get(self.scada_url + '/rsdu/scada/api/model/objstruct/cnodes{}'.format(params),
                             headers={'Authorization': 'RSDU ' + self.session_id})
            r_json = r.json()

            if r.status_code == 200:
                return r_json
            else:
                return None

        except requests.exceptions.RequestException:
            return None

    def put_marker_on_object(self, guid_object, text, type='OTYP_TAG_COMMENT'):

        marker = {
            "object_guid": guid_object,
            "type_alias": type,
            "is_visible": True,
            "create_user_id": self.user_id,
            "text": text,
            "start_time": datetime.now(timezone.utc).isoformat()
        }

        try:
            r = requests.post(self.scada_url + '/rsdu/scada/api/oic/disptags/tags',
                              json=marker, headers={'Authorization': 'RSDU ' + self.session_id})

            if r.status_code != 200:
                return None

            result = r.json()

        except requests.exceptions.RequestException:
            return None

        return result.get('id')
