import time
import uuid

from flask import current_app as app

import notify_journal
from helper_database import cursor_to_json
from outage import calculate_for_customer_outage_journal
from scada_gateway import ScadaGateway

# Состояние объектов топологии.
network_objects = {}

# Кэш объектов для сигналов
objects_cache_for_signal = {}

# Кэш объектов для определения класса напряжения
object_cache_for_state = {}


def to_color_voltage_state(object_state_topology):
    """
    Уточнить классом напряжения состояние "Под напряжением"
    :param object_state_topology:
    :return:
    """
    global object_cache_for_state

    object_state_oms = []

    if object_state_topology is not None:
        for item in object_state_topology:

            state = item['State']
            cache_object = object_cache_for_state.get(item['ObjectId'])

            if cache_object:
                object_type = cache_object.get('type')
            else:
                object_type = None

            if state == 'UnderVoltage':
                if cache_object:
                    class_u = cache_object.get('class_u')

                    if class_u == 0.4:
                        state = 'UnderVoltage04kV'
                    elif class_u > 1:
                        state = 'UnderVoltage{}kV'.format(int(class_u))
            # TODO: 'position' должна устанавливаться из состояния КА полученного от ОИК
            if object_type == 'OTYP_RECLOSER':
                object_state_oms.append({'guid': item['ObjectId'], 'state': state, 'position': 'SS_PWS_ON'})
            else:
                object_state_oms.append({'guid': item['ObjectId'], 'state': state})

    return object_state_oms


def load_object_cache_for_state(db):
    """
    Загрузка кэша объектов имеющих класс напряжения. Это необходимо для
    правильного заполнения статуса, поскольку сервер топологии возвращает
    только статус Под_напряжением без указания класса напряжения,
    а на карте необходимо отобразить это состояние разными цветами в зависимости
    от класса напряжения.

    equipment_type содержит список типов оборудования отображаемых на GIS подложке
    :param db:
    """

    equipment_type = "'OTYP_RECLOSER', 'OTYP_AIR_POWER_LINE', 'OTYP_IKZ', 'OTYP_AIR_LINE_SEGMENT'"

    if db is None:
        return

    global object_cache_for_state
    object_cache_for_state = {}

    sql = 'SELECT BIN_TO_UUID(guid) as guid, equipment.name as name, equipment.class_u as class_u, ' \
          'equipment_type.alias as type ' \
          'FROM equipment ' \
          'LEFT JOIN equipment_type ON equipment.equipment_type_id = equipment_type.id ' \
          'WHERE ' \
          'equipment_type.alias in ({}) ' \
          'ORDER BY guid'.format(equipment_type)

    cursor = db.cursor()

    cursor.execute(sql)

    items = cursor_to_json(cursor)

    for item in items:
        object_cache_for_state[item['guid']] = item


def network_objects_update(new_network_objects):
    """
    Определение списка измененных состояний объектов
    :param new_network_objects:
    :return: Список состояний объектов
    """
    network_objects_changed = []
    global network_objects

    if network_objects is not None and len(network_objects) == 0:
        for item in new_network_objects:
            network_objects[item['ObjectId']] = item['State']
    else:
        for item in new_network_objects:
            if network_objects.get(item['ObjectId']) is None:
                network_objects_changed.append({'ObjectId': item['ObjectId'], 'State': item['State']})
            else:
                if network_objects[item['ObjectId']] != item['State']:
                    network_objects_changed.append({'ObjectId': item['ObjectId'], 'State': item['State']})
            network_objects[item['ObjectId']] = item['State']
    return network_objects_changed


def topology_changed(db, rgw_request=None):
    """
    Обработка события об изменении топологии сети
    :param db:
    :param rgw_request:
    :return:
    """
    oms_login = app.config.get('OMS_LOGIN')
    oms_password = app.config.get('OMS_PASSWORD')
    rsdu_api_url = app.config.get('RSDU_API_URL')

    scada = ScadaGateway(rsdu_api_url, oms_login, oms_password)
    if scada.logon():
        new_network_objects = scada.topology_network_state_types()
        update_objects = []
        colored_update_objects = []
        if new_network_objects is not None:
            update_objects = network_objects_update(new_network_objects)
            colored_update_objects = to_color_voltage_state(update_objects)

        scada.logoff()

        calculate_for_customer_outage_journal(db)

        return {'status': 'Ok',
                'state_changed': len(update_objects),
                'data': {'timestamp': int(time.time()),
                         'items': colored_update_objects}
                }
    else:
        return {'status': 'Error'}


def object_state(db):
    """
    Получить текущее состояние всех объектов топологии у сервера топологии и
    установить статусы в зависимости от класса напряжения
    :param db:
    :return:
    """
    global object_cache_for_state

    object_state_oms = []

    oms_login = app.config.get('OMS_LOGIN')
    oms_password = app.config.get('OMS_PASSWORD')
    rsdu_api_url = app.config.get('RSDU_API_URL')

    scada = ScadaGateway(rsdu_api_url, oms_login, oms_password)

    if scada.logon():
        object_state_topology = scada.topology_network_state_types()
        scada.logoff()

        object_state_oms = to_color_voltage_state(object_state_topology)

    return {'timestamp': int(time.time()), 'items': object_state_oms}


def signal_arrived(db, signal):
    """
    Create signal description and save its to notify_journal
    :param db:
    :param signal:
    :return:
    """
    global objects_cache_for_signal

    ids = signal.get('dir_param')

    if ids is None:
        return None

    oms_login = app.config.get('OMS_LOGIN')
    oms_password = app.config.get('OMS_PASSWORD')
    rsdu_api_url = app.config.get('RSDU_API_URL')

    scada = ScadaGateway(rsdu_api_url, oms_login, oms_password, objects_cache_for_signal)

    if scada.logon() is False:
        return None

    equipment_json = scada.model_objstruct_objects(ids=ids)

    if equipment_json is None:
        return None

    if len(equipment_json) == 0:
        return None

    equipment_guid = equipment_json[0]['Uid']
    equipment_name = equipment_json[0]['Name']

    object_json = scada.model_objstruct_objects(guid=equipment_guid, placement=True)

    scada.logoff()

    object_guid = object_json['Uid']
    object_name = object_json['Name']
    notify = signal['notify']
    state = signal['state']
    timestamp = signal['timestamp']
    guid = str(uuid.uuid4())

    notify_journal.insert(db, guid, equipment_guid, equipment_name, notify, object_guid, object_name, state, timestamp)

    notify_json = [{
        "equipment_guid": equipment_guid,
        "equipment_name": equipment_name,
        "id": guid,
        "notify": notify,
        "object_guid": object_guid,
        "object_name": object_name,
        "state": state,
        "timestamp": timestamp
    }]

    return notify_json
