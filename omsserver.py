"""
    pathfinder omsserver.py
    :copyright: (c) 2018 by Pavel Konovalov
    pasha@nettlecom.com
    Created on 02.07.2018

    @startuml
    RMQ -> oms_gw_rmq_toplopogy: rsdu.scada.topology.NETWORK_STATE_UPDATE
    oms_gw_rmq_toplopogy -> omsserver: http://<omsserver>/rsdu/oms/api/omsgw/topochanged
    activate omsserver
    omsserver -> omsserver: omsgw.topology_changed
    omsserver -> SCADA: /rsdu/scada/api/app/topology/raw/network/state
    activate SCADA
    SCADA -> omsserver: JSON ...{}
    deactivate SCADA
    deactivate omsserver
    omsserver -> oms_gw_rmq_toplopogy: {Status: Ok|Error}
    @enduml

"""
import random
import time
import uuid
from threading import Lock
from threading import Thread

import requests
from flask import Flask, request
from flask_socketio import SocketIO
from flaskext.mysql import MySQL

import application
import application_state
import car
import car_journal
import customer
import gis
import gis_index
import gis_marker
import gis_user_map
import message
import mobile_team
import notify_journal
import omsgw
import outage
import outage_demand
import outage_demand_state
import status
import user
import user_path
from helper_crossdomain import crossdomain
from helper_json import resp
from helper_session import is_login
import gis_kml_style
import gis_mobile

app = Flask(__name__)
app.secret_key = '6aa80874-8984-4d72-b82e-5e1fe2a26060'

socketio = SocketIO(app, async_mode='gevent')

mysql = MySQL()
app.config.from_pyfile('config.ini')
mysql.init_app(app)

thread_state_changed_bg = None
thread_notify = None
thread_marker_set = None
thread_marker_remove = None
thread_car_set = None
thread_car_remove = None
thread_lock = Lock()

markers = []
cars = ['05a64b41-8120-11e9-a5eb-0050568518fc', '13a60adf-8120-11e9-a5eb-0050568518fc',
        '13a91386-8120-11e9-a5eb-0050568518fc', '13abc020-8120-11e9-a5eb-0050568518fc']

with app.app_context():
    omsgw.load_object_cache_for_state(mysql.get_db())


def emit_event(event, msg):
    socketio.emit(event, msg)


def thread_signal(socketio_emit, notify_json):
    if notify_json:
        socketio_emit('notify', notify_json)


def thread_state_changed(socketio_emit, notify_json):
    if notify_json:
        socketio_emit('state-changed', notify_json)


def background_thread_state_changed():
    styles = ['Undefined', 'UnderVoltage', 'Isolated', 'PartiallyGrounded', 'Grounded', 'Failure', 'Damaged',
              'Selected', 'UnderVoltage10kV', 'UnderVoltage6kV', 'UnderVoltage04kV']
    count = 0
    while True:
        socketio.sleep(10)
        count += 1
        emit_event('state-changed', {'timestamp': '2019-01-09 00:14:24',
                                     'items': [
                                         {'guid': '7a1f396b-09b9-4662-e050-0cace6001c14',
                                          'state': styles[count % len(styles) - 1]},
                                         {'guid': 'a6471e9b-f556-374f-982c-c60cf7d2cfc8',
                                          'state': styles[count % len(styles) - 1]}
                                     ]})


def background_thread_marker_set():
    global markers
    styles = ['marker-Zvonok-076420.png']
    # , 'marker-Brigada_OVB-200850.png', 'marker-Avariya-466037.png']
    idx = 100
    while True:
        socketio.sleep(63)
        idx += 1
        state = random.choice(styles)
        guid = str(uuid.uuid4())
        markers.append(guid)
        emit_json = [{
            "guid": guid,
            "icon": "/static/marker/{}".format(state),
            "lat": 58.044376,
            "lng": 32.822090,
            "name": "Звонок №{}".format(idx),
            "timestamp": int(time.time())
        }]
        emit_event('marker-set', emit_json)


def background_thread_marker_remove():
    global markers
    while True:
        socketio.sleep(123)
        if len(markers) > 0:
            guid = markers.pop(0)
            if guid is not None:
                emit_json = [guid]
                emit_event('marker-remove', emit_json)


def background_thread_car_set():
    while True:
        socketio.sleep(60)

        try:
            r = requests.get('http://127.0.0.1/rsdu/oms/api/gis/cars/coordinates/')

            emit_event('car-set', r.json())
            if r.status_code != 200:
                return None

        except requests.exceptions.RequestException:
            return None


def background_thread_car_remove():
    global cars
    while True:
        socketio.sleep(600)
        guid = random.choice(cars)
        emit_json = [guid]
        emit_event('car-remove', emit_json)


@socketio.on('connect')
def test_connect():
    global thread_state_changed_bg
    global thread_car_set
    # global thread_state_changed
    #     global thread_notify
    #     global thread_marker_set
    #     global thread_marker_remove
    #
    #     with thread_lock:
    #         #if thread_state_changed is None:
    #         #    thread_state_changed = socketio.start_background_task(background_thread_state_changed)
    #
    #         # if thread_marker_set is None:
    #         #    thread_marker_set = socketio.start_background_task(background_thread_marker_set)
    #
    #         # if thread_marker_remove is None:
    #         #    thread_marker_remove = socketio.start_background_task(background_thread_marker_remove)

    if thread_car_set is None:
        thread_car_set = socketio.start_background_task(background_thread_car_set)

    # if thread_state_changed_bg is None:
    #   thread_state_changed_bg = socketio.start_background_task(background_thread_state_changed)

    #
    # if thread_car_remove is None:
    #   thread_car_remove = socketio.start_background_task(background_thread_car_remove)


"""
Регистрация нового сеанса пользователя OMS. Возвращается сессионный ключ и описание пользователя.
"""


@app.route('/rsdu/oms/api/user/login/', methods=['POST'])
@app.route('/rsdu/oms/api/login/', methods=['POST'])
@crossdomain(origin='*', headers='Session-Key')
def user_login():
    login_and_password = request.get_json(force=True)
    login = login_and_password.get('login')
    password = login_and_password.get('password')
    return resp(200, user.login(mysql.get_db(), login, password, request.remote_addr))


"""
Закрытие сеанса пользователя OMS.
"""


@app.route('/rsdu/oms/api/user/logout/')
@app.route('/rsdu/oms/api/logout/')
@crossdomain(origin='*', headers='Session-Key')
def user_logout():
    session_key = request.headers.get('Session-Key', type=str, default=None)
    accept_language = request.headers.get('Accept-Language', type=str, default='ru-RU')
    if is_login(mysql.get_db(), session_key):
        return resp(200, user.logout(mysql.get_db(), session_key))
    else:
        return resp(200, status.message(status.Code.SessionNotFound, accept_language))


"""
Получить список пользователей OMS.
"""


@app.route('/rsdu/oms/api/user/list/')
@crossdomain(origin='*', headers='Session-Key')
def user_list():
    accept_language = request.headers.get('Accept-Language', type=str, default='ru-RU')
    if is_login(mysql.get_db(), request.headers.get('Session-Key', type=str, default=None)):
        return resp(200, user.get_list(mysql.get_db(),
                                       request.args.get('order', type=str, default='name-asc'),
                                       request.args.get('limit', type=int, default=None),
                                       request.args.get('page', type=int, default=None)))
    else:
        return resp(200, status.message(status.Code.SessionNotFound, accept_language))


"""
Получить описание текущего (заданног) пользователя OMS.
"""


@app.route('/rsdu/oms/api/user/')
@app.route('/rsdu/oms/api/user/<int:key>/')
@crossdomain(origin='*', headers='Session-Key')
def user_item(key=None):
    accept_language = request.headers.get('Accept-Language', type=str, default='ru-RU')
    session_key = request.headers.get('Session-Key', type=str, default=None)
    if is_login(mysql.get_db(), session_key):
        return resp(200, user.get_item(mysql.get_db(), session_key, key))
    else:
        return resp(200, status.message(status.Code.SessionNotFound, accept_language))


@app.route('/rsdu/oms/api/mobile_team/list/')
@crossdomain(origin='*', headers='Session-Key')
def mobile_team_list():
    accept_language = request.headers.get('Accept-Language', type=str, default='ru-RU')
    if is_login(mysql.get_db(), request.headers.get('Session-Key', type=str, default=None)):
        return resp(200, mobile_team.get_list(mysql.get_db(),
                                              request.args.get('order', type=str, default='name-asc'),
                                              request.args.get('limit', type=int, default=None),
                                              request.args.get('page', type=int, default=None)))
    else:
        return resp(200, status.message(status.Code.SessionNotFound, accept_language))


@app.route('/rsdu/oms/api/mobile_team/<int:key>/')
@crossdomain(origin='*', headers='Session-Key')
def mobile_team_item(key=None):
    accept_language = request.headers.get('Accept-Language', type=str, default='ru-RU')
    if is_login(mysql.get_db(), request.headers.get('Session-Key', type=str, default=None)):
        return resp(200, mobile_team.get_item(mysql.get_db(), key))
    else:
        return resp(200, status.message(status.Code.SessionNotFound, accept_language))


@app.route('/rsdu/oms/api/add_point/user/<int:key>/')
@crossdomain(origin='*', headers='Session-Key')
def add_point_user(key=None):
    accept_language = request.headers.get('Accept-Language', type=str, default='ru-RU')
    if is_login(mysql.get_db(), request.headers.get('Session-Key', type=str, default=None)):
        return resp(200, user_path.add_point(mysql.get_db(), key,
                                             request.args.get('lat', type=float, default=None),
                                             request.args.get('lng', type=float, default=None),
                                             request.args.get('time_stamp', type=str, default=None)))
    else:
        return resp(200, status.message(status.Code.SessionNotFound, accept_language))


@app.route('/rsdu/oms/api/get_last_point/user/<int:key>/')
@crossdomain(origin='*', headers='Session-Key')
def get_last_point(key=None):
    accept_language = request.headers.get('Accept-Language', type=str, default='ru-RU')
    if is_login(mysql.get_db(), request.headers.get('Session-Key', type=str, default=None)):
        return resp(200, user_path.get_last_point(mysql.get_db(), key))
    else:
        return resp(200, status.message(status.Code.SessionNotFound, accept_language))


@app.route('/rsdu/oms/api/get_last_points/')
@crossdomain(origin='*', headers='Session-Key')
def get_last_points():
    accept_language = request.headers.get('Accept-Language', type=str, default='ru-RU')
    if is_login(mysql.get_db(), request.headers.get('Session-Key', type=str, default=None)):
        return resp(200, user_path.get_last_points(mysql.get_db()))
    else:
        return resp(200, status.message(status.Code.SessionNotFound, accept_language))


@app.route('/rsdu/oms/api/message/list/')
@crossdomain(origin='*', headers='Session-Key')
def message_list():
    accept_language = request.headers.get('Accept-Language', type=str, default='ru-RU')
    if is_login(mysql.get_db(), request.headers.get('Session-Key', type=str, default=None)):
        return resp(200, message.get_list(mysql.get_db(),
                                          request.args.get('order', type=str, default='time-stamp-asc'),
                                          request.args.get('limit', type=int, default=None),
                                          request.args.get('page', type=int, default=None),
                                          request.args.get('user_from', type=int, default=None),
                                          request.args.get('user_to', type=int, default=None),
                                          request.args.get('has_received', type=int, default=None)))
    else:
        return resp(200, status.message(status.Code.SessionNotFound, accept_language))


@app.route('/rsdu/oms/api/message/conversation/')
@crossdomain(origin='*', headers='Session-Key')
def message_conversation():
    accept_language = request.headers.get('Accept-Language', type=str, default='ru-RU')
    session_key = request.headers.get('Session-Key', type=str, default=None)
    if is_login(mysql.get_db(), session_key):
        return resp(200, message.get_conversation(mysql.get_db(),
                                                  session_key,
                                                  request.args.get('with_user', type=int, default=None),
                                                  request.args.get('order', type=str, default='time-stamp-asc'),
                                                  request.args.get('limit', type=int, default=None),
                                                  request.args.get('page', type=int, default=None)
                                                  ))
    else:
        return resp(200, status.message(status.Code.SessionNotFound, accept_language))


@app.route('/rsdu/oms/api/message/<string:guid>/')
@crossdomain(origin='*', headers='Session-Key')
def message_item(guid=None):
    accept_language = request.headers.get('Accept-Language', type=str, default='ru-RU')
    if is_login(mysql.get_db(), request.headers.get('Session-Key', type=str, default=None)):
        return resp(200, message.get_item_by_guid(mysql.get_db(), guid))
    else:
        return resp(200, status.message(status.Code.SessionNotFound, accept_language))


@app.route('/rsdu/oms/api/message/get_last_messages/')
@crossdomain(origin='*', headers='Session-Key')
def get_last_messages():
    accept_language = request.headers.get('Accept-Language', type=str, default='ru-RU')
    session_key = request.headers.get('Session-Key', type=str, default=None)
    if is_login(mysql.get_db(), request.headers.get('Session-Key', type=str, default=None)):
        return resp(200, message.get_last_messages(mysql.get_db(),
                                                   session_key))
    else:
        return resp(200, status.message(status.Code.SessionNotFound, accept_language))


@app.route('/rsdu/oms/api/message/delete/<int:key>/', methods=['GET'])
@crossdomain(origin='*', headers='Session-Key')
def message_delete(key=None):
    accept_language = request.headers.get('Accept-Language', type=str, default='ru-RU')
    session_key = request.headers.get('Session-Key', type=str, default=None)
    if is_login(mysql.get_db(), session_key):
        return resp(200, message.delete_item(mysql.get_db(), key))
    else:
        return resp(200, status.message(status.Code.SessionNotFound, accept_language))


@app.route('/rsdu/oms/api/message/send/<int:key>/', methods=['GET', 'POST'])
@crossdomain(origin='*', headers='Session-Key')
def message_send(key=None):
    accept_language = request.headers.get('Accept-Language', type=str, default='ru-RU')
    session_key = request.headers.get('Session-Key', type=str, default=None)
    if is_login(mysql.get_db(), session_key):
        message_json = request.get_json(force=True)
        return resp(200, message.send(mysql.get_db(), key, session_key, message_json))
    else:
        return resp(200, status.message(status.Code.SessionNotFound, accept_language))


@app.route('/rsdu/oms/api/message/has_received/<int:key>/')
@crossdomain(origin='*', headers='Session-Key')
def message_has_received(key=None):
    accept_language = request.headers.get('Accept-Language', type=str, default='ru-RU')
    if is_login(mysql.get_db(), request.headers.get('Session-Key', type=str, default=None)):
        return resp(200, message.set_has_received(mysql.get_db(), key))
    else:
        return resp(200, status.message(status.Code.SessionNotFound, accept_language))


@app.route('/rsdu/oms/api/application/list/')
@crossdomain(origin='*', headers='Session-Key')
def application_list():
    accept_language = request.headers.get('Accept-Language', type=str, default='ru-RU')
    if is_login(mysql.get_db(), request.headers.get('Session-Key', type=str, default=None)):
        return resp(200, application.get_list(mysql.get_db(),
                                              request.args.get('order', type=str, default='name-asc'),
                                              request.args.get('limit', type=int, default=None),
                                              request.args.get('page', type=int, default=None)))
    else:
        return resp(200, status.message(status.Code.SessionNotFound, accept_language))


@app.route('/rsdu/oms/api/application/<int:key>/')
@crossdomain(origin='*', headers='Session-Key')
def application_item(key=None):
    accept_language = request.headers.get('Accept-Language', type=str, default='ru-RU')
    if is_login(mysql.get_db(), request.headers.get('Session-Key', type=str, default=None)):
        return resp(200, application.get_item(mysql.get_db(), key))
    else:
        return resp(200, status.message(status.Code.SessionNotFound, accept_language))


@app.route('/rsdu/oms/api/application_state/list/')
@crossdomain(origin='*', headers='Session-Key')
def application_state_list():
    accept_language = request.headers.get('Accept-Language', type=str, default='ru-RU')
    if is_login(mysql.get_db(), request.headers.get('Session-Key', type=str, default=None)):
        return resp(200, application_state.get_list(mysql.get_db(),
                                                    request.args.get('order', type=str, default='name-asc'),
                                                    request.args.get('limit', type=int, default=None),
                                                    request.args.get('page', type=int, default=None)))
    else:
        return resp(200, status.message(status.Code.SessionNotFound, accept_language))


@app.route('/rsdu/oms/api/application_state/<int:key>/')
@crossdomain(origin='*', headers='Session-Key')
def application_state_item(key=None):
    accept_language = request.headers.get('Accept-Language', type=str, default='ru-RU')
    if is_login(mysql.get_db(), request.headers.get('Session-Key', type=str, default=None)):
        return resp(200, application_state.get_item(mysql.get_db(), key))
    else:
        return resp(200, status.message(status.Code.SessionNotFound, accept_language))


# --- GIS ---
@app.route('/rsdu/oms/api/gis/layer_group/list/')
@crossdomain(origin='*', headers='Session-Key')
def gis_layer_group_get_list():
    accept_language = request.headers.get('Accept-Language', type=str, default='ru-RU')
    if is_login(mysql.get_db(), request.headers.get('Session-Key', type=str, default=None)):
        return resp(200, gis.layer_group_get_list(mysql.get_db()))
    else:
        return resp(200, status.message(status.Code.SessionNotFound, accept_language))


@app.route('/rsdu/oms/api/gis/layer_subgroup/list/')
@crossdomain(origin='*', headers='Session-Key')
def gis_layer_subgroup_get_list():
    accept_language = request.headers.get('Accept-Language', type=str, default='ru-RU')
    if is_login(mysql.get_db(), request.headers.get('Session-Key', type=str, default=None)):
        return resp(200, gis.layer_subgroup_get_list(mysql.get_db()))
    else:
        return resp(200, status.message(status.Code.SessionNotFound, accept_language))


@app.route('/rsdu/oms/api/gis/map_provider/list/')
@crossdomain(origin='*', headers='Session-Key')
def gis_map_provider_list():
    accept_language = request.headers.get('Accept-Language', type=str, default='ru-RU')
    if is_login(mysql.get_db(), request.headers.get('Session-Key', type=str, default=None)):
        return resp(200, gis.map_provider_get_list(mysql.get_db()))
    else:
        return resp(200, status.message(status.Code.SessionNotFound, accept_language))


@app.route('/rsdu/oms/api/gis/layer/list/')
@crossdomain(origin='*', headers='Session-Key')
def gis_layer_list():
    accept_language = request.headers.get('Accept-Language', type=str, default='ru-RU')
    if is_login(mysql.get_db(), request.headers.get('Session-Key', type=str, default=None)):
        return resp(200, gis.layer_get_list(mysql.get_db()))
    else:
        return resp(200, status.message(status.Code.SessionNotFound, accept_language))

@app.route('/rsdu/oms/api/gis_mobile/layer/list/')
@crossdomain(origin='*', headers='Session-Key')
def gis_mobile_layer_list():
    accept_language = request.headers.get('Accept-Language', type=str, default='ru-RU')
    if is_login(mysql.get_db(), request.headers.get('Session-Key', type=str, default=None)):
        return resp(200, gis_mobile.layer_get_list(mysql.get_db()))
    else:
        return resp(200, status.message(status.Code.SessionNotFound, accept_language))

@app.route('/rsdu/oms/api/gis/layer/<int:key>/')
@crossdomain(origin='*', headers='Session-Key')
def gis_layer_get_item(key=None):
    accept_language = request.headers.get('Accept-Language', type=str, default='ru-RU')
    if is_login(mysql.get_db(), request.headers.get('Session-Key', type=str, default=None)):
        return resp(200, gis.layer_get_item(mysql.get_db(), key))
    else:
        return resp(200, status.message(status.Code.SessionNotFound, accept_language))


@app.route('/rsdu/oms/api/gis/object/search/', methods=['POST'])
@crossdomain(origin='*', headers='Session-Key')
def gis_search():
    search = request.get_json(force=True)
    return resp(200, gis_index.fulltext_search_in_index(mysql.get_db(), search))


"""
Получить текущее состояние объектов (оборудования)
"""


@app.route('/rsdu/oms/api/gis/object/state/')
@crossdomain(origin='*', headers='Session-Key')
def gis_get_object_state():
    return resp(200, omsgw.object_state(mysql.get_db()))


@app.route('/rsdu/oms/api/gis/object/<string:guid>/')
@crossdomain(origin='*', headers='Session-Key')
def gis_get_object_by_guid(guid=None):
    accept_language = request.headers.get('Accept-Language', type=str, default='ru-RU')
    if is_login(mysql.get_db(), request.headers.get('Session-Key', type=str, default=None)):
        return resp(200, gis_index.get_object_by_guid(mysql.get_db(), guid))
    else:
        return resp(200, status.message(status.Code.SessionNotFound, accept_language))


@app.route('/rsdu/oms/api/gis/user_map/list/')
@crossdomain(origin='*', headers='Session-Key')
def gis_get_user_map():
    accept_language = request.headers.get('Accept-Language', type=str, default='ru-RU')
    session_key = request.headers.get('Session-Key', type=str, default=None)
    if is_login(mysql.get_db(), session_key):
        return resp(200, gis_user_map.get_list(mysql.get_db(), session_key))
    else:
        return resp(200, status.message(status.Code.SessionNotFound, accept_language))


@app.route('/rsdu/oms/api/gis/user_map/insert/', methods=['GET', 'POST'])
@crossdomain(origin='*', headers='Session-Key')
def gis_insert_user_map():
    accept_language = request.headers.get('Accept-Language', type=str, default='ru-RU')
    session_key = request.headers.get('Session-Key', type=str, default=None)
    if is_login(mysql.get_db(), session_key):
        user_map_json = request.get_json(force=True)
        return resp(200, gis_user_map.insert(mysql.get_db(), session_key, user_map_json))
    else:
        return resp(200, status.message(status.Code.SessionNotFound, accept_language))


@app.route('/rsdu/oms/api/gis/user_map/update/<int:key>/', methods=['GET', 'POST'])
@crossdomain(origin='*', headers='Session-Key')
def gis_update_user_map(key=None):
    accept_language = request.headers.get('Accept-Language', type=str, default='ru-RU')
    session_key = request.headers.get('Session-Key', type=str, default=None)
    if is_login(mysql.get_db(), session_key):
        user_map_json = request.get_json(force=True)
        return resp(200, gis_user_map.update(mysql.get_db(), session_key, user_map_json, key))
    else:
        return resp(200, status.message(status.Code.SessionNotFound, accept_language))


@app.route('/rsdu/oms/api/gis/user_map/update/', methods=['GET', 'POST'])
@crossdomain(origin='*', headers='Session-Key')
def gis_update_user_map2():
    accept_language = request.headers.get('Accept-Language', type=str, default='ru-RU')
    session_key = request.headers.get('Session-Key', type=str, default=None)
    if is_login(mysql.get_db(), session_key):
        user_map_json = request.get_json(force=True)
        return resp(200, gis_user_map.update(mysql.get_db(), session_key, user_map_json))
    else:
        return resp(200, status.message(status.Code.SessionNotFound, accept_language))


@app.route('/rsdu/oms/api/gis/user_map/delete/<int:key>/')
@crossdomain(origin='*', headers='Session-Key')
def gis_delete_user_map(key=None):
    accept_language = request.headers.get('Accept-Language', type=str, default='ru-RU')
    session_key = request.headers.get('Session-Key', type=str, default=None)
    if is_login(mysql.get_db(), request.headers.get('Session-Key', type=str, default=None)):
        return resp(200, gis_user_map.delete(mysql.get_db(), session_key, key))
    else:
        return resp(200, status.message(status.Code.SessionNotFound, accept_language))


@app.route('/rsdu/oms/api/gis/user_map/select/<int:key>/')
@crossdomain(origin='*', headers='Session-Key')
def gis_select_user_map(key=None):
    accept_language = request.headers.get('Accept-Language', type=str, default='ru-RU')
    session_key = request.headers.get('Session-Key', type=str, default=None)
    if is_login(mysql.get_db(), request.headers.get('Session-Key', type=str, default=None)):
        return resp(200, gis_user_map.set_current(mysql.get_db(), session_key, key))
    else:
        return resp(200, status.message(status.Code.SessionNotFound, accept_language))


@app.route('/rsdu/oms/api/help/status/')
@crossdomain(origin='*', headers='Session-Key')
def help_error():
    return resp(200, status.message_list())


@app.route('/rsdu/oms/api/gis/object/state/list/')
@crossdomain(origin='*', headers='Session-Key')
def object_state_style_get_list():
    return resp(200, gis.object_state_style_get_list(mysql.get_db()))


@app.route('/rsdu/oms/api/gis/cars/list/')
@crossdomain(origin='*', headers='Session-Key')
def gis_get_cars_list():
    session_key = request.headers.get('Session-Key', type=str, default=None)
    accept_language = request.headers.get('Accept-Language', type=str, default='ru-RU')
    if is_login(mysql.get_db(), session_key):
        return resp(200, car.get_list(mysql.get_db()))
    else:
        return resp(200, status.message(status.Code.SessionNotFound, accept_language))


@app.route('/rsdu/oms/api/gis/cars/coordinates/')
@crossdomain(origin='*', headers='Session-Key')
def gis_get_cars_coordinates_list():
    return resp(200, car_journal.get_list(mysql.get_db()))
    # session_key = request.headers.get('Session-Key', type=str, default=None)
    # accept_language = request.headers.get('Accept-Language', type=str, default='ru-RU')
    # if is_login(mysql.get_db(), session_key):
    #     return resp(200, car_journal.get_list(mysql.get_db()))
    # else:
    #     return resp(200, status.message(status.Code.SessionNotFound, accept_language))


@app.route('/rsdu/oms/api/gis/notify/list/')
@crossdomain(origin='*', headers='Session-Key')
def gis_get_notify_list():
    session_key = request.headers.get('Session-Key', type=str, default=None)
    accept_language = request.headers.get('Accept-Language', type=str, default='ru-RU')
    if is_login(mysql.get_db(), session_key):
        return resp(200, notify_journal.get_list(mysql.get_db()))
    else:
        return resp(200, status.message(status.Code.SessionNotFound, accept_language))


@app.route('/rsdu/oms/api/gis/marker/list/')
@crossdomain(origin='*', headers='Session-Key')
def gis_get_marker_list():
    session_key = request.headers.get('Session-Key', type=str, default=None)
    accept_language = request.headers.get('Accept-Language', type=str, default='ru-RU')
    if is_login(mysql.get_db(), session_key):
        return resp(200, gis_marker.get_list(mysql.get_db()))
    else:
        return resp(200, status.message(status.Code.SessionNotFound, accept_language))


# Notifications callbacks from OMS gateway
# Уведомление об изменении топологии
@app.route('/rsdu/oms/api/omsgw/topochanged', methods=['POST'])
@crossdomain(origin='*', headers='Session-Key')
def topology_changed():
    r = omsgw.topology_changed(mysql.get_db(), request.get_json(force=True))

    notify_json = r.get('data')
    if notify_json is not None:
        socketio_thread = Thread(target=thread_state_changed, args=(socketio.emit, notify_json))
        socketio_thread.daemon = True
        socketio_thread.start()

    return resp(200, {'status': r['status'], 'state_changed': r.get('state_changed', 0)})


# Уведомление о приходе сигнала о коммутации
@app.route('/rsdu/oms/api/omsgw/signal', methods=['POST'])
@crossdomain(origin='*', headers='Session-Key')
def signal_arrived():
    notify_json = omsgw.signal_arrived(mysql.get_db(), request.get_json(force=True))

    socketio_thread = Thread(target=thread_signal, args=(socketio.emit, notify_json))
    socketio_thread.daemon = True
    socketio_thread.start()

    return resp(200, 'OK')


@app.route('/rsdu/oms/api/outage/demand/state/list/')
@crossdomain(origin='*', headers='Session-Key')
def outage_demand_state_list():
    session_key = request.headers.get('Session-Key', type=str, default=None)
    accept_language = request.headers.get('Accept-Language', type=str, default='ru-RU')
    if is_login(mysql.get_db(), session_key):
        return resp(200, outage_demand_state.get_list(mysql.get_db()))
    else:
        return resp(200, status.message(status.Code.SessionNotFound, accept_language))


@app.route('/rsdu/oms/api/outage/demand/list/')
@crossdomain(origin='*', headers='Session-Key')
def outage_demand_list():
    session_key = request.headers.get('Session-Key', type=str, default=None)
    accept_language = request.headers.get('Accept-Language', type=str, default='ru-RU')
    if is_login(mysql.get_db(), session_key):
        return resp(200, outage_demand.get_list(mysql.get_db(),
                                                request.args.get('order', type=str, default='timestamp-desc'),
                                                request.args.get('limit', type=int, default=None),
                                                request.args.get('page', type=int, default=None)))
    else:
        return resp(200, status.message(status.Code.SessionNotFound, accept_language))


@app.route('/rsdu/oms/api/outage/demand/create/', methods=['POST'])
@crossdomain(origin='*', headers='Session-Key')
def outage_demand_create():
    session_key = request.headers.get('Session-Key', type=str, default=None)
    accept_language = request.headers.get('Accept-Language', type=str, default='ru-RU')
    if is_login(mysql.get_db(), session_key):
        return resp(200, outage_demand.create(mysql.get_db(), request.get_json(force=True)))
    else:
        return resp(200, status.message(status.Code.SessionNotFound, accept_language))


@app.route('/rsdu/oms/api/outage/demand/update/<int:key>/', methods=['POST'])
@crossdomain(origin='*', headers='Session-Key')
def outage_demand_update(key=None):
    session_key = request.headers.get('Session-Key', type=str, default=None)
    accept_language = request.headers.get('Accept-Language', type=str, default='ru-RU')
    if is_login(mysql.get_db(), session_key):
        return resp(200, outage_demand.update(mysql.get_db(), key, request.get_json(force=True)))
    else:
        return resp(200, status.message(status.Code.SessionNotFound, accept_language))


@app.route('/rsdu/oms/api/customer/list/')
@crossdomain(origin='*', headers='Session-Key')
def customer_list():
    return resp(200, customer.get_list(mysql.get_db()))


@app.route('/rsdu/oms/api/customer/locality/search/', methods=['POST'])
@crossdomain(origin='*', headers='Session-Key')
def search_in_locality():
    search = request.get_json(force=True)
    return resp(200, customer.search_in_locality(mysql.get_db(), search))


@app.route('/rsdu/oms/api/customer/street/search/', methods=['POST'])
@crossdomain(origin='*', headers='Session-Key')
def search_in_street():
    search = request.get_json(force=True)
    return resp(200, customer.search_in_street(mysql.get_db(), search))


@app.route('/rsdu/oms/api/customer/search/', methods=['POST'])
@crossdomain(origin='*', headers='Session-Key')
def search_in_customer():
    search = request.get_json(force=True)
    return resp(200, customer.search(mysql.get_db(), search))


@app.route('/rsdu/oms/api/customer/outage/state/')
@crossdomain(origin='*', headers='Session-Key')
def customer_outage_journal():
    return resp(200, outage.get_customer_outage_journal(mysql.get_db()))


@app.route('/rsdu/oms/api/kml_style/list/')
@crossdomain(origin='*', headers='Session-Key')
def kml_stile_list():
    return resp(200, gis_kml_style.get_list())


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
