"""
    omsserver objects_state.py
    :copyright: (c) 2019 by Pavel Konovalov
    pasha@nettlecom.com
    Created on 2019-06-19
"""

import configparser

import pymysql

from scada_gateway import ScadaGateway


def objects_state():
    objects_network_state = []

    config = configparser.ConfigParser()
    config.read('config.cfg')

    oms = config['OMS']
    oms_login = oms.get('LOGIN')
    oms_password = oms.get('PASSWORD')
    rsdu_api_url = oms.get('RSDU_API_URL')

    mysql = config['MYSQL']

    db = pymysql.connect(mysql['MYSQL_DATABASE_HOST'], mysql['MYSQL_DATABASE_USER'],
                         mysql['MYSQL_DATABASE_PASSWORD'], mysql['MYSQL_DATABASE_DB'])

    cursor = db.cursor()

    sql = 'select equipment.name as equipment, equipment_type.name as equipment_type, equipment.class_u as class_u ' \
          'from equipment ' \
          'LEFT JOIN equipment_type ON equipment.equipment_type_id = equipment_type.id ' \
          'where BIN_TO_UUID(equipment.guid) = %s'

    scada = ScadaGateway(rsdu_api_url, oms_login, oms_password)

    if scada.logon():
        objects_network_state = scada.topology_network_state_types()
        scada.logoff()

        for item in objects_network_state:
            guid = item.get('ObjectId')
            state = item.get('State')
            try:
                cursor.execute(sql, guid)
                object_name = cursor.fetchone()

                print('{};{};{};{};{}'.format(guid, object_name[1], state, object_name[2], object_name[0]))
            except Exception as e:
                print(guid)

    cursor.close()
    db.close()


if __name__ == "__main__":
    objects_state()
