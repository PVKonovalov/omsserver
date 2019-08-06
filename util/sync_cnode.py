import configparser

import model_updater
import pymysql

from helper_database import cursor_to_json
from scada_gateway import ScadaGateway


def get_new_cnode():
    scada = ScadaGateway('http://10.5.165.42:8080', 'admin', 'passme')
    scada.logon()

    cnodes = scada.model_objstruct_cnodes(is_consumer=True)
    scada.logoff()

    cnodes_new = []

    for item in cnodes:
        idc = item['Id']
        guid = item['Uid']
        name = item['Name']
        name_equipment = item['ObjectName']
        object_json = item.get('Object')
        equipment_id = 'NULL'

        if object_json:
            equipment_id = object_json['Id']

        cnodes_new.append(
            {'idc': idc, 'guid': guid, 'name': name, 'name_equipment': name_equipment, 'equipment_id': equipment_id})

    return cnodes_new


def get_current_cnode():

    sql = 'SELECT id as idc ,BIN_TO_UUID(guid) as guid, name,name_equipment, equipment_id ' \
          'from cnode'

    config = configparser.ConfigParser()
    config.read('config.cfg')

    db = pymysql.connect(config['MYSQL']['MYSQL_DATABASE_HOST'], config['MYSQL']['MYSQL_DATABASE_USER'],
                         config['MYSQL']['MYSQL_DATABASE_PASSWORD'], config['MYSQL']['MYSQL_DATABASE_DB'])

    cursor = db.cursor()
    cursor.execute(sql)
    cnodes = cursor_to_json(cursor)

    db.close()

    return cnodes


if __name__ == "__main__":

    cnodes_new_raw = get_new_cnode()
    cnodes_current_raw = get_current_cnode()

    cnodes_new = model_updater.sync_models(cnodes_new_raw, cnodes_current_raw)

    f = open('cnode.sql', 'w', encoding='utf-8')

    for item, value in cnodes_new.items():
        if value.get('action'):
            if value['action'] == 'a':
                data = value['data']

                idc = data['idc']
                guid = data['guid']
                name = data['name']
                name_equipment = data['name_equipment']
                equipment_id = data['equipment_id']

                f.write(
                    "insert into cnode (id,guid,name,name_equipment, equipment_id) "
                    "values ({},UUID_TO_BIN('{}'),'{}','{}',{});\n".format(
                        idc, guid, name, name_equipment, equipment_id))

                #print(value)
    f.close()
