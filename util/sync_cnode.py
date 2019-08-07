"""
Синхронизация модели оборудования в БД OMS с БД оборудования РСДУ5
Формируются файлы cnode_insert.sql, cnode_update.sql, cnode_delete.sql
для загрузки модели в БД OMS
"""

import configparser
import hashlib
import json
import pymysql

from helper_database import cursor_to_json
from scada_gateway import ScadaGateway


def prepare_model_and_hash(model_raw):
    model = {}

    for cnode_data in model_raw:
        guid = cnode_data.get('guid')

        if guid is None:
            return None

        model[guid] = {'guid': guid, 'action': '', 'cnode_data': cnode_data,
                       'hash': hashlib.md5(
                           json.dumps(cnode_data, sort_keys=True, indent=2).encode("utf-8")).hexdigest()}

    return model


def sync_models(model_new_raw, model_old_raw):
    model = {}

    model_new = prepare_model_and_hash(model_new_raw)
    model_old = prepare_model_and_hash(model_old_raw)

    for guid, value in model_old.items():
        model[guid] = value
        model[guid]['action'] = 'd'

    for guid, value in model_new.items():

        if model.get(guid) is None:
            model[guid] = value
            model[guid]['action'] = 'a'
        elif value['hash'] != model[guid]['hash']:
            model[guid]['action'] = 'u'
        else:
            del model[guid]['action']

    return model


def get_new_cnode(config):
    scada = ScadaGateway(config['OMS']['RSDU_API_URL'], config['OMS']['LOGIN'], config['OMS']['PASSWORD'])
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


def get_current_cnode(config):
    db = pymysql.connect(config['MYSQL']['MYSQL_DATABASE_HOST'], config['MYSQL']['MYSQL_DATABASE_USER'],
                         config['MYSQL']['MYSQL_DATABASE_PASSWORD'], config['MYSQL']['MYSQL_DATABASE_DB'])

    cursor = db.cursor()

    sql = 'SELECT id as idc, BIN_TO_UUID(guid) as guid, name, name_equipment, equipment_id from cnode'
    cursor.execute(sql)

    cnodes = cursor_to_json(cursor)

    db.close()

    return cnodes


if __name__ == "__main__":

    config = configparser.ConfigParser()
    config.read('config.cfg')

    cnodes_new_raw = get_new_cnode(config)
    cnodes_current_raw = get_current_cnode(config)

    cnodes_new = sync_models(cnodes_new_raw, cnodes_current_raw)

    f_insert = open('cnode_insert.sql', 'w', encoding='utf-8')
    f_update = open('cnode_update.sql', 'w', encoding='utf-8')
    f_delete = open('cnode_delete.sql', 'w', encoding='utf-8')

    for item, value in cnodes_new.items():
        if value.get('action'):
            if value['action'] == 'a':
                cnode_data = value['cnode_data']
                idc = cnode_data['idc']
                guid = cnode_data['guid']
                name = cnode_data['name']
                name_equipment = cnode_data['name_equipment']
                equipment_id = cnode_data['equipment_id']

                f_insert.write(
                    "insert into cnode (id,guid,name,name_equipment,equipment_id) "
                    "values ({},UUID_TO_BIN('{}'),'{}','{}',{});\n".format(
                        idc, guid, name, name_equipment, equipment_id))

            if value['action'] == 'd':
                cnode_data = value['cnode_data']
                guid = cnode_data['guid']

                f_delete.write("delete from cnode where guid = UUID_TO_BIN('{}');\n".format(guid))

            if value['action'] == 'u':
                cnode_data = value['cnode_data']
                guid = cnode_data['guid']
                name = cnode_data['name']
                name_equipment = cnode_data['name_equipment']
                equipment_id = cnode_data['equipment_id']

                f_update.write("update cnode set name = '{}', name_equipment = '{}', equipment_id = '{}' "
                               "where guid = UUID_TO_BIN('{}');\n".format(name, name_equipment, equipment_id, guid))

    f_insert.close()
    f_update.close()
    f_delete.close()
