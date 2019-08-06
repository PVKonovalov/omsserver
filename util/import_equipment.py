from scada_gateway import ScadaGateway

if __name__ == "__main__":
    f = open('equip.sql', 'w', encoding='utf-8')
    f1 = open('equip_type.sql', 'w', encoding='utf-8')
    cache = {}
    scada = ScadaGateway('http://10.5.165.42:8080', 'admin', 'passme', cache)
    scada.logon()

    obj_json = scada.model_objstruct_objects()

    type_list = {}
    ids = 11
    type_id = 0
    for item in obj_json:
        ide = item['Id']
        guid = item['Uid']
        guid_parent = item.get('ParentUid')
        if guid_parent:
            guid_parent = "UUID_TO_BIN('{}')".format(guid_parent)
        else:
            guid_parent = 'NULL'

        name = item['Name']
        type_alias = item['Type']['DefineAlias']
        type_name = item['Type']['Name']
        location = item.get('GeoLocation')
        properties = item.get('Properties')
        class_u = 'NULL'

        if properties:
            param_uclass = properties.get('PARAM_UCLASS')
            if param_uclass:
                class_u = param_uclass.get('Value', 'NULL')

        lat = 'NULL'
        lng = 'NULL'

        if location:
            if location['Latitude'] != 0 and location['Longitude'] != 0:
                lat = location['Latitude']
                lng = location['Longitude']

        if type_alias not in type_list:
            type_list[type_alias] = {'id': ids, 'alias': type_alias, 'name': type_name}
            type_id = ids

            ids += 1
        else:
            type_id = type_list[type_alias]['id']

        # if class_u is 'NULL':
        #     continue

        f.write(
            "insert into equipment (id,guid,name,lat,lng,equipment_type_id, guid_parent, class_u) "
            "values ({},UUID_TO_BIN('{}'),'{}',{},{},{},{},{});\n".format(
                ide, guid, name, lat, lng, type_id, guid_parent, class_u))

    f.close()

    for item in type_list:
        f1.write("insert into equipment_type (id,name,alias) values ({},'{}','{}');\n".format(type_list[item]['id'],
                                                                                              type_list[item]['name'],
                                                                                              type_list[item]['alias']))
    f1.close()

    f = open('cnode.sql', 'w', encoding='utf-8')
    cnodes = scada.model_objstruct_cnodes(is_consumer=True)

    for item in cnodes:
        idc = item['Id']
        guid = item['Uid']
        name = item['Name']
        name_equipment = item['ObjectName']
        object_json = item.get('Object')
        equipment_id = 'NULL'

        if object_json:
            equipment_id = object_json['Id']

        f.write(
            "insert into cnode (id,guid,name,name_equipment, equipment_id) "
            "values ({},UUID_TO_BIN('{}'),'{}','{}',{});\n".format(
                idc, guid, name, name_equipment, equipment_id))

    f.close()

    scada.logoff()
