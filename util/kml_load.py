"""
    pathfinder kml_load.py
    :copyright: (c) 2019 by Pavel Konovalov
    pasha@nettlecom.com
    Created on 2019-03-18
"""

import configparser
import json
import os
import sys

import pymysql

import util.kml2geojson as kml2geojson


def layer_id_by_name(file):
    layername = os.path.splitext(file)[0]

    sql = 'SELECT id FROM gis_layer where name = %s'

    config = configparser.ConfigParser()
    config.read('config.cfg')

    try:
        db = pymysql.connect(config['MYSQL']['MYSQL_DATABASE_HOST'], config['MYSQL']['MYSQL_DATABASE_USER'],
                             config['MYSQL']['MYSQL_DATABASE_PASSWORD'], config['MYSQL']['MYSQL_DATABASE_DB'])

        cursor = db.cursor()
        cursor.execute(sql, layername)
        gis_layer_id = cursor.fetchone()[0]

        db.close()

    except Exception as e:
        print(e)
        return None

    return gis_layer_id


def kml_load_directory(path_kml_directory):
    """
    Load all *.kml files from all subdirectories
    :param path_kml_directory:
    """
    for (dirpath, dirnames, filenames) in os.walk(path_kml_directory):
        ui_command = input('Update or Insert or Quit (u/i/q)?')

        if ui_command in ['Q', 'q'] or ui_command not in ['u', 'U', 'I', 'i']:
            return

        for file in filenames:
            if ".kml" in file:
                if ui_command in ['u', 'U']:
                    gis_layer_id = layer_id_by_name(file)

                    if gis_layer_id is None:
                        gis_layer_id = int(input('Enter layers Id for {}:'.format(file)))
                    else:
                        print('{} Id: {}'.format(file, gis_layer_id))

                    kml_load_file(os.path.join(dirpath, file), gis_layer_id)
                else:
                    kml_load_file(os.path.join(dirpath, file))


def kml_load_file(file_path, gis_layer_id=None):
    """
    Load KML file to database. Insert if gis_layer_id is None and update if it has value.
    :param file_path:
    :param gis_layer_id:
    :return:
    """

    name, ext = os.path.splitext(file_path)

    if ext == '.kml':
        if not os.path.isfile(file_path):
            print('Error while open file "{}". No such file.'.format(file_path))
            return

        path_json = os.path.dirname(file_path)

        filenames = kml2geojson.convert(file_path, path_json)

        if len(filenames) > 1:
            print('KML file contains {} layers.'.format(len(filenames)))

        full_path_json = '{}/{}'.format(path_json, filenames[0])

    if ext == '.geojson':
        full_path_json = file_path

    if not os.path.isfile(full_path_json):
        print('Error while open file "{}". No such file.'.format(full_path_json))
        return

    json_file = open(full_path_json, "r")
    json_str = json_file.read()

    config = configparser.ConfigParser()
    config.read('config.cfg')

    db = pymysql.connect(config['MYSQL']['MYSQL_DATABASE_HOST'], config['MYSQL']['MYSQL_DATABASE_USER'],
                         config['MYSQL']['MYSQL_DATABASE_PASSWORD'], config['MYSQL']['MYSQL_DATABASE_DB'])

    json_json = json.loads(json_str)

    cursor = db.cursor()

    if gis_layer_id is None:
        sql = 'insert into gis_layer (kml, name, enabled) values (%s, %s, 1)'
        try:
            cursor.execute(sql, (json_str, json_json['name'].replace('_', ' ')))
            db.commit()
            gis_layer_id = cursor.lastrowid
            print('File {} has been loaded.'.format(full_path_json))
        except Exception:
            db.close()
            print('Error while insert {}'.format(full_path_json))
    else:
        sql = 'update gis_layer set kml = %s, enabled = 1 where id = %s'

        try:
            cursor.execute(sql, (json_str, gis_layer_id))
            db.commit()
            print('File {} has been loaded.'.format(full_path_json))
        except Exception:
            db.close()
            print('Error while update {}'.format(full_path_json))

    sql_delete = 'delete from gis_index where gis_layer_id = %s'
    cursor.execute(sql_delete, gis_layer_id)
    db.commit()

    count_index = 0

    for feature in json_json['features']:

        latitude = None
        longitude = None
        guid = None

        if 'guid' in feature['properties']:
            guid = feature['properties']['guid']

        if 'geometry' in feature:
            geometry = feature['geometry']

            if 'coordinates' in geometry:
                if geometry['type'] == 'LineString':
                    latitude = geometry['coordinates'][0][1]
                    longitude = geometry['coordinates'][0][0]
                elif geometry['type'] == 'Polygon':
                    latitude = geometry['coordinates'][0][0][1]
                    longitude = geometry['coordinates'][0][0][0]
                elif geometry['type'] == 'Point':
                    latitude = geometry['coordinates'][1]
                    longitude = geometry['coordinates'][0]
        else:
            latitude = feature['properties']['Широта']
            longitude = feature['properties']['Долгота']

        name = ''

        if 'name' in feature['properties']:
            name = feature['properties']['name']
        elif 'Наименование' in feature['properties']:
            name = feature['properties']['Наименование']
        elif 'description' in feature['properties']:
            name = feature['properties']['description']

        if 'ПС' in feature['properties']:
            name += ' ПС ' + feature['properties']['ПС']

        if 'ВЛ' in feature['properties']:
            name += ' ВЛ ' + feature['properties']['ВЛ']

        if 'Опора' in feature['properties']:
            name += ' Опора ' + feature['properties']['Опора']

        if 'КТП' in feature['properties']:
            name += ' ' + feature['properties']['КТП']

        name = name.strip()
        count_index += 1

        try:
            if name != '' and latitude is not None and longitude is not None and guid is not None:
                sql_insert = 'insert into gis_index (guid,name,gis_layer_id,coordinates) ' \
                             'values (UUID_TO_BIN(%s),%s,%s,Point(%s,%s))'
                cursor.execute(sql_insert, (guid, name.strip(), gis_layer_id, longitude, latitude))

            else:
                print('Error while insert index {} with params "{}" "{}" "{}" "{}" "{}".'.format(count_index, guid,
                                                                                                 name.strip(),
                                                                                                 gis_layer_id,
                                                                                                 longitude, latitude))

        except Exception:
            print('Error while insert index {} with params "{}" "{}" "{}" "{}" "{}".'.format(count_index, guid,
                                                                                             name.strip(), gis_layer_id,
                                                                                             longitude, latitude))

    db.commit()
    db.close()
    print('{} items was added into index.'.format(count_index))


if __name__ == "__main__":

    if len(sys.argv) == 2:
        path_file = sys.argv[1]
        if os.path.isdir(path_file):
            kml_load_directory(path_file)
        elif os.path.isfile(path_file):
            kml_load_file(path_file)
        else:
            print('Error while loading kml file from path {}'.format(path_file))
    elif len(sys.argv) == 3:
        kml_load_file(sys.argv[1], sys.argv[2])
    else:
        print('Load kml file into database.\nkml_load.py <Input file.kml> [<id>]')

