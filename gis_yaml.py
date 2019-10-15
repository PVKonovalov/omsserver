"""
    omsserver gis_style.py
    :copyright: (c) 2019 by Pavel Konovalov
    pasha@nettlecom.com
    Created on 2019-08-02
"""
import yaml
import customer


def get_kml_style():
    """
    Возвращает JSON описание стилей KML объектов из файла gis_style.yaml
    :return:
    """
    stream = open('gis_kml_style.yaml', 'r', encoding='utf8')

    if stream:
        json_data = yaml.load(stream, Loader=yaml.SafeLoader)
        if json_data:
            return json_data

    return None


def get_map_legend():
    """
    Возвращает JSON описание легенды карты из файла gis_map_legend.yaml
    :return:
    """
    stream = open('gis_map_legend.yaml', 'r', encoding='utf8')

    if stream:
        json_data = yaml.load(stream, Loader=yaml.SafeLoader)
        if json_data:
            return json_data

    return None


def get_extra_state(db, guid, layer=None):
    """
    Возвращает JSON описание легенды карты из файла gis_map_legend.yaml
    :return:
    """

    extra_state = None
    json_item = {}

    if layer:
        # json_item['Слой'] = layer
        if layer == 122:
            extra_state = customer.get_extra_state(db, guid)
            json_item.update(extra_state)

    stream = open('extra_state.yaml', 'r', encoding='utf8')

    if stream:
        json_data = yaml.load(stream, Loader=yaml.SafeLoader)
        json_item_yaml = json_data.get(guid)
        if json_item_yaml:
            json_item.update(json_item_yaml)

    return json_item
