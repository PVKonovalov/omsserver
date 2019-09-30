"""
    omsserver gis_style.py
    :copyright: (c) 2019 by Pavel Konovalov
    pasha@nettlecom.com
    Created on 2019-08-02
"""
import yaml


def get_list():
    """
    Возвращает JSON описание стилей KML объектов из файла gis_style.yaml
    :return:
    """
    stream = open('gis_kml_style.yaml', 'r', encoding='utf8')

    if stream:
        gis_style = yaml.load(stream, Loader=yaml.SafeLoader)
        if gis_style:
            return gis_style

    return None
