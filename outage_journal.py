import yaml
import datetime


def get_list(db, date_from: datetime = None, date_to: datetime = None):
    """
    Возвращает JSON описание записисей из журнала нарушений outage_journal.yaml
    :return:
    """
    stream = open('outage_journal.yaml', 'r', encoding='utf8')

    if stream:
        json_data = yaml.load(stream, Loader=yaml.SafeLoader)
        if json_data:
            return json_data

    return None
