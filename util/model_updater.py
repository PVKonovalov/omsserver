'''
Синхронизация моделей. Обязательное наличие поля guid на первом уровне
В новой есть guid которого нет в старой - добавляем
В новой нет guid который есть в старой - удаляем
Есть оба guid но не совпадает hash - обновляем
Иначе - ничего не делаем с записью старой модели
{
'guid':'',
'data':{},
'action':'u|d|a',
'hash':''
}
'''

import hashlib
import json


def prepare_model(model_raw):
    model = {}

    for item in model_raw:
        guid = item.get('guid')

        if guid is None:
            return None

        model[guid] = {'guid': guid, 'action': '', 'data': item,
                       'hash': hashlib.md5(json.dumps(item, sort_keys=True, indent=2).encode("utf-8")).hexdigest()}

    return model


def sync_models(model_new_raw, model_old_raw):

    model = {}

    model_new = prepare_model(model_new_raw)
    model_old = prepare_model(model_old_raw)

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

