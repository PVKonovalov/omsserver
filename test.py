"""
    omsserver test.py
    :copyright: (c) 2019 by Pavel Konovalov
    pasha@nettlecom.com
    Created on 2019-06-07
"""

from scada_gateway import ScadaGateway

scada = ScadaGateway('http://10.5.165.42:8080', 'admin', 'passme')

scada.logon()

obj_json = scada.model_objstruct_cnodes(is_consumer=True)

print(obj_json)
scada.logoff()
