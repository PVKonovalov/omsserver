"""
    pathfinder helper_uuid.py
    :copyright: (c) 2019 by Pavel Konovalov
    pasha@nettlecom.com
    Created on 2019-03-21
"""

import uuid


def is_valid_uuid(val):
    """
    Validation guid string
    :param val:
    :return:
    """
    if val is None:
        return False

    try:
        uuid.UUID(str(val))
        return True
    except ValueError:
        return False
