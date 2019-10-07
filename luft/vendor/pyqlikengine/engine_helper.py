# -*- coding: utf-8 -*-
"""Qlik Engine."""

import math
from typing import Any, Dict, List, Union

from luft.vendor.pyqlikengine.engine_app_api import EngineAppApi
from luft.vendor.pyqlikengine.engine_field_api import EngineFieldApi
from luft.vendor.pyqlikengine.engine_generic_object_api import EngineGenericObjectApi
from luft.vendor.pyqlikengine.engine_global_api import EngineGlobalApi
from luft.vendor.pyqlikengine.structs import Structs


def get_hypercube_data(connection: object, app_handle: int,
                       measures: Union[List[Dict[str, str]], None] = None,
                       dimensions: Union[List[str], None] = None,
                       selections: Union[Dict[str, List[Any]]] = None,
                       date_valid: str = None):
    """Get data from Qlik App in json format."""
    mes_width = len(measures) if measures else 0
    dim_width = len(dimensions) if dimensions else 0

    ega = EngineGlobalApi(connection)
    # Define Dimensions of hypercube
    dimensions = dimensions or []
    hc_inline_dim = Structs.nx_inline_dimension_def(dimensions)

    # Set sorting of Dimension by Measure
    hc_mes_sort = Structs.nx_sort_by()

    # Build hypercube from above definition
    hc_dim = Structs.nx_hypercube_dimensions(hc_inline_dim)
    meas_ids = [mea.get('id') for mea in (measures or [])]
    hc_mes = Structs.nx_hypercube_measure_ids(hc_mes_sort, meas_ids)

    width = mes_width + dim_width
    height = int(math.floor(10000 / width))
    nx_page = Structs.nx_page(0, 0, height, width)
    hc_def = Structs.hypercube_def('$', hc_dim, hc_mes, [nx_page])

    eaa = EngineAppApi(connection)
    app_layout = eaa.get_app_layout(app_handle).get('qLayout')
    hc_response = eaa.create_object(
        app_handle, 'CH01', 'Chart', 'qHyperCubeDef', hc_def)
    hc_handle = ega.get_handle(hc_response)

    egoa = EngineGenericObjectApi(connection)

    efa = EngineFieldApi(connection)

    if selections:
        for field in selections.keys():
            field_handle = ega.get_handle(
                eaa.get_field(app_handle, field))
            values: List[Dict[str, Any]] = []
            for select_value in selections[field]:
                if isinstance(select_value, str):
                    values.append({'qText': select_value})
                else:
                    values.append(
                        {'qIsNumeric': True, 'qNumber': select_value})

            efa.select_values(field_handle, values)

    i = 0
    while i % height == 0:
        nx_page = Structs.nx_page(i, 0, height, width)
        hc_data = egoa.get_hypercube_data(
            hc_handle, '/qHyperCubeDef', [nx_page])
        elems = hc_data['qDataPages'][0]['qMatrix']

        results = []

        for elem in elems:
            j = 0
            dim_dict = {}
            for dim in (dimensions or []):
                if 'qText' in elem[j].keys():
                    dim_dict[dim.lower()] = elem[j]['qText']
                else:
                    dim_dict[dim.lower()] = None
                j += 1
            for meas in (measures or []):
                result = {}
                result['date_valid'] = date_valid
                result['app_id'] = app_layout.get('qFileName')
                result['app_name'] = app_layout.get('qTitle')
                result['app_stream_id'] = app_layout.get('stream').get('id')
                result['app_stream_name'] = app_layout.get(
                    'stream').get('name')
                result['dimensions'] = dim_dict
                result['selections'] = selections
                result['measure_id'] = meas.get('id')
                result['measure_name'] = meas.get('name')
                if 'qNum' in elem[j].keys() and not elem[j].get('qIsNull'):
                    result['measure_value'] = elem[j]['qNum']
                else:
                    result['measure_value'] = None
                results.append(result)
                j += 1
            i += 1

    return results
