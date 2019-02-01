"""

Projection Information Source: https://developers.arcgis.com/rest/services-reference/projected-coordinate-systems.htm
Geographic Projection Source:
"""
import os
import json
import pandas as pd

__all__ = ['lookup_coordinate_system', 'info']
#----------------------------------------------------------------------
_lutbl = None
#----------------------------------------------------------------------
def _load_data():
    """loads the coordinate information into memory"""

    _fp = r"%s\prj.json" % os.path.dirname(__file__)
    global _lutbl
    if _lutbl is None:
        with open(_fp, 'r') as reader:
            _lutbl = pd.DataFrame(json.loads(reader.read()))
            _lutbl.columns = ['WKID', 'NAME', 'WKT']
            del reader
    return _lutbl
#----------------------------------------------------------------------
def lookup_coordinate_system(wkid):
    """
    Looks up the wkt and name of the wkid coordinate system

    ================     =========================================
    ** Arguement **      ** Description **
    ----------------     -----------------------------------------
    wkid                 Requred Integer/List. The SRS identifier.
    ================     =========================================

    :returns: list of dictionaries

    [
      {
         'WKID': 2000,
         'NAME': 'Anguilla_1957_British_West_Indies_Grid',
         'WKT': 'PROJCS["Anguilla_1957_British_West_Indies_Grid",GEOGCS["GCS_Anguilla_1957",DATUM["D_Anguilla_1957",SPHEROID["Clarke_1880_RGS",6378249.145,293.465]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Transverse_Mercator"],PARAMETER["False_Easting",400000.0],PARAMETER["False_Northing",0.0],PARAMETER["Central_Meridian",-62.0],PARAMETER["Scale_Factor",0.9995],PARAMETER["Latitude_Of_Origin",0.0],UNIT["Meter",1.0]]'
      }
    ]


    """
    if wkid == 102100:
        wkid = 3857
    global _lutbl
    if _lutbl is None:
        _load_data()
    if isinstance(wkid, (int, float)):
        q = _lutbl['WKID'] == int(wkid)
    elif isinstance(wkid, (list, tuple)):
        q = _lutbl['WKID'].isin(wkid)
    else:
        raise ValueError("Invalid wkid. Must be int or list.")
    if len(_lutbl[q]) == 0:
        raise ValueError("Invalid WKID")
    return _lutbl[q].to_dict('records')
#----------------------------------------------------------------------
def info():
    """Returns all the support projections

    :returns: pd.DataFrame
    """
    global _lutbl
    if _lutbl is None:
        _lutbl = _load_data()
    return _lutbl
