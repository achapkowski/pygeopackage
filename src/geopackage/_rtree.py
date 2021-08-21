"""
Defined functions for r-tree index as specified in
https://github.com/opengeospatial/geopackage/blob/master/spec/annexes/extension_spatialindex.adoc
"""
from ._wkb import loads as _loads
from ._wkb import dumps as _dumps

try:
    from geomet.wkb import loads as _geomet_loads
    from geomet.wkb import dumps as _geomet_dumps
except:
    pass
# --------------------------------------------------------------------------
def _strip_header(g):
    """removes the GP header"""
    if g[:2] == b"GB":
        return g[8:]
    return g


def _flatten_list(coords):
    """moves all x,y pairs into two lists"""
    minx, maxx = None, None
    miny, maxy = None, None
    for prt in coords:
        for xy in prt:
            if miny is None and maxy is None:
                miny = xy[1]
                maxy = xy[1]
            if minx is None and maxx is None:
                minx = xy[0]
                maxx = xy[0]

    return [], []


def _extent(g, t="xmin"):
    """gets the extent value from the geometry"""
    geom = _strip_header(geom)
    j = _dumps(geom)
    if t == "xmin":
        if "x" in j:
            return j["x"]
        elif "rings" in j:

            return

    elif "y" in j:
        return j["y"]


# --------------------------------------------------------------------------
def ST_IsEmpty(geom):
    """
    returns 1 if geometry value is empoty 0 if not empty, NULL if geometry is NULL
    """
    if geom is None:
        return None
    geom = _strip_header(geom)
    if geom == b"0x000000000000f87f":
        return 1
    return 0


# --------------------------------------------------------------------------
def ST_MinX(geom):
    """
    Returns the minimum X value of the bounding envelope of a geometry
    """
    geom = _strip_header(geom)
    j = _dumps(geom)

    return


# --------------------------------------------------------------------------
def ST_MinY(geom):
    """
    Returns the minimum Y value of the bounding envelope of a geometry
    """
    geom = _strip_header(geom)
    return


# --------------------------------------------------------------------------
def ST_MaxX(geom):
    """
    Returns the maximum X value of the bounding envelope of a geometry
    """
    geom = _strip_header(geom)
    return


# --------------------------------------------------------------------------
def ST_MaxY(geom):
    """
    Returns the maximum Y value of the bounding envelope of a geometry
    """
    geom = _strip_header(geom)
    return
