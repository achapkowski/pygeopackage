"""
PyTests for validating the package.
"""

import os
import unittest
import pytest

import geopackage
from geopackage import GeoPackage


class TestGeoPackageClass(unittest.TestCase):
    """
    Tests the geopackage level operations
    """
    def test_with_context(self):
        with GeoPackage(path="sample1.gpkg") as gpkg:
            assert gpkg._con
        os.remove('sample1.gpkg')
    def test_create_overwrite(self):
        """tests creation gpkg patterns"""
        gpkg = GeoPackage("sample1.gpkg")
        del gpkg
        gpkg = GeoPackage("sample1.gpkg", True)
        del gpkg
        gpkg = GeoPackage("sample1.gpkg", False)
        del gpkg
        os.remove('sample1.gpkg')
    def test_length(self):
        with GeoPackage(path="sample1.gpkg") as gpkg:
            assert len(gpkg) == 0
            gpkg.create(name='TommyTutone')
            assert len(gpkg) == 1
        os.remove('sample1.gpkg')
    def test_gpkg_create_table(self):
        """tests creating attribute tables"""
        with GeoPackage(path="sample1.gpkg", overwrite=True) as gpkg:
            from geopackage._geopackage import Table
            tbl = gpkg.create(name='TwistedSister')
            assert isinstance(tbl, Table)
        os.remove('sample1.gpkg')
    def test_gpkg_create_spatial_table(self):
        """tests creating spatial tables"""
        with GeoPackage(path="sample1.gpkg", overwrite=True) as gpkg:
            from geopackage._geopackage import SpatialTable
            tbl = gpkg.create(name='MenWithoutHats', wkid=4326, geometry_type='polygon')
            assert isinstance(tbl, SpatialTable)
        os.remove('sample1.gpkg')
    def test_exists(self):
        """tests the table exists function"""
        with GeoPackage(path="sample1.gpkg", overwrite=True) as gpkg:
            assert gpkg.exists("Maroon5") == False # not an 80s band
            gpkg.create(name='TommyTutone')
            assert gpkg.exists("TommyTutone") == True
        os.remove('sample1.gpkg')
    def test_list_tables(self):
        """tests listing tables"""
        with GeoPackage(path="sample1.gpkg") as gpkg:
            assert len(gpkg) == 0
            gpkg.create(name='TommyTutone')
            gpkg.create(name='Timbuk3', wkid=4326, geometry_type="point")
            assert len(gpkg) == 2
            assert len([tbl for tbl in gpkg.tables]) == 2
        os.remove('sample1.gpkg')
    def test_get_tables(self):
        """tests listing tables"""
        with GeoPackage(path="sample1.gpkg", overwrite=True) as gpkg:
            assert len(gpkg) == 0
            gpkg.create(name='TommyTutone')
            gpkg.create(name='Timbuk3', wkid=4326, geometry_type="point")
            gpkg.create(name='Kajagoogoo', wkid=4326, geometry_type="multipoint")
            gpkg.create(name='DeborahAllen', wkid=4326, geometry_type="polygon")
            gpkg.create(name='ThomasDolby', wkid=4326, geometry_type="line")
            assert gpkg.get("TommyTutone")
            assert gpkg.get("Chumbawamba") is None # 90s Band, I SHOULD NOT EXIST
        os.remove('sample1.gpkg')
    def test_create_table_fields_test(self):
        """
        Creates a tables with multiple table field combinations

        """
        fields={
            "txtfld":"TEXT",
            'field1' : "BLOB",
            'field2' : "FLOAT",
            'field3' : "DOUBLE",
            "field4" : "SHORT",
            "field5" : "DATE",
            "field6" : "GUID"
        }
        with GeoPackage(path="sample1.gpkg", overwrite=True) as gpkg:
            assert len(gpkg) == 0
            tbl1 = gpkg.create(name='JackWagner')
            tbl2 = gpkg.create(name='Devo',
                        fields=fields
            )
            assert len(tbl1.fields) == 1 #  1 accounts for OBJECTID
            assert len(tbl2.fields) == (len(fields) + 1) # +1 accounts for OBJECTID
        os.remove('sample1.gpkg')
    def test_create_spatial_table_fields_test(self):
        """
        Creates a spatial tables with multiple table field combinations

        """
        fields={
            "txtfld":"TEXT",
            'field1' : "BLOB",
            'field2' : "FLOAT",
            'field3' : "DOUBLE",
            "field4" : "SHORT",
            "field5" : "DATE",
            "field6" : "GUID"
        }
        with GeoPackage(path="sample1.gpkg", overwrite=True) as gpkg:
            assert len(gpkg) == 0
            tbl1 = gpkg.create(name='Quarterflash', wkid=2351, geometry_type="point")
            tbl2 = gpkg.create(name='TheWeatherGirls',
                               fields=fields,
                               wkid=4768,
                               geometry_type="line"
            )
            assert len(tbl1.fields) == 2 #  2 (OBJECTID and SHAPE COLUMN)
            assert len(tbl2.fields) == (len(fields) + 2)
        os.remove('sample1.gpkg')


class TestTableClass(unittest.TestCase):
    """
    Tests the geopackage table/spatial table level operations
    """
    #todo: clean up and add the tests
    pass
class TestRowClass(unittest.TestCase):
    """
    Tests the row object
    """
    #todo: clean up and add the tests
    pass

