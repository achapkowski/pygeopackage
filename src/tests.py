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
    # ---------------------------------------------------------------------
    def test_add_field(self):
        """tests adding a field on a table"""
        with GeoPackage(path="sample1960s.gpkg") as gpkg:
            tbl = gpkg.create(name="TheSurfaris")
            tbl.add_field(name="WipeOut", data_type="TEXT")
            assert "WipeOut" in tbl.fields.keys()
        os.remove('sample1960s.gpkg')
    # ---------------------------------------------------------------------
    def test_remove_field(self):
        """tests dropping a field on a table"""
        with GeoPackage(path="sample1960s.gpkg") as gpkg:
            tbl = gpkg.create(name="Greenbaum")
            tbl.add_field(name="SpiritInTheSky", data_type="TEXT")
            assert "SpiritInTheSky" in tbl.fields.keys()
            tbl.delete_field(name='SpiritInTheSky')
            assert not "SpiritInTheSky" in tbl.fields.keys()
        os.remove('sample1960s.gpkg')
    # ---------------------------------------------------------------------
    def test_property_attribute_table(self):
        """tests the dtype property on an attribute table"""
        with GeoPackage(path="sample1960s.gpkg") as gpkg:
            tbl = gpkg.create(name="NapoleonXIV")
            assert tbl.dtype == 'attribute'
        os.remove('sample1960s.gpkg')
    # ---------------------------------------------------------------------
    def test_property_spatial_table(self):
        """tests the dtype property on an attribute table"""
        with GeoPackage(path="sample1960s.gpkg") as gpkg:
            tbl = gpkg.create(name="NapoleonXIV", wkid=4326, geometry_type="point")
            assert tbl.dtype == "spatial"
            assert tbl.wkid == 4326
            assert tbl.geometry_type.lower() == "point"
        os.remove('sample1960s.gpkg')
    # ---------------------------------------------------------------------
    def test_rows_table(self):
        """tests the dtype property on an attribute table"""
        data = [
            {'song' : "Midnight Mary", 'artist' : "Joey Powers"},
            {'song' : "What Kind of Fool", 'artist' : "The Murmaids"},
            {'song' : "Hippy Hippy Shake", 'artist' : "The Swinging Blue Jeans"}
        ]
        with GeoPackage(path="sample1960s.gpkg") as gpkg:
            tbl = gpkg.create(name="OneHitWonders", fields={
                "song" : "TEXT",
                "artist" : "TEXT"
            })
            tbl.insert(row=data[0])
            tbl.insert(row=data[1])
            tbl.insert(row=data[2])
            assert len([row for row in tbl.rows()]) == 3
            assert len([row for row in tbl.rows(where="""song = 'Midnight Mary'""")]) == 1
            row = [row for row in tbl.rows(where="""song = 'Midnight Mary'""", fields=['artist'])][0]
            assert row.keys() == ['artist', 'OBJECTID']
            assert row.values() == row.values() == ['Joey Powers', 1]
        os.remove('sample1960s.gpkg')
class TestRowClass(unittest.TestCase):
    """
    Tests the row object
    """
    # ---------------------------------------------------------------------
    def test_rows_table(self):
        """tests the dtype property on an attribute table"""
        data = [
            {'song' : "Midnight Mary", 'artist' : "Joey Powers"},
            {'song' : "What Kind of Fool", 'artist' : "The Murmaids"},
            {'song' : "Hippy Hippy Shake", 'artist' : "The Swinging Blue Jeans"}
        ]
        with GeoPackage(path="sample1960s.gpkg") as gpkg:
            tbl = gpkg.create(name="OneHitWonders", fields={
                "song" : "TEXT",
                "artist" : "TEXT"
            })
            tbl.insert(row=data[0])
            tbl.insert(row=data[1])
            tbl.insert(row=data[2])
            assert len([row for row in tbl.rows()]) == 3
            assert len([row for row in tbl.rows(where="""song = 'Midnight Mary'""")]) == 1
            row = [row for row in tbl.rows(where="""song = 'Midnight Mary'""", fields=['artist'])][0]
            assert row.keys() == ['artist', 'OBJECTID']
            assert row.values() == row.values() == ['Joey Powers', 1]
        os.remove('sample1960s.gpkg')

if __name__ == "__main__":
    TestTableClass().test_rows_table()