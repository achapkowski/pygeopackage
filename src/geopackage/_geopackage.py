import os
import sys
import json
import sqlite3
from typing import Generator
from typing import Union, Any, Dict
import tempfile
from io import BytesIO, StringIO
from sqlite3 import Binary as sBinary
from collections import OrderedDict, MutableMapping

try:
    import geomet
    from geomet import wkt as geometwkt
    from geomet import wkb as geometwkb

    _HASGEOMET = True
except ImportError:
    _HASGEOMET = False

from ._gpkg import _create_feature_class, _create_gpkg, _create_table, _insert_values
from ._wkb import loads, dumps

# ----------------------------------------------------------------------
def _handle_wkb(wkb):
    """handles the insert convertion for the custom geometry types"""
    if isinstance(wkb, bytes):
        wkb = bytearray(wkb)
    return wkb


# ----------------------------------------------------------------------
def _adapt_wkb(wkb):
    """ensures the wkb values are bytes, not bytearrays"""
    return wkb


########################################################################
class GeoPackage(object):
    """
    A single instance of a GeoPackage file.
    """

    _con = None
    _dir = None
    _path = None
    _db_name = None

    # ----------------------------------------------------------------------
    def __init__(self, path, overwrite=False):
        """Constructor"""

        self._dir = os.path.dirname(path)
        self._db_name = os.path.basename(path)

        if self._db_name.lower().endswith(".gpkg") == False:
            self._db_name += ".gpkg"
        self._path = os.path.join(self._dir, self._db_name)
        if os.path.isfile(self._path) and overwrite:
            os.remove(self._path)
        self._path = _create_gpkg(
            name=self._db_name, path=self._dir, overwrite=overwrite
        )
        self._con = sqlite3.connect(self._path, detect_types=sqlite3.PARSE_DECLTYPES)
        # register custom dtypes
        sqlite3.register_adapter(bytearray, _adapt_wkb)
        for g in [
            "POINT",
            "LINESTRING",
            "POLYGON",
            "MULTIPOINT",
            "MULTILINESTRING",
            "MULTIPOLYGON",
        ]:
            sqlite3.register_converter(g, _handle_wkb)

    # ----------------------------------------------------------------------
    def _setup(self):
        """sets up the registration for the GeoPackage"""
        if self._path != ":memory:":
            self._con.close()
            self._con = None
            self._con = sqlite3.connect(
                self._path, detect_types=sqlite3.PARSE_DECLTYPES
            )
        # register custom dtypes
        sqlite3.register_adapter(bytearray, _adapt_wkb)
        for g in [
            "POINT",
            "LINESTRING",
            "POLYGON",
            "MULTIPOINT",
            "MULTILINESTRING",
            "MULTIPOLYGON",
        ]:
            sqlite3.register_converter(g, _handle_wkb)

    # ----------------------------------------------------------------------
    def __len__(self):
        """returns the number of registered tables"""
        try:
            sql = """SELECT count(*) FROM gpkg_contents"""
            cur = self._con.execute(sql)
            return cur.fetchone()[0]
        except:
            return 0

    # ----------------------------------------------------------------------
    def __enter__(self) -> "GeoPackage":
        if self._con is None:
            self._con = sqlite3.connect(self._path)
        return self

    # ----------------------------------------------------------------------
    def __exit__(self, type, value, traceback):
        self._con.commit()
        self._con.close()

    # ----------------------------------------------------------------------
    def exists(self, name: str) -> bool:
        """
        Returns boolean if the table exists

        :returns: boolean

        """
        sql = "SELECT table_name, data_type FROM gpkg_contents"
        cur = self._con.execute(sql)
        for tbl in cur:
            if tbl[0].lower() == name.lower():
                return True
        return False

    # ----------------------------------------------------------------------
    def get(self, name: str) -> Union["Table", "SpatialTable"]:
        """
        Returns a table if it exists in the geopackage.

        ===============     ===============================================
        **Arguements**      **Description**
        ---------------     -----------------------------------------------
        name                Optional String. The name of the table or
                            feature class.
        ===============     ===============================================

        :returns: Table/SpatialTable

        """
        sql = "SELECT table_name, data_type FROM gpkg_contents where table_name = ?"
        cur = self._con.execute(sql, [name])
        for tbl in cur:
            if tbl[1] == "attributes":
                return Table(tbl[0], self._con)
            else:
                return SpatialTable(tbl[0], self._con)

    # ----------------------------------------------------------------------
    @property
    def tables(self) -> Generator["Table", "SpatialTable"]:
        """
        Gets a list of registered table names with the geopackage

        :returns: iterator

        """
        sql = "SELECT table_name, data_type FROM gpkg_contents"
        cur = self._con.execute(sql)
        for tbl in cur:
            if tbl[1] == "attributes":
                yield Table(tbl[0], self._con)
            else:
                yield SpatialTable(tbl[0], self._con)

    # ----------------------------------------------------------------------
    def enable(self) -> bool:
        """
        enables the sqlite database to be a geopackage

        :returns: Boolean

        """
        try:
            self._setup()
        except:
            return False
        return True

    # ----------------------------------------------------------------------
    def create(
        self,
        name: str,
        fields: Dict[str, str] = None,
        wkid: int = None,
        geometry_type: str = None,
        overwrite: bool = True,
    ) -> Union["Table", "SpatialTable"]:
        """
        The `create` method generates a new table or feature class in the
        geopackage.

        ===============     ===============================================
        **Arguements**      **Description**
        ---------------     -----------------------------------------------
        name                Optional String. The name of the table or
                            feature class.
        ---------------     -----------------------------------------------
        fields              Optional dict.  The columns to add to a table.
                            An OBJECTID field is always created for any table.

                            Allowed Fields:

                            + TEXT -Any string of characters.
                            + FLOAT - Fractional numbers between -3.4E38 and 1.2E38.
                            + DOUBLE - Fractional numbers between -2.2E308 and 1.8E308.
                            + SHORT - Whole numbers between -32,768 and 32,767.
                            + LONG - Whole numbers between -2,147,483,648 and 2,147,483,647.
                            + DATE -Date and/or time.
                            + BLOB -Long sequence of binary numbers.
                            + GUID -Globally unique identifier.

        ---------------     -----------------------------------------------
        wkid                Optional Int. The SRS code for the feature class.
        ---------------     -----------------------------------------------
        geometry_type       Optional String. If given the output will be a
                            SpatialTable instead of a Table. Allowed values
                            are: point, line, polygon, and multipoint.
        ---------------     -----------------------------------------------
        overwrite           Optional Boolean. If True, the geopackage will
                            attempt to erase the table and recreate it with
                            the new schema.  All records from the old table
                            will be lost.
        ===============     ===============================================

        :returns: Table/SpatialTable
        """
        if overwrite:
            sql_drop = """DROP TABLE IF EXISTS %s""" % name
            sql_delete_row = """DELETE FROM gpkg_contents where table_name = '{tbl}'""".format(
                tbl=name
            )
            sql_geom_col = """DELETE FROM gpkg_geometry_columns where table_name = '{tbl}'""".format(
                tbl=name
            )
            self._con.execute(sql_drop)
            self._con.execute(sql_delete_row)
            self._con.execute(sql_geom_col)
            self._con.commit()
        elif self.exists(name) and overwrite == False:
            raise ValueError(
                "Table %s exists. Please pick a different table name" % name
            )
        if geometry_type:
            iftrue = _create_feature_class(
                con=self._con,
                name=name,
                wkid=wkid,
                fields=fields,
                geometry=geometry_type,
            )
            if iftrue:
                return SpatialTable(table=name, con=self._con)

        else:
            iftrue = _create_table(con=self._con, name=name, fields=fields)
            if iftrue:
                return Table(table=name, con=self._con)

        return


########################################################################
class _Row(MutableMapping, OrderedDict):
    """
    A Single Row Entry. This class is created by the `Table` class.

    ** It should not be created by a user. **
    """

    _con = None
    _values = None
    _table_name = None
    _dict = None
    _keys = None
    # ----------------------------------------------------------------------
    def __init__(self, values, table_name=None, con=None, header=None):
        """Constructor"""
        self._table_name = table_name
        self._con = con
        self._values = values
        self._header = header

    # ----------------------------------------------------------------------
    def __str__(self):
        return "<Row OBJECTID=%s>" % self["OBJECTID"]

    # ----------------------------------------------------------------------
    def __repr__(self):
        return self.__str__()

    # ----------------------------------------------------------------------
    def __setattr__(self, name, value):
        if name in {"_values", "_dict", "_table_name", "_con", "_keys", "_header"}:
            super().__setattr__(name, value)
        elif name.lower() == "shape":
            self._values[name] = value
            self._update()
        elif name.lower() != "objectid" and name in self.keys():
            self._values[name] = value
            self._update()

        elif name.lower() == "objectid":
            raise ValueError("OBJECTID values cannot be updated.")
        else:
            raise ValueError("The field: {field} does not exist.".format(field=name))

    # ----------------------------------------------------------------------
    def __getattr__(self, name):
        if name in self._values:
            return self._values[name]
        return

    # ----------------------------------------------------------------------
    def __getitem__(self, name):
        return self.__getattr__(name)

    # ----------------------------------------------------------------------
    def __setitem__(self, name, value):
        self.__setattr__(name, value)

    # ----------------------------------------------------------------------
    def keys(self):
        """returns the column names in the dataset"""
        return list(self._values.keys())

    # ----------------------------------------------------------------------
    @property
    def fields(self):
        """returns the field names in the dataset"""
        return self.keys()

    # ----------------------------------------------------------------------
    def as_dict(self):
        """returns the row as a dictionary"""
        return dict(zip(self.keys(), self.values()))

    # ----------------------------------------------------------------------
    def values(self):
        """returns the row values"""
        return list(self._values.values())

    # ----------------------------------------------------------------------
    def _update(self):
        """updates the current row"""
        txts = []
        values = []
        for k, v in self._values.items():
            if k.lower() != "objectid" and k.lower() != "shape":
                txts.append("%s=?" % k)
                values.append(v)
            elif k.lower() == "shape":
                if isinstance(v, dict) and "coordinates" not in v:
                    v = self._header + dumps(v, False)
                elif isinstance(v, dict) and "coordinates" in v:
                    v = self._gpheader + geometwkb.dumps(obj=v)
                elif isinstance(v, str):
                    gj = geometwkt.loads(v)
                    v = self._gpheader + geometwkb.dumps(obj=gj)
                elif isinstance(v, (bytes, bytearray)):
                    if isinstance(v, (bytearray)):
                        v = bytes(v)
                    if len(v) > 2 and v[:2] != b"GB":
                        v = self._gpheader + v
                elif v is None:
                    v = self._gpheader + b"0x000000000000f87f"
                else:
                    raise ValueError(
                        (
                            "Shape column must be Esri JSON dictionary, "
                            "WKT, GeoJSON dictionary, or WKB (bytes)"
                        )
                    )
                txts.append("%s=?" % k)
                values.append(v)
        sql = """UPDATE {table} SET {values} WHERE OBJECTID={oid}""".format(
            table=self._table_name, values=",".join(txts), oid=self._values["OBJECTID"]
        )
        cur = self._con.execute(sql, values)
        self._con.commit()
        del sql
        del values, txts
        return True

    # ----------------------------------------------------------------------
    def delete(self):
        """
        Deletes the current row

        :returns: Boolean
        """
        try:
            cur = self._con.execute(
                """DELETE FROM {tbl} WHERE OBJECTID=?""".format(tbl=self._table_name),
                [self["OBJECTID"]],
            )
            return True
        except:
            return False


########################################################################
class Table(object):
    """
    A Table object is a attribute only set of data. No spatial data is associated with
    this information.


    ===============     ===============================================
    **Arguements**      **Description**
    ---------------     -----------------------------------------------
    table               Requred String. The name of the table.
    ---------------     -----------------------------------------------
    con                 Required sqlite3.Connection.  The active connection
                        to the geopackage.
    ===============     ===============================================

    """

    _con = None
    _table_name = None
    _create_sql = None
    _fields = None
    # ----------------------------------------------------------------------
    def __init__(self, table, con):
        """Constructor"""
        self._con = con
        self._table_name = table

    # ----------------------------------------------------------------------
    @property
    def dtype(self):
        """returns the table type"""
        return "attribute"

    # ----------------------------------------------------------------------
    def __str__(self):
        return "<Attribute Table: {table}>".format(table=self._table_name)

    # ----------------------------------------------------------------------
    def __repr__(self):
        return self.__str__()

    # ----------------------------------------------------------------------
    def __iter__(self):
        for row in self.rows():
            yield row

    # ----------------------------------------------------------------------
    @property
    def fields(self):
        """
        returns the field information for a table

        :returns: Dictionary

        """
        if self._fields is None:
            sql = """PRAGMA table_info({tbl});""".format(tbl=self._table_name)
            rows = self._con.execute(sql).fetchall()
            self._fields = {row[1]: row[2] for row in rows}
        return self._fields

    # ----------------------------------------------------------------------
    def add_field(self, name, data_type):
        """

        Adds a new column to the table.

        ===============     ===============================================
        **Arguements**      **Description**
        ---------------     -----------------------------------------------
        name                Required String. The name of the field.
        ---------------     -----------------------------------------------
        data_types          Required String.  The type of column to add.

                            Allowed Data Types:

                            + TEXT -Any string of characters.
                            + FLOAT - Fractional numbers between -3.4E38 and 1.2E38.
                            + DOUBLE - Fractional numbers between -2.2E308 and 1.8E308.
                            + SHORT - Whole numbers between -32,768 and 32,767.
                            + LONG - Whole numbers between -2,147,483,648 and 2,147,483,647.
                            + DATE -Date and/or time.
                            + BLOB -Long sequence of binary numbers.
                            + GUID -Globally unique identifier.
        ===============     ===============================================

        :returns: Boolean

        """
        _field_lookup = {
            "text": [
                "TEXT",
                "check((typeof({field}) = 'text' or typeof({field}) = 'null') and not length({field}) > {l})",
            ],
            "float": [
                "DOUBLE",
                """check(typeof({field}) = 'real' or typeof({field}) = 'null')""",
            ],
            "double": [
                "DOUBLE",
                "check(typeof({field}) = 'real' or typeof({field}) = 'null')",
            ],
            "short": [
                "SMALLINT",
                "check((typeof({field}) = 'integer' or typeof({field}) = 'null') and {field} >= -32768 and {field} <= 32767)",
            ],
            "long": [
                "MEDIUMINT",
                "check((typeof({field}) = 'integer' or typeof({field}) = 'null') and {field} >= -2147483648 and {field} <= 2147483647)",
            ],
            "integer": [
                "MEDIUMINT",
                "check((typeof({field}) = 'integer' or typeof({field}) = 'null') and {field} >= -2147483648 and {field} <= 2147483647)",
            ],
            "date": [
                "DATETIME",
                "check((typeof({field}) = 'text' or typeof({field}) = 'null') and strftime('%Y-%m-%dT%H:%M:%fZ',{field}))",
            ],
            "blob": [
                "BLOB",
                "check(typeof({field}) = 'blob' or typeof({field}) = 'null')",
            ],
            "guid": [
                "TEXT(38)",
                "check((typeof({field}) = 'text' or typeof({field}) = 'null') and not length({field}) > 38)",
            ],
        }
        try:
            row = _field_lookup[data_type.lower()]
            if row[0].lower() != "text":
                fld = "{field} {dtype} {st}".format(
                    field=name, dtype=row[0], st=row[1]
                ).format(field=name)
            else:
                fld = "{field} {dtype}".format(field=name, dtype=row[0])
            sql = """ALTER TABLE {table} ADD COLUMN {dtype};""".format(
                table=self._table_name, field=name, dtype=fld
            )
            self._con.execute(sql)
            self._con.commit()
            self._fields = None
            return True
        except:
            return False

    # ----------------------------------------------------------------------
    def delete_field(self, name):
        """
        Drops a Field from a Table

        ===============     ===============================================
        **Arguements**      **Description**
        ---------------     -----------------------------------------------
        name                Required String. The name of the field to remove.
        ===============     ===============================================

        :returns: boolean
        """
        fields = ",".join([fld for fld in self.fields if fld.lower() != name.lower()])
        sql = """
        CREATE TABLE temp_bkup AS SELECT {fields} FROM {table};
        DROP TABLE {table};
        ALTER TABLE temp_bkup RENAME TO {table};
        """.format(
            table=self._table_name, fields=fields
        )
        self._con.executescript(sql)
        self._con.commit()
        self._fields = None
        return True

    # ----------------------------------------------------------------------
    def rows(self, where=None, fields="*"):
        """
        Search/update cursor like iterator

        ===============     ===============================================
        **Arguements**      **Description**
        ---------------     -----------------------------------------------
        where               Optional String. Optional Sql where clause.
        ---------------     -----------------------------------------------
        fields              Optional List. The default is all fields (*).
                            A list of fields can be provided to limit the
                            data that is returned.
        ===============     ===============================================

        :returns: _Row object
        """
        if isinstance(fields, (list, tuple)):
            if "OBJECTID" not in fields:
                fields.append("OBJECTID")
            fields = ",".join(fields)
        if where is None:
            query = """SELECT {fields} from {tbl} """.format(
                tbl=self._table_name, fields=fields
            )
        else:
            query = """SELECT {fields} from {tbl} WHERE {where}""".format(
                tbl=self._table_name, fields=fields, where=where
            )
        cursor = self._con.cursor()
        c = cursor.execute(query)
        columns = [d[0] for d in c.description]
        for row in c:
            yield _Row(
                values=dict(zip(columns, row)),
                table_name=self._table_name,
                con=self._con,
            )

    # ----------------------------------------------------------------------
    def insert(self, row):
        """
        Inserts a new row via dictionary

        ===============     ===============================================
        **Arguements**      **Description**
        ---------------     -----------------------------------------------
        row                 Required Dictionary.  Insert a new row via a
                            dictionary. The key/value pair must match up to
                            the field names in the table.
        ==============      ===============================================

        :returns: Boolean

        """
        values = None
        if isinstance(row, dict):
            keys = row.keys()
            values = [list(row.values())]
        elif isinstance(row, (list, tuple)):
            keys = row[0].keys()
            values = [list(r.values()) for r in row]
        q = ["?"] * len(keys)
        q = ",".join(q)
        sql = """INSERT INTO {table} ({fields})
                     VALUES({q})""".format(
            table=self._table_name, fields=",".join(keys), q=q
        )
        inserts = []
        cur = self._con.cursor()
        cur.execute(sql, list(values[0]))
        self._con.commit()
        return True

    # ----------------------------------------------------------------------
    def to_pandas(self, where=None, fields="*", ftype=None):
        """
        Exports a table to a Pandas' DataFrame.

        ===============     ===============================================
        **Arguements**      **Description**
        ---------------     -----------------------------------------------
        where               Optional String. Optional Sql where clause.
        ---------------     -----------------------------------------------
        fields              Optional List. The default is all fields (*).
                            A list of fields can be provided to limit the
                            data that is returned.
        ---------------     -----------------------------------------------
        ftype               Optional String. This value can be dataframe
                            format type.  The value can be None or esri.

                                + None - means the dataframe will be a raw view of the table.
                                + esri - means the dataframe will be a spatially enable dataframe. (Requires Python API for ArcGIS)

        ===============     ===============================================

        :returns: pd.DataFrame

        """
        import pandas as pd

        if isinstance(fields, (list, tuple)):
            if "OBJECTID" not in fields:
                fields.append("OBJECTID")
            fields = ",".join(fields)
        if where is None:
            query = """SELECT {fields} from {tbl} """.format(
                tbl=self._table_name, fields=fields
            )
        else:
            query = """SELECT {fields} from {tbl} WHERE {where}""".format(
                tbl=self._table_name, fields=fields, where=where
            )
        if ftype is None:
            return pd.read_sql_query(query, self._con)
        elif str(ftype).lower() == "esri":
            try:
                from arcgis.geometry import Geometry
                from arcgis.features import GeoAccessor, GeoSeriesAccessor

                df = pd.read_sql_query(query, self._con)
                fields = list(self.fields.keys())
                lower_fields = [str(fld).lower() for fld in fields]
                if "shape" in lower_fields:
                    idx = lower_fields.index("shape")
                    SHAPE = fields[idx]
                    df[SHAPE] = df[SHAPE].apply(lambda x: Geometry(x[8:]))
                    df.spatial.set_geometry(SHAPE)
                    try:
                        df.spatial.project(self.wkid)
                    except:
                        print("nope")
                return df
            except ImportError:
                raise Exception(
                    "The Python API for ArcGIS is required to import using ftype `esri`"
                )
            except Exception as e:
                raise Exception(e)


########################################################################
class SpatialTable(Table):
    """
    Represents a Feature Class inside a GeoPackage

    """

    _con = None
    _wkid = None
    _gtype = None
    _fields = None
    _table_name = None
    _create_sql = None
    _gp_header = None
    # ----------------------------------------------------------------------
    def __init__(self, table, con):
        """Constructor"""
        self._table_name = table
        self._con = con
        self._refresh()

    # ----------------------------------------------------------------------
    def _refresh(self):
        """internal method that refreshes the table information"""
        self._sd_lu = (
            """SELECT * from gpkg_geometry_columns where table_name = '%s'"""
            % self._table_name
        )
        cur = self._con.execute(self._sd_lu)
        self._gp_header = None

        for row in cur:
            self._gtype = row[2]
            self._wkid = row[3]
            break
        del cur

    # ----------------------------------------------------------------------
    def __str__(self):
        return "<Spatial Table: {table}, {gt}>".format(
            table=self._table_name, gt=self._gtype
        )

    # ----------------------------------------------------------------------
    def __repr__(self):
        return self.__str__()

    # ----------------------------------------------------------------------
    @property
    def geometry_type(self):
        if self._gtype is None:
            self._refresh()
        return self._gtype

    # ----------------------------------------------------------------------
    @property
    def dtype(self):
        """
        Returns the table type

        :returns: String

        """
        return "spatial"

    # ----------------------------------------------------------------------
    @property
    def wkid(self):
        """
        Returns the Spatial Table's WKID/SRS ID

        :returns: Integer
        """
        if self._wkid is None:
            self._refresh()
        return self._wkid

    # ----------------------------------------------------------------------
    def rows(self, where=None, fields="*"):
        """
        Search/update cursor like iterator

        ===============     ===============================================
        **Arguements**      **Description**
        ---------------     -----------------------------------------------
        where               Optional String. Optional Sql where clause.
        ---------------     -----------------------------------------------
        fields              Optional List. The default is all fields (*).
                            A list of fields can be provided to limit the
                            data that is returned.
        ===============     ===============================================

        :returns: _Row object
        """
        if isinstance(fields, (list, tuple)):
            if "OBJECTID" not in fields:
                fields.append("OBJECTID")
            fields = ",".join(fields)
        if where is None:
            query = """SELECT {fields} from {tbl} """.format(
                tbl=self._table_name, fields=fields
            )
        else:
            query = """SELECT {fields} from {tbl} WHERE {where}""".format(
                tbl=self._table_name, fields=fields, where=where
            )
        cursor = self._con.cursor()
        c = cursor.execute(query)
        columns = [d[0] for d in c.description]
        for row in c:
            yield _Row(
                values=dict(zip(columns, row)),
                table_name=self._table_name,
                con=self._con,
                header=self._gpheader,
            )

    # ----------------------------------------------------------------------
    def _flag_to_bytes(self, code):
        """converts single integer to bytes"""
        return int(code).to_bytes(1, byteorder="little")

    # ----------------------------------------------------------------------
    def _srid_to_bytes(self, srid):
        """converst WKID values to bytes"""
        return int(srid).to_bytes(4, byteorder="little")

    # ----------------------------------------------------------------------
    def _build_gp_header(self, version=0, empty=1):
        """assembles the GP header for WKB geometry"""
        return (
            b"GP"
            + self._flag_to_bytes(version)
            + self._flag_to_bytes(empty)
            + self._srid_to_bytes(self.wkid)
        )

    # ----------------------------------------------------------------------
    @property
    def _gpheader(self):
        """internal only, builds the geopackage binary header"""
        if self._gp_header is None:
            self._gp_header = self._build_gp_header()
        return self._gp_header

    # ----------------------------------------------------------------------
    def insert(self, row, geom_format="EsriJSON"):
        """
        Inserts a new row via dictionary

        ===============     ===============================================
        **Arguements**      **Description**
        ---------------     -----------------------------------------------
        row                 Required Dictionary.  Insert a new row via a
                            dictionary. The key/value pair must match up to
                            the field names in the table.
        ---------------     -----------------------------------------------
        geom_format         Optional String. When providing geometries
                            during insertion of new records, the method
                            needs to know the format of the geometry. The
                            formats supported values are: EsriJSON,
                            GeoJSON, WKT, and WKB.

                            The default geometry format is`EsriJSON`.

                            **Note**

                            GeoJSON and WKT require the package `geomet` to
                            be installed.

        ==============      ===============================================

        :returns: Boolean

        """
        if _HASGEOMET == False and geom_format.lower() in ["wkt", "geojson"]:
            raise ValueError(
                (
                    "The package `geomet` is required to work with "
                    "WKT and GeoJSON. Run `pip install geomet` to install."
                )
            )
        if isinstance(row, _Row):
            row = row._values
        values = None
        flds = {fld.lower(): fld for fld in row.keys()}
        if "shape" in flds:
            if (
                isinstance(row[flds["shape"]], dict)
                and geom_format.lower() == "esrijson"
            ):
                row[flds["shape"]] = self._gpheader + dumps(row[flds["shape"]], False)
            elif (
                isinstance(row[flds["shape"]], dict)
                and geom_format.lower() == "geojson"
            ):
                row[flds["shape"]] = self._gpheader + geometwkb.dumps(
                    obj=row[flds["shape"]]
                )
            elif isinstance(row[flds["shape"]], str) and geom_format.lower() == "wkt":
                gj = geometwkt.loads(row[flds["shape"]])
                row[flds["shape"]] = self._gpheader + geometwkb.dumps(obj=gj)
            elif isinstance(row[flds["shape"]], (bytes, bytearray)):
                if isinstance(row[flds["shape"]], (bytearray)):
                    row[flds["shape"]] = bytes(row[flds["shape"]])
                if len(row[flds["shape"]]) > 2 and row[flds["shape"]][:2] != b"GB":
                    row[flds["shape"]] = self._gpheader + row[flds["shape"]]
            elif row[flds["shape"]] is None:
                row[flds["shape"]] = self._gpheader + b"0x000000000000f87f"
            else:
                raise ValueError(
                    (
                        "Shape column must be Esri JSON dictionary, "
                        "WKT, GeoJSON dictionary, or WKB (bytes)"
                    )
                )
        if isinstance(row, dict):
            keys = row.keys()
            values = [list(row.values())]
        elif isinstance(row, (list, tuple)):
            keys = row[0].keys()
            values = [list(r.values()) for r in row]
        q = ["?"] * len(keys)
        q = ",".join(q)
        sql = """INSERT INTO {table} ({fields})
                     VALUES({q})""".format(
            table=self._table_name, fields=",".join(keys), q=q
        )
        inserts = []
        cur = self._con.cursor()
        cur.execute(sql, list(values[0]))
        self._con.commit()
        return True
