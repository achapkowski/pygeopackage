"""
This module contians operations to create, modify and update geopackages
"""
import os
import sys
import sqlite3
import tempfile

# --------------------------------------------------------------------------
_field_lookup = {
    "text": [
        "TEXT",
        "check((typeof({field}) = 'text' or typeof({field}) = 'null') and not length({field}) > {l})",
    ],
    "float": [
        "DOUBLE",
        """check(typeof({field}) = 'real' or typeof({field}) = 'null')""",
    ],
    "double": ["DOUBLE", "check(typeof({field}) = 'real' or typeof({field}) = 'null')"],
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
    "blob": ["BLOB", "check(typeof({field}) = 'blob' or typeof({field}) = 'null')"],
    "guid": [
        "TEXT(38)",
        "check((typeof({field}) = 'text' or typeof({field}) = 'null') and not length({field}) > 38)",
    ],
}
# --------------------------------------------------------------------------
_create_tables_sql = [
    """CREATE TABLE IF NOT EXISTS gpkg_contents (table_name TEXT NOT NULL PRIMARY KEY,data_type TEXT NOT NULL,identifier TEXT UNIQUE,description TEXT DEFAULT '',last_change DATETIME NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),min_x DOUBLE,min_y DOUBLE,max_x DOUBLE,max_y DOUBLE,srs_id INTEGER,CONSTRAINT fk_gc_r_srs_id FOREIGN KEY (srs_id) REFERENCES gpkg_spatial_ref_sys (srs_id))""",
    """CREATE TABLE IF NOT EXISTS gpkg_data_column_constraints (constraint_name TEXT NOT NULL,constraint_type TEXT NOT NULL /* 'range' || 'enum' | 'glob' */,value TEXT,min NUMERIC,min_is_inclusive BOOLEAN,max NUMERIC,max_is_inclusive BOOLEAN,description TEXT,CONSTRAINT gdcc_ntv UNIQUE (constraint_name,constraint_type,value))""",
    """CREATE TABLE IF NOT EXISTS gpkg_data_columns (table_name TEXT NOT NULL,column_name TEXT NOT NULL,name TEXT,title TEXT,description TEXT,mime_type TEXT,constraint_name TEXT,CONSTRAINT pk_gdc PRIMARY KEY (table_name,column_name),CONSTRAINT fk_gdc_tn FOREIGN KEY (table_name) REFERENCES gpkg_contents (table_name))""",
    """CREATE TABLE IF NOT EXISTS gpkg_extensions (table_name TEXT,column_name TEXT,extension_name TEXT NOT NULL,definition TEXT NOT NULL,scope TEXT NOT NULL,CONSTRAINT ge_tce UNIQUE (table_name,column_name,extension_name))""",
    """CREATE TABLE IF NOT EXISTS gpkg_geometry_columns (table_name TEXT NOT NULL,column_name TEXT NOT NULL,geometry_type_name TEXT NOT NULL,srs_id INTEGER NOT NULL,z TINYINT NOT NULL,m TINYINT NOT NULL,CONSTRAINT pk_geom_cols PRIMARY KEY (table_name, column_name),CONSTRAINT uk_gc_table_name UNIQUE (table_name),CONSTRAINT fk_gc_tn FOREIGN KEY (table_name) REFERENCES gpkg_contents (table_name),CONSTRAINT fk_gc_srs FOREIGN KEY (srs_id) REFERENCES gpkg_spatial_ref_sys (srs_id))""",
    """CREATE TABLE IF NOT EXISTS gpkg_metadata (id INTEGER CONSTRAINT m_pk PRIMARY KEY ASC AUTOINCREMENT NOT NULL UNIQUE,md_scope TEXT NOT NULL DEFAULT 'dataset',md_standard_uri TEXT NOT NULL,mime_type TEXT NOT NULL DEFAULT 'text/xml',metadata TEXT NOT NULL)""",
    """CREATE TABLE IF NOT EXISTS gpkg_metadata_reference (reference_scope TEXT NOT NULL,table_name TEXT,column_name TEXT,row_id_value INTEGER,timestamp DATETIME NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),md_file_id INTEGER NOT NULL,md_parent_id INTEGER,CONSTRAINT crmr_mfi_fk FOREIGN KEY (md_file_id) REFERENCES gpkg_metadata(id),CONSTRAINT crmr_mpi_fk FOREIGN KEY (md_parent_id) REFERENCES gpkg_metadata(id))""",
    """CREATE TABLE IF NOT EXISTS gpkg_spatial_ref_sys (srs_name TEXT NOT NULL,srs_id INTEGER NOT NULL PRIMARY KEY,organization TEXT NOT NULL,organization_coordsys_id INTEGER NOT NULL,definition TEXT NOT NULL,description TEXT)""",
    """CREATE TABLE IF NOT EXISTS gpkg_tile_matrix (table_name TEXT NOT NULL,zoom_level INTEGER NOT NULL,matrix_width INTEGER NOT NULL,matrix_height INTEGER NOT NULL,tile_width INTEGER NOT NULL,tile_height INTEGER NOT NULL,pixel_x_size DOUBLE NOT NULL,pixel_y_size DOUBLE NOT NULL,CONSTRAINT pk_ttm PRIMARY KEY (table_name,zoom_level),CONSTRAINT fk_ttm_table_name FOREIGN KEY (table_name) REFERENCES gpkg_contents(table_name))""",
    """CREATE TABLE IF NOT EXISTS gpkg_tile_matrix_set (table_name TEXT NOT NULL PRIMARY KEY,srs_id INTEGER NOT NULL,min_x DOUBLE NOT NULL,min_y DOUBLE NOT NULL,max_x DOUBLE NOT NULL,max_y DOUBLE NOT NULL,CONSTRAINT fk_gtms_table_name FOREIGN KEY (table_name) REFERENCES gpkg_contents (table_name),CONSTRAINT fk_gtms_srs FOREIGN KEY (srs_id) REFERENCES gpkg_spatial_ref_sys (srs_id))""",
]
# --------------------------------------------------------------------------
_initial_triggers_sql = [
    """CREATE TRIGGER IF NOT EXISTS 'gpkg_metadata_md_scope_insert' BEFORE INSERT ON 'gpkg_metadata' FOR EACH ROW BEGIN SELECT RAISE(ABORT, 'insert on table gpkg_metadata violates constraint: md_scope must be one of undefined | fieldSession | collectionSession | series | dataset | featureType | feature | attributeType | attribute | tile | model | catalogue | schema | taxonomy | software | service | collectionHardware | nonGeographicDataset | dimensionGroup') WHERE NOT(NEW.md_scope IN ('undefined','fieldSession','collectionSession','series','dataset','featureType','feature','attributeType','attribute','tile','model','catalogue','schema','taxonomy','software','service','collectionHardware','nonGeographicDataset','dimensionGroup'));END""",
    """CREATE TRIGGER IF NOT EXISTS 'gpkg_metadata_md_scope_update' BEFORE UPDATE OF 'md_scope' ON 'gpkg_metadata' FOR EACH ROW BEGIN SELECT RAISE(ABORT, 'update on table gpkg_metadata violates constraint: md_scope must be one of undefined | fieldSession | collectionSession | series | dataset | featureType | feature | attributeType | attribute | tile | model | catalogue | schema | taxonomy | software | service | collectionHardware | nonGeographicDataset | dimensionGroup') WHERE NOT(NEW.md_scope IN ('undefined','fieldSession','collectionSession','series','dataset','featureType','feature','attributeType','attribute','tile','model','catalogue','schema','taxonomy','software','service','collectionHardware','nonGeographicDataset','dimensionGroup'));END""",
    """CREATE TRIGGER IF NOT EXISTS 'gpkg_metadata_reference_column_name_insert' BEFORE INSERT ON 'gpkg_metadata_reference' FOR EACH ROW BEGIN SELECT RAISE(ABORT, 'insert on table gpkg_metadata_reference violates constraint: column name must be NULL when reference_scope is "geopackage","table" or "row"') WHERE (NEW.reference_scope IN ('geopackage','table','row') AND NEW.column_name IS NOT NULL); SELECT RAISE(ABORT, 'insert on table gpkg_metadata_reference violates constraint: column name must be defined for the specified table when reference_scope is "column" or "row/col"') WHERE (NEW.reference_scope IN ('column','row/col') AND NOT NEW.table_name IN (SELECT name FROM SQLITE_MASTER WHERE type = 'table' AND name = NEW.table_name AND sql LIKE ('%' || NEW.column_name || '%'))); END""",
    """CREATE TRIGGER IF NOT EXISTS 'gpkg_metadata_reference_column_name_update' BEFORE UPDATE OF column_name ON 'gpkg_metadata_reference' FOR EACH ROW BEGIN SELECT RAISE(ABORT, 'update on table gpkg_metadata_reference violates constraint: column name must be NULL when reference_scope is "geopackage","table" or "row"') WHERE (NEW.reference_scope IN ('geopackage','table','row') AND NEW.column_name IS NOT NULL); SELECT RAISE(ABORT, 'update on table gpkg_metadata_reference violates constraint: column name must be defined for the specified table when reference_scope is "column" or "row/col"') WHERE (NEW.reference_scope IN ('column','row/col') AND NOT NEW.table_name IN (SELECT name FROM SQLITE_MASTER WHERE type = 'table' AND name = NEW.table_name AND sql LIKE ('%' || NEW.column_name || '%'))); END""",
    """CREATE TRIGGER IF NOT EXISTS 'gpkg_metadata_reference_reference_scope_insert' BEFORE INSERT ON 'gpkg_metadata_reference' FOR EACH ROW BEGIN SELECT RAISE(ABORT, 'insert on table gpkg_metadata_reference violates constraint: reference_scope must be one of "geopackage", "table","column","row", "row/col"') WHERE NOT NEW.reference_scope IN ('geopackage','table','column','row','row/col');END""",
    """CREATE TRIGGER IF NOT EXISTS 'gpkg_metadata_reference_reference_scope_update' BEFORE UPDATE OF 'reference_scope' ON 'gpkg_metadata_reference' FOR EACH ROW BEGIN SELECT RAISE(ABORT, 'update on table gpkg_metadata_reference violates constraint: reference_scope must be one of "geopackage", "table","column","row", "row/col"') WHERE NOT NEW.reference_scope IN ('geopackage','table','column','row','row/col');END""",
    """CREATE TRIGGER IF NOT EXISTS 'gpkg_metadata_reference_timestamp_insert' BEFORE INSERT ON 'gpkg_metadata_reference' FOR EACH ROW BEGIN SELECT RAISE(ABORT, 'insert on table gpkg_metadata_reference violates constraint: timestamp must be a valid time in ISO 8601 "yyyy-mm-ddThh-mm-ss.cccZ" form') WHERE NOT (NEW.timestamp GLOB '[1-2][0-9][0-9][0-9]-[0-1][0-9]-[1-3][0-9]T[0-2][0-9]:[0-5][0-9]:[0-5][0-9].[0-9][0-9][0-9]Z' AND strftime('%s',NEW.timestamp) NOT NULL);END""",
    """CREATE TRIGGER IF NOT EXISTS 'gpkg_metadata_reference_timestamp_update' BEFORE UPDATE OF 'timestamp' ON 'gpkg_metadata_reference' FOR EACH ROW BEGIN SELECT RAISE(ABORT, 'update on table gpkg_metadata_reference violates constraint: timestamp must be a valid time in ISO 8601 "yyyy-mm-ddThh-mm-ss.cccZ" form') WHERE NOT (NEW.timestamp GLOB '[1-2][0-9][0-9][0-9]-[0-1][0-9]-[1-3][0-9]T[0-2][0-9]:[0-5][0-9]:[0-5][0-9].[0-9][0-9][0-9]Z' AND strftime('%s',NEW.timestamp) NOT NULL);END""",
    """CREATE TRIGGER IF NOT EXISTS 'gpkg_tile_matrix_matrix_height_insert' BEFORE INSERT ON 'gpkg_tile_matrix' FOR EACH ROW BEGIN SELECT RAISE(ABORT, 'insert on table ''gpkg_tile_matrix'' violates constraint: matrix_height cannot be less than 1') WHERE (NEW.matrix_height < 1);END""",
    """CREATE TRIGGER IF NOT EXISTS 'gpkg_tile_matrix_matrix_height_update' BEFORE UPDATE OF matrix_height ON 'gpkg_tile_matrix' FOR EACH ROW BEGIN SELECT RAISE(ABORT, 'update on table ''gpkg_tile_matrix'' violates constraint: matrix_height cannot be less than 1') WHERE (NEW.matrix_height < 1);END""",
    """CREATE TRIGGER IF NOT EXISTS 'gpkg_tile_matrix_matrix_width_insert' BEFORE INSERT ON 'gpkg_tile_matrix' FOR EACH ROW BEGIN SELECT RAISE(ABORT, 'insert on table ''gpkg_tile_matrix'' violates constraint: matrix_width cannot be less than 1') WHERE (NEW.matrix_width < 1);END""",
    """CREATE TRIGGER IF NOT EXISTS 'gpkg_tile_matrix_matrix_width_update' BEFORE UPDATE OF matrix_width ON 'gpkg_tile_matrix' FOR EACH ROW BEGIN SELECT RAISE(ABORT, 'update on table ''gpkg_tile_matrix'' violates constraint: matrix_width cannot be less than 1') WHERE (NEW.matrix_width < 1);END""",
    """CREATE TRIGGER IF NOT EXISTS 'gpkg_tile_matrix_pixel_x_size_insert' BEFORE INSERT ON 'gpkg_tile_matrix' FOR EACH ROW BEGIN SELECT RAISE(ABORT, 'insert on table ''gpkg_tile_matrix'' violates constraint: pixel_x_size must be greater than 0') WHERE NOT (NEW.pixel_x_size > 0);END""",
    """CREATE TRIGGER IF NOT EXISTS 'gpkg_tile_matrix_pixel_x_size_update' BEFORE UPDATE OF pixel_x_size ON 'gpkg_tile_matrix' FOR EACH ROW BEGIN SELECT RAISE(ABORT, 'update on table ''gpkg_tile_matrix'' violates constraint: pixel_x_size must be greater than 0') WHERE NOT (NEW.pixel_x_size > 0);END""",
    """CREATE TRIGGER IF NOT EXISTS 'gpkg_tile_matrix_pixel_y_size_insert' BEFORE INSERT ON 'gpkg_tile_matrix' FOR EACH ROW BEGIN SELECT RAISE(ABORT, 'insert on table ''gpkg_tile_matrix'' violates constraint: pixel_y_size must be greater than 0') WHERE NOT (NEW.pixel_y_size > 0);END""",
    """CREATE TRIGGER IF NOT EXISTS 'gpkg_tile_matrix_pixel_y_size_update' BEFORE UPDATE OF pixel_y_size ON 'gpkg_tile_matrix' FOR EACH ROW BEGIN SELECT RAISE(ABORT, 'update on table ''gpkg_tile_matrix'' violates constraint: pixel_y_size must be greater than 0') WHERE NOT (NEW.pixel_y_size > 0);END""",
    """CREATE TRIGGER IF NOT EXISTS 'gpkg_tile_matrix_zoom_level_insert' BEFORE INSERT ON 'gpkg_tile_matrix' FOR EACH ROW BEGIN SELECT RAISE(ABORT, 'insert on table ''gpkg_tile_matrix'' violates constraint: zoom_level cannot be less than 0') WHERE (NEW.zoom_level < 0);END""",
    """CREATE TRIGGER IF NOT EXISTS 'gpkg_tile_matrix_zoom_level_update' BEFORE UPDATE OF zoom_level ON 'gpkg_tile_matrix' FOR EACH ROW BEGIN SELECT RAISE(ABORT, 'update on table ''gpkg_tile_matrix'' violates constraint: zoom_level cannot be less than 0') WHERE (NEW.zoom_level < 0);END""",
]
# ----------------------------------------------------------------------
def _insert_values(con, tbl, fields, values):
    """inserts multiple values into a table"""
    q = ["?"] * len(fields)
    q = ",".join(q)
    sql = """INSERT INTO {table} ({fields})
                 VALUES({q})""".format(
        table=tbl, fields=",".join(fields), q=q
    )
    inserts = []
    cur = con.cursor()
    for val in values:
        if not isinstance(val, list):
            val = [val]
        cur.execute(sql, val)
        inserts.append(cur.lastrowid)
    con.commit()
    return inserts


# --------------------------------------------------------------------------
def _copy_table_schema(con, table_source, table_name_dest):
    """copies a table's schema to a new table"""
    from ._geopackage import SpatialTable, Table

    sql = """CREATE TABLE {dest} AS SELECT * FROM {source} WHERE 0""".format(
        source=table_source, dest=table_name_dest
    )
    # _insert_values(con=con, tbl="gpkg_contents",
    # fields=['table_name', 'data_type', 'identifier', 'srs_id'],
    # values=[[name, 'features', name, wkid]])

    # _insert_values(con=con, tbl="gpkg_geometry_columns",
    # fields=['table_name', 'column_name', 'geometry_type_name', 'srs_id', 'z', 'm'],
    # values=[[name, 'Shape', _geom_lu[geometry.lower()], wkid, int(has_z),int(has_m)]])
    try:
        con.execute(sql)
        con.commit()
    except Exception as e:
        raise Exception(e)


# ----------------------------------------------------------------------
def _create_gpkg(name, path=None, overwrite=False):
    """
    Creates an empty geopackage
    """
    if path is None:
        path = tempfile.gettempdir()
    elif os.path.isdir(path) == False and len(path) > 0:
        os.makedirs(path, exist_ok=True)
    if name.endswith(".gpkg") == False:
        name += ".gpkg"
    fp = os.path.join(path, name)
    if overwrite and os.path.isfile(fp):
        os.remove(fp)
    con = sqlite3.connect(database=fp)

    for sql in _create_tables_sql:
        con.execute(sql)
    for trg_sql in _initial_triggers_sql:
        con.execute(trg_sql)
    sql = """SELECT * from gpkg_extensions"""
    cur = con.execute(sql)
    result = cur.fetchall()
    con.commit()
    del cur
    if len(result) < 2:
        _insert_values(
            con=con,
            tbl="gpkg_extensions",
            fields=["extension_name", "definition", "scope"],
            values=[
                ["gpkg_metadata", "F.8 Metadata", "read-write"],
                ["gpkg_schema", "F.9 Schema", "read-write"],
            ],
        )
        _insert_values(
            con=con,
            tbl="gpkg_spatial_ref_sys",
            fields=[
                "srs_name",
                "srs_id",
                "organization",
                "organization_coordsys_id",
                "definition",
                "description",
            ],
            values=[
                [
                    "GCS_WGS_1984",
                    4326,
                    "EPSG",
                    "4326",
                    """GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",SPHEROID["WGS_1984",6378137.0,298.257223563]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]]""",
                    "WGS 1984",
                ],
                ["Undefined Cartesian", -1, "NONE", "-1", "undefined", None],
                ["Undefined Geographic", 0, "NONE", "0", "undefined", None],
            ],
        )
    con.commit()
    con.close()
    del con
    return fp


# --------------------------------------------------------------------------
def _create_table(con, name, fields=None):
    """creates an attribute table"""
    txts = ["OBJECTID INTEGER primary key autoincrement not null"]
    if fields is None:
        fields = {}

    for k, v in fields.items():
        row = _field_lookup[v.lower()]
        if row[0].lower() != "text":
            fld = "{field} {dtype} {st}".format(field=k, dtype=row[0], st=row[1])
            txts.append(fld.format(field=k))
        else:
            fld = "{field} {dtype}".format(field=k, dtype=row[0])
            txts.append(fld)
        del k, v
    try:
        sql = """CREATE TABLE IF NOT EXISTS {tbl} ({fields})""".format(
            tbl=name, fields=",".join(txts)
        )
        con.execute(sql)
        con.commit()
        _insert_values(
            con=con,
            tbl="gpkg_contents",
            fields=["table_name", "data_type", "identifier"],
            values=[[name, "attributes", name]],
        )
        return True
    except:
        return False


# ----------------------------------------------------------------------
def _create_feature_class(
    con, name, geometry="point", wkid=4326, fields=None, has_z=False, has_m=False
):
    """
    Creates a spatial table

    ==============     ====================================================
    **Argument**       **Description**
    --------------     ----------------------------------------------------
    name               Required String.  The name of the table.
    --------------     ----------------------------------------------------
    geometry           Optional String. The type of geometry to create.
                       This can be point, line, polygon or multipoint.
    --------------     ----------------------------------------------------
    wkid               Optional Integer.  The spatial reference code.
    ==============     ====================================================

    """
    _geom_lu = {
        "point": "POINT",
        "multipoint": "MULTIPOINT",
        "polygon": "MULTIPOLYGON",
        "polyline": "POLYLINE",
        "line": "POLYLINE",
    }
    txts = [
        "OBJECTID INTEGER primary key autoincrement not null",
        "'Shape' {gt}".format(gt=_geom_lu[geometry.lower()]),
    ]
    if fields is None:
        fields = {}

    for k, v in fields.items():
        row = _field_lookup[v.lower()]
        if row[0].lower() != "text":
            fld = "{field} {dtype} {st}".format(field=k, dtype=row[0], st=row[1])
            txts.append(fld.format(field=k))
        else:
            fld = "{field} {dtype}".format(field=k, dtype=row[0])
            txts.append(fld)
        del k, v
    sql = """CREATE TABLE IF NOT EXISTS {name} ({fields})""".format(
        name=name, fields=",".join(txts)
    )
    try:
        con.execute(sql)
        con.commit()
    except Exception as e:
        raise Exception(e)

    _insert_values(
        con=con,
        tbl="gpkg_contents",
        fields=["table_name", "data_type", "identifier", "srs_id"],
        values=[[name, "features", name, wkid]],
    )

    _insert_values(
        con=con,
        tbl="gpkg_geometry_columns",
        fields=["table_name", "column_name", "geometry_type_name", "srs_id", "z", "m"],
        values=[
            [name, "Shape", _geom_lu[geometry.lower()], wkid, int(has_z), int(has_m)]
        ],
    )

    sql = """select srs_id from gpkg_spatial_ref_sys where srs_id = {wkid};""".format(
        wkid=wkid
    )
    sr_check = len(con.execute(sql).fetchall())
    if sr_check == 0:
        from ._coord import lookup_coordinate_system

        res = lookup_coordinate_system(wkid=wkid)
        if len(res) > 0:
            res = res[0]
        _insert_values(
            con=con,
            tbl="gpkg_spatial_ref_sys",
            fields=[
                "srs_name",
                "srs_id",
                "organization",
                "description",
                "organization_coordsys_id",
                "definition",
            ],
            values=[
                [res["NAME"], res["WKID"], "ESRI", res["NAME"], res["WKID"], res["WKT"]]
            ],
        )
    return True
