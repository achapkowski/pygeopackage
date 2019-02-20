A package to read/write GeoPackage Vector Data.


### Getting Started

Use the GeoPackage library is easy as passing in a file path to the `GeoPackage` class.  This class contains the ability to manage, create, and modify spatial and attribute data.  It is designed to work with for `arcpy` and `shapely` geometries, so no matter what you do or how you want to manage your data, you now can.


```python
from geopackage import GeoPackage

fp = r"./data/geodata.gpkg"
with GeoPackage(fp) as gpkg:
    # List all Tables
    for table in gpkg.tables:
        print(table)
```

GeoPackages is designed to create a simple approach to data management.  The context manager handles all the `close` and `save` operations.  

### Reading Table Data

```python

fp = r"./data/geodata.gpkg"
with GeoPackage(fp) as gpkg:
    # List all Tables
    table = gpkg.get("census")
    for row in table.rows():
        print(row)

```

When reading information, an optional where clause and field names can be given.

### Writing Data

```python

fp = r"./data/geodata.gpkg"
with GeoPackage(fp) as gpkg:
    # List all Tables
    table = gpkg.get("census")
    for row in table.rows():
        print(row)

```