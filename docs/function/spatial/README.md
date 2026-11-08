# Spatial Functions

## Spatial Inputs and Diagnostics

See [spatial data types](../../data-types.md#9-spatial-types) for GEOMETRY
storage and type constraints. Use the appropriate [WKT](st_geomfromtext.md),
[WKB](st_geomfromwkb.md), or [GeoJSON](st_geomfromgeojson.md) constructor for
external values. Check each function's parameters, return value, and notes for
geometry-type requirements, NULL and empty inputs, SRID restrictions, and errors.

[Setting an SRID](st_srid.md) and [transforming coordinates](st_transform.md)
are separate operations. See [MySQL compatibility](../../mysql80-compatibility.md)
for the overall GIS support boundary and [spatial indexes](../../spatial-indexes.md)
for candidate filtering and exact predicate checks.

## Function Reference

| Function | Description |
| --- | --- |
| [`GEOMCOLLECTION`](geomcollection.md) | Constructs a geometry collection from geometry arguments. |
| [`GEOMETRYCOLLECTION`](geometrycollection.md) | Constructs a geometry collection from geometry arguments; an alias of `GEOMCOLLECTION`. |
| [`LINESTRING`](linestring.md) | Constructs a linestring from a sequence of points. |
| [`MBRCONTAINS`](mbrcontains.md) | Determines whether the minimum bounding rectangle of the first geometry contains the second. |
| [`MBRCOVEREDBY`](mbrcoveredby.md) | Determines whether the minimum bounding rectangle of the first geometry is covered by the second. |
| [`MBRCOVERS`](mbrcovers.md) | Determines whether the minimum bounding rectangle of the first geometry covers the second. |
| [`MBRDISJOINT`](mbrdisjoint.md) | Determines whether the minimum bounding rectangles of two geometries are disjoint. |
| [`MBREQUALS`](mbrequals.md) | Determines whether the minimum bounding rectangles of two geometries are equal. |
| [`MBRINTERSECTS`](mbrintersects.md) | Determines whether the minimum bounding rectangles of two geometries intersect. |
| [`MBROVERLAPS`](mbroverlaps.md) | Determines whether the minimum bounding rectangles of two geometries overlap. |
| [`MBRTOUCHES`](mbrtouches.md) | Determines whether the minimum bounding rectangles of two geometries touch. |
| [`MBRWITHIN`](mbrwithin.md) | Determines whether the minimum bounding rectangle of the first geometry is within the second. |
| [`MULTILINESTRING`](multilinestring.md) | Constructs a multilinestring from a sequence of linestrings. |
| [`MULTIPOINT`](multipoint.md) | Constructs a multipoint from a sequence of points. |
| [`MULTIPOLYGON`](multipolygon.md) | Constructs a multipolygon from a sequence of polygons. |
| [`POINT`](point.md) | Constructs a point from X and Y coordinates. |
| [`POLYGON`](polygon.md) | Constructs a polygon from a closed linestring. |
| [`ST_AREA`](st_area.md) | Returns the area of a polygon or multipolygon. |
| [`ST_ASBINARY`](st_asbinary.md) | Serializes a geometry to WKB. |
| [`ST_ASGEOJSON`](st_asgeojson.md) | Serializes a geometry to GeoJSON. |
| [`ST_ASTEXT`](st_astext.md) | Serializes a geometry to WKT. |
| [`ST_ASWKB`](st_aswkb.md) | Serializes a geometry to WKB; an alias of `ST_ASBINARY`. |
| [`ST_ASWKT`](st_aswkt.md) | Serializes a geometry to WKT; an alias of `ST_ASTEXT`. |
| [`ST_BUFFER_STRATEGY`](st_buffer_strategy.md) | Constructs a strategy byte string for buffer operations. |
| [`ST_CENTROID`](st_centroid.md) | Returns the centroid of a geometry. |
| [`ST_CONTAINS`](st_contains.md) | Determines whether the first geometry topologically contains the second. |
| [`ST_CONVEXHULL`](st_convexhull.md) | Returns the convex hull of a geometry. |
| [`ST_CROSSES`](st_crosses.md) | Determines whether two geometries topologically cross. |
| [`ST_DIMENSION`](st_dimension.md) | Returns the topological dimension of a geometry. |
| [`ST_DISJOINT`](st_disjoint.md) | Determines whether two geometries are topologically disjoint. |
| [`ST_DISTANCE`](st_distance.md) | Returns the shortest distance between two geometries. |
| [`ST_DISTANCE_SPHERE`](st_distance_sphere.md) | Returns the distance between two geometries on a sphere model. |
| [`ST_ENDPOINT`](st_endpoint.md) | Returns the endpoint of a linestring. |
| [`ST_ENVELOPE`](st_envelope.md) | Returns the minimum bounding rectangle of a geometry. |
| [`ST_EQUALS`](st_equals.md) | Determines whether two geometries are topologically equal. |
| [`ST_EXTERIORRING`](st_exteriorring.md) | Returns the exterior ring of a polygon. |
| [`ST_FRECHETDISTANCE`](st_frechetdistance.md) | Returns the discrete Fréchet distance between two geometries. |
| [`ST_GEOHASH`](st_geohash.md) | Generates a geohash from a point or latitude/longitude values. |
| [`ST_GEOMCOLLFROMTEXT`](st_geomcollfromtext.md) | Constructs a geometry collection from WKT. |
| [`ST_GEOMCOLLFROMTXT`](st_geomcollfromtxt.md) | Constructs a geometry collection from WKT; an alias of `ST_GEOMCOLLFROMTEXT`. |
| [`ST_GEOMCOLLFROMWKB`](st_geomcollfromwkb.md) | Constructs a geometry collection from WKB. |
| [`ST_GEOMETRYCOLLECTIONFROMTEXT`](st_geometrycollectionfromtext.md) | Constructs a geometry collection from WKT; an alias of `ST_GEOMCOLLFROMTEXT`. |
| [`ST_GEOMETRYCOLLECTIONFROMWKB`](st_geometrycollectionfromwkb.md) | Constructs a geometry collection from WKB; an alias of `ST_GEOMCOLLFROMWKB`. |
| [`ST_GEOMETRYFROMTEXT`](st_geometryfromtext.md) | Constructs a geometry from WKT; an alias of `ST_GEOMFROMTEXT`. |
| [`ST_GEOMETRYFROMWKB`](st_geometryfromwkb.md) | Constructs a geometry from WKB; an alias of `ST_GEOMFROMWKB`. |
| [`ST_GEOMETRYN`](st_geometryn.md) | Returns the Nth geometry in a geometry collection. |
| [`ST_GEOMETRYTYPE`](st_geometrytype.md) | Returns the type name of a geometry. |
| [`ST_GEOMFROMGEOJSON`](st_geomfromgeojson.md) | Constructs a geometry from GeoJSON. |
| [`ST_GEOMFROMTEXT`](st_geomfromtext.md) | Constructs a geometry from WKT. |
| [`ST_GEOMFROMWKB`](st_geomfromwkb.md) | Constructs a geometry from WKB. |
| [`ST_HAUSDORFFDISTANCE`](st_hausdorffdistance.md) | Returns the discrete Hausdorff distance between two geometries. |
| [`ST_INTERIORRINGN`](st_interiorringn.md) | Returns the Nth interior ring of a polygon. |
| [`ST_INTERSECTS`](st_intersects.md) | Determines whether two geometries topologically intersect. |
| [`ST_ISCLOSED`](st_isclosed.md) | Determines whether a linestring or multilinestring is closed. |
| [`ST_ISEMPTY`](st_isempty.md) | Determines whether a geometry is empty. |
| [`ST_ISSIMPLE`](st_issimple.md) | Determines whether a geometry is simple. |
| [`ST_ISVALID`](st_isvalid.md) | Determines whether a geometry is valid. |
| [`ST_LATFROMGEOHASH`](st_latfromgeohash.md) | Decodes the latitude from a geohash. |
| [`ST_LATITUDE`](st_latitude.md) | Returns the latitude of a geographic point. |
| [`ST_LENGTH`](st_length.md) | Returns the length of a linear geometry. |
| [`ST_LINEFROMTEXT`](st_linefromtext.md) | Constructs a linestring from WKT. |
| [`ST_LINEFROMWKB`](st_linefromwkb.md) | Constructs a linestring from WKB. |
| [`ST_LINEINTERPOLATEPOINT`](st_lineinterpolatepoint.md) | Returns the point interpolated along a linestring at a given fraction. |
| [`ST_LINEINTERPOLATEPOINTS`](st_lineinterpolatepoints.md) | Returns a multipoint of points interpolated along a linestring at a given interval. |
| [`ST_LINESTRINGFROMTEXT`](st_linestringfromtext.md) | Constructs a linestring from WKT; an alias of `ST_LINEFROMTEXT`. |
| [`ST_LINESTRINGFROMWKB`](st_linestringfromwkb.md) | Constructs a linestring from WKB; an alias of `ST_LINEFROMWKB`. |
| [`ST_LONGFROMGEOHASH`](st_longfromgeohash.md) | Decodes the longitude from a geohash. |
| [`ST_LONGITUDE`](st_longitude.md) | Returns the longitude of a geographic point. |
| [`ST_MAKEENVELOPE`](st_makeenvelope.md) | Constructs a rectangular envelope from two corner points. |
| [`ST_MLINEFROMTEXT`](st_mlinefromtext.md) | Constructs a multilinestring from WKT. |
| [`ST_MLINEFROMWKB`](st_mlinefromwkb.md) | Constructs a multilinestring from WKB. |
| [`ST_MPOINTFROMTEXT`](st_mpointfromtext.md) | Constructs a multipoint from WKT. |
| [`ST_MPOINTFROMWKB`](st_mpointfromwkb.md) | Constructs a multipoint from WKB. |
| [`ST_MPOLYFROMTEXT`](st_mpolyfromtext.md) | Constructs a multipolygon from WKT. |
| [`ST_MPOLYFROMWKB`](st_mpolyfromwkb.md) | Constructs a multipolygon from WKB. |
| [`ST_MULTILINESTRINGFROMTEXT`](st_multilinestringfromtext.md) | Constructs a multilinestring from WKT; an alias of `ST_MLINEFROMTEXT`. |
| [`ST_MULTILINESTRINGFROMWKB`](st_multilinestringfromwkb.md) | Constructs a multilinestring from WKB; an alias of `ST_MLINEFROMWKB`. |
| [`ST_MULTIPOINTFROMTEXT`](st_multipointfromtext.md) | Constructs a multipoint from WKT; an alias of `ST_MPOINTFROMTEXT`. |
| [`ST_MULTIPOINTFROMWKB`](st_multipointfromwkb.md) | Constructs a multipoint from WKB; an alias of `ST_MPOINTFROMWKB`. |
| [`ST_MULTIPOLYGONFROMTEXT`](st_multipolygonfromtext.md) | Constructs a multipolygon from WKT; an alias of `ST_MPOLYFROMTEXT`. |
| [`ST_MULTIPOLYGONFROMWKB`](st_multipolygonfromwkb.md) | Constructs a multipolygon from WKB; an alias of `ST_MPOLYFROMWKB`. |
| [`ST_NUMGEOMETRIES`](st_numgeometries.md) | Returns the number of geometries in a geometry collection. |
| [`ST_NUMINTERIORRING`](st_numinteriorring.md) | Returns the number of interior rings of a polygon; an alias of `ST_NUMINTERIORRINGS`. |
| [`ST_NUMINTERIORRINGS`](st_numinteriorrings.md) | Returns the number of interior rings of a polygon. |
| [`ST_NUMPOINTS`](st_numpoints.md) | Returns the number of points in a linestring. |
| [`ST_OVERLAPS`](st_overlaps.md) | Determines whether two geometries topologically overlap. |
| [`ST_POINTATDISTANCE`](st_pointatdistance.md) | Returns the point on a linestring at a given distance from the start point. |
| [`ST_POINTFROMGEOHASH`](st_pointfromgeohash.md) | Constructs a point from a geohash. |
| [`ST_POINTFROMTEXT`](st_pointfromtext.md) | Constructs a point from WKT. |
| [`ST_POINTFROMWKB`](st_pointfromwkb.md) | Constructs a point from WKB. |
| [`ST_POINTN`](st_pointn.md) | Returns the Nth point in a linestring. |
| [`ST_POLYFROMTEXT`](st_polyfromtext.md) | Constructs a polygon from WKT. |
| [`ST_POLYFROMWKB`](st_polyfromwkb.md) | Constructs a polygon from WKB. |
| [`ST_POLYGONFROMTEXT`](st_polygonfromtext.md) | Constructs a polygon from WKT; an alias of `ST_POLYFROMTEXT`. |
| [`ST_POLYGONFROMWKB`](st_polygonfromwkb.md) | Constructs a polygon from WKB; an alias of `ST_POLYFROMWKB`. |
| [`ST_SIMPLIFY`](st_simplify.md) | Simplifies a geometry using a tolerance. |
| [`ST_SRID`](st_srid.md) | Returns the SRID of a geometry, or returns the geometry with the SRID set. |
| [`ST_STARTPOINT`](st_startpoint.md) | Returns the start point of a linestring. |
| [`ST_SWAPXY`](st_swapxy.md) | Swaps the X and Y coordinates of every point in a geometry. |
| [`ST_TOUCHES`](st_touches.md) | Determines whether two geometries topologically touch. |
| [`ST_TRANSFORM`](st_transform.md) | Transforms a geometry to the target spatial reference system. |
| [`ST_VALIDATE`](st_validate.md) | Returns a topologically valid geometry unchanged, or NULL; no repair/normalization. |
| [`ST_WITHIN`](st_within.md) | Determines whether the first geometry is within the second geometry. |
| [`ST_X`](st_x.md) | Returns the X coordinate of a point, or returns the point with the X coordinate set. |
| [`ST_Y`](st_y.md) | Returns the Y coordinate of a point, or returns the point with the Y coordinate set. |

`ST_BUFFER`, `ST_UNION`, `ST_INTERSECTION`, `ST_DIFFERENCE`, and `ST_SYMDIFFERENCE` currently provide only recognizable entry points; executing them returns a clear not-supported error, so they are not listed in the table above as available functions.


## Spatial inputs and diagnostics

Spatial calls accept GEOMETRY values produced by spatial columns/constructors.
The geometry encoding must be valid and its SRID must resolve. Two-input operations
require the same SRID. A GEOMETRY value can have valid encoding but invalid topology;
ST_ISVALID and ST_VALIDATE inspect that topology rather than guaranteeing repair.
SQL NULL, an empty geometry and an unsupported geometry type are distinct inputs.

| Input/result case | Behavior |
| --- | --- |
| SQL NULL ordinary spatial operand | SQL NULL after arity validation |
| NULL member of LINESTRING/POLYGON/Multi/collection constructors | Error rather than silently skipping it |
| Empty input to distance/predicates | NULL where no distance/relation exists |
| Point/Point ST_TOUCHES or ST_CROSSES | NULL (inapplicable relationship) |
| Wrong type for a component accessor | NULL where inapplicable, or the stated type error |
| Malformed bytes, unknown SRID, mismatched two-input SRIDs | Error |
| Invalid topology in ST_VALIDATE | NULL; valid topology is returned unchanged |

MBR predicates test bounding rectangles. ST predicates test shape topology; boundary
inclusion differs between contains/within and MBR covers/covered-by. A rectangle
relationship is not proof of exact geometry containment.

| Capability | Current support |
| --- | --- |
| Cartesian/projected planar measurements | Supported with stated geometry/unit constraints |
| Geographic ST_AREA/ST_LENGTH/ST_DISTANCE | Explicit unsupported error |
| ST_DISTANCE_SPHERE | Spherical Point/MultiPoint calculation; default radius 6370986 metres |
| ST_TRANSFORM | Nonidentity 4326 to/from 3857; other pairs rejected |
| ST_BUFFER and geometric UNION/INTERSECTION/DIFFERENCE/SYMDIFFERENCE | Explicit unsupported error |
| ST_BUFFER_STRATEGY | Binary token construction only; does not enable ST_BUFFER |
| ST_ASGEOJSON precision/options | Explicit unsupported error |

Projected SRS unit metadata can support named-unit conversion. SRID 0 has no known
linear unit conversion. Query `information_schema.ST_SPATIAL_REFERENCE_SYSTEMS`
and `ST_UNITS_OF_MEASURE` for available definitions; a registry entry does not
imply every operation is implemented for that SRS. Measurements are not all
ellipsoidal/geodesic calculations.


## Construction and coordinate order

WKT/WKB constructors accept optional srid (default 0) and an axis-order option:
axis-order=srid-defined, axis-order=lat-long or axis-order=long-lat. Invalid options
are rejected. The constructor's requested type must match its WKT/WKB type.
WKB contains standard geometry bytes; mydb's internal geometry bytes additionally
carry SRID metadata and are not interchangeable with a bare WKB payload.

EPSG:4326 WKT uses latitude/longitude by default; GeoJSON coordinate arrays use
longitude/latitude. For example ST_GEOMFROMTEXT('POINT(39.9 116.4)',4326) represents
Beijing, while GeoJSON uses [116.4,39.9]. GeoJSON defaults to SRID 4326; an explicit
Cartesian SRID 0 retains X/Y coordinates. GeoJSON options control extra-dimension
handling; only the supported 2D internal geometry result is constructed.

ST_X/ST_Y are ordinate accessors. Geographic longitude/latitude accessors obey the
SRS axes and coordinate ranges. Setter overloads return a GEOMETRY; getters return
DOUBLE. ST_SRID assigns an SRS identity with the current axis-order adjustment,
whereas ST_TRANSFORM performs only its supported projection conversions. ST_SWAPXY
exchanges ordinates without projecting. Output WKT/WKB axis-order options are not
implemented; use the documented single-argument output forms.
