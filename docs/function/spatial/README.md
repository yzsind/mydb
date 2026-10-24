# Spatial Functions

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
| [`ST_VALIDATE`](st_validate.md) | Validates and normalizes a geometry. |
| [`ST_WITHIN`](st_within.md) | Determines whether the first geometry is within the second geometry. |
| [`ST_X`](st_x.md) | Returns the X coordinate of a point, or returns the point with the X coordinate set. |
| [`ST_Y`](st_y.md) | Returns the Y coordinate of a point, or returns the point with the Y coordinate set. |

`ST_BUFFER`, `ST_UNION`, `ST_INTERSECTION`, `ST_DIFFERENCE`, and `ST_SYMDIFFERENCE` currently provide only recognizable entry points; executing them returns a clear not-supported error, so they are not listed in the table above as available functions.
