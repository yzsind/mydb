use super::*;

const FIXTURE: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/function-compat/spatial.json"
));

function_fixture_test!(geomcollection, "GEOMCOLLECTION", FIXTURE);
function_fixture_test!(geometrycollection, "GEOMETRYCOLLECTION", FIXTURE);
function_fixture_test!(linestring, "LINESTRING", FIXTURE);
function_fixture_test!(mbrcontains, "MBRCONTAINS", FIXTURE);
function_fixture_test!(mbrcoveredby, "MBRCOVEREDBY", FIXTURE);
function_fixture_test!(mbrcovers, "MBRCOVERS", FIXTURE);
function_fixture_test!(mbrdisjoint, "MBRDISJOINT", FIXTURE);
function_fixture_test!(mbrequals, "MBREQUALS", FIXTURE);
function_fixture_test!(mbrintersects, "MBRINTERSECTS", FIXTURE);
function_fixture_test!(mbroverlaps, "MBROVERLAPS", FIXTURE);
function_fixture_test!(mbrtouches, "MBRTOUCHES", FIXTURE);
function_fixture_test!(mbrwithin, "MBRWITHIN", FIXTURE);
function_fixture_test!(multilinestring, "MULTILINESTRING", FIXTURE);
function_fixture_test!(multipoint, "MULTIPOINT", FIXTURE);
function_fixture_test!(multipolygon, "MULTIPOLYGON", FIXTURE);
function_fixture_test!(point, "POINT", FIXTURE);
function_fixture_test!(polygon, "POLYGON", FIXTURE);
function_fixture_test!(st_area, "ST_AREA", FIXTURE);
function_fixture_test!(st_asbinary, "ST_ASBINARY", FIXTURE);
function_fixture_test!(st_asgeojson, "ST_ASGEOJSON", FIXTURE);
function_fixture_test!(st_astext, "ST_ASTEXT", FIXTURE);
function_fixture_test!(st_aswkb, "ST_ASWKB", FIXTURE);
function_fixture_test!(st_aswkt, "ST_ASWKT", FIXTURE);
function_fixture_test!(st_buffer_strategy, "ST_BUFFER_STRATEGY", FIXTURE);
function_fixture_test!(st_centroid, "ST_CENTROID", FIXTURE);
function_fixture_test!(st_contains, "ST_CONTAINS", FIXTURE);
function_fixture_test!(st_convexhull, "ST_CONVEXHULL", FIXTURE);
function_fixture_test!(st_crosses, "ST_CROSSES", FIXTURE);
function_fixture_test!(st_dimension, "ST_DIMENSION", FIXTURE);
function_fixture_test!(st_disjoint, "ST_DISJOINT", FIXTURE);
function_fixture_test!(st_distance, "ST_DISTANCE", FIXTURE);
function_fixture_test!(st_distance_sphere, "ST_DISTANCE_SPHERE", FIXTURE);
function_fixture_test!(st_endpoint, "ST_ENDPOINT", FIXTURE);
function_fixture_test!(st_envelope, "ST_ENVELOPE", FIXTURE);
function_fixture_test!(st_equals, "ST_EQUALS", FIXTURE);
function_fixture_test!(st_exteriorring, "ST_EXTERIORRING", FIXTURE);
function_fixture_test!(st_frechetdistance, "ST_FRECHETDISTANCE", FIXTURE);
function_fixture_test!(st_geohash, "ST_GEOHASH", FIXTURE);
function_fixture_test!(st_geomcollfromtext, "ST_GEOMCOLLFROMTEXT", FIXTURE);
function_fixture_test!(st_geomcollfromtxt, "ST_GEOMCOLLFROMTXT", FIXTURE);
function_fixture_test!(st_geomcollfromwkb, "ST_GEOMCOLLFROMWKB", FIXTURE);
function_fixture_test!(
    st_geometrycollectionfromtext,
    "ST_GEOMETRYCOLLECTIONFROMTEXT",
    FIXTURE
);
function_fixture_test!(
    st_geometrycollectionfromwkb,
    "ST_GEOMETRYCOLLECTIONFROMWKB",
    FIXTURE
);
function_fixture_test!(st_geometryfromtext, "ST_GEOMETRYFROMTEXT", FIXTURE);
function_fixture_test!(st_geometryfromwkb, "ST_GEOMETRYFROMWKB", FIXTURE);
function_fixture_test!(st_geometryn, "ST_GEOMETRYN", FIXTURE);
function_fixture_test!(st_geometrytype, "ST_GEOMETRYTYPE", FIXTURE);
function_fixture_test!(st_geomfromgeojson, "ST_GEOMFROMGEOJSON", FIXTURE);
function_fixture_test!(st_geomfromtext, "ST_GEOMFROMTEXT", FIXTURE);
function_fixture_test!(st_geomfromwkb, "ST_GEOMFROMWKB", FIXTURE);
function_fixture_test!(st_hausdorffdistance, "ST_HAUSDORFFDISTANCE", FIXTURE);
function_fixture_test!(st_interiorringn, "ST_INTERIORRINGN", FIXTURE);
function_fixture_test!(st_intersects, "ST_INTERSECTS", FIXTURE);
function_fixture_test!(st_isclosed, "ST_ISCLOSED", FIXTURE);
function_fixture_test!(st_isempty, "ST_ISEMPTY", FIXTURE);
function_fixture_test!(st_issimple, "ST_ISSIMPLE", FIXTURE);
function_fixture_test!(st_isvalid, "ST_ISVALID", FIXTURE);
function_fixture_test!(st_latfromgeohash, "ST_LATFROMGEOHASH", FIXTURE);
function_fixture_test!(st_latitude, "ST_LATITUDE", FIXTURE);
function_fixture_test!(st_length, "ST_LENGTH", FIXTURE);
function_fixture_test!(st_linefromtext, "ST_LINEFROMTEXT", FIXTURE);
function_fixture_test!(st_linefromwkb, "ST_LINEFROMWKB", FIXTURE);
function_fixture_test!(st_lineinterpolatepoint, "ST_LINEINTERPOLATEPOINT", FIXTURE);
function_fixture_test!(
    st_lineinterpolatepoints,
    "ST_LINEINTERPOLATEPOINTS",
    FIXTURE
);
function_fixture_test!(st_linestringfromtext, "ST_LINESTRINGFROMTEXT", FIXTURE);
function_fixture_test!(st_linestringfromwkb, "ST_LINESTRINGFROMWKB", FIXTURE);
function_fixture_test!(st_longfromgeohash, "ST_LONGFROMGEOHASH", FIXTURE);
function_fixture_test!(st_longitude, "ST_LONGITUDE", FIXTURE);
function_fixture_test!(st_makeenvelope, "ST_MAKEENVELOPE", FIXTURE);
function_fixture_test!(st_mlinefromtext, "ST_MLINEFROMTEXT", FIXTURE);
function_fixture_test!(st_mlinefromwkb, "ST_MLINEFROMWKB", FIXTURE);
function_fixture_test!(st_mpointfromtext, "ST_MPOINTFROMTEXT", FIXTURE);
function_fixture_test!(st_mpointfromwkb, "ST_MPOINTFROMWKB", FIXTURE);
function_fixture_test!(st_mpolyfromtext, "ST_MPOLYFROMTEXT", FIXTURE);
function_fixture_test!(st_mpolyfromwkb, "ST_MPOLYFROMWKB", FIXTURE);
function_fixture_test!(
    st_multilinestringfromtext,
    "ST_MULTILINESTRINGFROMTEXT",
    FIXTURE
);
function_fixture_test!(
    st_multilinestringfromwkb,
    "ST_MULTILINESTRINGFROMWKB",
    FIXTURE
);
function_fixture_test!(st_multipointfromtext, "ST_MULTIPOINTFROMTEXT", FIXTURE);
function_fixture_test!(st_multipointfromwkb, "ST_MULTIPOINTFROMWKB", FIXTURE);
function_fixture_test!(st_multipolygonfromtext, "ST_MULTIPOLYGONFROMTEXT", FIXTURE);
function_fixture_test!(st_multipolygonfromwkb, "ST_MULTIPOLYGONFROMWKB", FIXTURE);
function_fixture_test!(st_numgeometries, "ST_NUMGEOMETRIES", FIXTURE);
function_fixture_test!(st_numinteriorring, "ST_NUMINTERIORRING", FIXTURE);
function_fixture_test!(st_numinteriorrings, "ST_NUMINTERIORRINGS", FIXTURE);
function_fixture_test!(st_numpoints, "ST_NUMPOINTS", FIXTURE);
function_fixture_test!(st_overlaps, "ST_OVERLAPS", FIXTURE);
function_fixture_test!(st_pointatdistance, "ST_POINTATDISTANCE", FIXTURE);
function_fixture_test!(st_pointfromgeohash, "ST_POINTFROMGEOHASH", FIXTURE);
function_fixture_test!(st_pointfromtext, "ST_POINTFROMTEXT", FIXTURE);
function_fixture_test!(st_pointfromwkb, "ST_POINTFROMWKB", FIXTURE);
function_fixture_test!(st_pointn, "ST_POINTN", FIXTURE);
function_fixture_test!(st_polyfromtext, "ST_POLYFROMTEXT", FIXTURE);
function_fixture_test!(st_polyfromwkb, "ST_POLYFROMWKB", FIXTURE);
function_fixture_test!(st_polygonfromtext, "ST_POLYGONFROMTEXT", FIXTURE);
function_fixture_test!(st_polygonfromwkb, "ST_POLYGONFROMWKB", FIXTURE);
function_fixture_test!(st_simplify, "ST_SIMPLIFY", FIXTURE);
function_fixture_test!(st_srid, "ST_SRID", FIXTURE);
function_fixture_test!(st_startpoint, "ST_STARTPOINT", FIXTURE);
function_fixture_test!(st_swapxy, "ST_SWAPXY", FIXTURE);
function_fixture_test!(st_touches, "ST_TOUCHES", FIXTURE);
function_fixture_test!(st_transform, "ST_TRANSFORM", FIXTURE);
function_fixture_test!(st_validate, "ST_VALIDATE", FIXTURE);
function_fixture_test!(st_within, "ST_WITHIN", FIXTURE);
function_fixture_test!(st_x, "ST_X", FIXTURE);
function_fixture_test!(st_y, "ST_Y", FIXTURE);
