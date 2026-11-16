# Vector Functions

For HNSW/IVF query behavior, native datatypes, WITH options, quantization,
training, storage limits, and recovery, see [Vector indexes](../../vector-indexes.md).

| Function | Description |
| --- | --- |
| [`COSINE_DISTANCE`](cosine_distance.md) | Computes the cosine distance between two vectors. |
| [`DISTANCE`](distance.md) | Computes the distance between two vectors using the specified metric, defaulting to L2 for numeric vectors and HAMMING for BINARY. |
| [`DOT_PRODUCT`](dot_product.md) | Computes the dot product of two vectors. |
| [`FROM_VECTOR`](from_vector.md) | Converts a vector to its text representation; an alias of `VECTOR_TO_STRING`. |
| [`INNER_PRODUCT`](inner_product.md) | Computes the inner product of two vectors; an alias of `DOT_PRODUCT`. |
| [`L2_DISTANCE`](l2_distance.md) | Computes the Euclidean distance between two vectors. |
| [`L2_NORMALIZE`](l2_normalize.md) | Normalizes a vector to a unit vector. |
| [`MAX_INNER_PRODUCT_DISTANCE`](max_inner_product_distance.md) | Returns the negation of the inner product, used for maximum inner product nearest-neighbor ordering. |
| [`STRING_TO_VECTOR`](string_to_vector.md) | Parses a text representation into a vector. |
| [`SUBVECTOR`](subvector.md) | Extracts a subvector by start position and length. |
| [`TO_VECTOR`](to_vector.md) | Parses a text representation into a vector; an alias of `STRING_TO_VECTOR`. |
| [`VECTOR_DIM`](vector_dim.md) | Returns the dimension of a vector. |
| [`VECTOR_NORM`](vector_norm.md) | Returns the L2 norm of a vector. |
| [`VECTOR_TO_STRING`](vector_to_string.md) | Converts a vector to its text representation. |
| [`VEC_DISTANCE`](vec_distance.md) | Computes the vector distance using the specified metric; an alias of `DISTANCE`. |

## Native datatype functions

| Function | Description |
| --- | --- |
| [`VECTOR_DATATYPE`](vector_datatype.md)(v) | Returns the native datatype name. |
| [`VECTOR_FROM_BINARY`](vector_from_binary.md)(payload, datatype, dimension) | Constructs a vector from explicit native bytes; validates dimensions, finite floats and binary padding. |
| [`VECTOR_TO_BINARY`](vector_to_binary.md)(v) | Returns native payload bytes without an envelope. |
| [`HAMMING_DISTANCE`](hamming_distance.md)(a,b) | Counts differing bits of equal-dimension BINARY vectors. |
| [`JACCARD_DISTANCE`](jaccard_distance.md)(a,b) | Returns binary Jaccard distance; two empty sets have distance zero. |

`SUBVECTOR` preserves datatype and slices BINARY by bits. `VECTOR_NORM`
accepts numeric types. `L2_NORMALIZE` returns FLOAT32 and checks its dimension
limit. Norm and normalization reject BINARY. See [native datatypes](../../vector-indexes.md).


## Native datatype limits

| Datatype | Maximum dimension | Element/payload rules |
| --- | ---: | --- |
| FLOAT32 | 16383 | IEEE binary32, finite after conversion, 4 little-endian bytes/element |
| FLOAT16 | 32766 | IEEE binary16, finite after conversion, 2 little-endian bytes/element |
| BFLOAT16 | 32766 | bfloat16 rounding, finite after conversion, 2 little-endian bytes/element |
| INT8 | 65532 | Integral coordinates in [-128,127], signed two's-complement bytes |
| BINARY | 524256 bits | Coordinates 0/1, bit i stored at i%8 in byte i/8; unused tail bits zero |

Vectors must be nonempty; dimensions/payload length and finite-coordinate checks
are enforced. Reduced float datatypes round to their precision, rather than
retaining every decimal input digit. Numeric datatypes can participate in mixed
numeric distance evaluation. Numeric-to-BINARY conversion is not implicit.
STRING_TO_VECTOR/TO_VECTOR can convert an existing numeric VECTOR, defaulting to
FLOAT32 when datatype is omitted. Raw payloads require explicit datatype and dimension.

All vector functions validate arity, then return SQL NULL for any SQL NULL operand.
Invalid encodings, scalar integer operands, incompatible datatypes and invalid
slices raise a vector error. SUBVECTOR is one-based and requires the entire positive
slice to fit; BINARY positions/counts are bits, with repacked zero tail padding.
L2_NORMALIZE returns FLOAT32, enforces its dimension bound and preserves a zero
vector after conversion. COSINE_DISTANCE on a zero norm returns NULL.

Numeric distance returns actual Euclidean L2, cosine distance or negative inner
product as documented. DOT_PRODUCT returns the positive inner product. Binary
HAMMING/JACCARD have their own semantics; index configuration must match the query
metric. Use the documented ORDER BY distance LIMIT query shapes and EXPLAIN in
[Vector indexes](../../vector-indexes.md#distances-and-queries); wrapping an arbitrary
expression does not by itself guarantee an ANN access path.

The default vector_result_format is NATIVE. Use VECTOR_TO_STRING for a portable
text display, or explicitly set the session format to TEXT for text-protocol
results. Prepared binary results retain the native wire type. VECTOR_TO_BINARY
exports payload only; transport datatype/dimension alongside it.
