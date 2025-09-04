(** LZ4 compression support for ClickHouse/Proton protocol *)

(** Exception raised when compression/decompression fails *)
exception Compression_error of string

(** Exception raised when checksum verification fails *)
exception Checksum_mismatch of string

(** Compression methods supported by ClickHouse *)
type method_t = 
  | None   (** No compression *)
  | LZ4    (** LZ4 compression *)
  | ZSTD   (** ZSTD compression (not yet implemented) *)

(** Convert compression method to byte value used in protocol *)
val method_to_byte : method_t -> int

(** Convert byte value from protocol to compression method *)
val method_of_byte : int -> method_t

(** Maximum block size for compression (1MB) *)
val max_block_size : int

(** Header size constants *)
val checksum_size : int
val compress_header_size : int
val header_size : int

(** Compress data using LZ4 *)
val compress_lz4 : bytes -> bytes

(** Decompress LZ4 data with known uncompressed size *)
val decompress_lz4 : bytes -> int -> bytes

(** Write a compressed block to output channel with proper ClickHouse framing *)
val write_compressed_block : out_channel -> bytes -> method_t -> unit

(** Read a compressed block from input channel and decompress it *)
val read_compressed_block : in_channel -> method_t -> bytes

(** Streaming decompression reader *)
type reader

(** Create a new streaming reader *)
val create_reader : in_channel -> method_t -> reader

(** Read data from streaming reader *)
val read : reader -> bytes -> int -> int -> int

(** Streaming compression writer *)
type writer

(** Create a new streaming writer *)
val create_writer : out_channel -> method_t -> writer

(** Write data to streaming writer *)
val write : writer -> bytes -> int -> int -> unit

(** Flush pending data in streaming writer *)
val flush : writer -> unit