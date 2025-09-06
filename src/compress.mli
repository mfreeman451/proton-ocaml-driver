(** LZ4 compression support for ClickHouse/Proton protocol *)

(** Exception raised when compression/decompression fails *)
exception Compression_error of string

(** Exception raised when checksum verification fails *)
exception Checksum_mismatch of string

(** Compression methods supported by ClickHouse *)
type method_t = 
  | None   (** No compression *)
  | LZ4    (** LZ4 compression *)
  | ZSTD   (** ZSTD compression *)

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

(** Compress data using ZSTD *)
val compress_zstd : bytes -> bytes

(** Decompress ZSTD data with known uncompressed size *)
val decompress_zstd : bytes -> int -> bytes

(** New: Lwt-friendly write that avoids frame + checksum_input allocations *)
val write_compressed_block_lwt : (string list -> unit Lwt.t) -> bytes -> method_t -> unit Lwt.t
(* Deprecated sync and reader APIs removed. *)
