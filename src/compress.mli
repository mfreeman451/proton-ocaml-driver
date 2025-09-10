(** {1 Compression Module}

    Provides compression and decompression functionality for the Proton wire protocol. Supports LZ4
    and ZSTD compression methods with CityHash checksums for data integrity.

    This module handles the low-level compression format used by ClickHouse/Proton, including proper
    framing with checksums and headers.

    @since 1.0.0 *)

(** {2 Exceptions} *)

exception Compression_error of string
(** Exception raised when compression or decompression operations fail.

    @param message Detailed error description

    @since 1.0.0 *)

exception Checksum_mismatch of string
(** Exception raised when data integrity check fails during decompression.

    @param message Details about the checksum mismatch

    @since 1.0.0 *)

(** {2 Types} *)

(** Compression methods supported by the Proton protocol.

    - [None]: No compression, data is sent raw
    - [LZ4]: LZ4 compression for fast compression/decompression
    - [ZSTD]: Zstandard compression for better compression ratios

    @since 1.0.0 *)
type method_t =
  | None  (** No compression *)
  | LZ4  (** LZ4 compression - fast with reasonable compression *)
  | ZSTD  (** ZSTD compression - slower but better compression ratio *)

(** {2 Protocol Conversion} *)

val method_to_byte : method_t -> int
(** [method_to_byte method] converts a compression method to its wire protocol byte value.

    @param method The compression method to convert
    @return The byte value used in the protocol (0x82 for LZ4, 0x90 for ZSTD)

    @since 1.0.0 *)

val method_of_byte : int -> method_t
(** [method_of_byte byte] converts a wire protocol byte to a compression method.

    @param byte The protocol byte value
    @return The corresponding compression method
    @raise Invalid_argument if the byte value is not recognized

    @since 1.0.0 *)

(** {2 Constants} *)

val max_block_size : int
(** Maximum size of a single compression block (1MB).

    Data larger than this will be split into multiple blocks.

    @since 1.0.0 *)

val checksum_size : int
(** Size of the CityHash checksum in bytes (16 bytes for CityHash128).

    @since 1.0.0 *)

val compress_header_size : int
(** Size of the compression header (1 byte method + 4 bytes compressed size + 4 bytes uncompressed
    size).

    @since 1.0.0 *)

val header_size : int
(** Total header size including checksum (25 bytes total).

    @since 1.0.0 *)

(** {2 Compression Functions} *)

val compress_lz4 : bytes -> bytes
(** [compress_lz4 data] compresses data using LZ4 algorithm.

    @param data The raw data to compress
    @return Compressed data bytes
    @raise Compression_error if compression fails

    @since 1.0.0 *)

val decompress_lz4 : bytes -> int -> bytes
(** [decompress_lz4 data uncompressed_size] decompresses LZ4 compressed data.

    @param data The compressed data
    @param uncompressed_size Expected size of uncompressed data
    @return Decompressed data bytes
    @raise Compression_error if decompression fails

    @since 1.0.0 *)

val compress_zstd : bytes -> bytes
(** [compress_zstd data] compresses data using ZSTD algorithm.

    @param data The raw data to compress
    @return Compressed data bytes
    @raise Compression_error if compression fails

    @since 1.0.0 *)

val decompress_zstd : bytes -> int -> bytes
(** [decompress_zstd data uncompressed_size] decompresses ZSTD compressed data.

    @param data The compressed data
    @param uncompressed_size Expected size of uncompressed data
    @return Decompressed data bytes
    @raise Compression_error if decompression fails

    @since 1.0.0 *)

(** {2 I/O Functions} *)

val write_compressed_block_lwt : (string list -> unit Lwt.t) -> bytes -> method_t -> unit Lwt.t
(** [write_compressed_block_lwt writer data method] writes a compressed block using Lwt.

    Optimized function that writes compressed data with proper framing while avoiding unnecessary
    allocations. Handles checksum calculation and header formatting according to the Proton
    protocol.

    Streams in <= 1MB frames: if [data] exceeds {!max_block_size}, this will emit multiple
    compression frames back-to-back via [writer]. The caller should send any protocol packet header
    once before invoking this function.

    @param writer Function to write string list to output
    @param data Raw data to compress and write
    @param method Compression method to use
    @return Promise that resolves when write is complete

    Example:
    {[
      let write_data conn data =
        Compress.write_compressed_block_lwt (Connection.write_raw conn) data LZ4
    ]}

    @since 1.0.0 *)

(** {2 Streaming API} *)

(** Streaming compression writer that accepts incremental writes and emits protocol-framed
    compressed blocks (<= 1MB each). *)
module Stream : sig
  type t

  val create : (string list -> unit Lwt.t) -> method_t -> t
  (** [create writer method] creates a new streaming compressor. The caller should have already
      written any protocol packet header. *)

  val write_bytes : t -> bytes -> int -> int -> unit Lwt.t
  (** [write_bytes t b off len] appends [len] bytes from [b] at [off] into the stream. When the
      internal buffer reaches {!max_block_size}, a frame is compressed and written. *)

  val write_string : t -> string -> unit Lwt.t
  (** [write_string t s] convenience wrapper for strings. *)

  val write_char : t -> char -> unit Lwt.t
  (** [write_char t c] writes a single byte. *)

  val flush : t -> unit Lwt.t
  (** [flush t] compresses and writes any buffered data as a final frame. *)
end
