(** {1 Compression Module}
    
    Provides compression and decompression functionality for the Proton wire protocol.
    Supports LZ4 and ZSTD compression methods with CityHash checksums for data integrity.
    
    This module handles the low-level compression format used by ClickHouse/Proton,
    including proper framing with checksums and headers.
    
    @since 1.0.0
*)

(** {2 Exceptions} *)

(** Exception raised when compression or decompression operations fail.
    
    @param message Detailed error description
    
    @since 1.0.0 *)
exception Compression_error of string

(** Exception raised when data integrity check fails during decompression.
    
    @param message Details about the checksum mismatch
    
    @since 1.0.0 *)
exception Checksum_mismatch of string

(** {2 Types} *)

(** Compression methods supported by the Proton protocol.
    
    - [None]: No compression, data is sent raw
    - [LZ4]: LZ4 compression for fast compression/decompression
    - [ZSTD]: Zstandard compression for better compression ratios
    
    @since 1.0.0 *)
type method_t = 
  | None   (** No compression *)
  | LZ4    (** LZ4 compression - fast with reasonable compression *)
  | ZSTD   (** ZSTD compression - slower but better compression ratio *)

(** {2 Protocol Conversion} *)

(** [method_to_byte method] converts a compression method to its wire protocol byte value.
    
    @param method The compression method to convert
    @return The byte value used in the protocol (0x82 for LZ4, 0x90 for ZSTD)
    
    @since 1.0.0 *)
val method_to_byte : method_t -> int

(** [method_of_byte byte] converts a wire protocol byte to a compression method.
    
    @param byte The protocol byte value
    @return The corresponding compression method
    @raise Invalid_argument if the byte value is not recognized
    
    @since 1.0.0 *)
val method_of_byte : int -> method_t

(** {2 Constants} *)

(** Maximum size of a single compression block (1MB).
    
    Data larger than this will be split into multiple blocks.
    
    @since 1.0.0 *)
val max_block_size : int

(** Size of the CityHash checksum in bytes (16 bytes for CityHash128).
    
    @since 1.0.0 *)
val checksum_size : int

(** Size of the compression header (1 byte method + 4 bytes compressed size + 4 bytes uncompressed size).
    
    @since 1.0.0 *)
val compress_header_size : int

(** Total header size including checksum (25 bytes total).
    
    @since 1.0.0 *)
val header_size : int

(** {2 Compression Functions} *)

(** [compress_lz4 data] compresses data using LZ4 algorithm.
    
    @param data The raw data to compress
    @return Compressed data bytes
    @raise Compression_error if compression fails
    
    @since 1.0.0 *)
val compress_lz4 : bytes -> bytes

(** [decompress_lz4 data uncompressed_size] decompresses LZ4 compressed data.
    
    @param data The compressed data
    @param uncompressed_size Expected size of uncompressed data
    @return Decompressed data bytes
    @raise Compression_error if decompression fails
    
    @since 1.0.0 *)
val decompress_lz4 : bytes -> int -> bytes

(** [compress_zstd data] compresses data using ZSTD algorithm.
    
    @param data The raw data to compress
    @return Compressed data bytes
    @raise Compression_error if compression fails
    
    @since 1.0.0 *)
val compress_zstd : bytes -> bytes

(** [decompress_zstd data uncompressed_size] decompresses ZSTD compressed data.
    
    @param data The compressed data
    @param uncompressed_size Expected size of uncompressed data
    @return Decompressed data bytes
    @raise Compression_error if decompression fails
    
    @since 1.0.0 *)
val decompress_zstd : bytes -> int -> bytes

(** {2 I/O Functions} *)

(** [write_compressed_block_lwt writer data method] writes a compressed block using Lwt.
    
    Optimized function that writes compressed data with proper framing while
    avoiding unnecessary allocations. Handles checksum calculation and header
    formatting according to the Proton protocol.
    
    @param writer Function to write string list to output
    @param data Raw data to compress and write
    @param method Compression method to use
    @return Promise that resolves when write is complete
    
    Example:
    {[
      let write_data conn data =
        Compress.write_compressed_block_lwt 
          (Connection.write_raw conn)
          data
          LZ4
    ]}
    
    @since 1.0.0 *)
val write_compressed_block_lwt : (string list -> unit Lwt.t) -> bytes -> method_t -> unit Lwt.t
