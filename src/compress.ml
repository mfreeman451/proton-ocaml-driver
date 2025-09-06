(* LZ4 compression support for ClickHouse/Proton protocol *)

exception Compression_error of string
exception Checksum_mismatch of string

type method_t = 
  | None 
  | LZ4 
  | ZSTD

let method_to_byte = function
  | None -> 0x02
  | LZ4 -> 0x82
  | ZSTD -> 0x90

let method_of_byte = function
  | 0x02 -> None
  | 0x82 -> LZ4
  | 0x90 -> ZSTD
  | b -> failwith (Printf.sprintf "Unknown compression method: 0x%02x" b)

(* Constants matching Go driver *)
let checksum_size = 16
let compress_header_size = 1 + 4 + 4  (* method + compressed_size + uncompressed_size *)
let header_size = checksum_size + compress_header_size
let max_block_size = 1 lsl 20  (* 1MB *)

(* Note: We don't need the compressed_block record type since we handle
   compression frame format directly in read/write functions *)

(* Compress data using LZ4 *)
let compress_lz4 (data : bytes) : bytes =
  LZ4.Bytes.compress data

(* Decompress LZ4 data *)
let decompress_lz4 (data : bytes) (uncompressed_size : int) : bytes =
  LZ4.Bytes.decompress ~length:uncompressed_size data

(* Compress data using ZSTD *)
let compress_zstd (data : bytes) : bytes =
  let data_str = Bytes.to_string data in
  let compressed = Zstd.compress ~level:3 data_str in
  Bytes.of_string compressed

(* Decompress ZSTD data *)
let decompress_zstd (data : bytes) (uncompressed_size : int) : bytes =
  let data_str = Bytes.to_string data in
  let decompressed = Zstd.decompress uncompressed_size data_str in
  Bytes.of_string decompressed

(* Build-a-frame path removed in favor of writev-based writers *)

(* Legacy sync write/read paths removed *)

(* New: Lwt-friendly write that avoids frame + checksum_input allocations *)
let write_compressed_block_lwt
    (writev_fn : string list -> unit Lwt.t)
    (data : bytes)
    (method_ : method_t) : unit Lwt.t =
  match method_ with
  | None ->
      (* Write as-is; rely on caller to send uncompressed payloads appropriately *)
      writev_fn [Bytes.unsafe_to_string data]
  | LZ4 | ZSTD ->
      let uncompressed_size = Bytes.length data in
      let compressed_data =
        match method_ with
        | LZ4  -> compress_lz4 data
        | ZSTD -> compress_zstd data
        | None -> assert false
      in
      let compressed_size = Bytes.length compressed_data in
      (* header: method(1) + compressed_size(4) + uncompressed_size(4) *)
      let header = Bytes.create compress_header_size in
      let method_byte =
        match method_ with LZ4 -> method_to_byte LZ4 | ZSTD -> method_to_byte ZSTD | None -> assert false
      in
      Bytes.set header 0 (Char.chr method_byte);
      Binary.bytes_set_int32_le header 1 (Int32.of_int (compressed_size + compress_header_size));
      Binary.bytes_set_int32_le header 5 (Int32.of_int uncompressed_size);

      (* checksum over header+payload without concatenating them *)
      let hash = Cityhash.cityhash128_2sub header 0 compress_header_size compressed_data 0 compressed_size in
      let checksum = Cityhash.to_bytes hash in

      writev_fn
        [ Bytes.unsafe_to_string checksum
        ; Bytes.unsafe_to_string header
        ; Bytes.unsafe_to_string compressed_data
        ]

(* Legacy buffered reader/writer APIs removed *)
