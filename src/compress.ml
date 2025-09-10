(* LZ4 compression support for ClickHouse/Proton protocol *)
open Lwt.Syntax
open Printf
module U = Unix

exception Compression_error of string
exception Checksum_mismatch of string

type method_t = None | LZ4 | ZSTD

let method_to_byte = function None -> 0x02 | LZ4 -> 0x82 | ZSTD -> 0x90

let method_of_byte = function
  | 0x02 -> None
  | 0x82 -> LZ4
  | 0x90 -> ZSTD
  | b -> failwith (Printf.sprintf "Unknown compression method: 0x%02x" b)

(* Constants matching Go driver *)
let checksum_size = 16
let compress_header_size = 1 + 4 + 4 (* method + compressed_size + uncompressed_size *)
let header_size = checksum_size + compress_header_size
let max_block_size = 1 lsl 20 (* 1MB *)

(* Note: We don't need the compressed_block record type since we handle
   compression frame format directly in read/write functions *)

(* Compress data using LZ4 *)
let compress_lz4 (data : bytes) : bytes = LZ4.Bytes.compress data

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

(* Lwt-friendly write that avoids frame + checksum_input allocations *)
let write_compressed_block_lwt (writev_fn : string list -> unit Lwt.t) (data : bytes)
    (method_ : method_t) : unit Lwt.t =
  match method_ with
  | None ->
      (* Write as-is; rely on caller to send uncompressed payloads appropriately *)
      writev_fn [ Bytes.unsafe_to_string data ]
  | LZ4 | ZSTD ->
      (* Stream in <= 1MB chunks, each with its own header+checksum. *)
      let total_len = Bytes.length data in
      let rec loop off =
        if off >= total_len then Lwt.return_unit
        else
          let remain = total_len - off in
          let chunk_len = if remain > max_block_size then max_block_size else remain in
          let chunk = Bytes.sub data off chunk_len in
          let compressed_chunk =
            match method_ with
            | LZ4 -> compress_lz4 chunk
            | ZSTD -> compress_zstd chunk
            | None -> assert false
          in
          let compressed_size = Bytes.length compressed_chunk in
          let total = checksum_size + compress_header_size + compressed_size in
          let frame = Bytes.create total in
          let method_byte =
            match method_ with
            | LZ4 -> method_to_byte LZ4
            | ZSTD -> method_to_byte ZSTD
            | None -> assert false
          in
          (* header inside frame *)
          Bytes.set frame checksum_size (Char.chr method_byte);
          Binary.bytes_set_int32_le frame (checksum_size + 1)
            (Int32.of_int (compressed_size + compress_header_size));
          Binary.bytes_set_int32_le frame (checksum_size + 5) (Int32.of_int chunk_len);
          Bytes.blit compressed_chunk 0 frame (checksum_size + compress_header_size) compressed_size;
          (* checksum over contiguous header+payload slice (method-first)
             Use C++ CityHash128 binding to match ClickHouse exactly. *)
          let h_cxx =
            Cityhash.cityhash128_sub frame checksum_size (compress_header_size + compressed_size)
          in
          let checksum = Bytes.create 16 in
          Binary.bytes_set_int64_le checksum 0 h_cxx.low;
          Binary.bytes_set_int64_le checksum 8 h_cxx.high;
          (* debug: also compute alternate variants to compare with server *)
          let dbg =
            match Sys.getenv_opt "PROTON_DEBUG" with
            | Some ("1" | "true" | "TRUE" | "yes" | "YES") -> true
            | _ -> false
          in
          if dbg then (
            let hex s =
              let len = Bytes.length s in
              let out = Bytes.create (2 * len) in
              let hexdig = "0123456789abcdef" in
              for i = 0 to len - 1 do
                let v = Char.code (Bytes.get s i) in
                Bytes.set out (2 * i) (String.unsafe_get hexdig ((v lsr 4) land 0xF));
                Bytes.set out ((2 * i) + 1) (String.unsafe_get hexdig (v land 0xF))
              done;
              Bytes.unsafe_to_string out
            in
            (* sizes-first alt header *)
            let alt = Bytes.create (compress_header_size + compressed_size) in
            Binary.bytes_set_int32_le alt 0 (Int32.of_int (compressed_size + compress_header_size));
            Binary.bytes_set_int32_le alt 4 (Int32.of_int chunk_len);
            Bytes.set alt 8 (Char.chr method_byte);
            Bytes.blit compressed_chunk 0 alt compress_header_size compressed_size;
            let h_alt = Cityhash.cityhash128_sub alt 0 (Bytes.length alt) in
            let c_sizes_first = Cityhash.to_bytes h_alt in
            printf
              "[compress] frame(method=0x%02x) comp_size=%d uncomp=%d checksum=%s \
               alt_sizes_first=%s use=%s\n\
               %!"
              method_byte
              (compressed_size + compress_header_size)
              chunk_len (hex checksum) (hex c_sizes_first) "cxx";
            (* Dump hashed slice header and first 32 bytes of payload for verification *)
            let hdr = Bytes.sub frame checksum_size compress_header_size in
            let payload0 =
              let take = min 32 compressed_size in
              Bytes.sub frame (checksum_size + compress_header_size) take
            in
            printf "[compress] hashed_hdr=%s payload0=%s hashed_len=%d\n%!" (hex hdr) (hex payload0)
              (compress_header_size + compressed_size);
            (* Optional full dump of hashed slice to files for debugging *)
            match Sys.getenv_opt "PROTON_DUMP_HASH" with
            | Some ("1" | "true" | "TRUE" | "yes" | "YES") ->
                let to_hash_len = compress_header_size + compressed_size in
                let to_hash = Bytes.create to_hash_len in
                Bytes.blit frame checksum_size to_hash 0 compress_header_size;
                Bytes.blit frame
                  (checksum_size + compress_header_size)
                  to_hash compress_header_size compressed_size;
                (try if not (Sys.file_exists "debug") then U.mkdir "debug" 0o755 with _ -> ());
                let ts = int_of_float (U.gettimeofday ()) in
                let base =
                  sprintf "debug/hashed_m%02x_c%d_u%d_t%d" method_byte
                    (compressed_size + compress_header_size)
                    chunk_len ts
                in
                let binf = base ^ ".bin" in
                let hexf = base ^ ".hex" in
                let metaf = base ^ ".meta" in
                let ocb = open_out_bin binf in
                output_bytes ocb to_hash;
                close_out ocb;
                let och = open_out hexf in
                let n = Bytes.length to_hash in
                for i = 0 to n - 1 do
                  let v = Char.code (Bytes.get to_hash i) in
                  output_string och (Printf.sprintf "%02x" v)
                done;
                close_out och;
                let ocm = open_out metaf in
                fprintf ocm
                  "method=0x%02x\ncomp_size=%d\nuncomp=%d\nhash_len=%d\nuse=%s\nchecksum=%s\n"
                  method_byte
                  (compressed_size + compress_header_size)
                  chunk_len to_hash_len "cxx" (hex checksum);
                close_out ocm
            | _ -> ());
          let dbg =
            match Sys.getenv_opt "PROTON_DEBUG" with
            | Some ("1" | "true" | "TRUE" | "yes" | "YES") -> true
            | _ -> false
          in
          (if dbg then
             let hex s =
               let len = Bytes.length s in
               let out = Bytes.create (2 * len) in
               let hexdig = "0123456789abcdef" in
               for i = 0 to len - 1 do
                 let v = Char.code (Bytes.get s i) in
                 Bytes.set out (2 * i) (String.unsafe_get hexdig ((v lsr 4) land 0xF));
                 Bytes.set out ((2 * i) + 1) (String.unsafe_get hexdig (v land 0xF))
               done;
               Bytes.unsafe_to_string out
             in
             printf "[compress] frame(method=0x%02x) comp_size=%d uncomp=%d checksum=%s\n%!"
               method_byte
               (compressed_size + compress_header_size)
               chunk_len (hex checksum));

          Bytes.blit checksum 0 frame 0 checksum_size;
          let dbg =
            match Sys.getenv_opt "PROTON_DEBUG" with
            | Some ("1" | "true" | "TRUE" | "yes" | "YES") -> true
            | _ -> false
          in
          (if dbg then
             let hex s =
               let len = Bytes.length s in
               let out = Bytes.create (2 * len) in
               let hexdig = "0123456789abcdef" in
               for i = 0 to len - 1 do
                 let v = Char.code (Bytes.get s i) in
                 Bytes.set out (2 * i) (String.unsafe_get hexdig ((v lsr 4) land 0xF));
                 Bytes.set out ((2 * i) + 1) (String.unsafe_get hexdig (v land 0xF))
               done;
               Bytes.unsafe_to_string out
             in
             let onwire = Bytes.sub frame 0 16 in
             printf "[compress] onwire checksum=%s\n%!" (hex onwire));
          let* () = writev_fn [ Bytes.unsafe_to_string frame ] in
          loop (off + chunk_len)
      in
      loop 0

module Stream = struct
  type t = {
    method_ : method_t;
    writev : string list -> unit Lwt.t;
    buf : bytes; (* uncompressed accumulation buffer *)
    mutable off : int; (* current fill offset *)
  }

  let create writev method_ = { method_; writev; buf = Bytes.create max_block_size; off = 0 }

  let[@inline] write_frame (t : t) (chunk : bytes) (len : int) : unit Lwt.t =
    match t.method_ with
    | None ->
        (* No compression: write raw chunk directly. This path is not
           currently used by callers, but keep it safe. *)
        t.writev [ Bytes.unsafe_to_string (Bytes.sub chunk 0 len) ]
    | LZ4 | ZSTD ->
        let to_compress = if len = Bytes.length chunk then chunk else Bytes.sub chunk 0 len in
        let compressed =
          match t.method_ with
          | LZ4 -> compress_lz4 to_compress
          | ZSTD -> compress_zstd to_compress
          | None -> assert false
        in
        let compressed_size = Bytes.length compressed in
        let total = checksum_size + compress_header_size + compressed_size in
        let frame = Bytes.create total in
        let method_byte =
          match t.method_ with
          | LZ4 -> method_to_byte LZ4
          | ZSTD -> method_to_byte ZSTD
          | None -> assert false
        in
        (* method-first header layout inside frame *)
        Bytes.set frame checksum_size (Char.chr method_byte);
        Binary.bytes_set_int32_le frame (checksum_size + 1)
          (Int32.of_int (compressed_size + compress_header_size));
        Binary.bytes_set_int32_le frame (checksum_size + 5) (Int32.of_int len);
        Bytes.blit compressed 0 frame (checksum_size + compress_header_size) compressed_size;
        (* Use C++ CityHash128 binding to match ClickHouse exactly. *)
        let h_cxx =
          Cityhash.cityhash128_sub frame checksum_size (compress_header_size + compressed_size)
        in
        let checksum = Bytes.create 16 in
        Binary.bytes_set_int64_le checksum 0 h_cxx.low;
        Binary.bytes_set_int64_le checksum 8 h_cxx.high;
        let dbg =
          match Sys.getenv_opt "PROTON_DEBUG" with
          | Some ("1" | "true" | "TRUE" | "yes" | "YES") -> true
          | _ -> false
        in
        if dbg then (
          let hex s =
            let len = Bytes.length s in
            let out = Bytes.create (2 * len) in
            let hexdig = "0123456789abcdef" in
            for i = 0 to len - 1 do
              let v = Char.code (Bytes.get s i) in
              Bytes.set out (2 * i) (String.unsafe_get hexdig ((v lsr 4) land 0xF));
              Bytes.set out ((2 * i) + 1) (String.unsafe_get hexdig (v land 0xF))
            done;
            Bytes.unsafe_to_string out
          in
          let alt = Bytes.create (compress_header_size + compressed_size) in
          Binary.bytes_set_int32_le alt 0 (Int32.of_int (compressed_size + compress_header_size));
          Binary.bytes_set_int32_le alt 4 (Int32.of_int len);
          Bytes.set alt 8 (Char.chr method_byte);
          Bytes.blit compressed 0 alt compress_header_size compressed_size;
          let h_alt = Cityhash.cityhash128_sub alt 0 (Bytes.length alt) in
          let c_sizes_first = Cityhash.to_bytes h_alt in
          printf
            "[compress] frame(method=0x%02x) comp_size=%d uncomp=%d checksum=%s alt_sizes_first=%s \
             use=%s\n\
             %!"
            method_byte
            (compressed_size + compress_header_size)
            len (hex checksum) (hex c_sizes_first) "cxx";
          let hdr = Bytes.sub frame checksum_size compress_header_size in
          let payload0 =
            let take = min 32 compressed_size in
            Bytes.sub frame (checksum_size + compress_header_size) take
          in
          printf "[compress] hashed_hdr=%s payload0=%s hashed_len=%d\n%!" (hex hdr) (hex payload0)
            (compress_header_size + compressed_size);
          match Sys.getenv_opt "PROTON_DUMP_HASH" with
          | Some ("1" | "true" | "TRUE" | "yes" | "YES") ->
              let to_hash_len = compress_header_size + compressed_size in
              let to_hash = Bytes.create to_hash_len in
              Bytes.blit frame checksum_size to_hash 0 compress_header_size;
              Bytes.blit frame
                (checksum_size + compress_header_size)
                to_hash compress_header_size compressed_size;
              (try if not (Sys.file_exists "debug") then U.mkdir "debug" 0o755 with _ -> ());
              let ts = int_of_float (U.gettimeofday ()) in
              let base =
                sprintf "debug/hashed_m%02x_c%d_u%d_t%d" method_byte
                  (compressed_size + compress_header_size)
                  len ts
              in
              let ocb = open_out_bin (base ^ ".bin") in
              output_bytes ocb to_hash;
              close_out ocb;
              let och = open_out (base ^ ".hex") in
              let n = Bytes.length to_hash in
              for i = 0 to n - 1 do
                let v = Char.code (Bytes.get to_hash i) in
                output_string och (Printf.sprintf "%02x" v)
              done;
              close_out och;
              let ocm = open_out (base ^ ".meta") in
              fprintf ocm
                "method=0x%02x\ncomp_size=%d\nuncomp=%d\nhash_len=%d\nuse=%s\nchecksum=%s\n"
                method_byte
                (compressed_size + compress_header_size)
                len to_hash_len "cxx" (hex checksum);
              close_out ocm
          | _ -> ());
        let dbg =
          match Sys.getenv_opt "PROTON_DEBUG" with
          | Some ("1" | "true" | "TRUE" | "yes" | "YES") -> true
          | _ -> false
        in
        (if dbg then
           let hex s =
             let len = Bytes.length s in
             let out = Bytes.create (2 * len) in
             let hexdig = "0123456789abcdef" in
             for i = 0 to len - 1 do
               let v = Char.code (Bytes.get s i) in
               Bytes.set out (2 * i) (String.unsafe_get hexdig ((v lsr 4) land 0xF));
               Bytes.set out ((2 * i) + 1) (String.unsafe_get hexdig (v land 0xF))
             done;
             Bytes.unsafe_to_string out
           in
           printf "[compress] frame(method=0x%02x) comp_size=%d uncomp=%d checksum=%s\n%!"
             method_byte
             (compressed_size + compress_header_size)
             len (hex checksum));
        Bytes.blit checksum 0 frame 0 checksum_size;
        let dbg =
          match Sys.getenv_opt "PROTON_DEBUG" with
          | Some ("1" | "true" | "TRUE" | "yes" | "YES") -> true
          | _ -> false
        in
        (if dbg then
           let hex s =
             let len = Bytes.length s in
             let out = Bytes.create (2 * len) in
             let hexdig = "0123456789abcdef" in
             for i = 0 to len - 1 do
               let v = Char.code (Bytes.get s i) in
               Bytes.set out (2 * i) (String.unsafe_get hexdig ((v lsr 4) land 0xF));
               Bytes.set out ((2 * i) + 1) (String.unsafe_get hexdig (v land 0xF))
             done;
             Bytes.unsafe_to_string out
           in
           let onwire = Bytes.sub frame 0 16 in
           printf "[compress] onwire checksum=%s\n%!" (hex onwire));
        t.writev [ Bytes.unsafe_to_string frame ]

  let rec write_bytes (t : t) (b : bytes) (off : int) (len : int) : unit Lwt.t =
    if len = 0 then Lwt.return_unit
    else
      let remaining = max_block_size - t.off in
      if len <= remaining then (
        Bytes.blit b off t.buf t.off len;
        t.off <- t.off + len;
        Lwt.return_unit)
      else (
        (* fill up, flush frame, continue *)
        Bytes.blit b off t.buf t.off remaining;
        let* () = write_frame t t.buf max_block_size in
        t.off <- 0;
        write_bytes t b (off + remaining) (len - remaining))

  let write_string (t : t) (s : string) : unit Lwt.t =
    write_bytes t (Bytes.unsafe_of_string s) 0 (String.length s)

  let write_char (t : t) (c : char) : unit Lwt.t =
    if t.off < max_block_size then (
      Bytes.set t.buf t.off c;
      t.off <- t.off + 1;
      Lwt.return_unit)
    else
      let* () = write_frame t t.buf max_block_size in
      t.off <- 0;
      Bytes.set t.buf 0 c;
      t.off <- 1;
      Lwt.return_unit

  let flush (t : t) : unit Lwt.t =
    if t.off = 0 then Lwt.return_unit
    else
      let* () = write_frame t t.buf t.off in
      t.off <- 0;
      Lwt.return_unit
end
