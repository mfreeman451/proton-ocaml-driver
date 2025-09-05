(* LZ4 compression support for ClickHouse/Proton protocol *)

open Binary

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

(* Write a compressed block to output channel *)
let write_compressed_block oc (data : bytes) (method_ : method_t) =
  match method_ with
  | None ->
      (* No compression - just write the data *)
      output oc data 0 (Bytes.length data)
  | LZ4 ->
      let uncompressed_size = Bytes.length data in
      
      (* Compress the data *)
      let compressed_data = compress_lz4 data in
      let compressed_size = Bytes.length compressed_data in
      
      (* Create header: method (1) + compressed_size (4) + uncompressed_size (4) *)
      let header = Bytes.create compress_header_size in
      Bytes.set header 0 (Char.chr (method_to_byte LZ4));
      bytes_set_int32_le header 1 (Int32.of_int (compressed_size + compress_header_size));
      bytes_set_int32_le header 5 (Int32.of_int uncompressed_size);
      
      (* Calculate checksum over header + compressed data *)
      let checksum_input = Bytes.create (compress_header_size + compressed_size) in
      Bytes.blit header 0 checksum_input 0 compress_header_size;
      Bytes.blit compressed_data 0 checksum_input compress_header_size compressed_size;
      
      let hash = Cityhash.cityhash128 checksum_input in
      let checksum = Cityhash.to_bytes hash in
      
      (* Write: checksum + header + compressed data *)
      output oc checksum 0 checksum_size;
      output oc header 0 compress_header_size;
      output oc compressed_data 0 compressed_size;
      flush oc
  | ZSTD ->
      let uncompressed_size = Bytes.length data in
      
      (* Compress the data *)
      let compressed_data = compress_zstd data in
      let compressed_size = Bytes.length compressed_data in
      
      (* Create header: method (1) + compressed_size (4) + uncompressed_size (4) *)
      let header = Bytes.create compress_header_size in
      Bytes.set header 0 (Char.chr (method_to_byte ZSTD));
      bytes_set_int32_le header 1 (Int32.of_int (compressed_size + compress_header_size));
      bytes_set_int32_le header 5 (Int32.of_int uncompressed_size);
      
      (* Calculate checksum over header + compressed data *)
      let checksum_input = Bytes.create (compress_header_size + compressed_size) in
      Bytes.blit header 0 checksum_input 0 compress_header_size;
      Bytes.blit compressed_data 0 checksum_input compress_header_size compressed_size;
      
      let hash = Cityhash.cityhash128 checksum_input in
      let checksum = Cityhash.to_bytes hash in
      
      (* Write: checksum + header + compressed data *)
      output oc checksum 0 checksum_size;
      output oc header 0 compress_header_size;
      output oc compressed_data 0 compressed_size;
      flush oc

(* Read a compressed block from input channel *)
let read_compressed_block ic (method_ : method_t) : bytes =
  match method_ with
  | None ->
      (* For uncompressed, we need to know the size - this should be handled by caller *)
      failwith "Uncompressed block reading requires size from caller"
  | LZ4 ->
      (* Read header: checksum (16) + method (1) + compressed_size (4) + uncompressed_size (4) *)
      let header = really_input_string ic header_size |> Bytes.of_string in
      
      (* Extract fields *)
      let _checksum = Bytes.sub header 0 checksum_size in
      let method_byte = Char.code (Bytes.get header checksum_size) in
      let compressed_size = bytes_get_int32_le header (checksum_size + 1) in
      let uncompressed_size = bytes_get_int32_le header (checksum_size + 5) in
      
      (* Verify method *)
      if method_byte <> method_to_byte LZ4 then
        failwith (Printf.sprintf "Expected LZ4 method, got 0x%02x" method_byte);
      
      (* Read compressed data *)
      let compressed_data_size = Int32.to_int compressed_size - compress_header_size in
      let compressed_data = really_input_string ic compressed_data_size |> Bytes.of_string in
      
      (* Verify checksum *)
      let checksum_input = Bytes.create (compress_header_size + compressed_data_size) in
      Bytes.blit header checksum_size checksum_input 0 compress_header_size;
      Bytes.blit compressed_data 0 checksum_input compress_header_size compressed_data_size;
      
      let calculated_hash = Cityhash.cityhash128 checksum_input in
      let calculated_checksum = Cityhash.to_bytes calculated_hash in
      
      let received_checksum = Bytes.sub header 0 checksum_size in
      let hex_of_bytes bytes =
        Bytes.to_string bytes 
        |> String.fold_left (fun acc c -> acc ^ Printf.sprintf "%02x" (Char.code c)) ""
      in
      if not (Bytes.equal calculated_checksum received_checksum) then
        raise (Checksum_mismatch 
          (Printf.sprintf "LZ4 checksum verification failed: expected %s, got %s"
            (hex_of_bytes received_checksum)
            (hex_of_bytes calculated_checksum)))
      else
        (* Decompress *)
        decompress_lz4 compressed_data (Int32.to_int uncompressed_size)
  | ZSTD ->
      (* Read header: checksum (16) + method (1) + compressed_size (4) + uncompressed_size (4) *)
      let header = really_input_string ic header_size |> Bytes.of_string in
      
      (* Extract fields *)
      let _checksum = Bytes.sub header 0 checksum_size in
      let method_byte = Char.code (Bytes.get header checksum_size) in
      let compressed_size = bytes_get_int32_le header (checksum_size + 1) in
      let uncompressed_size = bytes_get_int32_le header (checksum_size + 5) in
      
      (* Verify method *)
      if method_byte <> method_to_byte ZSTD then
        failwith (Printf.sprintf "Expected ZSTD method, got 0x%02x" method_byte);
      
      (* Read compressed data *)
      let compressed_data_size = Int32.to_int compressed_size - compress_header_size in
      let compressed_data = really_input_string ic compressed_data_size |> Bytes.of_string in
      
      (* Verify checksum *)
      let checksum_input = Bytes.create (compress_header_size + compressed_data_size) in
      Bytes.blit header checksum_size checksum_input 0 compress_header_size;
      Bytes.blit compressed_data 0 checksum_input compress_header_size compressed_data_size;
      
      let calculated_hash = Cityhash.cityhash128 checksum_input in
      let calculated_checksum = Cityhash.to_bytes calculated_hash in
      
      let received_checksum = Bytes.sub header 0 checksum_size in
      let hex_of_bytes bytes =
        Bytes.to_string bytes 
        |> String.fold_left (fun acc c -> acc ^ Printf.sprintf "%02x" (Char.code c)) ""
      in
      if not (Bytes.equal calculated_checksum received_checksum) then
        raise (Checksum_mismatch 
          (Printf.sprintf "ZSTD checksum verification failed: expected %s, got %s"
            (hex_of_bytes received_checksum)
            (hex_of_bytes calculated_checksum)))
      else
        (* Decompress *)
        decompress_zstd compressed_data (Int32.to_int uncompressed_size)

let read_compressed_block_br br (method_ : method_t) : bytes =
  match method_ with
  | None -> failwith "Uncompressed block reading requires size from caller"
  | LZ4 ->
      let header = Bytes.create header_size in
      Buffered_reader.really_input br header 0 header_size;
      let method_byte = Char.code (Bytes.get header checksum_size) in
      if method_byte <> method_to_byte LZ4 then
        failwith (Printf.sprintf "Expected LZ4 method, got 0x%02x" method_byte);

      let compressed_size = bytes_get_int32_le header (checksum_size + 1) in
      let uncompressed_size = bytes_get_int32_le header (checksum_size + 5) in
      let compressed_data_size = (Int32.to_int compressed_size) - compress_header_size in
      let compressed_data = Bytes.create compressed_data_size in
      Buffered_reader.really_input br compressed_data 0 compressed_data_size;

      let checksum_input = Bytes.create (compress_header_size + compressed_data_size) in
      Bytes.blit header checksum_size checksum_input 0 compress_header_size;
      Bytes.blit compressed_data 0 checksum_input compress_header_size compressed_data_size;
      let calculated_hash = Cityhash.cityhash128 checksum_input in
      let calculated_checksum = Cityhash.to_bytes calculated_hash in
      let received_checksum = Bytes.sub header 0 checksum_size in
      if not (Bytes.equal calculated_checksum received_checksum) then
        raise (Checksum_mismatch "LZ4 checksum verification failed (br)")
      else
        decompress_lz4 compressed_data (Int32.to_int uncompressed_size)
  | ZSTD ->
      let header = Bytes.create header_size in
      Buffered_reader.really_input br header 0 header_size;
      let method_byte = Char.code (Bytes.get header checksum_size) in
      if method_byte <> method_to_byte ZSTD then
        failwith (Printf.sprintf "Expected ZSTD method, got 0x%02x" method_byte);

      let compressed_size = bytes_get_int32_le header (checksum_size + 1) in
      let uncompressed_size = bytes_get_int32_le header (checksum_size + 5) in
      let compressed_data_size = (Int32.to_int compressed_size) - compress_header_size in
      let compressed_data = Bytes.create compressed_data_size in
      Buffered_reader.really_input br compressed_data 0 compressed_data_size;

      let checksum_input = Bytes.create (compress_header_size + compressed_data_size) in
      Bytes.blit header checksum_size checksum_input 0 compress_header_size;
      Bytes.blit compressed_data 0 checksum_input compress_header_size compressed_data_size;
      let calculated_hash = Cityhash.cityhash128 checksum_input in
      let calculated_checksum = Cityhash.to_bytes calculated_hash in
      let received_checksum = Bytes.sub header 0 checksum_size in
      if not (Bytes.equal calculated_checksum received_checksum) then
        raise (Checksum_mismatch "ZSTD checksum verification failed (br)")
      else
        decompress_zstd compressed_data (Int32.to_int uncompressed_size)

(* Reader type for streaming decompression *)
type reader = {
  ic: in_channel;
  method_: method_t;
  mutable buffer: bytes; [@warning "-69"]  (* This field is mutated in read_block *)
  mutable pos: int;
  mutable valid: int;
}

let create_reader ic method_ = {
  ic;
  method_;
  buffer = Bytes.create max_block_size;
  pos = 0;
  valid = 0;
}

let read_block reader =
  if reader.method_ = None then
    failwith "Uncompressed streaming not implemented"
  else
    let block = read_compressed_block reader.ic reader.method_ in
    reader.buffer <- block;
    reader.pos <- 0;
    reader.valid <- Bytes.length block

let read reader buf off len =
  let rec loop off len copied =
    if len = 0 then copied
    else
      (* Need more data? *)
      let available = reader.valid - reader.pos in
      if available = 0 then begin
        read_block reader;
        loop off len copied
      end else
        let to_copy = min len available in
        Bytes.blit reader.buffer reader.pos buf off to_copy;
        reader.pos <- reader.pos + to_copy;
        loop (off + to_copy) (len - to_copy) (copied + to_copy)
  in
  loop off len 0

(* Writer type for streaming compression *)
type writer = {
  oc: out_channel;
  method_: method_t;
  mutable buffer: bytes; [@warning "-69"]  (* Contents mutated via Bytes.blit *)
  mutable pos: int;
}

let create_writer oc method_ = {
  oc;
  method_;
  buffer = Bytes.create max_block_size;
  pos = 0;
}

let flush_block writer =
  if writer.pos > 0 then begin
    let data = Bytes.sub writer.buffer 0 writer.pos in
    write_compressed_block writer.oc data writer.method_;
    writer.pos <- 0
  end

let write writer buf off len =
  let rec loop off len =
    if len = 0 then ()
    else
      let available = Bytes.length writer.buffer - writer.pos in
      let to_copy = min len available in
      Bytes.blit buf off writer.buffer writer.pos to_copy;
      writer.pos <- writer.pos + to_copy;
      
      if writer.pos = Bytes.length writer.buffer then
        flush_block writer;
      
      loop (off + to_copy) (len - to_copy)
  in
  loop off len

let flush writer = flush_block writer
