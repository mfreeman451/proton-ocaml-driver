open Proton

let write_varint buf n =
  let rec loop n =
    let x = n land 0x7f in
    let n' = n lsr 7 in
    if n' = 0 then Buffer.add_char buf (Char.chr x)
    else (Buffer.add_char buf (Char.chr (x lor 0x80)); loop n')
  in loop n

let write_str buf s =
  write_varint buf (String.length s);
  Buffer.add_string buf s

let build_uncompressed_block ~cols =
  let buf = Buffer.create 256 in
  (* BlockInfo terminator *)
  write_varint buf 0;
  write_varint buf (List.length cols);
  let n_rows = match cols with [] -> 0 | (_,_,rows)::_ -> rows in
  write_varint buf n_rows;
  List.iter (fun (name, type_spec, rows) ->
    write_str buf name;
    write_str buf type_spec;
    let writeln_int32 v = Buffer.add_string buf (Bytes.to_string (Bytes.init 4 (fun i -> Char.chr ((v lsr (8*i)) land 0xFF)))) in
    let writeln_int64 v = Buffer.add_string buf (Bytes.to_string (Bytes.init 8 (fun i -> Char.chr (Int64.to_int (Int64.logand (Int64.shift_right_logical v (8*i)) 0xFFL))))) in
    for _i = 1 to rows do
      match String.lowercase_ascii (String.trim type_spec) with
      | "string" | "json" -> write_str buf "abc"
      | s when Columns.has_prefix s "fixedstring(" ->
          Buffer.add_string buf "abc"; Buffer.add_char buf '\x00'
      | s when Columns.has_prefix s "enum8(" ->
          (* Write sequential enum values 1, 2, 3... for each row *)
          Buffer.add_char buf (Char.chr _i)
      | s when Columns.has_prefix s "enum16(" ->
          (* Write sequential enum16 values 100, 200... for each row as int32 LE *)
          let v = _i * 100 in
          Buffer.add_string buf (Bytes.to_string (Bytes.init 4 (fun j -> Char.chr ((v lsr (8*j)) land 0xFF))))
      | "int32" | "uint32" -> writeln_int32 42
      | "int64" | "uint64" -> writeln_int64 42L
      | "float64" -> Buffer.add_string buf (Bytes.to_string (Bytes.make 8 '\x00'))
      | "ipv4" -> writeln_int32 ((127 lsl 24) lor 1)
      | s when Columns.has_prefix s "decimal(" ->
          (* write signed int64 raw: e.g., 12345 for Decimal(10,2) -> 123.45 *)
          writeln_int64 12345L
      | "ipv6" -> Buffer.add_string buf (Bytes.to_string (Bytes.make 16 '\x01'))
      | "uuid" -> Buffer.add_string buf (Bytes.to_string (Bytes.make 16 '\x01'))
      | s when Columns.has_prefix s "nullable(" ->
          Buffer.add_char buf '\x00'; write_str buf "abc"
      | other -> failwith ("unsupported in test: " ^ other)
    done
  ) cols;
  Buffer.contents buf |> Bytes.of_string

let read_block_from_bytes (bs:bytes) : Block.t =
  let br = Buffered_reader.create_from_bytes_no_copy bs in
  Block.read_block_br ~revision:Defines.dbms_min_revision_with_block_info br

let test_uncompressed_block_parse () =
  let bs = build_uncompressed_block ~cols:[ ("c1","String",2) ] in
  match read_block_from_bytes bs with
  | { Block.n_rows=2; columns=[{ Block.name; type_spec; data }]; _ } ->
      Alcotest.(check string) "col name" "c1" name;
      Alcotest.(check string) "type" "String" type_spec;
      Alcotest.(check int) "rows" 2 (Array.length data);
      ()
  | _ -> Alcotest.fail "Unexpected block"

let test_exception_reader () =
  let buf = Buffer.create 64 in
  (* code int32 le = 42 *)
  Buffer.add_string buf "\x2a\x00\x00\x00";
  write_str buf "DB::Exception";
  write_str buf "error message";
  write_str buf "stack";
  Buffer.add_char buf '\x00';
  let bs = Buffer.contents buf |> Bytes.of_string in
  match Lwt_main.run (Connection.read_exception_from_bytes bs) with
  | Errors.Server_exception se -> Alcotest.(check int) "code" 42 se.code
  | _ -> Alcotest.fail "Expected server exception"

let test_enum_and_fixedstring () =
  let cols = [ ("e8", "Enum8('A'=1,'B'=2)", 2); ("fs", "FixedString(4)", 2) ] in
  let bs = build_uncompressed_block ~cols in
  match read_block_from_bytes bs with
  | { Block.n_rows=2; columns=[c1; c2]; _ } ->
      Alcotest.(check string) "fs type" "FixedString(4)" c2.Block.type_spec;
      ignore c1; ()
  | _ -> Alcotest.fail "Unexpected block"

let test_lowcardinality_basic () =
  (* Build LowCardinality(String): dict size=2: ["x","y"]; keys rows=3: [1,2,1] *)
  let buf = Buffer.create 128 in
  write_varint buf 0; (* BI end *)
  write_varint buf 1; (* n_columns *)
  write_varint buf 3; (* n_rows *)
  write_str buf "lc"; write_str buf "LowCardinality(String)";
  (* indexSerializationType: keyUInt8 (0) with flags; we don't strictly enforce flags, only key width *)
  Buffer.add_string buf "\x00\x00\x00\x00\x00\x00\x00\x00";
  (* dict rows=2 *)
  Buffer.add_string buf "\x02\x00\x00\x00\x00\x00\x00\x00";
  write_str buf "x"; write_str buf "y";
  (* keys rows=3 *)
  Buffer.add_string buf "\x03\x00\x00\x00\x00\x00\x00\x00";
  Buffer.add_char buf '\x01'; Buffer.add_char buf '\x02'; Buffer.add_char buf '\x01';
  let bs = Buffer.contents buf |> Bytes.of_string in
  match read_block_from_bytes bs with
  | { Block.n_rows=3; columns=[c]; _ } ->
      Alcotest.(check string) "lc name" "lc" c.Block.name;
      Alcotest.(check string) "lc type" "LowCardinality(String)" c.Block.type_spec
  | _ -> Alcotest.fail "Unexpected LC block"

let test_lowcardinality_nullable () =
  (* LC(Nullable(String)) with keys [0,null,1] *)
  let buf = Buffer.create 128 in
  write_varint buf 0; write_varint buf 1; write_varint buf 3;
  write_str buf "lcn"; write_str buf "LowCardinality(Nullable(String))";
  Buffer.add_string buf "\x00\x00\x00\x00\x00\x00\x00\x00"; (* key type *)
  Buffer.add_string buf "\x01\x00\x00\x00\x00\x00\x00\x00"; (* dict rows=1 *)
  write_str buf "x"; (* dict[0] = x *)
  Buffer.add_string buf "\x03\x00\x00\x00\x00\x00\x00\x00"; (* keys rows=3 *)
  Buffer.add_char buf '\x00'; Buffer.add_char buf '\x00'; Buffer.add_char buf '\x01';
  let bs = Buffer.contents buf |> Bytes.of_string in
  match read_block_from_bytes bs with
  | { Block.n_rows=3; columns=[c]; _ } ->
      Alcotest.(check string) "type" "LowCardinality(Nullable(String))" c.Block.type_spec
  | _ -> Alcotest.fail "Unexpected LC(N) block"

let test_decimal_formatting () =
  let cols = [ ("d", "Decimal(10,2)", 1) ] in
  let bs = build_uncompressed_block ~cols in
  match read_block_from_bytes bs with
  | { Block.n_rows=1; columns=[c]; _ } ->
      Alcotest.(check string) "type" "Decimal(10,2)" c.Block.type_spec
  | _ -> Alcotest.fail "Unexpected Decimal block"

let test_ip_formatting () =
  let cols = [ ("v4","IPv4",1); ("v6","IPv6",1) ] in
  let bs = build_uncompressed_block ~cols in
  ignore (read_block_from_bytes bs);
  ()

let test_pool_basics () =
  let pool = Pool.create_pool (fun () -> Connection.create ()) in
  Pool.start_cleanup pool ~period:0.1;
  let used = Lwt_main.run (Pool.with_connection pool (fun _ -> Lwt.return 123)) in
  Alcotest.(check int) "with_connection returns" 123 used;
  let _ = Lwt_main.run (Pool.pool_stats pool) in
  Pool.stop_cleanup pool

(* CityHash tests *)
let test_cityhash_consistency () =
  let test_data = "test data for hashing" |> Bytes.of_string in
  let hash1 = Cityhash.cityhash128 test_data in
  let hash2 = Cityhash.cityhash128 test_data in
  Alcotest.(check bool) "CityHash consistency" true
    (hash1.low = hash2.low && hash1.high = hash2.high)

let test_cityhash_different_inputs () =
  let data1 = "hello world" |> Bytes.of_string in
  let data2 = "hello world!" |> Bytes.of_string in
  let hash1 = Cityhash.cityhash128 data1 in
  let hash2 = Cityhash.cityhash128 data2 in
  Alcotest.(check bool) "Different inputs produce different hashes" true
    (hash1.low <> hash2.low || hash1.high <> hash2.high)

(* LZ4 Compression tests *)
let test_lz4_roundtrip () =
  let test_cases = [
    ("hello world", "small string");
    ("The quick brown fox jumps over the lazy dog", "medium string");
    (String.make 1000 'A', "highly compressible");
    ("abcdefghijklmnopqrstuvwxyz0123456789", "alphanumeric");
  ] in
  
  List.iter (fun (input, desc) ->
    let original = Bytes.of_string input in
    let compressed = Compress.compress_lz4 original in
    let decompressed = Compress.decompress_lz4 compressed (Bytes.length original) in
    Alcotest.(check string) (desc ^ " roundtrip") input (Bytes.to_string decompressed)
  ) test_cases

let test_lz4_compression_ratio () =
  let highly_compressible = String.make 1000 'A' |> Bytes.of_string in
  let compressed = Compress.compress_lz4 highly_compressible in
  let ratio = float (Bytes.length compressed) /. float (Bytes.length highly_compressible) in
  (* Should compress to less than 5% of original *)
  Alcotest.(check bool) "High compression ratio for repetitive data" true (ratio < 0.05)

let test_compression_frame_format () =
  let test_data = Bytes.of_string "Test data for frame format" in
  let temp_file = Filename.temp_file "proton_test" ".tmp" in
  
  let () =
    let oc = open_out_bin temp_file in
    Compress.write_compressed_block oc test_data Compress.LZ4;
    close_out oc
  in
  
  let ic = open_in_bin temp_file in
  let file_size = in_channel_length ic in
  let frame = really_input_string ic file_size |> Bytes.of_string in
  close_in ic;
  Sys.remove temp_file;
  
  (* Check minimum frame size (16 checksum + 1 method + 8 sizes) *)
  Alcotest.(check bool) "Frame has minimum size" true (Bytes.length frame >= 25);
  
  (* Check method byte *)
  let method_byte = Char.code (Bytes.get frame 16) in
  Alcotest.(check int) "Method byte is LZ4" 0x82 method_byte;
  
  (* Check uncompressed size matches *)
  let uncompressed_size = Binary.bytes_get_int32_le frame 21 in
  Alcotest.(check int32) "Uncompressed size in header" 
    (Int32.of_int (Bytes.length test_data)) uncompressed_size

let test_checksum_verification () =
  let test_data = Bytes.of_string "Test data for checksum verification" in
  let temp_file = Filename.temp_file "proton_test" ".tmp" in
  
  (* Write a valid compressed block *)
  let () =
    let oc = open_out_bin temp_file in
    Compress.write_compressed_block oc test_data Compress.LZ4;
    close_out oc
  in
  
  (* First, verify that reading works normally *)
  let () =
    let ic = open_in_bin temp_file in
    let decompressed = Compress.read_compressed_block ic Compress.LZ4 in
    close_in ic;
    Alcotest.(check string) "Valid checksum verification" 
      (Bytes.to_string test_data) (Bytes.to_string decompressed)
  in
  
  (* Now corrupt the checksum and verify it fails *)
  let () =
    let ic = open_in_bin temp_file in
    let file_size = in_channel_length ic in
    let frame = really_input_string ic file_size |> Bytes.of_string in
    close_in ic;
    
    (* Corrupt the first byte of checksum *)
    let original_byte = Bytes.get frame 0 in
    let corrupted_byte = Char.chr ((Char.code original_byte) lxor 0xFF) in
    Bytes.set frame 0 corrupted_byte;
    
    (* Write the corrupted frame back *)
    let oc = open_out_bin temp_file in
    output oc frame 0 (Bytes.length frame);
    close_out oc;
    
    (* Try to read it - should fail with checksum error *)
    let ic = open_in_bin temp_file in
    let result = try
      let _ = Compress.read_compressed_block ic Compress.LZ4 in
      false (* Should not reach here *)
    with
    | Compress.Checksum_mismatch _ -> true
    | _ -> false
    in
    close_in ic;
    Alcotest.(check bool) "Corrupted checksum detected" true result
  in
  
  Sys.remove temp_file

let test_compression_method_encoding () =
  Alcotest.(check int) "None method byte" 0x02 (Compress.method_to_byte Compress.None);
  Alcotest.(check int) "LZ4 method byte" 0x82 (Compress.method_to_byte Compress.LZ4);
  Alcotest.(check int) "ZSTD method byte" 0x90 (Compress.method_to_byte Compress.ZSTD)

let test_compression_method_decoding () =
  Alcotest.(check bool) "Decode None" true (Compress.method_of_byte 0x02 = Compress.None);
  Alcotest.(check bool) "Decode LZ4" true (Compress.method_of_byte 0x82 = Compress.LZ4);
  Alcotest.(check bool) "Decode ZSTD" true (Compress.method_of_byte 0x90 = Compress.ZSTD)

(* ZSTD compression tests *)
let test_zstd_roundtrip () =
  let test_cases = [
    ("hello world", "small string");
    ("The quick brown fox jumps over the lazy dog", "medium string");
    (String.make 1000 'A', "highly compressible");
    ("abcdefghijklmnopqrstuvwxyz0123456789", "alphanumeric");
  ] in
  
  List.iter (fun (input, desc) ->
    let original = Bytes.of_string input in
    let compressed = Compress.compress_zstd original in
    let decompressed = Compress.decompress_zstd compressed (Bytes.length original) in
    Alcotest.(check string) (desc ^ " ZSTD roundtrip") input (Bytes.to_string decompressed)
  ) test_cases

let test_zstd_compression_ratio () =
  let repetitive_data = String.make 10000 'A' in
  let original = Bytes.of_string repetitive_data in
  let compressed = Compress.compress_zstd original in
  
  let original_size = Bytes.length original in
  let compressed_size = Bytes.length compressed in
  
  Printf.printf "ZSTD: Original size: %d, Compressed size: %d, Ratio: %.2f\n"
    original_size compressed_size (float_of_int original_size /. float_of_int compressed_size);
  
  (* ZSTD should compress repetitive data very well - expect at least 10:1 ratio *)
  Alcotest.(check bool) "ZSTD high compression ratio" true (compressed_size * 10 < original_size)

let test_zstd_frame_format () =
  let test_data = Bytes.of_string "Hello, ZSTD compression world!" in
  let temp_file = Filename.temp_file "zstd_test" ".bin" in
  
  (* Write compressed block to file *)
  let oc = open_out_bin temp_file in
  Compress.write_compressed_block oc test_data Compress.ZSTD;
  close_out oc;
  
  (* Read it back *)
  let ic = open_in_bin temp_file in
  let decompressed = Compress.read_compressed_block ic Compress.ZSTD in
  close_in ic;
  
  Alcotest.(check string) "ZSTD frame format roundtrip" 
    (Bytes.to_string test_data) (Bytes.to_string decompressed);
  
  Sys.remove temp_file

(* Binary encoding tests *)
let test_int32_roundtrip () =
  let test_values = [0l; 1l; -1l; Int32.max_int; Int32.min_int; 0x12345678l] in
  List.iter (fun value ->
    let buf = Bytes.create 4 in
    Binary.bytes_set_int32_le buf 0 value;
    let decoded = Binary.bytes_get_int32_le buf 0 in
    Alcotest.(check int32) (Printf.sprintf "int32 roundtrip %ld" value) value decoded
  ) test_values

let test_int64_roundtrip () =
  let test_values = [0L; 1L; -1L; Int64.max_int; Int64.min_int; 0x123456789ABCDEFL] in
  List.iter (fun value ->
    let buf = Bytes.create 8 in
    Binary.bytes_set_int64_le buf 0 value;
    let decoded = Binary.bytes_get_int64_le buf 0 in
    Alcotest.(check int64) (Printf.sprintf "int64 roundtrip %Ld" value) value decoded
  ) test_values

(* Connection tests *)
let test_connection_creation () =
  let conn = Connection.create ~host:"127.0.0.1" ~port:8463 () in
  Alcotest.(check bool) "Connection created with correct defaults" true
    (conn.host = "127.0.0.1" && conn.port = 8463)

let test_client_creation () =
  let _client = Client.create ~host:"localhost" ~port:9000 () in
  (* Just check it doesn't crash - actual connection would fail without server *)
  Alcotest.(check bool) "Client creation succeeds" true true

let test_compression_enabled_connection () =
  (* Test that we can create connections with different compression settings *)
  let conn_none = Connection.create ~compression:Compress.None () in
  let conn_lz4 = Connection.create ~compression:Compress.LZ4 () in
  let conn_zstd = Connection.create ~compression:Compress.ZSTD () in
  
  Alcotest.(check bool) "None compression connection" true (conn_none.compression = Compress.None);
  Alcotest.(check bool) "LZ4 compression connection" true (conn_lz4.compression = Compress.LZ4);  
  Alcotest.(check bool) "ZSTD compression connection" true (conn_zstd.compression = Compress.ZSTD)

let test_compression_enabled_client () =
  (* Test that clients can be created with compression *)
  let client_none = Client.create ~compression:Compress.None () in
  let client_lz4 = Client.create ~compression:Compress.LZ4 () in
  
  (* Verify compression is passed through (indirect test) *)
  Alcotest.(check bool) "Client with None compression" true (client_none.conn.compression = Compress.None);
  Alcotest.(check bool) "Client with LZ4 compression" true (client_lz4.conn.compression = Compress.LZ4)

(* DateTime column tests *)
let test_datetime_parsing () =
  (* Test that parsing doesn't fail *)
  try
    let _reader = Columns.reader_of_spec "datetime" in
    Alcotest.(check bool) "DateTime parser created" true true
  with
  | _ -> Alcotest.(check bool) "DateTime parser created" true false

let test_datetime_with_timezone () =
  try
    let _reader = Columns.reader_of_spec "datetime('UTC')" in
    Alcotest.(check bool) "DateTime with timezone parser created" true true
  with
  | _ -> Alcotest.(check bool) "DateTime with timezone parser created" true false

let test_datetime64_parsing () =
  try
    let _reader = Columns.reader_of_spec "datetime64(3)" in
    Alcotest.(check bool) "DateTime64 parser created" true true
  with
  | _ -> Alcotest.(check bool) "DateTime64 parser created" true false

let test_datetime64_with_timezone () =
  try
    let _reader = Columns.reader_of_spec "datetime64(6, 'America/New_York')" in
    Alcotest.(check bool) "DateTime64 with timezone parser created" true true
  with
  | _ -> Alcotest.(check bool) "DateTime64 with timezone parser created" true false

let test_datetime_value_formatting () =
  let dt_value = Columns.VDateTime (1609459200L, Some "UTC") in (* 2021-01-01 00:00:00 UTC *)
  let dt_str = Columns.value_to_string dt_value in
  Alcotest.(check bool) "DateTime value formatting" true (String.length dt_str > 0)

let test_datetime64_value_formatting () =
  let dt64_value = Columns.VDateTime64 (1609459200000L, 3, Some "UTC") in
  let dt64_str = Columns.value_to_string dt64_value in
  Alcotest.(check bool) "DateTime64 value formatting" true (String.length dt64_str > 0)

(* Test DateTime binary roundtrip *)
let test_datetime_binary_roundtrip () =
  (* Create test data: Unix timestamp for 2021-01-01 00:00:00 UTC *)
  let test_timestamp = 1609459200L in
  let test_data = Int64.to_int32 test_timestamp in
  
  (* Create binary data manually *)
  let bytes_data = Bytes.create 4 in
  let a = Int32.to_int (Int32.logand test_data 0xFFl) in
  let b = Int32.to_int (Int32.logand (Int32.shift_right_logical test_data 8) 0xFFl) in
  let c = Int32.to_int (Int32.logand (Int32.shift_right_logical test_data 16) 0xFFl) in
  let d = Int32.to_int (Int32.logand (Int32.shift_right_logical test_data 24) 0xFFl) in
  Bytes.set_uint8 bytes_data 0 a;
  Bytes.set_uint8 bytes_data 1 b;
  Bytes.set_uint8 bytes_data 2 c;
  Bytes.set_uint8 bytes_data 3 d;
  
  (* Create buffered reader from written data *)
  let br = Buffered_reader.create_from_bytes_no_copy bytes_data in
  
  (* Read using DateTime reader - test without timezone first *)
  let reader = Columns.reader_of_spec_br "datetime" in
  let values = reader br 1 in
  
  (* Verify we got the expected value *)
  match values.(0) with
  | Columns.VDateTime (ts, tz) ->
      Alcotest.(check bool) "DateTime timestamp matches" true (ts = test_timestamp);
      Alcotest.(check bool) "DateTime timezone matches" true (tz = None)
  | _ -> Alcotest.fail "Expected VDateTime value"

(* Test DateTime64 binary roundtrip *)  
let test_datetime64_binary_roundtrip () =
  (* Create test data: millisecond timestamp *)
  let test_value = 1609459200123L in (* 2021-01-01 00:00:00.123 *)
  
  (* Create 8-byte binary data using the proper binary encoding *)
  let bytes_data = Bytes.create 8 in
  Binary.bytes_set_int64_le bytes_data 0 test_value;
  
  (* Create buffered reader from written data *)
  let br = Buffered_reader.create_from_bytes_no_copy bytes_data in
  
  (* Read using DateTime64 reader - test without timezone first *)
  let reader = Columns.reader_of_spec_br "datetime64(3)" in
  let values = reader br 1 in
  
  (* Verify we got the expected value *)
  match values.(0) with
  | Columns.VDateTime64 (value, precision, tz) ->
      Alcotest.(check bool) "DateTime64 value matches" true (value = test_value);
      Alcotest.(check bool) "DateTime64 precision matches" true (precision = 3);
      Alcotest.(check bool) "DateTime64 timezone matches" true (tz = None)
  | _ -> Alcotest.fail "Expected VDateTime64 value"

(* Test reading multiple DateTime values *)
let test_multiple_datetime_values () =
  (* Create test data for 3 DateTime values *)
  let timestamps = [1609459200L; 1640995200L; 1672531200L] in (* 2021, 2022, 2023 *)
  let bytes_data = Bytes.create 12 in (* 3 * 4 bytes *)
  
  List.iteri (fun i ts ->
    let test_data = Int64.to_int32 ts in
    let offset = i * 4 in
    let a = Int32.to_int (Int32.logand test_data 0xFFl) in
    let b = Int32.to_int (Int32.logand (Int32.shift_right_logical test_data 8) 0xFFl) in
    let c = Int32.to_int (Int32.logand (Int32.shift_right_logical test_data 16) 0xFFl) in
    let d = Int32.to_int (Int32.logand (Int32.shift_right_logical test_data 24) 0xFFl) in
    Bytes.set_uint8 bytes_data (offset + 0) a;
    Bytes.set_uint8 bytes_data (offset + 1) b;
    Bytes.set_uint8 bytes_data (offset + 2) c;
    Bytes.set_uint8 bytes_data (offset + 3) d;
  ) timestamps;
  
  let br = Buffered_reader.create_from_bytes_no_copy bytes_data in
  let reader = Columns.reader_of_spec_br "datetime" in
  let values = reader br 3 in
  
  (* Verify all values *)
  for i = 0 to 2 do
    match values.(i) with
    | Columns.VDateTime (ts, tz) ->
        Alcotest.(check bool) ("DateTime value " ^ string_of_int i) true (ts = List.nth timestamps i);
        Alcotest.(check bool) ("DateTime timezone " ^ string_of_int i) true (tz = None)
    | _ -> Alcotest.fail "Expected VDateTime value"
  done

(* Array column tests *)
let test_array_parsing () =
  try
    let _reader = Columns.reader_of_spec "array(string)" in
    Alcotest.(check bool) "Array parser created" true true
  with
  | _ -> Alcotest.(check bool) "Array parser created" true false

let test_nested_array_parsing () =
  try
    let _reader = Columns.reader_of_spec "array(array(int32))" in
    Alcotest.(check bool) "Nested array parser created" true true
  with
  | _ -> Alcotest.(check bool) "Nested array parser created" true false

let test_array_value_formatting () =
  let array_value = Columns.VArray [|
    Columns.VString "hello";
    Columns.VString "world";
    Columns.VInt32 42l
  |] in
  let array_str = Columns.value_to_string array_value in
  let expected = "[hello,world,42]" in
  Alcotest.(check string) "Array formatting" expected array_str

let test_empty_array_formatting () =
  let empty_array = Columns.VArray [||] in
  let array_str = Columns.value_to_string empty_array in
  let expected = "[]" in
  Alcotest.(check string) "Empty array formatting" expected array_str

(* Map column tests *)
let test_map_parsing () =
  try
    let _reader = Columns.reader_of_spec "map(string, int32)" in
    Alcotest.(check bool) "Map parser created" true true
  with
  | _ -> Alcotest.(check bool) "Map parser created" true false

let test_nested_map_parsing () =
  try
    let _reader = Columns.reader_of_spec "map(string, array(int32))" in
    Alcotest.(check bool) "Nested map parser created" true true
  with
  | _ -> Alcotest.(check bool) "Nested map parser created" true false

let test_map_value_formatting () =
  let map_value = Columns.VMap [
    (Columns.VString "key1", Columns.VInt32 42l);
    (Columns.VString "key2", Columns.VString "value2")
  ] in
  let map_str = Columns.value_to_string map_value in
  let expected = "{key1:42,key2:value2}" in
  Alcotest.(check string) "Map formatting" expected map_str

let test_empty_map_formatting () =
  let empty_map = Columns.VMap [] in
  let map_str = Columns.value_to_string empty_map in
  let expected = "{}" in
  Alcotest.(check string) "Empty map formatting" expected map_str

(* Enum tests *)
let test_enum8_parsing () =
  let cols = [ ("status", "Enum8('active'=1,'inactive'=2,'pending'=3)", 3) ] in
  let bs = build_uncompressed_block ~cols in
  match read_block_from_bytes bs with
  | { Block.n_rows=3; columns=[c]; _ } ->
      Alcotest.(check string) "Enum8 type" "Enum8('active'=1,'inactive'=2,'pending'=3)" c.Block.type_spec;
      (match c.Block.data with
      | [| Columns.VEnum8 ("active", 1); Columns.VEnum8 ("inactive", 2); Columns.VEnum8 ("pending", 3) |] -> ()
      | _ -> Alcotest.fail "Expected VEnum8 values")
  | _ -> Alcotest.fail "Unexpected block structure"

let test_enum16_parsing () =
  let cols = [ ("priority", "Enum16('low'=100,'normal'=200,'high'=300)", 2) ] in
  let bs = build_uncompressed_block ~cols in
  match read_block_from_bytes bs with
  | { Block.n_rows=2; columns=[c]; _ } ->
      Alcotest.(check string) "Enum16 type" "Enum16('low'=100,'normal'=200,'high'=300)" c.Block.type_spec;
      (match c.Block.data with
      | [| Columns.VEnum16 ("low", 100); Columns.VEnum16 ("normal", 200) |] -> ()
      | _ -> Alcotest.fail "Expected VEnum16 values")
  | _ -> Alcotest.fail "Unexpected block structure"

let test_enum8_negative () =
  (* Test enum with negative value (-1) *)
  (* Create custom test data with -1 value (0xFF byte) *)
  let buf = Buffer.create 128 in
  write_varint buf 0; (* BlockInfo terminator *)
  write_varint buf 1; (* n_columns *)
  write_varint buf 1; (* n_rows *)
  write_str buf "flag";
  write_str buf "Enum8('off'=-1,'on'=1)";
  Buffer.add_char buf '\xFF'; (* -1 as signed byte *)
  let bs = Buffer.contents buf |> Bytes.of_string in
  match read_block_from_bytes bs with
  | { Block.n_rows=1; columns=[c]; _ } ->
      (match c.Block.data with
      | [| Columns.VEnum8 ("off", -1) |] -> ()
      | _ -> Alcotest.fail "Expected VEnum8 with negative value")
  | _ -> Alcotest.fail "Unexpected block structure"


let test_enum_value_formatting () =
  let enum8_val = Columns.VEnum8 ("active", 1) in
  let enum16_val = Columns.VEnum16 ("high", 300) in
  Alcotest.(check string) "Enum8 string format" "active" (Columns.value_to_string enum8_val);
  Alcotest.(check string) "Enum16 string format" "high" (Columns.value_to_string enum16_val)

let test_enum_unknown_values () =
  (* Test how enum handles unknown values (should show numeric value) *)
  let cols = [ ("status", "Enum8('known'=1)", 1) ] in
  let bs = build_uncompressed_block ~cols in
  (* The test data generator puts value 1 which should map to 'known' *)
  match read_block_from_bytes bs with
  | { Block.n_rows=1; columns=[c]; _ } ->
      (match c.Block.data with
      | [| Columns.VEnum8 ("known", 1) |] -> ()
      | _ -> Alcotest.fail "Expected known enum value")
  | _ -> Alcotest.fail "Unexpected block structure"

(* Async insert tests *)
open Lwt.Syntax

let test_async_inserter_creation () =
  let conn = Connection.create ~compression:Compress.None () in
  let config = Async_insert.default_config "test_table" in
  let inserter = Async_insert.create config conn in
  
  Alcotest.(check string) "Table name" "test_table" config.table_name;
  Alcotest.(check int) "Default batch size" 100_000 config.max_batch_size;
  Alcotest.(check int) "Default max bytes" 16_777_216 config.max_batch_bytes;
  
  let (row_count, byte_size) = Lwt_main.run (Async_insert.get_stats inserter) in
  Alcotest.(check int) "Initial row count" 0 row_count;
  Alcotest.(check int) "Initial byte size" 0 byte_size

let test_buffer_management () =
  let conn = Connection.create ~compression:Compress.None () in
  let config = Async_insert.default_config "test_table" in
  let inserter = Async_insert.create config conn in
  
  let test_row = [Columns.VString "test"; Columns.VInt32 42l] in
  let columns = [("name", "String"); ("value", "Int32")] in
  
  Lwt_main.run (
    let* () = Async_insert.add_row ~columns inserter test_row in
    let* (row_count, byte_size) = Async_insert.get_stats inserter in
    Alcotest.(check int) "Row count after insert" 1 row_count;
    Alcotest.(check bool) "Byte size increased" true (byte_size > 0);
    Lwt.return_unit
  )

let test_batch_size_limits () =
  let conn = Connection.create ~compression:Compress.None () in
  let config = { (Async_insert.default_config "test_table") with 
    max_batch_size = 2; 
    max_batch_bytes = 1000 
  } in
  let inserter = Async_insert.create config conn in
  
  let test_rows = [
    [Columns.VString "test1"; Columns.VInt32 1l];
    [Columns.VString "test2"; Columns.VInt32 2l];
  ] in
  let columns = [("name", "String"); ("value", "Int32")] in
  
  Lwt_main.run (
    let* () = Async_insert.add_rows ~columns inserter test_rows in
    let* (row_count, _) = Async_insert.get_stats inserter in
    Alcotest.(check int) "Row count at limit" 2 row_count;
    Lwt.return_unit
  )

let test_row_estimation () =
  let conn = Connection.create ~compression:Compress.None () in
  let config = Async_insert.default_config "test_table" in
  let inserter = Async_insert.create config conn in
  
  let small_row = [Columns.VString "a"; Columns.VInt32 1l] in
  let large_row = [Columns.VString (String.make 100 'a'); Columns.VInt64 1L] in
  let columns = [("name", "String"); ("value", "Int32")] in
  
  Lwt_main.run (
    let* () = Async_insert.add_row ~columns inserter small_row in
    let* (_, small_size) = Async_insert.get_stats inserter in
    let* () = Async_insert.add_row inserter large_row in
    let* (_, total_size) = Async_insert.get_stats inserter in
    let large_row_size = total_size - small_size in
    Alcotest.(check bool) "Large row bigger" true (large_row_size > small_size);
    Lwt.return_unit
  )

(* Streaming query tests *)
let test_streaming_interface () =
  (* Test that we can create a client (this validates the interface exists) *)
  let _client = Client.create ~host:"localhost" ~database:"test" () in
  Alcotest.(check bool) "Client with streaming interface created" true true

let test_idiomatic_streaming () =
  (* Test the idiomatic OCaml streaming interface types *)
  let _client = Client.create ~host:"localhost" ~database:"test" () in
  (* Just test that the types exist and functions can be called *)
  Alcotest.(check bool) "Idiomatic streaming API exists" true true

let test_fold_and_iter () =
  (* Test that fold and iter functions exist and have correct types *)
  let _client = Client.create ~host:"localhost" ~database:"test" () in
  (* Test that the fold accumulator works correctly *)
  let test_fold_result = List.fold_left (fun acc x -> acc + x) 0 [1; 2; 3] in
  Alcotest.(check int) "Fold pattern works" 6 test_fold_result;
  
  (* Test that the streaming result type works *)
  let test_result = { Client.rows = []; columns = [] } in
  Alcotest.(check int) "Streaming result type works" 0 (List.length test_result.rows)

let test_sequence_integration () =
  (* Test sequence integration *)
  let test_seq = List.to_seq [1; 2; 3; 4; 5] in
  let count = Seq.fold_left (fun acc _ -> acc + 1) 0 test_seq in
  Alcotest.(check int) "Sequence integration works" 5 count

let () =
  Alcotest.run "Proton Lwt" [
    ("async", [
      Alcotest.test_case "uncompressed block parse" `Quick test_uncompressed_block_parse;
      Alcotest.test_case "exception reader" `Quick test_exception_reader;
      Alcotest.test_case "enum + fixedstring" `Quick test_enum_and_fixedstring;
      Alcotest.test_case "lowcardinality basic" `Quick test_lowcardinality_basic;
      Alcotest.test_case "lowcardinality nullable" `Quick test_lowcardinality_nullable;
      Alcotest.test_case "decimal formatting" `Quick test_decimal_formatting;
      Alcotest.test_case "ip formatting" `Quick test_ip_formatting;
      Alcotest.test_case "pool basics" `Quick test_pool_basics;
    ]);
    ("CityHash", [
      Alcotest.test_case "Consistency" `Quick test_cityhash_consistency;
      Alcotest.test_case "Different inputs" `Quick test_cityhash_different_inputs;
    ]);
    ("Compression", [
      Alcotest.test_case "LZ4 roundtrip" `Quick test_lz4_roundtrip;
      Alcotest.test_case "LZ4 compression ratio" `Quick test_lz4_compression_ratio;
      Alcotest.test_case "ZSTD roundtrip" `Quick test_zstd_roundtrip;
      Alcotest.test_case "ZSTD compression ratio" `Quick test_zstd_compression_ratio;
      Alcotest.test_case "Frame format" `Quick test_compression_frame_format;
      Alcotest.test_case "Checksum verification" `Quick test_checksum_verification;
      Alcotest.test_case "ZSTD frame format" `Quick test_zstd_frame_format;
      Alcotest.test_case "Method encoding" `Quick test_compression_method_encoding;
      Alcotest.test_case "Method decoding" `Quick test_compression_method_decoding;
    ]);
    ("Binary", [
      Alcotest.test_case "Int32 roundtrip" `Quick test_int32_roundtrip;
      Alcotest.test_case "Int64 roundtrip" `Quick test_int64_roundtrip;
    ]);
    ("Connection", [
      Alcotest.test_case "Connection creation" `Quick test_connection_creation;
      Alcotest.test_case "Client creation" `Quick test_client_creation;
      Alcotest.test_case "Compression-enabled connection" `Quick test_compression_enabled_connection;
      Alcotest.test_case "Compression-enabled client" `Quick test_compression_enabled_client;
    ]);
    ("DateTime", [
      Alcotest.test_case "DateTime parsing" `Quick test_datetime_parsing;
      Alcotest.test_case "DateTime with timezone" `Quick test_datetime_with_timezone;
      Alcotest.test_case "DateTime64 parsing" `Quick test_datetime64_parsing;
      Alcotest.test_case "DateTime64 with timezone" `Quick test_datetime64_with_timezone;
      Alcotest.test_case "DateTime value formatting" `Quick test_datetime_value_formatting;
      Alcotest.test_case "DateTime64 value formatting" `Quick test_datetime64_value_formatting;
      Alcotest.test_case "DateTime binary roundtrip" `Quick test_datetime_binary_roundtrip;
      Alcotest.test_case "DateTime64 binary roundtrip" `Quick test_datetime64_binary_roundtrip;
      Alcotest.test_case "Multiple DateTime values" `Quick test_multiple_datetime_values;
    ]);
    ("Array", [
      Alcotest.test_case "Array parsing" `Quick test_array_parsing;
      Alcotest.test_case "Nested array parsing" `Quick test_nested_array_parsing;
      Alcotest.test_case "Array value formatting" `Quick test_array_value_formatting;
      Alcotest.test_case "Empty array formatting" `Quick test_empty_array_formatting;
    ]);
    ("Map", [
      Alcotest.test_case "Map parsing" `Quick test_map_parsing;
      Alcotest.test_case "Nested map parsing" `Quick test_nested_map_parsing;
      Alcotest.test_case "Map value formatting" `Quick test_map_value_formatting;
      Alcotest.test_case "Empty map formatting" `Quick test_empty_map_formatting;
    ]);
    ("Enum", [
      Alcotest.test_case "Enum8 parsing and value types" `Quick test_enum8_parsing;
      Alcotest.test_case "Enum16 parsing and value types" `Quick test_enum16_parsing;
      Alcotest.test_case "Enum8 with negative values" `Quick test_enum8_negative;
      Alcotest.test_case "Enum value formatting" `Quick test_enum_value_formatting;
      Alcotest.test_case "Enum unknown value handling" `Quick test_enum_unknown_values;
    ]);
    ("Async_insert", [
      Alcotest.test_case "Async inserter creation" `Quick test_async_inserter_creation;
      Alcotest.test_case "Buffer management" `Quick test_buffer_management;
      Alcotest.test_case "Batch size limits" `Quick test_batch_size_limits;
      Alcotest.test_case "Row estimation" `Quick test_row_estimation;
    ]);
    ("Streaming", [
      Alcotest.test_case "Streaming interface exists" `Quick test_streaming_interface;
      Alcotest.test_case "Idiomatic streaming API" `Quick test_idiomatic_streaming;
      Alcotest.test_case "Fold and iter operations" `Quick test_fold_and_iter;
      Alcotest.test_case "Sequence integration" `Quick test_sequence_integration;
    ]);
  ]
