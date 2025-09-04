(* Main test suite for Proton OCaml driver *)

open Proton

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

(* Compression frame format tests *)
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

(* Protocol tests *)
let test_compression_method_encoding () =
  Alcotest.(check int) "None method byte" 0x02 (Compress.method_to_byte Compress.None);
  Alcotest.(check int) "LZ4 method byte" 0x82 (Compress.method_to_byte Compress.LZ4);
  Alcotest.(check int) "ZSTD method byte" 0x90 (Compress.method_to_byte Compress.ZSTD)

let test_compression_method_decoding () =
  Alcotest.(check bool) "Decode None" true (Compress.method_of_byte 0x02 = Compress.None);
  Alcotest.(check bool) "Decode LZ4" true (Compress.method_of_byte 0x82 = Compress.LZ4);
  Alcotest.(check bool) "Decode ZSTD" true (Compress.method_of_byte 0x90 = Compress.ZSTD)

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

(* Define test suites *)
let cityhash_tests = [
  Alcotest.test_case "Consistency" `Quick test_cityhash_consistency;
  Alcotest.test_case "Different inputs" `Quick test_cityhash_different_inputs;
]

let compression_tests = [
  Alcotest.test_case "LZ4 roundtrip" `Quick test_lz4_roundtrip;
  Alcotest.test_case "LZ4 compression ratio" `Quick test_lz4_compression_ratio;
  Alcotest.test_case "Frame format" `Quick test_compression_frame_format;
  Alcotest.test_case "Checksum verification" `Quick test_checksum_verification;
  Alcotest.test_case "Method encoding" `Quick test_compression_method_encoding;
  Alcotest.test_case "Method decoding" `Quick test_compression_method_decoding;
]

let binary_tests = [
  Alcotest.test_case "Int32 roundtrip" `Quick test_int32_roundtrip;
  Alcotest.test_case "Int64 roundtrip" `Quick test_int64_roundtrip;
]

let connection_tests = [
  Alcotest.test_case "Connection creation" `Quick test_connection_creation;
  Alcotest.test_case "Client creation" `Quick test_client_creation;
  Alcotest.test_case "Compression-enabled connection" `Quick test_compression_enabled_connection;
  Alcotest.test_case "Compression-enabled client" `Quick test_compression_enabled_client;
]

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
  let br = Buffered_reader.create_from_bytes bytes_data in
  
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
  
  (* Create 8-byte binary data manually *)
  let bytes_data = Bytes.create 8 in
  let low = Int64.to_int (Int64.logand test_value 0xFFFFFFFFL) in
  let high = Int64.to_int (Int64.shift_right_logical test_value 32) in
  
  (* Low 4 bytes *)
  let a = low land 0xFF in
  let b = (low lsr 8) land 0xFF in
  let c = (low lsr 16) land 0xFF in
  let d = (low lsr 24) land 0xFF in
  
  (* High 4 bytes *)
  let e = high land 0xFF in
  let f = (high lsr 8) land 0xFF in
  let g = (high lsr 16) land 0xFF in
  let h = (high lsr 24) land 0xFF in
  
  Bytes.set_uint8 bytes_data 0 a;
  Bytes.set_uint8 bytes_data 1 b;
  Bytes.set_uint8 bytes_data 2 c;
  Bytes.set_uint8 bytes_data 3 d;
  Bytes.set_uint8 bytes_data 4 e;
  Bytes.set_uint8 bytes_data 5 f;
  Bytes.set_uint8 bytes_data 6 g;
  Bytes.set_uint8 bytes_data 7 h;
  
  (* Create buffered reader from written data *)
  let br = Buffered_reader.create_from_bytes bytes_data in
  
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
  
  let br = Buffered_reader.create_from_bytes bytes_data in
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

let datetime_tests = [
  Alcotest.test_case "DateTime parsing" `Quick test_datetime_parsing;
  Alcotest.test_case "DateTime with timezone" `Quick test_datetime_with_timezone;
  Alcotest.test_case "DateTime64 parsing" `Quick test_datetime64_parsing;
  Alcotest.test_case "DateTime64 with timezone" `Quick test_datetime64_with_timezone;
  Alcotest.test_case "DateTime value formatting" `Quick test_datetime_value_formatting;
  Alcotest.test_case "DateTime64 value formatting" `Quick test_datetime64_value_formatting;
  Alcotest.test_case "DateTime binary roundtrip" `Quick test_datetime_binary_roundtrip;
  Alcotest.test_case "DateTime64 binary roundtrip" `Quick test_datetime64_binary_roundtrip;
  Alcotest.test_case "Multiple DateTime values" `Quick test_multiple_datetime_values;
]

(* Main test runner *)
let () =
  Alcotest.run "Proton OCaml Driver" [
    "CityHash", cityhash_tests;
    "Compression", compression_tests;
    "Binary", binary_tests;
    "Connection", connection_tests;
    "DateTime", datetime_tests;
  ]