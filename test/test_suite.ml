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

(* Define test suites *)
let cityhash_tests = [
  Alcotest.test_case "Consistency" `Quick test_cityhash_consistency;
  Alcotest.test_case "Different inputs" `Quick test_cityhash_different_inputs;
]

let compression_tests = [
  Alcotest.test_case "LZ4 roundtrip" `Quick test_lz4_roundtrip;
  Alcotest.test_case "LZ4 compression ratio" `Quick test_lz4_compression_ratio;
  Alcotest.test_case "Frame format" `Quick test_compression_frame_format;
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
]

(* Main test runner *)
let () =
  Alcotest.run "Proton OCaml Driver" [
    "CityHash", cityhash_tests;
    "Compression", compression_tests;
    "Binary", binary_tests;
    "Connection", connection_tests;
  ]