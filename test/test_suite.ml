(* Main test suite for Proton OCaml driver - Lwt/Async version *)

open Proton
open Lwt.Infix

(* CityHash tests - still synchronous *)
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

(* LZ4 Compression tests - still synchronous *)
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

(* Binary encoding tests - still synchronous *)
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

(* Connection tests - now synchronous creation, no actual connection *)
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

(* TLS configuration tests - synchronous config creation *)
let test_tls_config_creation () =
  (* Test basic TLS configuration *)
  let tls_config = {
    Connection.enable_tls = true;
    ca_cert_file = None;
    client_cert_file = None;
    client_key_file = None;
    verify_hostname = true;
    insecure_skip_verify = false;
  } in
  let conn = Connection.create ~tls_config () in
  Alcotest.(check bool) "TLS enabled in connection" true conn.tls_config.enable_tls

let test_tls_insecure_config () =
  (* Test insecure TLS configuration *)
  let insecure_tls_config = {
    Connection.enable_tls = true;
    ca_cert_file = None;
    client_cert_file = None;
    client_key_file = None;
    verify_hostname = false;
    insecure_skip_verify = true;
  } in
  let conn = Connection.create ~tls_config:insecure_tls_config () in
  Alcotest.(check bool) "Insecure TLS config" true conn.tls_config.insecure_skip_verify;
  Alcotest.(check bool) "Hostname verification disabled" false conn.tls_config.verify_hostname

let test_tls_default_config () =
  (* Test that default TLS config is now enabled by default *)
  let conn = Connection.create () in
  Alcotest.(check bool) "TLS enabled by default" true conn.tls_config.enable_tls;
  Alcotest.(check bool) "Hostname verification enabled by default" true conn.tls_config.verify_hostname;
  Alcotest.(check bool) "Secure by default" false conn.tls_config.insecure_skip_verify

let test_tls_with_client_creation () =
  (* Test that Client can be created with TLS config *)
  let tls_config = {
    Connection.enable_tls = false; (* Disabled for testing *)
    ca_cert_file = None;
    client_cert_file = None;
    client_key_file = None;
    verify_hostname = true;
    insecure_skip_verify = false;
  } in
  let client = Client.create ~host:"localhost" ~port:9440 ~tls_config () in
  (* Verify the TLS config is stored properly *)
  Alcotest.(check bool) "Client TLS config stored" false client.conn.tls_config.enable_tls

let test_tls_mtls_config () =
  (* Test mTLS configuration structure *)
  let mtls_config = {
    Connection.enable_tls = true;
    ca_cert_file = Some "/path/to/ca.pem";
    client_cert_file = Some "/path/to/client.pem";
    client_key_file = Some "/path/to/client.key";
    verify_hostname = true;
    insecure_skip_verify = false;
  } in
  let conn = Connection.create ~tls_config:mtls_config () in
  (* Verify mTLS config fields *)
  Alcotest.(check bool) "mTLS enabled" true conn.tls_config.enable_tls;
  Alcotest.(check (option string)) "CA cert file" (Some "/path/to/ca.pem") conn.tls_config.ca_cert_file;
  Alcotest.(check (option string)) "Client cert file" (Some "/path/to/client.pem") conn.tls_config.client_cert_file;
  Alcotest.(check (option string)) "Client key file" (Some "/path/to/client.key") conn.tls_config.client_key_file

let test_tls_connection_state () =
  (* Test connection state fields *)
  let conn = Connection.create () in
  (* Check initial state *)
  Alcotest.(check bool) "Initially not connected" false conn.connected;
  (* We can't easily test the Option types with Alcotest, so just check they're None *)
  Alcotest.(check bool) "No TLS connection initially" true (conn.tls = None);
  Alcotest.(check bool) "No file descriptor initially" true (conn.fd = None)

(* Column tests - still synchronous *)
let test_datetime_parsing () =
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

let test_array_parsing () =
  try
    let _reader = Columns.reader_of_spec "array(string)" in
    Alcotest.(check bool) "Array parser created" true true
  with
  | _ -> Alcotest.(check bool) "Array parser created" true false

let test_map_parsing () =
  try
    let _reader = Columns.reader_of_spec "map(string, int32)" in
    Alcotest.(check bool) "Map parser created" true true
  with
  | _ -> Alcotest.(check bool) "Map parser created" true false

(* Note: Pool module was not mentioned in the new implementation, 
   so we'll skip pool tests for now *)

(* Define test suites *)
let cityhash_tests = [
  Alcotest.test_case "Consistency" `Quick test_cityhash_consistency;
  Alcotest.test_case "Different inputs" `Quick test_cityhash_different_inputs;
]

let compression_tests = [
  Alcotest.test_case "LZ4 roundtrip" `Quick test_lz4_roundtrip;
  Alcotest.test_case "LZ4 compression ratio" `Quick test_lz4_compression_ratio;
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

let tls_tests = [
  Alcotest.test_case "TLS config creation" `Quick test_tls_config_creation;
  Alcotest.test_case "TLS insecure config" `Quick test_tls_insecure_config;
  Alcotest.test_case "TLS default config" `Quick test_tls_default_config;
  Alcotest.test_case "TLS with client creation" `Quick test_tls_with_client_creation;
  Alcotest.test_case "TLS mTLS config" `Quick test_tls_mtls_config;
  Alcotest.test_case "TLS connection state" `Quick test_tls_connection_state;
]

let column_tests = [
  Alcotest.test_case "DateTime parsing" `Quick test_datetime_parsing;
  Alcotest.test_case "DateTime with timezone" `Quick test_datetime_with_timezone;
  Alcotest.test_case "DateTime64 parsing" `Quick test_datetime64_parsing;
  Alcotest.test_case "Array parsing" `Quick test_array_parsing;
  Alcotest.test_case "Map parsing" `Quick test_map_parsing;
]

(* Main test runner *)
let () =
  Alcotest.run "Proton OCaml Driver (Async/TLS)" [
    "CityHash", cityhash_tests;
    "Compression", compression_tests;
    "Binary", binary_tests;
    "Connection", connection_tests;
    "TLS", tls_tests;
    "Columns", column_tests;
  ]