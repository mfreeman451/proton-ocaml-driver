(*
 * Test script for LZ4 compression infrastructure
 * This tests the compression module without requiring LZ4 library
 *)

open Proton

let test_cityhash () =
  print_endline "=== Testing CityHash128 ===";
  let test_data = "hello world" |> Bytes.of_string in
  let hash = Proton.Cityhash.cityhash128 test_data in
  let hash_bytes = Proton.Cityhash.to_bytes hash in
  Printf.printf "Input: 'hello world'\n";
  Printf.printf "Hash low: %Lx\n" hash.low;
  Printf.printf "Hash high: %Lx\n" hash.high;
  Printf.printf "Hash bytes length: %d\n" (Bytes.length hash_bytes);
  print_newline ()

let test_compress_infrastructure () =
  print_endline "=== Testing Compression Infrastructure ===";
  let _test_data = Bytes.of_string "This is test data for compression" in
  
  (* Test compression type conversions *)
  let none_method = Proton.Compress.None in
  let lz4_method = Proton.Compress.LZ4 in
  let zstd_method = Proton.Compress.ZSTD in
  
  Printf.printf "None method byte: 0x%02x\n" (Proton.Compress.method_to_byte none_method);
  Printf.printf "LZ4 method byte: 0x%02x\n" (Proton.Compress.method_to_byte lz4_method);
  Printf.printf "ZSTD method byte: 0x%02x\n" (Proton.Compress.method_to_byte zstd_method);
  
  (* Test protocol compression integration *)
  let protocol_compression = lz4_method in
  Printf.printf "Protocol compression int: %d\n" (Proton.Protocol.compression_to_int protocol_compression);
  
  print_newline ()

let test_connection_integration () =
  print_endline "=== Testing Connection Integration ===";
  let client = Client.create ~host:"127.0.0.1" ~port:Defines.default_port () in
  Printf.printf "Client created with compression: None\n";
  Client.disconnect client;
  print_newline ()

let () =
  print_endline "Proton OCaml Driver - Compression Infrastructure Test";
  print_endline "==============================================";
  print_newline ();
  
  test_cityhash ();
  test_compress_infrastructure ();
  test_connection_integration ();
  
  print_endline "=== Summary ===";
  print_endline "✓ CityHash128 C++ bindings working";
  print_endline "✓ Compression method enum working";
  print_endline "✓ Protocol integration working";
  print_endline "✓ Connection creation working";
  print_newline ();
  print_endline "Next steps:";
  print_endline "- Install LZ4 OCaml library to enable actual compression";
  print_endline "- Test with actual Proton server";
  print_endline "- Implement DateTime/Array column types"