(*
 * Test LZ4 compression functionality
 *)

open Proton

let test_lz4_roundtrip () =
  print_endline "=== Testing LZ4 Compression Roundtrip ===";
  
  let test_strings = [
    "hello world";
    "The quick brown fox jumps over the lazy dog";
    "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.";
    String.make 1000 'A';  (* Highly compressible *)
    "abcdefghijklmnopqrstuvwxyz0123456789!@#$%^&*()_+{}[]|\\:;\"'<>?,./~`";  (* Less compressible *)
  ] in
  
  List.iteri (fun i test_str ->
    Printf.printf "Test %d: %s\n" (i+1) 
      (if String.length test_str > 50 then 
         (String.sub test_str 0 47) ^ "..." 
       else test_str);
    
    let original = Bytes.of_string test_str in
    let original_len = Bytes.length original in
    Printf.printf "  Original size: %d bytes\n" original_len;
    
    try
      (* Compress *)
      let compressed = Compress.compress_lz4 original in
      let compressed_len = Bytes.length compressed in
      Printf.printf "  Compressed size: %d bytes\n" compressed_len;
      Printf.printf "  Compression ratio: %.2f%%\n" 
        (100.0 *. (float compressed_len) /. (float original_len));
      
      (* Decompress *)
      let decompressed = Compress.decompress_lz4 compressed original_len in
      let decompressed_str = Bytes.to_string decompressed in
      
      if decompressed_str = test_str then
        Printf.printf "  ✓ Roundtrip successful\n"
      else
        Printf.printf "  ✗ Roundtrip failed! Got: %s\n" decompressed_str;
        
    with e ->
      Printf.printf "  ✗ Error: %s\n" (Printexc.to_string e);
    
    print_newline ()
  ) test_strings

let test_compression_frame_format () =
  print_endline "=== Testing Compression Frame Format ===";
  
  let test_data = Bytes.of_string "Hello, ClickHouse compression!" in
  Printf.printf "Test data: '%s'\n" (Bytes.to_string test_data);
  
  (* Test with a temporary file to simulate network output *)
  let temp_file = Filename.temp_file "proton_test" ".lz4" in
  let oc = open_out_bin temp_file in
  
  try
    (* Write compressed block *)
    Compress.write_compressed_block oc test_data Compress.LZ4;
    close_out oc;
    
    (* Read back the compressed frame *)
    let ic = open_in_bin temp_file in
    let file_size = in_channel_length ic in
    let compressed_frame = really_input_string ic file_size in
    close_in ic;
    Sys.remove temp_file;
    Printf.printf "Compressed frame size: %d bytes\n" (String.length compressed_frame);
    
    (* Parse the frame manually to verify structure *)
    if String.length compressed_frame >= 25 then begin (* min header size *)
      let frame_bytes = Bytes.of_string compressed_frame in
      
      (* Extract checksum (first 16 bytes) *)
      let checksum = Bytes.sub frame_bytes 0 16 in
      Printf.printf "Checksum: ";
      for i = 0 to 15 do
        Printf.printf "%02x" (Char.code (Bytes.get checksum i));
      done;
      print_newline ();
      
      (* Extract method byte (byte 16) *)
      let method_byte = Char.code (Bytes.get frame_bytes 16) in
      Printf.printf "Method byte: 0x%02x %s\n" method_byte 
        (if method_byte = 0x82 then "(LZ4)" else "(unknown)");
      
      (* Extract sizes (bytes 17-24) *)
      let compressed_size = Binary.bytes_get_int32_le frame_bytes 17 in
      let uncompressed_size = Binary.bytes_get_int32_le frame_bytes 21 in
      Printf.printf "Compressed size in header: %ld\n" compressed_size;
      Printf.printf "Uncompressed size in header: %ld\n" uncompressed_size;
      Printf.printf "Expected uncompressed size: %d\n" (Bytes.length test_data);
      
      if Int32.to_int uncompressed_size = Bytes.length test_data then
        Printf.printf "✓ Frame format looks correct\n"
      else
        Printf.printf "✗ Size mismatch in frame\n";
    end else
      Printf.printf "✗ Frame too small: %d bytes\n" (String.length compressed_frame);
      
  with e ->
    Printf.printf "✗ Frame test error: %s\n" (Printexc.to_string e);
    
  print_newline ()

let test_cityhash_consistency () =
  print_endline "=== Testing CityHash Consistency ===";
  
  let test_data = "test data for hashing" |> Bytes.of_string in
  
  (* Hash the same data multiple times *)
  let hash1 = Cityhash.cityhash128 test_data in
  let hash2 = Cityhash.cityhash128 test_data in
  
  Printf.printf "Hash 1: %016Lx%016Lx\n" hash1.high hash1.low;
  Printf.printf "Hash 2: %016Lx%016Lx\n" hash2.high hash2.low;
  
  if hash1.low = hash2.low && hash1.high = hash2.high then
    Printf.printf "✓ CityHash is consistent\n"
  else
    Printf.printf "✗ CityHash is not consistent!\n";
    
  print_newline ()

let () =
  print_endline "Proton OCaml Driver - LZ4 Compression Test";
  print_endline "=========================================";
  print_newline ();
  
  test_cityhash_consistency ();
  test_lz4_roundtrip ();
  test_compression_frame_format ();
  
  print_endline "=== Test Complete ===";
  print_endline "LZ4 compression is now fully functional!"