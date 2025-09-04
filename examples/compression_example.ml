(*
 * Example demonstrating LZ4 compression usage with Proton driver
 *)

let () =
  let open Proton in
  
  print_endline "=== Proton OCaml Driver - Compression Example ===";
  print_newline ();
  
  (* Create clients with different compression settings *)
  print_endline "1. Creating clients with different compression settings:";
  
  let client_none = Client.create 
    ~host:"127.0.0.1" 
    ~port:Defines.default_port 
    ~compression:Compress.None
    () in
  print_endline "   ✓ Client with no compression";
  
  let client_lz4 = Client.create 
    ~host:"127.0.0.1" 
    ~port:Defines.default_port 
    ~compression:Compress.LZ4
    () in
  print_endline "   ✓ Client with LZ4 compression";
  
  print_newline ();
  
  (* Show compression settings *)
  print_endline "2. Compression settings:";
  Printf.printf "   None client compression: %s\n" 
    (match client_none.conn.compression with
     | Compress.None -> "None"
     | Compress.LZ4 -> "LZ4" 
     | Compress.ZSTD -> "ZSTD");
     
  Printf.printf "   LZ4 client compression: %s\n"
    (match client_lz4.conn.compression with
     | Compress.None -> "None"
     | Compress.LZ4 -> "LZ4"
     | Compress.ZSTD -> "ZSTD");
     
  print_newline ();
  
  (* Demonstrate compression method encoding *)
  print_endline "3. Compression protocol bytes:";
  Printf.printf "   None method byte: 0x%02x\n" (Compress.method_to_byte Compress.None);
  Printf.printf "   LZ4 method byte:  0x%02x\n" (Compress.method_to_byte Compress.LZ4);
  Printf.printf "   ZSTD method byte: 0x%02x\n" (Compress.method_to_byte Compress.ZSTD);
  
  print_newline ();
  
  (* Test compression directly *)
  print_endline "4. Testing LZ4 compression directly:";
  let test_data = "The quick brown fox jumps over the lazy dog" in
  let original = Bytes.of_string test_data in
  let compressed = Compress.compress_lz4 original in
  let decompressed = Compress.decompress_lz4 compressed (Bytes.length original) in
  
  Printf.printf "   Original size: %d bytes\n" (Bytes.length original);
  Printf.printf "   Compressed size: %d bytes\n" (Bytes.length compressed);
  Printf.printf "   Compression ratio: %.2f%%\n" 
    (100.0 *. (float (Bytes.length compressed)) /. (float (Bytes.length original)));
  Printf.printf "   Roundtrip successful: %b\n" 
    (Bytes.to_string decompressed = test_data);
  
  print_newline ();
  
  (* Cleanup *)
  Client.disconnect client_none;
  Client.disconnect client_lz4;
  
  print_endline "=== Compression example completed ===";
  print_endline "Note: To test with actual Proton server:";
  print_endline "1. Start Proton server on localhost:8463";
  print_endline "2. Use Client.execute with queries";
  print_endline "3. Compression will be negotiated automatically"