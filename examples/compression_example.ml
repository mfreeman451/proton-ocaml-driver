(*
 * Example demonstrating LZ4 compression usage with Proton driver
 *)

let () =
  let open Proton in
  Lwt_main.run @@ let open Lwt.Infix in
  let client_lz4 = Client.create 
    ~host:"127.0.0.1" 
    ~port:Defines.default_port 
    ~compression:Compress.LZ4
    () in
  Lwt_io.printl "=== Proton OCaml Driver - Compression Example ===" >>= fun () ->
  Lwt_io.printl "3. Compression protocol bytes:" >>= fun () ->
  Lwt_io.printf "   None method byte: 0x%02x\n" (Compress.method_to_byte Compress.None) >>= fun () ->
  Lwt_io.printf "   LZ4 method byte:  0x%02x\n" (Compress.method_to_byte Compress.LZ4) >>= fun () ->
  Lwt_io.printf "   ZSTD method byte: 0x%02x\n" (Compress.method_to_byte Compress.ZSTD) >>= fun () ->
  let test_data = "The quick brown fox jumps over the lazy dog" in
  let original = Bytes.of_string test_data in
  let compressed = Compress.compress_lz4 original in
  let decompressed = Compress.decompress_lz4 compressed (Bytes.length original) in
  Lwt_io.printf "   Original size: %d bytes\n" (Bytes.length original) >>= fun () ->
  Lwt_io.printf "   Compressed size: %d bytes\n" (Bytes.length compressed) >>= fun () ->
  Lwt_io.printf "   Compression ratio: %.2f%%%%\n" 
    (100.0 *. (float (Bytes.length compressed)) /. (float (Bytes.length original))) >>= fun () ->
  Lwt_io.printf "   Roundtrip successful: %b\n" 
    (Bytes.to_string decompressed = test_data) >>= fun () ->
  Client.disconnect client_lz4
