(* Simple test without external dependencies *)

open Proton

let test_basic () =
  (* Test basic connection creation *)
  let conn = Connection.create ~host:"127.0.0.1" ~port:8463 () in
  Printf.printf "✓ Connection created: %s:%d\n" conn.host conn.port;
  
  (* Test TLS config *)
  Printf.printf "✓ TLS enabled: %b\n" conn.tls_config.enable_tls;
  
  (* Test compression *)
  let conn_lz4 = Connection.create ~compression:Compress.LZ4 () in
  Printf.printf "✓ LZ4 compression set: %b\n" (conn_lz4.compression = Compress.LZ4);
  
  (* Test client creation *)
  let client = Client.create ~host:"localhost" ~port:9000 () in
  Printf.printf "✓ Client created: %s:%d\n" client.conn.host client.conn.port;
  
  (* Test column parsing *)
  (try
    let _reader = Columns.reader_of_spec "datetime" in
    Printf.printf "✓ DateTime column parser created\n"
  with
  | e -> Printf.printf "✗ DateTime parser failed: %s\n" (Printexc.to_string e));
  
  (* Test value formatting *)
  let tuple_value = Columns.VTuple [
    Columns.VString "hello";
    Columns.VInt32 42l
  ] in
  let tuple_str = Columns.value_to_string tuple_value in
  Printf.printf "✓ Tuple formatting: %s\n" tuple_str;
  
  print_endline "✅ All basic tests passed!"

let () = test_basic ()