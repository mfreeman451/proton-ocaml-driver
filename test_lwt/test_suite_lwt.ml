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
    for _i = 1 to rows do
      match String.lowercase_ascii (String.trim type_spec) with
      | "string" -> write_str buf "abc"
      | "int32" | "uint32" -> Buffer.add_string buf "\x2a\x00\x00\x00"
      | "int64" | "uint64" -> Buffer.add_string buf "\x2a\x00\x00\x00\x00\x00\x00\x00"
      | "float64" -> Buffer.add_string buf (Bytes.to_string (Bytes.init 8 (fun _ -> Char.chr 0)))
      | s when String.length s >= 9 && String.sub s 0 9 = "nullable(" ->
          Buffer.add_char buf '\x00'; write_str buf "abc"
      | other -> failwith ("unsupported in test: " ^ other)
    done
  ) cols;
  Buffer.contents buf |> Bytes.of_string

let test_uncompressed_block_parse () =
  let open Lwt.Infix in
  let bs = build_uncompressed_block ~cols:[ ("c1","String",2) ] in
  let pos = ref 0 in
  let read_fn buf off len =
    let remaining = Bytes.length bs - !pos in
    let to_copy = min len remaining in
    Bytes.blit bs !pos buf off to_copy; pos := !pos + to_copy; Lwt.return to_copy
  in
  let t = Connection.create ~compression:Compress.None () in
  t.Connection.srv <- Some { Context.name=""; version_major=0; version_minor=0; version_patch=0; revision=Defines.dbms_min_revision_with_block_info; timezone=None; display_name="" };
  match Lwt_main.run (Connection.receive_data t ~raw:true read_fn) with
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

let test_pool_basics () =
  let pool = Pool.create_pool (fun () -> Connection.create ()) in
  Pool.start_cleanup pool ~period:0.1;
  let used = Lwt_main.run (Pool.with_connection pool (fun _ -> Lwt.return 123)) in
  Alcotest.(check int) "with_connection returns" 123 used;
  let _ = Lwt_main.run (Pool.pool_stats pool) in
  Pool.stop_cleanup pool

let () =
  Alcotest.run "Proton Lwt" [
    ("async", [
      Alcotest.test_case "uncompressed block parse" `Quick test_uncompressed_block_parse;
      Alcotest.test_case "exception reader" `Quick test_exception_reader;
      Alcotest.test_case "pool basics" `Quick test_pool_basics;
    ]);
  ]
