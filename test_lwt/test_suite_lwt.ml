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
      | s when String.length s >= 12 && String.sub s 0 12 = "fixedstring(" ->
          Buffer.add_string buf "abc"; Buffer.add_char buf '\x00'
      | s when String.length s >= 6 && String.sub s 0 6 = "enum8(" ->
          Buffer.add_char buf (Char.chr 1)
      | s when String.length s >= 7 && String.sub s 0 7 = "enum16(" ->
          Buffer.add_string buf "\x01\x00"
      | "int32" | "uint32" -> writeln_int32 42
      | "int64" | "uint64" -> writeln_int64 42L
      | "float64" -> Buffer.add_string buf (Bytes.to_string (Bytes.make 8 '\x00'))
      | "ipv4" -> writeln_int32 ((127 lsl 24) lor 1)
      | s when String.length s >= 8 && String.sub s 0 8 = "decimal(" ->
          (* write signed int64 raw: e.g., 12345 for Decimal(10,2) -> 123.45 *)
          writeln_int64 12345L
      | "ipv6" -> Buffer.add_string buf (Bytes.to_string (Bytes.make 16 '\x01'))
      | "uuid" -> Buffer.add_string buf (Bytes.to_string (Bytes.make 16 '\x01'))
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

let test_enum_and_fixedstring () =
  let open Lwt.Infix in
  let cols = [ ("e8", "Enum8('A'=1,'B'=2)", 2); ("fs", "FixedString(4)", 2) ] in
  let bs = build_uncompressed_block ~cols in
  let pos = ref 0 in
  let read_fn buf off len = let rem = Bytes.length bs - !pos in let n = min len rem in Bytes.blit bs !pos buf off n; pos := !pos + n; Lwt.return n in
  let t = Connection.create ~compression:Compress.None () in
  t.Connection.srv <- Some { Context.name=""; version_major=0; version_minor=0; version_patch=0; revision=Defines.dbms_min_revision_with_block_info; timezone=None; display_name="" };
  match Lwt_main.run (Connection.receive_data t ~raw:true read_fn) with
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
  let pos = ref 0 in
  let read_fn b o l = let rem = Bytes.length bs - !pos in let n = min l rem in Bytes.blit bs !pos b o n; pos := !pos + n; Lwt.return n in
  let t = Connection.create ~compression:Compress.None () in
  t.Connection.srv <- Some { Context.name=""; version_major=0; version_minor=0; version_patch=0; revision=Defines.dbms_min_revision_with_block_info; timezone=None; display_name="" };
  match Lwt_main.run (Connection.receive_data t ~raw:true read_fn) with
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
  let pos = ref 0 in
  let read_fn b o l = let rem = Bytes.length bs - !pos in let n = min l rem in Bytes.blit bs !pos b o n; pos := !pos + n; Lwt.return n in
  let t = Connection.create ~compression:Compress.None () in
  t.Connection.srv <- Some { Context.name=""; version_major=0; version_minor=0; version_patch=0; revision=Defines.dbms_min_revision_with_block_info; timezone=None; display_name="" };
  match Lwt_main.run (Connection.receive_data t ~raw:true read_fn) with
  | { Block.n_rows=3; columns=[c]; _ } ->
      Alcotest.(check string) "type" "LowCardinality(Nullable(String))" c.Block.type_spec
  | _ -> Alcotest.fail "Unexpected LC(N) block"

let test_decimal_formatting () =
  let cols = [ ("d", "Decimal(10,2)", 1) ] in
  let bs = build_uncompressed_block ~cols in
  let pos = ref 0 in
  let read_fn buf off len = let rem = Bytes.length bs - !pos in let n = min len rem in Bytes.blit bs !pos buf off n; pos := !pos + n; Lwt.return n in
  let t = Connection.create ~compression:Compress.None () in
  t.Connection.srv <- Some { Context.name=""; version_major=0; version_minor=0; version_patch=0; revision=Defines.dbms_min_revision_with_block_info; timezone=None; display_name="" };
  match Lwt_main.run (Connection.receive_data t ~raw:true read_fn) with
  | { Block.n_rows=1; columns=[c]; _ } ->
      Alcotest.(check string) "type" "Decimal(10,2)" c.Block.type_spec
  | _ -> Alcotest.fail "Unexpected Decimal block"

let test_ip_formatting () =
  let cols = [ ("v4","IPv4",1); ("v6","IPv6",1) ] in
  let bs = build_uncompressed_block ~cols in
  let pos = ref 0 in
  let read_fn buf off len = let rem = Bytes.length bs - !pos in let n = min len rem in Bytes.blit bs !pos buf off n; pos := !pos + n; Lwt.return n in
  let t = Connection.create ~compression:Compress.None () in
  t.Connection.srv <- Some { Context.name=""; version_major=0; version_minor=0; version_patch=0; revision=Defines.dbms_min_revision_with_block_info; timezone=None; display_name="" };
  ignore (Lwt_main.run (Connection.receive_data t ~raw:true read_fn));
  ()

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
      Alcotest.test_case "enum + fixedstring" `Quick test_enum_and_fixedstring;
      Alcotest.test_case "lowcardinality basic" `Quick test_lowcardinality_basic;
      Alcotest.test_case "lowcardinality nullable" `Quick test_lowcardinality_nullable;
      Alcotest.test_case "decimal formatting" `Quick test_decimal_formatting;
      Alcotest.test_case "ip formatting" `Quick test_ip_formatting;
      Alcotest.test_case "pool basics" `Quick test_pool_basics;
    ]);
  ]
