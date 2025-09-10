open Proton

let test_has_prefix () =
  let check msg exp got = Alcotest.(check bool) msg exp got in
  (* has_prefix available via Column (include Column_types) *)
  check "exact match" true (Column.has_prefix "datetime64" "datetime64");
  check "simple true" true (Column.has_prefix "datetime64(3)" "datetime64");
  check "simple false" false (Column.has_prefix "date" "datetime");
  check "longer pattern than string" false (Column.has_prefix "abc" "abcdef");
  check "empty pattern" true (Column.has_prefix "abc" "");
  check "empty string, non-empty pattern" false (Column.has_prefix "" "x")

let test_reader_resolution_noalloc_prefix () =
  (* Ensure common prefixes resolve to reader functions and accept n=0 *)
  let specs =
    [
      "Int32";
      "UInt64";
      "Float64";
      "FixedString(4)";
      "Enum8('A'=1)";
      "Enum16('A'=1)";
      "Decimal(10,2)";
      "Array(Int32)";
      "Map(Int32,String)";
      "Tuple(Int32,Float64)";
      "Nullable(Int32)";
      "DateTime";
      "DateTime64(3)";
      (* LowCardinality readers require dictionary+keys headers even for n=0; skip in this zero-length micro-test *)
    ]
  in
  List.iter
    (fun spec ->
      (* buffered reader variant *)
      let br = Buffered_reader.create_from_bytes_no_copy (Bytes.create 0) in
      let r_br = Column.reader_of_spec_br spec in
      let arr = r_br br 0 in
      Alcotest.(check int) ("zero-length ok: " ^ spec) 0 (Array.length arr);
      (* channel variant *)
      let ic = open_in_gen [ Open_binary ] 0 "/dev/null" in
      let r = Column.reader_of_spec spec in
      let arr2 = r ic 0 in
      Alcotest.(check int) ("zero-length ok ch: " ^ spec) 0 (Array.length arr2);
      close_in_noerr ic)
    specs

let () =
  let open Alcotest in
  run "columns-prefix-and-readers"
    [
      ("prefix", [ test_case "has_prefix" `Quick test_has_prefix ]);
      ("readers", [ test_case "resolve readers (n=0)" `Quick test_reader_resolution_noalloc_prefix ]);
    ]
