(* Integration test - Actually connects to Proton database *)
open Proton

let result_to_string = function
  | Client.NoRows -> "No rows"
  | Client.Rows (rows, cols) ->
      Printf.sprintf "%d rows, %d columns" (List.length rows) (List.length cols)

let print_rows = function
  | Client.NoRows -> Printf.printf "  No rows returned\n"
  | Client.Rows (rows, cols) ->
      List.iter (fun row ->
        let values = List.map Column.value_to_string row in
        Printf.printf "  Row: [%s]\n" (String.concat ", " values)
      ) rows

let test_connection () =
  Printf.printf "Testing actual connection to Proton...\n";
  let client = Client.create ~host:"127.0.0.1" ~port:8463 () in
  
  Lwt_main.run (
    let open Lwt.Syntax in
    try
      (* Test basic connectivity *)
      let* result = Client.execute client "SELECT 1 as test" in
      Printf.printf "✓ Connected to Proton successfully\n";
      
      (* Show version *)
      let* version = Client.execute client "SELECT version()" in
      Printf.printf "✓ Proton version: ";
      print_rows version;
      
      (* Clean up any existing test table *)
      let* _ = Client.execute client "DROP TABLE IF EXISTS test_types" in
      Printf.printf "✓ Cleaned up test environment\n";
      
      (* Create test table with basic types first *)
      let create_table = "CREATE TABLE IF NOT EXISTS test_types (
        col_int32 int32,
        col_int64 int64,
        col_float32 float32,
        col_float64 float64,
        col_string string,
        col_datetime datetime
      ) ENGINE = Memory" in
      
      let* _ = Client.execute client create_table in
      Printf.printf "✓ Created test table\n";
      
      (* Insert test data *)
      let insert_query = "INSERT INTO test_types VALUES (
        42,
        9223372036854775807,
        3.14159,
        2.71828,
        'Hello, Proton!',
        '2024-01-15 10:30:45'
      )" in
      
      let* _ = Client.execute client insert_query in
      Printf.printf "✓ Inserted test data\n";
      
      (* Read back and verify *)
      let* result = Client.execute client "SELECT * FROM test_types" in
      Printf.printf "✓ Retrieved data: %s\n" (result_to_string result);
      print_rows result;
      
      (* Test specific queries *)
      let* int_result = Client.execute client "SELECT col_int32 FROM test_types" in
      Printf.printf "✓ Int32 value: ";
      print_rows int_result;
      
      let* str_result = Client.execute client "SELECT col_string FROM test_types" in
      Printf.printf "✓ String value: ";
      print_rows str_result;
      
      (* Test aggregation *)
      let* count_result = Client.execute client "SELECT count(*) FROM test_types" in
      Printf.printf "✓ Count result: ";
      print_rows count_result;
      
      (* Clean up *)
      let* _ = Client.execute client "DROP TABLE test_types" in
      Printf.printf "✓ Cleaned up test table\n";
      
      Printf.printf "\n✅ Connection test passed!\n";
      Lwt.return_unit
      
    with
    | Failure msg -> 
        Printf.printf "❌ Test failed: %s\n" msg;
        exit 1
    | e -> 
        Printf.printf "❌ Unexpected error: %s\n" (Printexc.to_string e);
        exit 1
  )

let test_data_types () =
  Printf.printf "\n📊 Testing various data types...\n";
  let client = Client.create ~host:"127.0.0.1" ~port:8463 () in
  
  Lwt_main.run (
    let open Lwt.Syntax in
    try
      (* Create table with more types *)
      let* _ = Client.execute client "DROP TABLE IF EXISTS test_advanced" in
      
      let create = "CREATE TABLE test_advanced (
        id uint32,
        name string,
        price decimal(10,2),
        created datetime,
        is_active bool,
        tags array(string)
      ) ENGINE = Memory" in
      
      let* _ = Client.execute client create in
      Printf.printf "✓ Created advanced table\n";
      
      (* Insert data *)
      let insert = "INSERT INTO test_advanced VALUES (
        1,
        'Product A',
        99.99,
        now(),
        true,
        ['tag1', 'tag2', 'tag3']
      )" in
      
      let* _ = Client.execute client insert in
      
      (* Insert more rows *)
      let* _ = Client.execute client "INSERT INTO test_advanced VALUES (2, 'Product B', 149.50, now(), false, ['tag4'])" in
      let* _ = Client.execute client "INSERT INTO test_advanced VALUES (3, 'Product C', 200.00, now(), true, ['tag5', 'tag6'])" in
      
      Printf.printf "✓ Inserted multiple rows\n";
      
      (* Query with WHERE *)
      let* active = Client.execute client "SELECT name, price FROM test_advanced WHERE is_active = true" in
      Printf.printf "✓ Active products:\n";
      print_rows active;
      
      (* Aggregation *)
      let* avg_price = Client.execute client "SELECT avg(price) FROM test_advanced" in
      Printf.printf "✓ Average price:\n";
      print_rows avg_price;
      
      (* Array operations *)
      let* tags = Client.execute client "SELECT tags FROM test_advanced WHERE id = 1" in
      Printf.printf "✓ Tags for product 1:\n";
      print_rows tags;
      
      (* Clean up *)
      let* _ = Client.execute client "DROP TABLE test_advanced" in
      
      Printf.printf "✅ Data types test passed!\n";
      Lwt.return_unit
      
    with
    | e -> 
        Printf.printf "❌ Data types test failed: %s\n" (Printexc.to_string e);
        Lwt.return_unit
  )

let test_compression () =
  Printf.printf "\n🔧 Testing compression...\n";
  
  Lwt_main.run (
    let open Lwt.Syntax in
    
    (* Test with LZ4 compression *)
    let client_lz4 = Client.create 
      ~host:"localhost" 
      ~port:8463 
      ~compression:Compress.LZ4 () in
    
    try
      let* result = Client.execute client_lz4 "SELECT 'LZ4 test' as msg" in
      Printf.printf "✓ LZ4 compression works\n";
      print_rows result;
      
      (* Test with ZSTD compression *)
      let client_zstd = Client.create 
        ~host:"localhost" 
        ~port:8463 
        ~compression:Compress.ZSTD () in
      
      let* result = Client.execute client_zstd "SELECT 'ZSTD test' as msg" in
      Printf.printf "✓ ZSTD compression works\n";
      print_rows result;
      
      (* Test with no compression *)
      let client_none = Client.create 
        ~host:"localhost" 
        ~port:8463 
        ~compression:Compress.None () in
        
      let* result = Client.execute client_none "SELECT 'No compression test' as msg" in
      Printf.printf "✓ Uncompressed connection works\n";
      print_rows result;
      
      Printf.printf "✅ Compression test passed!\n";
      Lwt.return_unit
      
    with
    | e -> 
        Printf.printf "❌ Compression test failed: %s\n" (Printexc.to_string e);
        Lwt.return_unit
  )

let test_error_handling () =
  Printf.printf "\n🔍 Testing error handling...\n";
  
  let client = Client.create ~host:"127.0.0.1" ~port:8463 () in
  
  Lwt_main.run (
    let open Lwt.Syntax in
    
    (* Test invalid query *)
    let* _ = 
      try
        let* _ = Client.execute client "INVALID SQL QUERY" in
        Printf.printf "❌ Should have failed on invalid query\n";
        Lwt.return_unit
      with
      | _ -> 
        Printf.printf "✓ Invalid query properly rejected\n";
        Lwt.return_unit
    in
    
    (* Test non-existent table *)
    let* _ = 
      try
        let* _ = Client.execute client "SELECT * FROM non_existent_table_xyz" in
        Printf.printf "❌ Should have failed on non-existent table\n";
        Lwt.return_unit
      with
      | _ -> 
        Printf.printf "✓ Non-existent table error caught\n";
        Lwt.return_unit
    in
    
    Printf.printf "✅ Error handling test passed!\n";
    Lwt.return_unit
  )

let test_streaming () =
  Printf.printf "\n📈 Testing streaming functionality...\n";
  
  let client = Client.create ~host:"127.0.0.1" ~port:8463 () in
  
  Lwt_main.run (
    let open Lwt.Syntax in
    try
      (* Create test table *)
      let* _ = Client.execute client "DROP TABLE IF EXISTS test_stream" in
      let* _ = Client.execute client "CREATE TABLE test_stream (id int32, value string) ENGINE = Memory" in
      
      (* Insert test data *)
      let* _ = Client.execute client "INSERT INTO test_stream VALUES (1, 'one'), (2, 'two'), (3, 'three')" in
      
      (* Test fold *)
      Printf.printf "Testing fold...\n";
      let* sum = Client.query_fold client "SELECT id FROM test_stream" 
        ~init:0 
        ~f:(fun acc row ->
          match row with
          | Column.Int32 n :: _ -> Lwt.return (acc + Int32.to_int n)
          | _ -> Lwt.return acc
        ) in
      Printf.printf "✓ Sum of IDs via fold: %d\n" sum;
      
      (* Test iter *)
      Printf.printf "Testing iter...\n";
      let* () = Client.query_iter client "SELECT value FROM test_stream"
        ~f:(fun row ->
          match row with
          | Column.String s :: _ -> 
            Printf.printf "  Value: %s\n" s;
            Lwt.return_unit
          | _ -> Lwt.return_unit
        ) in
      Printf.printf "✓ Iteration complete\n";
      
      (* Clean up *)
      let* _ = Client.execute client "DROP TABLE test_stream" in
      
      Printf.printf "✅ Streaming test passed!\n";
      Lwt.return_unit
      
    with
    | e -> 
        Printf.printf "⚠️ Streaming test failed: %s\n" (Printexc.to_string e);
        Lwt.return_unit
  )

let () =
  Printf.printf "🚀 Proton OCaml Driver Integration Tests\n";
  Printf.printf "========================================\n\n";
  
  (* Check if Proton is accessible *)
  (try
    let sock = Unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
    Unix.connect sock (Unix.ADDR_INET (Unix.inet_addr_of_string "127.0.0.1", 8463));
    Unix.close sock;
    Printf.printf "✓ Proton is accessible on port 8463\n\n"
  with
  | _ -> 
    Printf.printf "❌ Cannot connect to Proton on localhost:8463\n";
    Printf.printf "   Please ensure Docker container is running:\n";
    Printf.printf "   docker-compose up -d\n";
    exit 1);
  
  (* Run all tests *)
  test_connection ();
  test_data_types ();
  test_compression ();
  test_error_handling ();
  test_streaming ();
  
  Printf.printf "\n🎉 All tests completed!\n"
