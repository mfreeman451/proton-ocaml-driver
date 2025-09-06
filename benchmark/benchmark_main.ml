open Proton
open Printf

let host = "127.0.0.1"
let port = 8463

(* Simple timing utilities *)
let time_it_lwt name f =
  let open Lwt.Syntax in
  let start_time = Unix.gettimeofday () in
  let* result = f () in
  let end_time = Unix.gettimeofday () in
  let elapsed = end_time -. start_time in
  printf "[%s] %.3f seconds\n%!" name elapsed;
  Lwt.return (result, elapsed)

(* Generate test data *)
let generate_insert_data count =
  Array.init count (fun i ->
    let id = i + 1 in
    let name = sprintf "User%d" id in
    let value = Random.float 10000.0 in
    (id, name, value)
  )

(* Benchmark: Insert N rows *)
let bench_batch_insert count =
  let open Lwt.Syntax in
  let client = Client.create ~host ~port () in
  let table_name = sprintf "bench_insert_%d" (Random.int 100000) in
  
  printf "Setting up table for %d inserts...\n%!" count;
  let* () = 
    let drop_sql = sprintf "DROP STREAM IF EXISTS %s" table_name in
    let create_sql = sprintf {|
      CREATE STREAM %s (
        id int32,
        name string,
        value float64,
        ts datetime64(3)
      )
    |} table_name in
    let* _ = Client.execute client drop_sql in
    let* _ = Client.execute client create_sql in
    Lwt.return_unit
  in
  
  let data = generate_insert_data count in
  printf "Starting %d inserts...\n%!" count;
  
  let* (_, elapsed) = time_it_lwt (sprintf "INSERT_%d_ROWS" count) (fun () ->
    Lwt_list.iter_s (fun (id, name, value) ->
      let insert_sql = sprintf 
        "INSERT INTO %s (id, name, value, ts) SELECT to_int32(%d), '%s', to_float64(%f), now()"
        table_name id name value
      in
      let* _ = Client.execute client insert_sql in
      Lwt.return_unit
    ) (Array.to_list data)
  ) in
  
  let rate = float_of_int count /. elapsed in
  printf "✅ %d rows inserted at %.0f rows/second\n\n%!" count rate;
  
  let* _ = Client.execute client (sprintf "DROP STREAM IF EXISTS %s" table_name) in
  Lwt.return rate

(* Benchmark: Read N rows using streaming *)
let bench_streaming_read count =
  let open Lwt.Syntax in
  let client = Client.create ~host ~port () in
  let table_name = sprintf "bench_read_%d" (Random.int 100000) in
  
  printf "Setting up table for %d row read test...\n%!" count;
  (* Setup test data *)
  let* () = 
    let drop_sql = sprintf "DROP STREAM IF EXISTS %s" table_name in
    let create_sql = sprintf {|
      CREATE STREAM %s (
        id int32,
        name string,
        value float64,
        ts datetime64(3)
      )
    |} table_name in
    let* _ = Client.execute client drop_sql in
    let* _ = Client.execute client create_sql in
    Lwt.return_unit
  in
  
  (* Insert test data *)
  printf "Inserting %d rows for read test...\n%!" count;
  let data = generate_insert_data count in
  let* () = 
    Lwt_list.iter_s (fun (id, name, value) ->
      let insert_sql = sprintf 
        "INSERT INTO %s (id, name, value, ts) SELECT to_int32(%d), '%s', to_float64(%f), now()"
        table_name id name value
      in
      let* _ = Client.execute client insert_sql in
      Lwt.return_unit
    ) (Array.to_list data)
  in
  
  (* Give data time to be available *)
  let* () = Lwt_unix.sleep 0.1 in
  
  printf "Starting streaming read of %d rows...\n%!" count;
  (* Now benchmark the read via streaming *)
  let conn = Connection.create ~host ~port ~compression:Compress.None () in
  let query = sprintf "SELECT id, name, value, ts FROM %s" table_name in
  
  let* (rows_read, elapsed) = time_it_lwt (sprintf "STREAM_READ_%d_ROWS" count) (fun () ->
    let* () = Connection.send_query conn query in
    
    let rec read_rows acc_count attempts max_attempts =
      if acc_count >= count || attempts >= max_attempts then
        Lwt.return acc_count
      else
        let open Lwt.Infix in
        Lwt.catch
          (fun () ->
            Lwt_unix.with_timeout 1.0 (fun () -> Connection.receive_packet conn) >>= function
            | Connection.PData block ->
                let rows = Block.get_rows block in
                let row_count = List.length rows in
                if row_count > 0 then
                  printf "  received batch: %d rows\n%!" row_count;
                read_rows (acc_count + row_count) 0 max_attempts
            | Connection.PEndOfStream ->
                printf "  stream ended\n%!";
                Lwt.return acc_count
            | _ -> read_rows acc_count (attempts + 1) max_attempts)
          (fun _ -> read_rows acc_count (attempts + 1) max_attempts)
    in
    
    read_rows 0 0 20
  ) in
  
  let rate = float_of_int rows_read /. elapsed in
  printf "✅ %d rows read at %.0f rows/second\n\n%!" rows_read rate;
  
  let* () = Connection.disconnect conn in
  let* _ = Client.execute client (sprintf "DROP STREAM IF EXISTS %s" table_name) in
  Lwt.return rate

let run_performance_tests sizes =
  let open Lwt.Syntax in
  printf "\n=== INSERT PERFORMANCE TESTS ===\n\n%!";
  
  let* insert_rates = 
    Lwt_list.map_s (fun size ->
      let* rate = bench_batch_insert size in
      Lwt.return (size, rate)
    ) sizes
  in
  
  printf "=== READ PERFORMANCE TESTS ===\n\n%!";
  
  let* read_rates = 
    Lwt_list.map_s (fun size ->
      let* rate = bench_streaming_read size in
      Lwt.return (size, rate)
    ) sizes
  in
  
  printf "=== PERFORMANCE SUMMARY ===\n\n%!";
  printf "Insert Performance:\n%!";
  List.iter (fun (size, rate) ->
    printf "  %6d rows: %8.0f rows/sec\n%!" size rate
  ) insert_rates;
  
  printf "\nRead Performance:\n%!";
  List.iter (fun (size, rate) ->
    printf "  %6d rows: %8.0f rows/sec\n%!" size rate
  ) read_rates;
  
  Lwt.return_unit

let test_million_rows () =
  let open Lwt.Syntax in
  printf "\n=== 1 MILLION ROW CHALLENGE ===\n\n%!";
  
  printf "Testing 1M row insert performance...\n%!";
  let* insert_rate = bench_batch_insert 1_000_000 in
  
  printf "1M Insert Results:\n%!";
  printf "  Rate: %.0f rows/second\n%!" insert_rate;
  printf "  Time: %.2f minutes\n%!" (1_000_000.0 /. insert_rate /. 60.0);
  
  Lwt.return_unit

let run_benchmarks () =
  let open Lwt.Syntax in
  Random.self_init ();
  printf "=== 🚀 Proton OCaml Driver Performance Benchmarks 🚀 ===\n\n%!";
  
  printf "Testing connection to %s:%d...\n%!" host port;
  let* () = 
    Lwt.catch (fun () ->
      let client = Client.create ~host ~port () in
      let* _ = Client.execute client "SELECT 1" in
      printf "✅ Connection successful!\n\n%!";
      Lwt.return_unit
    ) (fun e -> 
      printf "❌ Connection failed: %s\n%!" (Printexc.to_string e);
      printf "Make sure Proton is running on %s:%d\n\n%!" host port;
      exit 1
    )
  in
  
  (* Test with progressively larger datasets *)
  let test_sizes = [100; 1000; 10000; 50000] in
  let* () = run_performance_tests test_sizes in
  
  (* Million row challenge *)
  let* () = test_million_rows () in
  
  printf "\n🎉 All benchmarks completed!\n%!";
  Lwt.return_unit

let () = Lwt_main.run (run_benchmarks ())