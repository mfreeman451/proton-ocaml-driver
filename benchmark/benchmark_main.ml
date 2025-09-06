open Proton
open Printf

let host = "127.0.0.1"
let port = 8463

(* -------- Reader micro-benchmarks (cache effectiveness) -------- *)

let encode_int32_values n : bytes =
  let open Binary in
  let b = Bytes.create (4 * n) in
  for i = 0 to n - 1 do
    let v = Int32.of_int (i land 0x7fffffff) in
    bytes_set_int32_le b (4 * i) v
  done; b

let encode_uint64_values n : bytes =
  let open Binary in
  let b = Bytes.create (8 * n) in
  for i = 0 to n - 1 do
    let v = Int64.of_int i in
    bytes_set_int64_le b (8 * i) v
  done; b

let encode_float64_values n : bytes =
  let open Binary in
  let b = Bytes.create (8 * n) in
  for i = 0 to n - 1 do
    let f = float_of_int i +. 0.125 in
    let bits = Int64.bits_of_float f in
    bytes_set_int64_le b (8 * i) bits
  done; b

let encode_date32_values n : bytes =
  let open Binary in
  let b = Bytes.create (4 * n) in
  for i = 0 to n - 1 do
    (* arbitrary day count *)
    let days = Int32.of_int (i mod (365 * 50)) in
    bytes_set_int32_le b (4 * i) days
  done; b

let encode_array_int32 n ~k : bytes =
  (* Array(Int32): [uint64 offsets]*n then [int32 values]*total *)
  let open Binary in
  let total = n * k in
  let b = Bytes.create (8 * n + 4 * total) in
  (* offsets *)
  for i = 0 to n - 1 do
    let off = Int64.of_int ((i + 1) * k) in
    bytes_set_int64_le b (8 * i) off
  done;
  (* values *)
  let base = 8 * n in
  for i = 0 to total - 1 do
    let v = Int32.of_int (i land 0x7fffffff) in
    bytes_set_int32_le b (base + 4 * i) v
  done; b

let encode_tuple_int32_float64 n : bytes =
  (* Tuple(Int32, Float64): column-wise: int32[n] then float64[n] *)
  let open Binary in
  let b = Bytes.create (4 * n + 8 * n) in
  for i = 0 to n - 1 do
    let v = Int32.of_int (i land 0x7fffffff) in
    bytes_set_int32_le b (4 * i) v
  done;
  let base = 4 * n in
  for i = 0 to n - 1 do
    let f = float_of_int i +. 0.5 in
    let bits = Int64.bits_of_float f in
    bytes_set_int64_le b (base + 8 * i) bits
  done; b

let encode_nullable_int32 n : bytes =
  (* Nullable(Int32): null flags [uint8]*n then values [int32]*n *)
  let open Binary in
  let b = Bytes.create (n + 4 * n) in
  for i = 0 to n - 1 do
    (* all non-null -> flag 0 *)
    Bytes.set b i (Char.chr 0);
  done;
  let base = n in
  for i = 0 to n - 1 do
    let v = Int32.of_int (i land 0x7fffffff) in
    bytes_set_int32_le b (base + 4 * i) v
  done; b

let time f =
  let t0 = Unix.gettimeofday () in
  let x = f () in
  let t1 = Unix.gettimeofday () in
  (x, t1 -. t0)

let bench_reader_for_spec ~spec ~rows ~loops make_bytes =
  (* Prepare data once; new BR per iteration *)
  let payload = make_bytes rows in
  (* Baseline: recompile reader every iteration (no cache) *)
  let (_, cold_s) = time (fun () ->
    for _i = 1 to loops do
      let br = Buffered_reader.create_from_bytes_no_copy payload in
      let reader = Column.compile_reader_br spec in
      ignore (reader br rows)
    done) in
  (* Cached: compile once, then reuse *)
  Column.reset_cache_stats (); Column.clear_reader_caches ();
  let (_, warm_s) = time (fun () ->
    let reader = Column.reader_of_spec_br spec in
    for _i = 1 to loops do
      let br = Buffered_reader.create_from_bytes_no_copy payload in
      ignore (reader br rows)
    done) in
  let stats = Column.get_cache_stats () in
  printf "  %-28s | rows=%5d loops=%4d | cold=%.4fs warm=%.4fs | %s\n%!"
    spec rows loops cold_s warm_s (Column.cache_stats_to_string stats)

let run_reader_micro_benchmarks () =
  printf "\n=== READER MICRO-BENCHMARKS (cache effectiveness) ===\n%!";
  let rows = 512 and loops = 1000 in
  bench_reader_for_spec ~spec:"int32" ~rows ~loops encode_int32_values;
  bench_reader_for_spec ~spec:"uint64" ~rows ~loops encode_uint64_values;
  bench_reader_for_spec ~spec:"float64" ~rows ~loops encode_float64_values;
  bench_reader_for_spec ~spec:"date32" ~rows ~loops encode_date32_values;
  bench_reader_for_spec ~spec:"array(int32)" ~rows ~loops (fun n -> encode_array_int32 n ~k:3);
  bench_reader_for_spec ~spec:"tuple(int32,float64)" ~rows ~loops encode_tuple_int32_float64;
  bench_reader_for_spec ~spec:"nullable(int32)" ~rows ~loops encode_nullable_int32;
  printf "\n%!"

(* Simple timing utilities *)
let time_it_lwt name f =
  let open Lwt.Syntax in
  let start_time = Unix.gettimeofday () in
  let* result = f () in
  let end_time = Unix.gettimeofday () in
  let elapsed = end_time -. start_time in
  printf "[%s] %.3f seconds\n%!" name elapsed;
  Lwt.return (result, elapsed)

(* Generate test data as Column.value lists *)
let generate_batch_data count =
  let rows = Array.init count (fun i ->
    let id = Int32.of_int (i + 1) in
    let name = sprintf "User%d" (i + 1) in
    let value = Random.float 10000.0 in
    [Column.Int32 id; Column.String name; Column.Float64 value]
  ) in
  Array.to_list rows

(* Helper: verify row count with simple retry to handle eventual visibility *)
let verify_count client table_name ~expected =
  let open Lwt.Syntax in
  let rec loop attempts =
    let query = sprintf "SELECT to_uint64(count()) FROM %s SETTINGS query_mode='table'" table_name in
    let* res = Lwt.catch
      (fun () -> Lwt_unix.with_timeout 5.0 (fun () -> Client.execute client query))
      (fun _ -> Lwt.return Client.NoRows)
    in
    let count = match res with
      | Client.Rows (rows, _) -> (match rows with
          | [ [ Column.UInt64 n ] ] -> Int64.to_int n
          | [ [ Column.Int64 n ] ] -> Int64.to_int n
          | [ [ Column.Int32 n ] ] -> Int32.to_int n
          | _ -> -1)
      | _ -> -1
    in
    if count = expected || attempts <= 0 then Lwt.return count
    else (
      (* brief backoff before retrying to accommodate stream semantics *)
      let* () = Lwt_unix.sleep 0.05 in
      loop (attempts - 1)
    )
  in
  loop 10

(* Benchmark: Async batch insert *)
let bench_async_batch_insert count =
  let open Lwt.Syntax in
  let client = Client.create ~host ~port ~compression:Compress.None () in

  let table_name = sprintf "bench_async_%d" (Random.int 100000) in
  
  (* Silent setup to keep output clean *)
  let* () = 
    let drop_sql = sprintf "DROP STREAM IF EXISTS %s" table_name in
    let create_sql = sprintf {|
      CREATE STREAM %s (
        id int32,
        name string,
        value float64
      )
    |} table_name in
    let* _ = Client.execute client drop_sql in
    let* _ = Client.execute client create_sql in
    Lwt.return_unit
  in
  
  let rows = generate_batch_data count in
  let columns = [("id", "int32"); ("name", "string"); ("value", "float64")] in
  
  let* (_, elapsed) = time_it_lwt (sprintf "ASYNC_BATCH_INSERT_%d_ROWS" count) (fun () ->
    (* Use the high-level async batch insert from Client *)
    Client.insert_rows ~columns client table_name rows
  ) in
  
  (* Verify rows actually inserted (poll without forcing table mode) *)
  let* verified = verify_count client table_name ~expected:count in
  let rate = float_of_int count /. elapsed in
  printf "async-batch: %7d rows | %6.2fs | %8.0f rows/s | verified=%b\n%!"
    count elapsed rate (verified = count);
  
  let* _ = Client.execute client (sprintf "DROP STREAM IF EXISTS %s" table_name) in
  Lwt.return rate

(* Benchmark: Manual async insert with custom config *)
let bench_manual_async_insert count batch_size =
  let open Lwt.Syntax in
  let conn = Connection.create ~host ~port ~compression:Compress.None () in

  let table_name = sprintf "bench_manual_%d" (Random.int 100000) in
  
  (* Silent setup to keep output clean *)
  let client = Client.create ~host ~port ~compression:Compress.None () in
  let* () = 
    let drop_sql = sprintf "DROP STREAM IF EXISTS %s" table_name in
    let create_sql = sprintf {|
      CREATE STREAM %s (
        id int32,
        name string,
        value float64
      )
    |} table_name in
    let* _ = Client.execute client drop_sql in
    let* _ = Client.execute client create_sql in
    Lwt.return_unit
  in
  
  let rows = generate_batch_data count in
  let columns = [("id", "int32"); ("name", "string"); ("value", "float64")] in
  
  (* Create custom config for smaller batches *)
  let config = { (Async_insert.default_config table_name) with 
    max_batch_size = batch_size;
    flush_interval = 1.0;  (* Flush every 1 second *)
  } in
  
  let* (_, elapsed) = time_it_lwt (sprintf "MANUAL_ASYNC_%d_BATCH_%d" count batch_size) (fun () ->
    let inserter = Async_insert.create config conn in
    Async_insert.start inserter;
    let* () = Async_insert.add_rows ~columns inserter rows in
    Async_insert.stop inserter
  ) in
  
  (* Verify rows actually inserted (poll) *)
  let* client2 = Lwt.return (Client.create ~host ~port () ) in
  let* verified = verify_count client2 table_name ~expected:count in
  let rate = float_of_int count /. elapsed in
  printf "manual-async: %7d rows (batch=%4d) | %6.2fs | %8.0f rows/s | verified=%b\n%!"
    count batch_size elapsed rate (verified = count);
  
  let* () = Connection.disconnect conn in
  Lwt.return rate

(* Benchmark: Streaming read with live data injection *)
let bench_live_streaming_read count =
  let open Lwt.Syntax in
  (* Use uncompressed connection for inserts until LZ4 write framing is aligned *)
  let client = Client.create ~host ~port ~compression:Compress.None () in
  let table_name = sprintf "bench_stream_%d" (Random.int 100000) in
  
  printf "Setting up streaming table for %d row live read test...\n%!" count;
  (* Setup test table *)
  let* () = 
    let drop_sql = sprintf "DROP STREAM IF EXISTS %s" table_name in
    let create_sql = sprintf {|
      CREATE STREAM %s (
        id int32,
        name string,
        value float64
      )
    |} table_name in
    let* _ = Client.execute client drop_sql in
    let* _ = Client.execute client create_sql in
    Lwt.return_unit
  in
  
  printf "Starting live streaming read (will inject %d rows)...\n%!" count;
  
  (* Start streaming connection FIRST *)
  let conn = Connection.create ~host ~port ~compression:Compress.None () in
  let query = sprintf "SELECT id, name, value FROM %s" table_name in
  let* () = Connection.send_query conn query in
  
  let* (rows_read, elapsed) = time_it_lwt (sprintf "LIVE_STREAM_READ_%d_ROWS" count) (fun () ->
    (* Start data injection in parallel with reading *)
    let inject_data () = 
      let rows = generate_batch_data count in
      let columns = [("id", "int32"); ("name", "string"); ("value", "float64")] in
      printf "  [INJECTOR] Starting to inject %d rows...\n%!" count;
      Client.insert_rows ~columns client table_name rows
    in
    
    (* Start data injection asynchronously *)
    let inject_promise = inject_data () in
    
    (* Read streaming data *)
    let rec read_rows acc_count =
      if acc_count >= count then begin
        printf "  [READER] Got all %d rows!\n%!" count;
        Lwt.return acc_count
      end else
        Lwt.catch
          (fun () ->
            let* pkt = Lwt_unix.with_timeout 2.0 (fun () -> Connection.receive_packet conn) in
            match pkt with
            | Connection.PData block ->
                let rows = Block.get_rows block in
                let row_count = List.length rows in
                if row_count > 0 then
                  printf "  [READER] received batch: %d rows (total: %d/%d)\n%!" row_count (acc_count + row_count) count;
                read_rows (acc_count + row_count)
            | Connection.PEndOfStream ->
                printf "  [READER] stream ended with %d rows\n%!" acc_count;
                Lwt.return acc_count
            | _ -> read_rows acc_count)
          (fun ex ->
            printf "  [READER] timeout or error after %d rows: %s\n%!" acc_count (Printexc.to_string ex);
            Lwt.return acc_count)
    in
    
    (* Wait for both injection and reading *)
    let* final_count = read_rows 0 in
    let* () = inject_promise in
    printf "  [INJECTOR] Completed data injection\n%!";
    Lwt.return final_count
  ) in
  
  let rate = float_of_int rows_read /. elapsed in
  printf "✅ %d rows read at %.0f rows/second (LIVE STREAMING)\n\n%!" rows_read rate;
  
  let* () = Connection.disconnect conn in
  let* _ = Client.execute client (sprintf "DROP STREAM IF EXISTS %s" table_name) in
  Lwt.return rate

let run_insert_benchmarks sizes =
  let open Lwt.Syntax in
  printf "\n=== ASYNC BATCH INSERT BENCHMARKS ===\n\n%!";
  
  let* async_rates = 
    Lwt_list.map_s (fun size ->
      let* rate = bench_async_batch_insert size in
      Lwt.return (size, rate)
    ) sizes
  in
  
  printf "\n=== MANUAL ASYNC INSERT BENCHMARKS (different batch sizes) ===\n\n%!";
  
  (* Test different batch sizes for 10K rows *)
  let test_size = 10000 in
  let batch_sizes = [100; 500; 1000; 5000] in
  let* manual_rates = 
    Lwt_list.map_s (fun batch_size ->
      let* rate = bench_manual_async_insert test_size batch_size in
      Lwt.return (batch_size, rate)
    ) batch_sizes
  in
  
  printf "=== INSERT PERFORMANCE SUMMARY ===\n\n%!";
  printf "Async Batch Insert Performance:\n%!";
  List.iter (fun (size, rate) ->
    printf "  %7d rows: %10.0f rows/sec\n%!" size rate
  ) async_rates;
  
  printf "\nManual Async Insert Performance (10K rows):\n%!";
  List.iter (fun (batch_size, rate) ->
    printf "  batch %4d: %10.0f rows/sec\n%!" batch_size rate
  ) manual_rates;
  
  Lwt.return_unit

let run_streaming_benchmarks sizes =
  let open Lwt.Syntax in
  printf "\n=== LIVE STREAMING READ BENCHMARKS ===\n\n%!";
  
  let* streaming_rates = 
    Lwt_list.map_s (fun size ->
      let* rate = bench_live_streaming_read size in
      Lwt.return (size, rate)
    ) sizes
  in
  
  printf "=== STREAMING READ PERFORMANCE SUMMARY ===\n\n%!";
  printf "Live Streaming Read Performance:\n%!";
  List.iter (fun (size, rate) ->
    printf "  %7d rows: %10.0f rows/sec\n%!" size rate
  ) streaming_rates;
  
  Lwt.return_unit

let test_million_rows () =
  let open Lwt.Syntax in
  printf "\n=== 🚀 1 MILLION ROW CHALLENGE 🚀 ===\n\n%!";
  
  printf "Testing 1M row async batch insert performance...\n%!";
  let* insert_rate = bench_async_batch_insert 1_000_000 in
  
  printf "1M Async Insert Results:\n%!";
  printf "  Rate: %.0f rows/second\n%!" insert_rate;
  printf "  Time: %.2f minutes\n%!" (1_000_000.0 /. insert_rate /. 60.0);
  (* Show cache stats for column readers to validate cache behavior *)
  let stats = Column.get_cache_stats () in
  printf "  Column Reader Cache: %s\n%!" (Column.cache_stats_to_string stats);
  
  Lwt.return_unit

let run_benchmarks () =
  let open Lwt.Syntax in
  Random.self_init ();
  printf "=== 🚀 Proton OCaml Driver ASYNC Performance Benchmarks 🚀 ===\n\n%!";
  (* Run local micro-benchmarks first (no server needed) *)
  run_reader_micro_benchmarks ();
  (* Allow running only the reader micro-benchmarks via env var *)
  let only_reader = match Sys.getenv_opt "ONLY_READER_MICRO" with
    | Some ("1"|"true"|"TRUE"|"yes"|"YES") -> true
    | _ -> false
  in
  if only_reader then (
    printf "\nONLY_READER_MICRO=1 set: skipping network benchmarks.\n%!";
    Lwt.return_unit
  ) else (
  
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
  
  (* Test with various sizes *)
  let test_sizes = [1000; 10000; 50000] in
  let streaming_sizes = [1000; 5000; 10000] in
  
  let* () = run_insert_benchmarks test_sizes in
  let* () = run_streaming_benchmarks streaming_sizes in
  
  (* Million row challenge *)
  let* () = test_million_rows () in
  
  printf "\n🎉 All async benchmarks completed!\n%!";
 
  Lwt.return_unit
  )

let () = Lwt_main.run (run_benchmarks ())
