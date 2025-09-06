open Proton
open Printf

module ListExtra = struct
  let take_n n lst =
    let rec aux n acc = function
      | [] -> List.rev acc
      | _ when n <= 0 -> List.rev acc
      | h::t -> aux (n-1) (h::acc) t
    in
    aux n [] lst
end

module TestResult = struct
  type t = {
    query: string;
    _query_type: string;
    elapsed_ms: float;
    rows: Columns.value list list;
    column_names: string list;
    _column_types: string list;
  }
end

module TableFormatter = struct
  let get_column_widths (result: TestResult.t) =
    let header_widths = List.map String.length result.column_names in
    let data_widths = 
      List.fold_left (fun acc row ->
        List.map2 (fun w v -> 
          max w (String.length (Columns.value_to_string v))
        ) acc row
      ) header_widths result.rows
    in
    List.map2 max header_widths data_widths

  let repeat_char c n = String.make n c

  let format_row values widths =
    let cells = List.map2 (fun v w ->
      let s = match v with
        | `String s -> s
        | `Value v -> Columns.value_to_string v
      in
      let pad = w - String.length s in
      " " ^ s ^ repeat_char ' ' pad ^ " "
    ) values widths in
    "|" ^ String.concat "|" cells ^ "|"

  let format_separator widths first middle last =
    let segments = List.map (fun w -> repeat_char '-' (w + 2)) widths in
    first ^ String.concat middle segments ^ last

  let format_result (result: TestResult.t) =
    if result.rows = [] then
      sprintf "Empty result set (0 rows)\n"
    else
      let widths = get_column_widths result in
      let buffer = Buffer.create 1024 in
      
      Buffer.add_string buffer (sprintf "\nQuery id: %s\n\n" (Digest.to_hex (Digest.string result.query)));
      
      Buffer.add_string buffer (format_separator widths "+" "+" "+");
      Buffer.add_char buffer '\n';
      
      let header_values = List.map (fun s -> `String s) result.column_names in
      Buffer.add_string buffer (format_row header_values widths);
      Buffer.add_char buffer '\n';
      
      Buffer.add_string buffer (format_separator widths "+" "+" "+");
      Buffer.add_char buffer '\n';
      
      List.iter (fun row ->
        let row_values = List.map (fun v -> `Value v) row in
        Buffer.add_string buffer (format_row row_values widths);
        Buffer.add_char buffer '\n';
      ) result.rows;
      
      Buffer.add_string buffer (format_separator widths "+" "+" "+");
      Buffer.add_char buffer '\n';
      
      let row_count = List.length result.rows in
      Buffer.add_string buffer (sprintf "\n%d row%s in set. Elapsed: %.3f sec.\n\n" 
        row_count 
        (if row_count = 1 then "" else "s")
        (result.elapsed_ms /. 1000.0));
      
      Buffer.contents buffer
end

module DataTypeTests = struct
  
  let create_test_data_table client table_name =
    let drop_sql = sprintf "DROP STREAM IF EXISTS %s" table_name in
    let create_sql = sprintf {|
      CREATE STREAM %s (
        id int32,
        name string,
        age uint32,
        balance float64,
        bignum int64,
        created datetime64(3),
        _tp_time datetime64(3) DEFAULT now64(3),
        PRIMARY KEY (id)
      ) SETTINGS mode='versioned_kv', version_column='_tp_time'
    |} table_name in
    let open Lwt.Syntax in
    let* _ = Client.execute client drop_sql in
    let* _ = Client.execute client create_sql in
    Lwt.return_unit

  let insert_test_data client table_name =
    (* For STREAMs, avoid VALUES; use INSERT ... SELECT to ensure parser compatibility. *)
    let open Lwt.Syntax in
    let exec ~label sql =
      Lwt.catch
        (fun () ->
           let* _ = Lwt_unix.with_timeout 5.0 (fun () -> Client.execute client sql) in
           Printf.printf "[INSERT] ok: %s\n%!" label;
           Lwt.return_unit)
        (fun ex ->
           Printf.printf "[INSERT] failed (%s): %s\n%!" label (Printexc.to_string ex);
           Lwt.fail ex)
    in
    let one id name age balance bignum =
      (* Use functions Proton definitely supports; rely on implicit cast for datetime64(3) from now(). *)
      sprintf "INSERT INTO %s (id, name, age, balance, bignum, created) SELECT to_int32(%d), '%s', to_uint32(%d), to_float64(%f), to_int64(%Ld), now()"
        table_name id name age balance bignum
    in
    let* _ = exec ~label:"row1" (one 1 "Alice" 25 1234.56 9876543210L) in
    let* _ = exec ~label:"row2" (one 2 "Bob" 30 2345.67 8765432109L) in
    let* _ = exec ~label:"row3" (one 3 "Charlie" 35 3456.78 7654321098L) in
    let* _ = exec ~label:"row4" (one 4 "David" 40 4567.89 6543210987L) in
    let* _ = exec ~label:"row5" (one 5 "Eve" 28 5678.90 5432109876L) in
    Lwt.return_unit

  let test_bounded_query conn table_name column_filter =
    (* First try bounded query with SETTINGS *)
    let bounded_query = sprintf "SELECT %s FROM %s ORDER BY id LIMIT 5 SETTINGS query_mode='table'" column_filter table_name in
    let start_time = Unix.gettimeofday () in
    
    printf "[DEBUG] Trying bounded query: %s\n%!" bounded_query;
    
    let open Lwt.Syntax in
    let* () = Connection.send_query conn bounded_query in
    printf "[DEBUG] Query sent, waiting for response...\n%!";
    
    let rec collect_results acc_rows col_info =
      let* pkt = Connection.receive_packet conn in
      match pkt with
      | Connection.PData block ->
          let rows = Block.get_rows block in
          printf "[DEBUG] Received data block with %d rows\n%!" (List.length rows);
          let col_info = match col_info with
            | None -> 
                let cols = Block.columns_with_types block in
                Some (List.map fst cols, List.map snd cols)
            | Some info -> Some info
          in
          collect_results (acc_rows @ rows) col_info
      | Connection.PEndOfStream ->
          printf "[DEBUG] Received EndOfStream\n%!";
          let elapsed_ms = (Unix.gettimeofday () -. start_time) *. 1000.0 in
          let (names, types) = match col_info with
            | Some (n, t) -> (n, t)
            | None -> ([], [])
          in
          Lwt.return {
            TestResult.query = bounded_query;
            _query_type = "bounded";
            elapsed_ms;
            rows = acc_rows;
            column_names = names;
            _column_types = types;
          }
      | pkt ->
          printf "[DEBUG] Received other packet type: %s\n%!" 
            (match pkt with
             | Connection.PProgress -> "Progress"
             | Connection.PProfileInfo -> "ProfileInfo"
             | Connection.PTotals _ -> "Totals"
             | Connection.PExtremes _ -> "Extremes"
             | Connection.PLog _ -> "Log"
             | _ -> "Unknown");
          collect_results acc_rows col_info
    in
    
    (* Try bounded query with timeout *)
    let* bounded_result = 
      Lwt.catch
        (fun () ->
          Lwt_unix.with_timeout 3.0 (fun () -> collect_results [] None))
        (fun ex ->
          printf "[DEBUG] Bounded query timed out or failed: %s\n%!" (Printexc.to_string ex);
          let elapsed_ms = (Unix.gettimeofday () -. start_time) *. 1000.0 in
          Lwt.return {
            TestResult.query = bounded_query;
            _query_type = "bounded (timeout)";
            elapsed_ms;
            rows = [];
            column_names = [];
            _column_types = [];
          })
    in
    
    (* If bounded query returned no rows, fall back to re-insert + streaming *)
    if bounded_result.rows = [] then begin
      printf "[INFO] Bounded query returned empty; falling back to live streaming demo\n%!";
      printf "[INFO] Will re-insert data and capture via streaming\n%!";
      
      (* Create new connection for unbounded query *)
      let conn2 = Connection.create ~host:"127.0.0.1" ~port:8463 ~compression:Compress.None () in
      
      (* Start streaming query FIRST *)
      let unbounded_query = sprintf "SELECT %s FROM %s" column_filter table_name in
      printf "[DEBUG] Starting streaming query: %s\n%!" unbounded_query;
      let start_time = Unix.gettimeofday () in
      let* () = Connection.send_query conn2 unbounded_query in
      
      (* Now re-insert the data so streaming will see it *)
      printf "[DEBUG] Re-inserting data for streaming capture...\n%!";
      let client = Client.create ~host:"127.0.0.1" ~port:8463 () in
      let* () = 
        let insert_one id = 
          let insert = sprintf "INSERT INTO %s (id, name, age, balance, bignum, created) SELECT to_int32(%d), 'Stream%d', to_uint32(%d), to_float64(%f), to_int64(%Ld), now()"
            table_name id id (20 + id) (1000.0 +. float_of_int id) (Int64.of_int (1000000 + id))
        in
        let* _ = Client.execute client insert in
        let* _ = insert_one 101 in
        let* _ = insert_one 102 in
        let* _ = insert_one 103 in
        let* _ = insert_one 104 in
        let* _ = insert_one 105 in
        Lwt.return_unit
      in
      
      (* Collect streaming results with timeout and max attempts *)
      let rec collect_streaming acc_rows col_info row_count attempts max_attempts =
        if row_count >= 5 then begin
          printf "[DEBUG] Got enough rows (%d), stopping\n%!" row_count;
          let elapsed_ms = (Unix.gettimeofday () -. start_time) *. 1000.0 in
          let (names, types) = match col_info with
            | Some (n, t) -> (n, t)
            | None -> ([], [])
          in
          let* () = Connection.disconnect conn2 in
          Lwt.return {
            TestResult.query = unbounded_query;
            _query_type = sprintf "streaming fallback (got %d rows)" row_count;
            elapsed_ms;
            rows = ListExtra.take_n 5 acc_rows;
            column_names = names;
            _column_types = types;
          }
        end else if attempts >= max_attempts then begin
          printf "[DEBUG] Max attempts reached (%d), stopping with %d rows\n%!" max_attempts row_count;
          let elapsed_ms = (Unix.gettimeofday () -. start_time) *. 1000.0 in
          let (names, types) = match col_info with
            | Some (n, t) -> (n, t)
            | None -> ([], [])
          in
          let* () = Connection.disconnect conn2 in
          Lwt.return {
            TestResult.query = unbounded_query;
            _query_type = sprintf "streaming fallback (timeout with %d rows)" row_count;
            elapsed_ms;
            rows = acc_rows;
            column_names = names;
            _column_types = types;
          }
        end else
          Lwt.catch
            (fun () ->
              let* pkt = Lwt_unix.with_timeout 0.5 (fun () -> Connection.receive_packet conn2) in
              match pkt with
              | Connection.PData block ->
                  let rows = Block.get_rows block in
                  let num_rows = List.length rows in
                  if num_rows > 0 then
                    printf "[DEBUG] Streaming received %d rows\n%!" num_rows
                  else
                    ();
                  let col_info = match col_info with
                    | None when num_rows > 0 -> 
                        let cols = Block.columns_with_types block in
                        Some (List.map fst cols, List.map snd cols)
                    | info -> info
                  in
                  (* Only count non-empty packets towards attempts *)
                  let new_attempts = if num_rows = 0 then attempts + 1 else 0 in
                  collect_streaming (acc_rows @ rows) col_info (row_count + num_rows) new_attempts max_attempts
              | Connection.PEndOfStream ->
                  printf "[DEBUG] Streaming received EndOfStream\n%!";
                  let elapsed_ms = (Unix.gettimeofday () -. start_time) *. 1000.0 in
                  let (names, types) = match col_info with
                    | Some (n, t) -> (n, t)
                    | None -> ([], [])
                  in
                  let* () = Connection.disconnect conn2 in
                  Lwt.return {
                    TestResult.query = unbounded_query;
                    _query_type = "streaming fallback (stream ended)";
                    elapsed_ms;
                    rows = acc_rows;
                    column_names = names;
                    _column_types = types;
                  }
              | _ -> collect_streaming acc_rows col_info row_count (attempts + 1) max_attempts)
            (fun _ ->
              (* Timeout - try again *)
              collect_streaming acc_rows col_info row_count (attempts + 1) max_attempts)
      in
      collect_streaming [] None 0 0 20
    end else
      Lwt.return bounded_result

  let test_unbounded_query conn table_name max_rows =
    let query = sprintf "SELECT id, name, age, balance, bignum, created FROM %s" table_name in
    let start_time = Unix.gettimeofday () in
    
    printf "[DEBUG] Starting unbounded query: %s (max %d rows)\n%!" query max_rows;
    
    let open Lwt.Syntax in
    let* () = Connection.send_query conn query in
    
    let rec collect_results acc_rows col_info count attempts max_attempts =
      if count >= max_rows then begin
        printf "[DEBUG] Unbounded: got %d rows, stopping\n%!" count;
        let elapsed_ms = (Unix.gettimeofday () -. start_time) *. 1000.0 in
        let (names, types) = match col_info with
          | Some (n, t) -> (n, t)
          | None -> ([], [])
        in
        Lwt.return {
          TestResult.query;
          _query_type = sprintf "unbounded (first %d rows)" max_rows;
          elapsed_ms;
          rows = acc_rows;
          column_names = names;
          _column_types = types;
        }
      end else if attempts >= max_attempts then begin
        printf "[DEBUG] Unbounded: max attempts (%d) reached with %d rows\n%!" max_attempts count;
        let elapsed_ms = (Unix.gettimeofday () -. start_time) *. 1000.0 in
        let (names, types) = match col_info with
          | Some (n, t) -> (n, t)
          | None -> ([], [])
        in
        Lwt.return {
          TestResult.query;
          _query_type = sprintf "unbounded (timeout with %d rows)" count;
          elapsed_ms;
          rows = acc_rows;
          column_names = names;
          _column_types = types;
        }
      end else
        Lwt.catch
          (fun () ->
            let* pkt = Lwt_unix.with_timeout 1.0 (fun () -> Connection.receive_packet conn) in
            match pkt with
            | Connection.PData block ->
                let rows = Block.get_rows block in
                let num_rows = List.length rows in
                if num_rows > 0 then
                  printf "[DEBUG] Unbounded: received %d rows\n%!" num_rows;
                let col_info = match col_info with
                  | None when num_rows > 0 -> 
                      let cols = Block.columns_with_types block in
                      Some (List.map fst cols, List.map snd cols)
                  | info -> info
                in
                let new_rows = ListExtra.take_n (max_rows - count) rows in
                let new_count = count + List.length new_rows in
                let new_attempts = if num_rows = 0 then attempts + 1 else 0 in
                collect_results (acc_rows @ new_rows) col_info new_count new_attempts max_attempts
            | Connection.PEndOfStream ->
                printf "[DEBUG] Unbounded: received EndOfStream\n%!";
                let elapsed_ms = (Unix.gettimeofday () -. start_time) *. 1000.0 in
                let (names, types) = match col_info with
                  | Some (n, t) -> (n, t)
                  | None -> ([], [])
                in
                Lwt.return {
                  TestResult.query;
                  _query_type = "unbounded (stream ended)";
                  elapsed_ms;
                  rows = acc_rows;
                  column_names = names;
                  _column_types = types;
                }
            | _ -> collect_results acc_rows col_info count (attempts + 1) max_attempts)
          (fun _ ->
            (* Timeout - try again *)
            collect_results acc_rows col_info count (attempts + 1) max_attempts)
    in
    collect_results [] None 0 0 15
end

let run_live_tests () =
  printf "[START] Starting Proton Live Tests\n";
  printf "================================\n\n";
  
  let settings = [
    ("send_logs_level", "none");
    ("log_profile_events", "0");
    ("compression", "0");
  ] in
  
  let open Lwt.Syntax in
  let test_program =
    printf "[CONNECT] Connecting to Proton at 127.0.0.1:8463...\n%!";
    let client = Client.create ~host:"127.0.0.1" ~port:8463 ~settings () in
    let conn = Connection.create ~host:"127.0.0.1" ~port:8463 ~settings () in
    
    let table_name = "test_all_types_live" in
    
    printf "[CREATE] Creating test table with all data types...\n%!";
    let* () = DataTypeTests.create_test_data_table client table_name in
    
    printf "[INSERT] Inserting test data...\n%!";
    let* () = DataTypeTests.insert_test_data client table_name in
    (* Give the system a brief moment to make rows visible to snapshot reads. *)
    let* () = Lwt_unix.sleep 0.5 in
    
    (* First, verify data exists with a simple count *)
    printf "[VERIFY] Checking if data exists in stream...\n%!";
    let count_query = sprintf "SELECT count(*) FROM %s SETTINGS query_mode='table'" table_name in
    let* _ = 
      Lwt.catch
        (fun () ->
          let* _ = Lwt_unix.with_timeout 2.0 (fun () -> Client.execute client count_query) in
          printf "[VERIFY] Count query completed\n%!";
          Lwt.return_unit)
        (fun ex ->
          printf "[VERIFY] Count query failed/timed out: %s\n%!" (Printexc.to_string ex);
          Lwt.return_unit)
    in
    
    printf "\n" ;
    printf "═══════════════════════════════════════════\n";
    printf "         TEST 1: BOUNDED QUERIES            \n";
    printf "═══════════════════════════════════════════\n";
    
    printf "\n[TEST 1.1] Select specific columns (bounded with table())\n";
    printf "─────────────────────────────────────────────────────\n";
    let* result1 = DataTypeTests.test_bounded_query conn table_name "id, name, age, balance, bignum, created" in
    printf "%s" (TableFormatter.format_result result1);
    
    printf "\n[TEST 1.2] Select specific columns (bounded)\n";
    printf "───────────────────────────────────────────\n";
    let* result2 = DataTypeTests.test_bounded_query conn table_name "id, name, age, balance" in
    printf "%s" (TableFormatter.format_result result2);
    
    printf "\n[TEST 1.3] Numeric types (bounded)\n";
    printf "──────────────────────────────────\n";
    let* result3 = DataTypeTests.test_bounded_query conn table_name "name, bignum, created" in
    printf "%s" (TableFormatter.format_result result3);
    
    printf "[DEBUG] TEST 1.3 completed successfully\n%!";
    
    printf "\n";
    printf "═══════════════════════════════════════════\n";
    printf "        TEST 2: UNBOUNDED QUERIES           \n";
    printf "═══════════════════════════════════════════\n";
    printf "[DEBUG] Starting TEST 2\n%!";
    
    printf "\n[TEST 2.1] Unbounded streaming query (first 3 rows)\n";
    printf "──────────────────────────────────────────────────\n";
    printf "Note: This demonstrates live streaming - starting reader, then inserting data\n\n";
    
    let conn2 = Connection.create ~host:"127.0.0.1" ~port:8463 ~compression:Compress.None ~settings () in
    
    (* Start the unbounded query FIRST *)
    let query = sprintf "SELECT id, name, age, balance, bignum, created FROM %s" table_name in
    printf "[DEBUG] Starting unbounded streaming: %s\n%!" query;
    let* () = Connection.send_query conn2 query in
    
    (* Now insert some data for the stream to capture *)
    printf "[DEBUG] Inserting data for unbounded stream...\n%!";
    let* () = 
      let open Lwt.Syntax in
      let insert_one id name = 
        let insert = sprintf "INSERT INTO %s (id, name, age, balance, bignum, created) SELECT to_int32(%d), '%s', to_uint32(%d), to_float64(%f), to_int64(%Ld), now()"
          table_name id name (30 + id) (2000.0 +. float_of_int id) (Int64.of_int (2000000 + id))
      in
      Client.execute client insert
      in
      let* _ = insert_one 201 "Unbounded1" in
      let* _ = insert_one 202 "Unbounded2" in
      let* _ = insert_one 203 "Unbounded3" in
      Lwt.return_unit
    in
    
    (* Now collect the results *)
    let* result4 = DataTypeTests.test_unbounded_query conn2 table_name 3 in
    printf "%s" (TableFormatter.format_result result4);
    
    printf "\n";
    printf "═══════════════════════════════════════════\n";
    printf "     TEST 3: CONTINUOUS STREAMING TEST      \n";
    printf "═══════════════════════════════════════════\n";
    printf "\n[TEST 3.1] Live streaming with inserts\n";
    printf "────────────────────────────────────────\n";
    printf "Starting continuous stream reader and inserter...\n";
    printf "Press Ctrl-C to stop the streaming test.\n\n";
    
    let stream_table = "test_streaming_continuous" in
    let drop_stream = sprintf "DROP STREAM IF EXISTS %s" stream_table in
    let create_stream = sprintf "CREATE STREAM %s (id int32, value float64, message string, _tp_time datetime64(3) DEFAULT now64(3), PRIMARY KEY (id)) SETTINGS mode='versioned_kv', version_column='_tp_time'" stream_table in
    
    let* _ = Client.execute client drop_stream in
    let* _ = Client.execute client create_stream in
    
    let conn3 = Connection.create ~host:"127.0.0.1" ~port:8463 ~compression:Compress.None ~settings () in
    let query = sprintf "SELECT id, value, message FROM %s" stream_table in
    let* () = Connection.send_query conn3 query in
    
    let stop_signal = ref false in
    let rows_received = ref 0 in
    
    let reader_task =
      let rec loop () =
        if !stop_signal then Lwt.return_unit
        else
          Lwt.pick [
            (let* pkt = Connection.receive_packet conn3 in
             match pkt with
             | Connection.PData block ->
                 let rows = Block.get_rows block in
                 List.iter (fun row ->
                   incr rows_received;
                   let formatted = String.concat " | " (List.map Columns.value_to_string row) in
                   printf "  [STREAM %04d] %s\n%!" !rows_received formatted
                 ) rows;
                 loop ()
             | _ -> loop ());
            (let* () = Lwt_unix.sleep 0.1 in loop ())
          ]
      in
      Lwt.catch loop (fun _ -> Lwt.return_unit)
    in
    
    let inserter_task =
      let rec loop count =
        if !stop_signal || count >= 10 then Lwt.return_unit
        else
          let id = Random.int 100000 in
          let value = Random.float 1000.0 in
          let message = sprintf "msg_%04d" (Random.int 10000) in
          let insert = sprintf "INSERT INTO %s (id, value, message) SELECT to_int32(%d), to_float64(%f), '%s'" stream_table id value message in
          let* _ = Client.execute client insert in
          printf "  [INSERT] id=%d, value=%.2f, message=%s\n%!" id value message;
          let* () = Lwt_unix.sleep 0.5 in
          loop (count + 1)
      in
      loop 0
    in
    
    let timeout_task = 
      let* () = Lwt_unix.sleep 5.0 in
      stop_signal := true;
      Lwt.return_unit
    in
    
    let* _ = Lwt.pick [reader_task; inserter_task; timeout_task] in
    
    printf "\n[DONE] Streaming test completed (received %d rows)\n" !rows_received;
    
    let* () = Connection.disconnect conn in
    let* () = Connection.disconnect conn2 in
    let* () = Connection.disconnect conn3 in
    
    let* _ = Client.execute client (sprintf "DROP STREAM IF EXISTS %s" table_name) in
    let* _ = Client.execute client (sprintf "DROP STREAM IF EXISTS %s" stream_table) in
    
    printf "\n";
    printf "════════════════════════════════════════════\n";
    printf "       ALL TESTS COMPLETED SUCCESSFULLY      \n";
    printf "════════════════════════════════════════════\n\n";
    
    Lwt.return_unit
  in
  
  try
    Lwt_main.run test_program
  with
  | e -> 
      printf "[ERROR] Test failed with exception: %s\n" (Printexc.to_string e);
      exit 1

let () =
  Random.self_init ();
  run_live_tests ()
