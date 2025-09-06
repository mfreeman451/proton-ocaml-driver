(* Async Insert Implementation for Proton OCaml Driver *)

open Lwt.Infix
open Columns

(* Configuration for async insert behavior *)
type config = {
  table_name: string;
  max_batch_size: int;        (* Maximum rows per batch *)
  max_batch_bytes: int;       (* Maximum bytes per batch *)
  flush_interval: float;      (* Seconds between automatic flushes *)
  max_retries: int;           (* Maximum retry attempts on failure *)
  retry_delay: float;         (* Initial retry delay in seconds *)
  response_timeout: float;    (* Seconds to wait for server end-of-stream *)
}

let default_config table_name = {
  table_name;
  max_batch_size = 1000;
  max_batch_bytes = 1_048_576; (* 1MB *)
  flush_interval = 5.0;        (* 5 seconds *)
  max_retries = 3;
  retry_delay = 1.0;
  response_timeout = 10.0;
}

(* Async insert buffer state *)
type buffer_state = {
  mutable rows: value list list;
  mutable row_count: int;
  mutable byte_size: int;
  mutable column_names: string list option;
  mutable column_types: string list option;
  mutable last_flush: float;
}

(* Async inserter instance *)
type t = {
  config: config;
  connection: Connection.t;
  buffer: buffer_state;
  mutex: Lwt_mutex.t;
  flush_condition: unit Lwt_condition.t;
  mutable running: bool;
  mutable flush_promise: unit Lwt.t option;
}

(* Estimate byte size of a row *)
let estimate_row_size (row: value list) : int =
  List.fold_left (fun acc value ->
    acc + match value with
    | VNull -> 1
    | VString s -> String.length s + 8
    | VInt32 _ | VUInt32 _ | VFloat64 _ -> 8
    | VInt64 _ | VUInt64 _ -> 8
    | VDateTime _ | VDateTime64 _ -> 16
    | VEnum8 (s, _) -> String.length s + 4
    | VEnum16 (s, _) -> String.length s + 4
    | VArray _ | VMap _ | VTuple _ -> 64 (* Rough estimate for complex types *)
  ) 16 row (* Base overhead per row *)

(* Create a new async inserter *)
let create config connection =
  let buffer = {
    rows = [];
    row_count = 0;
    byte_size = 0;
    column_names = None;
    column_types = None;
    last_flush = Unix.gettimeofday ();
  } in
  {
    config;
    connection;
    buffer;
    mutex = Lwt_mutex.create ();
    flush_condition = Lwt_condition.create ();
    running = false;
    flush_promise = None;
  }

(* Utility: escape single quotes for SQL string literals *)
let escape_sql_string s =
  let buf = Buffer.create (String.length s + 8) in
  String.iter (fun c -> if c = '\'' then Buffer.add_string buf "''" else Buffer.add_char buf c) s;
  Buffer.contents buf

(* Render a value as SQL expression, casting to declared type when needed. *)
let sql_expr_of_value (v:value) (type_spec:string) : string =
  let t = String.lowercase_ascii (String.trim type_spec) in
  match v, t with
  | VInt32 n, _ -> Printf.sprintf "to_int32(%ld)" n
  | VInt64 n, _ -> Printf.sprintf "to_int64(%Ld)" n
  | VUInt32 n, _ -> Printf.sprintf "to_uint32(%ld)" n
  | VUInt64 n, _ -> Printf.sprintf "to_uint64(%Ld)" n
  | VFloat64 f, _ -> Printf.sprintf "to_float64(%.*f)" 6 f
  | VString s, _ -> Printf.sprintf "'%s'" (escape_sql_string s)
  | _ -> failwith "Unsupported value type for SQL insert"

(* Build an INSERT ... VALUES (...), (...) statement from rows. *)
let _build_insert_values_sql table_name (columns: (string * string) list) (rows: value list list) : string =
  let col_names = columns |> List.map fst |> String.concat ", " in
  let render_row row =
    let values =
      List.mapi (fun i v ->
        let (_name, type_spec) = List.nth columns i in
        sql_expr_of_value v type_spec
      ) row
      |> String.concat ", "
    in
    Printf.sprintf "(%s)" values
  in
  let values_clause = rows |> List.map render_row |> String.concat ", " in
  Printf.sprintf "INSERT INTO %s (%s) VALUES %s" table_name col_names values_clause

(* Build an INSERT ... SELECT ... UNION ALL statement from rows for broad compatibility (fallback). *)
let _build_insert_select_sql table_name (columns: (string * string) list) (rows: value list list) : string =
  let col_names = columns |> List.map fst |> String.concat ", " in
  let expr_of (v:value) (type_spec:string) : string =
    let t = String.lowercase_ascii (String.trim type_spec) in
    match v, t with
    | VInt32 n, _ -> Printf.sprintf "to_int32(%ld)" n
    | VInt64 n, _ -> Printf.sprintf "to_int64(%Ld)" n
    | VUInt32 n, _ -> Printf.sprintf "to_uint32(%ld)" n
    | VUInt64 n, _ -> Printf.sprintf "to_uint64(%Ld)" n
    | VFloat64 f, _ -> Printf.sprintf "to_float64(%.*f)" 6 f
    | VString s, _ -> Printf.sprintf "'%s'" (escape_sql_string s)
    | _ -> failwith "Unsupported value type for SQL insert"
  in
  let select_of_row row =
    let exprs =
      List.mapi (fun i v ->
        let (_name, type_spec) = List.nth columns i in
        expr_of v type_spec
      ) row
      |> String.concat ", "
    in
    Printf.sprintf "SELECT %s" exprs
  in
  let union_selects = rows |> List.map select_of_row |> String.concat " UNION ALL " in
  Printf.sprintf "INSERT INTO %s (%s) %s" table_name col_names union_selects

(* Lightweight debug logging for async insert; opt-in via PROTON_INSERT_DEBUG *)
let env_insert_debug () =
  match Sys.getenv_opt "PROTON_INSERT_DEBUG" with
  | Some ("1" | "true" | "TRUE" | "yes" | "YES") -> true
  | _ -> false

let logf fmt =
  if env_insert_debug () then Printf.printf fmt else Printf.ifprintf stdout fmt

let chunk_list lst chunk_size =
  let rec aux acc current n = function
    | [] -> List.rev (if current = [] then acc else (List.rev current)::acc)
    | x::xs ->
        if n = 0 then aux ((List.rev current)::acc) [x] (chunk_size-1) xs
        else aux acc (x::current) (n-1) xs
  in
  aux [] [] chunk_size lst

(* Send a batch to the database using a SQL path (INSERT ... SELECT ...), chunked to avoid overly large statements. *)
let send_batch inserter (rows: value list list) (columns: (string * string) list) =
  let rec retry_send attempt =
    if attempt > inserter.config.max_retries then
      Lwt.fail_with (Printf.sprintf "Failed to insert batch after %d retries" inserter.config.max_retries)
    else
      Lwt.catch
        (fun () ->
          if columns = [] then Lwt.fail_with "Async_insert: columns must be provided for SQL path" else
          let chunk_size = min inserter.config.max_batch_size 500 in
          let chunks = chunk_list rows chunk_size in
          let rec insert_chunks = function
            | [] -> Lwt.return_unit
            | ch::rest ->
                let sql = _build_insert_select_sql inserter.config.table_name columns ch in
                logf "Executing SQL batch insert chunk (%d rows)…\n%!" (List.length ch);
                Connection.send_query inserter.connection sql >>= fun () ->
                let rec drain () =
                  Connection.receive_packet inserter.connection >>= function
                  | PEndOfStream -> Lwt.return_unit
                  | _ -> drain ()
                in
                Lwt_unix.with_timeout inserter.config.response_timeout drain >>= fun () ->
                insert_chunks rest
          in
          insert_chunks chunks
        )
        (fun exn ->
          logf "Insert attempt %d failed: %s\n%!" attempt (Printexc.to_string exn);
          let delay = inserter.config.retry_delay *. (float_of_int attempt) in
          (* Reset connection before retry to clear any partial state *)
          Connection.disconnect inserter.connection >>= fun () ->
          Lwt_unix.sleep delay >>= fun () ->
          retry_send (attempt + 1))
  in
  retry_send 1

(* Flush the current buffer *)
let flush_buffer inserter =
  Lwt_mutex.with_lock inserter.mutex (fun () ->
    if inserter.buffer.row_count = 0 then
      Lwt.return_unit
    else
      let rows = List.rev inserter.buffer.rows in
      let columns = match inserter.buffer.column_names, inserter.buffer.column_types with
        | Some names, Some types -> List.combine names types
        | _ -> [] (* Will be inferred from data *)
      in
      
      (* Reset buffer *)
      inserter.buffer.rows <- [];
      inserter.buffer.row_count <- 0;
      inserter.buffer.byte_size <- 0;
      inserter.buffer.last_flush <- Unix.gettimeofday ();
      
      logf "Flushing batch of %d rows\n%!" (List.length rows);
      send_batch inserter rows columns
  )

(* Check if buffer should be flushed *)
let should_flush inserter =
  let now = Unix.gettimeofday () in
  inserter.buffer.row_count >= inserter.config.max_batch_size ||
  inserter.buffer.byte_size >= inserter.config.max_batch_bytes ||
  (now -. inserter.buffer.last_flush) >= inserter.config.flush_interval

(* Background flush loop *)
let rec flush_loop inserter =
  if not inserter.running then
    Lwt.return_unit
  else
    Lwt_condition.wait inserter.flush_condition >>= fun () ->
    if inserter.running then
      Lwt.catch
        (fun () -> flush_buffer inserter)
        (fun exn -> 
          logf "Flush error: %s\n%!" (Printexc.to_string exn);
          Lwt.return_unit) >>= fun () ->
      flush_loop inserter
    else
      Lwt.return_unit

(* Timer-based flush loop *)
let rec timer_flush_loop inserter =
  if not inserter.running then
    Lwt.return_unit
  else
    Lwt_unix.sleep inserter.config.flush_interval >>= fun () ->
    if inserter.running then
      Lwt_mutex.with_lock inserter.mutex (fun () ->
        if should_flush inserter then
          Lwt_condition.signal inserter.flush_condition ();
        Lwt.return_unit
      ) >>= fun () ->
      timer_flush_loop inserter
    else
      Lwt.return_unit

(* Start the async inserter *)
let start inserter =
  if not inserter.running then (
    inserter.running <- true;
    let flush_task = flush_loop inserter in
    let timer_task = timer_flush_loop inserter in
    inserter.flush_promise <- Some (Lwt.join [flush_task; timer_task]);
    logf "Started async inserter for table %s\n%!" inserter.config.table_name
  )

(* Stop the async inserter *)
let stop inserter =
  if inserter.running then (
    inserter.running <- false;
    Lwt_condition.signal inserter.flush_condition ();
    match inserter.flush_promise with
    | Some promise -> 
      Lwt.catch (fun () -> promise) (fun _ -> Lwt.return_unit) >>= fun () ->
      flush_buffer inserter (* Final flush *)
    | None -> flush_buffer inserter
  ) else
    Lwt.return_unit

(* Add a row to the buffer *)
let add_row ?(columns=[]) inserter (row: value list) =
  Lwt_mutex.with_lock inserter.mutex (fun () ->
    (* Set column info on first row if provided *)
    if inserter.buffer.column_names = None && columns <> [] then (
      let (names, types) = List.split columns in
      inserter.buffer.column_names <- Some names;
      inserter.buffer.column_types <- Some types;
    );
    
    (* Add row to buffer *)
    inserter.buffer.rows <- row :: inserter.buffer.rows;
    inserter.buffer.row_count <- inserter.buffer.row_count + 1;
    inserter.buffer.byte_size <- inserter.buffer.byte_size + estimate_row_size row;
    
    (* Check if we should flush *)
    if should_flush inserter then
      Lwt_condition.signal inserter.flush_condition ();
    
    Lwt.return_unit
  )

(* Add multiple rows *)
let add_rows ?columns inserter rows =
  Lwt_list.iter_s (add_row ?columns inserter) rows

(* Force a flush *)
let flush inserter = flush_buffer inserter

(* Get buffer statistics *)
let get_stats inserter = Lwt_mutex.with_lock inserter.mutex (fun () ->
  Lwt.return (inserter.buffer.row_count, inserter.buffer.byte_size)
)
