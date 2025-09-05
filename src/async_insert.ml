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
}

let default_config table_name = {
  table_name;
  max_batch_size = 1000;
  max_batch_bytes = 1_048_576; (* 1MB *)
  flush_interval = 5.0;        (* 5 seconds *)
  max_retries = 3;
  retry_delay = 1.0;
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

(* Send a batch to the database *)
let send_batch inserter (rows: value list list) (columns: (string * string) list) =
  let rec retry_send attempt =
    if attempt > inserter.config.max_retries then
      Lwt.fail_with (Printf.sprintf "Failed to insert batch after %d retries" inserter.config.max_retries)
    else
      let insert_query = Printf.sprintf "INSERT INTO %s VALUES" inserter.config.table_name in
      Lwt.catch
        (fun () ->
          (* For now, we'll convert to a simple INSERT query *)
          (* In a real implementation, we'd use the Data packet protocol *)
          let values_str = List.map (fun row ->
            "(" ^ String.concat "," (List.map (fun v ->
              match v with
              | VNull -> "NULL"
              | VString s -> "'" ^ String.escaped s ^ "'"
              | VInt32 i -> Int32.to_string i
              | VUInt32 i -> Int32.to_string i
              | VInt64 i -> Int64.to_string i
              | VUInt64 i -> Int64.to_string i
              | VFloat64 f -> string_of_float f
              | VDateTime (ts, _) -> Int64.to_string ts
              | VDateTime64 (value, _, _) -> Int64.to_string value
              | VEnum8 (name, _) -> "'" ^ String.escaped name ^ "'"
              | VEnum16 (name, _) -> "'" ^ String.escaped name ^ "'"
              | VArray _ -> "'[]'"  (* Simplified for now *)
              | VMap _ -> "'{}'"    (* Simplified for now *)
              | VTuple _ -> "'()'"  (* Simplified for now *)
            ) row) ^ ")"
          ) rows in
          let full_query = insert_query ^ " " ^ String.concat "," values_str in
          Connection.send_query inserter.connection full_query >>= fun () ->
          (* Read response packets until end of stream *)
          let rec read_response () =
            Connection.receive_packet inserter.connection >>= function
            | PEndOfStream -> Lwt.return_unit
            | _ -> read_response ()
          in
          read_response ())
        (fun exn ->
          Printf.printf "Insert attempt %d failed: %s\n" attempt (Printexc.to_string exn);
          let delay = inserter.config.retry_delay *. (float_of_int attempt) in
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
      
      Printf.printf "Flushing batch of %d rows\n" (List.length rows);
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
          Printf.printf "Flush error: %s\n" (Printexc.to_string exn);
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
    Printf.printf "Started async inserter for table %s\n" inserter.config.table_name
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