(* file: async_insert.ml *)
open Lwt.Syntax
open Column

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
  max_batch_size = 100_000;    (* Increased default batch size for native protocol *)
  max_batch_bytes = 16_777_216; (* 16MB *)
  flush_interval = 1.0;        (* 1 second *)
  max_retries = 3;
  retry_delay = 1.0;
  response_timeout = 60.0;     (* Increased timeout for larger batches *)
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
    | Null -> 1
    | String s -> String.length s + 8
    | Int32 _ | UInt32 _ -> 4
    | Int64 _ | UInt64 _ | Float64 _ -> 8
    | DateTime _ -> 4
    | DateTime64 _ -> 8
    | Enum8 _ -> 1
    | Enum16 _ -> 2
    | Array _ | Map _ | Tuple _ -> 64 (* Rough estimate for complex types *)
  ) 0 row

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

(* Lightweight debug logging for async insert; opt-in via PROTON_INSERT_DEBUG *)
let env_insert_debug () =
  match Sys.getenv_opt "PROTON_INSERT_DEBUG" with
  | Some ("1" | "true" | "TRUE" | "yes" | "YES") -> true
  | _ -> false

let logf fmt =
  if env_insert_debug () then Printf.printf fmt else Printf.ifprintf stdout fmt

(* Sends a batch using the high-performance native protocol. *)
let chunk_rows rows chunk_size =
  let rec loop acc cur n = function
    | [] -> List.rev (if cur = [] then acc else (List.rev cur)::acc)
    | x::xs -> if n = 0 then loop ((List.rev (x::cur))::acc) [] (chunk_size-1) xs else loop acc (x::cur) (n-1) xs
  in
  if chunk_size <= 0 then [rows] else loop [] [] (chunk_size-1) rows

let send_native_batch inserter (rows: value list list) (columns: (string * string) list) =
  let n_rows = List.length rows in
  if n_rows = 0 then Lwt.return_unit
  else
    let rec retry_send attempt =
      if attempt > inserter.config.max_retries then
        Lwt.fail_with (Printf.sprintf "Failed to insert batch after %d retries" inserter.config.max_retries)
      else
        Lwt.catch
          (fun () ->
            let (names, _types) = List.split columns in
            (* Use Native format for binary block streaming over the native protocol. *)
            let insert_sql = Printf.sprintf "INSERT INTO %s (%s) FORMAT Native" inserter.config.table_name (String.concat ", " names) in

            (* 1. Send the INSERT query, but don't signal end of data. *)
            let* () = Connection.send_query_without_data_end inserter.connection insert_sql in

            (* 2. Wait for server-provided header block (schema). *)
            let rec await_header () =
              let* pkt = Connection.receive_packet inserter.connection in
              match pkt with
              | PData b when b.n_rows = 0 && b.columns <> [] -> Lwt.return_unit
              | PProgress | PProfileInfo | PTotals _ | PExtremes _ | PLog _ -> await_header ()
              | PData _ -> await_header ()
              | PEndOfStream -> Lwt.fail_with "Unexpected EndOfStream before data header"
            in
            let* () = await_header () in

            (* Transpose rows -> columns in O(n·m) *)
            let transpose_rows (rows : value list list) : value array array =
              match rows with
              | [] -> [||]
              | r0 :: _ ->
                  let cols = List.length r0 in
                  let n = List.length rows in
                  let out = Array.init cols (fun _ -> Array.make n Null) in
                  let rec fill r_idx = function
                    | [] -> ()
                    | row :: tl ->
                        let rec fill_row c_idx = function
                          | [] -> ()
                          | v :: vs -> out.(c_idx).(r_idx) <- v; fill_row (c_idx+1) vs
                        in
                        fill_row 0 row; fill (r_idx+1) tl
                  in
                  fill 0 rows; out
            in

            (* 3. Send data blocks (chunked if needed). *)
            let rows_per_block = max 1 (min inserter.config.max_batch_size 100_000) in
            let chunks = chunk_rows rows rows_per_block in
            let send_one_chunk ch =
              let cols_arrays = transpose_rows ch in
              let block_columns =
                List.mapi (fun i (name, type_spec) ->
                  { Block.name = name; type_spec; data = cols_arrays.(i) }
                ) columns
              in
              let n = if Array.length cols_arrays = 0 then 0 else Array.length cols_arrays.(0) in
              let data_block = { Block.info = Block_info.{ is_overflows=false; bucket_num = -1 }; columns = block_columns; n_rows = n } in
              Connection.send_data_block inserter.connection data_block
            in
            let* () = Lwt_list.iter_s send_one_chunk chunks in

            (* 4. Send an empty block to signify the end of the insert stream. *)
            let empty_block = { Block.info = Block_info.{ is_overflows=false; bucket_num = -1 }; columns = []; n_rows = 0 } in
            let* () = Connection.send_data_block inserter.connection empty_block in

            (* 5. Wait for the server to acknowledge completion. *)
            let rec drain () =
              let* pkt = Connection.receive_packet inserter.connection in
              match pkt with
              | PEndOfStream -> Lwt.return_unit
              | _ -> drain ()
            in
            Lwt_unix.with_timeout inserter.config.response_timeout drain
          )
          (fun exn ->
            logf "Native insert attempt %d failed: %s\n%!" attempt (Printexc.to_string exn);
            let delay = inserter.config.retry_delay *. (2. ** float_of_int (attempt - 1)) in
            (* Reset connection before retry to clear any partial state *)
            let* () = Connection.disconnect inserter.connection in
            let* () = Lwt_unix.sleep delay in
            retry_send (attempt + 1)
          )
    in
    retry_send 1

(* Flush the current buffer *)
let flush_buffer inserter =
  Lwt_mutex.with_lock inserter.mutex (fun () ->
    if inserter.buffer.row_count = 0 then
      Lwt.return_unit
    else
      let rows_to_send = List.rev inserter.buffer.rows in
      let columns_spec = match inserter.buffer.column_names, inserter.buffer.column_types with
        | Some names, Some types -> List.combine names types
        | _ -> failwith "Column names and types must be set before flushing"
      in
      
      (* Reset buffer before sending *)
      inserter.buffer.rows <- [];
      inserter.buffer.row_count <- 0;
      inserter.buffer.byte_size <- 0;
      inserter.buffer.last_flush <- Unix.gettimeofday ();
      
      logf "Flushing native batch of %d rows\n%!" (List.length rows_to_send);
      
      (* Use the new native batch sending function *)
      send_native_batch inserter rows_to_send columns_spec
  )

(* Check if buffer should be flushed *)
let should_flush inserter =
  let now = Unix.gettimeofday () in
  (inserter.buffer.row_count > 0 && (
    inserter.buffer.row_count >= inserter.config.max_batch_size ||
    inserter.buffer.byte_size >= inserter.config.max_batch_bytes ||
    (now -. inserter.buffer.last_flush) >= inserter.config.flush_interval
  ))

(* Background flush loop *)
let rec flush_loop inserter =
  if not inserter.running then
    Lwt.return_unit
  else
    let* () = Lwt_condition.wait inserter.flush_condition in
    if inserter.running then
      let* () =
        Lwt.catch
          (fun () -> flush_buffer inserter)
          (fun exn ->
            logf "Flush error: %s\n%!" (Printexc.to_string exn);
            Lwt.return_unit)
      in
      flush_loop inserter
    else
      Lwt.return_unit

(* Timer-based flush loop *)
let rec timer_flush_loop inserter =
  if not inserter.running then
    Lwt.return_unit
  else
    let* () = Lwt_unix.sleep inserter.config.flush_interval in
    if inserter.running then
      let* () =
        Lwt_mutex.with_lock inserter.mutex (fun () ->
            if should_flush inserter then Lwt_condition.signal inserter.flush_condition ();
            Lwt.return_unit)
      in
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
    let* () =
      match inserter.flush_promise with
      | Some promise ->
          Lwt.cancel promise;
          let* _ = Lwt.catch (fun () -> promise) (fun _ -> Lwt.return_unit) in
          Lwt.return_unit
      | None -> Lwt.return_unit
    in
    flush_buffer inserter (* Final flush *)
  ) else
    Lwt.return_unit

(* Add a row to the buffer *)
let add_row ?(columns=[]) inserter (row: value list) =
  Lwt_mutex.with_lock inserter.mutex (fun () ->
      let* () =
        if inserter.buffer.column_names = None then (
          if columns = [] then Lwt.fail_with "Column names/types must be provided with the first row"
          else (
            let (names, types) = List.split columns in
            inserter.buffer.column_names <- Some names;
            inserter.buffer.column_types <- Some types;
            Lwt.return_unit))
        else Lwt.return_unit
      in
      inserter.buffer.rows <- row :: inserter.buffer.rows;
      inserter.buffer.row_count <- inserter.buffer.row_count + 1;
      inserter.buffer.byte_size <- inserter.buffer.byte_size + estimate_row_size row;
      if should_flush inserter then Lwt_condition.signal inserter.flush_condition ();
      Lwt.return_unit)

(* Add multiple rows *)
let add_rows ?columns inserter rows =
  Lwt_list.iter_s (add_row ?columns inserter) rows

(* Force a flush *)
let flush inserter = flush_buffer inserter

(* Get buffer statistics *)
let get_stats inserter = Lwt_mutex.with_lock inserter.mutex (fun () ->
  Lwt.return (inserter.buffer.row_count, inserter.buffer.byte_size)
)
