open Connection
open Block
open Columns
open Lwt.Infix

type query_result =
  | NoRows
  | Rows of (Columns.value list list * (string * string) list)

type t = { conn : Connection.t }

let create ?host ?port ?database ?user ?password ?tls_config ?compression ?(settings=[]) () =
  let conn = Connection.create ?host ?port ?database ?user ?password ?tls_config ?compression ~settings () in
  { conn }

let disconnect c = Connection.disconnect c.conn

let execute c (query:string) : query_result Lwt.t =
  Connection.send_query c.conn query >>= fun () ->
  let cols_header = ref None in
  let rows_acc : Columns.value list list ref = ref [] in
  let rec loop () =
    Connection.receive_packet c.conn >>= function
    | PEndOfStream -> Lwt.return_unit
    | PData b ->
        if b.n_rows = 0 then
          (if !cols_header = None then cols_header := Some (Block.columns_with_types b); loop ())
        else
          let rows = Block.get_rows b in
          rows_acc := !rows_acc @ rows; loop ()
    | PTotals b ->
        let rows = Block.get_rows b in
        rows_acc := !rows_acc @ rows; loop ()
    | PExtremes _ -> loop ()
    | PLog _ -> loop ()
    | PProgress | PProfileInfo -> loop ()
  in
  loop () >|= fun () ->
  match !rows_acc, !cols_header with
  | [], _ -> NoRows
  | rows, Some columns -> Rows (rows, columns)
  | rows, None ->
      let columns =
        match rows with
        | r::_ -> List.mapi (fun i _ -> (Printf.sprintf "c%d" (i+1), "")) r
        | [] -> []
      in
      Rows (rows, columns)

(* Streaming query functionality *)

(* Streaming rows type similar to Go driver *)
type rows = {
  mutable current_block: Block.t option;
  mutable current_row: int;
  mutable columns_info: (string * string) list;
  connection: Connection.t;
  mutable finished: bool;
  mutable error: exn option;
}

(* Go-driver style streaming interface *)
let query_stream c (query:string) : rows Lwt.t =
  Connection.send_query c.conn query >>= fun () ->
  let rec get_first_block () =
    Connection.receive_packet c.conn >>= function
    | PEndOfStream -> 
        Lwt.return {
          current_block = None;
          current_row = 0;
          columns_info = [];
          connection = c.conn;
          finished = true;
          error = None;
        }
    | PData b ->
        if b.n_rows = 0 then
          (* Header block with column definitions *)
          let columns = Block.columns_with_types b in
          get_first_block () >|= fun next_rows ->
          { next_rows with columns_info = columns }
        else
          (* First data block *)
          let columns = Block.columns_with_types b in
          Lwt.return {
            current_block = Some b;
            current_row = 0;
            columns_info = columns;
            connection = c.conn;
            finished = false;
            error = None;
          }
    | PTotals b ->
        (* Handle totals block *)
        let columns = Block.columns_with_types b in
        Lwt.return {
          current_block = Some b;
          current_row = 0;
          columns_info = columns;
          connection = c.conn;
          finished = false;
          error = None;
        }
    | PExtremes _ | PLog _ | PProgress | PProfileInfo -> 
        get_first_block () (* Skip metadata packets *)
  in
  get_first_block ()

let next_row (rows: rows) : bool Lwt.t =
  if rows.finished || rows.error <> None then
    Lwt.return false
  else
    match rows.current_block with
    | None -> Lwt.return false
    | Some block ->
        if rows.current_row >= block.n_rows then
          (* Need to get next block *)
          let rec get_next_block () =
            Lwt.catch
              (fun () ->
                Connection.receive_packet rows.connection >>= function
                | PEndOfStream -> 
                    rows.finished <- true;
                    Lwt.return false
                | PData b when b.n_rows = 0 ->
                    (* Update column info from header block *)
                    let new_columns = Block.columns_with_types b in
                    if new_columns <> [] then rows.columns_info <- new_columns;
                    get_next_block ()
                | PData b ->
                    rows.current_block <- Some b;
                    rows.current_row <- 0;
                    Lwt.return true
                | PTotals b when b.n_rows > 0 ->
                    rows.current_block <- Some b;
                    rows.current_row <- 0;
                    Lwt.return true
                | PExtremes _ | PLog _ | PProgress | PProfileInfo | PTotals _ -> 
                    get_next_block () (* Skip metadata packets *))
              (fun exn ->
                rows.error <- Some exn;
                Lwt.return false)
          in
          get_next_block ()
        else
          (* More rows in current block *)
          Lwt.return true

let scan_row (rows: rows) : Columns.value list Lwt.t =
  match rows.current_block with
  | None -> Lwt.fail (Failure "No current block to scan")
  | Some block ->
      if rows.current_row >= block.n_rows then
        Lwt.fail (Failure "Current row index out of bounds")
      else
        let block_rows = Block.get_rows block in
        let row = List.nth block_rows rows.current_row in
        rows.current_row <- rows.current_row + 1;
        Lwt.return row

let close_rows (rows: rows) : unit Lwt.t =
  (* Drain any remaining packets *)
  let rec drain () =
    if rows.finished then
      Lwt.return_unit
    else
      Lwt.catch
        (fun () ->
          Connection.receive_packet rows.connection >>= function
          | PEndOfStream -> 
              rows.finished <- true;
              Lwt.return_unit
          | _ -> drain ())
        (fun exn ->
          rows.error <- Some exn;
          Lwt.return_unit)
  in
  drain ()

let columns (rows: rows) : (string * string) list =
  rows.columns_info

(* Async insert functionality *)
let create_async_inserter ?(config=None) c table_name =
  let final_config = match config with
    | None -> Async_insert.default_config table_name
    | Some cfg -> cfg
  in
  Async_insert.create final_config c.conn

let insert_rows c table_name ?(columns=[]) rows =
  let inserter = create_async_inserter c table_name in
  Async_insert.start inserter;
  Async_insert.add_rows ~columns inserter rows >>= fun () ->
  Async_insert.stop inserter

let insert_row c table_name ?(columns=[]) row =
  insert_rows c table_name ~columns [row]
