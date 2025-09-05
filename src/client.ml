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

(* Idiomatic OCaml streaming interface *)

type 'a streaming_result = {
  rows: 'a;
  columns: (string * string) list;
}

(* Core streaming implementation - processes packets and calls handler for each row *)
let stream_query_rows c query ~on_row ~on_columns =
  Connection.send_query c.conn query >>= fun () ->
  let columns_ref = ref [] in
  let rec loop () =
    Connection.receive_packet c.conn >>= function
    | PEndOfStream -> Lwt.return_unit
    | PData b ->
        (if b.n_rows = 0 then (
          (* Header block with column definitions *)
          let new_columns = Block.columns_with_types b in
          if new_columns <> [] then (
            columns_ref := new_columns;
            on_columns new_columns
          ) else Lwt.return_unit
        ) else Lwt.return_unit) >>= fun () ->
        (if b.n_rows > 0 then (
          (* Data block with actual rows *)
          let rows = Block.get_rows b in
          Lwt_list.iter_s on_row rows
        ) else Lwt.return_unit) >>= fun () -> 
        loop ()
    | PTotals b when b.n_rows > 0 ->
        let rows = Block.get_rows b in
        Lwt_list.iter_s on_row rows >>= fun () ->
        loop ()
    | PExtremes _ | PLog _ | PProgress | PProfileInfo | PTotals _ -> 
        loop () (* Skip metadata packets *)
  in
  loop () >|= fun () -> !columns_ref

let query_fold c query ~init ~f =
  let acc_ref = ref init in
  stream_query_rows c query 
    ~on_row:(fun row -> 
      f !acc_ref row >>= fun new_acc ->
      acc_ref := new_acc;
      Lwt.return_unit)
    ~on_columns:(fun _ -> Lwt.return_unit)
  >|= fun _ -> !acc_ref

let query_iter c query ~f =
  stream_query_rows c query
    ~on_row:f
    ~on_columns:(fun _ -> Lwt.return_unit)
  >|= fun _ -> ()

let query_collect c query =
  query_fold c query ~init:[] ~f:(fun acc row -> Lwt.return (row :: acc))
  >|= List.rev

let query_to_seq c query =
  (* This is tricky to implement properly with Lwt - for now return a simpler version *)
  query_collect c query >|= List.to_seq

let query_fold_with_columns c query ~init ~f =
  let acc_ref = ref init in
  let columns_ref = ref [] in
  stream_query_rows c query 
    ~on_row:(fun row -> 
      f !acc_ref row !columns_ref >>= fun new_acc ->
      acc_ref := new_acc;
      Lwt.return_unit)
    ~on_columns:(fun cols -> 
      columns_ref := cols;
      Lwt.return_unit)
  >|= fun final_columns -> 
  { rows = !acc_ref; columns = final_columns }

let query_iter_with_columns c query ~f =
  let columns_ref = ref [] in
  stream_query_rows c query
    ~on_row:(fun row -> f row !columns_ref)
    ~on_columns:(fun cols -> 
      columns_ref := cols;
      Lwt.return_unit)
  >|= fun final_columns -> final_columns

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
