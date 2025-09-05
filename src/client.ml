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
