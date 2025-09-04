open Connection
open Block
open Columns

type query_result =
  | NoRows
  | Rows of (Columns.value list list * (string * string) list)

type t = { conn : Connection.t }

let create ?host ?port ?database ?user ?password ?tls_config ?compression ?(settings=[]) () =
  let conn = Connection.create ?host ?port ?database ?user ?password ?tls_config ?compression ~settings () in
  { conn }

let disconnect c = Connection.disconnect c.conn

let execute c (query:string) : query_result =
  Connection.send_query c.conn query;
  let cols_header = ref None in
  let rows_acc : Columns.value list list ref = ref [] in
  let rec loop () =
    match Connection.receive_packet c.conn with
    | PEndOfStream -> ()
    | PData b ->
        if b.n_rows = 0 then
          if !cols_header = None then cols_header := Some (Block.columns_with_types b)
          else ()
        else
          let rows = Block.get_rows b in
          rows_acc := !rows_acc @ rows;
        loop ()
    | PTotals b ->
        (* You can optionally store totals; for phase 1 ignore or append *)
        let rows = Block.get_rows b in
        rows_acc := !rows_acc @ rows; loop ()
    | PExtremes _ -> loop ()
    | PLog _ -> loop ()
    | PProgress | PProfileInfo -> loop ()
  in
  loop ();
  match !rows_acc, !cols_header with
  | [], _ -> NoRows
  | rows, Some columns -> Rows (rows, columns)
  | rows, None -> (* some servers may not send 0-rows header first *)
      let columns =
        match rows with
        | r::_ -> List.mapi (fun i _ -> (Printf.sprintf "c%d" (i+1), "")) r
        | [] -> []
      in
      Rows (rows, columns)
