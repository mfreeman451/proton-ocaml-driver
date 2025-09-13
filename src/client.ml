open Connection
open Block
open Lwt.Syntax

type query_result = NoRows | Rows of (Column.value list list * (string * string) list)
type t = { conn : Connection.t }

let create ?host ?port ?database ?user ?password ?tls_config ?compression ?(settings = []) () =
  let conn =
    Connection.create ?host ?port ?database ?user ?password ?tls_config ?compression ~settings ()
  in
  { conn }

let disconnect c = Connection.disconnect c.conn

let execute c (query : string) : query_result Lwt.t =
  let* () = Connection.send_query c.conn query in
  let cols_header = ref None in
  let rows_acc : Column.value list list ref = ref [] in
  let debug =
    match Sys.getenv_opt "PROTON_DEBUG" with
    | Some ("1" | "true" | "TRUE" | "yes" | "YES") -> true
    | _ -> false
  in
  let rec loop () =
    let* pkt = Connection.receive_packet c.conn in
    match pkt with
    | PEndOfStream -> Lwt.return_unit
    | PData b ->
        if b.n_rows = 0 then (
          if !cols_header = None then (
            let cols = Block.columns_with_types b in
            if debug then (
              List.iter
                (fun (n, t) -> Printf.printf "[proton] header col name='%s' type='%s'\n" n t)
                cols;
              flush stdout);
            cols_header := Some cols);
          loop ())
        else
          let rows = Block.get_rows b in
          rows_acc := List.rev_append rows !rows_acc;
          loop ()
    | PTotals b ->
        let rows = Block.get_rows b in
        rows_acc := List.rev_append rows !rows_acc;
        loop ()
    | PExtremes _ -> loop ()
    | PLog _ -> loop ()
    | PProgress | PProfileInfo -> loop ()
  in
  let+ () = loop () in
  match (!rows_acc, !cols_header) with
  | [], _ -> NoRows
  | rows, Some columns -> Rows (List.rev rows, columns)
  | rows, None ->
      let columns =
        match rows with
        | r :: _ -> List.mapi (fun i _ -> (Printf.sprintf "c%d" (i + 1), "")) r
        | [] -> []
      in
      Rows (List.rev rows, columns)

(* Streaming query functionality *)

(* Idiomatic OCaml streaming interface *)

type 'a streaming_result = { rows : 'a; columns : (string * string) list }

(* Core streaming implementation - processes packets and calls handler for each row *)
let stream_query_rows c query ~on_row ~on_columns =
  let* () = Connection.send_query c.conn query in
  let columns_ref = ref [] in
  let rec loop () =
    let* pkt = Connection.receive_packet c.conn in
    match pkt with
    | PEndOfStream -> Lwt.return_unit
    | PData b ->
        let* () =
          if b.n_rows = 0 then
            (* Header block with column definitions *)
            let new_columns = Block.columns_with_types b in
            if new_columns <> [] then (
              columns_ref := new_columns;
              on_columns new_columns)
            else Lwt.return_unit
          else Lwt.return_unit
        in
        let* () =
          if b.n_rows > 0 then
            (* Data block with actual rows *)
            let rows = Block.get_rows b in
            Lwt_list.iter_s on_row rows
          else Lwt.return_unit
        in
        loop ()
    | PTotals b when b.n_rows > 0 ->
        let rows = Block.get_rows b in
        let* () = Lwt_list.iter_s on_row rows in
        loop ()
    | PExtremes _ | PLog _ | PProgress | PProfileInfo | PTotals _ ->
        loop () (* Skip metadata packets *)
  in
  let+ () = loop () in
  !columns_ref

let query_fold c query ~init ~f =
  let acc_ref = ref init in
  let+ _ =
    stream_query_rows c query
      ~on_row:(fun row ->
        let* new_acc = f !acc_ref row in
        acc_ref := new_acc;
        Lwt.return_unit)
      ~on_columns:(fun _ -> Lwt.return_unit)
  in
  !acc_ref

let query_iter c query ~f =
  let+ _ = stream_query_rows c query ~on_row:f ~on_columns:(fun _ -> Lwt.return_unit) in
  ()

let query_collect c query =
  let+ acc = query_fold c query ~init:[] ~f:(fun acc row -> Lwt.return (row :: acc)) in
  List.rev acc

let query_to_seq c query =
  (* This is tricky to implement properly with Lwt - for now return a simpler version *)
  let+ rows = query_collect c query in
  List.to_seq rows

let query_fold_with_columns c query ~init ~f =
  let acc_ref = ref init in
  let columns_ref = ref [] in
  let+ final_columns =
    stream_query_rows c query
      ~on_row:(fun row ->
        let* new_acc = f !acc_ref row !columns_ref in
        acc_ref := new_acc;
        Lwt.return_unit)
      ~on_columns:(fun cols ->
        columns_ref := cols;
        Lwt.return_unit)
  in
  { rows = !acc_ref; columns = final_columns }

let query_iter_with_columns c query ~f =
  let columns_ref = ref [] in
  let+ final_columns =
    stream_query_rows c query
      ~on_row:(fun row -> f row !columns_ref)
      ~on_columns:(fun cols ->
        columns_ref := cols;
        Lwt.return_unit)
  in
  final_columns

(* Async insert functionality *)
let create_async_inserter ?(config = None) c table_name =
  let final_config =
    match config with None -> Async_insert.default_config table_name | Some cfg -> cfg
  in
  Async_insert.create final_config c.conn

let insert_rows c table_name ?(columns = []) rows =
  let inserter = create_async_inserter c table_name in
  Async_insert.start inserter;
  let* () = Async_insert.add_rows ~columns inserter rows in
  Async_insert.stop inserter

let insert_row c table_name ?(columns = []) row = insert_rows c table_name ~columns [ row ]
