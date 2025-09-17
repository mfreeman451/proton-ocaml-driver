open Connection
open Block
open Lwt.Syntax

type query_result = NoRows | Rows of (Column.value list list * (string * string) list)
type t = { conn : Connection.t }

module StringSet = Set.Make (String)
module StringMap = Map.Make (String)

type statement_part = Text of string | Placeholder of string

type prepared_statement = {
  template : string;
  parts : statement_part list;
  placeholders : StringSet.t;
}

let escape_string_literal (s : string) =
  let buf = Buffer.create (String.length s) in
  String.iter
    (function
      | '\'' -> Buffer.add_string buf "''"
      | '\\' -> Buffer.add_string buf "\\\\"
      | '\n' -> Buffer.add_string buf "\\n"
      | '\r' -> Buffer.add_string buf "\\r"
      | '\t' -> Buffer.add_string buf "\\t"
      | c -> Buffer.add_char buf c)
    s;
  Buffer.contents buf

let rec value_to_literal (value : Column.value) : string =
  match value with
  | Column.Null -> "NULL"
  | Column.String s -> Printf.sprintf "'%s'" (escape_string_literal s)
  | Column.Int32 n -> Int32.to_string n
  | Column.Int64 n -> Int64.to_string n
  | Column.UInt32 n -> Int32.to_string n
  | Column.UInt64 n -> Printf.sprintf "%Lu" n
  | Column.Float64 f -> (
      match classify_float f with
      | FP_normal | FP_subnormal | FP_zero -> Printf.sprintf "%.17g" f
      | FP_infinite -> if f > 0. then "inf" else "-inf"
      | FP_nan -> "nan")
  | Column.DateTime (ts, tz) -> (
      match tz with
      | Some zone -> Printf.sprintf "toDateTime(%Ld, '%s')" ts (escape_string_literal zone)
      | None -> Printf.sprintf "toDateTime(%Ld)" ts)
  | Column.DateTime64 (ts, precision, tz) -> (
      match tz with
      | Some zone ->
          Printf.sprintf "toDateTime64(%Ld, %d, '%s')" ts precision (escape_string_literal zone)
      | None -> Printf.sprintf "toDateTime64(%Ld, %d)" ts precision)
  | Column.Enum8 (name, _) | Column.Enum16 (name, _) ->
      Printf.sprintf "'%s'" (escape_string_literal name)
  | Column.Array values ->
      let items = values |> Array.to_list |> List.map value_to_literal |> String.concat ", " in
      Printf.sprintf "[%s]" items
  | Column.Map pairs ->
      let args =
        List.fold_right (fun (k, v) acc -> value_to_literal k :: value_to_literal v :: acc) pairs []
      in
      Printf.sprintf "map(%s)" (String.concat ", " args)
  | Column.Tuple values ->
      let parts = List.map value_to_literal values |> String.concat ", " in
      Printf.sprintf "(%s)" parts

let parse_prepared_statement (query : string) : prepared_statement =
  let len = String.length query in
  let rec find_open idx =
    if idx > len - 2 then None
    else if query.[idx] = '{' && query.[idx + 1] = '{' then Some idx
    else find_open (idx + 1)
  in
  let rec find_close idx =
    if idx > len - 2 then invalid_arg "Client.prepare: unmatched '{{' in query"
    else if query.[idx] = '}' && query.[idx + 1] = '}' then idx
    else find_close (idx + 1)
  in
  let rec loop acc placeholders last_idx search_idx =
    match find_open search_idx with
    | None ->
        let trailing = String.sub query last_idx (len - last_idx) in
        let acc = if trailing = "" then acc else Text trailing :: acc in
        { template = query; parts = List.rev acc; placeholders }
    | Some open_idx ->
        let text_prefix = String.sub query last_idx (open_idx - last_idx) in
        let acc = if text_prefix = "" then acc else Text text_prefix :: acc in
        let close_idx = find_close (open_idx + 2) in
        let raw_name = String.sub query (open_idx + 2) (close_idx - open_idx - 2) in
        let name = String.trim raw_name in
        if String.equal name "" then invalid_arg "Client.prepare: empty placeholder";
        let placeholders = StringSet.add name placeholders in
        let acc = Placeholder name :: acc in
        loop acc placeholders (close_idx + 2) (close_idx + 2)
  in
  loop [] StringSet.empty 0 0

let render_prepared_query stmt params =
  let param_map = List.fold_left (fun acc (k, v) -> StringMap.add k v acc) StringMap.empty params in
  StringSet.iter
    (fun name ->
      if not (StringMap.mem name param_map) then
        invalid_arg (Printf.sprintf "Client.execute_prepared: missing parameter '%s'" name))
    stmt.placeholders;
  let buf = Buffer.create (String.length stmt.template + 64) in
  List.iter
    (function
      | Text chunk -> Buffer.add_string buf chunk
      | Placeholder name -> (
          match StringMap.find_opt name param_map with
          | Some value -> Buffer.add_string buf (value_to_literal value)
          | None ->
              invalid_arg (Printf.sprintf "Client.execute_prepared: missing parameter '%s'" name)))
    stmt.parts;
  Buffer.contents buf

module Prepared = struct
  type t = prepared_statement

  let render stmt ~params = render_prepared_query stmt params
  let placeholder_names stmt = StringSet.elements stmt.placeholders
end

let create ?host ?port ?database ?user ?password ?tls_config ?compression ?(settings = []) () =
  let conn =
    Connection.create ?host ?port ?database ?user ?password ?tls_config ?compression ~settings ()
  in
  { conn }

let disconnect c = Connection.disconnect c.conn

let execute_query_string c (query : string) : query_result Lwt.t =
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

let execute c query = execute_query_string c query
let prepare _ query = parse_prepared_statement query
let execute_prepared c stmt ~params = execute_query_string c (render_prepared_query stmt params)
let execute_with_params c query ~params = execute_prepared c (prepare c query) ~params

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

let stream_query_rows_prepared c stmt ~params ~on_row ~on_columns =
  let rendered = render_prepared_query stmt params in
  stream_query_rows c rendered ~on_row ~on_columns

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

let query_fold_prepared c stmt ~params ~init ~f =
  let acc_ref = ref init in
  let+ _ =
    stream_query_rows_prepared c stmt ~params
      ~on_row:(fun row ->
        let* new_acc = f !acc_ref row in
        acc_ref := new_acc;
        Lwt.return_unit)
      ~on_columns:(fun _ -> Lwt.return_unit)
  in
  !acc_ref

let query_fold_with_params c query ~params ~init ~f =
  query_fold_prepared c (prepare c query) ~params ~init ~f

let query_iter_prepared c stmt ~params ~f =
  let+ _ =
    stream_query_rows_prepared c stmt ~params ~on_row:f ~on_columns:(fun _ -> Lwt.return_unit)
  in
  ()

let query_iter_with_params c query ~params ~f = query_iter_prepared c (prepare c query) ~params ~f

let query_collect c query =
  let+ acc = query_fold c query ~init:[] ~f:(fun acc row -> Lwt.return (row :: acc)) in
  List.rev acc

let query_collect_prepared c stmt ~params =
  let+ acc =
    query_fold_prepared c stmt ~params ~init:[] ~f:(fun acc row -> Lwt.return (row :: acc))
  in
  List.rev acc

let query_collect_with_params c query ~params = query_collect_prepared c (prepare c query) ~params

let query_to_seq c query =
  (* This is tricky to implement properly with Lwt - for now return a simpler version *)
  let+ rows = query_collect c query in
  List.to_seq rows

let query_to_seq_prepared c stmt ~params =
  let+ rows = query_collect_prepared c stmt ~params in
  List.to_seq rows

let query_to_seq_with_params c query ~params = query_to_seq_prepared c (prepare c query) ~params

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

let query_fold_with_columns_prepared c stmt ~params ~init ~f =
  let acc_ref = ref init in
  let columns_ref = ref [] in
  let+ final_columns =
    stream_query_rows_prepared c stmt ~params
      ~on_row:(fun row ->
        let* new_acc = f !acc_ref row !columns_ref in
        acc_ref := new_acc;
        Lwt.return_unit)
      ~on_columns:(fun cols ->
        columns_ref := cols;
        Lwt.return_unit)
  in
  { rows = !acc_ref; columns = final_columns }

let query_fold_with_columns_with_params c query ~params ~init ~f =
  query_fold_with_columns_prepared c (prepare c query) ~params ~init ~f

let query_iter_with_columns_prepared c stmt ~params ~f =
  let columns_ref = ref [] in
  let+ final_columns =
    stream_query_rows_prepared c stmt ~params
      ~on_row:(fun row -> f row !columns_ref)
      ~on_columns:(fun cols ->
        columns_ref := cols;
        Lwt.return_unit)
  in
  final_columns

let query_iter_with_columns_with_params c query ~params ~f =
  query_iter_with_columns_prepared c (prepare c query) ~params ~f

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
