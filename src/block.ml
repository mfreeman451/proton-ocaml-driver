open Varint
open Binary
open Column

(* Keep the last seen header's column types to handle servers that omit
   repeating type specs in subsequent data blocks. *)
let last_header_types : (string array) option ref = ref None

type column = {
  name : string;
  type_spec : string;
  data : value array; (* length = n_rows or empty if header *)
}

type t = { n_columns : int; n_rows : int; columns : column array }

let read_block ~revision ic : t =
  let _info =
    if revision >= Defines.dbms_min_revision_with_block_info then Block_info.read ic
    else { Block_info.is_overflows = false; bucket_num = -1 }
  in
  let n_columns = read_varint_int ic in
  let n_rows = read_varint_int ic in
  let cols =
    let rec loop i acc =
      if i = n_columns then Array.of_list (List.rev acc)
      else
        let name = read_str ic in
        let raw_type = read_str ic in
        let type_spec =
          if n_rows = 0 then raw_type
          else if raw_type <> "" then raw_type
          else
            (* Substitute from last header types if available *)
            match !last_header_types with
            | Some arr when i < Array.length arr -> arr.(i)
            | _ -> raw_type
        in
        (match Sys.getenv_opt "PROTON_DEBUG" with
         | Some ("1"|"true"|"TRUE"|"yes"|"YES") ->
             Printf.printf "[proton] block col[%d/%d] name='%s' type='%s' rows=%d\n%!"
               (i+1) n_columns name type_spec n_rows
         | _ -> ());
        let data =
          if n_rows = 0 then [||]
          else
            let reader = Column.compile_reader type_spec in
            reader ic n_rows
        in
        let c = { name; type_spec; data } in
        loop (i + 1) (c :: acc)
    in
    loop 0 []
  in
  (* If this is a header block, remember its types for subsequent data blocks. *)
  if n_rows = 0 then (
    let types = Array.map (fun c -> c.type_spec) cols in
    last_header_types := Some types
  );
  { n_columns; n_rows; columns = cols }

let read_block_br ~revision br : t =
  let _info =
    if revision >= Defines.dbms_min_revision_with_block_info then Block_info.read_br br
    else { Block_info.is_overflows = false; bucket_num = -1 }
  in
  let n_columns = Binary.read_varint_int_br br in
  let n_rows = Binary.read_varint_int_br br in
  let cols =
    let rec loop i acc =
      if i = n_columns then Array.of_list (List.rev acc)
      else
        let name = Binary.read_str_br br in
        let raw_type = Binary.read_str_br br in
        let type_spec =
          if n_rows = 0 then raw_type
          else if raw_type <> "" then raw_type
          else
            match !last_header_types with
            | Some arr when i < Array.length arr -> arr.(i)
            | _ -> raw_type
        in
        (match Sys.getenv_opt "PROTON_DEBUG" with
         | Some ("1"|"true"|"TRUE"|"yes"|"YES") ->
             Printf.printf "[proton] block(br) col[%d/%d] name='%s' type='%s' rows=%d\n%!"
               (i+1) n_columns name type_spec n_rows
         | _ -> ());
        let data =
          if n_rows = 0 then [||]
          else
            let reader = Column.compile_reader_br type_spec in
            reader br n_rows
        in
        let c = { name; type_spec; data } in
        loop (i + 1) (c :: acc)
    in
    loop 0 []
  in
  if n_rows = 0 then (
    let types = Array.map (fun c -> c.type_spec) cols in
    last_header_types := Some types
  );
  { n_columns; n_rows; columns = cols }

let get_rows (b : t) : value list list =
  if b.n_rows = 0 then []
  else
    let n_cols = Array.length b.columns in
    let arrays = Array.map (fun c -> c.data) b.columns in
    let rows = ref [] in
    for r = b.n_rows - 1 downto 0 do
      let row = ref [] in
      for c = n_cols - 1 downto 0 do
        row := arrays.(c).(r) :: !row
      done;
      rows := !row :: !rows
    done;
    !rows

let columns_with_types (b : t) : (string * string) list =
  Array.to_list (Array.map (fun c -> (c.name, c.type_spec)) b.columns)
