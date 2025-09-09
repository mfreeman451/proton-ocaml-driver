open Varint
open Binary
open Column

type column = {
  name : string;
  type_spec : string;
  data : value array;  (* length = n_rows or empty if header *)
}

type t = {
  n_columns : int;
  n_rows : int;
  columns : column array;
}

let read_block ~revision ic : t =
  let _info =
    if revision >= Defines.dbms_min_revision_with_block_info
    then Block_info.read ic
    else { Block_info.is_overflows=false; bucket_num = -1 }
  in
  let n_columns = read_varint_int ic in
  let n_rows = read_varint_int ic in
  let cols =
    let rec loop i acc =
      if i = n_columns then Array.of_list (List.rev acc)
      else
        let name = read_str ic in
        let type_spec = read_str ic in
        let data =
          if n_rows = 0 then [||]
          else
            let reader = Column.compile_reader type_spec in
            reader ic n_rows
        in
        let c = { name; type_spec; data } in
        loop (i+1) (c::acc)
    in
    loop 0 []
  in
  { n_columns; n_rows; columns = cols }

let read_block_br ~revision br : t =
  let _info =
    if revision >= Defines.dbms_min_revision_with_block_info
    then Block_info.read_br br
    else { Block_info.is_overflows=false; bucket_num = -1 }
  in
  let n_columns = Binary.read_varint_int_br br in
  let n_rows = Binary.read_varint_int_br br in
  let cols =
    let rec loop i acc =
      if i = n_columns then Array.of_list (List.rev acc)
      else
        let name = Binary.read_str_br br in
        let type_spec = Binary.read_str_br br in
        let data =
          if n_rows = 0 then [||]
          else
            let reader = Column.compile_reader_br type_spec in
            reader br n_rows
        in
        let c = { name; type_spec; data } in
        loop (i+1) (c::acc)
    in
    loop 0 []
  in
  { n_columns; n_rows; columns = cols }

let get_rows (b:t) : value list list =
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

let columns_with_types (b:t) : (string * string) list =
  Array.to_list (Array.map (fun c -> (c.name, c.type_spec)) b.columns)
