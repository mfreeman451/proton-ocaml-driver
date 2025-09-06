open Columns_types
open Binary

let reader_lowcardinality_of_spec ~(resolver:(string -> (in_channel -> int -> value array))) (s:string)
  : ((in_channel -> int -> value array)) option =
  let s = String.lowercase_ascii (String.trim s) in
  if has_prefix s "lowcardinality(" then (
    let inner = String.sub s 15 (String.length s - 16) |> String.trim in
    let inner_nullable, inner_base =
      if has_prefix inner "nullable(" then true, (String.sub inner 9 (String.length inner - 10) |> String.trim) else false, inner in
    let dict_reader = resolver inner_base in
    Some (fun ic n ->
      let index_ser = read_uint64_le ic in
      let key_type = Int64.to_int (Int64.logand index_ser 0xFFL) in
      let index_rows = read_uint64_le ic |> Int64.to_int in
      let dict = if index_rows > 0 then dict_reader ic index_rows else [||] in
      let keys_rows = read_uint64_le ic |> Int64.to_int in
      let read_key () = match key_type with
        | 0 -> input_byte ic
        | 1 -> Binary.read_int16_le ic |> Int32.to_int
        | 2 -> Binary.read_int32_le ic |> Int32.to_int
        | _ -> read_uint64_le ic |> Int64.to_int in
      let res = Array.make n VNull in
      for i=0 to n-1 do
        let idx = if i < keys_rows then read_key () else 0 in
        if inner_nullable && idx = 0 then res.(i) <- VNull else
        let di = if inner_nullable then idx - 1 else idx in
        res.(i) <- if di >= 0 && di < Array.length dict then dict.(di) else VNull
      done;
      res)
  ) else None

let reader_lowcardinality_of_spec_br ~(resolver:(string -> (Buffered_reader.t -> int -> value array))) (s:string)
  : ((Buffered_reader.t -> int -> value array)) option =
  let s = String.lowercase_ascii (String.trim s) in
  if has_prefix s "lowcardinality(" then (
    let inner = String.sub s 15 (String.length s - 16) |> String.trim in
    let inner_nullable, inner_base =
      if has_prefix inner "nullable(" then true, (String.sub inner 9 (String.length inner - 10) |> String.trim) else false, inner in
    let dict_reader = resolver inner_base in
    Some (fun br n ->
      let index_ser = read_uint64_le_br br in
      let key_type = Int64.to_int (Int64.logand index_ser 0xFFL) in
      let index_rows = read_uint64_le_br br |> Int64.to_int in
      let dict = if index_rows > 0 then dict_reader br index_rows else [||] in
      let keys_rows = read_uint64_le_br br |> Int64.to_int in
      let read_key () = match key_type with
        | 0 -> read_uint8_br br
        | 1 -> Binary.read_int16_le_br br |> Int32.to_int
        | 2 -> Binary.read_int32_le_br br |> Int32.to_int
        | _ -> read_uint64_le_br br |> Int64.to_int in
      let res = Array.make n VNull in
      for i=0 to n-1 do
        let idx = if i < keys_rows then read_key () else 0 in
        if inner_nullable && idx = 0 then res.(i) <- VNull else
        let di = if inner_nullable then idx - 1 else idx in
        res.(i) <- if di >= 0 && di < Array.length dict then dict.(di) else VNull
      done;
      res)
  ) else None
