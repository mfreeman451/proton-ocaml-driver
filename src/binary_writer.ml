open Binary

let write_varint_to_buffer buf n =
  let rec loop n =
    if n >= 128 then (
      Buffer.add_char buf (Char.chr ((n land 127) lor 128));
      loop (n lsr 7)
    ) else (
      Buffer.add_char buf (Char.chr n)
    )
  in
  loop n

let write_bool_to_buffer buf b =
  Buffer.add_char buf (if b then '\001' else '\000')

let write_int32_le_to_buffer buf n =
  let a = Int32.to_int (Int32.logand n 0xFFl) in
  let b = Int32.to_int (Int32.logand (Int32.shift_right_logical n 8) 0xFFl) in
  let c = Int32.to_int (Int32.logand (Int32.shift_right_logical n 16) 0xFFl) in
  let d = Int32.to_int (Int32.logand (Int32.shift_right_logical n 24) 0xFFl) in
  Buffer.add_char buf (Char.chr a);
  Buffer.add_char buf (Char.chr b);
  Buffer.add_char buf (Char.chr c);
  Buffer.add_char buf (Char.chr d)

let write_string_to_buffer buf s =
  write_varint_to_buffer buf (String.length s);
  Buffer.add_string buf s

(* Write block info based on Go driver's encodeBlockInfo *)
let write_block_info_to_buffer buf (bi : Block_info.t) =
  write_varint_to_buffer buf 1; (* field 1 *)
  write_bool_to_buffer buf bi.is_overflows;
  write_varint_to_buffer buf 2; (* field 2 *)
  write_int32_le_to_buffer buf (Int32.of_int bi.bucket_num);
  write_varint_to_buffer buf 0 (* end marker *)

(* Serialize a column value based on its type *)
let rec write_value_to_buffer buf value =
  match value with
  | Columns.VString s -> write_string_to_buffer buf s
  | Columns.VInt32 n -> write_int32_le_to_buffer buf n
  | Columns.VInt64 n -> 
      write_int32_le_to_buffer buf (Int64.to_int32 (Int64.logand n 0xFFFFFFFFL));
      write_int32_le_to_buffer buf (Int64.to_int32 (Int64.shift_right_logical n 32))
  | Columns.VUInt32 n -> write_int32_le_to_buffer buf n
  | Columns.VUInt64 n ->
      write_int32_le_to_buffer buf (Int64.to_int32 (Int64.logand n 0xFFFFFFFFL));
      write_int32_le_to_buffer buf (Int64.to_int32 (Int64.shift_right_logical n 32))
  | Columns.VFloat64 f ->
      let bits = Int64.bits_of_float f in
      write_int32_le_to_buffer buf (Int64.to_int32 (Int64.logand bits 0xFFFFFFFFL));
      write_int32_le_to_buffer buf (Int64.to_int32 (Int64.shift_right_logical bits 32))
  | Columns.VDateTime (timestamp, _) ->
      write_int32_le_to_buffer buf (Int64.to_int32 timestamp)
  | Columns.VDateTime64 (timestamp, precision, _) ->
      write_int32_le_to_buffer buf (Int64.to_int32 (Int64.logand timestamp 0xFFFFFFFFL));
      write_int32_le_to_buffer buf (Int64.to_int32 (Int64.shift_right_logical timestamp 32))
  | Columns.VEnum8 (_, value) -> Buffer.add_char buf (Char.chr value)
  | Columns.VEnum16 (_, value) ->
      let a = value land 0xFF in
      let b = (value lsr 8) land 0xFF in
      Buffer.add_char buf (Char.chr a);
      Buffer.add_char buf (Char.chr b)
  | Columns.VArray values ->
      write_varint_to_buffer buf (List.length values);
      List.iter (write_value_to_buffer buf) values
  | Columns.VMap pairs ->
      write_varint_to_buffer buf (List.length pairs);
      List.iter (fun (k, v) ->
        write_value_to_buffer buf k;
        write_value_to_buffer buf v
      ) pairs
  | Columns.VTuple values ->
      write_varint_to_buffer buf (List.length values);
      List.iter (write_value_to_buffer buf) values
  | Columns.VNull -> Buffer.add_char buf '\000'