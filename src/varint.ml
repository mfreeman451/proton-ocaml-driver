(* ULEB128 varint, like ClickHouse *)
let write_varint oc (n : int64) =
  let rec loop (x : int64) =
    let byte = Int64.to_int (Int64.logand x 0x7fL) in
    let x' = Int64.shift_right_logical x 7 in
    if Int64.compare x' 0L = 0 then output_byte oc byte
    else (
      output_byte oc (byte lor 0x80);
      loop x')
  in
  loop n

let read_varint ic : int64 =
  let rec loop shift acc =
    let b = input_byte ic in
    let v = Int64.logor acc (Int64.shift_left (Int64.of_int (b land 0x7f)) shift) in
    if b land 0x80 <> 0 then loop (shift + 7) v else v
  in
  loop 0 0L

let write_varint_int oc (n : int) = write_varint oc (Int64.of_int n)
let read_varint_int ic : int = Int64.to_int (read_varint ic)
