(* Binary encoding/decoding functions for ClickHouse protocol *)

open Varint

(* Helper to read exactly n bytes *)
let really_input_bytes ic n =
  let buf = Bytes.create n in
  really_input ic buf 0 n;
  buf

(* Basic types - Little Endian *)
let read_uint8 ic = input_byte ic

let write_uint8 oc v = output_byte oc v

let read_int32_le ic =
  let a = input_byte ic in
  let b = input_byte ic in
  let c = input_byte ic in
  let d = input_byte ic in
  Int32.logor
    (Int32.of_int a)
    (Int32.logor
       (Int32.shift_left (Int32.of_int b) 8)
       (Int32.logor
          (Int32.shift_left (Int32.of_int c) 16)
          (Int32.shift_left (Int32.of_int d) 24)))

let write_int32_le oc v =
  let a = Int32.to_int (Int32.logand v 0xFFl) in
  let b = Int32.to_int (Int32.logand (Int32.shift_right_logical v 8) 0xFFl) in
  let c = Int32.to_int (Int32.logand (Int32.shift_right_logical v 16) 0xFFl) in
  let d = Int32.to_int (Int32.logand (Int32.shift_right_logical v 24) 0xFFl) in
  output_byte oc a;
  output_byte oc b;
  output_byte oc c;
  output_byte oc d

let read_uint64_le ic =
  let low = read_int32_le ic in
  let high = read_int32_le ic in
  Int64.logor 
    (Int64.logand (Int64.of_int32 low) 0xFFFFFFFFL)
    (Int64.shift_left (Int64.of_int32 high) 32)

let write_uint64_le oc v =
  let low = Int64.to_int (Int64.logand v 0xFFFFFFFFL) in
  let high = Int64.to_int (Int64.shift_right_logical v 32) in
  write_int32_le oc (Int32.of_int low);
  write_int32_le oc (Int32.of_int high)

let read_float64_le ic =
  let bits = read_uint64_le ic in
  Int64.float_of_bits bits

let write_float64_le oc v =
  let bits = Int64.bits_of_float v in
  write_uint64_le oc bits

(* String reading/writing *)
let read_str ic =
  let len = read_varint_int ic in
  if len = 0 then ""
  else
    let buf = really_input_bytes ic len in
    Bytes.to_string buf

let write_str oc s =
  let len = String.length s in
  write_varint_int oc len;
  output_string oc s

(* Bytes helpers for compression module *)
let bytes_get_int32_le buf offset =
  let a = Char.code (Bytes.get buf offset) in
  let b = Char.code (Bytes.get buf (offset + 1)) in
  let c = Char.code (Bytes.get buf (offset + 2)) in
  let d = Char.code (Bytes.get buf (offset + 3)) in
  Int32.logor
    (Int32.of_int a)
    (Int32.logor
       (Int32.shift_left (Int32.of_int b) 8)
       (Int32.logor
          (Int32.shift_left (Int32.of_int c) 16)
          (Int32.shift_left (Int32.of_int d) 24)))

let bytes_set_int32_le buf offset v =
  let a = Int32.to_int (Int32.logand v 0xFFl) in
  let b = Int32.to_int (Int32.logand (Int32.shift_right_logical v 8) 0xFFl) in
  let c = Int32.to_int (Int32.logand (Int32.shift_right_logical v 16) 0xFFl) in
  let d = Int32.to_int (Int32.logand (Int32.shift_right_logical v 24) 0xFFl) in
  Bytes.set buf offset (Char.chr a);
  Bytes.set buf (offset + 1) (Char.chr b);
  Bytes.set buf (offset + 2) (Char.chr c);
  Bytes.set buf (offset + 3) (Char.chr d)

let bytes_get_int64_le buf offset =
  let low = bytes_get_int32_le buf offset in
  let high = bytes_get_int32_le buf (offset + 4) in
  Int64.logor 
    (Int64.logand (Int64.of_int32 low) 0xFFFFFFFFL)
    (Int64.shift_left (Int64.of_int32 high) 32)

let bytes_set_int64_le buf offset v =
  let low = Int64.to_int (Int64.logand v 0xFFFFFFFFL) in
  let high = Int64.to_int (Int64.shift_right_logical v 32) in
  bytes_set_int32_le buf offset (Int32.of_int low);
  bytes_set_int32_le buf (offset + 4) (Int32.of_int high)