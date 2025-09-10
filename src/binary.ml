(* Binary encoding/decoding functions for ClickHouse protocol *)

open Varint

(* Helper to read exactly n bytes *)
let really_input_bytes ic n =
  let buf = Bytes.create n in
  really_input ic buf 0 n;
  buf

(* Basic types - Little Endian *)
let[@inline] read_uint8 ic = input_byte ic
let[@inline] write_uint8 oc v = output_byte oc v

let[@inline] read_int32_le ic =
  let a = input_byte ic in
  let b = input_byte ic in
  let c = input_byte ic in
  let d = input_byte ic in
  Int32.logor (Int32.of_int a)
    (Int32.logor
       (Int32.shift_left (Int32.of_int b) 8)
       (Int32.logor (Int32.shift_left (Int32.of_int c) 16) (Int32.shift_left (Int32.of_int d) 24)))

let[@inline] write_int32_le oc v =
  let a = Int32.to_int (Int32.logand v 0xFFl) in
  let b = Int32.to_int (Int32.logand (Int32.shift_right_logical v 8) 0xFFl) in
  let c = Int32.to_int (Int32.logand (Int32.shift_right_logical v 16) 0xFFl) in
  let d = Int32.to_int (Int32.logand (Int32.shift_right_logical v 24) 0xFFl) in
  output_byte oc a;
  output_byte oc b;
  output_byte oc c;
  output_byte oc d

let[@inline] read_uint64_le ic =
  let low = read_int32_le ic in
  let high = read_int32_le ic in
  Int64.logor
    (Int64.logand (Int64.of_int32 low) 0xFFFFFFFFL)
    (Int64.shift_left (Int64.of_int32 high) 32)

let[@inline] write_uint64_le oc v =
  let low32 = Int64.to_int32 (Int64.logand v 0xFFFFFFFFL) in
  let high32 = Int64.to_int32 (Int64.shift_right_logical v 32) in
  write_int32_le oc low32;
  write_int32_le oc high32

let[@inline] read_float64_le ic =
  let bits = read_uint64_le ic in
  Int64.float_of_bits bits

let[@inline] write_float64_le oc v =
  let bits = Int64.bits_of_float v in
  write_uint64_le oc bits

(* String reading/writing *)
let read_str ic =
  let len = read_varint_int ic in
  if len = 0 then "" else really_input_string ic len

let write_str oc s =
  let len = String.length s in
  write_varint_int oc len;
  output_string oc s

(* Bytes-reader variants *)
let really_input_bytes_br br n =
  let buf = Bytes.create n in
  Buffered_reader.really_input br buf 0 n;
  buf

(* keep read_uint8_br as alias *)

let[@inline] read_int32_le_br br =
  let a = Buffered_reader.input_byte br in
  let b = Buffered_reader.input_byte br in
  let c = Buffered_reader.input_byte br in
  let d = Buffered_reader.input_byte br in
  Int32.logor (Int32.of_int a)
    (Int32.logor
       (Int32.shift_left (Int32.of_int b) 8)
       (Int32.logor (Int32.shift_left (Int32.of_int c) 16) (Int32.shift_left (Int32.of_int d) 24)))

let[@inline] read_uint64_le_br br =
  let low = read_int32_le_br br in
  let high = read_int32_le_br br in
  Int64.logor
    (Int64.logand (Int64.of_int32 low) 0xFFFFFFFFL)
    (Int64.shift_left (Int64.of_int32 high) 32)

let[@inline] read_int64_le_br br =
  let low = read_int32_le_br br in
  let high = read_int32_le_br br in
  Int64.logor (Int64.of_int32 low) (Int64.shift_left (Int64.of_int32 high) 32)

let[@inline] read_float64_le_br br =
  let bits = read_uint64_le_br br in
  Int64.float_of_bits bits

let read_varint_int_br br : int =
  let rec loop shift acc =
    let b = Buffered_reader.input_byte br in
    let v = acc lor ((b land 0x7f) lsl shift) in
    if b land 0x80 <> 0 then loop (shift + 7) v else v
  in
  loop 0 0

let read_str_br br =
  let len = read_varint_int_br br in
  if len = 0 then "" else Buffered_reader.really_input_string br len

(* Bytes helpers for compression module *)
let[@inline] bytes_get_int32_le buf offset =
  let a = Char.code (Bytes.get buf offset) in
  let b = Char.code (Bytes.get buf (offset + 1)) in
  let c = Char.code (Bytes.get buf (offset + 2)) in
  let d = Char.code (Bytes.get buf (offset + 3)) in
  Int32.logor (Int32.of_int a)
    (Int32.logor
       (Int32.shift_left (Int32.of_int b) 8)
       (Int32.logor (Int32.shift_left (Int32.of_int c) 16) (Int32.shift_left (Int32.of_int d) 24)))

let[@inline] bytes_set_int32_le buf offset v =
  let a = Int32.to_int (Int32.logand v 0xFFl) in
  let b = Int32.to_int (Int32.logand (Int32.shift_right_logical v 8) 0xFFl) in
  let c = Int32.to_int (Int32.logand (Int32.shift_right_logical v 16) 0xFFl) in
  let d = Int32.to_int (Int32.logand (Int32.shift_right_logical v 24) 0xFFl) in
  Bytes.set buf offset (Char.chr a);
  Bytes.set buf (offset + 1) (Char.chr b);
  Bytes.set buf (offset + 2) (Char.chr c);
  Bytes.set buf (offset + 3) (Char.chr d)

let[@inline] bytes_get_int64_le buf offset =
  let low = bytes_get_int32_le buf offset in
  let high = bytes_get_int32_le buf (offset + 4) in
  Int64.logor
    (Int64.logand (Int64.of_int32 low) 0xFFFFFFFFL)
    (Int64.shift_left (Int64.logand (Int64.of_int32 high) 0xFFFFFFFFL) 32)

let[@inline] bytes_set_int64_le buf offset v =
  let low32 = Int64.to_int32 (Int64.logand v 0xFFFFFFFFL) in
  let high32 = Int64.to_int32 (Int64.shift_right_logical v 32) in
  bytes_set_int32_le buf offset low32;
  bytes_set_int32_le buf (offset + 4) high32

let read_int16_le ic =
  let a = input_byte ic in
  let b = input_byte ic in
  let v = a lor (b lsl 8) in
  if v land 0x8000 <> 0 then Int32.of_int (v - 0x10000) else Int32.of_int v

let[@inline] read_int16_le_br br =
  let a = Buffered_reader.input_byte br in
  let b = Buffered_reader.input_byte br in
  let v = a lor (b lsl 8) in
  if v land 0x8000 <> 0 then Int32.of_int (v - 0x10000) else Int32.of_int v

let[@inline] read_uint8_br br = Buffered_reader.input_byte br

let read_int64_le ic =
  let low = read_int32_le ic in
  let high = read_int32_le ic in
  Int64.logor (Int64.of_int32 low) (Int64.shift_left (Int64.of_int32 high) 32)
