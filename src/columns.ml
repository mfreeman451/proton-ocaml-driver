open Binary
open Varint

type value =
  | VNull
  | VString of string
  | VInt32  of int32
  | VInt64  of int64
  | VUInt32 of int32   (* we'll keep as signed carrier *)
  | VUInt64 of int64   (* ditto *)
  | VFloat64 of float

let value_to_string = function
  | VNull -> "NULL"
  | VString s -> s
  | VInt32 i -> Int32.to_string i
  | VUInt32 i -> Int32.to_string i
  | VInt64 i -> Int64.to_string i
  | VUInt64 i -> Int64.to_string i
  | VFloat64 f -> string_of_float f

(* reading helpers for simple types *)
let read_n_int32 ic n =
  Array.init n (fun _ -> VInt32 (read_int32_le ic))
let read_n_uint32 ic n =
  Array.init n (fun _ -> VUInt32 (read_int32_le ic))
let read_n_int64 ic n =
  Array.init n (fun _ -> VInt64 (read_uint64_le ic))
let read_n_uint64 ic n =
  Array.init n (fun _ -> VUInt64 (read_uint64_le ic))
let read_n_float64 ic n =
  Array.init n (fun _ -> VFloat64 (read_float64_le ic))
let read_n_string ic n =
  Array.init n (fun _ -> VString (read_str ic))

let read_nulls_map ic n : bool array =
  Array.init n (fun _ -> (read_uint8 ic) <> 0)

let apply_nulls nulls v =
  Array.mapi (fun i x -> if nulls.(i) then VNull else x) v

(* Parse a column type spec (lowercase); implement a subset *)
let trim s = String.trim s
let starts_with ~prefix s =
  let n = String.length prefix in
  String.length s >= n && String.sub s 0 n = prefix

(* Returns reader function that consumes n items *)
let rec reader_of_spec (spec:string) : (in_channel -> int -> value array) =
  let s = String.lowercase_ascii (trim spec) in
  if s = "string" then (fun ic n -> read_n_string ic n)
  else if s = "int32" then (fun ic n -> read_n_int32 ic n)
  else if s = "int64" then (fun ic n -> read_n_int64 ic n)
  else if s = "uint32" then (fun ic n -> read_n_uint32 ic n)
  else if s = "uint64" then (fun ic n -> read_n_uint64 ic n)
  else if s = "float64" then (fun ic n -> read_n_float64 ic n)
  else if starts_with ~prefix:"nullable(" s then
    let inner = String.sub s 9 (String.length s - 10) |> trim in
    let inner_reader = reader_of_spec inner in
    (fun ic n ->
       let nulls = read_nulls_map ic n in
       let vals = inner_reader ic n in
       apply_nulls nulls vals)
  else
    failwith (Printf.sprintf "Unsupported column type in Phase 1: %s" spec)

(* Bytes-reader variants *)
let read_n_int32_br br n =
  Array.init n (fun _ -> VInt32 (read_int32_le_br br))
let read_n_uint32_br br n =
  Array.init n (fun _ -> VUInt32 (read_int32_le_br br))
let read_n_int64_br br n =
  Array.init n (fun _ -> VInt64 (read_uint64_le_br br))
let read_n_uint64_br br n =
  Array.init n (fun _ -> VUInt64 (read_uint64_le_br br))
let read_n_float64_br br n =
  Array.init n (fun _ -> VFloat64 (read_float64_le_br br))
let read_n_string_br br n =
  Array.init n (fun _ -> VString (read_str_br br))

let read_nulls_map_br br n : bool array =
  Array.init n (fun _ -> (read_uint8_br br) <> 0)

let rec reader_of_spec_br (spec:string) : (Buffered_reader.t -> int -> value array) =
  let s = String.lowercase_ascii (trim spec) in
  if s = "string" then (fun br n -> read_n_string_br br n)
  else if s = "int32" then (fun br n -> read_n_int32_br br n)
  else if s = "int64" then (fun br n -> read_n_int64_br br n)
  else if s = "uint32" then (fun br n -> read_n_uint32_br br n)
  else if s = "uint64" then (fun br n -> read_n_uint64_br br n)
  else if s = "float64" then (fun br n -> read_n_float64_br br n)
  else if starts_with ~prefix:"nullable(" s then
    let inner = String.sub s 9 (String.length s - 10) |> trim in
    let inner_reader = reader_of_spec_br inner in
    (fun br n ->
       let nulls = read_nulls_map_br br n in
       let vals = inner_reader br n in
       apply_nulls nulls vals)
  else
    failwith (Printf.sprintf "Unsupported column type in Phase 1: %s" spec)
