type value =
  | VNull
  | VString of string
  | VInt32  of int32
  | VInt64  of int64
  | VUInt32 of int32
  | VUInt64 of int64
  | VFloat64 of float
  | VDateTime of int64 * string option
  | VDateTime64 of int64 * int * string option
  | VArray of value list
  | VMap of (value * value) list
  | VTuple of value list

let rec value_to_string = function
  | VNull -> "NULL"
  | VString s -> s
  | VInt32 i -> Int32.to_string i
  | VUInt32 i -> Int32.to_string i
  | VInt64 i -> Int64.to_string i
  | VUInt64 i -> Int64.to_string i
  | VFloat64 f -> string_of_float f
  | VDateTime (ts, tz) ->
      let tz_str = match tz with Some z -> " " ^ z | None -> "" in
      Printf.sprintf "%Ld%s" ts tz_str
  | VDateTime64 (value, precision, tz) ->
      let tz_str = match tz with Some z -> " " ^ z | None -> "" in
      Printf.sprintf "%Ld(p=%d)%s" value precision tz_str
  | VArray values ->
      let elements = List.map value_to_string values in
      "[" ^ String.concat "," elements ^ "]"
  | VMap pairs ->
      let pair_strs = List.map (fun (k, v) -> value_to_string k ^ ":" ^ value_to_string v) pairs in
      "{" ^ String.concat "," pair_strs ^ "}"
  | VTuple vs -> "(" ^ String.concat "," (List.map value_to_string vs) ^ ")"

