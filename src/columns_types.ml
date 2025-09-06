type value =
  | Null
  | String of string
  | Int32  of int32
  | Int64  of int64
  | UInt32 of int32
  | UInt64 of int64
  | Float64 of float
  | DateTime of int64 * string option
  | DateTime64 of int64 * int * string option
  | Enum8 of string * int  (* name, value *)
  | Enum16 of string * int (* name, value *)
  | Array of value array
  | Map of (value * value) list
  | Tuple of value list

(* Small inlineable helper to avoid substring allocations when checking prefixes *)
let[@inline] has_prefix (s:string) (p:string) : bool =
  let ls = String.length s and lp = String.length p in
  if ls < lp then false else (
    let rec loop i =
      if i = lp then true
      else if s.[i] <> p.[i] then false
      else loop (i+1)
    in
    loop 0
  )

let rec value_to_string = function
  | Null -> "NULL"
  | String s -> s
  | Int32 i -> Int32.to_string i
  | UInt32 i -> Int32.to_string i
  | Int64 i -> Int64.to_string i
  | UInt64 i -> Int64.to_string i
  | Float64 f -> string_of_float f
  | DateTime (ts, tz) ->
      let tz_str = match tz with Some z -> " " ^ z | None -> "" in
      Printf.sprintf "%Ld%s" ts tz_str
  | DateTime64 (value, precision, tz) ->
      let tz_str = match tz with Some z -> " " ^ z | None -> "" in
      Printf.sprintf "%Ld(p=%d)%s" value precision tz_str
  | Enum8 (name, _) -> name
  | Enum16 (name, _) -> name
  | Array values ->
      let elements =
        (* Build without intermediate strings where easy *)
        let acc = ref [] in
        Array.iter (fun v -> acc := (value_to_string v) :: !acc) values;
        List.rev !acc
      in
      "[" ^ String.concat "," elements ^ "]"
  | Map pairs ->
      let pair_strs = List.map (fun (k, v) -> value_to_string k ^ ":" ^ value_to_string v) pairs in
      "{" ^ String.concat "," pair_strs ^ "}"
  | Tuple vs -> "(" ^ String.concat "," (List.map value_to_string vs) ^ ")"
