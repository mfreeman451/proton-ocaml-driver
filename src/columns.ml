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
  | VDateTime of int64 * string option  (* Unix timestamp, timezone *)
  | VDateTime64 of int64 * int * string option  (* value, precision, timezone *)
  | VArray of value list  (* Array of values *)
  | VMap of (value * value) list  (* Map of key-value pairs *)

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
      let pair_strs = List.map (fun (k, v) -> 
        value_to_string k ^ ":" ^ value_to_string v) pairs in
      "{" ^ String.concat "," pair_strs ^ "}"

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

(* DateTime readers - DateTime stores Unix timestamp as UInt32 *)
let read_n_datetime ic n timezone =
  Array.init n (fun _ -> 
    let ts_int32 = read_int32_le ic in
    let ts = Int64.logand (Int64.of_int32 ts_int32) 0xFFFFFFFFL in
    VDateTime (ts, timezone))

(* DateTime64 readers - stores scaled value as UInt64 *)
let read_n_datetime64 ic n precision timezone =
  Array.init n (fun _ ->
    let value = read_uint64_le ic in
    VDateTime64 (value, precision, timezone))

(* Array readers - reads offsets then values *)
let read_n_array ic n element_reader =
  (* Read offsets (UInt64 array) *)
  let offsets = Array.init n (fun _ -> read_uint64_le ic) in
  
  (* Calculate total number of elements *)
  let total_elements = 
    if n = 0 then 0
    else Int64.to_int offsets.(n-1)
  in
  
  (* Read all element values *)
  let all_elements = element_reader ic total_elements in
  
  (* Split elements into arrays using offsets *)
  let result = Array.make n (VArray []) in
  let start_idx = ref 0 in
  for i = 0 to n - 1 do
    let end_idx = Int64.to_int offsets.(i) in
    let array_elements = Array.sub all_elements !start_idx (end_idx - !start_idx) in
    result.(i) <- VArray (Array.to_list array_elements);
    start_idx := end_idx
  done;
  result

(* Map readers - reads offsets then keys then values *)
let read_n_map ic n key_reader value_reader =
  (* Read offsets (UInt64 array) *)
  let offsets = Array.init n (fun _ -> read_uint64_le ic) in
  
  (* Calculate total number of key-value pairs *)
  let total_pairs = 
    if n = 0 then 0
    else Int64.to_int offsets.(n-1)
  in
  
  (* Read all keys and values *)
  let all_keys = key_reader ic total_pairs in
  let all_values = value_reader ic total_pairs in
  
  (* Split into maps using offsets *)
  let result = Array.make n (VMap []) in
  let start_idx = ref 0 in
  for i = 0 to n - 1 do
    let end_idx = Int64.to_int offsets.(i) in
    let map_pairs = ref [] in
    for j = !start_idx to end_idx - 1 do
      map_pairs := (all_keys.(j), all_values.(j)) :: !map_pairs
    done;
    result.(i) <- VMap (List.rev !map_pairs);
    start_idx := end_idx
  done;
  result

let read_nulls_map ic n : bool array =
  Array.init n (fun _ -> (read_uint8 ic) <> 0)

let apply_nulls nulls v =
  Array.mapi (fun i x -> if nulls.(i) then VNull else x) v

(* Parse DateTime type parameters *)
let parse_datetime_params s =
  if not (String.contains s '(') then None
  else
    let start = String.index s '(' in
    let end_pos = String.index s ')' in
    let params_str = String.sub s (start + 1) (end_pos - start - 1) in
    Some (String.trim params_str)

let parse_datetime64_params s =
  if not (String.contains s '(') then (3, None) (* default precision *)
  else
    let start = String.index s '(' in
    let end_pos = String.index s ')' in
    let params_str = String.sub s (start + 1) (end_pos - start - 1) in
    let parts = String.split_on_char ',' params_str in
    match parts with
    | [precision_str] -> 
        (int_of_string (String.trim precision_str), None)
    | [precision_str; tz_str] ->
        let precision = int_of_string (String.trim precision_str) in
        let tz = String.trim tz_str in
        let clean_tz = if String.length tz >= 2 && tz.[0] = '\'' && tz.[String.length tz - 1] = '\'' 
                      then String.sub tz 1 (String.length tz - 2)
                      else tz in
        (precision, Some clean_tz)
    | _ -> (3, None)

(* Parse Array type parameters *)
let parse_array_element_type s =
  if not (String.contains s '(') then failwith "Invalid array type"
  else
    let start = String.index s '(' in
    let end_pos = String.rindex s ')' in
    let element_type = String.sub s (start + 1) (end_pos - start - 1) in
    String.trim element_type

(* Parse Map type parameters *)
let parse_map_types s =
  if not (String.contains s '(') then failwith "Invalid map type"
  else
    let start = String.index s '(' in
    let end_pos = String.rindex s ')' in
    let types_str = String.sub s (start + 1) (end_pos - start - 1) in
    let parts = String.split_on_char ',' types_str in
    match parts with
    | [key_type; value_type] -> (String.trim key_type, String.trim value_type)
    | _ -> failwith "Map must have exactly 2 types: key and value"

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
  else if starts_with ~prefix:"datetime64" s then
    let (precision, timezone) = parse_datetime64_params s in
    (fun ic n -> read_n_datetime64 ic n precision timezone)
  else if starts_with ~prefix:"datetime" s then
    let timezone = match parse_datetime_params s with
      | None -> None
      | Some tz -> Some (if tz.[0] = '\'' && tz.[String.length tz - 1] = '\'' 
                        then String.sub tz 1 (String.length tz - 2) else tz)
    in
    (fun ic n -> read_n_datetime ic n timezone)
  else if starts_with ~prefix:"array(" s then
    let element_type = parse_array_element_type s in
    let element_reader = reader_of_spec element_type in
    (fun ic n -> read_n_array ic n element_reader)
  else if starts_with ~prefix:"map(" s then
    let (key_type, value_type) = parse_map_types s in
    let key_reader = reader_of_spec key_type in
    let value_reader = reader_of_spec value_type in
    (fun ic n -> read_n_map ic n key_reader value_reader)
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

(* DateTime buffered reader variants *)
let read_n_datetime_br br n timezone =
  Array.init n (fun _ -> 
    let ts_int32 = read_int32_le_br br in
    let ts = Int64.logand (Int64.of_int32 ts_int32) 0xFFFFFFFFL in
    VDateTime (ts, timezone))

let read_n_datetime64_br br n precision timezone =
  Array.init n (fun _ ->
    let value = read_uint64_le_br br in
    VDateTime64 (value, precision, timezone))

(* Array buffered reader *)
let read_n_array_br br n element_reader =
  (* Read offsets (UInt64 array) *)
  let offsets = Array.init n (fun _ -> read_uint64_le_br br) in
  
  (* Calculate total number of elements *)
  let total_elements = 
    if n = 0 then 0
    else Int64.to_int offsets.(n-1)
  in
  
  (* Read all element values *)
  let all_elements = element_reader br total_elements in
  
  (* Split elements into arrays using offsets *)
  let result = Array.make n (VArray []) in
  let start_idx = ref 0 in
  for i = 0 to n - 1 do
    let end_idx = Int64.to_int offsets.(i) in
    let array_elements = Array.sub all_elements !start_idx (end_idx - !start_idx) in
    result.(i) <- VArray (Array.to_list array_elements);
    start_idx := end_idx
  done;
  result

(* Map buffered reader *)
let read_n_map_br br n key_reader value_reader =
  (* Read offsets (UInt64 array) *)
  let offsets = Array.init n (fun _ -> read_uint64_le_br br) in
  
  (* Calculate total number of key-value pairs *)
  let total_pairs = 
    if n = 0 then 0
    else Int64.to_int offsets.(n-1)
  in
  
  (* Read all keys and values *)
  let all_keys = key_reader br total_pairs in
  let all_values = value_reader br total_pairs in
  
  (* Split into maps using offsets *)
  let result = Array.make n (VMap []) in
  let start_idx = ref 0 in
  for i = 0 to n - 1 do
    let end_idx = Int64.to_int offsets.(i) in
    let map_pairs = ref [] in
    for j = !start_idx to end_idx - 1 do
      map_pairs := (all_keys.(j), all_values.(j)) :: !map_pairs
    done;
    result.(i) <- VMap (List.rev !map_pairs);
    start_idx := end_idx
  done;
  result

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
  else if starts_with ~prefix:"datetime64" s then
    let (precision, timezone) = parse_datetime64_params s in
    (fun br n -> read_n_datetime64_br br n precision timezone)
  else if starts_with ~prefix:"datetime" s then
    let timezone = match parse_datetime_params s with
      | None -> None
      | Some tz -> Some (if tz.[0] = '\'' && tz.[String.length tz - 1] = '\'' 
                        then String.sub tz 1 (String.length tz - 2) else tz)
    in
    (fun br n -> read_n_datetime_br br n timezone)
  else if starts_with ~prefix:"array(" s then
    let element_type = parse_array_element_type s in
    let element_reader = reader_of_spec_br element_type in
    (fun br n -> read_n_array_br br n element_reader)
  else if starts_with ~prefix:"map(" s then
    let (key_type, value_type) = parse_map_types s in
    let key_reader = reader_of_spec_br key_type in
    let value_reader = reader_of_spec_br value_type in
    (fun br n -> read_n_map_br br n key_reader value_reader)
  else if starts_with ~prefix:"nullable(" s then
    let inner = String.sub s 9 (String.length s - 10) |> trim in
    let inner_reader = reader_of_spec_br inner in
    (fun br n ->
       let nulls = read_nulls_map_br br n in
       let vals = inner_reader br n in
       apply_nulls nulls vals)
  else
    failwith (Printf.sprintf "Unsupported column type in Phase 1: %s" spec)
