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
  | VTuple of value list  (* Tuple values *)

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
  | VTuple vs -> "(" ^ String.concat "," (List.map value_to_string vs) ^ ")"

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

(* Parse Tuple type parameters, split by commas at top level *)
let parse_tuple_types s =
  if not (String.contains s '(') then failwith "Invalid tuple type"
  else
    let start = String.index s '(' in
    let end_pos = String.rindex s ')' in
    let inner = String.sub s (start + 1) (end_pos - start - 1) in
    let rec split acc depth i last =
      if i = String.length inner then List.rev ((String.sub inner last (i - last)) :: acc)
      else match inner.[i] with
        | '(' -> split acc (depth+1) (i+1) last
        | ')' -> split acc (depth-1) (i+1) last
        | ',' when depth = 0 -> split ((String.sub inner last (i-last))::acc) depth (i+1) (i+1)
        | _ -> split acc depth (i+1) last
    in
    split [] 0 0 0 |> List.map String.trim

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
  else if s = "bool" then (fun ic n -> Array.init n (fun _ -> VUInt32 (read_int32_le ic)))
  else if starts_with ~prefix:"fixedstring(" s then
    let inside = String.sub s 12 (String.length s - 13) |> trim in
    let len = int_of_string inside in
    (fun ic n ->
      Array.init n (fun _ ->
        let buf = really_input_bytes ic len in
        let rec rstrip i = if i > 0 && Bytes.get buf (i-1) = Char.chr 0 then rstrip (i-1) else i in
        let l = rstrip len in
        VString (Bytes.sub_string buf 0 l)))
  else if s = "uuid" then
    (fun ic n ->
      Array.init n (fun _ ->
        let b = really_input_bytes ic 16 in
        let hex i = Printf.sprintf "%02x" (Char.code (Bytes.get b i)) in
        let part a b = String.concat "" (List.init (b-a+1) (fun k -> hex (a+k))) in
        let s = part 0 3 ^ part 4 5 ^ part 6 7 ^ "-" ^ part 8 9 ^ "-" ^ part 10 11 ^ "-" ^ part 12 13 ^ "-" ^ part 14 15 in
        VString s))
  else if s = "date" then
    (fun ic n ->
      Array.init n (fun _ ->
        let a = input_byte ic in
        let b = input_byte ic in
        let days = a lor (b lsl 8) in
        let seconds = Int64.of_int (days * 86400) in
        let tm = Unix.gmtime (Int64.to_float seconds) in
        let s = Printf.sprintf "%04d-%02d-%02d" (tm.tm_year+1900) (tm.tm_mon+1) tm.tm_mday in
        VString s))
  else if s = "date32" then
    (fun ic n ->
      Array.init n (fun _ ->
        let days = read_int32_le ic |> Int32.to_int in
        let seconds = Int64.of_int (days * 86400) in
        let tm = Unix.gmtime (Int64.to_float seconds) in
        let s = Printf.sprintf "%04d-%02d-%02d" (tm.tm_year+1900) (tm.tm_mon+1) tm.tm_mday in
        VString s))
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
  else if starts_with ~prefix:"lowcardinality(" s then
    let inner = String.sub s 15 (String.length s - 16) |> trim in
    let (inner_nullable, inner_base) =
      if starts_with ~prefix:"nullable(" inner then (true, String.sub inner 9 (String.length inner - 10) |> trim)
      else (false, inner)
    in
    let dict_reader = reader_of_spec inner_base in
    (fun ic n ->
      let index_serialization_type = read_uint64_le ic in
      let key_type = Int64.to_int (Int64.logand index_serialization_type 0xFFL) in
      (* read dictionary rows *)
      let index_rows = Binary.read_int64_le ic |> Int64.to_int in
      let dict = if index_rows > 0 then dict_reader ic index_rows else [||] in
      let keys_rows = Binary.read_int64_le ic |> Int64.to_int in
      let read_key () = match key_type with
        | 0 -> input_byte ic
        | 1 -> Binary.read_int16_le ic |> Int32.to_int
        | 2 -> Binary.read_int32_le ic |> Int32.to_int
        | _ -> let v = read_uint64_le ic in Int64.to_int v
      in
      let res = Array.make n VNull in
      for i = 0 to n-1 do
        let idx = if i < keys_rows then read_key () else 0 in
        if inner_nullable && idx = 0 then res.(i) <- VNull
        else
          let di = if inner_nullable then idx - 1 else idx in
          res.(i) <- if di >= 0 && di < Array.length dict then dict.(di) else VNull
      done;
      res)
  else if starts_with ~prefix:"tuple(" s then
    let types = parse_tuple_types s in
    let readers = List.map reader_of_spec types in
    (fun ic n ->
      let cols = List.map (fun r -> r ic n) readers in
      Array.init n (fun i -> VTuple (List.map (fun a -> a.(i)) cols)))
  else if starts_with ~prefix:"enum8(" s then
    let mapping_str = String.sub spec 6 (String.length spec - 7) in
    let pairs = String.split_on_char ',' mapping_str |> List.filter (fun s -> String.trim s <> "") in
    let table = Hashtbl.create 16 in
    List.iter (fun p -> match String.split_on_char '=' p with
      | [name; v] ->
          let name = String.trim name |> fun s -> if s.[0] = '\'' then String.sub s 1 (String.length s - 2) else s in
          let v = int_of_string (String.trim v) in Hashtbl.add table v name
      | _ -> ()) pairs;
    (fun ic n ->
      Array.init n (fun _ ->
        let b = input_byte ic in
        let v = if b > 127 then b - 256 else b in
        VString (try Hashtbl.find table v with Not_found -> string_of_int v)))
  else if starts_with ~prefix:"enum16(" s then
    let mapping_str = String.sub spec 7 (String.length spec - 8) in
    let pairs = String.split_on_char ',' mapping_str |> List.filter (fun s -> String.trim s <> "") in
    let table = Hashtbl.create 32 in
    List.iter (fun p -> match String.split_on_char '=' p with
      | [name; v] ->
          let name = String.trim name |> fun s -> if s.[0] = '\'' then String.sub s 1 (String.length s - 2) else s in
          let v = int_of_string (String.trim v) in Hashtbl.add table v name
      | _ -> ()) pairs;
    (fun ic n ->
      Array.init n (fun _ ->
        let v = Int32.to_int (Binary.read_int16_le ic) in
        VString (try Hashtbl.find table v with Not_found -> string_of_int v)))
  else if starts_with ~prefix:"decimal(" s then
    (* Decimal(P,S) support for P<=18 using signed 64-bit storage *)
    let inside = String.sub s 8 (String.length s - 9) |> String.trim in
    let parts = String.split_on_char ',' inside |> List.map String.trim in
    let precision, scale = match parts with | [p;s] -> int_of_string p, int_of_string s | _ -> (18, 0) in
    if precision > 18 then (fun _ _ -> failwith "Decimal precision > 18 not supported yet") else
    (fun ic n ->
      Array.init n (fun _ ->
        let raw = Binary.read_int64_le ic in
        let neg = Int64.compare raw 0L < 0 in
        let abs = if neg then Int64.neg raw else raw in
        let s = Int64.to_string abs in
        let len = String.length s in
        let res = if scale = 0 then s else if len <= scale then "0." ^ String.make (scale - len) '0' ^ s else String.sub s 0 (len - scale) ^ "." ^ String.sub s (len - scale) scale in
        VString (if neg then "-" ^ res else res)))
  else if s = "ipv4" then
    (fun ic n ->
      Array.init n (fun _ ->
        let v = read_int32_le ic in
        let x = Int32.to_int v in
        let b0 = x land 0xFF in
        let b1 = (x lsr 8) land 0xFF in
        let b2 = (x lsr 16) land 0xFF in
        let b3 = (x lsr 24) land 0xFF in
        VString (Printf.sprintf "%d.%d.%d.%d" b0 b1 b2 b3)))
  else if s = "ipv6" then
    (fun ic n ->
      Array.init n (fun _ ->
        let b = really_input_bytes ic 16 in
        let hex i = Printf.sprintf "%02x" (Char.code (Bytes.get b i)) in
        let parts = List.init 8 (fun i -> hex (2*i) ^ hex (2*i+1)) in
        VString (String.concat ":" parts)))
  else if s = "json" then (fun ic n -> read_n_string ic n)
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
  else if s = "bool" then (fun br n -> Array.init n (fun _ -> VUInt32 (read_int32_le_br br)))
  else if starts_with ~prefix:"fixedstring(" s then
    let inside = String.sub s 12 (String.length s - 13) |> trim in
    let len = int_of_string inside in
    (fun br n ->
      Array.init n (fun _ ->
        let buf = really_input_bytes_br br len in
        let rec rstrip i = if i > 0 && Bytes.get buf (i-1) = Char.chr 0 then rstrip (i-1) else i in
        let l = rstrip len in
        VString (Bytes.sub_string buf 0 l)))
  else if s = "uuid" then
    (fun br n ->
      Array.init n (fun _ ->
        let b = really_input_bytes_br br 16 in
        let hex i = Printf.sprintf "%02x" (Char.code (Bytes.get b i)) in
        let part a b = String.concat "" (List.init (b-a+1) (fun k -> hex (a+k))) in
        let s = part 0 3 ^ part 4 5 ^ part 6 7 ^ "-" ^ part 8 9 ^ "-" ^ part 10 11 ^ "-" ^ part 12 13 ^ "-" ^ part 14 15 in
        VString s))
  else if s = "date" then
    (fun br n ->
      Array.init n (fun _ ->
        let a = Buffered_reader.input_byte br in
        let b = Buffered_reader.input_byte br in
        let days = a lor (b lsl 8) in
        let seconds = Int64.of_int (days * 86400) in
        let tm = Unix.gmtime (Int64.to_float seconds) in
        let s = Printf.sprintf "%04d-%02d-%02d" (tm.tm_year+1900) (tm.tm_mon+1) tm.tm_mday in
        VString s))
  else if s = "date32" then
    (fun br n ->
      Array.init n (fun _ ->
        let days = read_int32_le_br br |> Int32.to_int in
        let seconds = Int64.of_int (days * 86400) in
        let tm = Unix.gmtime (Int64.to_float seconds) in
        let s = Printf.sprintf "%04d-%02d-%02d" (tm.tm_year+1900) (tm.tm_mon+1) tm.tm_mday in
        VString s))
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
  else if starts_with ~prefix:"lowcardinality(" s then
    let inner = String.sub s 15 (String.length s - 16) |> trim in
    let (inner_nullable, inner_base) =
      if starts_with ~prefix:"nullable(" inner then (true, String.sub inner 9 (String.length inner - 10) |> trim)
      else (false, inner)
    in
    let dict_reader = reader_of_spec_br inner_base in
    (fun br n ->
      let index_serialization_type = read_uint64_le_br br in
      let key_type = Int64.to_int (Int64.logand index_serialization_type 0xFFL) in
      let index_rows = read_uint64_le_br br |> Int64.to_int in
      let dict = if index_rows > 0 then dict_reader br index_rows else [||] in
      let keys_rows = read_uint64_le_br br |> Int64.to_int in
      let read_key () = match key_type with
        | 0 -> read_uint8_br br
        | 1 -> Binary.read_int16_le_br br |> Int32.to_int
        | 2 -> Binary.read_int32_le_br br |> Int32.to_int
        | _ -> read_uint64_le_br br |> Int64.to_int
      in
      let res = Array.make n VNull in
      for i = 0 to n-1 do
        let idx = if i < keys_rows then read_key () else 0 in
        if inner_nullable && idx = 0 then res.(i) <- VNull
        else
          let di = if inner_nullable then idx - 1 else idx in
          res.(i) <- if di >= 0 && di < Array.length dict then dict.(di) else VNull
      done;
      res)
  else if starts_with ~prefix:"tuple(" s then
    let types = parse_tuple_types s in
    let readers = List.map reader_of_spec_br types in
    (fun br n ->
      let cols = List.map (fun r -> r br n) readers in
      Array.init n (fun i -> VTuple (List.map (fun a -> a.(i)) cols)))
  else if starts_with ~prefix:"enum8(" s then
    let mapping_str = String.sub spec 6 (String.length spec - 7) in
    let pairs = String.split_on_char ',' mapping_str |> List.filter (fun s -> String.trim s <> "") in
    let table = Hashtbl.create 16 in
    List.iter (fun p -> match String.split_on_char '=' p with
      | [name; v] ->
          let name = String.trim name |> fun s -> if s.[0] = '\'' then String.sub s 1 (String.length s - 2) else s in
          let v = int_of_string (String.trim v) in Hashtbl.add table v name
      | _ -> ()) pairs;
    (fun br n ->
      Array.init n (fun _ ->
        let b = read_uint8_br br in
        let v = if b > 127 then b - 256 else b in
        VString (try Hashtbl.find table v with Not_found -> string_of_int v)))
  else if starts_with ~prefix:"enum16(" s then
    let mapping_str = String.sub spec 7 (String.length spec - 8) in
    let pairs = String.split_on_char ',' mapping_str |> List.filter (fun s -> String.trim s <> "") in
    let table = Hashtbl.create 32 in
    (fun br n ->
      (* build table on first read to use Binary.read_int16_le_br *)
      let () = if Hashtbl.length table = 0 then
        List.iter (fun p -> match String.split_on_char '=' p with
          | [name; v] ->
              let name = String.trim name |> fun s -> if s.[0] = '\'' then String.sub s 1 (String.length s - 2) else s in
              let v = Int32.of_int (int_of_string (String.trim v)) in Hashtbl.add table (Int32.to_int v) name
          | _ -> ()) pairs in
      Array.init n (fun _ ->
        let v = Binary.read_int16_le_br br |> Int32.to_int in
        VString (try Hashtbl.find table v with Not_found -> string_of_int v)))
  else if starts_with ~prefix:"decimal(" s then
    let inside = String.sub s 8 (String.length s - 9) |> String.trim in
    let parts = String.split_on_char ',' inside |> List.map String.trim in
    let precision, scale = match parts with | [p;s] -> int_of_string p, int_of_string s | _ -> (18, 0) in
    if precision > 18 then (fun _ _ -> failwith "Decimal precision > 18 not supported yet") else
    (fun br n ->
      Array.init n (fun _ ->
        let raw = Binary.read_int64_le_br br in
        let neg = Int64.compare raw 0L < 0 in
        let abs = if neg then Int64.neg raw else raw in
        let s = Int64.to_string abs in
        let len = String.length s in
        let res = if scale = 0 then s else if len <= scale then "0." ^ String.make (scale - len) '0' ^ s else String.sub s 0 (len - scale) ^ "." ^ String.sub s (len - scale) scale in
        VString (if neg then "-" ^ res else res)))
  else if s = "ipv4" then
    (fun br n ->
      Array.init n (fun _ ->
        let v = read_int32_le_br br in
        let x = Int32.to_int v in
        let b0 = x land 0xFF in
        let b1 = (x lsr 8) land 0xFF in
        let b2 = (x lsr 16) land 0xFF in
        let b3 = (x lsr 24) land 0xFF in
        VString (Printf.sprintf "%d.%d.%d.%d" b0 b1 b2 b3)))
  else if s = "ipv6" then
    (fun br n ->
      Array.init n (fun _ ->
        let b = really_input_bytes_br br 16 in
        let hex i = Printf.sprintf "%02x" (Char.code (Bytes.get b i)) in
        let parts = List.init 8 (fun i -> hex (2*i) ^ hex (2*i+1)) in
        VString (String.concat ":" parts)))
  else if s = "json" then (fun br n -> read_n_string_br br n)
  else if starts_with ~prefix:"nullable(" s then
    let inner = String.sub s 9 (String.length s - 10) |> trim in
    let inner_reader = reader_of_spec_br inner in
    (fun br n ->
       let nulls = read_nulls_map_br br n in
       let vals = inner_reader br n in
       apply_nulls nulls vals)
  else
    failwith (Printf.sprintf "Unsupported column type in Phase 1: %s" spec)
