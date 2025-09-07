open Column_types
open Binary

let trim = String.trim
let starts_with ~prefix s = has_prefix s prefix

let parse_array_element_type s =
  if not (String.contains s '(') then failwith "Invalid array type" else
  let start = String.index s '(' in let end_pos = String.rindex s ')' in
  String.sub s (start + 1) (end_pos - start - 1) |> String.trim

let parse_map_types s =
  if not (String.contains s '(') then failwith "Invalid map type" else
  let start = String.index s '(' in let end_pos = String.rindex s ')' in
  let types_str = String.sub s (start + 1) (end_pos - start - 1) in
  match String.split_on_char ',' types_str with | [k;v] -> (String.trim k, String.trim v) | _ -> failwith "Map must have exactly 2 types"

let parse_tuple_types s =
  if not (String.contains s '(') then failwith "Invalid tuple type" else
  let start = String.index s '(' in let end_pos = String.rindex s ')' in
  let inner = String.sub s (start + 1) (end_pos - start - 1) in
  let rec split acc depth i last =
    if i = String.length inner then List.rev ((String.sub inner last (i - last)) :: acc) else
    match inner.[i] with
    | '(' -> split acc (depth+1) (i+1) last
    | ')' -> split acc (depth-1) (i+1) last
    | ',' when depth = 0 -> split ((String.sub inner last (i-last))::acc) depth (i+1) (i+1)
    | _ -> split acc depth (i+1) last in
  split [] 0 0 0 |> List.map String.trim

(* New: assumes [s] is already normalized (lowercased + trimmed) *)
let reader_complex_of_spec_normalized ~(resolver:(string -> (in_channel -> int -> value array))) (s:string)
  : ((in_channel -> int -> value array)) option =
  match true with
  | _ when starts_with ~prefix:"array(" s ->
      let element_type = parse_array_element_type s in
      let element_reader = resolver element_type in
      Some (fun ic n ->
        let offsets = Array.make n 0L in
        for i=0 to n-1 do offsets.(i) <- read_uint64_le ic done;
        let total = if n=0 then 0 else Int64.to_int offsets.(n-1) in
        let all = element_reader ic total in
        let res = Array.make n (Array [||]) in
        let start_idx = ref 0 in
        for i=0 to n-1 do let e = Int64.to_int offsets.(i) in res.(i) <- Array (Array.sub all !start_idx (e - !start_idx)); start_idx := e done; res)
  | _ when starts_with ~prefix:"map(" s ->
      let (kt, vt) = parse_map_types s in
      let kr = resolver kt and vr = resolver vt in
      Some (fun ic n ->
        let offsets = Array.make n 0L in
        for i=0 to n-1 do offsets.(i) <- read_uint64_le ic done;
        let total = if n=0 then 0 else Int64.to_int offsets.(n-1) in
        let keys = kr ic total and vals = vr ic total in
        let res = Array.make n (Map []) in
        let idx = ref 0 in
        for i=0 to n-1 do let e = Int64.to_int offsets.(i) in let pairs = ref [] in while !idx < e do pairs := (keys.(!idx), vals.(!idx)) :: !pairs; incr idx done; res.(i) <- Map (List.rev !pairs) done; res)
  | _ when starts_with ~prefix:"tuple(" s ->
      let types = parse_tuple_types s in
      let readers = List.map resolver types in
      Some (fun ic n ->
        let cols = List.map (fun r -> r ic n) readers in
        let res = Array.make n Null in
        for i=0 to n-1 do res.(i) <- Tuple (List.map (fun a -> a.(i)) cols) done; res)
  | _ when starts_with ~prefix:"nullable(" s ->
      let inner = String.sub s 9 (String.length s - 10) |> trim in
      let inner_reader = resolver inner in
      Some (fun ic n ->
        let nulls = Array.make n false in
        for i=0 to n-1 do nulls.(i) <- (read_uint8 ic <> 0) done;
        let vals = inner_reader ic n in
        for i=0 to n-1 do if nulls.(i) then vals.(i) <- Null done; vals)
  | _ -> None


(* New: assumes [s] is already normalized (lowercased + trimmed) *)
let reader_complex_of_spec_br_normalized ~(resolver:(string -> (Buffered_reader.t -> int -> value array))) (s:string)
  : ((Buffered_reader.t -> int -> value array)) option =
  match true with
  | _ when starts_with ~prefix:"array(" s ->
      let element_type = parse_array_element_type s in
      let element_reader = resolver element_type in
      Some (fun br n ->
        let offsets = Array.make n 0L in
        for i=0 to n-1 do offsets.(i) <- read_uint64_le_br br done;
        let total = if n=0 then 0 else Int64.to_int offsets.(n-1) in
        let all = element_reader br total in
        let res = Array.make n (Array [||]) in
        let start_idx = ref 0 in
        for i=0 to n-1 do let e = Int64.to_int offsets.(i) in res.(i) <- Array (Array.sub all !start_idx (e - !start_idx)); start_idx := e done; res)
  | _ when starts_with ~prefix:"map(" s ->
      let (kt, vt) = parse_map_types s in
      let kr = resolver kt and vr = resolver vt in
      Some (fun br n ->
        let offsets = Array.make n 0L in
        for i=0 to n-1 do offsets.(i) <- read_uint64_le_br br done;
        let total = if n=0 then 0 else Int64.to_int offsets.(n-1) in
        let keys = kr br total and vals = vr br total in
        let res = Array.make n (Map []) in
        let idx = ref 0 in
        for i=0 to n-1 do let e = Int64.to_int offsets.(i) in let pairs = ref [] in while !idx < e do pairs := (keys.(!idx), vals.(!idx)) :: !pairs; incr idx done; res.(i) <- Map (List.rev !pairs) done; res)
  | _ when starts_with ~prefix:"tuple(" s ->
      let types = parse_tuple_types s in
      let readers = List.map resolver types in
      Some (fun br n ->
        let cols = List.map (fun r -> r br n) readers in
        let res = Array.make n Null in
        for i=0 to n-1 do res.(i) <- Tuple (List.map (fun a -> a.(i)) cols) done; res)
  | _ when starts_with ~prefix:"nullable(" s ->
      let inner = String.sub s 9 (String.length s - 10) |> trim in
      let inner_reader = resolver inner in
      Some (fun br n ->
        let nulls = Array.make n false in
        for i=0 to n-1 do nulls.(i) <- (read_uint8_br br <> 0) done;
        let vals = inner_reader br n in
        for i=0 to n-1 do if nulls.(i) then vals.(i) <- Null done; vals)
  | _ -> None
