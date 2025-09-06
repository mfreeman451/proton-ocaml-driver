open Columns_types
open Binary

let parse_datetime_params s =
  if not (String.contains s '(') then None
  else
    let start = String.index s '(' in
    let end_pos = String.index s ')' in
    let params_str = String.sub s (start + 1) (end_pos - start - 1) in
    Some (String.trim params_str)

let parse_datetime64_params s =
  if not (String.contains s '(') then (3, None)
  else
    let start = String.index s '(' in
    let end_pos = String.index s ')' in
    let params_str = String.sub s (start + 1) (end_pos - start - 1) in
    let parts = String.split_on_char ',' params_str in
    match parts with
    | [precision_str] -> (int_of_string (String.trim precision_str), None)
    | [precision_str; tz_str] ->
        let precision = int_of_string (String.trim precision_str) in
        let tz = String.trim tz_str in
        let clean_tz = if String.length tz >= 2 && tz.[0] = '\'' && tz.[String.length tz - 1] = '\'' then String.sub tz 1 (String.length tz - 2) else tz in
        (precision, Some clean_tz)
    | _ -> (3, None)

let reader_datetime_of_spec (s:string)
  : ((in_channel -> int -> value array)) option =
  let s = String.lowercase_ascii (String.trim s) in
  match true with
  | _ when s = "date" -> Some (fun ic n -> Array.init n (fun _ ->
      let a = read_uint8 ic in
      let b = read_uint8 ic in
      let days = a lor (b lsl 8) in
      let seconds = Int64.of_int (days * 86400) in
      VString (let tm = Unix.gmtime (Int64.to_float seconds) in Printf.sprintf "%04d-%02d-%02d" (tm.tm_year+1900) (tm.tm_mon+1) tm.tm_mday)))
  | _ when s = "date32" -> Some (fun ic n -> Array.init n (fun _ ->
      let days = read_int32_le ic |> Int32.to_int in
      let seconds = Int64.of_int (days * 86400) in
      VString (let tm = Unix.gmtime (Int64.to_float seconds) in Printf.sprintf "%04d-%02d-%02d" (tm.tm_year+1900) (tm.tm_mon+1) tm.tm_mday)))
  | _ when has_prefix s "datetime64" ->
      let (precision, timezone) = parse_datetime64_params s in
      Some (fun ic n -> Array.init n (fun _ -> let value = read_uint64_le ic in VDateTime64 (value, precision, timezone)))
  | _ when has_prefix s "datetime" ->
      let timezone = match parse_datetime_params s with None -> None | Some tz -> Some (if tz.[0]='\'' && tz.[String.length tz - 1]='\'' then String.sub tz 1 (String.length tz - 2) else tz) in
      Some (fun ic n -> Array.init n (fun _ -> let ts = read_int32_le ic |> Int64.of_int32 in VDateTime (Int64.logand ts 0xFFFFFFFFL, timezone)))
  | _ -> None

let reader_datetime_of_spec_br (s:string)
  : ((Buffered_reader.t -> int -> value array)) option =
  let s = String.lowercase_ascii (String.trim s) in
  match true with
  | _ when s = "date" -> Some (fun br n -> Array.init n (fun _ ->
      let a = Buffered_reader.input_byte br in
      let b = Buffered_reader.input_byte br in
      let days = a lor (b lsl 8) in
      let seconds = Int64.of_int (days * 86400) in
      VString (let tm = Unix.gmtime (Int64.to_float seconds) in Printf.sprintf "%04d-%02d-%02d" (tm.tm_year+1900) (tm.tm_mon+1) tm.tm_mday)))
  | _ when s = "date32" -> Some (fun br n -> Array.init n (fun _ ->
      let days = read_int32_le_br br |> Int32.to_int in
      let seconds = Int64.of_int (days * 86400) in
      VString (let tm = Unix.gmtime (Int64.to_float seconds) in Printf.sprintf "%04d-%02d-%02d" (tm.tm_year+1900) (tm.tm_mon+1) tm.tm_mday)))
  | _ when has_prefix s "datetime64" ->
      let (precision, timezone) = parse_datetime64_params s in
      Some (fun br n -> Array.init n (fun _ -> let value = read_uint64_le_br br in VDateTime64 (value, precision, timezone)))
  | _ when has_prefix s "datetime" ->
      let timezone = match parse_datetime_params s with None -> None | Some tz -> Some (if tz.[0]='\'' && tz.[String.length tz - 1]='\'' then String.sub tz 1 (String.length tz - 2) else tz) in
      Some (fun br n -> Array.init n (fun _ -> let ts = read_int32_le_br br |> Int64.of_int32 in VDateTime (Int64.logand ts 0xFFFFFFFFL, timezone)))
  | _ -> None
