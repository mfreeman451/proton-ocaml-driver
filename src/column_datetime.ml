open Column_types
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
  | _ when s = "date" -> Some (fun ic n -> let a = Array.make n Null in for i=0 to n-1 do
      let a0 = read_uint8 ic in
      let b0 = read_uint8 ic in
      let days = a0 lor (b0 lsl 8) in
      let seconds = Int64.mul (Int64.of_int days) 86400L in
      a.(i) <- DateTime (seconds, None)
    done; a)
  | _ when s = "date32" -> Some (fun ic n -> let a = Array.make n Null in for i=0 to n-1 do
      let days = read_int32_le ic |> Int32.to_int in
      let seconds = Int64.mul (Int64.of_int days) 86400L in
      a.(i) <- DateTime (seconds, None)
    done; a)
  | _ when has_prefix s "datetime64" ->
      let (precision, timezone) = parse_datetime64_params s in
      Some (fun ic n -> let a = Array.make n Null in for i=0 to n-1 do let value = read_uint64_le ic in a.(i) <- DateTime64 (value, precision, timezone) done; a)
  | _ when has_prefix s "datetime" ->
      let timezone = match parse_datetime_params s with None -> None | Some tz -> Some (if tz.[0]='\'' && tz.[String.length tz - 1]='\'' then String.sub tz 1 (String.length tz - 2) else tz) in
      Some (fun ic n -> let a = Array.make n Null in for i=0 to n-1 do let ts = read_int32_le ic |> Int64.of_int32 in a.(i) <- DateTime (Int64.logand ts 0xFFFFFFFFL, timezone) done; a)
  | _ -> None

let reader_datetime_of_spec_br (s:string)
  : ((Buffered_reader.t -> int -> value array)) option =
  let s = String.lowercase_ascii (String.trim s) in
  match true with
  | _ when s = "date" -> Some (fun br n -> let a = Array.make n Null in for i=0 to n-1 do
      let a0 = Buffered_reader.input_byte br in
      let b0 = Buffered_reader.input_byte br in
      let days = a0 lor (b0 lsl 8) in
      let seconds = Int64.mul (Int64.of_int days) 86400L in
      a.(i) <- DateTime (seconds, None)
    done; a)
  | _ when s = "date32" -> Some (fun br n -> let a = Array.make n Null in for i=0 to n-1 do
      let days = read_int32_le_br br |> Int32.to_int in
      let seconds = Int64.mul (Int64.of_int days) 86400L in
      a.(i) <- DateTime (seconds, None)
    done; a)
  | _ when has_prefix s "datetime64" ->
      let (precision, timezone) = parse_datetime64_params s in
      Some (fun br n -> let a = Array.make n Null in for i=0 to n-1 do let value = read_uint64_le_br br in a.(i) <- DateTime64 (value, precision, timezone) done; a)
  | _ when has_prefix s "datetime" ->
      let timezone = match parse_datetime_params s with None -> None | Some tz -> Some (if tz.[0]='\'' && tz.[String.length tz - 1]='\'' then String.sub tz 1 (String.length tz - 2) else tz) in
      Some (fun br n -> let a = Array.make n Null in for i=0 to n-1 do let ts = read_int32_le_br br |> Int64.of_int32 in a.(i) <- DateTime (Int64.logand ts 0xFFFFFFFFL, timezone) done; a)
  | _ -> None

