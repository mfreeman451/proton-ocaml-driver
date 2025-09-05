open Columns_types
open Binary

let reader_primitive_of_spec (s:string)
  : ((in_channel -> int -> value array)) option =
  let s = String.lowercase_ascii (String.trim s) in
  match true with
  | _ when s = "string" -> Some (fun ic n -> Array.init n (fun _ -> VString (read_str ic)))
  | _ when s = "int32" -> Some (fun ic n -> Array.init n (fun _ -> VInt32 (read_int32_le ic)))
  | _ when s = "uint32" -> Some (fun ic n -> Array.init n (fun _ -> VUInt32 (read_int32_le ic)))
  | _ when s = "int64" -> Some (fun ic n -> Array.init n (fun _ -> VInt64 (read_uint64_le ic)))
  | _ when s = "uint64" -> Some (fun ic n -> Array.init n (fun _ -> VUInt64 (read_uint64_le ic)))
  | _ when s = "float64" -> Some (fun ic n -> Array.init n (fun _ -> VFloat64 (read_float64_le ic)))
  | _ when s = "bool" -> Some (fun ic n -> Array.init n (fun _ -> VUInt32 (read_int32_le ic)))
  | _ when String.length s >= 12 && String.sub s 0 12 = "fixedstring(" ->
      let inside = String.sub s 12 (String.length s - 13) |> String.trim in
      let len = int_of_string inside in
      Some (fun ic n ->
        Array.init n (fun _ ->
          let buf = really_input_bytes ic len in
          let rec rstrip i = if i > 0 && Bytes.get buf (i-1) = Char.chr 0 then rstrip (i-1) else i in
          let l = rstrip len in
          VString (Bytes.sub_string buf 0 l)))
  | _ when s = "uuid" ->
      Some (fun ic n ->
        Array.init n (fun _ ->
          let b = really_input_bytes ic 16 in
          let hex i = Printf.sprintf "%02x" (Char.code (Bytes.get b i)) in
          let part a b = String.concat "" (List.init (b-a+1) (fun k -> hex (a+k))) in
          let s = part 0 3 ^ part 4 5 ^ part 6 7 ^ "-" ^ part 8 9 ^ "-" ^ part 10 11 ^ "-" ^ part 12 13 ^ "-" ^ part 14 15 in
          VString s)))
  | _ when s = "ipv4" ->
      Some (fun ic n -> Array.init n (fun _ ->
        let v = read_int32_le ic |> Int32.to_int in
        let b0 = v land 0xFF and b1 = (v lsr 8) land 0xFF and b2 = (v lsr 16) land 0xFF and b3 = (v lsr 24) land 0xFF in
        VString (Printf.sprintf "%d.%d.%d.%d" b0 b1 b2 b3)))
  | _ when s = "ipv6" ->
      Some (fun ic n -> Array.init n (fun _ ->
        let b = really_input_bytes ic 16 in
        let hex i = Printf.sprintf "%02x" (Char.code (Bytes.get b i)) in
        VString (String.concat ":" (List.init 8 (fun i -> hex (2*i) ^ hex (2*i+1))))))
  | _ when s = "json" -> Some (fun ic n -> Array.init n (fun _ -> VString (read_str ic)))
  | _ -> None

let reader_primitive_of_spec_br (s:string)
  : ((Buffered_reader.t -> int -> value array)) option =
  let s = String.lowercase_ascii (String.trim s) in
  match true with
  | _ when s = "string" -> Some (fun br n -> Array.init n (fun _ -> VString (read_str_br br)))
  | _ when s = "int32" -> Some (fun br n -> Array.init n (fun _ -> VInt32 (read_int32_le_br br)))
  | _ when s = "uint32" -> Some (fun br n -> Array.init n (fun _ -> VUInt32 (read_int32_le_br br)))
  | _ when s = "int64" -> Some (fun br n -> Array.init n (fun _ -> VInt64 (read_uint64_le_br br)))
  | _ when s = "uint64" -> Some (fun br n -> Array.init n (fun _ -> VUInt64 (read_uint64_le_br br)))
  | _ when s = "float64" -> Some (fun br n -> Array.init n (fun _ -> VFloat64 (read_float64_le_br br)))
  | _ when s = "bool" -> Some (fun br n -> Array.init n (fun _ -> VUInt32 (read_int32_le_br br)))
  | _ when String.length s >= 12 && String.sub s 0 12 = "fixedstring(" ->
      let inside = String.sub s 12 (String.length s - 13) |> String.trim in
      let len = int_of_string inside in
      Some (fun br n ->
        Array.init n (fun _ ->
          let buf = really_input_bytes_br br len in
          let rec rstrip i = if i > 0 && Bytes.get buf (i-1) = Char.chr 0 then rstrip (i-1) else i in
          let l = rstrip len in
          VString (Bytes.sub_string buf 0 l)))
  | _ when s = "uuid" ->
      Some (fun br n ->
        Array.init n (fun _ ->
          let b = really_input_bytes_br br 16 in
          let hex i = Printf.sprintf "%02x" (Char.code (Bytes.get b i)) in
          let part a b = String.concat "" (List.init (b-a+1) (fun k -> hex (a+k))) in
          let s = part 0 3 ^ part 4 5 ^ part 6 7 ^ "-" ^ part 8 9 ^ "-" ^ part 10 11 ^ "-" ^ part 12 13 ^ "-" ^ part 14 15 in
          VString s)))
  | _ when s = "ipv4" ->
      Some (fun br n -> Array.init n (fun _ ->
        let v = read_int32_le_br br |> Int32.to_int in
        let b0 = v land 0xFF and b1 = (v lsr 8) land 0xFF and b2 = (v lsr 16) land 0xFF and b3 = (v lsr 24) land 0xFF in
        VString (Printf.sprintf "%d.%d.%d.%d" b0 b1 b2 b3)))
  | _ when s = "ipv6" ->
      Some (fun br n -> Array.init n (fun _ ->
        let b = really_input_bytes_br br 16 in
        let hex i = Printf.sprintf "%02x" (Char.code (Bytes.get b i)) in
        VString (String.concat ":" (List.init 8 (fun i -> hex (2*i) ^ hex (2*i+1))))))
  | _ when s = "json" -> Some (fun br n -> Array.init n (fun _ -> VString (read_str_br br)))
  | _ -> None

