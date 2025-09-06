open Columns_types
open Binary

(* Cheap formatters to avoid Printf in hot paths *)
let hex_tab = [| "0";"1";"2";"3";"4";"5";"6";"7";"8";"9";"a";"b";"c";"d";"e";"f" |]

let two_hex (b:int) (buf:bytes) (off:int) =
  let hi = (b lsr 4) land 0xF and lo = b land 0xF in
  Bytes.blit_string hex_tab.(hi) 0 buf off 1;
  Bytes.blit_string hex_tab.(lo) 0 buf (off+1) 1

let format_uuid_16 (b:bytes) : string =
  let out = Bytes.create 36 in
  let write_byte i o = two_hex (Char.code (Bytes.get b i)) out o in
  write_byte 0 0; write_byte 1 2; write_byte 2 4; write_byte 3 6;  Bytes.set out 8 '-';
  write_byte 4 9; write_byte 5 11;                               Bytes.set out 13 '-';
  write_byte 6 14; write_byte 7 16;                              Bytes.set out 18 '-';
  write_byte 8 19; write_byte 9 21;                              Bytes.set out 23 '-';
  write_byte 10 24; write_byte 11 26; write_byte 12 28; write_byte 13 30; write_byte 14 32; write_byte 15 34;
  Bytes.unsafe_to_string out

let write_u8_decimal (out:bytes) (off:int) (v:int) : int =
  if v >= 100 then (
    let h = v / 100 in Bytes.set out off (Char.chr (48 + h));
    let rem = v - (h * 100) in
    let t = rem / 10 in Bytes.set out (off+1) (Char.chr (48 + t));
    let o = rem - (t * 10) in Bytes.set out (off+2) (Char.chr (48 + o));
    3
  ) else if v >= 10 then (
    let t = v / 10 in Bytes.set out off (Char.chr (48 + t));
    let o = v - (t * 10) in Bytes.set out (off+1) (Char.chr (48 + o));
    2
  ) else (
    Bytes.set out off (Char.chr (48 + v)); 1
  )

let format_ipv4 (v:int) : string =
  let out = Bytes.create 15 in
  let b0 = v land 0xFF
  and b1 = (v lsr 8) land 0xFF
  and b2 = (v lsr 16) land 0xFF
  and b3 = (v lsr 24) land 0xFF in
  let len0 = write_u8_decimal out 0 b0 in Bytes.set out len0 '.';
  let len1 = len0 + 1 + write_u8_decimal out (len0 + 1) b1 in Bytes.set out len1 '.';
  let len2 = len1 + 1 + write_u8_decimal out (len1 + 1) b2 in Bytes.set out len2 '.';
  let len3 = len2 + 1 + write_u8_decimal out (len2 + 1) b3 in
  Bytes.sub_string out 0 len3

let format_ipv6_16 (b:bytes) : string =
  (* Full form: 8 hextets with leading zeros, lowercase, separated by ':' *)
  let out = Bytes.create 39 in
  let write_hextet idx off =
    two_hex (Char.code (Bytes.get b (2*idx))) out off;
    two_hex (Char.code (Bytes.get b (2*idx + 1))) out (off + 2)
  in
  write_hextet 0 0;  Bytes.set out 4  ':';
  write_hextet 1 5;  Bytes.set out 9  ':';
  write_hextet 2 10; Bytes.set out 14 ':';
  write_hextet 3 15; Bytes.set out 19 ':';
  write_hextet 4 20; Bytes.set out 24 ':';
  write_hextet 5 25; Bytes.set out 29 ':';
  write_hextet 6 30; Bytes.set out 34 ':';
  write_hextet 7 35;
  Bytes.unsafe_to_string out

let reader_primitive_of_spec (s:string)
  : ((in_channel -> int -> value array)) option =
  let s = String.lowercase_ascii (String.trim s) in
  match true with
  | _ when s = "string" -> Some (fun ic n -> let a = Array.make n Null in for i=0 to n-1 do a.(i) <- String (read_str ic) done; a)
  | _ when s = "int8" -> Some (fun ic n -> let a = Array.make n Null in for i=0 to n-1 do let b = read_uint8 ic in let v = if b > 127 then b - 256 else b in a.(i) <- Int32 (Int32.of_int v) done; a)
  | _ when s = "uint8" -> Some (fun ic n -> let a = Array.make n Null in for i=0 to n-1 do a.(i) <- UInt32 (Int32.of_int (read_uint8 ic)) done; a)
  | _ when s = "int16" -> Some (fun ic n -> let a = Array.make n Null in for i=0 to n-1 do let a1 = read_uint8 ic and b1 = read_uint8 ic in let v = a1 lor (b1 lsl 8) in let v = if v land 0x8000 <> 0 then v - 0x10000 else v in a.(i) <- Int32 (Int32.of_int v) done; a)
  | _ when s = "uint16" -> Some (fun ic n -> let a = Array.make n Null in for i=0 to n-1 do let a1 = read_uint8 ic and b1 = read_uint8 ic in a.(i) <- UInt32 (Int32.of_int (a1 lor (b1 lsl 8))) done; a)
  | _ when s = "int32" -> Some (fun ic n -> let a = Array.make n Null in for i=0 to n-1 do a.(i) <- Int32 (read_int32_le ic) done; a)
  | _ when s = "uint32" -> Some (fun ic n -> let a = Array.make n Null in for i=0 to n-1 do a.(i) <- UInt32 (read_int32_le ic) done; a)
  | _ when s = "int64" -> Some (fun ic n -> let a = Array.make n Null in for i=0 to n-1 do a.(i) <- Int64 (read_uint64_le ic) done; a)
  | _ when s = "uint64" -> Some (fun ic n -> let a = Array.make n Null in for i=0 to n-1 do a.(i) <- UInt64 (read_uint64_le ic) done; a)
  | _ when s = "float64" -> Some (fun ic n -> let a = Array.make n Null in for i=0 to n-1 do a.(i) <- Float64 (read_float64_le ic) done; a)
  | _ when s = "bool" -> Some (fun ic n -> let a = Array.make n Null in for i=0 to n-1 do a.(i) <- UInt32 (read_int32_le ic) done; a)
  | _ when has_prefix s "fixedstring(" ->
      let inside = String.sub s 12 (String.length s - 13) |> String.trim in
      let len = int_of_string inside in
      Some (fun ic n ->
        let a = Array.make n Null in
        for i=0 to n-1 do
          let buf = really_input_bytes ic len in
          let rec rstrip i = if i > 0 && Bytes.get buf (i-1) = Char.chr 0 then rstrip (i-1) else i in
          let l = rstrip len in
          a.(i) <- String (Bytes.sub_string buf 0 l)
        done; a)
  | _ when s = "uuid" ->
      Some (fun ic n ->
        let a = Array.make n Null in
        for i=0 to n-1 do
          let b = really_input_bytes ic 16 in
          a.(i) <- String (format_uuid_16 b)
        done; a)
  | _ when s = "ipv4" ->
      Some (fun ic n -> let a = Array.make n Null in for i=0 to n-1 do
        let v = read_int32_le ic |> Int32.to_int in
        a.(i) <- String (format_ipv4 v)
      done; a)
  | _ when s = "ipv6" ->
      Some (fun ic n -> let a = Array.make n Null in for i=0 to n-1 do
        let b = really_input_bytes ic 16 in
        a.(i) <- String (format_ipv6_16 b)
      done; a)
  | _ when s = "json" -> Some (fun ic n -> let a = Array.make n Null in for i=0 to n-1 do a.(i) <- String (read_str ic) done; a)
  | _ when has_prefix s "enum8(" -> 
      (* Parse enum8 mapping *)
      let inside = String.sub s 6 (String.length s - 7) in
      let pairs = String.split_on_char ',' inside |> List.filter (fun x -> String.trim x <> "") in
      let tbl = Hashtbl.create 16 in
      List.iter (fun p -> match String.split_on_char '=' p with
        | [name; v] ->
            let name = String.trim name in
            let name = if String.length name >= 2 && name.[0] = '\'' then String.sub name 1 (String.length name - 2) else name in
            let v = int_of_string (String.trim v) in Hashtbl.add tbl v name
        | _ -> ()) pairs;
      Some (fun ic n -> let a = Array.make n Null in for i=0 to n-1 do
        let b = read_uint8 ic in
        let v = if b > 127 then b - 256 else b in
        let s = try Hashtbl.find tbl v with Not_found -> string_of_int v in
        a.(i) <- Enum8 (s, v)
      done; a)
  | _ when has_prefix s "enum16(" ->
      (* Parse enum16 mapping *)
      let inside = String.sub s 7 (String.length s - 8) in
      let pairs = String.split_on_char ',' inside |> List.filter (fun x -> String.trim x <> "") in
      let tbl = Hashtbl.create 32 in
      List.iter (fun p -> match String.split_on_char '=' p with
        | [name; v] ->
            let name = String.trim name in
            let name = if String.length name >= 2 && name.[0] = '\'' then String.sub name 1 (String.length name - 2) else name in
            let v = int_of_string (String.trim v) in Hashtbl.add tbl v name
        | _ -> ()) pairs;
      Some (fun ic n -> let a = Array.make n Null in for i=0 to n-1 do
        let v32 = read_int32_le ic in
        let v = Int32.to_int v32 in
        let s = try Hashtbl.find tbl v with Not_found -> string_of_int v in
        a.(i) <- Enum16 (s, v)
      done; a)
  | _ when has_prefix s "decimal" ->
  (* Decimal values are stored as integers. For Decimal(10,2) it's int64 *)
  Some (fun ic n -> let a = Array.make n Null in for i=0 to n-1 do a.(i) <- Int64 (read_uint64_le ic) done; a)
  | _ -> None

let reader_primitive_of_spec_br (s:string)
  : ((Buffered_reader.t -> int -> value array)) option =
  let s = String.lowercase_ascii (String.trim s) in
  match true with
  | _ when s = "string" -> Some (fun br n -> let a = Array.make n Null in for i=0 to n-1 do a.(i) <- String (read_str_br br) done; a)
  | _ when s = "int8" -> Some (fun br n -> let a = Array.make n Null in for i=0 to n-1 do let b = read_uint8_br br in let v = if b > 127 then b - 256 else b in a.(i) <- Int32 (Int32.of_int v) done; a)
  | _ when s = "uint8" -> Some (fun br n -> let a = Array.make n Null in for i=0 to n-1 do a.(i) <- UInt32 (Int32.of_int (read_uint8_br br)) done; a)
  | _ when s = "int16" -> Some (fun br n -> let a = Array.make n Null in for i=0 to n-1 do let a1 = read_uint8_br br and b1 = read_uint8_br br in let v = a1 lor (b1 lsl 8) in let v = if v land 0x8000 <> 0 then v - 0x10000 else v in a.(i) <- Int32 (Int32.of_int v) done; a)
  | _ when s = "uint16" -> Some (fun br n -> let a = Array.make n Null in for i=0 to n-1 do let a1 = read_uint8_br br and b1 = read_uint8_br br in a.(i) <- UInt32 (Int32.of_int (a1 lor (b1 lsl 8))) done; a)
  | _ when s = "int32" -> Some (fun br n -> let a = Array.make n Null in for i=0 to n-1 do a.(i) <- Int32 (read_int32_le_br br) done; a)
  | _ when s = "uint32" -> Some (fun br n -> let a = Array.make n Null in for i=0 to n-1 do a.(i) <- UInt32 (read_int32_le_br br) done; a)
  | _ when s = "int64" -> Some (fun br n -> let a = Array.make n Null in for i=0 to n-1 do a.(i) <- Int64 (read_uint64_le_br br) done; a)
  | _ when s = "uint64" -> Some (fun br n -> let a = Array.make n Null in for i=0 to n-1 do a.(i) <- UInt64 (read_uint64_le_br br) done; a)
  | _ when s = "float64" -> Some (fun br n -> let a = Array.make n Null in for i=0 to n-1 do a.(i) <- Float64 (read_float64_le_br br) done; a)
  | _ when s = "bool" -> Some (fun br n -> let a = Array.make n Null in for i=0 to n-1 do a.(i) <- UInt32 (read_int32_le_br br) done; a)
  | _ when has_prefix s "fixedstring(" ->
      let inside = String.sub s 12 (String.length s - 13) |> String.trim in
      let len = int_of_string inside in
      Some (fun br n ->
        let a = Array.make n Null in
        for i=0 to n-1 do
          let buf = really_input_bytes_br br len in
          let rec rstrip i = if i > 0 && Bytes.get buf (i-1) = Char.chr 0 then rstrip (i-1) else i in
          let l = rstrip len in
          a.(i) <- String (Bytes.sub_string buf 0 l)
        done; a)
  | _ when s = "uuid" ->
      Some (fun br n ->
        let a = Array.make n Null in
        for i=0 to n-1 do
          let b = really_input_bytes_br br 16 in
          a.(i) <- String (format_uuid_16 b)
        done; a)
  | _ when s = "ipv4" ->
      Some (fun br n -> let a = Array.make n Null in for i=0 to n-1 do
        let v = read_int32_le_br br |> Int32.to_int in
        a.(i) <- String (format_ipv4 v)
      done; a)
  | _ when s = "ipv6" ->
      Some (fun br n -> let a = Array.make n Null in for i=0 to n-1 do
        let b = really_input_bytes_br br 16 in
        a.(i) <- String (format_ipv6_16 b)
      done; a)
  | _ when s = "json" -> Some (fun br n -> let a = Array.make n Null in for i=0 to n-1 do a.(i) <- String (read_str_br br) done; a)
  | _ when has_prefix s "enum8(" -> 
      (* Parse enum8 mapping *)
      let inside = String.sub s 6 (String.length s - 7) in
      let pairs = String.split_on_char ',' inside |> List.filter (fun x -> String.trim x <> "") in
      let tbl = Hashtbl.create 16 in
      List.iter (fun p -> match String.split_on_char '=' p with
        | [name; v] ->
            let name = String.trim name in
            let name = if String.length name >= 2 && name.[0] = '\'' then String.sub name 1 (String.length name - 2) else name in
            let v = int_of_string (String.trim v) in Hashtbl.add tbl v name
        | _ -> ()) pairs;
      Some (fun br n -> let a = Array.make n Null in for i=0 to n-1 do
        let b = read_uint8_br br in
        let v = if b > 127 then b - 256 else b in
        let s = try Hashtbl.find tbl v with Not_found -> string_of_int v in
        a.(i) <- Enum8 (s, v)
      done; a)
  | _ when has_prefix s "enum16(" ->
      (* Parse enum16 mapping *)
      let inside = String.sub s 7 (String.length s - 8) in
      let pairs = String.split_on_char ',' inside |> List.filter (fun x -> String.trim x <> "") in
      let tbl = Hashtbl.create 32 in
      List.iter (fun p -> match String.split_on_char '=' p with
        | [name; v] ->
            let name = String.trim name in
            let name = if String.length name >= 2 && name.[0] = '\'' then String.sub name 1 (String.length name - 2) else name in
            let v = int_of_string (String.trim v) in Hashtbl.add tbl v name
        | _ -> ()) pairs;
      Some (fun br n -> let a = Array.make n Null in for i=0 to n-1 do
        let v32 = read_int32_le_br br in
        let v = Int32.to_int v32 in
        let s = try Hashtbl.find tbl v with Not_found -> string_of_int v in
        a.(i) <- Enum16 (s, v)
      done; a)
  | _ when has_prefix s "decimal" ->
      (* Decimal values are stored as integers. For Decimal(10,2) it's int64 *)
      Some (fun br n -> let a = Array.make n Null in for i=0 to n-1 do a.(i) <- Int64 (read_uint64_le_br br) done; a)
  | _ -> None
