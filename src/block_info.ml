type t = {
  mutable is_overflows : bool;
  mutable bucket_num : int;
}

let read ic : t =
  let bi = { is_overflows=false; bucket_num = -1 } in
  let rec loop () =
    let field = Int64.to_int (Varint.read_varint ic) in
    match field with
    | 0 -> ()
    | 1 -> bi.is_overflows <- (Binary.read_uint8 ic) <> 0; loop ()
    | 2 ->
        (* int32 little endian *)
        let n = Int32.to_int (Binary.read_int32_le ic) in
        bi.bucket_num <- n; loop ()
    | _ ->
        (* ignore unknown *)
        loop ()
  in
  loop ();
  bi

let read_br br : t =
  let bi = { is_overflows=false; bucket_num = -1 } in
  let rec loop () =
    let field = Binary.read_varint_int_br br in
    match field with
    | 0 -> ()
    | 1 -> bi.is_overflows <- (Binary.read_uint8_br br) <> 0; loop ()
    | 2 ->
        let n = Int32.to_int (Binary.read_int32_le_br br) in
        bi.bucket_num <- n; loop ()
    | _ -> loop ()
  in
  loop ();
  bi
