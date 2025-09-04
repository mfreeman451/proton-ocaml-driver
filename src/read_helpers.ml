open Binary
open Errors

(* Mirrors proton_driver/readhelpers.read_exception *)
let rec read_exception ic : exn =
  let code   = Int32.to_int (Binary.read_int32_le ic) in
  let name   = Binary.read_str ic in
  let msg    = Binary.read_str ic in
  let stack  = Binary.read_str ic in
  let has_nested = (Binary.read_uint8 ic) <> 0 in
  let new_message =
    let base =
      (if name <> "DB::Exception" then name ^ ". " else "") ^
      msg ^ ". Stack trace:\n\n" ^ stack in
    base
  in
  let nested =
    if has_nested then
      (match read_exception ic with
       | Server_exception se -> Some se.message
       | e -> Some (Printexc.to_string e))
    else None
  in
  Server_exception { code; message = new_message; nested }
