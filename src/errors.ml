exception Network_error of string
exception Socket_timeout
exception Unexpected_packet of string
exception Unknown_packet of int
exception Server_exception of { code : int; message : string; nested : string option }

let () =
  Printexc.register_printer (function
    | Network_error s -> Some ("Network_error: " ^ s)
    | Unexpected_packet s -> Some ("Unexpected_packet: " ^ s)
    | Unknown_packet n -> Some (Printf.sprintf "Unknown_packet: %d" n)
    | Server_exception { code; message; nested } ->
        Some
          (Printf.sprintf "Server exception (code %d): %s%s" code message
             (match nested with None -> "" | Some s -> "\nNested: " ^ s))
    | _ -> None)
