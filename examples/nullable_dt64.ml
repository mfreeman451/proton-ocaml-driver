open Proton

let () =
  Lwt_main.run
  @@
  let open Lwt.Syntax in
  let client = Client.create ~host:"127.0.0.1" ~port:Defines.default_port () in
  let* res = Client.execute client "SELECT CAST(NULL AS nullable(datetime64(3))) AS x" in
  let* () =
    match res with
    | Client.NoRows -> Lwt_io.printl "No rows returned."
    | Client.Rows (_rows, columns) ->
        let cols = columns |> List.map (fun (n, t) -> n ^ ":" ^ t) |> String.concat ", " in
        Lwt_io.printf "Columns: %s\n" cols
  in
  Client.disconnect client
