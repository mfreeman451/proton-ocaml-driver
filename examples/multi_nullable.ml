open Proton

let () =
  Lwt_main.run
  @@
  let open Lwt.Syntax in
  let client = Client.create ~host:"127.0.0.1" ~port:Defines.default_port () in
  let sql =
    "SELECT cast('x' as nullable(string)) as service_type, cast('y' as nullable(string)) as \
     service_status, cast(now64(3) as nullable(datetime64(3))) as last_heartbeat"
  in
  let* res = Client.execute client sql in
  let* () =
    match res with
    | Client.NoRows -> Lwt_io.printl "No rows returned."
    | Client.Rows (rows, columns) ->
        let cols = columns |> List.map (fun (n, t) -> n ^ ":" ^ t) |> String.concat ", " in
        let row_strs =
          rows
          |> List.map (fun r -> r |> List.map Column.value_to_string |> String.concat ", ")
          |> String.concat "\n"
        in
        let* () = Lwt_io.printf "Columns: %s\n" cols in
        Lwt_io.printf "Rows:\n%s\n" row_strs
  in
  Client.disconnect client
