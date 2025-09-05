let () =
  let open Proton in
  Lwt_main.run @@ let open Lwt.Infix in
  let client = Client.create ~host:"127.0.0.1" ~port:Defines.default_port () in
  Client.execute client "SELECT 1 AS x, 'hello' AS s, toFloat64(3.14) AS f" >>= function
  | Client.NoRows ->
      Lwt_io.printl "No rows returned." >>= fun () ->
      Client.disconnect client
  | Client.Rows (rows, columns) ->
      let cols = String.concat ", " (List.map fst columns) in
      Lwt_io.printf "Columns: %s\n" cols >>= fun () ->
      Lwt_list.iter_s (fun row ->
        let cells = row |> List.map Columns.value_to_string |> String.concat " | " in
        Lwt_io.printf "Row: %s\n" cells
      ) rows >>= fun () ->
      Client.disconnect client
