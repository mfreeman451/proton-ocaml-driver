let () =
  let open Proton in
  Lwt_main.run
  @@
  let open Lwt.Syntax in
  let client = Client.create ~host:"127.0.0.1" ~port:Defines.default_port () in
  let* res = Client.execute client "SELECT 1 AS x, 'hello' AS s, toFloat64(3.14) AS f" in
  match res with
  | Client.NoRows ->
      let* () = Lwt_io.printl "No rows returned." in
      Client.disconnect client
  | Client.Rows (rows, columns) ->
      let cols = String.concat ", " (List.map fst columns) in
      let* () = Lwt_io.printf "Column names: %s\n" cols in
      let* () =
        Lwt_list.iter_s
          (fun row ->
            let cells = row |> List.map Column.value_to_string |> String.concat " | " in
            Lwt_io.printf "Row: %s\n" cells)
          rows
      in
      Client.disconnect client
