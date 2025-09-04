let () =
  let open Proton in
  let client = Client.create ~host:"127.0.0.1" ~port:Defines.default_port () in
  match Client.execute client "SELECT 1 AS x, 'hello' AS s, toFloat64(3.14) AS f" with
  | Client.NoRows ->
      print_endline "No rows returned."
  | Client.Rows (rows, columns) ->
      let cols = String.concat ", " (List.map fst columns) in
      Printf.printf "Columns: %s\n" cols;
      List.iter (fun row ->
        let cells = row |> List.map Columns.value_to_string |> String.concat " | " in
        Printf.printf "Row: %s\n" cells
      ) rows;
  Client.disconnect client
