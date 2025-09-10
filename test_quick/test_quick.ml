open Proton

let () =
  Printf.printf "Quick connection test...\n";
  try
    let client = Client.create ~host:"127.0.0.1" ~port:8463 () in
    Printf.printf "✓ Client created successfully\n";

    Lwt_main.run
      (let open Lwt.Syntax in
       let* result = Client.execute client "SELECT 1 as test, version()" in
       match result with
       | Client.NoRows ->
           Printf.printf "No rows returned\n";
           Lwt.return_unit
       | Client.Rows (rows, cols) ->
           Printf.printf "✓ Query executed! Got %d rows, %d columns\n" (List.length rows)
             (List.length cols);
           List.iter
             (fun row ->
               let values = List.map Column.value_to_string row in
               Printf.printf "  Values: [%s]\n" (String.concat ", " values))
             rows;
           Lwt.return_unit)
  with e -> Printf.printf "Error: %s\n" (Printexc.to_string e)
