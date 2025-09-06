open Proton
open Printf

let test_connection_debug () =
  printf "Creating client for 127.0.0.1:8463...\n%!";
  let settings = [
    ("send_logs_level", "none");
    ("log_profile_events", "0");
  ] in
  let client = Client.create ~host:"127.0.0.1" ~port:8463 ~settings () in
  printf "Client created successfully\n%!";
  
  printf "Attempting to execute simple query...\n%!";
  let timeout_promise = 
    let open Lwt.Syntax in
    let+ () = Lwt_unix.sleep 30.0 in
    printf "❌ Operation timed out after 30 seconds\n%!";
    `Timeout
  in
  
  let query_promise = 
    let open Lwt.Syntax in
    let* result = Client.execute client "SELECT 1" in
    printf "✅ Query executed successfully\n%!";
    Lwt.return (`Success result)
  in
  
  Lwt_main.run (
    let open Lwt.Syntax in
    let* result = Lwt.pick [timeout_promise; query_promise] in
    match result with
    | `Timeout -> 
        printf "Connection test timed out\n%!";
        Lwt.return ()
    | `Success query_result ->
        (match query_result with
        | Client.NoRows -> printf "Query returned no rows\n%!"
        | Client.Rows (rows, cols) -> 
            printf "Query returned %d rows with %d columns\n%!" 
              (List.length rows) (List.length cols);
            List.iteri (fun i (name, ty) -> printf "  col[%d]: name='%s' type='%s'\n%!" i name ty) cols;
            List.iteri (fun ri row ->
              let vs = List.map Column.value_to_string row in
              printf "  row[%d]: %s\n%!" ri (String.concat ", " vs)
            ) rows);
        Lwt.return ()
  )

let () = 
  try
    test_connection_debug ()
  with
  | e -> printf "❌ Exception: %s\n%!" (Printexc.to_string e)
