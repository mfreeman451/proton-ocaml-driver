open Proton

let () =
  Printf.printf "🔌 Minimal Connection Test\n";
  Printf.printf "=========================\n\n";
  
  try
    Printf.printf "1. Creating client with 127.0.0.1:8463...\n";
    let client = Client.create ~host:"127.0.0.1" ~port:8463 () in
    Printf.printf "   ✅ Client created\n";
    
    Printf.printf "2. Testing simple query: SELECT 1...\n";
    let result = Lwt_main.run (
      Lwt.catch
        (fun () -> 
          let open Lwt.Syntax in
          let+ r = Client.execute client "SELECT 1" in
          Some r)
        (fun e -> Printf.printf "   Query failed: %s\n" (Printexc.to_string e); Lwt.return None)
    ) in
    
    match result with
    | None -> Printf.printf "   ❌ Query failed\n"
    | Some (Client.NoRows) -> Printf.printf "   ⚠️  Query returned no rows\n"
    | Some (Client.Rows (rows, _)) -> 
        Printf.printf "   ✅ Query successful! Got %d rows\n" (List.length rows);
        List.iter (fun row ->
          let values = List.map Column.value_to_string row in
          Printf.printf "      Row: [%s]\n" (String.concat ", " values)
        ) rows
        
  with
  | e -> Printf.printf "❌ Failed: %s\n" (Printexc.to_string e)
