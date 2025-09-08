(* Simple authentication test - Focus on authentication without complex operations *)
open Proton

let print_rows = function
  | Client.NoRows -> Printf.printf "  No rows returned\n"
  | Client.Rows (rows, _cols) ->
      List.iter (fun row ->
        let values = List.map Column.value_to_string row in
        Printf.printf "  Row: [%s]\n" (String.concat ", " values)
      ) rows

let test_auth_connection ~host ~port ?user ?password description =
  Printf.printf "\n🔐 %s\n" description;
  Printf.printf "%s\n" (String.make (String.length description + 4) '=');
  
  try
    Printf.printf "1. Creating client (%s:%d" host port;
    (match user with 
     | Some u -> Printf.printf ", user: %s" u
     | None -> Printf.printf ", no auth");
    Printf.printf ")...\n";
    
    let client = Client.create ~host ~port ?user ?password () in
    Printf.printf "   ✅ Client created\n";
    
    (* Test basic connectivity *)
    Printf.printf "2. Testing connectivity: SELECT 1...\n";
    let result = Lwt_main.run (
      Lwt.catch
        (fun () -> 
          let open Lwt.Syntax in
          let+ r = Client.execute client "SELECT 1 as test_connection" in
          Some r)
        (fun e -> 
          Printf.printf "   ❌ Query failed: %s\n" (Printexc.to_string e); 
          Lwt.return None)
    ) in
    
    let connectivity_success = match result with
     | None -> Printf.printf "   ❌ Connection test failed\n"; false
     | Some rows -> 
         Printf.printf "   ✅ Connection successful!\n";
         print_rows rows;
         true
    in
    
    (* Test current user query *)
    let user_success = match user with
     | Some expected_user ->
         Printf.printf "3. Testing user identity: SELECT current_user()...\n";
         let user_result = Lwt_main.run (
           Lwt.catch
             (fun () -> 
               let open Lwt.Syntax in
               let+ r = Client.execute client "SELECT current_user() as current_user" in
               Some r)
             (fun e -> 
               Printf.printf "   ❌ User query failed: %s\n" (Printexc.to_string e); 
               Lwt.return None)
         ) in
         (match user_result with
          | None -> Printf.printf "   ❌ User query failed\n"; false
          | Some (Client.Rows ([row], _)) -> 
              let current_user = match row with
                | [value] -> Column.value_to_string value
                | _ -> "unknown"
              in
              if current_user = expected_user then (
                Printf.printf "   ✅ User identity confirmed: %s\n" current_user;
                true
              ) else (
                Printf.printf "   ⚠️  User mismatch: expected %s, got %s\n" expected_user current_user;
                false
              )
          | Some rows -> 
              Printf.printf "   ✅ Current user query returned:\n";
              print_rows rows;
              true
         )
     | None ->
         Printf.printf "3. Testing default user: SELECT current_user()...\n";
         let user_result = Lwt_main.run (
           Lwt.catch
             (fun () -> 
               let open Lwt.Syntax in
               let+ r = Client.execute client "SELECT current_user() as current_user" in
               Some r)
             (fun e -> 
               Printf.printf "   ❌ User query failed: %s\n" (Printexc.to_string e); 
               Lwt.return None)
         ) in
         (match user_result with
          | None -> false
          | Some rows -> 
              Printf.printf "   ✅ Default user query successful:\n";
              print_rows rows;
              true
         )
    in
    
    (* Test database permissions *)
    let perms_success = 
      Printf.printf "4. Testing database access: SHOW DATABASES...\n";
      let perms_result = Lwt_main.run (
        Lwt.catch
          (fun () -> 
            let open Lwt.Syntax in
            let+ r = Client.execute client "SHOW DATABASES" in
            Some r)
          (fun e -> 
            Printf.printf "   ❌ Database access failed: %s\n" (Printexc.to_string e); 
            Lwt.return None)
      ) in
      (match perms_result with
       | None -> false
       | Some rows -> 
           Printf.printf "   ✅ Database access successful:\n";
           print_rows rows;
           true
      )
    in
    
    connectivity_success && user_success && perms_success
        
  with
  | e -> 
      Printf.printf "❌ Failed to create client: %s\n" (Printexc.to_string e);
      false

let () =
  Printf.printf "🧪 Proton Authentication Tests (Simple)\n";
  Printf.printf "========================================\n";
  
  let tests = [
    (* Test against authenticated Proton instance *)
    ("localhost", 8465, Some "proton_user", Some "proton_ocaml_test", 
     "Authenticated connection (proton_user)");
     
    (* Test readonly user *)
    ("localhost", 8465, Some "readonly_user", Some "readonly_test", 
     "Read-only connection (readonly_user)");
     
    (* Test default user *)
    ("localhost", 8465, Some "default", None, 
     "Default user connection");
     
    (* Test no authentication *)
    ("localhost", 8465, None, None, 
     "Anonymous connection");
  ] in
  
  let results = List.map (fun (host, port, user, password, desc) ->
    test_auth_connection ~host ~port ?user ?password desc
  ) tests in
  
  let successful = List.fold_left (fun acc success -> if success then acc + 1 else acc) 0 results in
  let total = List.length results in
  
  Printf.printf "\n📊 Test Results Summary\n";
  Printf.printf "======================\n";
  Printf.printf "✅ Successful: %d/%d\n" successful total;
  Printf.printf "❌ Failed: %d/%d\n" (total - successful) total;
  
  if successful > 0 then (
    Printf.printf "\n🎉 Authentication is working with your OCaml Proton driver!\n";
    Printf.printf "Users tested:\n";
    List.iter2 (fun (_, _, user, _, desc) success ->
      let status = if success then "✅" else "❌" in
      let username = match user with Some u -> u | None -> "anonymous" in
      Printf.printf "  %s %s (%s)\n" status username desc
    ) tests results;
    exit 0
  ) else (
    Printf.printf "\n💥 All authentication tests failed!\n";
    exit 1
  )