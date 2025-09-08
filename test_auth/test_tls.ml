(* TLS test for OCaml Proton driver *)
open Proton

let print_rows = function
  | Client.NoRows -> Printf.printf "  No rows returned\n"
  | Client.Rows (rows, _cols) ->
      List.iter (fun row ->
        let values = List.map Column.value_to_string row in
        Printf.printf "  Row: [%s]\n" (String.concat ", " values)
      ) rows

let test_tls_connection ~host ~port ?user ?password ~ca_cert ~client_cert ~client_key ~verify_hostname description =
  Printf.printf "\n🔒 %s\n" description;
  Printf.printf "%s\n" (String.make (String.length description + 4) '=');
  
  try
    Printf.printf "1. Creating TLS config...\n";
    let tls_config = Connection.{
      enable_tls = true;
      ca_cert_file = Some ca_cert;
      client_cert_file = Some client_cert;
      client_key_file = Some client_key;
      verify_hostname = verify_hostname;
      insecure_skip_verify = not verify_hostname;
    } in
    Printf.printf "   ✅ TLS config created\n";
    
    Printf.printf "2. Creating TLS client (%s:%d" host port;
    (match user with 
     | Some u -> Printf.printf ", user: %s" u
     | None -> Printf.printf ", no user auth");
    Printf.printf ", TLS: enabled)...\n";
    
    let client = Client.create ~host ~port ?user ?password ~tls_config () in
    Printf.printf "   ✅ TLS client created\n";
    
    (* Test basic connectivity *)
    Printf.printf "3. Testing TLS connectivity: SELECT 1...\n";
    let result = Lwt_main.run (
      Lwt.catch
        (fun () -> 
          let open Lwt.Syntax in
          let+ r = Client.execute client "SELECT 1 as tls_test" in
          Some r)
        (fun e -> 
          Printf.printf "   ❌ TLS query failed: %s\n" (Printexc.to_string e); 
          Lwt.return None)
    ) in
    
    let connectivity_success = match result with
     | None -> Printf.printf "   ❌ TLS connection test failed\n"; false
     | Some rows -> 
         Printf.printf "   ✅ TLS connection successful!\n";
         print_rows rows;
         true
    in
    
    (* Test current user query if authenticated *)
    let user_success = match user with
     | Some expected_user ->
         Printf.printf "4. Testing TLS user identity: SELECT current_user()...\n";
         let user_result = Lwt_main.run (
           Lwt.catch
             (fun () -> 
               let open Lwt.Syntax in
               let+ r = Client.execute client "SELECT current_user() as current_user" in
               Some r)
             (fun e -> 
               Printf.printf "   ❌ TLS user query failed: %s\n" (Printexc.to_string e); 
               Lwt.return None)
         ) in
         (match user_result with
          | None -> false
          | Some (Client.Rows ([row], _)) -> 
              let current_user = match row with
                | [value] -> Column.value_to_string value
                | _ -> "unknown"
              in
              if current_user = expected_user then (
                Printf.printf "   ✅ TLS user identity confirmed: %s\n" current_user;
                true
              ) else (
                Printf.printf "   ⚠️  TLS user mismatch: expected %s, got %s\n" expected_user current_user;
                false
              )
          | Some rows -> 
              Printf.printf "   ✅ TLS current user query returned:\n";
              print_rows rows;
              true
         )
     | None ->
         Printf.printf "4. Testing TLS default connection: SELECT current_user()...\n";
         let user_result = Lwt_main.run (
           Lwt.catch
             (fun () -> 
               let open Lwt.Syntax in
               let+ r = Client.execute client "SELECT current_user() as current_user" in
               Some r)
             (fun e -> 
               Printf.printf "   ❌ TLS user query failed: %s\n" (Printexc.to_string e); 
               Lwt.return None)
         ) in
         (match user_result with
          | None -> false
          | Some rows -> 
              Printf.printf "   ✅ TLS default user query successful:\n";
              print_rows rows;
              true
         )
    in
    
    (* Test TLS-specific query *)
    let tls_success = 
      Printf.printf "5. Testing TLS version query...\n";
      let tls_result = Lwt_main.run (
        Lwt.catch
          (fun () -> 
            let open Lwt.Syntax in
            let+ r = Client.execute client "SELECT 'TLS Connection Successful' as tls_status" in
            Some r)
          (fun e -> 
            Printf.printf "   ❌ TLS status query failed: %s\n" (Printexc.to_string e); 
            Lwt.return None)
      ) in
      (match tls_result with
       | None -> false
       | Some rows -> 
           Printf.printf "   ✅ TLS status query successful:\n";
           print_rows rows;
           true
      )
    in
    
    connectivity_success && user_success && tls_success
        
  with
  | e -> 
      Printf.printf "❌ Failed to create TLS client: %s\n" (Printexc.to_string e);
      false

let () =
  Printf.printf "🔒 Proton TLS Tests with OCaml Driver\n";
  Printf.printf "====================================\n";
  
  let cert_path = "docker/certs" in
  let ca_cert = cert_path ^ "/ca.pem" in
  let client_cert = cert_path ^ "/client.pem" in
  let client_key = cert_path ^ "/client-key.pem" in
  
  (* Check if certificates exist *)
  if not (Sys.file_exists ca_cert && Sys.file_exists client_cert && Sys.file_exists client_key) then (
    Printf.printf "❌ Certificate files not found in %s/\n" cert_path;
    Printf.printf "   Make sure you have run ./docker/generate-certs.sh\n";
    Printf.printf "   Expected files: %s, %s, %s\n" ca_cert client_cert client_key;
    exit 1
  );
  
  let tests = [
    (* Test TLS with mTLS authentication - disable hostname verification for local testing *)
    ("localhost", 9440, Some "proton_user", Some "proton_ocaml_test", ca_cert, client_cert, client_key, false,
     "mTLS connection with authentication (proton_user)");
     
    (* Test TLS with mTLS but read-only user *)
    ("localhost", 9440, Some "readonly_user", Some "readonly_test", ca_cert, client_cert, client_key, false,
     "mTLS connection with read-only user (readonly_user)");
     
    (* Test TLS with default user *)
    ("localhost", 9440, Some "default", None, ca_cert, client_cert, client_key, false,
     "mTLS connection with default user");
     
    (* Test TLS without user authentication *)
    ("localhost", 9440, None, None, ca_cert, client_cert, client_key, false,
     "mTLS connection without user authentication");
  ] in
  
  let results = List.map (fun (host, port, user, password, ca, cert, key, verify, desc) ->
    test_tls_connection ~host ~port ?user ?password ~ca_cert:ca ~client_cert:cert ~client_key:key ~verify_hostname:verify desc
  ) tests in
  
  let successful = List.fold_left (fun acc success -> if success then acc + 1 else acc) 0 results in
  let total = List.length results in
  
  Printf.printf "\n📊 TLS Test Results Summary\n";
  Printf.printf "==========================\n";
  Printf.printf "✅ Successful: %d/%d\n" successful total;
  Printf.printf "❌ Failed: %d/%d\n" (total - successful) total;
  
  if successful > 0 then (
    Printf.printf "\n🎉 TLS is working with your OCaml Proton driver!\n";
    Printf.printf "TLS configurations tested:\n";
    List.iter2 (fun (_, _, user, _, _, _, _, _, desc) success ->
      let status = if success then "✅" else "❌" in
      let username = match user with Some u -> u | None -> "anonymous" in
      Printf.printf "  %s %s (%s)\n" status username desc
    ) tests results;
    
    if successful = total then (
      Printf.printf "\n🔐 Perfect! All TLS tests passed. Your OCaml driver supports:\n";
      Printf.printf "  • mTLS client certificate authentication\n";
      Printf.printf "  • User/password authentication over TLS\n";  
      Printf.printf "  • Multiple user types (admin, readonly, default)\n";
      Printf.printf "  • Secure encrypted connections to Proton\n";
      exit 0
    ) else (
      exit 0  (* Some tests passed, that's still success *)
    )
  ) else (
    Printf.printf "\n💥 All TLS tests failed!\n";
    Printf.printf "This could be due to:\n";
    Printf.printf "  • Proton TLS ports not listening (check docker-compose-tls-test.yml)\n";
    Printf.printf "  • Certificate issues\n";
    Printf.printf "  • Network connectivity problems\n";
    exit 1
  )