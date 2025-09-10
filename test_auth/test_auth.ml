(* Authentication test - Tests both authenticated and non-authenticated connections *)
open Proton

let print_rows = function
  | Client.NoRows -> Printf.printf "  No rows returned\n"
  | Client.Rows (rows, _cols) ->
      List.iter
        (fun row ->
          let values = List.map Column.value_to_string row in
          Printf.printf "  Row: [%s]\n" (String.concat ", " values))
        rows

let test_connection_with_auth ~host ~port ?user ?password description =
  Printf.printf "\n🔐 %s\n" description;
  Printf.printf "%s\n" (String.make (String.length description + 4) '=');

  try
    Printf.printf "1. Creating client (%s:%d" host port;
    (match user with Some u -> Printf.printf ", user: %s" u | None -> Printf.printf ", no auth");
    Printf.printf ")...\n";

    let client = Client.create ~host ~port ?user ?password () in
    Printf.printf "   ✅ Client created\n";

    Printf.printf "2. Testing connectivity: SELECT 1...\n";
    let result =
      Lwt_main.run
        (Lwt.catch
           (fun () ->
             let open Lwt.Syntax in
             let+ r = Client.execute client "SELECT 1" in
             Some r)
           (fun e ->
             Printf.printf "   ❌ Query failed: %s\n" (Printexc.to_string e);
             Lwt.return None))
    in

    (match result with
    | None ->
        Printf.printf "   ❌ Connection test failed\n";
        false
    | Some rows ->
        Printf.printf "   ✅ Connection successful!\n";
        print_rows rows;
        true)
    (* Test user query if authenticated *)
    && (match user with
       | Some _ -> (
           Printf.printf "3. Testing user query: SELECT current_user()...\n";
           let user_result =
             Lwt_main.run
               (Lwt.catch
                  (fun () ->
                    let open Lwt.Syntax in
                    let+ r = Client.execute client "SELECT current_user()" in
                    Some r)
                  (fun e ->
                    Printf.printf "   ❌ User query failed: %s\n" (Printexc.to_string e);
                    Lwt.return None))
           in
           match user_result with
           | None ->
               Printf.printf "   ❌ User query failed\n";
               false
           | Some rows ->
               Printf.printf "   ✅ Current user query successful!\n";
               print_rows rows;
               true)
       | None ->
           Printf.printf "3. Skipping user query (no authentication)\n";
           true)
    &&
    (* Test basic data operations *)
    (Printf.printf "4. Testing table operations...\n";
     let ops_result =
       Lwt_main.run
         (Lwt.catch
            (fun () ->
              let open Lwt.Syntax in
              (* Drop existing test table *)
              let* _ = Client.execute client "DROP STREAM IF EXISTS test_auth_table" in
              Printf.printf "   ✅ Cleaned up existing table\n";

              (* Create test table *)
              let* _ =
                Client.execute client
                  "CREATE STREAM IF NOT EXISTS test_auth_table (id int32, name string)"
              in
              Printf.printf "   ✅ Created test table\n";

              (* Insert test data *)
              let* _ =
                Client.execute client "INSERT INTO test_auth_table (id, name) VALUES (1, 'test')"
              in
              Printf.printf "   ✅ Inserted test data\n";

              (* Query test data *)
              let* rows =
                Client.execute client "SELECT id, name FROM test_auth_table ORDER BY id"
              in
              Printf.printf "   ✅ Queried test data:\n";
              print_rows rows;

              (* Clean up *)
              let* _ = Client.execute client "DROP STREAM test_auth_table" in
              Printf.printf "   ✅ Cleaned up test table\n";

              Lwt.return true)
            (fun e ->
              Printf.printf "   ❌ Table operations failed: %s\n" (Printexc.to_string e);
              Lwt.return false))
     in
     ops_result)
  with e ->
    Printf.printf "❌ Failed to create client: %s\n" (Printexc.to_string e);
    false

let () =
  Printf.printf "🧪 Proton Authentication Tests\n";
  Printf.printf "==============================\n";

  let tests =
    [
      (* Test against authenticated Proton instance *)
      ( "localhost",
        8465,
        Some "proton_user",
        Some "proton_ocaml_test",
        "Testing authenticated connection (proton_user)" );
      (* Test readonly user *)
      ( "localhost",
        8465,
        Some "readonly_user",
        Some "readonly_test",
        "Testing read-only authenticated connection (readonly_user)" );
      (* Test default user without password *)
      ("localhost", 8465, Some "default", None, "Testing default user connection");
      (* Test no authentication *)
      ("localhost", 8465, None, None, "Testing anonymous connection");
    ]
  in

  let results =
    List.map
      (fun (host, port, user, password, desc) ->
        test_connection_with_auth ~host ~port ?user ?password desc)
      tests
  in

  let successful = List.fold_left (fun acc success -> if success then acc + 1 else acc) 0 results in
  let total = List.length results in

  Printf.printf "\n📊 Test Results Summary\n";
  Printf.printf "======================\n";
  Printf.printf "✅ Successful: %d/%d\n" successful total;
  Printf.printf "❌ Failed: %d/%d\n" (total - successful) total;

  if successful = total then (
    Printf.printf "\n🎉 All authentication tests passed!\n";
    exit 0)
  else (
    Printf.printf "\n💥 Some authentication tests failed!\n";
    exit 1)
