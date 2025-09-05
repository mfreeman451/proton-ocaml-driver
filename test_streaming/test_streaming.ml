open Proton
open Printf

let run () =
  printf "[streaming] Connecting to 127.0.0.1:8463...\n%!";
  let settings = [
    ("send_logs_level", "none");
    ("log_profile_events", "0");
  ] in
  let client = Client.create ~host:"127.0.0.1" ~port:8463 ~settings () in

  let stream = "test_streaming" in
  let drop_stream = sprintf "DROP STREAM IF EXISTS %s" stream in
  let create_stream = sprintf "CREATE STREAM %s (id int32, name string)" stream in

  let open Lwt.Syntax in
  let program =
    (* Recreate stream *)
    let* _ = Lwt.catch (fun () -> Client.execute client drop_stream) (fun _ -> Lwt.return Client.NoRows) in
    let* _ = Client.execute client create_stream in
    printf "[streaming] Created stream '%s'\n%!" stream;

    (* Start streaming reader: unbounded select that prints first N rows *)
    let target = 5 in
    let printed = ref 0 in
    let conn = Connection.create ~host:"127.0.0.1" ~port:8463 ~settings () in
    let query = sprintf "SELECT id, name FROM %s" stream in
    let* () = Connection.send_query conn query in
    printf "[streaming] Reader subscribed, waiting for rows...\n%!";

    let open Lwt.Infix in
    let rec reader_loop () =
      if !printed >= target then Lwt.return_unit else
      Connection.receive_packet conn >>= function
      | Connection.PData b ->
          let rows = Block.get_rows b in
          List.iter (fun row ->
            if !printed < target then (
              let s = String.concat ", " (List.map Columns.value_to_string row) in
              printf "[streaming] row: %s\n%!" s;
              incr printed
            )
          ) rows;
          reader_loop ()
      | Connection.PEndOfStream -> Lwt.return_unit
      | _ -> reader_loop ()
    in
    let reader = Lwt.catch reader_loop (fun _ -> Lwt.return_unit) in

    (* Insert rows with small delays so reader picks them up live *)
    let* () = Lwt_unix.sleep 0.2 in
    let insert_one i name =
      Client.execute client (sprintf "INSERT INTO %s (id, name) SELECT to_int32(%d), '%s'" stream i name) >|= fun _ -> ()
    in
    let* () = insert_one 1 "one" in
    let* () = Lwt_unix.sleep 0.15 in
    let* () = insert_one 2 "two" in
    let* () = Lwt_unix.sleep 0.15 in
    let* () = insert_one 3 "three" in
    let* () = Lwt_unix.sleep 0.15 in
    let* () = insert_one 4 "four" in
    let* () = Lwt_unix.sleep 0.15 in
    let* () = insert_one 5 "five" in
    printf "[streaming] Inserted 5 rows\n%!";

    (* Wait up to 5s for reader to print target rows *)
    let timeout = Lwt_unix.sleep 5.0 >|= fun () -> `Timeout in
    let* _ = Lwt.pick [ (reader >|= fun () -> `Done); timeout ] in
    printf "[streaming] Printed %d/%d rows\n%!" !printed target;
    let* () = Connection.disconnect conn in

    (* Clean up *)
    let* _ = Client.execute client drop_stream in
    printf "[streaming] Dropped stream\n%!";
    Lwt.return_unit
  in
  Lwt_main.run program

let () =
  try run () with
  | e -> Printf.printf "❌ Exception: %s\n%!" (Printexc.to_string e)

