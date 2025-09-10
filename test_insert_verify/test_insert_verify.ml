open Proton
open Printf

let run () =
  printf "[insert_verify] Connecting to 127.0.0.1:8463...\n%!";
  let settings = [ ("send_logs_level", "none"); ("log_profile_events", "0") ] in
  let client = Client.create ~host:"127.0.0.1" ~port:8463 ~settings () in

  let table = "test_insert_verify" in
  let drop_table = sprintf "DROP TABLE IF EXISTS %s" table in
  let create_table = sprintf "CREATE TABLE %s (id int32, name string) ENGINE = Memory" table in
  let drop_stream = sprintf "DROP STREAM IF EXISTS %s" table in
  let create_stream = sprintf "CREATE STREAM %s (id int32, name string)" table in
  let insert = sprintf "INSERT INTO %s VALUES (1, 'one'), (2, 'two'), (3, 'three')" table in
  (* Unused earlier bounded queries removed for streaming-first demo *)

  let open Lwt.Syntax in
  let program =
    (* Try TABLE dialect first; fallback to STREAM if server rejects it. *)
    let ddl_mode_stream = ref false in
    let* _ =
      Lwt.catch
        (fun () -> Client.execute client drop_table)
        (fun _ex ->
          ddl_mode_stream := true;
          Client.execute client drop_stream)
    in
    if !ddl_mode_stream then printf "[insert_verify] Using STREAM dialect (dropped if existed)\n%!"
    else printf "[insert_verify] Using TABLE dialect (dropped if existed)\n%!";
    let* _ =
      if !ddl_mode_stream then Client.execute client create_stream
      else Client.execute client create_table
    in
    printf "[insert_verify] Created %s\n%!" (if !ddl_mode_stream then "stream" else "table");
    (* Skip DESCRIBE for simplicity in streaming demo *)
    (* Unbounded stream read: start reader, then perform inserts so reader catches them. *)
    let conn = Connection.create ~host:"127.0.0.1" ~port:8463 ~settings () in
    let query = Printf.sprintf "SELECT id, name FROM %s" table in
    let printed = ref 0 in
    let* () = Connection.send_query conn query in
    let rec loop () =
      if !printed >= 3 then Lwt.return_unit
      else
        let* pkt = Connection.receive_packet conn in
        match pkt with
        | Connection.PData b ->
            let rows = Block.get_rows b in
            List.iter
              (fun row ->
                if !printed < 3 then (
                  let s = String.concat ", " (List.map Column.value_to_string row) in
                  printf "[insert_verify] stream row: %s\n%!" s;
                  incr printed))
              rows;
            loop ()
        | Connection.PEndOfStream -> Lwt.return_unit
        | _ -> loop ()
    in
    let reader_task = Lwt.catch loop (fun _ -> Lwt.return_unit) in
    (* Let the reader subscribe *)
    let* () = Lwt_unix.sleep 0.2 in
    (* Perform inserts now *)
    let* _ =
      if !ddl_mode_stream then
        let* _ =
          Client.execute client
            (sprintf "INSERT INTO %s (id, name) SELECT to_int32(1), 'one'" table)
        in
        let* _ =
          Client.execute client
            (sprintf "INSERT INTO %s (id, name) SELECT to_int32(2), 'two'" table)
        in
        Client.execute client
          (sprintf "INSERT INTO %s (id, name) SELECT to_int32(3), 'three'" table)
      else Client.execute client insert
    in
    printf "[insert_verify] Inserted 3 rows (%s mode)\n%!"
      (if !ddl_mode_stream then "stream" else "table");
    let sleep_done =
      let+ () = Lwt_unix.sleep 5.0 in
      ()
    in
    let* _ = Lwt.pick [ reader_task; sleep_done ] in
    printf "[insert_verify] Printed %d row(s) from live stream\n%!" !printed;
    let* () = Connection.disconnect conn in
    if !printed = 0 then
      printf "[insert_verify] No rows observed from table(%s) within 3s\n%!" table;
    Lwt.return_unit
  in
  (* Ensure cleanup *)
  let finalize () =
    let open Lwt.Syntax in
    (* Try both cleanups, ignore errors. *)
    let* _ =
      Lwt.catch
        (fun () -> Client.execute client drop_table)
        (fun _ -> Lwt.return Proton.Client.NoRows)
    in
    let* _ =
      Lwt.catch
        (fun () -> Client.execute client drop_stream)
        (fun _ -> Lwt.return Proton.Client.NoRows)
    in
    printf "[insert_verify] Dropped table/stream\n%!";
    Lwt.return_unit
  in
  Lwt_main.run (Lwt.finalize (fun () -> program) finalize)

let () = try run () with e -> Printf.printf "❌ Exception: %s\n%!" (Printexc.to_string e)
