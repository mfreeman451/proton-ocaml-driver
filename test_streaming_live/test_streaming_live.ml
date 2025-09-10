open Proton
open Printf

let random_name () =
  let n = Random.bits () land 0xFFFFF in
  Printf.sprintf "name_%x" n

let run () =
  Random.self_init ();
  printf "[live] Connecting to 127.0.0.1:8463...\n%!";
  let settings = [ ("send_logs_level", "none"); ("log_profile_events", "0") ] in
  let client = Client.create ~host:"127.0.0.1" ~port:8463 ~settings () in

  let stream = "test_streaming_live" in
  let drop_stream = Printf.sprintf "DROP STREAM IF EXISTS %s" stream in
  let create_stream = Printf.sprintf "CREATE STREAM %s (id int32, name string)" stream in

  let open Lwt.Syntax in
  let program =
    (* Recreate stream *)
    let* _ =
      Lwt.catch (fun () -> Client.execute client drop_stream) (fun _ -> Lwt.return Client.NoRows)
    in
    let* _ = Client.execute client create_stream in
    printf "[live] Created stream '%s'\n%!" stream;

    (* Reader connection and task *)
    let conn = Connection.create ~host:"127.0.0.1" ~port:8463 ~settings () in
    let query = Printf.sprintf "SELECT id, name FROM %s" stream in
    let* () = Connection.send_query conn query in
    printf "[live] Reader subscribed, streaming rows... (Ctrl-C to stop)\n%!";

    let rec reader_loop () =
      let* pkt = Connection.receive_packet conn in
      match pkt with
      | Connection.PData b ->
          let rows = Block.get_rows b in
          List.iter
            (fun row ->
              let s = String.concat ", " (List.map Column.value_to_string row) in
              printf "[live] row: %s\n%!" s)
            rows;
          reader_loop ()
      | Connection.PEndOfStream ->
          (* For unbounded streams we don't expect this, but just resubscribe. *)
          printf "[live] end-of-stream, resubscribing...\n%!";
          let* () = Connection.send_query conn query in
          reader_loop ()
      | _ -> reader_loop ()
    in

    (* Inserter task: periodically insert random rows forever *)
    let rec inserter_loop () =
      let id = Random.int 1_000_000 in
      let name = random_name () in
      let sql =
        Printf.sprintf "INSERT INTO %s (id, name) SELECT to_int32(%d), '%s'" stream id name
      in
      let* () =
        Lwt.catch
          (fun () ->
            let* _ = Client.execute client sql in
            printf "[live] insert: (%d, %s)\n%!" id name;
            Lwt.return_unit)
          (fun ex ->
            printf "[live] insert error: %s\n%!" (Printexc.to_string ex);
            Lwt.return_unit)
      in
      let* () = Lwt_unix.sleep 0.5 in
      inserter_loop ()
    in

    (* SIGINT -> stop *)
    let stop_t, stop_w = Lwt.wait () in
    let _sigint = Lwt_unix.on_signal Sys.sigint (fun _ -> Lwt.wakeup_later stop_w ()) in

    let reader = Lwt.catch reader_loop (fun _ -> Lwt.return_unit) in
    let inserter = Lwt.catch inserter_loop (fun _ -> Lwt.return_unit) in

    let* _ = Lwt.pick [ reader; inserter; stop_t ] in

    printf "[live] Shutting down...\n%!";
    let* () = Connection.disconnect conn in
    let* _ =
      Lwt.catch (fun () -> Client.execute client drop_stream) (fun _ -> Lwt.return Client.NoRows)
    in
    printf "[live] Dropped stream and exit.\n%!";
    Lwt.return_unit
  in
  Lwt_main.run program

let () = try run () with e -> Printf.printf "❌ Exception: %s\n%!" (Printexc.to_string e)
