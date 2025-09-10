(* TLS Example for Proton OCaml Driver *)

open Proton
open Lwt.Syntax

let tls_secure_example () =
  (* Example: Create a secure TLS connection with system CA trust *)
  let tls_config =
    {
      Connection.enable_tls = true;
      ca_cert_file = None;
      (* Use system trust store *)
      client_cert_file = None;
      client_key_file = None;
      verify_hostname = true;
      insecure_skip_verify = false;
    }
  in

  let client =
    Client.create ~host:"secure-proton-server.example.com" ~port:9440 (* TLS port *)
      ~tls_config ~compression:Compress.LZ4 ()
  in

  Printf.printf "Created secure TLS client for %s:%d\n" client.conn.host client.conn.port;

  (* Note: Actual connection would require a real server *)
  Printf.printf "TLS enabled: %b\n" client.conn.tls_config.enable_tls;
  Printf.printf "Hostname verification: %b\n" client.conn.tls_config.verify_hostname;
  Printf.printf "Compression: LZ4\n";

  Client.disconnect client

let mtls_example () =
  (* Example: Create mTLS connection with client certificates *)
  let mtls_config =
    {
      Connection.enable_tls = true;
      ca_cert_file = Some "/path/to/custom/ca.pem";
      client_cert_file = Some "/path/to/client.pem";
      client_key_file = Some "/path/to/client.key";
      verify_hostname = true;
      insecure_skip_verify = false;
    }
  in

  let client =
    Client.create ~host:"mtls-proton-server.example.com" ~port:9440 ~tls_config:mtls_config
      ~compression:Compress.LZ4 ()
  in

  Printf.printf "\nCreated mTLS client for %s:%d\n" client.conn.host client.conn.port;

  Printf.printf "Custom CA: %s\n"
    (match client.conn.tls_config.ca_cert_file with Some path -> path | None -> "System default");

  Printf.printf "Client cert: %s\n"
    (match client.conn.tls_config.client_cert_file with Some path -> path | None -> "None");

  Client.disconnect client

let insecure_dev_example () =
  (* Example: Insecure TLS for development/testing *)
  let dev_tls_config =
    {
      Connection.enable_tls = true;
      ca_cert_file = None;
      client_cert_file = None;
      client_key_file = None;
      verify_hostname = false;
      insecure_skip_verify = true;
      (* WARNING: Only for development! *)
    }
  in

  let client = Client.create ~host:"localhost" ~port:9440 ~tls_config:dev_tls_config () in

  Printf.printf "\nCreated insecure TLS client for development\n";
  Printf.printf "⚠️  WARNING: This configuration skips certificate verification!\n";
  Printf.printf "⚠️  Only use for development/testing environments!\n";

  Client.disconnect client

let () =
  Lwt_main.run
    (let* () = tls_secure_example () in
     let* () = mtls_example () in
     let* () = insecure_dev_example () in
     Printf.printf "\n✅ TLS examples completed successfully!\n";
     Lwt.return_unit)
