open Protocol
open Errors
open Context
open Block
open Lwt.Syntax

(* Small inlineable helper to avoid substring allocation in hot paths *)
let[@inline] has_prefix (s:string) (p:string) : bool =
  let ls = String.length s and lp = String.length p in
  if ls < lp then false else (
    let rec loop i = if i = lp then true else if s.[i] <> p.[i] then false else loop (i+1) in
    loop 0)

type tls_config = {
  enable_tls: bool;
  ca_cert_file: string option;
  client_cert_file: string option;
  client_key_file: string option;
  verify_hostname: bool;
  insecure_skip_verify: bool;
}

type server_info = Context.server_info

type packet =
  | PData of Block.t
  | PProgress
  | PProfileInfo
  | PTotals of Block.t
  | PExtremes of Block.t
  | PLog of Block.t
  | PEndOfStream

type t = {
  host : string;
  port : int;
  database : string;
  user : string;
  password : string;
  tls_config : tls_config;
  mutable fd : Lwt_unix.file_descr option;
  mutable tls : Tls_lwt.Unix.t option;
  mutable connected : bool;
  mutable srv : server_info option;
  ctx : Context.t;
  compression : Protocol.compression;
}

(* Ensure full writes on non-TLS sockets. Lwt_unix.write_string may write
   fewer bytes than requested; loop until everything is sent. *)
let rec write_all fd s off len =
  if len = 0 then Lwt.return_unit
  else
    let* n = Lwt_unix.write_string fd s off len in
    if n = 0 then Lwt.fail End_of_file
    else write_all fd s (off + n) (len - n)

let writev_all fd ss =
  Lwt_list.iter_s (fun s -> write_all fd s 0 (String.length s)) ss

let default_tls_config = {
  enable_tls = false;  (* Changed to false by default for testing *)
  ca_cert_file = None;
  client_cert_file = None;
  client_key_file = None;
  verify_hostname = true;
  insecure_skip_verify = false;
}

let create ?(host="127.0.0.1") ?(port=Defines.default_port)
           ?(database=Defines.default_database)
           ?(user=Defines.default_user)
           ?(password=Defines.default_password)
           ?(tls_config=default_tls_config)
           ?(compression=Compress.LZ4)
           ?(settings=[]) () =
  let ctx = Context.make () in
  ctx.settings <- settings;
  {
    host; port; database; user; password;
    tls_config;
    fd=None; tls=None; connected=false; srv=None;
    ctx;
    compression;
  }

let env_debug_enabled () =
  match Sys.getenv_opt "PROTON_DEBUG" with
  | Some ("1" | "true" | "TRUE" | "yes" | "YES") -> true
  | _ -> false

let debugv f = if env_debug_enabled () then f () else ()

let hex_of_string (s:string) : string =
  let len = String.length s in
  let out = Bytes.create (2 * len) in
  let hexdig = "0123456789abcdef" in
  for i = 0 to len - 1 do
    let v = Char.code (String.unsafe_get s i) in
    Bytes.set out (2 * i)     (String.unsafe_get hexdig ((v lsr 4) land 0xF));
    Bytes.set out (2 * i + 1) (String.unsafe_get hexdig (v land 0xF))
  done;
  Bytes.unsafe_to_string out

let read_file path =
  let ic = open_in_bin path in
  let len = in_channel_length ic in
  let s = really_input_string ic len in
  close_in ic; s

let lwt_read_exact read_fn len =
  let buf = Bytes.create len in
  let rec loop off =
    if off = len then Lwt.return buf
    else
      let* n = read_fn buf off (len - off) in
      if n = 0 then Lwt.fail End_of_file else loop (off + n)
  in
  loop 0

let read_byte read_fn =
  let+ b = lwt_read_exact read_fn 1 in
  Char.code (Bytes.get b 0)

let read_varint_int_lwt read_fn =
  let rec loop shift acc =
    let* b = read_byte read_fn in
    let v = acc lor ((b land 0x7f) lsl shift) in
    if (b land 0x80) <> 0 then loop (shift + 7) v else Lwt.return v
  in loop 0 0

let read_int32_le_lwt read_fn =
  let+ buf = lwt_read_exact read_fn 4 in
  let a = Char.code (Bytes.get buf 0) in
  let b = Char.code (Bytes.get buf 1) in
  let c = Char.code (Bytes.get buf 2) in
  let d = Char.code (Bytes.get buf 3) in
  Int32.logor
    (Int32.of_int a)
    (Int32.logor
       (Int32.shift_left (Int32.of_int b) 8)
       (Int32.logor
          (Int32.shift_left (Int32.of_int c) 16)
          (Int32.shift_left (Int32.of_int d) 24)))

let read_uint64_le_lwt read_fn =
  let+ buf = lwt_read_exact read_fn 8 in
  let open Int64 in
  let b i = Int64.of_int (Char.code (Bytes.get buf i)) in
  logor (b 0)
    (logor (shift_left (b 1) 8)
       (logor (shift_left (b 2) 16)
          (logor (shift_left (b 3) 24)
             (logor (shift_left (b 4) 32)
                (logor (shift_left (b 5) 40)
                   (logor (shift_left (b 6) 48)
                      (shift_left (b 7) 56)))))))

let[@warning "-32"] read_float64_le_lwt read_fn =
  let+ u = read_uint64_le_lwt read_fn in
  Int64.float_of_bits u

let read_str_lwt read_fn =
  let* len = read_varint_int_lwt read_fn in
  if len = 0 then Lwt.return ""
  else
    let+ b = lwt_read_exact read_fn len in
    Bytes.to_string b

let write_buf writev_fn (buf:Buffer.t) =
  let s = Buffer.contents buf in
  writev_fn [s]

let write_varint_int_to_buffer buf n =
  let rec loop n =
    let x = n land 0x7f in
    let n' = n lsr 7 in
    if n' = 0 then Buffer.add_char buf (Char.chr x)
    else (Buffer.add_char buf (Char.chr (x lor 0x80)); loop n')
  in loop n

let connect t =
  if t.connected then Lwt.return_unit else
  let fd = Lwt_unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
  (* Try to parse as IP address first, if that fails, resolve as hostname *)
  let get_addr host =
    try Lwt.return (Unix.inet_addr_of_string host)
    with Invalid_argument _ | Failure _ ->
      (* It's not an IP address, try to resolve as hostname *)
      let* host_entry = Lwt_unix.gethostbyname host in
      if Array.length host_entry.Unix.h_addr_list > 0 then
        Lwt.return host_entry.Unix.h_addr_list.(0)
      else
        Lwt.fail (Failure ("Cannot resolve host: " ^ host))
  in
  let* addr = get_addr t.host in
  let sockaddr = Unix.ADDR_INET (addr, t.port) in
  debugv (fun () -> Printf.printf "[proton] connect: %s:%d (addr=%s)\n" t.host t.port (Unix.string_of_inet_addr addr));
  let* () = Lwt_unix.with_timeout Defines.dbms_default_connect_timeout_sec (fun () -> Lwt_unix.connect fd sockaddr) in
  debugv (fun () -> Printf.printf "[proton] connect: socket connected\n");
  let* res = (if t.tls_config.enable_tls then
     let open Tls in
     let authenticator =
       if t.tls_config.insecure_skip_verify then (fun ?ip:_ ~host:_ _ -> Ok None)
       else match t.tls_config.ca_cert_file with
         | Some _ -> (match Ca_certs.authenticator () with Ok a -> a | Error (`Msg m) -> failwith m)
         | None -> (match Ca_certs.authenticator () with Ok a -> a | Error (`Msg m) -> failwith m)
     in
     let certificates =
       match t.tls_config.client_cert_file, t.tls_config.client_key_file with
       | Some cert_file, Some key_file ->
           let cert_pem = read_file cert_file in
           let key_pem = read_file key_file in
           let remove_ws s = String.concat "" (String.split_on_char '\n' s |> List.map String.trim) in
           let b64_decode_exn s = Base64.decode_exn (remove_ws s) in
           let find_blocks pem ~label =
             let begin_marker = "-----BEGIN " ^ label ^ "-----" in
             let end_marker = "-----END " ^ label ^ "-----" in
             let rec collect from acc =
               match String.index_from_opt pem from '-' with
               | None -> List.rev acc
               | Some i when i + String.length begin_marker <= String.length pem && String.sub pem i (String.length begin_marker) = begin_marker ->
                   let content_start = i + String.length begin_marker in
                   (* find end marker *)
                   let rec find_end j =
                     match String.index_from_opt pem j '-' with
                     | None -> None
                     | Some k when k + String.length end_marker <= String.length pem && String.sub pem k (String.length end_marker) = end_marker -> Some k
                     | Some k -> find_end (k + 1)
                   in
                   begin match find_end content_start with
                   | None -> List.rev acc
                   | Some k ->
                       let content = String.sub pem content_start (k - content_start) in
                       collect (k + String.length end_marker) (content :: acc)
                   end
               | Some i -> collect (i + 1) acc
             in
             collect 0 []
           in
           let cert_blocks = find_blocks cert_pem ~label:"CERTIFICATE" in
           if cert_blocks = [] then failwith "No CERTIFICATE blocks found in client_cert_file";
           let certs =
             cert_blocks
             |> List.map (fun b -> Cstruct.of_string (b64_decode_exn b))
             |> List.map (fun der -> match X509.Certificate.decode_der der with Ok c -> c | Error (`Msg m) -> failwith m)
           in
           let key_labels = ["PRIVATE KEY"; "RSA PRIVATE KEY"; "EC PRIVATE KEY"] in
           let rec find_key = function
             | [] -> None
             | lab :: tl ->
                 let blocks = find_blocks key_pem ~label:lab in
                 match blocks with
                 | b :: _ -> Some b
                 | [] -> find_key tl
           in
           let key_block = match find_key key_labels with Some b -> b | None -> failwith "No PRIVATE KEY block found in client_key_file" in
           let key_der = Cstruct.of_string (b64_decode_exn key_block) in
           let key = match X509.Private_key.decode_der key_der with Ok k -> k | Error (`Msg m) -> failwith m in
           `Single (certs, key)
       | Some _, None | None, Some _ -> raise (Invalid_argument "Both client_cert_file and client_key_file must be set for mTLS")
       | None, None -> `None
     in
     let config = match Config.client ~authenticator ~certificates () with Ok c -> c | Error (`Msg m) -> failwith m in
     (match Domain_name.of_string t.host with
      | Ok raw ->
          let h = Domain_name.host_exn raw in
          let+ tls = Tls_lwt.Unix.client_of_fd config ~host:h fd in
          `Tls (fd, tls)
      | Error _ ->
          let+ tls = Tls_lwt.Unix.client_of_fd config fd in
          `Tls (fd, tls))
   else Lwt.return (`Fd fd)) in
  match res with
  | `Tls (fd', tls) -> t.tls <- Some tls; t.fd <- Some fd'; t.connected <- true; debugv (fun () -> Printf.printf "[proton] TLS handshake complete\n"); Lwt.return_unit
  | `Fd fd' -> t.fd <- Some fd'; t.connected <- true; Lwt.return_unit

let disconnect t =
  let* () = (match t.tls with Some tls -> Tls_lwt.Unix.shutdown tls `read_write | None -> Lwt.return_unit) in
  let* () = (match t.fd with Some fd -> (Lwt.catch (fun () -> Lwt_unix.close fd) (fun _ -> Lwt.return_unit)) | None -> Lwt.return_unit) in
  t.tls <- None; t.fd <- None; t.connected <- false; t.srv <- None; Lwt.return_unit

let write_settings_as_strings buf settings ~settings_is_important =
  List.iter (fun (k,v) ->
    write_varint_int_to_buffer buf (String.length k); Buffer.add_string buf k;
    Buffer.add_char buf (Char.chr (if settings_is_important then 1 else 0));
    write_varint_int_to_buffer buf (String.length v); Buffer.add_string buf v
  ) settings;
  write_varint_int_to_buffer buf 0

let send_hello t =
  let writev_fn = match t.tls, t.fd with
    | Some tls, _ -> (fun ss -> Tls_lwt.Unix.writev tls ss)
    | None, Some fd -> (fun ss -> writev_all fd ss)
    | _ -> failwith "not connected"
  in
  let buf = Buffer.create 256 in
  write_varint_int_to_buffer buf (client_packet_to_int Hello);
  let s = Defines.dbms_name ^ " " ^ Defines.client_name in
  debugv (fun () -> Printf.printf "[proton] send_hello: client_name='%s' maj=%d min=%d rev=%d db='%s' user='%s' pw_len=%d\n"
    s Defines.client_version_major Defines.client_version_minor Defines.client_revision t.database t.user (String.length t.password));
  write_varint_int_to_buffer buf (String.length s); Buffer.add_string buf s;
  write_varint_int_to_buffer buf Defines.client_version_major;
  write_varint_int_to_buffer buf Defines.client_version_minor;
  write_varint_int_to_buffer buf Defines.client_revision;
  write_varint_int_to_buffer buf (String.length t.database); Buffer.add_string buf t.database;
  write_varint_int_to_buffer buf (String.length t.user); Buffer.add_string buf t.user;
  write_varint_int_to_buffer buf (String.length t.password); Buffer.add_string buf t.password;
  let payload = Buffer.contents buf in
  debugv (fun () -> Printf.printf "[proton] send_hello: bytes=%s\n" (hex_of_string payload));
  writev_fn [payload]

let receive_hello t =
  let read_fn = match t.tls, t.fd with
    | Some tls, _ -> (fun b o l -> let+ n = Tls_lwt.Unix.read tls ~off:o b in if n > l then l else n)
    | None, Some fd -> (fun b o l -> Lwt_unix.read fd b o l)
    | _ -> fun _ _ _ -> Lwt.fail (Failure "not connected")
  in
  debugv (fun () -> Printf.printf "[proton] receive_hello: waiting for server hello...\n");
  let* p = Lwt_unix.with_timeout Defines.dbms_default_sync_request_timeout_sec (fun () -> read_varint_int_lwt read_fn) in
  debugv (fun () -> Printf.printf "[proton] receive_hello: server packet=%d\n" p);
  match Protocol.server_packet_of_int p with
  | SHello ->
      let* server_name = read_str_lwt read_fn in
      let* version_major = read_varint_int_lwt read_fn in
      let* version_minor = read_varint_int_lwt read_fn in
      let* server_revision = read_varint_int_lwt read_fn in
      let* server_timezone =
        if server_revision >= Defines.dbms_min_revision_with_server_timezone then
          let+ tz = read_str_lwt read_fn in Some tz
        else Lwt.return_none
      in
      let* server_display_name =
        if server_revision >= Defines.dbms_min_revision_with_server_display_name then
          read_str_lwt read_fn
        else Lwt.return ""
      in
      let* version_patch =
        if server_revision >= Defines.dbms_min_revision_with_version_patch then
          read_varint_int_lwt read_fn
        else Lwt.return server_revision
      in
      let si = { Context.name = server_name; version_major; version_minor; version_patch;
                 revision = server_revision; timezone = (match server_timezone with Some tz -> Some tz | None -> None); display_name = server_display_name } in
      debugv (fun () -> Printf.printf "[proton] receive_hello: name='%s' ver=%d.%d.%d rev=%d tz=%s display='%s'\n"
        server_name version_major version_minor version_patch server_revision
        (match si.timezone with Some tz -> tz | None -> "-") server_display_name);
      t.srv <- Some si; t.ctx.server_info <- Some si; Lwt.return_unit
  | SException -> Lwt.fail (Failure "Server exception in hello")
  | _other -> Lwt.fail (Unexpected_packet (Printf.sprintf "Expected HELLO, got %d" p))

let force_connect t =
  let* () = (if not t.connected then connect t else Lwt.return_unit) in
  match t.srv with
  | Some _ -> Lwt.return_unit
  | None -> let* () = send_hello t in receive_hello t

(* Send a data block *)
let send_data_block t (block: Block.t) =
  let* () = force_connect t in
  let writev_fn = match t.tls, t.fd with
    | Some tls, _ -> (fun ss -> Tls_lwt.Unix.writev tls ss)
    | None, Some fd -> (fun ss -> writev_all fd ss)
    | _ -> failwith "not connected"
  in
  (* Debug info about the outgoing block *)
  let is_header = (block.n_rows = 0 && Array.length block.columns > 0) in
  let is_terminator = (block.n_rows = 0 && Array.length block.columns = 0) in
  debugv (fun () -> Printf.printf "[proton] send_data_block: cols=%d rows=%d kind=%s\n"
    (Array.length block.columns) block.n_rows
    (if is_header then "header" else if is_terminator then "terminator" else "data"));
  (* Server revision for block info support. *)
  let revision = match t.srv with Some s -> s.revision | None -> failwith "no server revision" in
  (* Build packet header and table name; coalesce write with payload *)
  let hdr = Buffer.create 32 in
  Binary_writer.write_varint_to_buffer hdr (Protocol.client_packet_to_int Protocol.Data);
  Binary_writer.write_string_to_buffer hdr "";
  let hdr_s = Buffer.contents hdr in
  match t.compression with
  | Compress.None ->
      (* Build entire payload in memory for uncompressed case. *)
      let buf_block = Buffer.create 4096 in
      if revision >= Defines.dbms_min_revision_with_block_info then
        Binary_writer.write_block_info_to_buffer buf_block Block_info.{ is_overflows=false; bucket_num = -1 };
      Binary_writer.write_varint_to_buffer buf_block (Array.length block.columns);
      Binary_writer.write_varint_to_buffer buf_block block.n_rows;
      Array.iter (fun col ->
        Binary_writer.write_string_to_buffer buf_block col.Block.name;
        Binary_writer.write_string_to_buffer buf_block col.Block.type_spec;
        for i = 0 to Array.length col.Block.data - 1 do
          Binary_writer.write_value_to_buffer buf_block col.Block.data.(i)
        done
      ) block.columns;
      let payload_s = Buffer.contents buf_block in
      writev_fn [hdr_s; payload_s]
  | cmpr ->
      (* Write header once, then stream frames. *)
      let* () = writev_fn [hdr_s] in
      let cs = Compress.Stream.create writev_fn cmpr in
      (* Write block info if supported by server revision. *)
      let revision = match t.srv with Some s -> s.revision | None -> 0 in
      let* () =
        if revision >= Defines.dbms_min_revision_with_block_info then (
          let b = Buffer.create 16 in
          Binary_writer.write_block_info_to_buffer b Block_info.{ is_overflows=false; bucket_num = -1 };
          Compress.Stream.write_string cs (Buffer.contents b)
        ) else Lwt.return_unit
      in
      (* n_columns, n_rows *)
      let b_hdr = Buffer.create 16 in
      Binary_writer.write_varint_to_buffer b_hdr (Array.length block.columns);
      Binary_writer.write_varint_to_buffer b_hdr block.n_rows;
      let* () = Compress.Stream.write_string cs (Buffer.contents b_hdr) in
      (* columns: names, types, values *)
      let* () =
        Lwt_list.iter_s (fun col ->
          let bmeta = Buffer.create 64 in
          Binary_writer.write_string_to_buffer bmeta col.Block.name;
          Binary_writer.write_string_to_buffer bmeta col.Block.type_spec;
          let* () = Compress.Stream.write_string cs (Buffer.contents bmeta) in
          (* Stream each value *)
          let rec loop i =
            if i = Array.length col.Block.data then Lwt.return_unit
            else (
              let vb = Buffer.create 32 in
              Binary_writer.write_value_to_buffer vb col.Block.data.(i);
              let* () = Compress.Stream.write_string cs (Buffer.contents vb) in
              loop (i+1)
            )
          in
          loop 0
        ) (Array.to_list block.columns)
      in
      Compress.Stream.flush cs

let send_query t ?(query_id="") (query:string) =
  let* () = force_connect t in
  let writev_fn = match t.tls, t.fd with
    | Some tls, _ -> (fun ss -> Tls_lwt.Unix.writev tls ss)
    | None, Some fd -> (fun ss -> writev_all fd ss)
    | _ -> failwith "not connected"
  in
  let buf = Buffer.create 512 in
  write_varint_int_to_buffer buf (client_packet_to_int Query);
  write_varint_int_to_buffer buf (String.length query_id); Buffer.add_string buf query_id;
  let revision = match t.srv with Some s -> s.revision | None -> failwith "no server revision" in
  if revision >= Defines.dbms_min_revision_with_client_info then begin
    Buffer.add_char buf (Char.chr 1);
    write_varint_int_to_buffer buf 0; write_varint_int_to_buffer buf 0; write_varint_int_to_buffer buf (String.length "0.0.0.0:0"); Buffer.add_string buf "0.0.0.0:0";
    if revision >= Defines.dbms_min_protocol_version_with_initial_query_start_time then Buffer.add_string buf "\x00\x00\x00\x00\x00\x00\x00\x00";
    Buffer.add_char buf (Char.chr 1);
    let user_env = try Sys.getenv "USER" with _ -> "" in
    write_varint_int_to_buffer buf (String.length user_env); Buffer.add_string buf user_env;
    let hostn = Unix.gethostname () in
    write_varint_int_to_buffer buf (String.length hostn); Buffer.add_string buf hostn;
    let clientn = Defines.dbms_name ^ " " ^ Defines.client_name in
    write_varint_int_to_buffer buf (String.length clientn); Buffer.add_string buf clientn;
    write_varint_int_to_buffer buf Defines.client_version_major;
    write_varint_int_to_buffer buf Defines.client_version_minor;
    write_varint_int_to_buffer buf Defines.client_revision;
    if revision >= Defines.dbms_min_revision_with_quota_key_in_client_info then write_varint_int_to_buffer buf 0;
    if revision >= Defines.dbms_min_protocol_version_with_distributed_depth then write_varint_int_to_buffer buf 0;
    if revision >= Defines.dbms_min_revision_with_version_patch then write_varint_int_to_buffer buf Defines.client_version_patch;
    if revision >= Defines.dbms_min_revision_with_opentelemetry then Buffer.add_char buf (Char.chr 0);
    if revision >= Defines.dbms_min_revision_with_parallel_replicas then (
      (* collaborate_with_initiator, count_participating_replicas, number_of_current_replica *)
      write_varint_int_to_buffer buf 0;
      write_varint_int_to_buffer buf 0;
      write_varint_int_to_buffer buf 0
    )
  end;
  let settings_as_strings = match t.srv with Some s -> s.revision >= Defines.dbms_min_revision_with_settings_serialized_as_strings | None -> true in
  if settings_as_strings then write_settings_as_strings buf t.ctx.settings ~settings_is_important:false else write_varint_int_to_buffer buf 0;
  if revision >= Defines.dbms_min_revision_with_interserver_secret then write_varint_int_to_buffer buf 0;
  write_varint_int_to_buffer buf (qps_to_int Complete);
  write_varint_int_to_buffer buf (compression_to_int t.compression);
  write_varint_int_to_buffer buf (String.length query); Buffer.add_string buf query;
  let* () = write_buf writev_fn buf in
  (* Per Proton/ClickHouse protocol, send an empty Data block to indicate no external tables / end of data. *)
  let empty_block = { Block.n_columns = 0; n_rows = 0; columns = [||] } in
  send_data_block t empty_block

(* Variant used for INSERT via native protocol: do NOT send the trailing
   empty Data block here, because the client must immediately stream the
   data blocks (followed by a final empty block) after the Query. *)
let send_query_without_data_end t ?(query_id="") (query:string) =
  let* () = force_connect t in
  let writev_fn = match t.tls, t.fd with
    | Some tls, _ -> (fun ss -> Tls_lwt.Unix.writev tls ss)
    | None, Some fd -> (fun ss -> writev_all fd ss)
    | _ -> failwith "not connected"
  in
  let buf = Buffer.create 512 in
  write_varint_int_to_buffer buf (client_packet_to_int Query);
  write_varint_int_to_buffer buf (String.length query_id); Buffer.add_string buf query_id;
  let revision = match t.srv with Some s -> s.revision | None -> failwith "no server revision" in
  if revision >= Defines.dbms_min_revision_with_client_info then begin
    Buffer.add_char buf (Char.chr 1);
    write_varint_int_to_buffer buf 0; write_varint_int_to_buffer buf 0; write_varint_int_to_buffer buf (String.length "0.0.0.0:0"); Buffer.add_string buf "0.0.0.0:0";
    if revision >= Defines.dbms_min_protocol_version_with_initial_query_start_time then Buffer.add_string buf "\x00\x00\x00\x00\x00\x00\x00\x00";
    Buffer.add_char buf (Char.chr 1);
    let user_env = try Sys.getenv "USER" with _ -> "" in
    write_varint_int_to_buffer buf (String.length user_env); Buffer.add_string buf user_env;
    let hostn = Unix.gethostname () in
    write_varint_int_to_buffer buf (String.length hostn); Buffer.add_string buf hostn;
    let clientn = Defines.dbms_name ^ " " ^ Defines.client_name in
    write_varint_int_to_buffer buf (String.length clientn); Buffer.add_string buf clientn;
    write_varint_int_to_buffer buf Defines.client_version_major;
    write_varint_int_to_buffer buf Defines.client_version_minor;
    write_varint_int_to_buffer buf Defines.client_revision;
    if revision >= Defines.dbms_min_revision_with_quota_key_in_client_info then write_varint_int_to_buffer buf 0;
    if revision >= Defines.dbms_min_protocol_version_with_distributed_depth then write_varint_int_to_buffer buf 0;
    if revision >= Defines.dbms_min_revision_with_version_patch then write_varint_int_to_buffer buf Defines.client_version_patch;
    if revision >= Defines.dbms_min_revision_with_opentelemetry then Buffer.add_char buf (Char.chr 0);
    if revision >= Defines.dbms_min_revision_with_parallel_replicas then (
      (* collaborate_with_initiator, count_participating_replicas, number_of_current_replica *)
      write_varint_int_to_buffer buf 0;
      write_varint_int_to_buffer buf 0;
      write_varint_int_to_buffer buf 0
    )
  end;
  let settings_as_strings = match t.srv with Some s -> s.revision >= Defines.dbms_min_revision_with_settings_serialized_as_strings | None -> true in
  if settings_as_strings then write_settings_as_strings buf t.ctx.settings ~settings_is_important:false else write_varint_int_to_buffer buf 0;
  if revision >= Defines.dbms_min_revision_with_interserver_secret then write_varint_int_to_buffer buf 0;
  write_varint_int_to_buffer buf (qps_to_int Complete);
  write_varint_int_to_buffer buf (compression_to_int t.compression);
  write_varint_int_to_buffer buf (String.length query); Buffer.add_string buf query;
  debugv (fun () -> Printf.printf "[proton] send_query_without_data_end: %s\n" query);
  (* For INSERT ... FORMAT Native, ClickHouse/Proton expects an explicit end
     of external tables: send a zero-column, zero-row Data block first. Then
     the client will send the header, data block(s), and a final terminator. *)
  let* () = write_buf writev_fn buf in
  let empty_block = { Block.n_columns = 0; n_rows = 0; columns = [||] } in
  send_data_block t empty_block

let read_progress t read_fn =
  let* _rows = read_varint_int_lwt read_fn in
  let* _bytes = read_varint_int_lwt read_fn in
  let revision = match t.srv with Some s -> s.revision | None -> 0 in
  let* () =
    if revision >= Defines.dbms_min_revision_with_total_rows_in_progress then
      let+ _ = read_varint_int_lwt read_fn in ()
    else Lwt.return_unit
  in
  if revision >= Defines.dbms_min_revision_with_client_write_info then (
    let* _ = read_varint_int_lwt read_fn in
    let+ _ = read_varint_int_lwt read_fn in ()
  ) else Lwt.return_unit

let read_profile_info _t read_fn =
  let* _ = read_varint_int_lwt read_fn in
  let* _ = read_varint_int_lwt read_fn in
  let* _ = read_varint_int_lwt read_fn in
  let* _ = read_byte read_fn in
  let* _ = read_varint_int_lwt read_fn in
  let+ _ = read_byte read_fn in ()

let read_compressed_block_lwt read_fn : bytes Lwt.t =
  let open Compress in
  let header_len = header_size in
  let* header = lwt_read_exact read_fn header_len in
  (* Try both header interpretations: method-first and sizes-first. *)
  let meth_mf = Char.code (Bytes.get header checksum_size) in
  let comp_mf = Binary.bytes_get_int32_le header (checksum_size + 1) |> Int32.to_int in
  let uncomp_mf = Binary.bytes_get_int32_le header (checksum_size + 5) |> Int32.to_int in

  let meth_sf = Char.code (Bytes.get header (checksum_size + 8)) in
  let comp_sf = Binary.bytes_get_int32_le header checksum_size |> Int32.to_int in
  let uncomp_sf = Binary.bytes_get_int32_le header (checksum_size + 4) |> Int32.to_int in

  let is_method b = (b = 0x02 || b = 0x82 || b = 0x90) in
  let method_byte, compressed_size, uncompressed_size =
    if is_method meth_mf && comp_mf >= compress_header_size && uncomp_mf >= 0 then
      (meth_mf, comp_mf, uncomp_mf)
    else if is_method meth_sf && comp_sf >= compress_header_size && uncomp_sf >= 0 then
      (meth_sf, comp_sf, uncomp_sf)
    else
      raise (Compression_error "Invalid compressed block header")
  in
  let payload_size = compressed_size - compress_header_size in
  let* payload = lwt_read_exact read_fn payload_size in
  (* Build the contiguous slice we hash on write: [method|sizes|payload] *)
  let to_hash_len = compress_header_size + payload_size in
  let to_hash = Bytes.create to_hash_len in
  Bytes.blit header checksum_size to_hash 0 compress_header_size;
  Bytes.blit payload 0 to_hash compress_header_size payload_size;
  let h_cxx = Cityhash.cityhash128 to_hash in
  let calc_cxx = Cityhash.to_bytes h_cxx in
  let received_checksum = Bytes.sub header 0 checksum_size in
  if not (Bytes.equal calc_cxx received_checksum) then (
    if env_debug_enabled () then (
      let hex b =
        let len = Bytes.length b in
        let out = Bytes.create (2*len) in
        let hexdig = "0123456789abcdef" in
        for i=0 to len-1 do
          let v = Char.code (Bytes.get b i) in
          Bytes.set out (2*i) (String.unsafe_get hexdig ((v lsr 4) land 0xF));
          Bytes.set out (2*i+1) (String.unsafe_get hexdig (v land 0xF));
        done; Bytes.unsafe_to_string out
      in
      Printf.printf "[decompress] checksum mismatch: recv=%s calccxx=%s hdr=%s hash_len=%d\n%!"
        (hex received_checksum) (hex calc_cxx)
        (hex (Bytes.sub header checksum_size compress_header_size)) to_hash_len
    );
    Lwt.fail (Compression_error "Compression checksum verification failed")
  )
  else (
    match method_of_byte method_byte with
    | None -> Lwt.return payload
    | LZ4 -> Lwt.return (decompress_lz4 payload uncompressed_size)
    | ZSTD -> Lwt.return (decompress_zstd payload uncompressed_size)
  )

let read_uncompressed_block_lwt read_fn : bytes Lwt.t =
  let buf = Buffer.create 4096 in
  (* Read a varint from the wire, append its raw bytes to buf, and return the int *)
  let read_varint_raw_to_buf () : int Lwt.t =
    let raw = Bytes.create 10 in
    let rec loop shift acc idx =
      let b = Bytes.create 1 in
      let* n = read_fn b 0 1 in
      if n = 0 then Lwt.fail End_of_file else
      let vb = Bytes.get b 0 in
      Bytes.set raw idx vb;
      let v = (Char.code vb land 0x7f) lsl shift in
      if (Char.code vb land 0x80) <> 0 then loop (shift + 7) (acc lor v) (idx + 1)
      else (
        (* append the collected raw varint bytes *)
        Buffer.add_subbytes buf raw 0 (idx + 1);
        Lwt.return (acc lor v)
      )
    in
    loop 0 0 0
  in
  let read_and_copy_bytes len =
    let+ b = lwt_read_exact read_fn len in Buffer.add_bytes buf b; b
  in
  let read_and_copy_str () =
    let* len = read_varint_raw_to_buf () in
    if len = 0 then Lwt.return ""
    else let+ s = read_and_copy_bytes len in Bytes.to_string s
  in
  (* BlockInfo *)
  let rec copy_block_info () =
    let* field = read_varint_raw_to_buf () in
    if field = 0 then Lwt.return_unit
    else if field = 1 then let* _ = read_and_copy_bytes 1 in copy_block_info ()
    else if field = 2 then let* _ = read_and_copy_bytes 4 in copy_block_info ()
    else copy_block_info ()
  in
  let* () = copy_block_info () in
  (* n_columns, n_rows *)
  let* n_columns = read_varint_raw_to_buf () in
  let* n_rows = read_varint_raw_to_buf () in
  let rec copy_columns i =
    if i = n_columns then Lwt.return_unit else
    let* _col_name = read_and_copy_str () in
    let* col_type = read_and_copy_str () in
    let t = String.lowercase_ascii (String.trim col_type) in
    let fixed_bytes_per_row =
      if t = "uint8" || t = "int8" || has_prefix t "enum8(" then Some 1 else
      if t = "uint16" || t = "int16" || has_prefix t "enum16(" then Some 2 else
      if t = "uint32" || t = "int32" || t = "float32" || t = "datetime" then Some 4 else
      if t = "uint64" || t = "int64" || t = "float64" || has_prefix t "datetime64" then Some 8 else
      None
    in
    let* () =
      match fixed_bytes_per_row with
      | Some sz -> let+ _ = read_and_copy_bytes (sz * n_rows) in ()
      | None ->
          if t = "string" || t = "json" then (
            let rec loop r =
              if r = n_rows then Lwt.return_unit
              else let* _ = read_and_copy_str () in loop (r+1)
            in loop 0
          ) else (
            Lwt.fail (Failure (Printf.sprintf "unsupported uncompressed column type: %s" col_type))
          )
    in
    copy_columns (i+1)
  in
  let* () = copy_columns 0 in
  Lwt.return (Buffer.to_bytes buf)

let[@warning "-32"] rec read_all_remaining read_fn acc =
  (* Helper to read all remaining data from stream into accumulator *)
  let chunk = Bytes.create 8192 in
  let* n = read_fn chunk 0 8192 in
  if n = 0 then 
    Lwt.return acc
  else (
    Buffer.add_subbytes acc chunk 0 n;
    read_all_remaining read_fn acc
  )

let receive_data_block t ~compressible read_fn : Block.t Lwt.t =
  let revision = match t.srv with Some s -> s.revision | None -> 0 in
  (* Server sends table name string before block. Discard it. *)
  let* _table_name = read_str_lwt read_fn in
  if compressible && t.compression <> Compress.None then (
    let* decompressed = read_compressed_block_lwt read_fn in
    let br = Buffered_reader.create_from_bytes_no_copy decompressed in
    Lwt.return (Block.read_block_br ~revision br)
  ) else (
    let* bytes = read_uncompressed_block_lwt read_fn in
    let br = Buffered_reader.create_from_bytes_no_copy bytes in
    Lwt.return (Block.read_block_br ~revision br)
  )

let receive_packet t : packet Lwt.t =
  let read_fn = match t.tls, t.fd with
    | Some tls, _ -> (fun b o l -> let+ n = Tls_lwt.Unix.read tls ~off:o b in if n > l then l else n)
    | None, Some fd -> (fun b o l -> Lwt_unix.read fd b o l)
    | _ -> fun _ _ _ -> Lwt.fail (Failure "not connected")
  in
  let* ptype = read_varint_int_lwt read_fn in
  match Protocol.server_packet_of_int ptype with
  | SData -> let+ b = receive_data_block t ~compressible:true read_fn in PData b
  | SException ->
      let rec read_exception_lwt () =
        let* code32 = read_int32_le_lwt read_fn in
        let code = Int32.to_int code32 in
        let* name = read_str_lwt read_fn in
        let* msg = read_str_lwt read_fn in
        let* stack = read_str_lwt read_fn in
        let* has_nested = read_byte read_fn in
        let base = (if name <> "DB::Exception" then name ^ ". " else "") ^ msg ^ ". Stack trace:\n\n" ^ stack in
        let* nested =
          if has_nested <> 0 then
            let+ nested_exn = read_exception_lwt () in
              match nested_exn with
              | Errors.Server_exception se -> Some se.message
              | e -> Some (Printexc.to_string e)
          else Lwt.return_none
        in
        Lwt.return (Errors.Server_exception { code; message = base; nested })
      in
      let* e = read_exception_lwt () in Lwt.fail e
  | SProgress -> let+ () = read_progress t read_fn in PProgress
  | SEndOfStream -> Lwt.return PEndOfStream
  | SProfileInfo -> let+ () = read_profile_info t read_fn in PProfileInfo
  | STotals -> let+ b = receive_data_block t ~compressible:true read_fn in PTotals b
  | SExtremes -> let+ b = receive_data_block t ~compressible:true read_fn in PExtremes b
  | SLog -> let+ b = receive_data_block t ~compressible:false read_fn in PLog b
  | STableColumns -> let* _ = read_str_lwt read_fn in let+ _ = read_str_lwt read_fn in PProgress
  | SPartUUIDs | SReadTaskRequest -> let+ _ = receive_data_block t ~compressible:true read_fn in PProgress
  | SProfileEvents -> let+ _ = receive_data_block t ~compressible:false read_fn in PProgress
  | SHello | SPong | STablesStatusResponse -> Lwt.return PProgress

let[@warning "-32"] read_exception_from_bytes (bs:bytes) : exn Lwt.t =
  let pos = ref 0 in
  let read_fn buf off len =
    let remaining = Bytes.length bs - !pos in
    let to_copy = min len remaining in
    Bytes.blit bs !pos buf off to_copy; pos := !pos + to_copy; Lwt.return to_copy
  in
  let rec read_exception_lwt () =
    let* code32 = read_int32_le_lwt read_fn in
    let code = Int32.to_int code32 in
    let* name = read_str_lwt read_fn in
    let* msg = read_str_lwt read_fn in
    let* stack = read_str_lwt read_fn in
    let* has_nested = read_byte read_fn in
    let base = (if name <> "DB::Exception" then name ^ ". " else "") ^ msg ^ ". Stack trace:\n\n" ^ stack in
    let* nested =
      if has_nested <> 0 then
        let+ nested_exn = read_exception_lwt () in
          match nested_exn with
          | Errors.Server_exception se -> Some se.message
          | e -> Some (Printexc.to_string e)
      else Lwt.return_none
    in
    Lwt.return (Errors.Server_exception { code; message = base; nested })
  in
  read_exception_lwt ()
