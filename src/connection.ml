open Protocol
open Errors
open Context
open Block
open Lwt.Infix

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

let debugf fmt =
  if env_debug_enabled () then Printf.printf fmt else Printf.ifprintf stdout fmt

let hex_of_string s =
  let b = Bytes.unsafe_of_string s in
  let len = Bytes.length b in
  let buf = Buffer.create (2 * len) in
  for i = 0 to len - 1 do
    Buffer.add_string buf (Printf.sprintf "%02x" (Char.code (Bytes.get b i)))
  done;
  Buffer.contents buf

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
      read_fn buf off (len - off) >>= fun n ->
      if n = 0 then Lwt.fail End_of_file else loop (off + n)
  in
  loop 0

let read_byte read_fn =
  lwt_read_exact read_fn 1 >|= fun b -> Char.code (Bytes.get b 0)

let read_varint_int_lwt read_fn =
  let rec loop shift acc =
    read_byte read_fn >>= fun b ->
    let v = acc lor ((b land 0x7f) lsl shift) in
    if (b land 0x80) <> 0 then loop (shift + 7) v else Lwt.return v
  in loop 0 0

let read_int32_le_lwt read_fn =
  lwt_read_exact read_fn 4 >|= fun buf ->
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
  lwt_read_exact read_fn 8 >|= fun buf ->
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

let read_float64_le_lwt read_fn =
  read_uint64_le_lwt read_fn >|= Int64.float_of_bits

let read_str_lwt read_fn =
  read_varint_int_lwt read_fn >>= fun len ->
  if len = 0 then Lwt.return ""
  else lwt_read_exact read_fn len >|= Bytes.to_string

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
      Lwt_unix.gethostbyname host >>= fun host_entry ->
      if Array.length host_entry.Unix.h_addr_list > 0 then
        Lwt.return host_entry.Unix.h_addr_list.(0)
      else
        Lwt.fail (Failure ("Cannot resolve host: " ^ host))
  in
  get_addr t.host >>= fun addr ->
  let sockaddr = Unix.ADDR_INET (addr, t.port) in
  debugf "[proton] connect: %s:%d (addr=%s)\n" t.host t.port (Unix.string_of_inet_addr addr);
  Lwt_unix.with_timeout Defines.dbms_default_connect_timeout_sec (fun () -> Lwt_unix.connect fd sockaddr) >>= fun () ->
  debugf "[proton] connect: socket connected\n";
  (if t.tls_config.enable_tls then
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
           let certs = match X509.Certificate.decode_pem_multiple cert_pem with Ok cs -> cs | Error (`Msg m) -> failwith m in
           let key = match X509.Private_key.decode_pem key_pem with Ok k -> k | Error (`Msg m) -> failwith m in
           `Single (certs, key)
       | Some _, None | None, Some _ -> raise (Invalid_argument "Both client_cert_file and client_key_file must be set for mTLS")
       | None, None -> `None
     in
     let config = match Config.client ~authenticator ~certificates () with Ok c -> c | Error (`Msg m) -> failwith m in
     (match Domain_name.of_string t.host with
      | Ok raw -> let h = Domain_name.host_exn raw in Tls_lwt.Unix.client_of_fd config ~host:h fd
      | Error _ -> Tls_lwt.Unix.client_of_fd config fd) >|= fun tls -> `Tls (fd, tls)
   else Lwt.return (`Fd fd)) >>= function
  | `Tls (fd', tls) -> t.tls <- Some tls; t.fd <- Some fd'; t.connected <- true; debugf "[proton] TLS handshake complete\n"; Lwt.return_unit
  | `Fd fd' -> t.fd <- Some fd'; t.connected <- true; Lwt.return_unit

let disconnect t =
  (match t.tls with Some tls -> Tls_lwt.Unix.shutdown tls `read_write | None -> Lwt.return_unit) >>= fun () ->
  (match t.fd with Some fd -> (Lwt.catch (fun () -> Lwt_unix.close fd) (fun _ -> Lwt.return_unit)) | None -> Lwt.return_unit) >>= fun () ->
  t.tls <- None; t.fd <- None; t.connected <- false; Lwt.return_unit

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
    | None, Some fd -> (fun ss -> Lwt_list.iter_s (fun s -> Lwt_unix.write_string fd s 0 (String.length s) >|= ignore) ss)
    | _ -> failwith "not connected"
  in
  let buf = Buffer.create 256 in
  write_varint_int_to_buffer buf (client_packet_to_int Hello);
  let s = Defines.dbms_name ^ " " ^ Defines.client_name in
  debugf "[proton] send_hello: client_name='%s' maj=%d min=%d rev=%d db='%s' user='%s' pw_len=%d\n"
    s Defines.client_version_major Defines.client_version_minor Defines.client_revision t.database t.user (String.length t.password);
  write_varint_int_to_buffer buf (String.length s); Buffer.add_string buf s;
  write_varint_int_to_buffer buf Defines.client_version_major;
  write_varint_int_to_buffer buf Defines.client_version_minor;
  write_varint_int_to_buffer buf Defines.client_revision;
  write_varint_int_to_buffer buf (String.length t.database); Buffer.add_string buf t.database;
  write_varint_int_to_buffer buf (String.length t.user); Buffer.add_string buf t.user;
  write_varint_int_to_buffer buf (String.length t.password); Buffer.add_string buf t.password;
  let payload = Buffer.contents buf in
  debugf "[proton] send_hello: bytes=%s\n" (hex_of_string payload);
  write_buf writev_fn buf

let receive_hello t =
  let read_fn = match t.tls, t.fd with
    | Some tls, _ -> (fun b o l -> Tls_lwt.Unix.read tls ~off:o b >|= fun n -> if n > l then l else n)
    | None, Some fd -> (fun b o l -> Lwt_unix.read fd b o l)
    | _ -> fun _ _ _ -> Lwt.fail (Failure "not connected")
  in
  debugf "[proton] receive_hello: waiting for server hello...\n";
  Lwt_unix.with_timeout Defines.dbms_default_sync_request_timeout_sec (fun () -> read_varint_int_lwt read_fn) >>= fun p ->
  debugf "[proton] receive_hello: server packet=%d\n" p;
  match Protocol.server_packet_of_int p with
  | SHello ->
      read_str_lwt read_fn >>= fun server_name ->
      read_varint_int_lwt read_fn >>= fun version_major ->
      read_varint_int_lwt read_fn >>= fun version_minor ->
      read_varint_int_lwt read_fn >>= fun server_revision ->
      (if server_revision >= Defines.dbms_min_revision_with_server_timezone then
         read_str_lwt read_fn >|= fun tz -> Some tz else Lwt.return_none) >>= fun server_timezone ->
      (if server_revision >= Defines.dbms_min_revision_with_server_display_name then
         read_str_lwt read_fn else Lwt.return "") >>= fun server_display_name ->
      (if server_revision >= Defines.dbms_min_revision_with_version_patch then
         read_varint_int_lwt read_fn else Lwt.return server_revision) >>= fun version_patch ->
      let si = { Context.name = server_name; version_major; version_minor; version_patch;
                 revision = server_revision; timezone = (match server_timezone with Some tz -> Some tz | None -> None); display_name = server_display_name } in
      debugf "[proton] receive_hello: name='%s' ver=%d.%d.%d rev=%d tz=%s display='%s'\n"
        server_name version_major version_minor version_patch server_revision
        (match si.timezone with Some tz -> tz | None -> "-") server_display_name;
      t.srv <- Some si; t.ctx.server_info <- Some si; Lwt.return_unit
  | SException -> Lwt.fail (Failure "Server exception in hello")
  | _other -> Lwt.fail (Unexpected_packet (Printf.sprintf "Expected HELLO, got %d" p))

let force_connect t =
  (if not t.connected then connect t else Lwt.return_unit) >>= fun () ->
  match t.srv with
  | Some _ -> Lwt.return_unit
  | None -> send_hello t >>= fun () -> receive_hello t

(* Send a data block *)
let send_data_block t (block: Block.t) =
  force_connect t >>= fun () ->
  let writev_fn = match t.tls, t.fd with
    | Some tls, _ -> (fun ss -> Tls_lwt.Unix.writev tls ss)
    | None, Some fd -> (fun ss -> Lwt_list.iter_s (fun s -> Lwt_unix.write_string fd s 0 (String.length s) >|= ignore) ss)
    | _ -> failwith "not connected"
  in
  (* Build block payload (info + columns + rows) in a separate buffer. *)
  let buf_block = Buffer.create 4096 in
  (* Write block info *)
  let revision = match t.srv with Some s -> s.revision | None -> failwith "no server revision" in
  if revision >= Defines.dbms_min_revision_with_block_info then
    Binary_writer.write_block_info_to_buffer buf_block block.info;
  
  (* Write block data *)
  Binary_writer.write_varint_to_buffer buf_block (List.length block.columns);  (* n_columns *)
  Binary_writer.write_varint_to_buffer buf_block block.n_rows;                 (* n_rows *)
  
  (* Write each column *)
  List.iter (fun col ->
    Binary_writer.write_string_to_buffer buf_block col.Block.name;
    Binary_writer.write_string_to_buffer buf_block col.Block.type_spec;
    
    (* Write column data *)
    for i = 0 to Array.length col.Block.data - 1 do
      Binary_writer.write_value_to_buffer buf_block col.Block.data.(i)
    done
  ) block.columns;
  (* Send packet header and table name (uncompressed) *)
  let hdr = Buffer.create 32 in
  Binary_writer.write_varint_to_buffer hdr (Protocol.client_packet_to_int Protocol.Data);
  Binary_writer.write_string_to_buffer hdr "";
  write_buf writev_fn hdr >>= fun () ->
  (* Send block payload (compressed if enabled) *)
  let payload = Buffer.contents buf_block in
  match t.compression with
  | Compress.None -> writev_fn [payload]
  | cmpr ->
      let frame = Compress.build_compressed_frame (Bytes.unsafe_of_string payload) cmpr in
      let s = Bytes.unsafe_to_string frame in
      writev_fn [s]

let send_query t ?(query_id="") (query:string) =
  force_connect t >>= fun () ->
  let writev_fn = match t.tls, t.fd with
    | Some tls, _ -> (fun ss -> Tls_lwt.Unix.writev tls ss)
    | None, Some fd -> (fun ss -> Lwt_list.iter_s (fun s -> Lwt_unix.write_string fd s 0 (String.length s) >|= ignore) ss)
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
  write_buf writev_fn buf >>= fun () ->
  (* Per Proton/ClickHouse protocol, send an empty Data block to indicate no external tables / end of data. *)
  let empty_block = { Block.info = { Block_info.is_overflows=false; bucket_num = -1 }; columns = []; n_rows = 0 } in
  send_data_block t empty_block

let read_progress t read_fn =
  read_varint_int_lwt read_fn >>= fun _rows ->
  read_varint_int_lwt read_fn >>= fun _bytes ->
  let revision = match t.srv with Some s -> s.revision | None -> 0 in
  (if revision >= Defines.dbms_min_revision_with_total_rows_in_progress then read_varint_int_lwt read_fn >|= ignore else Lwt.return_unit) >>= fun () ->
  (if revision >= Defines.dbms_min_revision_with_client_write_info then read_varint_int_lwt read_fn >>= fun _ -> read_varint_int_lwt read_fn >|= ignore else Lwt.return_unit)

let read_profile_info _t read_fn =
  read_varint_int_lwt read_fn >>= fun _ ->
  read_varint_int_lwt read_fn >>= fun _ ->
  read_varint_int_lwt read_fn >>= fun _ ->
  read_byte read_fn >>= fun _ ->
  read_varint_int_lwt read_fn >>= fun _ ->
  read_byte read_fn >|= fun _ -> ()

let read_compressed_block_lwt read_fn : bytes Lwt.t =
  let open Compress in
  let header_len = header_size in
  lwt_read_exact read_fn header_len >>= fun header ->
  let method_byte = Char.code (Bytes.get header checksum_size) in
  let compressed_size = Binary.bytes_get_int32_le header (checksum_size + 1) |> Int32.to_int in
  let uncompressed_size = Binary.bytes_get_int32_le header (checksum_size + 5) |> Int32.to_int in
  let payload_size = compressed_size - compress_header_size in
  lwt_read_exact read_fn payload_size >>= fun payload ->
  let checksum_input = Bytes.create (compress_header_size + payload_size) in
  Bytes.blit header checksum_size checksum_input 0 compress_header_size;
  Bytes.blit payload 0 checksum_input compress_header_size payload_size;
  let calculated_checksum = Cityhash.cityhash128 checksum_input |> Cityhash.to_bytes in
  let received_checksum = Bytes.sub header 0 checksum_size in
  if not (Bytes.equal calculated_checksum received_checksum) then Lwt.fail (Compression_error "Compression checksum verification failed")
  else (
    match method_of_byte method_byte with
    | None -> Lwt.return payload
    | LZ4 -> Lwt.return (decompress_lz4 payload uncompressed_size)
    | ZSTD -> Lwt.return (decompress_zstd payload uncompressed_size)
  )

let read_uncompressed_block_lwt read_fn : bytes Lwt.t =
  let buf = Buffer.create 4096 in
  let write_varint n = write_varint_int_to_buffer buf n in
  let write_bytes b = Buffer.add_bytes buf b in
  let read_and_copy_varint () = read_varint_int_lwt read_fn >|= fun v -> write_varint v; v in
  let read_and_copy_bytes len = lwt_read_exact read_fn len >|= fun b -> write_bytes b; b in
  let read_and_copy_str () = read_varint_int_lwt read_fn >>= fun len -> write_varint len; if len = 0 then Lwt.return "" else lwt_read_exact read_fn len >|= fun b -> let s = Bytes.to_string b in Buffer.add_string buf s; s in
  (* BlockInfo *)
  let rec copy_block_info () =
    read_and_copy_varint () >>= fun field ->
    if field = 0 then Lwt.return_unit
    else if field = 1 then read_and_copy_bytes 1 >>= fun _ -> copy_block_info ()
    else if field = 2 then read_and_copy_bytes 4 >>= fun _ -> copy_block_info ()
    else copy_block_info ()
  in
  copy_block_info () >>= fun () ->
  (* n_columns, n_rows *)
  read_and_copy_varint () >>= fun n_columns ->
  read_and_copy_varint () >>= fun n_rows ->
  let rec copy_columns i =
    if i = n_columns then Lwt.return_unit else
    read_and_copy_str () >>= fun _col_name ->
    read_and_copy_str () >>= fun col_type ->
    let t = String.lowercase_ascii (String.trim col_type) in
    let fixed_bytes_per_row =
      if t = "uint8" || t = "int8" || String.length t >= 6 && String.sub t 0 6 = "enum8(" then Some 1 else
      if t = "uint16" || t = "int16" || String.length t >= 7 && String.sub t 0 7 = "enum16(" then Some 2 else
      if t = "uint32" || t = "int32" || t = "float32" || t = "datetime" then Some 4 else
      if t = "uint64" || t = "int64" || t = "float64" || String.length t >= 9 && String.sub t 0 9 = "datetime64" then Some 8 else
      None
    in
    (match fixed_bytes_per_row with
     | Some sz -> read_and_copy_bytes (sz * n_rows) >|= fun _ -> ()
     | None ->
         if t = "string" || t = "json" then (
           let rec loop r =
             if r = n_rows then Lwt.return_unit
             else read_and_copy_str () >>= fun _ -> loop (r+1)
           in loop 0
         ) else (
           Lwt.fail (Failure (Printf.sprintf "unsupported uncompressed column type: %s" col_type))
         )) >>= fun () ->
    copy_columns (i+1)
  in
  copy_columns 0 >>= fun () ->
  Lwt.return (Buffer.to_bytes buf)

let rec read_all_remaining read_fn acc =
  (* Helper to read all remaining data from stream into accumulator *)
  let chunk = Bytes.create 8192 in
  read_fn chunk 0 8192 >>= fun n ->
  if n = 0 then 
    Lwt.return acc
  else (
    Buffer.add_subbytes acc chunk 0 n;
    read_all_remaining read_fn acc
  )

let receive_data_block t ~compressible read_fn : Block.t Lwt.t =
  let revision = match t.srv with Some s -> s.revision | None -> 0 in
  (* Server sends table name string before block. Discard it. *)
  read_str_lwt read_fn >>= fun _table_name ->
  if compressible && t.compression <> Compress.None then (
    read_compressed_block_lwt read_fn >>= fun decompressed ->
    let br = Buffered_reader.create_from_bytes decompressed in
    Lwt.return (Block.read_block_br ~revision br)
  ) else (
    read_uncompressed_block_lwt read_fn >>= fun bytes ->
    let br = Buffered_reader.create_from_bytes bytes in
    Lwt.return (Block.read_block_br ~revision br)
  )

let receive_packet t : packet Lwt.t =
  let read_fn = match t.tls, t.fd with
    | Some tls, _ -> (fun b o l -> Tls_lwt.Unix.read tls ~off:o b >|= fun n -> if n > l then l else n)
    | None, Some fd -> (fun b o l -> Lwt_unix.read fd b o l)
    | _ -> fun _ _ _ -> Lwt.fail (Failure "not connected")
  in
  read_varint_int_lwt read_fn >>= fun ptype ->
  match Protocol.server_packet_of_int ptype with
  | SData -> receive_data_block t ~compressible:true read_fn >|= fun b -> PData b
  | SException ->
      let rec read_exception_lwt () =
        read_int32_le_lwt read_fn >>= fun code32 ->
        let code = Int32.to_int code32 in
        read_str_lwt read_fn >>= fun name ->
        read_str_lwt read_fn >>= fun msg ->
        read_str_lwt read_fn >>= fun stack ->
        read_byte read_fn >>= fun has_nested ->
        let base = (if name <> "DB::Exception" then name ^ ". " else "") ^ msg ^ ". Stack trace:\n\n" ^ stack in
        (if has_nested <> 0 then
           read_exception_lwt () >|= fun nested_exn ->
             match nested_exn with
             | Errors.Server_exception se -> Some se.message
             | e -> Some (Printexc.to_string e)
         else Lwt.return_none) >>= fun nested ->
        Lwt.return (Errors.Server_exception { code; message = base; nested })
      in
      read_exception_lwt () >>= fun e -> Lwt.fail e
  | SProgress -> read_progress t read_fn >|= fun () -> PProgress
  | SEndOfStream -> Lwt.return PEndOfStream
  | SProfileInfo -> read_profile_info t read_fn >|= fun () -> PProfileInfo
  | STotals -> receive_data_block t ~compressible:true read_fn >|= fun b -> PTotals b
  | SExtremes -> receive_data_block t ~compressible:true read_fn >|= fun b -> PExtremes b
  | SLog -> receive_data_block t ~compressible:false read_fn >|= fun b -> PLog b
  | STableColumns -> read_str_lwt read_fn >>= fun _ -> read_str_lwt read_fn >|= fun _ -> PProgress
  | SPartUUIDs | SReadTaskRequest -> receive_data_block t ~compressible:true read_fn >|= fun _ -> PProgress
  | SProfileEvents -> receive_data_block t ~compressible:false read_fn >|= fun _ -> PProgress
  | SHello | SPong | STablesStatusResponse -> Lwt.return PProgress

let read_exception_from_bytes (bs:bytes) : exn Lwt.t =
  let pos = ref 0 in
  let read_fn buf off len =
    let remaining = Bytes.length bs - !pos in
    let to_copy = min len remaining in
    Bytes.blit bs !pos buf off to_copy; pos := !pos + to_copy; Lwt.return to_copy
  in
  let rec read_exception_lwt () =
    read_int32_le_lwt read_fn >>= fun code32 ->
    let code = Int32.to_int code32 in
    read_str_lwt read_fn >>= fun name ->
    read_str_lwt read_fn >>= fun msg ->
    read_str_lwt read_fn >>= fun stack ->
    read_byte read_fn >>= fun has_nested ->
    let base = (if name <> "DB::Exception" then name ^ ". " else "") ^ msg ^ ". Stack trace:\n\n" ^ stack in
    (if has_nested <> 0 then
       read_exception_lwt () >|= fun nested_exn ->
         match nested_exn with
         | Errors.Server_exception se -> Some se.message
         | e -> Some (Printexc.to_string e)
     else Lwt.return_none) >>= fun nested ->
    Lwt.return (Errors.Server_exception { code; message = base; nested })
  in
  read_exception_lwt ()
