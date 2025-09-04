open Protocol
open Varint
open Binary
open Errors
open Read_helpers
open Context
open Block

type tls_config = {
  enable_tls: bool;
  ca_cert_file: string option;  (* Custom CA certificate file *)
  client_cert_file: string option;  (* Client certificate for mTLS *)
  client_key_file: string option;   (* Client private key for mTLS *)
  verify_hostname: bool;
  insecure_skip_verify: bool;
}

type server_info = Context.server_info

type packet =
  | PData of Block.t
  | PProgress  (* we skip payload in phase 1 after reading it *)
  | PProfileInfo  (* read & drop *)
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
  mutable ic : in_channel option;
  mutable oc : out_channel option;
  mutable ssl_sock : Ssl.socket option;
  mutable br : Buffered_reader.t option; (* persistent reader for TLS mode *)
  mutable connected : bool;
  mutable srv : server_info option;
  ctx : Context.t;
  compression : Protocol.compression;
  mutable compress_reader : Compress.reader option;
  mutable compress_writer : Compress.writer option;
}

let default_tls_config = {
  enable_tls = false;
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
           ?(compression=Compress.None)
           ?(settings=[]) () =
  let ctx = Context.make () in
  ctx.settings <- settings;
  {
    host; port; database; user; password;
    tls_config;
    ic=None; oc=None; ssl_sock=None; br=None; connected=false; srv=None;
    ctx;
    compression;
    compress_reader=None;
    compress_writer=None;
  }

let with_io t f =
  match t.ic, t.oc with
  | Some ic, Some oc -> f ic oc
  | _ -> raise (Network_error "connection not established")

let setup_ssl_socket t fd : Ssl.socket =
  let ctx = Ssl.create_context Ssl.TLSv1_2 Ssl.Client_context in
  (match t.tls_config.ca_cert_file with
   | Some cafile -> Ssl.load_verify_locations ctx cafile ""
   | None -> ignore (Ssl.set_default_verify_paths ctx));
  if not t.tls_config.insecure_skip_verify then
    Ssl.set_verify ctx [Ssl.Verify_peer] (Some Ssl.client_verify_callback)
  else
    Ssl.set_verify ctx [] None;
  (match t.tls_config.client_cert_file, t.tls_config.client_key_file with
   | Some cert, Some key -> Ssl.use_certificate ctx cert key
   | Some _, None | None, Some _ -> raise (Invalid_argument "Both client_cert_file and client_key_file must be set for mTLS")
   | None, None -> ());
  let ssl = Ssl.embed_socket fd ctx in
  (try Ssl.set_client_SNI_hostname ssl t.host with _ -> ());
  if t.tls_config.verify_hostname && not t.tls_config.insecure_skip_verify then (
    (try Ssl.set_host ssl t.host with _ -> ());
  );
  Ssl.Runtime_lock.connect ssl;
  if not t.tls_config.insecure_skip_verify then (
    try Ssl.verify ssl with
    | Ssl.Verify_error _ -> raise (Network_error "TLS verify failed")
    | Ssl.Certificate_error msg -> raise (Network_error ("TLS certificate error: " ^ msg))
  );
  ssl

let connect t =
  if t.connected then ()
  else
    let addr = Unix.inet_addr_of_string t.host in
    let sockaddr = Unix.ADDR_INET (addr, t.port) in
    let fd = Unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
    Unix.setsockopt_float fd Unix.SO_RCVTIMEO Defines.dbms_default_timeout_sec;
    Unix.setsockopt_float fd Unix.SO_SNDTIMEO Defines.dbms_default_timeout_sec;
    Unix.connect fd sockaddr;
    if t.tls_config.enable_tls then begin
      let ssl = setup_ssl_socket t fd in
      t.ssl_sock <- Some ssl;
      t.br <- Some (Buffered_reader.create_from_ssl ssl)
    end else begin
      let ic = Unix.in_channel_of_descr fd in
      let oc = Unix.out_channel_of_descr fd in
      t.ic <- Some ic; 
      t.oc <- Some oc;
      if t.compression <> Compress.None then begin
        t.compress_reader <- Some (Compress.create_reader ic t.compression);
        t.compress_writer <- Some (Compress.create_writer oc t.compression)
      end
    end;
    t.connected <- true

let disconnect t =
  (match t.ssl_sock with Some s -> (try ignore (Ssl.close_notify s) with _ -> ()); Ssl.shutdown s | None -> ());
  (match t.ic with Some ic -> close_in_noerr ic | None -> ());
  (match t.oc with Some oc -> close_out_noerr oc | None -> ());
  t.ic <- None; 
  t.oc <- None;
  t.ssl_sock <- None;
  t.br <- None;
  t.compress_reader <- None;
  t.compress_writer <- None;
  t.connected <- false

let send_hello t =
  if t.tls_config.enable_tls then begin
    let buf = Buffer.create 128 in
    let write_uint8 v = Buffer.add_char buf (Char.chr v) in
    let _write_int32_le (_:int32) = () in
    let rec write_varint_int' n =
      let x = n land 0x7f in
      let n' = n lsr 7 in
      if n' = 0 then write_uint8 x
      else (write_uint8 (x lor 0x80); write_varint_int' n')
    in
    let write_str' s =
      write_varint_int' (String.length s);
      Buffer.add_string buf s
    in
    write_varint_int' (client_packet_to_int Hello);
    write_str' (Defines.dbms_name ^ " " ^ Defines.client_name);
    write_varint_int' Defines.client_version_major;
    write_varint_int' Defines.client_version_minor;
    write_varint_int' Defines.client_revision;
    write_str' t.database;
    write_str' t.user;
    write_str' t.password;
    let data = Buffer.contents buf in
    match t.ssl_sock with
    | Some s -> ignore (Ssl.Runtime_lock.write_substring s data 0 (String.length data)); Ssl.Runtime_lock.flush s
    | None -> failwith "TLS socket not initialized"
  end else
    with_io t (fun _ic oc ->
      write_varint_int oc (client_packet_to_int Hello);
      write_str oc (Defines.dbms_name ^ " " ^ Defines.client_name);
      write_varint_int oc Defines.client_version_major;
      write_varint_int oc Defines.client_version_minor;
      write_varint_int oc Defines.client_revision;
      write_str oc t.database;
      write_str oc t.user;
      write_str oc t.password;
      flush oc
    )

let receive_hello t =
  if t.tls_config.enable_tls then begin
    let br = match t.br with Some br -> br | None -> failwith "TLS reader not initialized" in
    let p = Binary.read_varint_int_br br in
    match Protocol.server_packet_of_int p with
    | SHello ->
        let server_name = Binary.read_str_br br in
        let version_major = Binary.read_varint_int_br br in
        let version_minor = Binary.read_varint_int_br br in
        let server_revision = Binary.read_varint_int_br br in
        let server_timezone =
          if server_revision >= Defines.dbms_min_revision_with_server_timezone
          then Some (Binary.read_str_br br) else None
        in
        let server_display_name =
          if server_revision >= Defines.dbms_min_revision_with_server_display_name
          then Binary.read_str_br br else ""
        in
        let version_patch =
          if server_revision >= Defines.dbms_min_revision_with_version_patch
          then Binary.read_varint_int_br br else server_revision
        in
        let si = {
          Context.name = server_name;
          version_major; version_minor; version_patch;
          revision = server_revision;
          timezone = server_timezone;
          display_name = server_display_name;
        } in
        t.srv <- Some si; t.ctx.server_info <- Some si
    | SException -> failwith "Exception during TLS hello not supported via br helper"
    | other -> raise (Unexpected_packet (Printf.sprintf "Expected HELLO, got %d" p))
  end else
    with_io t (fun ic _oc ->
      let p = read_varint_int ic in
      match Protocol.server_packet_of_int p with
      | SHello ->
          let server_name = read_str ic in
          let version_major = read_varint_int ic in
          let version_minor = read_varint_int ic in
          let server_revision = read_varint_int ic in
          let server_timezone =
            if server_revision >= Defines.dbms_min_revision_with_server_timezone
            then Some (read_str ic) else None
          in
          let server_display_name =
            if server_revision >= Defines.dbms_min_revision_with_server_display_name
            then read_str ic else ""
          in
          let version_patch =
            if server_revision >= Defines.dbms_min_revision_with_version_patch
            then read_varint_int ic else server_revision
          in
          let si = {
            Context.name = server_name;
            version_major; version_minor; version_patch;
            revision = server_revision;
            timezone = server_timezone;
            display_name = server_display_name;
          } in
          t.srv <- Some si; t.ctx.server_info <- Some si
      | SException ->
          let e = Read_helpers.read_exception ic in raise e
      | other ->
          raise (Unexpected_packet (Printf.sprintf "Expected HELLO, got %d" p))
    )

let force_connect t =
  if not t.connected then (connect t; send_hello t; receive_hello t)

let write_settings_as_strings oc settings ~settings_is_important =
  List.iter (fun (k,v) ->
    write_str oc k;
    write_uint8 oc (if settings_is_important then 1 else 0);
    write_str oc v
  ) settings;
  write_str oc "" (* end-of-settings *)

let send_query t ?(query_id="") (query:string) =
  force_connect t;
  if t.tls_config.enable_tls then begin
    let buf = Buffer.create 256 in
    let write_uint8 v = Buffer.add_char buf (Char.chr v) in
    let write_int32_le v =
      let v = Int32.to_int v in
      Buffer.add_char buf (Char.chr (v land 0xFF));
      Buffer.add_char buf (Char.chr ((v lsr 8) land 0xFF));
      Buffer.add_char buf (Char.chr ((v lsr 16) land 0xFF));
      Buffer.add_char buf (Char.chr ((v lsr 24) land 0xFF))
    in
    let write_uint64_le v =
      let low = Int64.to_int (Int64.logand v 0xFFFFFFFFL) in
      let high = Int64.to_int (Int64.shift_right_logical v 32) in
      write_int32_le (Int32.of_int low);
      write_int32_le (Int32.of_int high)
    in
    let rec write_varint_int' n =
      let x = n land 0x7f in
      let n' = n lsr 7 in
      if n' = 0 then Buffer.add_char buf (Char.chr x)
      else (Buffer.add_char buf (Char.chr (x lor 0x80)); write_varint_int' n')
    in
    let write_str' s =
      write_varint_int' (String.length s);
      Buffer.add_string buf s
    in
    Buffer.clear buf;
    write_varint_int' (client_packet_to_int Query);
    write_str' query_id;
    let revision = match t.srv with Some s -> s.revision | None -> failwith "no server revision" in
    if revision >= Defines.dbms_min_revision_with_client_info then begin
      write_uint8 1;
      write_str' ""; write_str' ""; write_str' "0.0.0.0:0";
      if revision >= Defines.dbms_min_protocol_version_with_initial_query_start_time then write_uint64_le 0L;
      write_uint8 1;
      write_str' (try Sys.getenv "USER" with _ -> "");
      write_str' (Unix.gethostname ());
      write_str' (Defines.dbms_name ^ " " ^ Defines.client_name);
      write_varint_int' Defines.client_version_major;
      write_varint_int' Defines.client_version_minor;
      write_varint_int' Defines.client_revision;
      if revision >= Defines.dbms_min_revision_with_quota_key_in_client_info then write_str' "";
      if revision >= Defines.dbms_min_protocol_version_with_distributed_depth then write_varint_int' 0;
      if revision >= Defines.dbms_min_revision_with_version_patch then write_varint_int' Defines.client_version_patch;
      if revision >= Defines.dbms_min_revision_with_opentelemetry then write_uint8 0
    end;
    let settings_as_strings = match t.srv with Some s -> s.revision >= Defines.dbms_min_revision_with_settings_serialized_as_strings | None -> true in
    if settings_as_strings then
      List.iter (fun (k,v) -> write_str' k; write_uint8 0; write_str' v) t.ctx.settings
    else write_str' "";
    if revision >= Defines.dbms_min_revision_with_interserver_secret then write_str' "";
    write_varint_int' (qps_to_int Complete);
    write_varint_int' (compression_to_int t.compression);
    write_str' query;
    let data = Buffer.contents buf in
    match t.ssl_sock with
    | Some s -> ignore (Ssl.Runtime_lock.write_substring s data 0 (String.length data)); Ssl.Runtime_lock.flush s
    | None -> failwith "TLS socket not initialized"
  end else
    with_io t (fun _ic oc ->
      write_varint_int oc (client_packet_to_int Query);
      write_str oc query_id;
      let revision =
        match t.srv with Some s -> s.revision | None -> failwith "no server revision"
      in
      if revision >= Defines.dbms_min_revision_with_client_info then begin
        write_uint8 oc 1;
        write_str oc ""; write_str oc ""; write_str oc "0.0.0.0:0";
        if revision >= Defines.dbms_min_protocol_version_with_initial_query_start_time then
          write_uint64_le oc 0L;
        write_uint8 oc 1;
        write_str oc (try Sys.getenv "USER" with _ -> "");
        write_str oc (Unix.gethostname ());
        write_str oc (Defines.dbms_name ^ " " ^ Defines.client_name);
        write_varint_int oc Defines.client_version_major;
        write_varint_int oc Defines.client_version_minor;
        write_varint_int oc Defines.client_revision;
        if revision >= Defines.dbms_min_revision_with_quota_key_in_client_info then
          write_str oc "";
        if revision >= Defines.dbms_min_protocol_version_with_distributed_depth then
          write_varint_int oc 0;
        if revision >= Defines.dbms_min_revision_with_version_patch then
          write_varint_int oc Defines.client_version_patch;
        if revision >= Defines.dbms_min_revision_with_opentelemetry then
          write_uint8 oc 0
      end;
      let settings_as_strings =
        match t.srv with
        | Some s -> s.revision >= Defines.dbms_min_revision_with_settings_serialized_as_strings
        | None -> true
      in
      if settings_as_strings then
        write_settings_as_strings oc t.ctx.settings ~settings_is_important:false
      else
        write_str oc "";
      if revision >= Defines.dbms_min_revision_with_interserver_secret then
        write_str oc "";
      write_varint_int oc (qps_to_int Complete);
      write_varint_int oc (compression_to_int t.compression);
      write_str oc query;
      flush oc
    )

let read_progress t ic =
  let revision =
    match t.srv with Some s -> s.revision | None -> 0
  in
  let _rows = read_varint ic in
  let _bytes = read_varint ic in
  if revision >= Defines.dbms_min_revision_with_total_rows_in_progress then ignore (read_varint ic);
  if revision >= Defines.dbms_min_revision_with_client_write_info then begin
    ignore (read_varint ic); ignore (read_varint ic)
  end

let read_profile_info _t ic =
  (* rows, blocks, bytes (varint), applied_limit (u8), rows_before_limit (varint), calculated_rows_before_limit (u8) *)
  ignore (read_varint ic); ignore (read_varint ic); ignore (read_varint ic);
  ignore (read_uint8 ic); ignore (read_varint ic); ignore (read_uint8 ic)

let receive_data_plain t ~raw ic : Block.t =
  let revision = match t.srv with Some s -> s.revision | None -> 0 in
  if raw || t.compression = Compress.None then
    Block.read_block ~revision ic
  else
    let decompressed_data = Compress.read_compressed_block ic t.compression in
    let br = Buffered_reader.create_from_bytes decompressed_data in
    Block.read_block_br ~revision br

let receive_data_tls t ~raw br : Block.t =
  let revision = match t.srv with Some s -> s.revision | None -> 0 in
  if raw || t.compression = Compress.None then
    Block.read_block_br ~revision br
  else
    let decompressed_data = Compress.read_compressed_block_br br t.compression in
    let br2 = Buffered_reader.create_from_bytes decompressed_data in
    Block.read_block_br ~revision br2

let receive_packet t : packet =
  if t.tls_config.enable_tls then begin
    let br = match t.br with Some br -> br | None -> failwith "TLS reader not initialized" in
    let ptype = Binary.read_varint_int_br br in
    match Protocol.server_packet_of_int ptype with
    | SData -> let b = receive_data_tls t ~raw:false br in PData b
    | SException -> failwith "Server exception over TLS not yet parsed via br"
    | SProgress ->
        let revision = match t.srv with Some s -> s.revision | None -> 0 in
        let _rows = Binary.read_varint_int_br br in
        let _bytes = Binary.read_varint_int_br br in
        if revision >= Defines.dbms_min_revision_with_total_rows_in_progress then ignore (Binary.read_varint_int_br br);
        if revision >= Defines.dbms_min_revision_with_client_write_info then begin
          ignore (Binary.read_varint_int_br br); ignore (Binary.read_varint_int_br br)
        end; PProgress
    | SEndOfStream -> PEndOfStream
    | SProfileInfo ->
        ignore (Binary.read_varint_int_br br); ignore (Binary.read_varint_int_br br); ignore (Binary.read_varint_int_br br);
        ignore (Binary.read_uint8_br br); ignore (Binary.read_varint_int_br br); ignore (Binary.read_uint8_br br); PProfileInfo
    | STotals -> let b = receive_data_tls t ~raw:false br in PTotals b
    | SExtremes -> let b = receive_data_tls t ~raw:false br in PExtremes b
    | SLog -> let b = receive_data_tls t ~raw:true br in PLog b
    | STableColumns -> ignore (Binary.read_str_br br); ignore (Binary.read_str_br br); PProgress
    | SPartUUIDs | SReadTaskRequest | SProfileEvents -> let _ = receive_data_tls t ~raw:false br in PProgress
    | SPong | SHello -> PProgress
  end else
    with_io t (fun ic _oc ->
      let ptype = read_varint_int ic in
      match Protocol.server_packet_of_int ptype with
      | SData -> let b = receive_data_plain t ~raw:false ic in PData b
      | SException -> let e = read_exception ic in raise e
      | SProgress -> read_progress t ic; PProgress
      | SEndOfStream -> PEndOfStream
      | SProfileInfo -> read_profile_info t ic; PProfileInfo
      | STotals -> let b = receive_data_plain t ~raw:false ic in PTotals b
      | SExtremes -> let b = receive_data_plain t ~raw:false ic in PExtremes b
      | SLog -> let b = receive_data_plain t ~raw:true ic in PLog b
      | STableColumns -> ignore (read_str ic); ignore (read_str ic); PProgress
      | SPartUUIDs | SReadTaskRequest | SProfileEvents -> let _ = receive_data_plain t ~raw:false ic in PProgress
      | SPong | SHello -> PProgress
    )
