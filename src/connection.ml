open Protocol
open Varint
open Binary
open Errors
open Read_helpers
open Context
open Block

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
  mutable ic : in_channel option;
  mutable oc : out_channel option;
  mutable connected : bool;
  mutable srv : server_info option;
  ctx : Context.t;
  compression : Protocol.compression;  (* phase 1: Disabled *)
}

let create ?(host="127.0.0.1") ?(port=Defines.default_port)
           ?(database=Defines.default_database)
           ?(user=Defines.default_user)
           ?(password=Defines.default_password)
           ?(settings=[]) () =
  let ctx = Context.make () in
  ctx.settings <- settings;
  {
    host; port; database; user; password;
    ic=None; oc=None; connected=false; srv=None;
    ctx;
    compression = Compress.None;
  }

let with_io t f =
  match t.ic, t.oc with
  | Some ic, Some oc -> f ic oc
  | _ -> raise (Network_error "connection not established")

let connect t =
  if t.connected then ()
  else
    let addr = Unix.inet_addr_of_string t.host in
    let sockaddr = Unix.ADDR_INET (addr, t.port) in
    let fd = Unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
    Unix.setsockopt_float fd Unix.SO_RCVTIMEO Defines.dbms_default_timeout_sec;
    Unix.setsockopt_float fd Unix.SO_SNDTIMEO Defines.dbms_default_timeout_sec;
    Unix.connect fd sockaddr;
    let ic = Unix.in_channel_of_descr fd in
    let oc = Unix.out_channel_of_descr fd in
    t.ic <- Some ic; t.oc <- Some oc; t.connected <- true

let disconnect t =
  (match t.ic with Some ic -> close_in_noerr ic | None -> ());
  (match t.oc with Some oc -> close_out_noerr oc | None -> ());
  t.ic <- None; t.oc <- None; t.connected <- false

let send_hello t =
  with_io t (fun _ic oc ->
    write_varint_int oc (client_packet_to_int Hello);
    write_str oc (Defines.dbms_name ^ " " ^ Defines.client_name);
    write_varint_int oc Defines.client_version_major;
    write_varint_int oc Defines.client_version_minor;
    (* client sends revision in place of version_patch for compat *)
    write_varint_int oc Defines.client_revision;
    write_str oc t.database;
    write_str oc t.user;
    write_str oc t.password;
    flush oc
  )

let receive_hello t =
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
  with_io t (fun _ic oc ->
    write_varint_int oc (client_packet_to_int Query);
    write_str oc query_id;
    let revision =
      match t.srv with Some s -> s.revision | None -> failwith "no server revision"
    in
    if revision >= Defines.dbms_min_revision_with_client_info then begin
      (* Minimal ClientInfo.write mirroring python/clientinfo.py *)
      (* query_kind: INITIAL_QUERY = 1 *)
      write_uint8 oc 1;
      (* initial_user, initial_query_id, initial_address *)
      write_str oc ""; write_str oc ""; write_str oc "0.0.0.0:0";
      if revision >= Defines.dbms_min_protocol_version_with_initial_query_start_time then
        (* start time micros; just write 0 *)
        write_uint64_le oc 0L;
      (* interface TCP = 1 *)
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
        write_uint8 oc 0 (* no otel header *)
    end;

    let settings_as_strings =
      match t.srv with
      | Some s -> s.revision >= Defines.dbms_min_revision_with_settings_serialized_as_strings
      | None -> true
    in
    if settings_as_strings then
      write_settings_as_strings oc t.ctx.settings ~settings_is_important:false
    else
      (* Old style typed settings not implemented in phase 1 *)
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

let receive_data t ~raw ic : Block.t =
  let revision = match t.srv with Some s -> s.revision | None -> 0 in
  (* In phase 1 raw=false or true is same (no compression). *)
  Block.read_block ~revision ic

let receive_packet t : packet =
  with_io t (fun ic _oc ->
    let ptype = read_varint_int ic in
    match Protocol.server_packet_of_int ptype with
    | SData ->
        let b = receive_data t ~raw:false ic in PData b
    | SException ->
        let e = read_exception ic in raise e
    | SProgress -> read_progress t ic; PProgress
    | SEndOfStream -> PEndOfStream
    | SProfileInfo -> read_profile_info t ic; PProfileInfo
    | STotals -> let b = receive_data t ~raw:false ic in PTotals b
    | SExtremes -> let b = receive_data t ~raw:false ic in PExtremes b
    | SLog -> let b = receive_data t ~raw:true ic in PLog b
    | STableColumns ->
        (* Two strings; read & drop *)
        ignore (read_str ic); ignore (read_str ic); PProgress
    | SPartUUIDs
    | SReadTaskRequest
    | SProfileEvents ->
        (* Not implemented in phase 1: read as data to keep stream aligned *)
        let _ = receive_data t ~raw:false ic in PProgress
    | SPong | SHello -> PProgress
  )
