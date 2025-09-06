(* Buffered reader that can handle both compressed and uncompressed data *)

type t = {
  mutable buffer: bytes;
  mutable pos: int;
  mutable valid: int;
  mutable eof: bool;
  fill_func: bytes -> int -> int -> int;  (* fill buffer function *)
}

(* Create a buffered reader from raw input channel *)
let create_from_channel ic =
  let fill_func buf offset len =
    try input ic buf offset len
    with End_of_file -> 0
  in
  {
    buffer = Bytes.create 8192;
    pos = 0;
    valid = 0;
    eof = false;
    fill_func;
  }

(* Create a buffered reader from a bytes source *)
let create_from_bytes (src:bytes) =
  let src_len = Bytes.length src in
  let cursor = ref 0 in
  let fill_func buf offset len =
    if !cursor >= src_len then 0
    else
      let remaining = src_len - !cursor in
      let to_copy = if len < remaining then len else remaining in
      Bytes.blit src !cursor buf offset to_copy;
      cursor := !cursor + to_copy;
      to_copy
  in
  {
    buffer = Bytes.create 8192;
    pos = 0;
    valid = 0;
    eof = false;
    fill_func;
  }

(* Create a buffered reader that aliases the provided bytes without copying. *)
let create_from_bytes_no_copy (src:bytes) =
  {
    buffer = src;
    pos = 0;
    valid = Bytes.length src;
    eof = false;
    (* One-shot: no refills; subsequent reads past end will mark eof via fill_buffer. *)
    fill_func = (fun _ _ _ -> 0);
  }

(* Note: TLS is handled in async path. For parsing bytes, use create_from_bytes. *)

(* Fill buffer with more data *)
let fill_buffer br =
  if not br.eof && br.pos >= br.valid then begin
    let bytes_read = br.fill_func br.buffer 0 (Bytes.length br.buffer) in
    if bytes_read = 0 then
      br.eof <- true
    else begin
      br.pos <- 0;
      br.valid <- bytes_read
    end
  end

(* Read a single byte *)
let[@inline] input_byte br =
  fill_buffer br;
  if br.eof then raise End_of_file;
  let byte = Bytes.get br.buffer br.pos in
  br.pos <- br.pos + 1;
  Char.code byte

(* Read exactly n bytes into buffer *)
let really_input br buf offset len =
  let rec loop copied =
    if copied >= len then ()
    else begin
      fill_buffer br;
      if br.eof then failwith "Unexpected end of stream";
      let available = br.valid - br.pos in
      let to_copy = min (len - copied) available in
      Bytes.blit br.buffer br.pos buf (offset + copied) to_copy;
      br.pos <- br.pos + to_copy;
      loop (copied + to_copy)
    end
  in
  loop 0

(* Read exactly n bytes and return as string *)
let[@inline] really_input_string br len =
  let buf = Bytes.create len in
  really_input br buf 0 len;
  Bytes.to_string buf
