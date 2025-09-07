include Column_types

let trim s = String.trim s

(* caches: type spec (normalized) -> compiled reader *)
let reader_cache : (string, in_channel -> int -> value array) Hashtbl.t = Hashtbl.create 64
let reader_br_cache : (string, Buffered_reader.t -> int -> value array) Hashtbl.t = Hashtbl.create 64

(* cache counters *)
let reader_cache_hits = ref 0
let reader_cache_misses = ref 0
let reader_br_cache_hits = ref 0
let reader_br_cache_misses = ref 0

let normalize_spec (spec:string) =
  String.lowercase_ascii (trim spec)

let rec compile_reader s : (in_channel -> int -> value array) =
  match Column_primitives.reader_primitive_of_spec s with
  | Some r -> r
  | None -> (match Column_datetime.reader_datetime_of_spec s with
             | Some r -> r
             | None -> (match Column_complex.reader_complex_of_spec ~resolver:reader_of_spec s with
                        | Some r -> r
                        | None -> (match Column_lowcardinality.reader_lowcardinality_of_spec ~resolver:reader_of_spec s with
                                   | Some r -> r
                                   | None -> failwith (Printf.sprintf "Unsupported column type: %s" s))))

and reader_of_spec (spec:string) : (in_channel -> int -> value array) =
  let s = normalize_spec spec in
  match Hashtbl.find_opt reader_cache s with
  | Some r -> incr reader_cache_hits; r
  | None ->
    incr reader_cache_misses;
    let r = compile_reader s in
    Hashtbl.add reader_cache s r; r

let rec compile_reader_br s : (Buffered_reader.t -> int -> value array) =
  match Column_primitives.reader_primitive_of_spec_br s with
  | Some r -> r
  | None -> (match Column_datetime.reader_datetime_of_spec_br s with
             | Some r -> r
             | None -> (match Column_complex.reader_complex_of_spec_br ~resolver:reader_of_spec_br s with
                        | Some r -> r
                        | None -> (match Column_lowcardinality.reader_lowcardinality_of_spec_br ~resolver:reader_of_spec_br s with
                                   | Some r -> r
                                   | None -> failwith (Printf.sprintf "Unsupported column type: %s" s))))

and reader_of_spec_br (spec:string) : (Buffered_reader.t -> int -> value array) =
  let s = normalize_spec spec in
  match Hashtbl.find_opt reader_br_cache s with
  | Some r -> incr reader_br_cache_hits; r
  | None ->
    incr reader_br_cache_misses;
    let r = compile_reader_br s in
    Hashtbl.add reader_br_cache s r; r

(* Stats helpers *)
type cache_stats = {
  reader_cache_size: int;
  reader_cache_hits: int;
  reader_cache_misses: int;
  reader_br_cache_size: int;
  reader_br_cache_hits: int;
  reader_br_cache_misses: int;
}

let get_cache_stats () : cache_stats = {
  reader_cache_size = Hashtbl.length reader_cache;
  reader_cache_hits = !reader_cache_hits;
  reader_cache_misses = !reader_cache_misses;
  reader_br_cache_size = Hashtbl.length reader_br_cache;
  reader_br_cache_hits = !reader_br_cache_hits;
  reader_br_cache_misses = !reader_br_cache_misses;
}

let cache_stats_to_string (s:cache_stats) =
  Printf.sprintf
    "readers: hits=%d misses=%d entries=%d | buffered: hits=%d misses=%d entries=%d"
    s.reader_cache_hits s.reader_cache_misses s.reader_cache_size s.reader_br_cache_hits s.reader_br_cache_misses s.reader_br_cache_size

let reset_cache_stats () =
  reader_cache_hits := 0;
  reader_cache_misses := 0;
  reader_br_cache_hits := 0;
  reader_br_cache_misses := 0

let clear_reader_caches () =
  Hashtbl.clear reader_cache;
  Hashtbl.clear reader_br_cache
