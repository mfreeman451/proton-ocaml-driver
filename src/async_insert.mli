(** Async Insert Implementation for Proton OCaml Driver *)

(** Configuration for async insert behavior *)
type config = {
  table_name: string;         (** Target table name *)
  max_batch_size: int;        (** Maximum rows per batch *)
  max_batch_bytes: int;       (** Maximum bytes per batch *)
  flush_interval: float;      (** Seconds between automatic flushes *)
  max_retries: int;           (** Maximum retry attempts on failure *)
  retry_delay: float;         (** Initial retry delay in seconds *)
  response_timeout: float;    (** Seconds to wait for server end-of-stream *)
}

(** Create default configuration for a table *)
val default_config : string -> config

(** Async inserter instance *)
type t

(** Create a new async inserter 
    @param config Configuration for the inserter
    @param connection Connection to use *)
val create : config -> Connection.t -> t

(** Start the async inserter background tasks *)
val start : t -> unit

(** Stop the async inserter and flush remaining data *)
val stop : t -> unit Lwt.t

(** Add a single row to the buffer 
    @param columns Optional column definitions (name, type) - only needed for first row
    @param inserter The async inserter instance
    @param row Row data as list of values *)
val add_row : ?columns:(string * string) list -> t -> Columns.value list -> unit Lwt.t

(** Add multiple rows to the buffer *)
val add_rows : ?columns:(string * string) list -> t -> Columns.value list list -> unit Lwt.t

(** Force a flush of the current buffer *)
val flush : t -> unit Lwt.t

(** Get buffer statistics: (row_count, byte_size) *)
val get_stats : t -> (int * int) Lwt.t
