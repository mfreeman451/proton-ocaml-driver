(* Connection is used via qualified access *)

(** Query result type for execute method *)
type query_result =
  | NoRows  (** No rows returned *)
  | Rows of (Column.value list list * (string * string) list)  (** Rows with column metadata *)

(** Streaming result with both rows and column information *)
type 'a streaming_result = {
  rows: 'a;
  columns: (string * string) list;
}

(** Client type *)
type t = { conn : Connection.t }

(** [create ?host ?port ?database ?user ?password ?tls_config ?compression ?settings ()] 
    creates a new client with the specified configuration *)
val create : ?host:string -> ?port:int -> ?database:string -> ?user:string -> 
             ?password:string -> ?tls_config:Connection.tls_config -> 
             ?compression:Protocol.compression -> ?settings:(string * string) list -> 
             unit -> t

(** [disconnect client] disconnects the client *)
val disconnect : t -> unit Lwt.t

(** [execute client query] executes a query and returns the result *)
val execute : t -> string -> query_result Lwt.t

(** Streaming query functionality *)

(** [query_fold client query ~init ~f] executes a query and folds over results *)
val query_fold : t -> string -> init:'acc -> f:('acc -> Column.value list -> 'acc Lwt.t) -> 'acc Lwt.t

(** [query_iter client query ~f] executes a query and iterates over each row *)  
val query_iter : t -> string -> f:(Column.value list -> unit Lwt.t) -> unit Lwt.t

(** [query_to_seq client query] returns a lazy sequence of rows *)
val query_to_seq : t -> string -> Column.value list Seq.t Lwt.t

(** [query_collect client query] collects all rows (convenience method) *)
val query_collect : t -> string -> Column.value list list Lwt.t

(** Advanced streaming with column metadata *)

(** [query_fold_with_columns client query ~init ~f] fold with column metadata *)
val query_fold_with_columns : t -> string -> init:'acc -> 
  f:('acc -> Column.value list -> (string * string) list -> 'acc Lwt.t) -> 
  'acc streaming_result Lwt.t

(** [query_iter_with_columns client query ~f] iterate with column metadata *)
val query_iter_with_columns : t -> string -> 
  f:(Column.value list -> (string * string) list -> unit Lwt.t) -> 
  (string * string) list Lwt.t

(** Async insert functionality *)

(** [create_async_inserter ?config client table_name] creates a new async inserter *)
val create_async_inserter : ?config:Async_insert.config option -> t -> string -> Async_insert.t

(** [insert_rows client table_name ?columns rows] inserts multiple rows into a table *)
val insert_rows : t -> string -> ?columns:(string * string) list -> Column.value list list -> unit Lwt.t

(** [insert_row client table_name ?columns row] inserts a single row into a table *)
val insert_row : t -> string -> ?columns:(string * string) list -> Column.value list -> unit Lwt.t
