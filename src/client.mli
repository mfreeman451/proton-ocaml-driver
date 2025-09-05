open Connection

(** Query result type *)
type query_result =
  | NoRows  (** No rows returned *)
  | Rows of (Columns.value list list * (string * string) list)  (** Rows with column metadata *)

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

(** Async insert functionality *)

(** [create_async_inserter ?config client table_name] creates a new async inserter *)
val create_async_inserter : ?config:Async_insert.config option -> t -> string -> Async_insert.t

(** [insert_rows client table_name ?columns rows] inserts multiple rows into a table *)
val insert_rows : t -> string -> ?columns:(string * string) list -> Columns.value list list -> unit Lwt.t

(** [insert_row client table_name ?columns row] inserts a single row into a table *)
val insert_row : t -> string -> ?columns:(string * string) list -> Columns.value list -> unit Lwt.t
