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
val disconnect : t -> unit

(** [execute client query] executes a query and returns the result *)
val execute : t -> string -> query_result