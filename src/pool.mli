open Connection

(** Connection pool configuration *)
type pool_config = {
  max_open: int;       (** Maximum number of open connections *)
  max_idle: int;       (** Maximum number of idle connections *)
  max_lifetime: float; (** Maximum lifetime of a connection in seconds *)
  idle_timeout: float; (** Idle timeout for connections in seconds *)
}

(** Connection pool type *)
type connection_pool

(** Default pool configuration *)
val default_pool_config : pool_config

(** [create_pool ?config create_fn] creates a new connection pool with the given
    configuration and connection creation function *)
val create_pool : ?config:pool_config -> (unit -> Connection.t) -> connection_pool

(** [get_connection pool] retrieves a connection from the pool.
    Returns [Ok conn] if successful, [Error msg] if the pool is at capacity. *)
val get_connection : connection_pool -> (Connection.t, string) result

(** [return_connection pool conn] returns a connection to the pool.
    The connection may be closed if the idle pool is full. *)
val return_connection : connection_pool -> Connection.t -> unit

(** [close_pool pool] closes all connections in the pool *)
val close_pool : connection_pool -> unit

(** [pool_stats pool] returns (active_count, idle_count, total_count) *)
val pool_stats : connection_pool -> int * int * int

(** [with_connection pool f] executes function [f] with a connection from the pool.
    The connection is automatically returned to the pool when done.
    Returns [Ok result] on success, [Error msg] on failure. *)
val with_connection : connection_pool -> (Connection.t -> 'a) -> ('a, string) result