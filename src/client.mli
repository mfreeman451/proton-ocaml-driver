(** {1 Timeplus Proton OCaml Driver Client}

    This module provides the main client interface for connecting to and interacting with Timeplus
    Proton database servers. It supports query execution, streaming results, and efficient bulk
    inserts.

    @since 1.0.0 *)

(** {2 Types} *)

(** Query result type for synchronous query execution.
    - [NoRows] indicates the query returned no data (e.g., DDL statements)
    - [Rows (data, columns)] contains the result rows and column metadata *)
type query_result =
  | NoRows  (** No rows returned *)
  | Rows of (Column.value list list * (string * string) list)  (** Rows with column metadata *)

type 'a streaming_result = { rows : 'a; columns : (string * string) list }
(** Streaming result container that pairs query results with column metadata. Used by streaming
    functions that need to return both data and schema information.

    @since 1.0.0 *)

type t = { conn : Connection.t }
(** The main client type representing a connection to a Proton server. Contains the underlying
    connection handle used for all database operations. *)

type prepared_statement
(** Prepared statement representation generated from a parameterized query template. The template
    syntax uses double curly braces ([{{name}}]) to mark named parameters. *)

module Prepared : sig
  type t = prepared_statement

  val render : t -> params:(string * Column.value) list -> string
  (** [render stmt ~params] produces the executable SQL string for the prepared statement using the
      supplied parameter bindings. *)

  val placeholder_names : t -> string list
  (** [placeholder_names stmt] returns the unique placeholder names referenced by the prepared
      statement. *)
end

val create :
  ?host:string ->
  ?port:int ->
  ?database:string ->
  ?user:string ->
  ?password:string ->
  ?tls_config:Connection.tls_config ->
  ?compression:Protocol.compression ->
  ?settings:(string * string) list ->
  unit ->
  t
(** [create ?host ?port ?database ?user ?password ?tls_config ?compression ?settings ()] creates a
    new client connection to a Proton server.

    @param host The server hostname (default: "localhost")
    @param port The server port (default: 8463)
    @param database The database to connect to (default: "default")
    @param user The username for authentication (default: "default")
    @param password The password for authentication (default: empty)
    @param tls_config Optional TLS configuration for secure connections
    @param compression Optional compression method (LZ4, ZSTD, etc.)
    @param settings Additional server settings as key-value pairs
    @return A new client instance

    Example:
    {[
      let client =
        Client.create ~host:"proton.example.com" ~port:8463 ~user:"myuser" ~password:"mypass" ()
    ]}

    @since 1.0.0 *)

val disconnect : t -> unit Lwt.t
(** [disconnect client] gracefully closes the connection to the Proton server.

    @param client The client to disconnect
    @return A promise that resolves when disconnection is complete

    @since 1.0.0 *)

val execute : t -> string -> query_result Lwt.t
(** [execute client query] executes a SQL query and returns all results at once.

    This function is best suited for queries that return a manageable amount of data. For large
    result sets, consider using the streaming functions instead.

    @param client The client connection to use
    @param query The SQL query string to execute
    @return A [query_result] containing either [NoRows] or [Rows] with data and column metadata

    Example:
    {[
      let%lwt result = Client.execute client "SELECT * FROM events LIMIT 10" in
      match result with
      | NoRows -> print_endline "No results"
      | Rows (data, columns) -> process_results data columns
    ]}

    @since 1.0.0 *)

val prepare : t -> string -> prepared_statement
(** [prepare client query] parses a parameterized query template into a prepared statement.

    Use [{{parameter_name}}] within the query string to mark parameters. The returned prepared
    statement can be executed multiple times with different parameter bindings.

    Example:
    {[
      let stmt = Client.prepare client "SELECT * FROM events WHERE tenant_id = {{tenant}}" in
      let%lwt _ =
        Client.execute_prepared client stmt ~params:[ ("tenant", Column.String "acme") ]
      in
      Lwt.return_unit
    ]}

    @since 1.1.0 *)

val execute_prepared :
  t -> prepared_statement -> params:(string * Column.value) list -> query_result Lwt.t
(** [execute_prepared client stmt ~params] executes a prepared statement with the provided
    parameters.

    Parameter values are supplied as an association list mapping placeholder names (without braces)
    to {!Column.value} values. All placeholders in the prepared statement must have a corresponding
    parameter entry.

    @since 1.1.0 *)

val execute_with_params : t -> string -> params:(string * Column.value) list -> query_result Lwt.t
(** [execute_with_params client query ~params] executes a parameterized query in one step.

    This is a convenience wrapper that combines {!prepare} and {!execute_prepared} for single-use
    queries. Placeholders use the same [{{name}}] syntax as prepared statements.

    Example:
    {[
      let%lwt rows =
        Client.execute_with_params client
          "SELECT * FROM metrics WHERE name = {{metric}} AND ts >= {{start_ts}}"
          ~params:[ ("metric", Column.String "latency"); ("start_ts", Column.Int64 1_700_000_000L) ]
      in
      Lwt.return rows
    ]}

    @since 1.1.0 *)

(** {2 Streaming Query Functions}

    These functions provide efficient streaming access to query results, allowing processing of
    large datasets without loading all data into memory. *)

val query_fold :
  t -> string -> init:'acc -> f:('acc -> Column.value list -> 'acc Lwt.t) -> 'acc Lwt.t
(** [query_fold client query ~init ~f] executes a query and folds over the results.

    Processes query results incrementally using a fold function, which is memory-efficient for large
    result sets.

    @param client The client connection to use
    @param query The SQL query string to execute
    @param init The initial accumulator value
    @param f The fold function applied to each row: (accumulator -> row -> new_accumulator)
    @return The final accumulator value after processing all rows

    Example:
    {[
      let%lwt sum =
        Client.query_fold client "SELECT value FROM metrics" ~init:0.0 ~f:(fun acc row ->
            match row with [ Column.Float v ] -> Lwt.return (acc +. v) | _ -> Lwt.return acc)
    ]}

    @since 1.0.0 *)

val query_fold_prepared :
  t ->
  prepared_statement ->
  params:(string * Column.value) list ->
  init:'acc ->
  f:('acc -> Column.value list -> 'acc Lwt.t) ->
  'acc Lwt.t
(** [query_fold_prepared client stmt ~params ~init ~f] folds over results of a prepared query.

    This variant accepts a pre-parsed statement created with {!prepare}, allowing it to be reused
    with different parameter bindings.

    @since 1.1.0 *)

val query_fold_with_params :
  t ->
  string ->
  params:(string * Column.value) list ->
  init:'acc ->
  f:('acc -> Column.value list -> 'acc Lwt.t) ->
  'acc Lwt.t
(** [query_fold_with_params client query ~params ~init ~f] folds over a parameterized query in one
    step. Equivalent to combining {!prepare} and {!query_fold_prepared} for a single execution.

    @since 1.1.0 *)

val query_iter : t -> string -> f:(Column.value list -> unit Lwt.t) -> unit Lwt.t
(** [query_iter client query ~f] executes a query and applies a function to each row.

    Useful for side-effecting operations on query results without accumulating values.

    @param client The client connection to use
    @param query The SQL query string to execute
    @param f The function to apply to each row
    @return A promise that resolves when all rows have been processed

    Example:
    {[
      Client.query_iter client "SELECT * FROM logs" ~f:(fun row ->
          print_row row;
          Lwt.return_unit)
    ]}

    @since 1.0.0 *)

val query_iter_prepared :
  t ->
  prepared_statement ->
  params:(string * Column.value) list ->
  f:(Column.value list -> unit Lwt.t) ->
  unit Lwt.t
(** [query_iter_prepared client stmt ~params ~f] iterates over the rows returned by a prepared
    query.

    @since 1.1.0 *)

val query_iter_with_params :
  t ->
  string ->
  params:(string * Column.value) list ->
  f:(Column.value list -> unit Lwt.t) ->
  unit Lwt.t
(** [query_iter_with_params client query ~params ~f] iterates over a parameterized query without
    explicitly managing a prepared statement.

    @since 1.1.0 *)

val query_to_seq : t -> string -> Column.value list Seq.t Lwt.t
(** [query_to_seq client query] executes a query and returns results as a lazy sequence.

    The sequence allows for on-demand processing of results using OCaml's [Seq] module.

    @param client The client connection to use
    @param query The SQL query string to execute
    @return A promise resolving to a lazy sequence of rows

    Example:
    {[
      let%lwt seq = Client.query_to_seq client "SELECT * FROM events" in
      let first_10 = Seq.take 10 seq |> List.of_seq
    ]}

    @since 1.0.0 *)

val query_to_seq_prepared :
  t -> prepared_statement -> params:(string * Column.value) list -> Column.value list Seq.t Lwt.t
(** [query_to_seq_prepared client stmt ~params] executes a prepared query and exposes the rows as a
    lazy sequence.

    @since 1.1.0 *)

val query_to_seq_with_params :
  t -> string -> params:(string * Column.value) list -> Column.value list Seq.t Lwt.t
(** [query_to_seq_with_params client query ~params] executes a parameterized query and returns a
    lazy sequence without explicitly creating a prepared statement.

    @since 1.1.0 *)

val query_collect : t -> string -> Column.value list list Lwt.t
(** [query_collect client query] executes a query and collects all results into a list.

    Convenience function that loads all query results into memory at once. Use with caution for
    large result sets.

    @param client The client connection to use
    @param query The SQL query string to execute
    @return A promise resolving to a list of all rows

    @since 1.0.0 *)

val query_collect_prepared :
  t -> prepared_statement -> params:(string * Column.value) list -> Column.value list list Lwt.t
(** [query_collect_prepared client stmt ~params] executes a prepared query and collects all rows.

    @since 1.1.0 *)

val query_collect_with_params :
  t -> string -> params:(string * Column.value) list -> Column.value list list Lwt.t
(** [query_collect_with_params client query ~params] executes a parameterized query and collects all
    rows without manually preparing it.

    @since 1.1.0 *)

(** {2 Advanced Streaming with Column Metadata}

    These functions provide access to both query results and column schema information, useful when
    the structure of results needs to be examined dynamically. *)

val query_fold_with_columns :
  t ->
  string ->
  init:'acc ->
  f:('acc -> Column.value list -> (string * string) list -> 'acc Lwt.t) ->
  'acc streaming_result Lwt.t
(** [query_fold_with_columns client query ~init ~f] folds over query results with column metadata.

    Similar to [query_fold] but provides column information to the fold function, allowing
    schema-aware processing of results.

    @param client The client connection to use
    @param query The SQL query string to execute
    @param init The initial accumulator value
    @param f The fold function: (accumulator -> row -> columns -> new_accumulator)
    @return A [streaming_result] containing the final accumulator and column metadata

    Example:
    {[
      let%lwt result =
        Client.query_fold_with_columns client "SELECT name, age FROM users" ~init:[]
          ~f:(fun acc row cols ->
            let record = List.combine cols row in
            Lwt.return (record :: acc))
    ]}

    @since 1.0.0 *)

val query_fold_with_columns_prepared :
  t ->
  prepared_statement ->
  params:(string * Column.value) list ->
  init:'acc ->
  f:('acc -> Column.value list -> (string * string) list -> 'acc Lwt.t) ->
  'acc streaming_result Lwt.t
(** [query_fold_with_columns_prepared client stmt ~params ~init ~f] folds over the rows and columns
    of a prepared query.

    @since 1.1.0 *)

val query_fold_with_columns_with_params :
  t ->
  string ->
  params:(string * Column.value) list ->
  init:'acc ->
  f:('acc -> Column.value list -> (string * string) list -> 'acc Lwt.t) ->
  'acc streaming_result Lwt.t
(** [query_fold_with_columns_with_params client query ~params ~init ~f] folds over a parameterized
    query, returning both accumulated rows and column metadata.

    @since 1.1.0 *)

val query_iter_with_columns :
  t ->
  string ->
  f:(Column.value list -> (string * string) list -> unit Lwt.t) ->
  (string * string) list Lwt.t
(** [query_iter_with_columns client query ~f] iterates over query results with column metadata.

    Applies a function to each row along with column information.

    @param client The client connection to use
    @param query The SQL query string to execute
    @param f The function to apply to each row with columns
    @return A promise resolving to the column metadata

    @since 1.0.0 *)

val query_iter_with_columns_prepared :
  t ->
  prepared_statement ->
  params:(string * Column.value) list ->
  f:(Column.value list -> (string * string) list -> unit Lwt.t) ->
  (string * string) list Lwt.t
(** [query_iter_with_columns_prepared client stmt ~params ~f] iterates over rows of a prepared query
    while providing column metadata.

    @since 1.1.0 *)

val query_iter_with_columns_with_params :
  t ->
  string ->
  params:(string * Column.value) list ->
  f:(Column.value list -> (string * string) list -> unit Lwt.t) ->
  (string * string) list Lwt.t
(** [query_iter_with_columns_with_params client query ~params ~f] iterates over a parameterized
    query, returning the final column metadata.

    @since 1.1.0 *)

(** {2 Data Insertion Functions}

    Functions for inserting data into Proton tables, including support for high-performance
    asynchronous bulk inserts. *)

val create_async_inserter : ?config:Async_insert.config option -> t -> string -> Async_insert.t
(** [create_async_inserter ?config client table_name] creates an asynchronous bulk inserter.

    Creates an inserter that batches inserts for improved performance. The inserter manages its own
    buffer and flushes data according to the configuration.

    @param config Optional configuration for the inserter (batch size, flush interval, etc.)
    @param client The client connection to use
    @param table_name The target table for inserts
    @return A new [Async_insert.t] instance

    @see 'Async_insert' for detailed configuration options

    Example:
    {[
      let inserter = Client.create_async_inserter client "events" in
      Async_insert.insert inserter row_data
    ]}

    @since 1.0.0 *)

val insert_rows :
  t -> string -> ?columns:(string * string) list -> Column.value list list -> unit Lwt.t
(** [insert_rows client table_name ?columns rows] performs a synchronous bulk insert.

    Inserts multiple rows in a single operation. More efficient than inserting rows individually for
    batch operations.

    @param client The client connection to use
    @param table_name The target table name
    @param columns Optional column names and types (inferred if not provided)
    @param rows List of rows to insert, each row is a list of [Column.value]
    @return A promise that resolves when the insert is complete

    Example:
    {[
      let rows =
        [ [ Column.String "event1"; Column.Int 42 ]; [ Column.String "event2"; Column.Int 43 ] ]
      in
      Client.insert_rows client "events" ~columns:[ ("name", "String"); ("value", "Int32") ] rows
    ]}

    @since 1.0.0 *)

val insert_row : t -> string -> ?columns:(string * string) list -> Column.value list -> unit Lwt.t
(** [insert_row client table_name ?columns row] inserts a single row into a table.

    Convenience function for inserting a single row. For multiple rows, use [insert_rows] or
    [create_async_inserter] for better performance.

    @param client The client connection to use
    @param table_name The target table name
    @param columns Optional column names and types
    @param row The row data as a list of [Column.value]
    @return A promise that resolves when the insert is complete

    Example:
    {[
      Client.insert_row client "events"
        ~columns:[ ("timestamp", "DateTime"); ("message", "String") ]
        [ Column.DateTime (Unix.time ()); Column.String "Application started" ]
    ]}

    @since 1.0.0 *)
