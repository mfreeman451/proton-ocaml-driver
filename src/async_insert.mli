(** {1 Asynchronous Bulk Insert Module}
    
    This module provides high-performance asynchronous bulk insert capabilities
    for Timeplus Proton. It manages buffering, batching, and automatic flushing
    of data to optimize throughput for streaming data ingestion.
    
    The inserter operates in the background, automatically batching rows and
    sending them to the server when thresholds are met or flush intervals expire.
    
    @since 1.0.0
*)

(** {2 Types} *)

(** Configuration parameters for controlling async insert behavior.
    These settings allow fine-tuning of batching, retry logic, and performance characteristics. *)
type config = {
  table_name: string;
  (** The name of the target table for inserts *)
  max_batch_size: int;
  (** Maximum number of rows to accumulate before triggering a flush.
      Larger values improve throughput but increase memory usage and latency. *)
  max_batch_bytes: int;
  (** Maximum size in bytes for a single batch.
      Prevents excessive memory usage for large rows. *)
  flush_interval: float;
  (** Time in seconds between automatic flushes.
      Ensures data is sent even during low-activity periods. *)
  max_retries: int;
  (** Maximum number of retry attempts when an insert fails.
      Uses exponential backoff between retries. *)
  retry_delay: float;
  (** Initial delay in seconds before the first retry.
      Subsequent retries use exponential backoff (2x, 4x, 8x, etc.). *)
  response_timeout: float;
  (** Maximum time in seconds to wait for server acknowledgment.
      Prevents indefinite blocking on network issues. *)
}

(** [default_config table_name] creates a default configuration for the specified table.
    
    Default values:
    - max_batch_size: 100,000 rows
    - max_batch_bytes: 10 MB
    - flush_interval: 1.0 second
    - max_retries: 3
    - retry_delay: 0.1 seconds
    - response_timeout: 5.0 seconds
    
    @param table_name The target table for inserts
    @return A configuration with sensible defaults
    
    Example:
    {[
      let config = Async_insert.default_config "events" in
      let custom_config = { config with max_batch_size = 50000 }
    ]}
    
    @since 1.0.0 *)
val default_config : string -> config

(** Abstract type representing an async inserter instance.
    Each inserter manages its own buffer, flush timer, and retry logic. *)
type t

(** {2 Creation and Lifecycle} *)

(** [create config connection] creates a new async inserter instance.
    
    The inserter is created in a stopped state. Call {!start} to begin processing.
    
    @param config Configuration parameters for the inserter
    @param connection The database connection to use for inserts
    @return A new inserter instance
    
    Example:
    {[
      let config = Async_insert.default_config "events" in
      let inserter = Async_insert.create config client.conn in
      Async_insert.start inserter
    ]}
    
    @since 1.0.0 *)
val create : config -> Connection.t -> t

(** [start inserter] starts the background processing tasks.
    
    Begins the flush timer and enables automatic batching.
    This function returns immediately; processing happens in the background.
    
    @param inserter The inserter to start
    
    @since 1.0.0 *)
val start : t -> unit

(** [stop inserter] stops the inserter and flushes any remaining data.
    
    Gracefully shuts down the inserter, ensuring all buffered data is sent
    to the server before returning. After stopping, the inserter cannot be restarted.
    
    @param inserter The inserter to stop
    @return A promise that resolves when all data has been flushed
    
    @since 1.0.0 *)
val stop : t -> unit Lwt.t

(** {2 Data Insertion} *)

(** [add_row ?columns inserter row] adds a single row to the insert buffer.
    
    The row is added to the internal buffer and will be sent to the server
    when batching thresholds are met or a flush occurs.
    
    @param columns Optional column definitions as [(name, type)] pairs.
                   Only required for the first row if not previously specified.
    @param inserter The async inserter instance
    @param row The row data as a list of {!Column.value} items
    @return A promise that resolves when the row is buffered (not necessarily sent)
    
    Example:
    {[
      Async_insert.add_row inserter
        ~columns:[("timestamp", "DateTime"); ("value", "Float64")]
        [Column.DateTime (Unix.time ()); Column.Float 42.0]
    ]}
    
    @since 1.0.0 *)
val add_row : ?columns:(string * string) list -> t -> Column.value list -> unit Lwt.t

(** [add_rows ?columns inserter rows] adds multiple rows to the insert buffer.
    
    More efficient than calling {!add_row} multiple times. The rows may be
    split across multiple batches if they exceed configured thresholds.
    
    @param columns Optional column definitions (only needed once)
    @param inserter The async inserter instance  
    @param rows List of rows to insert
    @return A promise that resolves when all rows are buffered
    
    Example:
    {[
      let rows = List.init 1000 (fun i ->
        [Column.Int i; Column.String (Printf.sprintf "event_%d" i)]
      ) in
      Async_insert.add_rows inserter rows
    ]}
    
    @since 1.0.0 *)
val add_rows : ?columns:(string * string) list -> t -> Column.value list list -> unit Lwt.t

(** {2 Buffer Management} *)

(** [flush inserter] forces an immediate flush of buffered data.
    
    Sends all currently buffered rows to the server immediately,
    regardless of batch size or timer settings. Useful for ensuring
    data persistence at specific points in your application.
    
    @param inserter The async inserter instance
    @return A promise that resolves when the flush is complete
    
    @since 1.0.0 *)
val flush : t -> unit Lwt.t

(** [get_stats inserter] returns current buffer statistics.
    
    Provides insight into the current state of the internal buffer,
    useful for monitoring and debugging.
    
    @param inserter The async inserter instance
    @return A tuple of [(row_count, byte_size)] representing the current buffer state
    
    Example:
    {[
      let%lwt (rows, bytes) = Async_insert.get_stats inserter in
      Printf.printf "Buffer contains %d rows (%d bytes)\n" rows bytes
    ]}
    
    @since 1.0.0 *)
val get_stats : t -> (int * int) Lwt.t
