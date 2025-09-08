(** {1 Connection Module}
    
    Low-level connection management for Timeplus Proton database.
    This module handles the network connection, TLS configuration, authentication,
    and packet-level communication with the server.
    
    Most users should use the higher-level {!Client} module instead of this module directly.
    
    @since 1.0.0
*)

(** {2 Types} *)

(** TLS/SSL configuration for secure connections.
    
    Controls how the client establishes secure connections to the server.
    By default, TLS is disabled for compatibility with local development setups.
    
    @since 1.0.0 *)
type tls_config = {
  enable_tls: bool;
  (** Whether to use TLS for the connection. Default: false *)
  
  ca_cert_file: string option;
  (** Path to CA certificate file for server verification *)
  
  client_cert_file: string option;
  (** Path to client certificate for mutual TLS authentication *)
  
  client_key_file: string option;
  (** Path to client private key for mutual TLS authentication *)
  
  verify_hostname: bool;
  (** Whether to verify the server's hostname matches the certificate. Default: true *)
  
  insecure_skip_verify: bool;
  (** Skip all certificate verification (INSECURE - only for development). Default: false *)
}

(** Server information received during handshake.
    
    Contains metadata about the connected Proton server including version,
    timezone, and display name.
    
    @since 1.0.0 *)
type server_info = Context.server_info

(** Packet types received from the server during query execution.
    
    Represents different types of data and control packets that can be
    received from the server during query processing.
    
    @since 1.0.0 *)
type packet =
  | PData of Block.t
  (** Data block containing query result rows *)
  
  | PProgress
  (** Progress update for long-running queries *)
  
  | PProfileInfo
  (** Query profiling information *)
  
  | PTotals of Block.t
  (** Aggregated totals for the query *)
  
  | PExtremes of Block.t
  (** Extreme values (min/max) for columns *)
  
  | PLog of Block.t
  (** Server log messages *)
  
  | PEndOfStream
  (** Indicates the end of the result stream *)

(** Connection handle representing an active connection to a Proton server.
    
    Contains all connection state including network socket, TLS session,
    server information, and configuration settings.
    
    @since 1.0.0 *)
type t = {
  host : string;
  (** Server hostname or IP address *)
  
  port : int;
  (** Server port number *)
  
  database : string;
  (** Default database name *)
  
  user : string;
  (** Username for authentication *)
  
  password : string;
  (** Password for authentication *)
  
  tls_config : tls_config;
  (** TLS configuration settings *)
  
  mutable fd : Lwt_unix.file_descr option;
  (** Unix file descriptor for the socket connection *)
  
  mutable tls : Tls_lwt.Unix.t option;
  (** TLS session if TLS is enabled *)
  
  mutable connected : bool;
  (** Whether the connection is currently active *)
  
  mutable srv : server_info option;
  (** Server information received during handshake *)
  
  ctx : Context.t;
  (** Connection context with settings and state *)
  
  compression : Protocol.compression;
  (** Compression method to use for data transfer *)
}

(** {2 Configuration} *)

(** Default TLS configuration with TLS disabled.
    
    Provides a safe default configuration suitable for local development.
    For production use, enable TLS and configure certificates appropriately.
    
    @since 1.0.0 *)
val default_tls_config : tls_config

(** {2 Connection Management} *)

(** [create ?host ?port ?database ?user ?password ?tls_config ?compression ?settings ()]
    creates a new connection handle.
    
    This only creates the connection object; use {!connect} to establish
    the actual network connection.
    
    @param host Server hostname (default: "127.0.0.1")
    @param port Server port (default: 8463)
    @param database Default database (default: "default")
    @param user Username (default: "default")
    @param password Password (default: empty)
    @param tls_config TLS configuration (default: {!default_tls_config})
    @param compression Compression method (default: LZ4)
    @param settings Additional server settings as key-value pairs
    @return A new connection handle
    
    Example:
    {[
      let conn = Connection.create
        ~host:"proton.example.com"
        ~port:8463
        ~user:"myuser"
        ~password:"mypass"
        ~tls_config:{ default_tls_config with enable_tls = true }
        ()
    ]}
    
    @since 1.0.0 *)
val create : ?host:string -> ?port:int -> ?database:string -> ?user:string ->
             ?password:string -> ?tls_config:tls_config -> 
             ?compression:Protocol.compression -> ?settings:(string * string) list ->
             unit -> t

(** [connect conn] establishes a connection to the server.
    
    Performs the network connection, TLS handshake (if configured),
    and authentication with the server.
    
    @param conn The connection handle
    @return A promise that resolves when connected
    @raise Connection_failed if the connection cannot be established
    
    @since 1.0.0 *)
val connect : t -> unit Lwt.t

(** [disconnect conn] closes the connection to the server.
    
    Gracefully closes the network connection and cleans up resources.
    
    @param conn The connection handle
    @return A promise that resolves when disconnected
    
    @since 1.0.0 *)
val disconnect : t -> unit Lwt.t

(** [force_connect conn] ensures the connection is established.
    
    If already connected, returns immediately. Otherwise, calls {!connect}.
    
    @param conn The connection handle
    @return A promise that resolves when connected
    
    @since 1.0.0 *)
val force_connect : t -> unit Lwt.t

(** {2 Query Execution} *)

(** [send_query conn ?query_id query] sends a query to the server.
    
    Sends a complete query including the data termination marker.
    Use this for queries that don't require sending data.
    
    @param conn The connection handle
    @param query_id Optional query identifier for tracking
    @param query The SQL query string
    @return A promise that resolves when the query is sent
    
    @since 1.0.0 *)
val send_query : t -> ?query_id:string -> string -> unit Lwt.t

(** [send_query_without_data_end conn ?query_id query] sends a query without termination.
    
    Sends a query but doesn't send the data termination marker.
    Use this when you need to send data blocks after the query (e.g., for INSERT).
    
    @param conn The connection handle
    @param query_id Optional query identifier
    @param query The SQL query string
    @return A promise that resolves when the query is sent
    
    @since 1.0.0 *)
val send_query_without_data_end : t -> ?query_id:string -> string -> unit Lwt.t

(** [send_data_block conn block] sends a data block to the server.
    
    Used for sending data during INSERT operations or when the server
    requests data.
    
    @param conn The connection handle
    @param block The data block to send
    @return A promise that resolves when the block is sent
    
    @since 1.0.0 *)
val send_data_block : t -> Block.t -> unit Lwt.t

(** [receive_packet conn] receives the next packet from the server.
    
    Waits for and reads the next packet from the server, which could be
    data, progress information, or control packets.
    
    @param conn The connection handle
    @return A promise resolving to the received packet
    @raise Server_exception if the server sends an error
    
    @since 1.0.0 *)
val receive_packet : t -> packet Lwt.t

(** Test helper: parse a server exception packet from raw bytes. *)
val read_exception_from_bytes : bytes -> exn Lwt.t

(** Test helper: read a compressed block payload using the same logic as the connection. *)
val read_compressed_block_lwt : (bytes -> int -> int -> int Lwt.t) -> bytes Lwt.t

(** {2 Protocol Functions} *)

(** [send_hello conn] sends the client handshake message.
    
    Sends client information including version, database, and credentials
    to initiate the connection handshake.
    
    @param conn The connection handle
    @return A promise that resolves when the hello is sent
    
    @since 1.0.0 *)
val send_hello : t -> unit Lwt.t

(** [receive_hello conn] receives and processes the server handshake.
    
    Receives the server's response to the client hello, extracting
    server information and capabilities.
    
    @param conn The connection handle
    @return A promise that resolves when the hello is received
    
    @since 1.0.0 *)
val receive_hello : t -> unit Lwt.t

(** {2 Low-level I/O} *)

(** [receive_data_block conn ~compressible read_fn] reads a data block.
    
    Low-level function to read a data block from the connection,
    handling compression if enabled.
    
    @param conn The connection handle
    @param compressible Whether the block may be compressed
    @param read_fn Function to read bytes from the connection
    @return A promise resolving to the data block
    
    @since 1.0.0 *)
val receive_data_block : t -> compressible:bool -> 
                        (bytes -> int -> int -> int Lwt.t) -> Block.t Lwt.t

(** {2 Utility Functions} *)

(** [hex_of_string s] converts a string to its hexadecimal representation.
    
    Useful for debugging binary protocol data.
    
    @param s The input string
    @return Hexadecimal string representation
    
    @since 1.0.0 *)
val hex_of_string : string -> string
