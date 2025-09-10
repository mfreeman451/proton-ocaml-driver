(** {1 Protocol Module}

    Wire protocol definitions for Timeplus Proton client-server communication. This module defines
    packet types, compression methods, and other protocol-level constants used in the binary
    protocol.

    @since 1.0.0 *)

(** {2 Packet Types} *)

(** Client-to-server packet types.

    Represents the different types of packets that can be sent from the client to the server.

    @since 1.0.0 *)
type client_packet =
  | Hello  (** Initial handshake packet with client information *)
  | Query  (** SQL query request *)
  | Data  (** Data block for INSERT operations *)
  | Cancel  (** Cancel current query execution *)
  | Ping  (** Keepalive ping request *)
  | TablesStatusRequest  (** Request table status information *)

val client_packet_to_int : client_packet -> int
(** [client_packet_to_int packet] converts a client packet type to its wire protocol integer.

    @param packet The client packet type
    @return Integer value for the wire protocol

    @since 1.0.0 *)

(** Server-to-client packet types.

    Represents the different types of packets that can be received from the server in response to
    client requests.

    @since 1.0.0 *)
type server_packet =
  | SHello  (** Server handshake response with server information *)
  | SData  (** Data block containing query results *)
  | SException  (** Error or exception from the server *)
  | SProgress  (** Query progress update *)
  | SPong  (** Response to ping *)
  | SEndOfStream  (** Marks the end of the result stream *)
  | SProfileInfo  (** Query profiling information *)
  | STotals  (** Aggregated totals for the query *)
  | SExtremes  (** Extreme values (min/max) for columns *)
  | STablesStatusResponse  (** Response to tables status request *)
  | SLog  (** Server log messages *)
  | STableColumns  (** Table column information *)
  | SPartUUIDs  (** Partition UUIDs *)
  | SReadTaskRequest  (** Read task request from server *)
  | SProfileEvents  (** Profile event information *)

val server_packet_of_int : int -> server_packet
(** [server_packet_of_int n] converts a wire protocol integer to a server packet type.

    @param n The integer value from the wire protocol
    @return The corresponding server packet type
    @raise Failure if the integer doesn't correspond to a known packet type

    @since 1.0.0 *)

(** {2 Compression} *)

type compression = Compress.method_t
(** Compression methods supported by the protocol.

    Alias for {!Compress.method_t} to provide a unified interface.

    @since 1.0.0 *)

val compression_to_int : compression -> int
(** [compression_to_int compression] converts compression method to protocol integer.

    Maps compression methods to their enable/disable values in the protocol. Returns 0 for no
    compression, 1 for enabled compression (LZ4 or ZSTD).

    @param compression The compression method
    @return Protocol integer (0 or 1)

    @since 1.0.0 *)

(** {2 Query Processing} *)

(** Query processing stage indicators.

    Controls how much processing the server performs on query results before sending them to the
    client.

    @since 1.0.0 *)
type query_processing_stage =
  | FetchColumns  (** Only fetch column metadata *)
  | WithMergeableState  (** Include mergeable state for distributed queries *)
  | Complete  (** Complete query processing (default) *)

val qps_to_int : query_processing_stage -> int
(** [qps_to_int stage] converts a query processing stage to its protocol integer.

    @param stage The query processing stage
    @return Protocol integer (0, 1, or 2)

    @since 1.0.0 *)
