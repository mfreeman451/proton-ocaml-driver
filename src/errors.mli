(** {1 Errors Module}

    Exception definitions for the Timeplus Proton OCaml driver. This module defines custom
    exceptions that can be raised during client-server communication and query execution.

    All exceptions are automatically registered with the OCaml runtime to provide meaningful error
    messages when printed.

    @since 1.0.0 *)

(** {2 Network Errors} *)

exception Network_error of string
(** [Network_error message] is raised when network communication fails.

    This exception indicates problems with the underlying network connection, such as connection
    loss, DNS resolution failures, or I/O errors.

    @param message Detailed description of the network error

    Example:
    {[
      try Client.execute client "SELECT 1"
      with Network_error msg -> Printf.eprintf "Network failed: %s\n" msg
    ]}

    @since 1.0.0 *)

exception Socket_timeout
(** [Socket_timeout] is raised when a socket operation times out.

    Indicates that a read or write operation on the socket did not complete within the configured
    timeout period.

    Example:
    {[
      try Client.execute client "SELECT sleep(100)"
      with Socket_timeout -> Printf.eprintf "Query timed out\n"
    ]}

    @since 1.0.0 *)

(** {2 Protocol Errors} *)

exception Unexpected_packet of string
(** [Unexpected_packet description] is raised when an unexpected packet type is received.

    This exception indicates a protocol violation where the server sent a packet type that was not
    expected in the current context.

    @param description Description of what was expected vs. what was received

    Example:
    {[
      try Client.execute client "SELECT 1"
      with Unexpected_packet desc -> Printf.eprintf "Protocol error: %s\n" desc
    ]}

    @since 1.0.0 *)

exception Unknown_packet of int
(** [Unknown_packet packet_id] is raised when an unrecognized packet ID is received.

    Indicates that the server sent a packet with an ID that is not defined in the protocol
    specification known to this client.

    @param packet_id The unknown packet identifier

    @since 1.0.0 *)

(** {2 Server Errors} *)

exception
  Server_exception of {
    code : int;  (** Numeric error code from the server *)
    message : string;  (** Primary error message describing the problem *)
    nested : string option;  (** Optional nested exception message providing additional context *)
  }
(** [Server_exception] represents an error reported by the Proton server.

    When the server encounters an error during query processing, it sends an exception packet with
    error details. This exception captures that server-side error information.

    The error code typically indicates the type of error (syntax error, table not found, permission
    denied, etc.), while the message provides a human-readable description.

    Example:
    {[
      try Client.execute client "SELECT * FROM nonexistent_table"
      with Server_exception { code; message; nested } -> (
        Printf.eprintf "Server error %d: %s\n" code message;
        match nested with Some n -> Printf.eprintf "Caused by: %s\n" n | None -> ())
    ]}

    @since 1.0.0 *)
