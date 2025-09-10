(** {1 Binary Writer Module}

    Low-level binary serialization functions for the Proton wire protocol. These functions write
    various data types to buffers in the format expected by the Proton server.

    @since 1.0.0 *)

(** {2 Primitive Type Writers} *)

val write_varint_to_buffer : Buffer.t -> int -> unit
(** [write_varint_to_buffer buffer value] writes a variable-length integer to the buffer.

    Uses LEB128 encoding for space-efficient integer serialization.

    @param buffer The target buffer
    @param value The integer value to write

    @since 1.0.0 *)

val write_bool_to_buffer : Buffer.t -> bool -> unit
(** [write_bool_to_buffer buffer value] writes a boolean value to the buffer.

    Booleans are encoded as a single byte (0 or 1).

    @param buffer The target buffer
    @param value The boolean value to write

    @since 1.0.0 *)

val write_int32_le_to_buffer : Buffer.t -> int32 -> unit
(** [write_int32_le_to_buffer buffer value] writes a 32-bit integer in little-endian format.

    @param buffer The target buffer
    @param value The int32 value to write

    @since 1.0.0 *)

val write_string_to_buffer : Buffer.t -> string -> unit
(** [write_string_to_buffer buffer value] writes a length-prefixed string to the buffer.

    The string is written with its length as a varint followed by the string bytes.

    @param buffer The target buffer
    @param value The string to write

    @since 1.0.0 *)

(** {2 Complex Type Writers} *)

val write_block_info_to_buffer : Buffer.t -> Block_info.t -> unit
(** [write_block_info_to_buffer buffer block_info] writes block metadata to the buffer.

    Serializes block information including checksums and compression details as required by the
    Proton protocol.

    @param buffer The target buffer
    @param block_info The block information to write

    @see 'Block_info.t' for block information structure

    @since 1.0.0 *)

val write_value_to_buffer : Buffer.t -> Column.value -> unit
(** [write_value_to_buffer buffer value] writes a column value to the buffer.

    Handles all supported column types including integers, floats, strings, dates, and arrays. The
    serialization format depends on the value type.

    @param buffer The target buffer
    @param value The column value to write

    @see 'Column.value' for supported value types

    @since 1.0.0 *)
