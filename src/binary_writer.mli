val write_varint_to_buffer : Buffer.t -> int -> unit
val write_bool_to_buffer : Buffer.t -> bool -> unit
val write_int32_le_to_buffer : Buffer.t -> int32 -> unit
val write_string_to_buffer : Buffer.t -> string -> unit
val write_block_info_to_buffer : Buffer.t -> Block_info.t -> unit
val write_value_to_buffer : Buffer.t -> Columns.value -> unit