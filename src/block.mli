(** {1 Block Module}

    Data block representation for Timeplus Proton query results. Blocks are the fundamental unit of
    data transfer between client and server, containing both data rows and column metadata.

    @since 1.0.0 *)

(** {2 Types} *)

type column = {
  name : string;  (** Column name as defined in the table schema *)
  type_spec : string;  (** Type specification (e.g., "String", "Int32", "Array(Float64)") *)
  data : Column.value array;  (** Array of values for this column across all rows *)
}
(** Column metadata and data within a block.

    Represents a single column in a data block, including its name, type specification, and the
    actual data values.

    @since 1.0.0 *)

type t = {
  n_columns : int;  (** Number of columns in the block *)
  n_rows : int;  (** Number of rows in the block *)
  columns : column array;  (** Array of columns with their data *)
}
(** Data block containing query results.

    A block represents a batch of rows returned from a query, with data organized by columns. Each
    block is self-contained with its own column metadata.

    @since 1.0.0 *)

(** {2 Reading Functions} *)

val read_block : revision:int -> in_channel -> t
(** [read_block ~revision ic] reads a data block from an input channel.

    Reads a complete data block including column metadata and row data according to the protocol
    revision.

    @param revision Protocol revision number
    @param ic Input channel to read from
    @return The parsed data block
    @raise End_of_file if the stream ends unexpectedly

    @since 1.0.0 *)

val read_block_br : revision:int -> Buffered_reader.t -> t
(** [read_block_br ~revision br] reads a data block using a buffered reader.

    Similar to {!read_block} but uses a buffered reader for better performance with compressed or
    network streams.

    @param revision Protocol revision number
    @param br Buffered reader to read from
    @return The parsed data block

    @since 1.0.0 *)

(** {2 Data Access} *)

val get_rows : t -> Column.value list list
(** [get_rows block] extracts all rows from a block as a list of lists.

    Transforms the column-oriented storage into row-oriented format for easier processing. Each
    inner list represents one row with values in column order.

    @param block The data block
    @return List of rows, where each row is a list of values

    Example:
    {[
      let block = read_block ~revision ic in
      let rows = Block.get_rows block in
      List.iter
        (fun row ->
          List.iter (fun value -> print_string (Column.value_to_string value ^ " ")) row;
          print_newline ())
        rows
    ]}

    @since 1.0.0 *)

val columns_with_types : t -> (string * string) list
(** [columns_with_types block] returns column names and their types.

    Extracts the schema information from a block as a list of (name, type) pairs. Useful for
    understanding the structure of query results.

    @param block The data block
    @return List of (column_name, column_type) pairs

    Example:
    {[
      let columns = Block.columns_with_types block in
      List.iter (fun (name, typ) -> Printf.printf "Column %s has type %s\n" name typ) columns
    ]}

    @since 1.0.0 *)
