(** {1 Column Module}
    
    Defines column data types and values for Timeplus Proton.
    This module provides the core type system for representing data values
    that can be stored in and retrieved from Proton tables.
    
    @since 1.0.0
*)

(** {2 Types} *)

(** Column value types supported by Proton.
    
    Represents all possible data types that can be stored in Proton columns.
    This is a variant type that covers primitive types, date/time types,
    enums, and complex types like arrays and maps.
    
    @since 1.0.0 *)
type value =
  | Null
  (** SQL NULL value *)
  
  | String of string
  (** UTF-8 string value *)
  
  | Int32 of int32
  (** 32-bit signed integer *)
  
  | Int64 of int64
  (** 64-bit signed integer *)
  
  | UInt32 of int32
  (** 32-bit unsigned integer (stored as signed) *)
  
  | UInt64 of int64
  (** 64-bit unsigned integer (stored as signed) *)
  
  | Float64 of float
  (** 64-bit floating point number *)
  
  | DateTime of int64 * string option
  (** DateTime with Unix timestamp and optional timezone.
      The timestamp is in seconds since Unix epoch. *)
  
  | DateTime64 of int64 * int * string option
  (** High-precision DateTime with value, precision (0-9), and optional timezone.
      The value interpretation depends on the precision. *)
  
  | Enum8 of string * int
  (** 8-bit enum with name and numeric value *)
  
  | Enum16 of string * int
  (** 16-bit enum with name and numeric value *)
  
  | Array of value array
  (** Array of values (all must be the same type) *)
  
  | Map of (value * value) list
  (** Map from keys to values *)
  
  | Tuple of value list
  (** Fixed-size tuple of potentially different types *)

(** {2 Value Conversion} *)

(** [value_to_string value] converts a column value to its string representation.
    
    Provides a human-readable string representation of any column value.
    Arrays and maps are formatted with appropriate delimiters.
    
    @param value The column value to convert
    @return String representation of the value
    
    Example:
    {[
      value_to_string (String "hello") = "hello"
      value_to_string (Int32 42l) = "42"
      value_to_string (Array [|Int32 1l; Int32 2l|]) = "[1,2]"
      value_to_string Null = "NULL"
    ]}
    
    @since 1.0.0 *)
val value_to_string : value -> string

(** {2 Column Reading} *)

(** [compile_reader spec] compiles a column type specification into a reader function.
    
    Creates an optimized reader function for the given column type specification.
    The reader can then be used to efficiently read multiple values of that type.
    Results are cached for performance.
    
    @param spec The column type specification (e.g., "String", "Array(Int32)")
    @return A function that reads an array of values from an input channel
    
    @since 1.0.0 *)
val compile_reader : string -> (in_channel -> int -> value array)

(** [compile_reader_br spec] compiles a column specification for buffered reading.
    
    Similar to {!compile_reader} but works with buffered readers for better
    performance with compressed or streamed data.
    
    @param spec The column type specification
    @return A function that reads values from a buffered reader
    
    @since 1.0.0 *)
val compile_reader_br : string -> (Buffered_reader.t -> int -> value array)

(** {2 Cache Management} *)

(** Statistics about the column reader cache.
    
    Tracks cache performance metrics for both regular and buffered readers.
    
    @since 1.0.0 *)
type cache_stats = {
  reader_cache_size: int;
  (** Number of entries in the regular reader cache *)
  
  reader_cache_hits: int;
  (** Number of cache hits for regular readers *)
  
  reader_cache_misses: int;
  (** Number of cache misses for regular readers *)
  
  reader_br_cache_size: int;
  (** Number of entries in the buffered reader cache *)
  
  reader_br_cache_hits: int;
  (** Number of cache hits for buffered readers *)
  
  reader_br_cache_misses: int;
  (** Number of cache misses for buffered readers *)
}

(** [get_cache_stats ()] returns current cache statistics.
    
    Provides insight into cache performance and usage patterns.
    
    @return Current cache statistics
    
    @since 1.0.0 *)
val get_cache_stats : unit -> cache_stats

(** [cache_stats_to_string stats] formats cache statistics as a string.
    
    @param stats The cache statistics to format
    @return Human-readable string representation
    
    @since 1.0.0 *)
val cache_stats_to_string : cache_stats -> string

(** [reset_cache_stats ()] resets all cache statistics to zero.
    
    Useful for measuring cache performance over specific time periods.
    
    @since 1.0.0 *)
val reset_cache_stats : unit -> unit

(** [clear_reader_caches ()] clears all cached column readers.
    
    Removes all entries from both reader caches. Useful when memory
    needs to be reclaimed or after schema changes.
    
    @since 1.0.0 *)
val clear_reader_caches : unit -> unit

(** {2 Utilities} *)

(** [has_prefix s p] returns true if string [s] starts with prefix [p].
    Optimized to avoid intermediate allocations in hot paths. *)
val has_prefix : string -> string -> bool

(** Resolve and cache a reader for a type spec. *)
val reader_of_spec : string -> (in_channel -> int -> value array)

(** Resolve and cache a buffered reader for a type spec. *)
val reader_of_spec_br : string -> (Buffered_reader.t -> int -> value array)
