(* CityHash128 implementation for ClickHouse protocol 
   Uses C++ bindings to the Google CityHash implementation *)

type uint128 = { low : int64; high : int64 }

(* External C++ function *)
external cityhash128_raw : bytes -> int64 * int64 = "cityhash128_stub"
external cityhash128_sub_raw : bytes -> int -> int -> int64 * int64 = "cityhash128_sub_stub"

external cityhash128_2sub_raw : bytes -> int -> int -> bytes -> int -> int -> int64 * int64
  = "cityhash128_2sub_stub_bytecode" "cityhash128_2sub_stub"

(* CityHash128 function that returns our uint128 type *)
let cityhash128 (data : bytes) : uint128 =
  let low, high = cityhash128_raw data in
  { low; high }

let cityhash128_sub (data : bytes) (off : int) (len : int) : uint128 =
  let low, high = cityhash128_sub_raw data off len in
  { low; high }

let cityhash128_2sub (d1 : bytes) (off1 : int) (len1 : int) (d2 : bytes) (off2 : int) (len2 : int) :
    uint128 =
  let low, high = cityhash128_2sub_raw d1 off1 len1 d2 off2 len2 in
  { low; high }

let to_bytes (hash : uint128) : bytes =
  let buf = Bytes.create 16 in
  Binary.bytes_set_int64_le buf 0 hash.low;
  Binary.bytes_set_int64_le buf 8 hash.high;
  buf
