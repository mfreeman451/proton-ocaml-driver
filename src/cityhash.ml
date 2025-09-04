(* CityHash128 implementation for ClickHouse protocol 
   Uses C++ bindings to the Google CityHash implementation *)

type uint128 = {
  low: int64;
  high: int64;
}

(* External C++ function *)
external cityhash128_raw : bytes -> (int64 * int64) = "cityhash128_stub"

(* CityHash128 function that returns our uint128 type *)
let cityhash128 (data : bytes) : uint128 =
  let (low, high) = cityhash128_raw data in
  { low; high }

let to_bytes (hash : uint128) : bytes =
  let buf = Bytes.create 16 in
  Binary.bytes_set_int64_le buf 0 hash.low;
  Binary.bytes_set_int64_le buf 8 hash.high;
  buf