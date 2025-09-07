(* Pure OCaml port of CityHash128 as implemented in proton-go-driver/lib/cityhash102 *)

open Int64

let k0 = 0xc3a5c85c97cb3127L
let k1 = 0xb492b66fbe98f273L
let k2 = 0x9ae16a3b2f90404fL
let k3 = 0xc949d7c7509e6557L
let kmul = 0x9ddfea08eb382d69L

let[@inline] rotate64 (v:int64) (shift:int) : int64 =
  if shift = 0 then v
  else logor (shift_right_logical v shift) (shift_left v (64 - shift))

let[@inline] fetch32_le (b:bytes) (off:int) : int64 =
  Int64.logand (Int64.of_int32 (Binary.bytes_get_int32_le b off)) 0xFFFFFFFFL

let[@inline] fetch64_le (b:bytes) (off:int) : int64 =
  Binary.bytes_get_int64_le b off

let[@inline] shift_mix (v:int64) : int64 =
  logxor v (shift_right_logical v 47)

let hash128to64 (u:int64) (v:int64) : int64 =
  let a = ref (mul (logxor u v) kmul) in
  a := logxor !a (shift_right_logical !a 47);
  let b = ref (mul (logxor v !a) kmul) in
  b := logxor !b (shift_right_logical !b 47);
  mul !b kmul

let hash_len_0to16 (s:bytes) (off:int) (len:int) : int64 =
  if len > 8 then (
    let a = fetch64_le s off in
    let b = fetch64_le s (off + len - 8) in
    logxor (hash128to64 a (rotate64 (add b (of_int len)) len)) b
  ) else if len >= 4 then (
    let a = fetch32_le s off in
    hash128to64 (add (of_int len) (shift_left a 3)) (fetch32_le s (off + len - 4))
  ) else if len > 0 then (
    let a = of_int (Char.code (Bytes.get s off)) in
    let b = of_int (Char.code (Bytes.get s (off + (len lsr 1)))) in
    let c = of_int (Char.code (Bytes.get s (off + len - 1))) in
    let y = add a (shift_left b 8) in
    let z = add (of_int len) (shift_left c 2) in
    mul (shift_mix (logxor (mul y k2) (mul z k3))) k2
  ) else k2

let weak_hash_len_32_with_seeds (s:bytes) (off:int) (a:int64) (b:int64) : int64 * int64 =
  let w = fetch64_le s off in
  let x = fetch64_le s (off + 8) in
  let y = fetch64_le s (off + 16) in
  let z = fetch64_le s (off + 24) in
  let a = add a w in
  let b = rotate64 (add (add b a) z) 21 in
  let c = a in
  let a = add (add a x) y in
  let b = add b (rotate64 a 44) in
  (add a z, add b c)

let city_murmur (s:bytes) (off:int) (len:int) (seed_lo:int64) (seed_hi:int64) : int64 * int64 =
  let a = ref seed_lo in
  let b = ref seed_hi in
  let c = ref 0L in
  let d = ref 0L in
  let l = ref (len - 16) in
  if !l <= 0 then (
    a := mul (shift_mix (mul !a k1)) k1;
    c := add (mul !b k1) (hash_len_0to16 s off len);
    d := shift_mix (add !a (if len >= 8 then fetch64_le s off else !c))
  ) else (
    c := hash128to64 (add (fetch64_le s (off + len - 8)) k1) !a;
    d := hash128to64 (add !b (of_int len)) (add !c (fetch64_le s (off + len - 16)));
    a := add !a !d;
    let pos = ref off in
    while !l > 0 do
      a := mul (shift_mix (mul (logxor !a (mul (fetch64_le s !pos) k1)) k1)) k1;
      b := logxor !b !a;
      c := mul (shift_mix (mul (logxor !c (mul (fetch64_le s (!pos + 8)) k1)) k1)) k1;
      d := logxor !d !c;
      pos := !pos + 16;
      l := !l - 16
    done
  );
  let a = hash128to64 !a !c in
  let b = hash128to64 !d !b in
  (logxor a b, hash128to64 b a)

let cityhash128_with_seed (s:bytes) (off:int) (len:int) (seed_lo:int64) (seed_hi:int64) : int64 * int64 =
  if len < 128 then city_murmur s off len seed_lo seed_hi
  else (
    let v0, v1 = ref 0L, ref 0L in
    let w0, w1 = ref 0L, ref 0L in
    let x = ref seed_lo in
    let y = ref seed_hi in
    let z = ref (mul (of_int len) k1) in
    v0 := add (mul (rotate64 (logxor !y k1) 49) k1) (fetch64_le s off);
    v1 := add (mul (rotate64 !v0 42) k1) (fetch64_le s (off + 8));
    w0 := add (mul (rotate64 (add !y !z) 35) k1) !x;
    w1 := mul (rotate64 (add !x (fetch64_le s (off + 88))) 53) k1;
    let pos = ref off in
    let remaining = ref len in
    (* main loop *)
    while !remaining >= 128 do
      x := mul (rotate64 (add (add (add !x !y) !v0) (fetch64_le s (!pos + 16))) 37) k1;
      y := mul (rotate64 (add !y (add !v1 (fetch64_le s (!pos + 48)))) 42) k1;
      x := logxor !x !w1;
      y := add !y (add !v0 (fetch64_le s (!pos + 40)));
      z := mul (rotate64 (add !z !w0) 33) k1;
      let v0', v1' = weak_hash_len_32_with_seeds s !pos (mul !v1 k1) (add !x !w0) in
      v0 := v0'; v1 := v1';
      let w0', w1' = weak_hash_len_32_with_seeds s (!pos + 32) (add !z !w1) (add !y (fetch64_le s (!pos + 16))) in
      w0 := w0'; w1 := w1';
      (* swap z and x *)
      let tmp = !z in z := !x; x := tmp;
      pos := !pos + 64;
      x := mul (rotate64 (add (add (add !x !y) !v0) (fetch64_le s (!pos + 16))) 37) k1;
      y := mul (rotate64 (add !y (add !v1 (fetch64_le s (!pos + 48)))) 42) k1;
      x := logxor !x !w1;
      y := add !y (add !v0 (fetch64_le s (!pos + 40)));
      z := mul (rotate64 (add !z !w0) 33) k1;
      let v0', v1' = weak_hash_len_32_with_seeds s !pos (mul !v1 k1) (add !x !w0) in
      v0 := v0'; v1 := v1';
      let w0', w1' = weak_hash_len_32_with_seeds s (!pos + 32) (add !z !w1) (add !y (fetch64_le s (!pos + 16))) in
      w0 := w0'; w1 := w1';
      let tmp2 = !z in z := !x; x := tmp2;
      pos := !pos + 64;
      remaining := !remaining - 128
    done;
    y := add (add (mul (rotate64 !w0 37) k0) !z) !y;
    x := add (mul (rotate64 (add !v0 !z) 49) k0) !x;
    (* tail *)
    let tail_done = ref 0 in
    while !tail_done < !remaining do
      tail_done := !tail_done + 32;
      y := add (mul (rotate64 (sub !y !x) 42) k0) !v1;
      w0 := add !w0 (fetch64_le s (off + !remaining - !tail_done + 16));
      x := add (mul (rotate64 !x 49) k0) !w0;
      w0 := add !w0 !v0;
      let v0', v1' = weak_hash_len_32_with_seeds s (off + !remaining - !tail_done) !v0 !v1 in
      v0 := v0'; v1 := v1'
    done;
    x := hash128to64 !x !v0;
    y := hash128to64 !y !w0;
    (add (hash128to64 (add !x !v1) !w1) !y,
     hash128to64 (add !x !w1) (add !y !v1))
  )

let cityhash128 (s:bytes) (off:int) (len:int) : int64 * int64 =
  if len >= 16 then
    let seed_lo = logxor (fetch64_le s off) k3 in
    let seed_hi = fetch64_le s (off + 8) in
    cityhash128_with_seed s (off + 16) (len - 16) seed_lo seed_hi
  else if len >= 8 then
    cityhash128_with_seed Bytes.empty 0 0 (logxor (fetch64_le s off) (mul (of_int len) k0)) (logxor (fetch64_le s (off + len - 8)) k1)
  else
    cityhash128_with_seed s off len k0 k1

let to_bytes (low:int64) (high:int64) : bytes =
  let buf = Bytes.create 16 in
  Binary.bytes_set_int64_le buf 0 low;
  Binary.bytes_set_int64_le buf 8 high;
  buf
