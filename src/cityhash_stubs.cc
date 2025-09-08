// OCaml bindings for CityHash128 using ClickHouse's CityHash v1.0.2

#include <caml/mlvalues.h>
#include <caml/memory.h>
#include <caml/alloc.h>
#include <caml/custom.h>
#include <caml/fail.h>
#include <cstring>

#include "city.h" // Provided by include_dirs in dune (ClickHouse/contrib/cityhash102/include)

extern "C" {

CAMLprim value cityhash128_stub(value v_data) {
  CAMLparam1(v_data);
  CAMLlocal1(v_result);

  const char *data = reinterpret_cast<const char*>(Bytes_val(v_data));
  size_t len = caml_string_length(v_data);

  CityHash_v1_0_2::uint128 h = CityHash_v1_0_2::CityHash128(data, len);
  v_result = caml_alloc_tuple(2);
  Store_field(v_result, 0, caml_copy_int64((int64_t)h.low64));
  Store_field(v_result, 1, caml_copy_int64((int64_t)h.high64));
  CAMLreturn(v_result);
}

CAMLprim value cityhash128_sub_stub(value v_data, value v_off, value v_len) {
  CAMLparam3(v_data, v_off, v_len);
  CAMLlocal1(v_result);

  const char *base = reinterpret_cast<const char*>(Bytes_val(v_data));
  size_t off = Long_val(v_off);
  size_t len = Long_val(v_len);
  const char *data = base + off;

  CityHash_v1_0_2::uint128 h = CityHash_v1_0_2::CityHash128(data, len);
  v_result = caml_alloc_tuple(2);
  Store_field(v_result, 0, caml_copy_int64((int64_t)h.low64));
  Store_field(v_result, 1, caml_copy_int64((int64_t)h.high64));
  CAMLreturn(v_result);
}

CAMLprim value cityhash128_2sub_stub(value v_d1, value v_off1, value v_len1,
                                     value v_d2, value v_off2, value v_len2) {
  CAMLparam5(v_d1, v_off1, v_len1, v_d2, v_off2);
  CAMLxparam1(v_len2);

  const char *base1 = reinterpret_cast<const char*>(Bytes_val(v_d1));
  size_t off1 = Long_val(v_off1);
  size_t len1 = Long_val(v_len1);

  const char *base2 = reinterpret_cast<const char*>(Bytes_val(v_d2));
  size_t off2 = Long_val(v_off2);
  size_t len2 = Long_val(v_len2);

  size_t total = len1 + len2;
  char *tmp = (char*)malloc(total);
  if (!tmp) caml_failwith("cityhash128_2sub: OOM");
  memcpy(tmp, base1 + off1, len1);
  memcpy(tmp + len1, base2 + off2, len2);

  CityHash_v1_0_2::uint128 h = CityHash_v1_0_2::CityHash128(tmp, total);
  free(tmp);

  CAMLlocal1(v_result);
  v_result = caml_alloc_tuple(2);
  Store_field(v_result, 0, caml_copy_int64((int64_t)h.low64));
  Store_field(v_result, 1, caml_copy_int64((int64_t)h.high64));
  CAMLreturn(v_result);
}

// bytecode trampoline for 6-arg external
CAMLprim value cityhash128_2sub_stub_bytecode(value *argv, int argn) {
  return cityhash128_2sub_stub(argv[0], argv[1], argv[2], argv[3], argv[4], argv[5]);
}

} // extern "C"

