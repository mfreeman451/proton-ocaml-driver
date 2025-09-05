// OCaml bindings for CityHash128
// This is a minimal implementation based on the Google CityHash code

#include <caml/mlvalues.h>
#include <caml/memory.h>
#include <caml/alloc.h>
#include <caml/custom.h>
#include <cstdint>
#include <cstring>
#include <utility>

typedef std::pair<uint64_t, uint64_t> uint128;

static uint64_t UNALIGNED_LOAD64(const char *p) {
  uint64_t result;
  memcpy(&result, p, sizeof(result));
  return result;
}

static uint32_t UNALIGNED_LOAD32(const char *p) {
  uint32_t result;
  memcpy(&result, p, sizeof(result));
  return result;
}

static uint64_t Fetch64(const char *p) {
  return UNALIGNED_LOAD64(p);
}

static uint32_t Fetch32(const char *p) {
  return UNALIGNED_LOAD32(p);
}

// Some primes between 2^63 and 2^64 for various uses
static const uint64_t k0 = 0xc3a5c85c97cb3127ULL;
static const uint64_t k1 = 0xb492b66fbe98f273ULL;
static const uint64_t k2 = 0x9ae16a3b2f90404fULL;
static const uint64_t k3 = 0xc949d7c7509e6557ULL;

// Magic numbers for 32-bit hashing
static const uint32_t c1 = 0xcc9e2d51;
static const uint32_t c2 = 0x1b873593;

// A 32-bit to 32-bit integer hash
static uint32_t fmix(uint32_t h) {
  h ^= h >> 16;
  h *= 0x85ebca6b;
  h ^= h >> 13;
  h *= 0xc2b2ae35;
  h ^= h >> 16;
  return h;
}

static uint64_t Rotate(uint64_t val, int shift) {
  return shift == 0 ? val : ((val >> shift) | (val << (64 - shift)));
}

static uint64_t ShiftMix(uint64_t val) {
  return val ^ (val >> 47);
}

static uint64_t HashLen16(uint64_t u, uint64_t v) {
  const uint64_t kMul = 0x9ddfea08eb382d69ULL;
  uint64_t a = (u ^ v) * kMul;
  a ^= (a >> 47);
  uint64_t b = (v ^ a) * kMul;
  b ^= (b >> 47);
  b *= kMul;
  return b;
}

static uint64_t HashLen0to16(const char *s, size_t len) {
  if (len > 8) {
    uint64_t a = Fetch64(s);
    uint64_t b = Fetch64(s + len - 8);
    return HashLen16(a, Rotate(b + len, len)) ^ b;
  }
  if (len >= 4) {
    uint64_t a = Fetch32(s);
    return HashLen16(len + (a << 3), Fetch32(s + len - 4));
  }
  if (len > 0) {
    uint8_t a = s[0];
    uint8_t b = s[len >> 1];
    uint8_t c = s[len - 1];
    uint32_t y = static_cast<uint32_t>(a) + (static_cast<uint32_t>(b) << 8);
    uint32_t z = len + (static_cast<uint32_t>(c) << 2);
    return ShiftMix(y * k2 ^ z * k3) * k2;
  }
  return k2;
}

// Return a 16-byte hash for s[0] ... s[31]
static std::pair<uint64_t, uint64_t> WeakHashLen32WithSeeds(
    const char *s, uint64_t a, uint64_t b) {
  uint64_t w = Fetch64(s);
  uint64_t x = Fetch64(s + 8);
  uint64_t y = Fetch64(s + 16);
  uint64_t z = Fetch64(s + 24);
  
  a += w;
  b = Rotate(b + a + z, 21);
  uint64_t c = a;
  a += x;
  a += y;
  b += Rotate(a, 44);
  return std::make_pair(a + z, b + c);
}

// A subroutine for CityHash128()
static uint128 CityMurmur(const char *s, size_t len, uint128 seed) {
  uint64_t a = seed.first;
  uint64_t b = seed.second;
  uint64_t c = 0;
  uint64_t d = 0;
  int64_t l = len - 16;
  
  if (l <= 0) {  // len <= 16
    a = ShiftMix(a * k1) * k1;
    c = b * k1 + HashLen0to16(s, len);
    d = ShiftMix(a + (len >= 8 ? Fetch64(s) : c));
  } else {  // len > 16
    c = HashLen16(Fetch64(s + len - 8) + k1, a);
    d = HashLen16(b + len, c + Fetch64(s + len - 16));
    a += d;
    do {
      a ^= ShiftMix(Fetch64(s) * k1) * k1;
      a *= k1;
      b ^= a;
      c ^= ShiftMix(Fetch64(s + 8) * k1) * k1;
      c *= k1;
      d ^= c;
      s += 16;
      l -= 16;
    } while (l > 0);
  }
  a = HashLen16(a, c);
  b = HashLen16(d, b);
  return uint128(a ^ b, HashLen16(b, a));
}

static uint128 CityHash128WithSeed(const char *s, size_t len, uint128 seed) {
  if (len < 128) {
    return CityMurmur(s, len, seed);
  }
  
  // We expect len >= 128
  std::pair<uint64_t, uint64_t> v, w;
  uint64_t x = seed.first;
  uint64_t y = seed.second;
  uint64_t z = len * k1;
  
  v.first = Rotate(y ^ k1, 49) * k1 + Fetch64(s);
  v.second = Rotate(v.first, 42) * k1 + Fetch64(s + 8);
  w.first = Rotate(y + z, 35) * k1 + x;
  w.second = Rotate(x + Fetch64(s + 88), 53) * k1;
  
  // Main loop
  do {
    x = Rotate(x + y + v.first + Fetch64(s + 8), 37) * k1;
    y = Rotate(y + v.second + Fetch64(s + 48), 42) * k1;
    x ^= w.second;
    y += v.first + Fetch64(s + 40);
    z = Rotate(z + w.first, 33) * k1;
    v = WeakHashLen32WithSeeds(s, v.second * k1, x + w.first);
    w = WeakHashLen32WithSeeds(s + 32, z + w.second, y + Fetch64(s + 16));
    std::swap(z, x);
    s += 64;
    x = Rotate(x + y + v.first + Fetch64(s + 8), 37) * k1;
    y = Rotate(y + v.second + Fetch64(s + 48), 42) * k1;
    x ^= w.second;
    y += v.first + Fetch64(s + 40);
    z = Rotate(z + w.first, 33) * k1;
    v = WeakHashLen32WithSeeds(s, v.second * k1, x + w.first);
    w = WeakHashLen32WithSeeds(s + 32, z + w.second, y + Fetch64(s + 16));
    std::swap(z, x);
    s += 64;
    len -= 128;
  } while (len >= 128);
  
  x += Rotate(v.first + z, 49) * k0;
  z += Rotate(w.first, 37) * k0;
  
  // Process tail
  for (size_t tail_done = 0; tail_done < len; ) {
    tail_done += 32;
    y = Rotate(x + y, 42) * k0 + v.second;
    w.first += Fetch64(s + len - tail_done + 16);
    x = x * k0 + w.first;
    z += w.second + Fetch64(s + len - tail_done);
    w.second += v.first;
    v = WeakHashLen32WithSeeds(s + len - tail_done, v.first + z, v.second);
  }
  
  x = HashLen16(x, v.first);
  y = HashLen16(y + z, w.first);
  return uint128(HashLen16(x + v.second, w.second) + y,
                 HashLen16(x + w.second, y + v.second));
}

static uint128 CityHash128(const char *s, size_t len) {
  if (len >= 16) {
    return CityHash128WithSeed(s + 16, len - 16,
                              uint128(Fetch64(s) ^ k3, Fetch64(s + 8)));
  } else if (len >= 8) {
    return CityHash128WithSeed(nullptr, 0,
                              uint128(Fetch64(s) ^ (len * k0), 
                                      Fetch64(s + len - 8) ^ k1));
  } else {
    return CityHash128WithSeed(s, len, uint128(k0, k1));
  }
}

// OCaml interface
extern "C" {

CAMLprim value cityhash128_stub(value v_data) {
  CAMLparam1(v_data);
  CAMLlocal1(v_result);
  
  const char *data = reinterpret_cast<const char*>(Bytes_val(v_data));
  size_t len = caml_string_length(v_data);
  
  uint128 hash = CityHash128(data, len);
  
  // Create a tuple with two int64 values
  v_result = caml_alloc_tuple(2);
  Store_field(v_result, 0, caml_copy_int64(hash.first));
  Store_field(v_result, 1, caml_copy_int64(hash.second));
  
  CAMLreturn(v_result);
}

} // extern "C"