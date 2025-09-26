// Compile ClickHouse's CityHash v1.0.2 into the stubs archive
// Force the portable code path so the build does not depend on SSE4.2 intrinsics.

#ifndef CITYHASH_FORCE_PORTABLE
#define CITYHASH_FORCE_PORTABLE
#endif

#ifdef __SSE4_2__
#undef __SSE4_2__
#endif

#include "city.cc"
