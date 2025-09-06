(lang dune 3.11)
(name proton)
(generate_opam_files true)
(implicit_transitive_deps true)

(package
 (name proton)
 (synopsis "High-performance OCaml driver for Timeplus Proton (ClickHouse native protocol)")
 (description "A feature-rich OCaml driver for Timeplus Proton, supporting streaming queries, async inserts, compression (LZ4/ZSTD), TLS security, connection pooling, and rich data types including Arrays, Maps, Enums, and DateTime64.")
 (homepage "https://github.com/mfreeman451/proton-ocaml-driver")
 (bug_reports "https://github.com/mfreeman451/proton-ocaml-driver/issues")
 (source (github "mfreeman451/proton-ocaml-driver"))
 (license MIT)
 (authors "mfreeman451")
 (maintainers "mfreeman451")
 (depends 
  (ocaml (>= 4.14.0))
  lz4
  zstd
  lwt
  tls
  tls-lwt
  ca-certs
  x509
  ptime
  domain-name
  mirage-crypto
  (alcotest :with-test)
  (odoc :with-doc)))
