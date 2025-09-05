(lang dune 3.11)
(name proton)
(generate_opam_files true)
(implicit_transitive_deps true)

(package
 (name proton)
 (synopsis "Minimal OCaml driver for Timeplus Proton (ClickHouse native protocol subset)")
 (depends 
  (ocaml (>= 4.14.0))
  lz4
  lwt
  tls
  tls-lwt
  ca-certs
  x509
  ptime
  domain-name
  mirage-crypto
  (alcotest :with-test)))