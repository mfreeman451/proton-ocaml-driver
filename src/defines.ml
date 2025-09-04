(* Mirrors proton_driver/defines.py *)

let default_database = "default"
let default_user = "default"
let default_password = ""

let default_port = 8463
let default_secure_port = 9440

(* Server revision gates (same numbers as in Python) *)
let dbms_min_revision_with_temporary_tables = 50264
let dbms_min_revision_with_total_rows_in_progress = 51554
let dbms_min_revision_with_block_info = 51903
let dbms_min_revision_with_client_info = 54032
let dbms_min_revision_with_server_timezone = 54058
let dbms_min_revision_with_quota_key_in_client_info = 54060
let dbms_min_revision_with_server_display_name = 54372
let dbms_min_revision_with_version_patch = 54401
let dbms_min_revision_with_server_logs = 54406
let dbms_min_revision_with_column_defaults_metadata = 54410
let dbms_min_revision_with_client_write_info = 54420
let dbms_min_revision_with_settings_serialized_as_strings = 54429
let dbms_min_revision_with_interserver_secret = 54441
let dbms_min_revision_with_opentelemetry = 54442
let dbms_min_protocol_version_with_distributed_depth = 54448
let dbms_min_protocol_version_with_initial_query_start_time = 54449
let dbms_min_protocol_version_with_incremental_profile_events = 54451
let dbms_min_revision_with_parallel_replicas = 54453

(* timeouts, sizes *)
let dbms_default_connect_timeout_sec = 10.
let dbms_default_timeout_sec = 300.
let dbms_default_sync_request_timeout_sec = 5.

let default_compress_block_size = 1_048_576
let default_insert_block_size   = 1_048_576

let dbms_name = "Proton"

let client_name = "ocaml-driver"
let client_version_major = 20
let client_version_minor = 10
let client_version_patch = 2
let client_revision      = dbms_min_revision_with_parallel_replicas

let buffer_size = 1_048_576

let strings_encoding = "UTF-8"
