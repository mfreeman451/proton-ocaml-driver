# Async Insert Guide

The Proton OCaml driver provides powerful async insert functionality for high-throughput data ingestion scenarios where you need to insert many rows efficiently without blocking your application.

## Key Features

- **Batching**: Automatically batches multiple rows before sending to reduce network overhead
- **Background processing**: Non-blocking inserts that don't block your main application logic
- **Configurable triggers**: Flush based on row count, byte size, or time intervals
- **Error handling**: Automatic retry logic with exponential backoff
- **Buffer management**: Automatic memory management with configurable limits
- **Binary protocol**: Uses ClickHouse native binary Data packet protocol for optimal performance

## Quick Start

### Simple Insert

```ocaml
open Proton

let client = Client.create ~host:"localhost" ~database:"mydb" () in

(* Insert a single row *)
let row = [
  Column.String "user123";
  Column.Int32 42l;
  Column.DateTime (Int64.of_float (Unix.gettimeofday ()), None)
] in

Client.insert_row client "events" 
  ~columns:[("user_id", "String"); ("value", "Int32"); ("timestamp", "DateTime")]
  row
```

### Batch Insert

```ocaml
let rows = [
  [Column.String "user1"; Column.Int32 10l];
  [Column.String "user2"; Column.Int32 20l];
  [Column.String "user3"; Column.Int32 30l];
] in

Client.insert_rows client "users"
  ~columns:[("name", "String"); ("score", "Int32")]
  rows
```

### Advanced Usage with Custom Configuration

```ocaml
(* Create custom configuration *)
let config = { 
  (Async_insert.default_config "events") with
  max_batch_size = 10000;     (* Batch up to 10k rows *)
  max_batch_bytes = 5_000_000; (* Or 5MB of data *)
  flush_interval = 10.0;       (* Flush every 10 seconds *)
  max_retries = 5;             (* Retry failed batches 5 times *)
  retry_delay = 2.0;           (* Start with 2 second retry delay *)
} in

(* Create and start async inserter *)
let inserter = Async_insert.create config client.conn in
Async_insert.start inserter;

(* Add rows continuously *)
let rec add_events () =
  let event = generate_event_data () in
  let%lwt () = Async_insert.add_row inserter event in
  (* Your application logic continues immediately *)
  let%lwt () = Lwt_unix.sleep 0.1 in
  add_events ()
in

Lwt.async add_events;

(* Later, when shutting down *)
let%lwt () = Async_insert.stop inserter in
Client.disconnect client
```

## Configuration Options

### `Async_insert.config`

- **`table_name`**: Target table name
- **`max_batch_size`**: Maximum rows per batch (default: 1000)
- **`max_batch_bytes`**: Maximum bytes per batch (default: 1MB)
- **`flush_interval`**: Seconds between automatic flushes (default: 5.0)
- **`max_retries`**: Maximum retry attempts on failure (default: 3)
- **`retry_delay`**: Initial retry delay in seconds (default: 1.0)

## API Reference

### Client Methods

- `Client.insert_row`: Insert a single row (convenience method)
- `Client.insert_rows`: Insert multiple rows (convenience method)
- `Client.create_async_inserter`: Create a reusable async inserter

### Async_insert Methods

- `Async_insert.create`: Create an async inserter
- `Async_insert.start`: Start background processing
- `Async_insert.stop`: Stop and flush remaining data
- `Async_insert.add_row`: Add a single row to buffer
- `Async_insert.add_rows`: Add multiple rows to buffer
- `Async_insert.flush`: Force immediate flush
- `Async_insert.get_stats`: Get current buffer statistics

## Value Types

All standard Proton value types are supported:

```ocaml
[
  Column.String "text_value";
  Column.Int32 42l;
  Column.Int64 1234567890L;
  Column.Float64 3.14159;
  Column.DateTime (timestamp, Some "UTC");
  Column.DateTime64 (timestamp, 3, Some "UTC");
  Column.Enum8 ("status", 1);
  Column.Array [| Int32 1l; Int32 2l; Int32 3l |];
  Column.Map [(String "key", String "value")];
]
```

## Performance Considerations

1. **Batch size**: Larger batches reduce network overhead but use more memory
2. **Flush interval**: Longer intervals improve throughput but increase latency
3. **Retry strategy**: More retries improve reliability but can delay error detection
4. **Binary protocol**: Uses native ClickHouse binary format for maximum efficiency

## Error Handling

The async inserter handles errors gracefully:
- Network failures trigger automatic retries with exponential backoff
- After max retries, errors are logged but don't crash your application
- Use `Async_insert.stop` to ensure all data is flushed before shutdown

## Thread Safety

The async inserter is thread-safe and uses Lwt mutexes internally. Multiple threads can safely call `add_row` concurrently.

## Real-World Example

```ocaml
(* High-throughput event ingestion system *)
open Lwt.Syntax

let setup_event_pipeline () =
  let client = Client.create ~host:"proton-cluster" ~database:"analytics" () in
  
  let config = { 
    (Async_insert.default_config "events") with
    max_batch_size = 5000;
    max_batch_bytes = 2_000_000;
    flush_interval = 5.0;
    max_retries = 3;
  } in
  
  let inserter = Async_insert.create config client.conn in
  Async_insert.start inserter;
  
  (* Process incoming events *)
  let process_event event_json =
    let event_row = [
      Column.String event_json.user_id;
      Column.String event_json.event_type;
      Column.DateTime (Int64.of_float event_json.timestamp, Some "UTC");
      Column.String (Yojson.to_string event_json.properties);
    ] in
    Async_insert.add_row inserter event_row
  in
  
  (* Setup graceful shutdown *)
  Lwt_unix.on_signal Sys.sigterm (fun _ ->
    Printf.printf "Shutting down event pipeline...\n";
    Lwt.async (fun () ->
      let* () = Async_insert.stop inserter in
      let* () = Client.disconnect client in
      Lwt_io.println "Pipeline shutdown complete"
    )
  );
  
  (inserter, process_event)
```
