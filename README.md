# 🐪 Timeplus Proton OCaml Driver

A high-performance, feature-rich OCaml driver for [Timeplus Proton](https://timeplus.com/) - the streaming database built on ClickHouse.

## ✨ Features

- **🚀 Streaming Queries** - Process large datasets with constant memory usage
- **🔄 Async Inserts** - High-throughput data ingestion with automatic batching
- **🗜️ Compression** - LZ4 and ZSTD support for reduced network overhead  
- **🔐 TLS Security** - Secure connections with certificate validation
- **🏊 Connection Pooling** - Efficient resource management for high-concurrency applications
- **📊 Rich Data Types** - Full support for ClickHouse types including Arrays, Maps, Enums, DateTime64
- **🎯 Idiomatic OCaml** - Functional API leveraging OCaml's strengths

## 🚀 Quick Start

### Installation

```bash
opam install proton
```

### Basic Usage

```ocaml
open Lwt.Infix
open Proton

(* Create a client *)
let client = Client.create ~host:"localhost" ~port:8463 ~database:"default" () in

(* Execute a simple query *)
let%lwt result = Client.execute client "SELECT name, age FROM users LIMIT 10" in
match result with
| Client.NoRows -> Lwt_io.println "No results found"
| Client.Rows (rows, columns) -> 
    List.iter (fun row ->
      let values = List.map Columns.value_to_string row in
      Printf.printf "%s\n" (String.concat ", " values)
    ) rows;
    Lwt.return_unit
```

## 🌊 Streaming Queries

Process arbitrarily large result sets with constant memory usage using OCaml's functional patterns:

### Fold Pattern - Aggregate Data

```ocaml
(* Calculate total sales amount *)
let%lwt total_sales = Client.query_fold client
  "SELECT amount FROM sales WHERE date >= '2024-01-01'"
  ~init:0.0
  ~f:(fun acc row -> match row with
    | [Columns.VFloat64 amount] -> Lwt.return (acc +. amount)  
    | _ -> Lwt.return acc)
in
Printf.printf "Total sales: $%.2f\n" total_sales
```

### Iter Pattern - Process Each Row

```ocaml
(* Send personalized emails *)
let%lwt () = Client.query_iter client
  "SELECT name, email FROM users WHERE active = 1"
  ~f:(fun row -> match row with
    | [Columns.VString name; Columns.VString email] ->
        send_email ~to:email ~subject:("Hi " ^ name) ~body:"..."
    | _ -> Lwt.return_unit)
```

### Sequence Pattern - Lazy Processing Pipeline

```ocaml
(* Process data in pipeline with lazy evaluation *)
let%lwt user_seq = Client.query_to_seq client "SELECT * FROM users" in
let processed = user_seq
  |> Seq.filter is_premium_user
  |> Seq.map extract_preferences  
  |> Seq.take 1000
  |> Seq.fold_left update_recommendations init_state
```

### Advanced - With Column Metadata

```ocaml
(* Get both data and column information *)
let%lwt result = Client.query_fold_with_columns client
  "SELECT id, name, created_at FROM products"
  ~init:[]
  ~f:(fun acc row columns ->
    let product = parse_product_row row columns in
    Lwt.return (product :: acc))
in
Printf.printf "Found %d products with columns: %s\n"
  (List.length result.rows)
  (String.concat "," (List.map fst result.columns))
```

## ⚡ High-Performance Async Inserts

Insert data efficiently with automatic batching, compression, and retry logic:

### Simple Batch Insert

```ocaml
(* Insert multiple rows efficiently *)
let rows = [
  [Columns.VString "user123"; Columns.VInt32 25l; Columns.VFloat64 99.99];
  [Columns.VString "user456"; Columns.VInt32 30l; Columns.VFloat64 149.50];
  [Columns.VString "user789"; Columns.VInt32 22l; Columns.VFloat64 75.25];
] in

let%lwt () = Client.insert_rows client "orders"
  ~columns:[("user_id", "String"); ("age", "Int32"); ("amount", "Float64")]
  rows
```

### Advanced Async Insert with Configuration

```ocaml
(* High-throughput streaming inserts *)
let config = { 
  (Async_insert.default_config "events") with
  max_batch_size = 10000;      (* Batch up to 10k rows *)
  max_batch_bytes = 5_000_000; (* Or 5MB of data *)
  flush_interval = 10.0;       (* Flush every 10 seconds *)
  max_retries = 5;             (* Retry failed batches 5 times *)
} in

let inserter = Client.create_async_inserter ~config client "events" in
Async_insert.start inserter;

(* Add rows continuously - non-blocking *)
let rec stream_events () =
  let event = generate_event () in
  let%lwt () = Async_insert.add_row inserter event in
  Lwt_unix.sleep 0.1 >>= stream_events
in
Lwt.async stream_events;

(* Later, clean shutdown *)
let%lwt () = Async_insert.stop inserter in
Client.disconnect client
```

## 🎯 Data Types

Full support for ClickHouse data types with OCaml-native representations:

```ocaml
open Columns

let sample_row = [
  VString "Hello World";
  VInt32 42l;
  VInt64 1234567890L;
  VFloat64 3.14159;
  VDateTime (Int64.of_float (Unix.gettimeofday ()), Some "UTC");
  VDateTime64 (Int64.of_float (Unix.gettimeofday () *. 1000.), 3, Some "UTC");
  VEnum8 ("status", 1);
  VArray [VInt32 1l; VInt32 2l; VInt32 3l];
  VMap [(VString "key1", VString "value1"); (VString "key2", VString "value2")];
]
```

## 🔧 Configuration Options

### Connection Configuration

```ocaml
let client = Client.create
  ~host:"proton.example.com"
  ~port:8463
  ~database:"analytics" 
  ~user:"readonly"
  ~password:"secret123"
  ~compression:Protocol.LZ4      (* or ZSTD, or None *)
  ~tls_config:{
    ca_file = Some "/path/to/ca.pem";
    cert_file = Some "/path/to/client.pem"; 
    key_file = Some "/path/to/client.key";
    verify_hostname = true;
  }
  ~settings:[
    ("max_block_size", "65536");
    ("connect_timeout", "10");
  ]
  ()
```

### Connection Pooling

```ocaml
(* For high-concurrency applications *)
let pool = Pool_lwt.create 
  ~max_connections:20
  ~create:(fun () -> 
    Client.create ~host:"localhost" ~database:"default" ())
  ~validate:(fun client -> 
    Lwt.catch (fun () -> Client.execute client "SELECT 1" >|= fun _ -> true) 
              (fun _ -> Lwt.return false))
  ()
in

Pool_lwt.use pool (fun client ->
  Client.query_fold client "SELECT * FROM large_table" ~init:0 ~f:(...)
)
```

## 📊 Real-World Examples

### Analytics Pipeline

```ocaml
(* Process clickstream data *)
let process_clickstream () =
  let%lwt () = Client.query_iter client
    "SELECT user_id, page, timestamp FROM clicks WHERE date = today()"
    ~f:(fun row -> match row with
      | [VString user_id; VString page; VDateTime (ts, _)] ->
          update_user_session user_id page ts
      | _ -> Lwt.return_unit)
  in
  Lwt_io.println "Clickstream processing complete"

(* Real-time aggregation *)
let calculate_metrics () =
  let%lwt metrics = Client.query_fold_with_columns client
    "SELECT country, COUNT(*) as visits, AVG(duration) as avg_duration 
     FROM sessions 
     WHERE timestamp >= now() - INTERVAL 1 HOUR
     GROUP BY country"
    ~init:[]
    ~f:(fun acc row _columns -> match row with
      | [VString country; VInt64 visits; VFloat64 duration] ->
          let metric = { country; visits = Int64.to_int visits; avg_duration = duration } in
          Lwt.return (metric :: acc)
      | _ -> Lwt.return acc)
  in
  publish_metrics_to_dashboard metrics.rows
```

### ETL Processing

```ocaml
(* Extract, Transform, Load pipeline *)
let etl_pipeline source_query transform_fn target_table =
  let inserter = Client.create_async_inserter client target_table in
  Async_insert.start inserter;
  
  let%lwt () = Client.query_iter client source_query
    ~f:(fun row ->
      let transformed = transform_fn row in
      Async_insert.add_row inserter transformed)
  in
  
  Async_insert.stop inserter
```

## 🛠️ Development Setup

### Prerequisites

```bash
# macOS
brew install opam pkg-config openssl@3 lz4

# Ubuntu/Debian  
sudo apt install opam pkg-config libssl-dev liblz4-dev build-essential

# Setup OCaml
opam init --disable-sandboxing
opam switch create proton 4.14.0
opam install dune lwt tls-lwt lz4 zstd alcotest
```

### Build & Test

```bash
git clone <repository-url>
cd proton-ocaml-driver
dune build
dune runtest
```

## 🤝 Contributing

Contributions are welcome! Please see our [contribution guidelines](CONTRIBUTING.md).

## 📝 License

This project is licensed under the Apache 2.0 License - see the [LICENSE](LICENSE) file for details.

## 🆘 Support

- **Documentation**: [Full API Documentation](docs/)
- **Issues**: [GitHub Issues](https://github.com/your-org/proton-ocaml-driver/issues)
- **Discussions**: [GitHub Discussions](https://github.com/your-org/proton-ocaml-driver/discussions)
- **Timeplus Community**: [Timeplus Slack](https://timeplus.com/slack)

---

Made with ❤️ for the OCaml and streaming analytics community.