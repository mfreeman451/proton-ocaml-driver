type client_packet =
  | Hello
  | Query
  | Data
  | Cancel
  | Ping
  | TablesStatusRequest

let client_packet_to_int = function
  | Hello -> 0 | Query -> 1 | Data -> 2 | Cancel -> 3 | Ping -> 4 | TablesStatusRequest -> 5

type server_packet =
  | SHello
  | SData
  | SException
  | SProgress
  | SPong
  | SEndOfStream
  | SProfileInfo
  | STotals
  | SExtremes
  | STablesStatusResponse
  | SLog
  | STableColumns
  | SPartUUIDs
  | SReadTaskRequest
  | SProfileEvents

let server_packet_of_int = function
  | 0 -> SHello | 1 -> SData | 2 -> SException | 3 -> SProgress
  | 4 -> SPong | 5 -> SEndOfStream | 6 -> SProfileInfo | 7 -> STotals
  | 8 -> SExtremes | 9 -> STablesStatusResponse | 10 -> SLog
  | 11 -> STableColumns | 12 -> SPartUUIDs | 13 -> SReadTaskRequest
  | 14 -> SProfileEvents
  | n -> failwith (Printf.sprintf "Unknown server packet: %d" n)

(* Use compression types from Compress module *)
type compression = Compress.method_t

let compression_to_int = function
  | Compress.None -> 0
  | Compress.LZ4 -> 1
  | Compress.ZSTD -> 1  (* Also enabled, but different method *)

type query_processing_stage = FetchColumns | WithMergeableState | Complete
let qps_to_int = function FetchColumns -> 0 | WithMergeableState -> 1 | Complete -> 2
