open Connection

type pool_config = {
  max_open: int;
  max_idle: int;
  max_lifetime: float;
  idle_timeout: float;
}

type pooled_connection = {
  conn: Connection.t;
  mutable last_used: float;
  created: float;
}

type connection_pool = {
  config: pool_config;
  mutable idle_connections: pooled_connection Queue.t;
  mutable active_count: int;
  mutable total_count: int;
  mutex: Mutex.t;
  create_connection: unit -> Connection.t;
}

let default_pool_config = {
  max_open = 20;
  max_idle = 5;
  max_lifetime = 3600.0; (* 1 hour *)
  idle_timeout = 300.0;  (* 5 minutes *)
}

let create_pool ?(config=default_pool_config) create_fn = {
  config;
  idle_connections = Queue.create ();
  active_count = 0;
  total_count = 0;
  mutex = Mutex.create ();
  create_connection = create_fn;
}

let current_time () = Unix.time ()

let is_connection_expired pooled_conn now config =
  (now -. pooled_conn.created > config.max_lifetime) ||
  (now -. pooled_conn.last_used > config.idle_timeout)

let cleanup_expired_connections pool =
  let now = current_time () in
  let cleaned_queue = Queue.create () in
  let expired_count = ref 0 in
  
  Queue.iter (fun pooled_conn ->
    if is_connection_expired pooled_conn now pool.config then (
      Connection.disconnect pooled_conn.conn;
      incr expired_count;
      pool.total_count <- pool.total_count - 1
    ) else (
      Queue.add pooled_conn cleaned_queue
    )
  ) pool.idle_connections;
  
  pool.idle_connections <- cleaned_queue;
  !expired_count

let get_connection pool =
  Mutex.lock pool.mutex;
  let result = 
    try
      (* First, clean up expired connections *)
      let _ = cleanup_expired_connections pool in
      
      (* Try to get an idle connection *)
      if not (Queue.is_empty pool.idle_connections) then (
        let pooled_conn = Queue.take pool.idle_connections in
        pooled_conn.last_used <- current_time ();
        pool.active_count <- pool.active_count + 1;
        Ok pooled_conn.conn
      )
      (* Check if we can create a new connection *)
      else if pool.total_count < pool.config.max_open then (
        let conn = pool.create_connection () in
        pool.total_count <- pool.total_count + 1;
        pool.active_count <- pool.active_count + 1;
        Ok conn
      )
      (* Pool is at capacity *)
      else (
        Error "Connection pool at maximum capacity"
      )
    with
    | exn -> Error ("Failed to get connection: " ^ Printexc.to_string exn)
  in
  Mutex.unlock pool.mutex;
  result

let return_connection pool conn =
  Mutex.lock pool.mutex;
  (try
    pool.active_count <- pool.active_count - 1;
    
    (* Check if we should keep this connection in the idle pool *)
    if Queue.length pool.idle_connections < pool.config.max_idle then (
      let pooled_conn = {
        conn = conn;
        last_used = current_time ();
        created = current_time (); (* Approximate - we don't track creation time *)
      } in
      Queue.add pooled_conn pool.idle_connections
    ) else (
      (* Close excess connections *)
      Connection.disconnect conn;
      pool.total_count <- pool.total_count - 1
    )
  with
  | exn -> 
    (* Ensure we always decrement active count *)
    Connection.disconnect conn;
    pool.total_count <- pool.total_count - 1;
    Printf.eprintf "Error returning connection to pool: %s\n" (Printexc.to_string exn)
  );
  Mutex.unlock pool.mutex

let close_pool pool =
  Mutex.lock pool.mutex;
  (try
    Queue.iter (fun pooled_conn ->
      Connection.disconnect pooled_conn.conn
    ) pool.idle_connections;
    Queue.clear pool.idle_connections;
    pool.total_count <- 0;
    pool.active_count <- 0;
  with
  | exn -> Printf.eprintf "Error closing connection pool: %s\n" (Printexc.to_string exn)
  );
  Mutex.unlock pool.mutex

let pool_stats pool =
  Mutex.lock pool.mutex;
  let stats = (pool.active_count, Queue.length pool.idle_connections, pool.total_count) in
  Mutex.unlock pool.mutex;
  stats

let with_connection pool f =
  match get_connection pool with
  | Ok conn ->
      (try
        let result = f conn in
        return_connection pool conn;
        Ok result
      with
      | exn ->
        return_connection pool conn;
        Error ("Connection operation failed: " ^ Printexc.to_string exn)
      )
  | Error msg -> Error msg