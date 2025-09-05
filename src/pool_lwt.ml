open Lwt.Infix

type pool_config = {
  max_open: int;
  max_idle: int;
  idle_timeout: float;    (* seconds *)
  max_lifetime: float;    (* seconds *)
}

type pooled_connection = {
  conn: Connection.t;
  mutable last_used: float;
  created: float;
}

type connection_pool = {
  config: pool_config;
  mutable idle: pooled_connection list;
  mutable total_count: int;
  mutex: Lwt_mutex.t;
  cond: unit Lwt_condition.t;
  create_connection: unit -> Connection.t;
  mutable cleanup_stopper: bool;
}

let default_pool_config = { max_open = 20; max_idle = 5; idle_timeout = 300.0; max_lifetime = 3600.0 }

let now () = Unix.gettimeofday ()

let create_pool ?(config=default_pool_config) create_fn = {
  config;
  idle = [];
  total_count = 0;
  mutex = Lwt_mutex.create ();
  cond = Lwt_condition.create ();
  create_connection = create_fn;
  cleanup_stopper = false;
}

let rec get_connection pool : Connection.t Lwt.t =
  Lwt_mutex.with_lock pool.mutex (fun () ->
    match pool.idle with
    | c::rest -> pool.idle <- rest; Lwt.return (`Conn c.conn)
    | [] ->
        if pool.total_count < pool.config.max_open then (
          let conn = pool.create_connection () in
          pool.total_count <- pool.total_count + 1;
          Lwt.return (`Conn conn)
        ) else (
          Lwt_condition.wait ~mutex:pool.mutex pool.cond >|= fun () -> `Wait
        )
  ) >>= function
  | `Conn c -> Lwt.return c
  | `Wait -> get_connection pool

let return_connection pool (conn:Connection.t) : unit Lwt.t =
  Lwt_mutex.with_lock pool.mutex (fun () ->
    if List.length pool.idle < pool.config.max_idle then (
      pool.idle <- { conn; last_used = now (); created = now () } :: pool.idle;
      Lwt_condition.signal pool.cond (); Lwt.return_unit
    ) else (
      pool.total_count <- pool.total_count - 1;
      Connection.disconnect conn
    )
  )

let with_connection pool (f:Connection.t -> 'a Lwt.t) : 'a Lwt.t =
  get_connection pool >>= fun conn ->
  Lwt.finalize
    (fun () -> f conn)
    (fun () -> return_connection pool conn)

let rec cleanup_loop pool period : unit Lwt.t =
  if pool.cleanup_stopper then Lwt.return_unit else
  Lwt_unix.sleep period >>= fun () ->
  let cutoff_idle = now () -. pool.config.idle_timeout in
  let cutoff_create = now () -. pool.config.max_lifetime in
  Lwt_mutex.with_lock pool.mutex (fun () ->
    let keep, drop = List.partition (fun pc -> pc.last_used >= cutoff_idle && pc.created >= cutoff_create) pool.idle in
    pool.idle <- keep;
    let to_close = List.length drop in
    pool.total_count <- pool.total_count - to_close;
    Lwt.return drop
  ) >>= fun drop_list ->
  Lwt_list.iter_s (fun pc -> Connection.disconnect pc.conn) drop_list >>= fun () ->
  cleanup_loop pool period

let start_cleanup pool ~period =
  pool.cleanup_stopper <- false;
  Lwt.async (fun () -> cleanup_loop pool period)

let stop_cleanup pool = pool.cleanup_stopper <- true

let pool_stats pool : (int * int) Lwt.t =
  Lwt_mutex.with_lock pool.mutex (fun () -> Lwt.return (pool.total_count, List.length pool.idle))
