type server_info = {
  name : string;
  version_major : int;
  version_minor : int;
  version_patch : int;
  revision : int;
  timezone : string option;
  display_name : string;
}

type t = {
  mutable server_info : server_info option;
  mutable settings : (string * string) list; (* serialized-as-strings path *)
  mutable client_settings : (string * string) list;
}

let make () = { server_info = None; settings = []; client_settings = [] }
