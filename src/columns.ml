include Columns_types

let trim s = String.trim s

let rec reader_of_spec (spec:string) : (in_channel -> int -> value array) =
  let s = String.lowercase_ascii (trim spec) in
  match Columns_primitives.reader_primitive_of_spec s with
  | Some r -> r
  | None -> (match Columns_datetime.reader_datetime_of_spec s with
             | Some r -> r
             | None -> (match Columns_complex.reader_complex_of_spec ~resolver:reader_of_spec s with
                        | Some r -> r
                        | None -> (match Columns_lowcardinality.reader_lowcardinality_of_spec ~resolver:reader_of_spec s with
                                   | Some r -> r
                                   | None -> failwith (Printf.sprintf "Unsupported column type: %s" spec))))

let rec reader_of_spec_br (spec:string) : (Buffered_reader.t -> int -> value array) =
  let s = String.lowercase_ascii (trim spec) in
  match Columns_primitives.reader_primitive_of_spec_br s with
  | Some r -> r
  | None -> (match Columns_datetime.reader_datetime_of_spec_br s with
             | Some r -> r
             | None -> (match Columns_complex.reader_complex_of_spec_br ~resolver:reader_of_spec_br s with
                        | Some r -> r
                        | None -> (match Columns_lowcardinality.reader_lowcardinality_of_spec_br ~resolver:reader_of_spec_br s with
                                   | Some r -> r
                                   | None -> failwith (Printf.sprintf "Unsupported column type: %s" spec))))

