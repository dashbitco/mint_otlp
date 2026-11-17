defmodule Mint.OTLP.Encoder do
  @moduledoc """
  Encodes OTLP export requests for traces and logs as protobuf.

  Telemetry is encoded upfront, as it is produced, and requests are
  built from the encoded parts. Each function returns a binary already
  encoded as a field of the message that contains it, such as a span
  within its `ScopeSpans` or a log record within its `ScopeLogs`. As protobuf fields may be concatenated, the
  parts can be stored, counted with `byte_size/1`, and put together
  into requests without being encoded again.

      resource = Mint.OTLP.Encoder.resource(%{"service.name" => "my_app"})
      scope = Mint.OTLP.Encoder.scope("my_lib", version: "1.0.0")
      span = Mint.OTLP.Encoder.span(trace_id, span_id, "GET /users", started_at, ended_at)

      request = Mint.OTLP.Encoder.traces(resource, [{scope, [span]}])
      {:ok, client, ref} = Mint.OTLP.export(client, request)

      log_record = Mint.OTLP.Encoder.log_record(time, "hello", severity: :info)
      request = Mint.OTLP.Encoder.logs(resource, [{scope, [log_record]}])
      {:ok, client, ref} = Mint.OTLP.export(client, request)

  The parts are not tagged with their type, so they must be given to the
  functions that expect them.

  ## Attributes

  Attributes are given as a map or a list of key-value pairs. Keys are
  strings or atoms. Values are encoded according to their type:

    * strings and atoms are strings
    * `true` and `false` are booleans
    * integers are 64-bit integers
    * floats are doubles
    * lists are arrays of values
    * maps are lists of key-value pairs

  ## Timestamps

  Timestamps are integers in nanoseconds since the Unix epoch, such as
  `System.os_time(:nanosecond)`.
  """

  import Bitwise

  @kinds %{internal: 1, server: 2, client: 3, producer: 4, consumer: 5}

  # The syslog mapping from the OpenTelemetry logs data model
  @severities %{
    debug: 5,
    info: 9,
    notice: 10,
    warning: 13,
    error: 17,
    critical: 18,
    alert: 19,
    emergency: 21
  }

  @doc """
  Encodes an `ExportTraceServiceRequest` with the given resource, as
  returned by `resource/1`, and spans grouped by scope, as returned by
  `scope/2` and `span/6`:

      message ExportTraceServiceRequest {
        repeated ResourceSpans resource_spans = 1;
      }

      message ResourceSpans {
        Resource resource = 1;
        repeated ScopeSpans scope_spans = 2;
      }

      message ScopeSpans {
        InstrumentationScope scope = 1;
        repeated Span spans = 2;
      }

  Returns `{:traces, iodata}`, to be given to `Mint.OTLP.export/3`.
  The iodata references the given binaries without copying them.
  """
  @spec traces(binary(), [{binary(), [binary()]}]) :: Mint.OTLP.request()
  def traces(resource, scope_spans) when is_binary(resource) and is_list(scope_spans) do
    {:traces, request(resource, scope_spans)}
  end

  @doc """
  Encodes an `ExportLogsServiceRequest` with the given resource, as
  returned by `resource/1`, and log records grouped by scope, as returned
  by `scope/2` and `log_record/3`:

      message ExportLogsServiceRequest {
        repeated ResourceLogs resource_logs = 1;
      }

      message ResourceLogs {
        Resource resource = 1;
        repeated ScopeLogs scope_logs = 2;
      }

      message ScopeLogs {
        InstrumentationScope scope = 1;
        repeated LogRecord log_records = 2;
      }

  Returns `{:logs, iodata}`, to be given to `Mint.OTLP.export/3`.
  The iodata references the given binaries without copying them.
  """
  @spec logs(binary(), [{binary(), [binary()]}]) :: Mint.OTLP.request()
  def logs(resource, scope_logs) when is_binary(resource) and is_list(scope_logs) do
    {:logs, request(resource, scope_logs)}
  end

  # Requests for all signals have the same shape, with a single resource
  # as field 1, and items grouped by scope as field 2.
  defp request(resource, scopes) do
    {scopes, size} =
      Enum.map_reduce(scopes, byte_size(resource), fn {scope, items}, acc ->
        size = Enum.reduce(items, byte_size(scope), &(byte_size(&1) + &2))
        prefix = <<0x12, varint(size)::binary>>
        {[prefix, scope | items], acc + byte_size(prefix) + size}
      end)

    [<<0x0A, varint(size)::binary>>, resource | scopes]
  end

  @doc """
  Encodes a `Resource`, which describes the entity producing telemetry,
  such as with the `service.name` attribute:

      message Resource {
        repeated KeyValue attributes = 1;
      }

  """
  @spec resource(Enumerable.t()) :: binary()
  def resource(attributes) do
    message(1, attributes(1, attributes))
  end

  @doc """
  Encodes an `InstrumentationScope`, which describes the library that
  produced the telemetry:

      message InstrumentationScope {
        string name = 1;
        string version = 2;
        repeated KeyValue attributes = 3;
      }

  ## Options

    * `:version` - the version of the library

    * `:attributes` - the attributes of the scope

  """
  @spec scope(String.t(), keyword()) :: binary()
  def scope(name, opts \\ []) when is_binary(name) do
    message(1, [
      string(1, name),
      string(2, Keyword.get(opts, :version, "")),
      attributes(3, Keyword.get(opts, :attributes, []))
    ])
  end

  @doc """
  Encodes a `Span`:

      message Span {
        bytes trace_id = 1;
        bytes span_id = 2;
        string trace_state = 3;
        bytes parent_span_id = 4;
        fixed32 flags = 16;
        string name = 5;
        SpanKind kind = 6;
        fixed64 start_time_unix_nano = 7;
        fixed64 end_time_unix_nano = 8;
        repeated KeyValue attributes = 9;
        repeated Event events = 11;
        repeated Link links = 13;
        Status status = 15;
      }

      message Status {
        string message = 2;
        StatusCode code = 3;
      }

  `trace_id` is a 16-byte binary and `span_id` an 8-byte binary.

  ## Options

    * `:parent_span_id` - the 8-byte id of the parent span, if any

    * `:trace_state` - the W3C trace state

    * `:flags` - the W3C trace flags, in the lower 8 bits, and whether
      the parent is remote, in bits 8 and 9

    * `:kind` - one of `:internal`, `:server`, `:client`, `:producer`,
      or `:consumer`

    * `:attributes` - the attributes of the span

    * `:events` - events, as returned by `event/3`

    * `:links` - links, as returned by `link/3`

    * `:status` - `:ok`, `:error`, or `{:error, message}`

  """
  @spec span(binary(), binary(), String.t(), integer(), integer(), keyword()) :: binary()
  def span(trace_id, span_id, name, start_time, end_time, opts \\ [])
      when is_binary(trace_id) and is_binary(span_id) and is_binary(name) and
             is_integer(start_time) and is_integer(end_time) do
    message(2, [
      string(1, trace_id),
      string(2, span_id),
      string(3, Keyword.get(opts, :trace_state, "")),
      string(4, Keyword.get(opts, :parent_span_id, "")),
      string(5, name),
      kind(Keyword.get(opts, :kind)),
      <<0x39, start_time::little-64, 0x41, end_time::little-64>>,
      attributes(9, Keyword.get(opts, :attributes, [])),
      Keyword.get(opts, :events, []),
      Keyword.get(opts, :links, []),
      status(Keyword.get(opts, :status)),
      flags(Keyword.get(opts, :flags))
    ])
  end

  defp kind(nil), do: []
  defp kind(kind), do: <<0x30, Map.fetch!(@kinds, kind)>>

  defp status(nil), do: []
  defp status(:ok), do: field(15, <<0x18, 1>>)
  defp status(:error), do: field(15, <<0x18, 2>>)
  defp status({:error, message}), do: field(15, [string(2, message), <<0x18, 2>>])

  defp flags(nil), do: []
  defp flags(flags), do: <<0x85, 0x01, flags::little-32>>

  defp fixed32(_tag, nil), do: []
  defp fixed32(tag, value), do: <<tag, value::little-32>>

  defp fixed64(_tag, nil), do: []
  defp fixed64(tag, value), do: <<tag, value::little-64>>

  @doc """
  Encodes an `Event` of a span:

      message Event {
        fixed64 time_unix_nano = 1;
        string name = 2;
        repeated KeyValue attributes = 3;
      }

  """
  @spec event(integer(), String.t(), Enumerable.t()) :: binary()
  def event(time, name, attributes \\ []) when is_integer(time) and is_binary(name) do
    message(11, [<<0x09, time::little-64>>, string(2, name), attributes(3, attributes)])
  end

  @doc """
  Encodes a `Link` from a span to another span:

      message Link {
        bytes trace_id = 1;
        bytes span_id = 2;
        string trace_state = 3;
        repeated KeyValue attributes = 4;
        fixed32 flags = 6;
      }

  ## Options

    * `:trace_state` - the W3C trace state of the linked span

    * `:flags` - the W3C trace flags of the linked span, as in `span/6`

    * `:attributes` - the attributes of the link

  """
  @spec link(binary(), binary(), keyword()) :: binary()
  def link(trace_id, span_id, opts \\ []) when is_binary(trace_id) and is_binary(span_id) do
    message(13, [
      string(1, trace_id),
      string(2, span_id),
      string(3, Keyword.get(opts, :trace_state, "")),
      attributes(4, Keyword.get(opts, :attributes, [])),
      fixed32(0x35, Keyword.get(opts, :flags))
    ])
  end

  @doc """
  Encodes a `LogRecord`:

      message LogRecord {
        fixed64 time_unix_nano = 1;
        fixed64 observed_time_unix_nano = 11;
        SeverityNumber severity_number = 2;
        string severity_text = 3;
        AnyValue body = 5;
        repeated KeyValue attributes = 6;
        fixed32 flags = 8;
        bytes trace_id = 9;
        bytes span_id = 10;
        string event_name = 12;
      }

  `body` is encoded as an attribute value, such as a string or a map,
  and it is not encoded if `nil`.

  ## Options

    * `:observed_time` - when the log record was observed, if it differs
      from when it happened

    * `:severity` - a `Logger` level, such as `:info`, encoded as the
      severity text and number, according to the syslog mapping in the
      OpenTelemetry logs data model

    * `:attributes` - the attributes of the log record

    * `:trace_id` - the 16-byte id of the trace of the log record, if any

    * `:span_id` - the 8-byte id of the span of the log record, if any

    * `:flags` - the W3C trace flags, as in `span/6`

    * `:event_name` - the name of the event, if the log record is one

  """
  @spec log_record(integer(), term(), keyword()) :: binary()
  def log_record(time, body, opts \\ []) when is_integer(time) do
    message(2, [
      <<0x09, time::little-64>>,
      severity(Keyword.get(opts, :severity)),
      if(body != nil, do: field(5, any_value(body)), else: []),
      attributes(6, Keyword.get(opts, :attributes, [])),
      fixed32(0x45, Keyword.get(opts, :flags)),
      string(9, Keyword.get(opts, :trace_id, "")),
      string(10, Keyword.get(opts, :span_id, "")),
      fixed64(0x59, Keyword.get(opts, :observed_time)),
      string(12, Keyword.get(opts, :event_name, ""))
    ])
  end

  defp severity(nil), do: []

  defp severity(level) do
    [<<0x10, Map.fetch!(@severities, level)>> | string(3, Atom.to_string(level))]
  end

  ## Common

  #     message KeyValue {
  #       string key = 1;
  #       AnyValue value = 2;
  #     }
  defp attributes(number, attributes) do
    Enum.map(attributes, fn {key, value} -> field(number, key_value(key, value)) end)
  end

  defp key_value(key, value), do: [string(1, to_string(key)), field(2, any_value(value))]

  #     message AnyValue {
  #       oneof value {
  #         string string_value = 1;
  #         bool bool_value = 2;
  #         int64 int_value = 3;
  #         double double_value = 4;
  #         ArrayValue array_value = 5;
  #         KeyValueList kvlist_value = 6;
  #       }
  #     }
  #
  #     message ArrayValue {
  #       repeated AnyValue values = 1;
  #     }
  #
  #     message KeyValueList {
  #       repeated KeyValue values = 1;
  #     }
  #
  # Unlike other fields, values are encoded even when empty, as they are
  # in a oneof.
  defp any_value(value) when is_binary(value), do: field(1, value)
  defp any_value(true), do: <<0x10, 1>>
  defp any_value(false), do: <<0x10, 0>>
  defp any_value(value) when is_atom(value), do: field(1, Atom.to_string(value))
  defp any_value(value) when is_integer(value), do: [0x18 | varint(value &&& 0xFFFFFFFFFFFFFFFF)]
  defp any_value(value) when is_float(value), do: <<0x21, value::little-float-64>>

  defp any_value(value) when is_list(value),
    do: field(5, Enum.map(value, &field(1, any_value(&1))))

  defp any_value(value) when is_map(value),
    do: field(6, Enum.map(value, fn {key, value} -> field(1, key_value(key, value)) end))

  # Empty strings are the default, so they are not encoded
  defp string(_number, ""), do: []
  defp string(number, binary) when is_binary(binary), do: field(number, binary)

  defp message(number, iodata), do: IO.iodata_to_binary(field(number, iodata))

  defp field(number, iodata),
    do: [varint(number <<< 3 ||| 2), varint(IO.iodata_length(iodata)) | iodata]

  defp varint(n) when n < 128, do: <<n>>
  defp varint(n), do: <<1::1, n &&& 127::7, varint(n >>> 7)::binary>>
end
