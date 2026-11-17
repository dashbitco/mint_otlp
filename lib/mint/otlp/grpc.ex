defmodule Mint.OTLP.GRPC do
  @moduledoc """
  OTLP/gRPC transport.

  Each export is a unary gRPC call to the `Export` method of the
  signal's service, over a `Mint.HTTP2` connection. Use the functions
  in `Mint.OTLP` to work with it.

  ## Flow control

  `Mint.HTTP2` keeps track of the HTTP/2 flow control windows but never
  waits on them. This module sends request bodies in chunks of at most
  the server's `SETTINGS_MAX_FRAME_SIZE`, as long as both the connection
  and the request windows allow, and keeps the rest of the body until
  `Mint.OTLP.stream/2` processes WINDOW_UPDATE frames from the server.
  Requests are sent in the order they were exported.
  """

  @behaviour Mint.OTLP

  alias Mint.HTTP2
  alias Mint.OTLP.{Error, Protobuf}

  @user_agent "mint-otlp/#{Mix.Project.config()[:version]}"

  @paths %{
    traces: "/opentelemetry.proto.collector.trace.v1.TraceService/Export",
    metrics: "/opentelemetry.proto.collector.metrics.v1.MetricsService/Export",
    logs: "/opentelemetry.proto.collector.logs.v1.LogsService/Export"
  }

  @codes %{
    1 => :cancelled,
    2 => :unknown,
    3 => :invalid_argument,
    4 => :deadline_exceeded,
    5 => :not_found,
    6 => :already_exists,
    7 => :permission_denied,
    8 => :resource_exhausted,
    9 => :failed_precondition,
    10 => :aborted,
    11 => :out_of_range,
    12 => :unimplemented,
    13 => :internal,
    14 => :unavailable,
    15 => :data_loss,
    16 => :unauthenticated
  }

  @retry_info "type.googleapis.com/google.rpc.RetryInfo"

  @typedoc """
  A gRPC status code other than `OK`, the `:code` of `Mint.OTLP.Error`.
  """
  @type code() ::
          :cancelled
          | :unknown
          | :invalid_argument
          | :deadline_exceeded
          | :not_found
          | :already_exists
          | :permission_denied
          | :resource_exhausted
          | :failed_precondition
          | :aborted
          | :out_of_range
          | :unimplemented
          | :internal
          | :unavailable
          | :data_loss
          | :unauthenticated

  @opaque t() :: %__MODULE__{
            conn: HTTP2.t(),
            headers: Mint.Types.headers(),
            requests: %{reference() => map()},
            sending: :queue.queue(reference())
          }

  defstruct [:conn, headers: [], requests: %{}, sending: :queue.new()]

  @doc """
  Connects to an OTLP/gRPC endpoint.

  Use the `:http` scheme for HTTP/2 with prior knowledge, which is
  what OTLP/gRPC uses without TLS, and `:https` otherwise.

  ## Options

    * `:headers` - headers to send with every export, such as
      `authorization`

    * `:user_agent` - the `user-agent` header. The OTLP specification
      recommends identifying the exporter and its version. Defaults to
      `"#{@user_agent}"`

  Other options are given to `Mint.HTTP2.connect/4`.
  """
  @impl Mint.OTLP
  def connect(scheme, address, port, opts) do
    {headers, opts} = Keyword.pop(opts, :headers, [])
    {user_agent, opts} = Keyword.pop(opts, :user_agent, @user_agent)

    with {:ok, conn} <- HTTP2.connect(scheme, address, port, opts) do
      headers = [
        {"content-type", "application/grpc"},
        {"te", "trailers"},
        {"user-agent", user_agent} | headers
      ]

      {:ok, %__MODULE__{conn: conn, headers: headers}}
    end
  end

  @impl Mint.OTLP
  def export(%__MODULE__{} = client, signal, request, opts) do
    size = Keyword.get_lazy(opts, :size, fn -> IO.iodata_length(request) end)
    headers = client.headers ++ timeout_header(opts[:timeout]) ++ Keyword.get(opts, :headers, [])

    # An iovec is a flat list of binaries, which we can split when sending
    # without copying. Building it only copies small binaries.
    body = [<<0, size::32>> | :erlang.iolist_to_iovec(request)]
    size = size + 5

    if fits_one_frame?(client.conn, size) do
      case HTTP2.request(client.conn, "POST", @paths[signal], headers, body) do
        {:ok, conn, ref} -> {:ok, put_request(%{client | conn: conn}, ref, [], 0), ref}
        {:error, conn, error} -> {:error, %{client | conn: conn}, error}
      end
    else
      case HTTP2.request(client.conn, "POST", @paths[signal], headers, :stream) do
        {:ok, conn, ref} ->
          client = put_request(%{client | conn: conn}, ref, body, size)

          case send_body(client, ref) do
            {:sent, client, []} ->
              {:ok, client, ref}

            {:blocked, client} ->
              {:ok, %{client | sending: :queue.in(ref, client.sending)}, ref}

            {:sent, client, [{:error, ^ref, error}]} ->
              {:error, client, error}
          end

        {:error, conn, error} ->
          {:error, %{client | conn: conn}, error}
      end
    end
  end

  defp timeout_header(nil), do: []
  defp timeout_header(timeout) when is_integer(timeout), do: [{"grpc-timeout", "#{timeout}m"}]

  defp fits_one_frame?(conn, size) do
    size <= HTTP2.get_window_size(conn, :connection) and
      size <= HTTP2.get_server_setting(conn, :initial_window_size) and
      size <= HTTP2.get_server_setting(conn, :max_frame_size)
  end

  defp put_request(client, ref, body, left) do
    request = %{body: body, left: left, status: nil, headers: [], data: []}
    %{client | requests: Map.put(client.requests, ref, request)}
  end

  @impl Mint.OTLP
  def stream(%__MODULE__{} = client, message) do
    case HTTP2.stream(client.conn, message) do
      {:ok, conn, responses} ->
        {client, responses} = handle_responses(%{client | conn: conn}, responses)
        {client, sent} = send_pending(client)
        {:ok, client, responses ++ sent}

      {:error, conn, error, responses} ->
        {client, responses} = handle_responses(%{client | conn: conn}, responses)

        if HTTP2.open?(conn, :write) do
          {client, sent} = send_pending(client)
          {:error, client, error, responses ++ sent}
        else
          # Exports that did not get a response will not get one anymore
          failed = for {ref, _} <- client.requests, do: {:error, ref, error}
          client = %{client | requests: %{}, sending: :queue.new()}
          {:error, client, error, responses ++ failed}
        end

      :unknown ->
        :unknown
    end
  end

  @impl Mint.OTLP
  def close(%__MODULE__{} = client) do
    {:ok, conn} = HTTP2.close(client.conn)
    {:ok, %{client | conn: conn}}
  end

  @impl Mint.OTLP
  def open?(%__MODULE__{} = client), do: HTTP2.open?(client.conn)

  @impl Mint.OTLP
  def open_request_count(%__MODULE__{} = client), do: map_size(client.requests)

  @doc """
  Returns whether an export that failed with `error` may be retried.

  As defined by the OTLP specification for OTLP/gRPC, an error is
  retryable if its code is `:cancelled`, `:deadline_exceeded`, `:aborted`,
  `:out_of_range`, `:unavailable`, or `:data_loss`, or if its code is
  `:resource_exhausted` and the server gave a retry delay.

  Use `Mint.OTLP.retryable?/1`, which also handles connection errors.
  """
  @impl Mint.OTLP
  def retryable?(%Error{transport: __MODULE__, code: code, retry_after: retry_after}) do
    code in [:cancelled, :deadline_exceeded, :aborted, :out_of_range, :unavailable, :data_loss] or
      (code == :resource_exhausted and retry_after != nil)
  end

  @impl Mint.OTLP
  def controlling_process(%__MODULE__{} = client, pid) do
    with {:ok, conn} <- HTTP2.controlling_process(client.conn, pid) do
      {:ok, %{client | conn: conn}}
    end
  end

  ## Sending

  defp send_pending(client) do
    {client, blocked, responses} =
      client.sending
      |> :queue.to_list()
      |> Enum.reduce({client, [], []}, fn ref, {client, blocked, responses} ->
        case send_body(client, ref) do
          {:sent, client, new} -> {client, blocked, new ++ responses}
          {:blocked, client} -> {client, [ref | blocked], responses}
        end
      end)

    {%{client | sending: :queue.from_list(Enum.reverse(blocked))}, Enum.reverse(responses)}
  end

  defp send_body(client, ref) do
    case client.requests do
      %{^ref => %{body: body, left: left}} ->
        conn = client.conn

        window =
          min(
            HTTP2.get_window_size(conn, :connection),
            HTTP2.get_window_size(conn, {:request, ref})
          )

        send_chunks(
          client,
          ref,
          body,
          left,
          window,
          HTTP2.get_server_setting(conn, :max_frame_size)
        )

      %{} ->
        # The server already answered or the request failed
        {:sent, client, []}
    end
  end

  defp send_chunks(client, ref, [], 0, _window, _max_frame) do
    case HTTP2.stream_request_body(client.conn, ref, :eof) do
      {:ok, conn} -> {:sent, put_body(%{client | conn: conn}, ref, [], 0), []}
      {:error, conn, error} -> {:sent, drop(%{client | conn: conn}, ref), [{:error, ref, error}]}
    end
  end

  defp send_chunks(client, ref, body, left, 0, _max_frame) do
    {:blocked, put_body(client, ref, body, left)}
  end

  defp send_chunks(client, ref, body, left, window, max_frame) do
    size = left |> min(window) |> min(max_frame)
    {chunk, rest} = split(body, size, [])

    case HTTP2.stream_request_body(client.conn, ref, chunk) do
      {:ok, conn} ->
        send_chunks(%{client | conn: conn}, ref, rest, left - size, window - size, max_frame)

      {:error, conn, error} ->
        {:sent, drop(%{client | conn: conn}, ref), [{:error, ref, error}]}
    end
  end

  defp put_body(client, ref, body, left) do
    update_in(client.requests[ref], &%{&1 | body: body, left: left})
  end

  defp drop(client, ref), do: %{client | requests: Map.delete(client.requests, ref)}

  # Splits a flat list of binaries after `size` bytes, without copying.
  defp split(rest, 0, acc), do: {Enum.reverse(acc), rest}

  defp split([binary | rest], size, acc) when byte_size(binary) <= size,
    do: split(rest, size - byte_size(binary), [binary | acc])

  defp split([binary | rest], size, acc) do
    head = binary_part(binary, 0, size)
    tail = binary_part(binary, size, byte_size(binary) - size)
    {Enum.reverse([head | acc]), [tail | rest]}
  end

  ## Responses

  defp handle_responses(client, responses) do
    {client, acc} = Enum.reduce(responses, {client, []}, &handle_response/2)
    {client, Enum.reverse(acc)}
  end

  defp handle_response({:status, ref, status}, {client, acc}) do
    {update_request(client, ref, &%{&1 | status: status}), acc}
  end

  # Both the headers and the trailers
  defp handle_response({:headers, ref, headers}, {client, acc}) do
    {update_request(client, ref, &%{&1 | headers: &1.headers ++ headers}), acc}
  end

  defp handle_response({:data, ref, data}, {client, acc}) do
    {update_request(client, ref, &%{&1 | data: [&1.data | data]}), acc}
  end

  defp handle_response({:done, ref}, {client, acc}) do
    case Map.pop(client.requests, ref) do
      {nil, _} -> {client, acc}
      {request, requests} -> {%{client | requests: requests}, [result(ref, request) | acc]}
    end
  end

  defp handle_response({:error, ref, error}, {client, acc}) do
    case Map.pop(client.requests, ref) do
      {nil, _} -> {client, acc}
      {_, requests} -> {%{client | requests: requests}, [{:error, ref, error} | acc]}
    end
  end

  defp handle_response(_other, {client, acc}), do: {client, acc}

  defp update_request(client, ref, fun) do
    case client.requests do
      %{^ref => request} -> %{client | requests: %{client.requests | ref => fun.(request)}}
      %{} -> client
    end
  end

  defp result(ref, %{status: 200} = request) do
    case List.keyfind(request.headers, "grpc-status", 0) do
      {_, "0"} ->
        case decode_response(IO.iodata_to_binary(request.data)) do
          {:ok, partial_success} -> {:ok, ref, partial_success}
          :error -> {:error, ref, error(:internal, "malformed response message")}
        end

      {_, status} ->
        code =
          case Integer.parse(status) do
            {status, ""} -> Map.get(@codes, status, :unknown)
            _ -> :unknown
          end

        message = grpc_message(request.headers)
        {:error, ref, error(code, message, retry_after(request.headers))}

      # As gRPC clients do, protocol errors are internal errors
      nil ->
        {:error, ref, error(:internal, "missing grpc-status header")}
    end
  end

  # As described in "HTTP to gRPC Status Code Mapping" from the gRPC docs
  defp result(ref, %{status: status}) do
    code =
      case status do
        400 -> :internal
        401 -> :unauthenticated
        403 -> :permission_denied
        404 -> :unimplemented
        status when status in [429, 502, 503, 504] -> :unavailable
        _ -> :unknown
      end

    {:error, ref, error(code, "HTTP status #{status}")}
  end

  defp error(code, message, retry_after \\ nil) do
    %Error{transport: __MODULE__, code: code, message: message, retry_after: retry_after}
  end

  defp grpc_message(headers) do
    case List.keyfind(headers, "grpc-message", 0) do
      {_, message} -> URI.decode(message)
      nil -> ""
    end
  end

  # The response is an Export*ServiceResponse whose partial_success
  # (field 1) has the number of rejected items (field 1) and an error
  # message (field 2), regardless of the signal.
  defp decode_response(<<>>), do: {:ok, %{rejected: 0, error_message: ""}}

  defp decode_response(<<0, size::32, message::binary-size(size)>>) do
    with {:ok, fields} <- Protobuf.decode(message) do
      case List.keyfind(fields, 1, 0) do
        {1, partial_success} when is_binary(partial_success) ->
          with {:ok, fields} <- Protobuf.decode(partial_success) do
            rejected = for({1, value} <- fields, do: value) |> List.last(0)
            error_message = for({2, value} <- fields, do: value) |> List.last("")
            {:ok, %{rejected: rejected, error_message: error_message}}
          end

        _ ->
          {:ok, %{rejected: 0, error_message: ""}}
      end
    end
  end

  defp decode_response(_data), do: :error

  # The retry delay comes as a google.rpc.RetryInfo in the details
  # (field 3) of the google.rpc.Status in grpc-status-details-bin.
  defp retry_after(headers) do
    with {_, details} <- List.keyfind(headers, "grpc-status-details-bin", 0),
         {:ok, status} <- Base.decode64(details, padding: false),
         {:ok, fields} <- Protobuf.decode(status) do
      Enum.find_value(fields, fn
        {3, any} when is_binary(any) -> retry_info(any)
        _ -> nil
      end)
    else
      _ -> nil
    end
  end

  defp retry_info(any) do
    with {:ok, fields} <- Protobuf.decode(any),
         {1, @retry_info} <- List.keyfind(fields, 1, 0),
         {2, retry_info} <- List.keyfind(fields, 2, 0),
         {:ok, fields} <- Protobuf.decode(retry_info),
         {1, duration} <- List.keyfind(fields, 1, 0),
         {:ok, fields} <- Protobuf.decode(duration) do
      seconds = for({1, value} <- fields, do: value) |> List.last(0)
      nanos = for({2, value} <- fields, do: value) |> List.last(0)
      seconds * 1000 + div(nanos, 1_000_000)
    else
      _ -> nil
    end
  end
end
