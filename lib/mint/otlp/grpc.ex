defmodule Mint.OTLP.GRPC do
  # OTLP/gRPC transport. Each export is a unary gRPC call to the `Export`
  # method of the signal's service, over a single `Mint.HTTP2` connection.
  @moduledoc false

  @behaviour Mint.OTLP

  require Logger

  alias Mint.HTTP2
  alias Mint.OTLP.{Decoder, Error}

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

  @retryable_codes [
    :cancelled,
    :deadline_exceeded,
    :aborted,
    :out_of_range,
    :unavailable,
    :data_loss
  ]

  @retry_defaults [initial_backoff: 5_000, max_backoff: 30_000, max_elapsed_time: 300_000]

  # Large writes block the owner and may exceed the socket send timeout
  @max_write 128 * 1024

  @opaque t() :: %__MODULE__{}

  # Each export is either :queued, in `queue`, or {:in_flight, request},
  # sent or being sent as `request.stream`, which is indexed in `streams`.
  # Exports leave `exports` only in finish/4, which returns their response.
  #
  # Retryable failures are about the server or the connection, rather
  # than a particular export, so they make the whole client back off.
  #
  # A single timer wakes the client at the earliest of `backoff_until`,
  # the deadlines of requests in flight, and when queued exports expire.
  defstruct [
    :id,
    :scheme,
    :address,
    :port,
    :connect_opts,
    :headers,
    :timeout,
    :backoff,
    # nil if retries are disabled
    :max_elapsed_time,
    conn: nil,
    exports: %{},
    streams: %{},
    queue: :queue.new(),
    # Whether a message to run the client was sent
    run?: false,
    # Failures since the last success, and until when to back off
    failures: 0,
    backoff_until: nil,
    # The last failure since the last success, for logs and for exports
    # that expire in the queue
    last_error: nil,
    # When the server last sent anything on the connection
    received_at: nil,
    timer: nil
  ]

  @impl Mint.OTLP
  def new(scheme, address, port, opts) do
    {user_agent, opts} = Keyword.pop!(opts, :user_agent)
    {headers, opts} = Keyword.pop(opts, :headers, [])
    {timeout, opts} = Keyword.pop(opts, :timeout, 10_000)
    {retry, opts} = Keyword.pop(opts, :retry, true)

    if not (is_integer(timeout) and timeout > 0) do
      raise ArgumentError, ":timeout must be a positive integer, got: #{inspect(timeout)}"
    end

    options = Map.new(Keyword.merge(@retry_defaults, if(is_list(retry), do: retry, else: [])))

    # Writes to a server that stopped reading would block forever
    transport_opts =
      Keyword.merge(
        [send_timeout: timeout, send_timeout_close: true],
        Keyword.get(opts, :transport_opts, [])
      )

    %__MODULE__{
      id: make_ref(),
      scheme: scheme,
      address: address,
      port: port,
      connect_opts: Keyword.merge(opts, mode: :active, transport_opts: transport_opts),
      headers: [
        {"content-type", "application/grpc"},
        {"te", "trailers"},
        {"user-agent", user_agent},
        {"grpc-timeout", grpc_timeout(timeout)} | headers
      ],
      timeout: timeout,
      backoff: Map.take(options, [:initial_backoff, :max_backoff]),
      max_elapsed_time: if(retry, do: options.max_elapsed_time)
    }
  end

  # grpc-timeout allows at most 8 digits
  defp grpc_timeout(timeout) when timeout < 100_000_000, do: "#{timeout}m"
  defp grpc_timeout(timeout), do: "#{min(div(timeout, 1000), 99_999_999)}S"

  # Exports only enqueue and send a message to run the client, once
  # per burst of exports. All work happens in stream/2, so every
  # response comes from stream/2 and export/4 never blocks.
  @impl Mint.OTLP
  def export(%__MODULE__{} = client, signal, request, opts) do
    ref = make_ref()
    size = Keyword.get_lazy(opts, :size, fn -> IO.iodata_length(request) end)

    export = %{
      path: Map.fetch!(@paths, signal),
      headers: client.headers ++ Keyword.get(opts, :headers, []),
      body: [<<0, size::32>> | request],
      frame_size: size + 5,
      expires_at: now() + (client.max_elapsed_time || client.timeout),
      state: :queued
    }

    client = %{
      client
      | exports: Map.put(client.exports, ref, export),
        queue: :queue.in(ref, client.queue)
    }

    if not client.run?, do: send(self(), {__MODULE__, client.id, :run})
    {:ok, %{client | run?: true}, ref}
  end

  @impl Mint.OTLP
  def stream(%__MODULE__{id: id} = client, {:timeout, timer, {__MODULE__, id, :wake}}) do
    # Cancelled timers may have been sent already
    {client, acc} =
      if timer == client.timer,
        do: expire(%{client | timer: nil}, []),
        else: {client, []}

    {client, acc} = step(client, acc, nil)
    {:responses, client, Enum.reverse(acc)}
  end

  # Running when there is nothing to do is harmless, so a message
  # sent before close/1 needs no special handling.
  def stream(%__MODULE__{id: id} = client, {__MODULE__, id, :run}) do
    {client, acc} = step(%{client | run?: false}, [], nil)
    {:responses, client, Enum.reverse(acc)}
  end

  def stream(%__MODULE__{conn: nil}, _message), do: :unknown

  def stream(%__MODULE__{} = client, message) do
    case HTTP2.stream(client.conn, message) do
      {:ok, conn, responses} -> handle_responses(client, conn, responses, nil)
      {:error, conn, error, responses} -> handle_responses(client, conn, responses, error)
      :unknown -> :unknown
    end
  end

  defp handle_responses(client, conn, responses, error) do
    client = %{client | conn: conn, received_at: now()}
    {client, acc} = Enum.reduce(responses, {client, []}, &handle_response/2)
    {client, acc} = step(client, acc, error)
    {:responses, client, Enum.reverse(acc)}
  end

  @impl Mint.OTLP
  def close(%__MODULE__{} = client) do
    cancel_timer(client.timer)
    if client.conn, do: close_connection(client)

    error = %Mint.TransportError{reason: :closed}
    responses = for ref <- Map.keys(client.exports), do: {:error, ref, error}

    client = %{
      client
      | conn: nil,
        exports: %{},
        streams: %{},
        queue: :queue.new(),
        failures: 0,
        backoff_until: nil,
        last_error: nil,
        timer: nil
    }

    {:responses, client, responses}
  end

  ## Steps

  # Runs after every event. Sending only happens while the connection
  # is writable and the connection is only dropped at the end, or when
  # found dead before the step, so the steps never act on a dropped
  # connection. Responses are accumulated in reverse order.
  defp step(client, acc, error) do
    client = connect(client)

    # On errors, the connection is dropped below
    {client, acc} =
      if error do
        {client, acc}
      else
        {client, acc} = send_bodies(client, acc)
        start_queued(client, acc)
      end

    {client, acc} = check_connection(client, acc, error)
    {schedule(client), acc}
  end

  # Exports wait in the queue while it fails, until they expire.
  defp connect(client) do
    if client.conn == nil and client.backoff_until == nil and
         not :queue.is_empty(client.queue) do
      case HTTP2.connect(client.scheme, client.address, client.port, client.connect_opts) do
        {:ok, conn} ->
          %{client | conn: conn, received_at: now()}

        {:error, error} ->
          back_off(client, error)
      end
    else
      client
    end
  end

  defp check_connection(%{conn: nil} = client, acc, _error), do: {client, acc}

  # Mint may stop reading from the socket after an error, so we drop
  # the connection, even if it is still open.
  defp check_connection(client, acc, error) when error != nil do
    drop_connection(client, error, acc)
  end

  defp check_connection(client, acc, nil) do
    error = %Mint.TransportError{reason: :closed}

    cond do
      HTTP2.open?(client.conn, :write) ->
        {client, acc}

      # After a GOAWAY, wait for the responses the server may still send,
      # but requests still sending their body cannot finish.
      HTTP2.open?(client.conn, :read) ->
        {client, acc} =
          Enum.reduce(client.streams, {client, acc}, fn {_stream, ref}, {client, acc} ->
            case client.exports[ref].state do
              {:in_flight, %{left: left}} when left > 0 -> retry(client, ref, error, acc)
              {:in_flight, _request} -> {client, acc}
            end
          end)

        if client.streams == %{}, do: drop_connection(client, error, acc), else: {client, acc}

      true ->
        drop_connection(client, error, acc)
    end
  end

  # The only place connections are dropped. Exports in flight are sent
  # again on a new connection, regardless of the error that dropped it,
  # such as a protocol error, as they got no response.
  defp drop_connection(client, error, acc) do
    close_connection(client)
    client = %{client | conn: nil}

    if client.streams != %{} do
      exports = map_size(client.streams)

      Logger.info(
        "OTLP connection closed with #{exports} exports in flight: " <> Exception.message(error)
      )
    end

    {client, acc} =
      Enum.reduce(client.streams, {client, acc}, fn {_stream, ref}, {client, acc} ->
        retry(client, ref, error, acc)
      end)

    # Servers routinely close connections, so the first time we
    # reconnect right away, when the timer fires. A success does not end
    # a backoff, so we may still be backing off.
    if client.failures == 0 do
      backoff_until = client.backoff_until || now()
      {%{client | failures: 1, backoff_until: backoff_until, last_error: error}, acc}
    else
      {back_off(client, error), acc}
    end
  end

  # Closing the socket is synchronous and messages from the socket
  # arrive in order, so once it is closed, all of its messages are
  # already in the mailbox and we remove them, so they are not given
  # to stream/2 as unknown messages.
  defp close_connection(client) do
    socket = HTTP2.get_socket(client.conn)
    {:ok, _conn} = HTTP2.close(client.conn)

    # Mint does not close the socket if it already considers it closed
    if client.scheme == :https, do: :ssl.close(socket), else: :gen_tcp.close(socket)
    flush_socket(socket)
  end

  defp flush_socket(socket) do
    receive do
      {tag, ^socket, _} when tag in [:tcp, :ssl, :tcp_error, :ssl_error] -> flush_socket(socket)
      {tag, ^socket} when tag in [:tcp_closed, :ssl_closed] -> flush_socket(socket)
    after
      0 -> :ok
    end
  end

  ## Timers

  # Arms the timer for the earliest time the client has something to
  # do. `nil` sorts after integers, so it means never.
  defp schedule(client) do
    at =
      Enum.reduce(client.exports, client.backoff_until, fn
        {_ref, %{state: :queued, expires_at: expires_at}}, at ->
          min(expires_at, at)

        {_ref, %{state: {:in_flight, request}}}, at ->
          min(request.started_at + client.timeout, at)
      end)

    # A cancelled timer may have been sent already, which stream/2
    # ignores, as it is no longer the current one. The timer is sent to
    # the calling process, which must be the one that owns the socket.
    cancel_timer(client.timer)
    timer = at && :erlang.start_timer(max(at - now(), 0), self(), {__MODULE__, client.id, :wake})
    %{client | timer: timer}
  end

  defp cancel_timer(nil), do: :ok
  defp cancel_timer(timer), do: :erlang.cancel_timer(timer, async: true, info: false)

  # Handles everything that is due.
  defp expire(client, acc) do
    now = now()
    client = if client.backoff_until <= now, do: %{client | backoff_until: nil}, else: client

    late =
      for {stream, ref} <- client.streams,
          {:in_flight, request} = client.exports[ref].state,
          now - request.started_at >= client.timeout,
          do: {stream, ref}

    {client, acc} =
      cond do
        late == [] ->
          {client, acc}

        # The server sent nothing while we waited, so the connection is
        # dead, or the server is too slow, so we also back off
        now - client.received_at >= client.timeout ->
          error = %Mint.TransportError{reason: :timeout}
          client |> back_off(error) |> drop_connection(error, acc)

        true ->
          error = error(:deadline_exceeded, "no response after #{client.timeout}ms")

          Enum.reduce(late, {client, acc}, fn {stream, ref}, {client, acc} ->
            client |> cancel_stream(stream) |> failed(ref, error, acc)
          end)
      end

    expired =
      for ref <- :queue.to_list(client.queue), client.exports[ref].expires_at <= now, do: ref

    # The last failure since the last success is likely why they could not be sent
    error = client.last_error || error(:deadline_exceeded, "could not be sent in time")
    finish_all(client, expired, acc, &discard(&1, &2, error, &3))
  end

  ## Export states

  # Moves an export to a new state, keeping `streams` and `queue` in sync.
  defp set_state(client, ref, state) do
    client = leave_state(client, ref)

    client =
      case state do
        :queued -> %{client | queue: :queue.in(ref, client.queue)}
        {:in_flight, request} -> %{client | streams: Map.put(client.streams, request.stream, ref)}
      end

    update_in(client.exports[ref], &%{&1 | state: state})
  end

  defp leave_state(client, ref) do
    case client.exports[ref].state do
      :queued -> %{client | queue: :queue.delete(ref, client.queue)}
      {:in_flight, %{stream: stream}} -> %{client | streams: Map.delete(client.streams, stream)}
    end
  end

  # The only place exports are removed and responses returned.
  defp finish(client, ref, response, acc) do
    client = leave_state(client, ref)
    {%{client | exports: Map.delete(client.exports, ref)}, [response | acc]}
  end

  # Takes the exports out of the queue at once, instead of searching it for each.
  defp finish_all(client, refs, acc, fun) do
    set = MapSet.new(refs)
    queue = :queue.filter(&(not MapSet.member?(set, &1)), client.queue)

    {client, acc} =
      Enum.reduce(refs, {%{client | queue: :queue.new()}, acc}, fn ref, {client, acc} ->
        fun.(client, ref, acc)
      end)

    {%{client | queue: queue}, acc}
  end

  # The client gave up on an export.
  defp discard(client, ref, error, acc) do
    Logger.error("Discarding OTLP export: " <> Exception.message(error))
    finish(client, ref, {:error, ref, error}, acc)
  end

  ## Retries

  # An attempt failed. Requests left unprocessed by a GOAWAY, or that
  # failed because of the connection, are retried right away, as the
  # connection is dropped and reconnects with its own backoff. Retryable
  # server responses are retried after the whole client backs off.
  defp failed(client, ref, error, acc) do
    case classify(error) do
      :requeue ->
        retry(client, ref, error, acc)

      :back_off ->
        {client, acc} = retry(client, ref, error, acc)
        {back_off(client, error), acc}

      :fail ->
        discard(client, ref, error, acc)
    end
  end

  # Puts the export back in the queue, unless it cannot be retried.
  defp retry(client, ref, error, acc) do
    if client.max_elapsed_time != nil and now() < client.exports[ref].expires_at do
      {set_state(client, ref, :queued), acc}
    else
      discard(client, ref, error, acc)
    end
  end

  # Exponential backoff with a multiplier of 1.5 and +-50% jitter,
  # respecting the delay given by the server. While already backing
  # off, failures only extend it to the server delay.
  defp back_off(client, error) do
    now = now()
    retry_after = retry_after(error)

    if client.backoff_until != nil and client.backoff_until > now do
      %{client | backoff_until: max(client.backoff_until, now + retry_after), last_error: error}
    else
      %{initial_backoff: initial, max_backoff: max} = client.backoff
      delay = min(max, initial * :math.pow(1.5, min(client.failures, 32)))
      delay = max(min(max, round(delay * (0.5 + :rand.uniform()))), retry_after)
      Logger.info("Backing off OTLP exports for #{delay}ms after: " <> Exception.message(error))

      %{
        client
        | failures: client.failures + 1,
          backoff_until: now + delay,
          last_error: error
      }
    end
  end

  defp classify(%Error{code: code, retry_after: retry_after}) do
    if code in @retryable_codes or (code == :resource_exhausted and retry_after != nil),
      do: :back_off,
      else: :fail
  end

  # The server did not process requests refused with REFUSED_STREAM, but
  # they would be sent again on the same connection, so we back off.
  # CANCEL maps to CANCELLED in gRPC.
  defp classify(%Mint.HTTPError{reason: {:server_closed_request, :refused_stream}}), do: :back_off
  defp classify(%Mint.HTTPError{reason: {:server_closed_request, :cancel}}), do: :back_off
  defp classify(error), do: if(connection_error?(error), do: :requeue, else: :fail)

  defp connection_error?(%Mint.TransportError{}), do: true
  defp connection_error?(%Mint.HTTPError{reason: :unprocessed}), do: true
  defp connection_error?(_error), do: false

  defp retry_after(%Error{retry_after: retry_after}) when retry_after != nil, do: retry_after
  defp retry_after(_error), do: 0

  defp now, do: System.monotonic_time(:millisecond)

  ## Sending

  # Mint.HTTP2 keeps track of the HTTP/2 flow control windows but never
  # waits on them. A request that fits the windows is sent at once.
  # Otherwise, we send as much of the body as the windows allow, which
  # Mint.HTTP2 splits into frames, and the rest as WINDOW_UPDATE frames
  # arrive, from send_bodies/2.

  # Starts queued exports while not backing off and the connection is
  # writable and has room for more streams.
  defp start_queued(client, acc) do
    with nil <- client.backoff_until,
         %{} = conn <- client.conn,
         true <- HTTP2.open?(conn, :write),
         true <- HTTP2.open_request_count(conn) < server_setting(conn, :max_concurrent_streams),
         {:value, ref} <- :queue.peek(client.queue) do
      {client, acc} = start_request(client, ref, acc)
      start_queued(client, acc)
    else
      _ -> {client, acc}
    end
  end

  defp start_request(client, ref, acc) do
    %{path: path, headers: headers, body: body, frame_size: frame_size} = client.exports[ref]
    conn = client.conn

    # The request fits the windows, so it is sent at once
    fits? =
      frame_size <= @max_write and
        frame_size <= HTTP2.get_window_size(conn, :connection) and
        frame_size <= server_setting(conn, :initial_window_size)

    case HTTP2.request(conn, "POST", path, headers, if(fits?, do: body, else: :stream)) do
      {:ok, conn, stream} ->
        request = %{
          stream: stream,
          started_at: now(),
          rest: if(fits?, do: [], else: body),
          left: if(fits?, do: 0, else: frame_size),
          status: nil,
          response_headers: [],
          data: []
        }

        client = set_state(%{client | conn: conn}, ref, {:in_flight, request})
        if fits?, do: {client, acc}, else: send_body(client, ref, acc)

      {:error, conn, error} ->
        send_failed(%{client | conn: conn}, ref, error, acc)
    end
  end

  defp send_failed(client, ref, error, acc) do
    {client, acc} = failed(client, ref, error, acc)
    {close_on_connection_error(client, error), acc}
  end

  defp cancel_stream(client, stream) do
    case HTTP2.cancel_request(client.conn, stream) do
      {:ok, conn} -> %{client | conn: conn}
      {:error, conn, error} -> close_on_connection_error(%{client | conn: conn}, error)
    end
  end

  # Mint keeps the connection open when writing to the socket fails,
  # so we close it, and it is dropped at the end of the step.
  defp close_on_connection_error(client, error) do
    if connection_error?(error) do
      {:ok, conn} = HTTP2.close(client.conn)
      %{client | conn: conn}
    else
      client
    end
  end

  defp send_bodies(client, acc) do
    Enum.reduce(client.streams, {client, acc}, fn {_stream, ref}, {client, acc} ->
      send_body(client, ref, acc)
    end)
  end

  # Sends as much of the body as the windows allow, in writes of at most
  # @max_write, finishing the body or exhausting the windows. Windows may
  # be negative when the server lowers its initial window.
  defp send_body(client, ref, acc) do
    with %{^ref => %{state: {:in_flight, %{left: left} = request}}} when left > 0 <-
           client.exports,
         true <- HTTP2.open?(client.conn, :write),
         %{stream: stream, rest: rest} = request,
         window = HTTP2.request_body_window(client.conn, stream),
         size when size > 0 <- left |> min(window) |> min(@max_write) |> max(0) do
      {data, rest} = split(rest, size, [])

      with {:ok, conn} <- HTTP2.stream_request_body(client.conn, stream, data),
           {:ok, conn} <- maybe_eof(conn, stream, left - size) do
        request = %{request | rest: rest, left: left - size}
        client = %{client | conn: conn}
        client = update_in(client.exports[ref], &%{&1 | state: {:in_flight, request}})
        send_body(client, ref, acc)
      else
        {:error, conn, error} ->
          client = cancel_stream(%{client | conn: conn}, stream)
          send_failed(client, ref, error, acc)
      end
    else
      _ -> {client, acc}
    end
  end

  defp maybe_eof(conn, stream, 0), do: HTTP2.stream_request_body(conn, stream, :eof)
  defp maybe_eof(conn, _stream, _left), do: {:ok, conn}

  defp server_setting(conn, name), do: HTTP2.get_server_setting(conn, name)

  # Splits iodata after `size` bytes. While the head of the list is a
  # binary, we split it into sub-binaries to avoid copying the payload.
  # Anything else is converted to an iovec, so it happens at most once.
  defp split(rest, 0, acc), do: {Enum.reverse(acc), rest}

  # Only if :size was larger than the request
  defp split([], _size, acc), do: {Enum.reverse(acc), []}

  defp split([binary | rest], size, acc) when is_binary(binary) and byte_size(binary) <= size,
    do: split(rest, size - byte_size(binary), [binary | acc])

  defp split([binary | rest], size, acc) when is_binary(binary) do
    {head, tail} = :erlang.split_binary(binary, size)
    {Enum.reverse([head | acc]), [tail | rest]}
  end

  defp split(iodata, size, acc), do: split(:erlang.iolist_to_iovec(iodata), size, acc)

  ## Responses

  # Streams no longer in `streams` were abandoned, so we ignore them.
  defp handle_response({:done, stream}, {client, acc}) do
    case client.streams do
      %{^stream => ref} ->
        {:in_flight, request} = client.exports[ref].state

        case result(request) do
          {:ok, info} ->
            client = %{client | failures: 0, last_error: nil}
            finish(client, ref, {:done, ref, info}, acc)

          {:error, error} ->
            failed(client, ref, error, acc)
        end

      %{} ->
        {client, acc}
    end
  end

  defp handle_response({:error, stream, error}, {client, acc}) do
    case client.streams do
      %{^stream => ref} -> failed(client, ref, error, acc)
      %{} -> {client, acc}
    end
  end

  defp handle_response({tag, stream, value}, {client, acc})
       when tag in [:status, :headers, :data] do
    case client.streams do
      %{^stream => ref} ->
        {:in_flight, request} = client.exports[ref].state
        request = put_response(request, tag, value)
        {update_in(client.exports[ref], &%{&1 | state: {:in_flight, request}}), acc}

      %{} ->
        {client, acc}
    end
  end

  defp handle_response(_other, {client, acc}), do: {client, acc}

  defp put_response(request, :status, status), do: %{request | status: status}
  defp put_response(request, :data, data), do: %{request | data: [request.data | data]}

  # Both the headers and the trailers
  defp put_response(request, :headers, headers),
    do: %{request | response_headers: request.response_headers ++ headers}

  defp result(request) do
    case List.keyfind(request.response_headers, "grpc-status", 0) do
      {_, "0"} when request.status == 200 ->
        {:ok, decode_response(request.data)}

      {_, status} when status != "0" ->
        code =
          case Integer.parse(status) do
            {status, ""} -> Map.get(@codes, status, :unknown)
            _ -> :unknown
          end

        message = grpc_message(request.response_headers)
        {:error, error(code, message, retry_after_header(request.response_headers))}

      # As gRPC clients do, protocol errors are internal errors
      nil when request.status == 200 ->
        {:error, error(:internal, "missing grpc-status header")}

      _ ->
        http_result(request.status)
    end
  end

  # As described in "HTTP to gRPC Status Code Mapping" from the gRPC docs
  defp http_result(status) do
    code =
      case status do
        400 -> :internal
        401 -> :unauthenticated
        403 -> :permission_denied
        404 -> :unimplemented
        status when status in [429, 502, 503, 504] -> :unavailable
        _ -> :unknown
      end

    {:error, error(code, "HTTP status #{status}")}
  end

  defp error(code, message, retry_after \\ nil) do
    %Error{transport: :grpc, code: code, message: message, retry_after: retry_after}
  end

  defp grpc_message(headers) do
    case List.keyfind(headers, "grpc-message", 0) do
      {_, message} ->
        URI.decode(message)

      nil ->
        ""
    end
  end

  # The response is a single message, which may be omitted when empty.
  defp decode_response(data) do
    case IO.iodata_to_binary(data) do
      <<>> -> Decoder.decode_export_response(<<>>)
      <<0, size::32, message::binary-size(size)>> -> Decoder.decode_export_response(message)
    end
  end

  # The retry delay comes in the google.rpc.Status of grpc-status-details-bin.
  defp retry_after_header(headers) do
    case List.keyfind(headers, "grpc-status-details-bin", 0) do
      {_, details} -> Decoder.decode_retry_after(Base.decode64!(details, padding: false))
      nil -> nil
    end
  end
end
