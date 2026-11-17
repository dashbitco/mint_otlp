defmodule Mint.OTLP do
  @moduledoc """
  Processless client for the OpenTelemetry Protocol (OTLP).

  This module exports traces, metrics, and logs to an OTLP endpoint,
  such as the OpenTelemetry Collector. Similar to `Mint.HTTP`, it does
  not spawn processes: a client is a data structure that wraps a
  socket, and the messages the socket sends to the owner process are
  given to `stream/2`.

  `Mint.OTLP` does not encode telemetry. `export/4` takes a request,
  such as an `ExportTraceServiceRequest`, already encoded as protobuf.

  ## Transports

  The transport is chosen with the `:transport` option to `connect/4`:

    * `:grpc` - OTLP/gRPC, see `Mint.OTLP.GRPC`

  ## Usage

      {:ok, client} = Mint.OTLP.connect(:http, "localhost", 4317)
      {:ok, client, ref} = Mint.OTLP.export(client, :traces, request)

      receive do
        message ->
          {:ok, client, responses} = Mint.OTLP.stream(client, message)
      end

  The request body is sent as the HTTP/2 flow control windows allow,
  so large requests are also sent while calling `stream/2`. Each
  export eventually results in one of the following responses:

    * `{:ok, ref, %{rejected: rejected, error_message: message}}` - the
      export succeeded. If `rejected` is not zero, the server rejected
      that many spans, data points, or log records, as described by
      `message`, and they must not be sent again.

    * `{:error, ref, reason}` - the export failed. See `retryable?/1`.

  """

  alias Mint.OTLP.GRPC

  @typedoc """
  A client, as returned by `connect/4`.
  """
  @type t() :: GRPC.t()

  @typedoc """
  The kind of telemetry.
  """
  @type signal() :: :traces | :metrics | :logs

  @typedoc """
  An error.
  """
  @type error() :: Mint.OTLP.Error.t() | Mint.Types.error()

  @typedoc """
  A response to an export.
  """
  @type response() ::
          {:ok, reference(), %{rejected: non_neg_integer(), error_message: String.t()}}
          | {:error, reference(), error()}

  @callback connect(Mint.Types.scheme(), Mint.Types.address(), :inet.port_number(), keyword()) ::
              {:ok, t()} | {:error, Mint.Types.error()}

  @callback export(t(), signal(), iodata(), keyword()) ::
              {:ok, t(), reference()} | {:error, t(), error()}

  @callback stream(t(), term()) ::
              {:ok, t(), [response()]}
              | {:error, t(), Mint.Types.error(), [response()]}
              | :unknown

  @callback close(t()) :: {:ok, t()}

  @callback open?(t()) :: boolean()

  @callback open_request_count(t()) :: non_neg_integer()

  @callback controlling_process(t(), pid()) :: {:ok, t()} | {:error, Mint.Types.error()}

  @callback retryable?(Mint.OTLP.Error.t()) :: boolean()

  @doc """
  Connects to an OTLP endpoint.

  ## Options

    * `:transport` - the OTLP transport. Only `:grpc` is supported.

  Other options are given to the transport. See `Mint.OTLP.GRPC.connect/4`.
  """
  @spec connect(Mint.Types.scheme(), Mint.Types.address(), :inet.port_number(), keyword()) ::
          {:ok, t()} | {:error, Mint.Types.error()}
  def connect(scheme, address, port, opts \\ []) do
    {transport, opts} = Keyword.pop(opts, :transport, :grpc)

    case transport do
      :grpc -> GRPC.connect(scheme, address, port, opts)
      other -> raise ArgumentError, "unknown OTLP transport: #{inspect(other)}"
    end
  end

  @doc """
  Exports an encoded request for the given signal.

  `request` is an `ExportTraceServiceRequest`, `ExportMetricsServiceRequest`,
  or `ExportLogsServiceRequest` encoded as protobuf, according to `signal`.

  The request body is sent as the HTTP/2 flow control windows allow,
  with what is left sent as `stream/2` receives window updates. Binaries
  in `request` are not copied, except for small ones.

  ## Options

    * `:size` - the size of `request`, if already known, to avoid
      computing it

    * `:headers` - headers for this export, in addition to the ones
      given to `connect/4`

    * `:timeout` - a timeout in milliseconds the server should respect

  """
  @spec export(t(), signal(), iodata(), keyword()) ::
          {:ok, t(), reference()} | {:error, t(), error()}
  def export(%module{} = client, signal, request, opts \\ [])
      when signal in [:traces, :metrics, :logs] do
    module.export(client, signal, request, opts)
  end

  @doc """
  Handles a message received by the process that owns the client.

  Returns `:unknown` if the message is not meant for this client.
  """
  @spec stream(t(), term()) ::
          {:ok, t(), [response()]} | {:error, t(), Mint.Types.error(), [response()]} | :unknown
  def stream(%module{} = client, message), do: module.stream(client, message)

  @doc """
  Closes the client.
  """
  @spec close(t()) :: {:ok, t()}
  def close(%module{} = client), do: module.close(client)

  @doc """
  Returns whether the client is open.
  """
  @spec open?(t()) :: boolean()
  def open?(%module{} = client), do: module.open?(client)

  @doc """
  Returns the number of exports waiting for a response.
  """
  @spec open_request_count(t()) :: non_neg_integer()
  def open_request_count(%module{} = client), do: module.open_request_count(client)

  @doc """
  Changes the process that owns the client's socket.
  """
  @spec controlling_process(t(), pid()) :: {:ok, t()} | {:error, Mint.Types.error()}
  def controlling_process(%module{} = client, pid), do: module.controlling_process(client, pid)

  @doc """
  Returns whether an export that failed with `error` may be retried.

  This follows the OTLP specification, which defines which errors are
  retryable for each transport. For `Mint.OTLP.Error`, this function
  delegates to its transport, see `Mint.OTLP.GRPC.retryable?/1`.

  Errors from the connection, such as when the connection fails or is
  closed, are retryable on a new connection.

  When the server gives a retry delay, it must be respected.
  """
  @spec retryable?(error()) :: boolean()
  def retryable?(%Mint.OTLP.Error{transport: transport} = error), do: transport.retryable?(error)

  def retryable?(%Mint.TransportError{}), do: true

  def retryable?(%Mint.HTTPError{reason: reason}) do
    case reason do
      :closed -> true
      :closed_for_writing -> true
      :too_many_concurrent_requests -> true
      {:server_closed_request, :refused_stream} -> true
      {:server_closed_connection, _, _} -> true
      _ -> false
    end
  end
end
