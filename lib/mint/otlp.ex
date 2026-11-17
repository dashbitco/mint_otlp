defmodule Mint.OTLP do
  @moduledoc """
  Processless client for the OpenTelemetry Protocol (OTLP).

  This module exports traces, metrics, and logs to an OTLP endpoint,
  such as the OpenTelemetry Collector. Similar to `Mint.HTTP`, it does
  not spawn processes: a client is a data structure owned by a process,
  which gives the messages it receives to `stream/2`.

  `Mint.OTLP` does not encode telemetry. `export/4` takes a request,
  such as an `ExportTraceServiceRequest`, already encoded as protobuf.
  On the other hand, it takes care of connecting, retrying, and
  reconnecting, as defined by the OTLP specification and configured
  when creating the client.

  ## Transports

  The transport is chosen with the `:transport` option to `new/4`:

    * `:grpc` - OTLP/gRPC, see `Mint.OTLP.GRPC`

  ## Usage

      client = Mint.OTLP.new(:http, "localhost", 4317)
      {:ok, client, ref} = Mint.OTLP.export(client, :traces, request)

      # In a loop, until the response arrives
      receive do
        message ->
          {:responses, client, responses} = Mint.OTLP.stream(client, message)
      end

  `stream/2` and `close/1` return the responses to the exports that
  finished, if any. Each export gets exactly one response, as one of:

    * `{:done, ref, %{rejected: rejected, error_message: message}}` - the
      export succeeded. If `rejected` is not zero, the server rejected
      that many spans, data points, or log records, as described by
      `message`, and they must not be sent again.

    * `{:error, ref, reason}` - the export failed and will not be retried,
      either because the error is not retryable or because retries were
      exhausted. Exports that could not be sent at all fail with the
      last error of the client, such as a connection error.
  """

  alias Mint.OTLP.GRPC

  @typedoc """
  A client, as returned by `new/4`.
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
          {:done, reference(), %{rejected: non_neg_integer(), error_message: String.t()}}
          | {:error, reference(), error()}

  @callback new(Mint.Types.scheme(), Mint.Types.address(), :inet.port_number(), keyword()) :: t()

  @callback export(t(), signal(), iodata(), keyword()) :: {:ok, t(), reference()}

  @callback stream(t(), term()) :: {:responses, t(), [response()]} | :unknown

  @callback close(t()) :: {:responses, t(), [response()]}

  @doc """
  Creates a client for an OTLP endpoint.

  It does not connect, the connection is established on the first export.

  ## Options

    * `:transport` - the OTLP transport. Only `:grpc` is supported.

  Other options are given to the transport. See `Mint.OTLP.GRPC`.
  """
  @spec new(Mint.Types.scheme(), Mint.Types.address(), :inet.port_number(), keyword()) :: t()
  def new(scheme, address, port, opts \\ []) do
    {transport, opts} = Keyword.pop(opts, :transport, :grpc)

    case transport do
      :grpc -> GRPC.new(scheme, address, port, opts)
      other -> raise ArgumentError, "unknown OTLP transport: #{inspect(other)}"
    end
  end

  @doc """
  Exports an encoded request for the given signal.

  `request` is an `ExportTraceServiceRequest`, `ExportMetricsServiceRequest`,
  or `ExportLogsServiceRequest` encoded as protobuf, according to `signal`.

  `{:ok, client, ref}` means the export started, not that the request was
  sent. Its outcome always arrives later as a response from `stream/2`
  or `close/1`, even if it fails right away. Until then, the request is kept by the
  client, so it can be retried.

  The request body is sent as the HTTP/2 flow control windows allow,
  with what is left sent as `stream/2` receives window updates. Binaries
  in `request` are not copied, except for small ones.

  ## Options

    * `:size` - the size of `request`, if already known, to avoid
      computing it

    * `:headers` - headers for this export, in addition to the ones
      given to `new/4`

  """
  @spec export(t(), signal(), iodata(), keyword()) :: {:ok, t(), reference()}
  def export(%module{} = client, signal, request, opts \\ [])
      when signal in [:traces, :metrics, :logs] do
    module.export(client, signal, request, opts)
  end

  @doc """
  Handles a message received by the process that owns the client.

  Returns `{:responses, client, responses}`, where `responses` are the
  exports that finished, if any, or `:unknown` if the message is not
  meant for this client.
  """
  @spec stream(t(), term()) :: {:responses, t(), [response()]} | :unknown
  def stream(%module{} = client, message), do: module.stream(client, message)

  @doc """
  Closes the connection and cancels all pending exports.

  Returns an error response for each pending export. The client may
  still be used, in which case it connects again.
  """
  @spec close(t()) :: {:responses, t(), [response()]}
  def close(%module{} = client), do: module.close(client)
end
