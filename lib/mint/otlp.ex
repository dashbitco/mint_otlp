defmodule Mint.OTLP do
  @moduledoc """
  Processless client for the OpenTelemetry Protocol (OTLP).

  This module exports traces, metrics, and logs to an OTLP endpoint,
  such as the OpenTelemetry Collector. Similar to `Mint.HTTP`, it does
  not spawn processes: a client is a data structure owned by a process,
  which gives the messages it receives to `stream/2`.

  It takes care of connecting, retrying, and reconnecting, as defined
  by the OTLP specification and configured when creating the client.

  `Mint.OTLP` does not encode telemetry. `export/4` takes a request,
  such as an `ExportTraceServiceRequest`, already encoded as protobuf.

  ## Transports

  The transport is chosen with the `:transport` option to `new/4`:

    * `:grpc` - OTLP/gRPC

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

  ## Retries

  As defined by the OTLP specification, failed exports are retried
  when the server returns `CANCELLED`, `DEADLINE_EXCEEDED`, `ABORTED`,
  `OUT_OF_RANGE`, `UNAVAILABLE`, or `DATA_LOSS`, or `RESOURCE_EXHAUSTED`
  with a retry delay, and when an attempt times out. These failures are
  about the server, so the whole client backs off: no export starts and
  no connection is attempted until the backoff ends. Backoff is
  exponential with jitter, respects the delay given by the server, if
  any, and resets once an export succeeds.

  Exports that do not succeed within `:max_elapsed_time` fail. If they
  are waiting to be sent, they fail with the last error the client got
  since an export last succeeded, or with `DEADLINE_EXCEEDED` if none.

  Failures that lead to retries are logged at the info level, and
  dropped exports at the error level.
  """

  alias Mint.OTLP.GRPC

  @typedoc """
  A client, as returned by `new/4`.
  """
  @opaque t() :: GRPC.t()

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

  @user_agent "mint-otlp/#{Mix.Project.config()[:version]}"

  @doc """
  Creates a client for an OTLP endpoint.

  It does not connect, the connection is established on the first export.

  ## Options

    * `:transport` - the OTLP transport. Only `:grpc` is supported.

    * `:headers` - headers to send with every export, such as
      `authorization`

    * `:user_agent` - the `user-agent` header. The OTLP specification
      recommends identifying the exporter and its version. Defaults to
      `"#{@user_agent}"`

  ## GRPC options

    * `:timeout` - how long to wait for the response to each attempt,
      in milliseconds, also sent to the server as `grpc-timeout`. It
      also sets how quickly dead connections are detected and is used
      as the `:send_timeout` of the socket, unless one is given in
      `:transport_opts`. Defaults to `10_000`

    * `:retry` - `false` to disable retries, `true` for the defaults,
      or a keyword list with:

      * `:initial_backoff` - the delay before the first retry, in
        milliseconds. Defaults to `5_000`

      * `:max_backoff` - the maximum delay between retries, in
        milliseconds. Defaults to `30_000`

      * `:max_elapsed_time` - for how long an export may wait to be sent
        or retried, in milliseconds. Defaults to `300_000`

      Without retries, exports fail on their first failure, or if they
      cannot be sent within `:timeout`. The backoff options still apply
      to reconnects.

  Other options are given to `Mint.HTTP2.connect/4`, except for `:mode`,
  which is always `:active`. Connecting blocks the calling process, so
  consider giving a `:timeout` in `:transport_opts`.
  """
  @spec new(Mint.Types.scheme(), Mint.Types.address(), :inet.port_number(), keyword()) :: t()
  def new(scheme, address, port, opts \\ []) do
    {transport, opts} = Keyword.pop(opts, :transport, :grpc)

    case transport do
      :grpc -> GRPC.new(scheme, address, port, Keyword.put_new(opts, :user_agent, @user_agent))
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
