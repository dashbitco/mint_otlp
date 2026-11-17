defmodule Mint.OTLP.Error do
  @moduledoc """
  An export that failed, as returned by the server, or as determined by
  the client, such as when the server does not respond in time.

  The public fields are:

    * `:transport` - the transport that returned the error, such as `:grpc`

    * `:code` - the error code, specific to the transport. With the
      `:grpc` transport, it is a gRPC status code, see `t:grpc_code/0`

    * `:message` - a message describing the error, which may be empty

    * `:retry_after` - the delay in milliseconds the server asked us to
      wait before retrying, or `nil`

  Errors from the underlying connection are returned as `Mint.HTTPError`
  and `Mint.TransportError`.
  """

  @typedoc """
  A gRPC status code other than `OK`.
  """
  @type grpc_code() ::
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

  @type t() :: %__MODULE__{
          transport: :grpc,
          code: term(),
          message: String.t(),
          retry_after: non_neg_integer() | nil
        }

  defexception [:transport, :code, :message, :retry_after]

  @impl true
  def message(%__MODULE__{code: code, message: ""}), do: "OTLP export failed with #{code}"

  def message(%__MODULE__{code: code, message: message}),
    do: "OTLP export failed with #{code}: #{message}"
end
