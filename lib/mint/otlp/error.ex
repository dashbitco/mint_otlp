defmodule Mint.OTLP.Error do
  @moduledoc """
  An export that failed.

  The public fields are:

    * `:transport` - the transport that returned the error, such as
      `Mint.OTLP.GRPC`

    * `:code` - the error code, specific to the transport. For example,
      `Mint.OTLP.GRPC` uses gRPC status codes, see `t:Mint.OTLP.GRPC.code/0`

    * `:message` - a message describing the error, which may be empty

    * `:retry_after` - the delay in milliseconds the server asked us to
      wait before retrying, or `nil`

  Errors from the underlying connection are returned as `Mint.HTTPError`
  and `Mint.TransportError`. See `Mint.OTLP.retryable?/1`.
  """

  @type t() :: %__MODULE__{
          transport: module(),
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
