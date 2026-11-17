defmodule Mint.OTLP.Case do
  @moduledoc """
  Tests against `Mint.OTLP.TestServer`, with a client for it in the
  context, as `:client`, and its port, as `:port`.
  """
  use ExUnit.CaseTemplate

  @retry [initial_backoff: 10, max_backoff: 50, max_elapsed_time: 5000]

  using do
    quote do
      import Mint.OTLP.TestHelpers

      alias Mint.OTLP
      alias Mint.OTLP.{RawServer, Requests, TestServer}

      @retry unquote(@retry)
    end
  end

  setup do
    port = Application.fetch_env!(:mint_otlp, :test_server_port)
    client = Mint.OTLP.new(:http, "localhost", port, headers: [{"x-new", "yes"}], retry: @retry)
    %{client: client, port: port}
  end
end
