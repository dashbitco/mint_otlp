defmodule Mint.OTLP.TestHelpers do
  @moduledoc false

  import ExUnit.Assertions

  @doc """
  Streams messages until the responses for all `refs` arrive.

  Messages from `Mint.OTLP.TestServer` are left in the mailbox.
  """
  def await(client, refs, timeout \\ 5000) do
    await(client, List.wrap(refs), %{}, timeout)
  end

  defp await(client, [], acc, _timeout), do: {client, acc}

  defp await(client, refs, acc, timeout) do
    receive do
      message when not (is_tuple(message) and elem(message, 0) == :test_server) ->
        {client, responses} =
          case Mint.OTLP.stream(client, message) do
            {:responses, client, responses} -> {client, responses}
            :unknown -> flunk("unknown message: #{inspect(message)}")
          end

        acc =
          Enum.reduce(responses, acc, fn response, acc ->
            Map.put(acc, elem(response, 1), response)
          end)

        await(client, refs -- Map.keys(acc), acc, timeout)
    after
      timeout -> flunk("timed out waiting for #{inspect(refs)}")
    end
  end

  # Requests the test server received
  def requests(count \\ 0) do
    receive do
      {:test_server, _, _, _} -> requests(count + 1)
    after
      100 -> count
    end
  end

  # A certificate for localhost, signed by a generated root
  def tls_options do
    opts = [key: {:namedCurve, :secp256r1}, digest: :sha256]
    san = {:Extension, {2, 5, 29, 17}, false, [dNSName: ~c"localhost"]}
    chain = %{root: opts, intermediates: [], peer: [extensions: [san]] ++ opts}

    %{server_config: server, client_config: client} =
      :public_key.pkix_test_data(%{server_chain: chain, client_chain: chain})

    server_opts = Keyword.take(server, [:cert, :key])
    server_opts = [scheme: :https, thousand_island_options: [transport_options: server_opts]]
    {server_opts, Keyword.take(client, [:cacerts])}
  end

  def unused_port do
    {:ok, socket} = :gen_tcp.listen(0, ip: :loopback)
    {:ok, port} = :inet.port(socket)
    :ok = :gen_tcp.close(socket)
    port
  end

  # Polls until the condition holds
  def wait_until(condition) do
    if not condition.() do
      Process.sleep(10)
      wait_until(condition)
    end
  end

  def wait_until_free(port) do
    case :gen_tcp.listen(port, ip: :loopback, reuseaddr: true) do
      {:ok, socket} ->
        :gen_tcp.close(socket)

      {:error, :eaddrinuse} ->
        Process.sleep(10)
        wait_until_free(port)
    end
  end

  # Gives the client its messages while the condition holds, expecting no responses
  def stream_while(client, condition) do
    if condition.(client) do
      receive do
        message when not (is_tuple(message) and elem(message, 0) == :test_server) ->
          {:responses, client, []} = Mint.OTLP.stream(client, message)
          stream_while(client, condition)
      after
        5000 -> flunk("condition did not change")
      end
    else
      client
    end
  end

  # Gives the client its messages for the given time, expecting no responses
  def stream_for(client, time) do
    stream_until(client, System.monotonic_time(:millisecond) + time)
  end

  def stream_until(client, deadline) do
    timeout = max(deadline - System.monotonic_time(:millisecond), 0)

    receive do
      message when not (is_tuple(message) and elem(message, 0) == :test_server) ->
        {:responses, client, []} = Mint.OTLP.stream(client, message)
        stream_until(client, deadline)
    after
      timeout -> client
    end
  end
end
