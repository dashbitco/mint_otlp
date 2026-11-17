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
end
