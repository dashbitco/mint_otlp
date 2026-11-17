defmodule Mint.OTLP.CollectorTest do
  # Runs against the OpenTelemetry Collector, see docker-compose.yml
  use ExUnit.Case, async: true

  import Mint.OTLP.TestHelpers

  alias Mint.OTLP
  alias Mint.OTLP.Requests

  @moduletag :integration

  setup do
    {:ok, client} = OTLP.connect(:http, "localhost", 4317)
    %{client: client}
  end

  test "exports traces, metrics, and logs", %{client: client} do
    {:ok, client, traces} = OTLP.export(client, :traces, Requests.traces(10))
    {:ok, client, metrics} = OTLP.export(client, :metrics, Requests.metrics())
    {:ok, client, logs} = OTLP.export(client, :logs, Requests.logs())

    {_client, responses} = await(client, [traces, metrics, logs])
    assert responses[traces] == {:ok, traces, %{rejected: 0, error_message: ""}}
    assert responses[metrics] == {:ok, metrics, %{rejected: 0, error_message: ""}}
    assert responses[logs] == {:ok, logs, %{rejected: 0, error_message: ""}}
  end

  test "exports large and concurrent requests", %{client: client} do
    # About 3MB each, below the 4MB gRPC default
    {refs, client} =
      Enum.map_reduce(1..3, client, fn _, client ->
        {:ok, client, ref} = OTLP.export(client, :traces, Requests.traces(30_000))
        {ref, client}
      end)

    {client, responses} = await(client, refs, 30_000)
    assert Enum.all?(responses, &match?({_, {:ok, _, %{rejected: 0}}}, &1))
    assert OTLP.open_request_count(client) == 0
  end

  test "rejects requests above the maximum message size", %{client: client} do
    {:ok, client, ref} = OTLP.export(client, :traces, Requests.traces(50_000))

    {_client, %{^ref => {:error, ^ref, error}}} = await(client, ref, 30_000)
    assert error.code == :resource_exhausted
    refute OTLP.retryable?(error)
  end

  test "rejects malformed requests", %{client: client} do
    {:ok, client, ref} = OTLP.export(client, :traces, <<0xFF, 0xFF, 0xFF>>)

    {_client, %{^ref => {:error, ^ref, error}}} = await(client, ref)
    assert error.code in [:internal, :invalid_argument]
    refute OTLP.retryable?(error)
  end
end
