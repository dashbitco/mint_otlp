defmodule Mint.OTLP.ExportTest do
  use Mint.OTLP.Case, async: true

  test "exports traces", %{client: client} do
    request = Requests.traces(3)
    {:ok, client, ref} = OTLP.export(client, {:traces, request}, headers: TestServer.headers())

    {_client, %{^ref => response}} = await(client, ref)
    assert response == {:done, ref, %{rejected: 0, error_message: ""}}

    assert_receive {:test_server, path, headers, body}
    assert path == "/opentelemetry.proto.collector.trace.v1.TraceService/Export"
    assert body == IO.iodata_to_binary([<<0, IO.iodata_length(request)::32>> | request])
    assert {"content-type", "application/grpc"} in headers
    assert {"te", "trailers"} in headers
    assert {"grpc-timeout", "10000m"} in headers
    assert {"x-new", "yes"} in headers
  end

  test "exports metrics and logs", %{client: client} do
    headers = TestServer.headers()

    {:ok, client, metrics} =
      OTLP.export(client, {:metrics, Requests.metrics()}, headers: headers)

    {:ok, client, logs} = OTLP.export(client, {:logs, Requests.logs()}, headers: headers)

    {_client, responses} = await(client, [metrics, logs])
    assert {:done, ^metrics, %{rejected: 0}} = responses[metrics]
    assert {:done, ^logs, %{rejected: 0}} = responses[logs]

    assert_receive {:test_server,
                    "/opentelemetry.proto.collector.metrics.v1.MetricsService/Export", _, _}

    assert_receive {:test_server, "/opentelemetry.proto.collector.logs.v1.LogsService/Export", _,
                    _}
  end

  test "accepts the size", %{client: client} do
    request = Requests.traces(10)
    size = IO.iodata_length(request)

    {:ok, client, ref} =
      OTLP.export(client, {:traces, request}, size: size, headers: TestServer.headers())

    {_client, %{^ref => {:done, ^ref, _}}} = await(client, ref)
    assert_receive {:test_server, _, _, <<0, ^size::32, _::binary-size(^size)>>}
  end

  test "sends large requests as the windows allow", %{client: client} do
    # About 1MB, while windows start at 64KB
    request = Requests.traces(10_000)
    size = IO.iodata_length(request)
    assert size > 1_000_000

    headers = TestServer.headers("sleep:50:ok")
    {:ok, client, ref} = OTLP.export(client, {:traces, request}, headers: headers)
    {_client, %{^ref => {:done, ^ref, _}}} = await(client, ref)

    assert_receive {:test_server, _, _, body}
    assert body == IO.iodata_to_binary([<<0, size::32>> | request])
  end

  test "sends large requests given as any iodata", %{client: client} do
    [first | rest] = Requests.traces(1_000)
    binary = IO.iodata_to_binary([first | rest])
    assert byte_size(binary) > 100_000

    # Nested lists, bytes as integers, and improper lists ending in binaries
    nested =
      rest
      |> Enum.chunk_every(7)
      |> Enum.map(fn [head | tail] -> [[head] | IO.iodata_to_binary(tail)] end)
      |> then(&[:binary.bin_to_list(first), [] | &1])

    assert IO.iodata_to_binary(nested) == binary

    for request <- [binary, nested], reduce: client do
      client ->
        {:ok, client, ref} =
          OTLP.export(client, {:traces, request}, headers: TestServer.headers())

        {client, %{^ref => {:done, ^ref, _}}} = await(client, ref)

        assert_receive {:test_server, _, _, body}
        assert body == <<0, byte_size(binary)::32, binary::binary>>
        client
    end
  end

  test "sends concurrent large requests", %{client: client} do
    requests = for i <- 1..5, do: Requests.traces(2_000, name: "span #{i}")

    {refs, client} =
      Enum.map_reduce(requests, client, fn request, client ->
        {:ok, client, ref} =
          OTLP.export(client, {:traces, request}, headers: TestServer.headers())

        {ref, client}
      end)

    {_client, responses} = await(client, refs)
    assert map_size(responses) == 5
    assert Enum.all?(responses, &match?({_, {:done, _, %{rejected: 0}}}, &1))

    bodies =
      for _ <- 1..5 do
        assert_receive {:test_server, _, _, body}
        body
      end

    expected =
      for request <- requests,
          do: IO.iodata_to_binary([<<0, IO.iodata_length(request)::32>> | request])

    assert Enum.sort(bodies) == Enum.sort(expected)
  end
end
