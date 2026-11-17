defmodule Mint.OTLPTest do
  use ExUnit.Case, async: true

  import Mint.OTLP.TestHelpers

  alias Mint.OTLP
  alias Mint.OTLP.{Requests, TestServer}

  setup do
    port = Application.fetch_env!(:mint_otlp, :test_server_port)
    {:ok, client} = OTLP.connect(:http, "localhost", port, headers: [{"x-connect", "yes"}])
    %{client: client}
  end

  describe "export/4" do
    test "exports traces", %{client: client} do
      request = Requests.traces(3)
      {:ok, client, ref} = OTLP.export(client, :traces, request, headers: TestServer.headers())
      assert OTLP.open_request_count(client) == 1

      {client, %{^ref => response}} = await(client, ref)
      assert response == {:ok, ref, %{rejected: 0, error_message: ""}}
      assert OTLP.open_request_count(client) == 0

      assert_receive {:test_server, path, headers, body}
      assert path == "/opentelemetry.proto.collector.trace.v1.TraceService/Export"
      assert body == IO.iodata_to_binary([<<0, IO.iodata_length(request)::32>> | request])
      assert {"content-type", "application/grpc"} in headers
      assert {"te", "trailers"} in headers
      assert {"x-connect", "yes"} in headers
    end

    test "exports metrics and logs", %{client: client} do
      headers = TestServer.headers()
      {:ok, client, metrics} = OTLP.export(client, :metrics, Requests.metrics(), headers: headers)
      {:ok, client, logs} = OTLP.export(client, :logs, Requests.logs(), headers: headers)

      {_client, responses} = await(client, [metrics, logs])
      assert {:ok, ^metrics, %{rejected: 0}} = responses[metrics]
      assert {:ok, ^logs, %{rejected: 0}} = responses[logs]

      assert_receive {:test_server,
                      "/opentelemetry.proto.collector.metrics.v1.MetricsService/Export", _, _}

      assert_receive {:test_server, "/opentelemetry.proto.collector.logs.v1.LogsService/Export",
                      _, _}
    end

    test "sends the timeout", %{client: client} do
      {:ok, client, ref} =
        OTLP.export(client, :traces, Requests.traces(1),
          headers: TestServer.headers(),
          timeout: 5000
        )

      await(client, ref)
      assert_receive {:test_server, _, headers, _}
      assert {"grpc-timeout", "5000m"} in headers
    end

    test "accepts the size", %{client: client} do
      request = Requests.traces(10)
      size = IO.iodata_length(request)

      {:ok, client, ref} =
        OTLP.export(client, :traces, request, size: size, headers: TestServer.headers())

      {_client, %{^ref => {:ok, ^ref, _}}} = await(client, ref)
      assert_receive {:test_server, _, _, <<0, ^size::32, _::binary-size(^size)>>}
    end

    test "sends large requests as the windows allow", %{client: client} do
      # About 1MB, while windows start at 64KB
      request = Requests.traces(10_000)
      size = IO.iodata_length(request)
      assert size > 1_000_000

      headers = TestServer.headers("sleep:50:ok")
      {:ok, client, ref} = OTLP.export(client, :traces, request, headers: headers)
      {_client, %{^ref => {:ok, ^ref, _}}} = await(client, ref)

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
            OTLP.export(client, :traces, request, headers: TestServer.headers())

          {client, %{^ref => {:ok, ^ref, _}}} = await(client, ref)

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
            OTLP.export(client, :traces, request, headers: TestServer.headers())

          {ref, client}
        end)

      {client, responses} = await(client, refs)
      assert map_size(responses) == 5
      assert Enum.all?(responses, &match?({_, {:ok, _, %{rejected: 0}}}, &1))
      assert OTLP.open_request_count(client) == 0

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

  describe "responses" do
    test "partial success", %{client: client} do
      headers = TestServer.headers("partial:3:invalid span")
      {:ok, client, ref} = OTLP.export(client, :traces, Requests.traces(5), headers: headers)

      {_client, %{^ref => response}} = await(client, ref)
      assert response == {:ok, ref, %{rejected: 3, error_message: "invalid span"}}
    end

    test "gRPC status", %{client: client} do
      headers = TestServer.headers("status:14:try again, later")
      {:ok, client, ref} = OTLP.export(client, :traces, Requests.traces(1), headers: headers)

      {_client, %{^ref => {:error, ^ref, error}}} = await(client, ref)
      assert %Mint.OTLP.Error{transport: Mint.OTLP.GRPC, code: :unavailable} = error
      assert error.message == "try again, later"
      assert error.retry_after == nil
      assert Exception.message(error) == "OTLP export failed with unavailable: try again, later"
      assert OTLP.retryable?(error)
    end

    test "non-retryable gRPC status", %{client: client} do
      headers = TestServer.headers("status:3:bad request")
      {:ok, client, ref} = OTLP.export(client, :traces, Requests.traces(1), headers: headers)

      {_client, %{^ref => {:error, ^ref, error}}} = await(client, ref)
      assert {error.code, error.message} == {:invalid_argument, "bad request"}
      refute OTLP.retryable?(error)
    end

    test "resource exhausted with and without retry info", %{client: client} do
      headers = TestServer.headers("retry:8:2500")
      {:ok, client, ref} = OTLP.export(client, :traces, Requests.traces(1), headers: headers)

      {client, %{^ref => {:error, ^ref, error}}} = await(client, ref)
      assert {error.code, error.message} == {:resource_exhausted, ""}
      assert error.retry_after == 2500
      assert OTLP.retryable?(error)

      headers = TestServer.headers("status:8:slow down")
      {:ok, client, ref} = OTLP.export(client, :traces, Requests.traces(1), headers: headers)

      {_client, %{^ref => {:error, ^ref, error}}} = await(client, ref)
      assert {error.code, error.message} == {:resource_exhausted, "slow down"}
      refute OTLP.retryable?(error)
    end

    test "HTTP status", %{client: client} do
      headers = TestServer.headers("http:503")
      {:ok, client, ref} = OTLP.export(client, :traces, Requests.traces(1), headers: headers)

      {_client, %{^ref => {:error, ^ref, error}}} = await(client, ref)
      assert {error.code, error.message} == {:unavailable, "HTTP status 503"}
      assert OTLP.retryable?(error)
    end
  end

  describe "client" do
    test "sends the user agent", %{client: client} do
      request = Requests.traces(1)
      {:ok, client, ref} = OTLP.export(client, :traces, request, headers: TestServer.headers())
      await(client, ref)
      assert_receive {:test_server, _, headers, _}
      assert {"user-agent", "mint-otlp/" <> _} = List.keyfind(headers, "user-agent", 0)

      port = Application.fetch_env!(:mint_otlp, :test_server_port)
      {:ok, client} = OTLP.connect(:http, "localhost", port, user_agent: "my-sdk/1.0")
      {:ok, client, ref} = OTLP.export(client, :traces, request, headers: TestServer.headers())
      await(client, ref)
      assert_receive {:test_server, _, headers, _}
      assert {"user-agent", "my-sdk/1.0"} in headers
    end

    test "ignores unknown messages", %{client: client} do
      assert OTLP.stream(client, :unknown) == :unknown
    end

    test "closes", %{client: client} do
      assert OTLP.open?(client)
      {:ok, client} = OTLP.close(client)
      refute OTLP.open?(client)
    end
  end
end
