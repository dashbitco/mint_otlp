defmodule Mint.OTLPTest do
  use ExUnit.Case, async: true

  import Mint.OTLP.TestHelpers

  alias Mint.OTLP
  alias Mint.OTLP.{RawServer, Requests, TestServer}

  @retry [initial_backoff: 10, max_backoff: 50, max_elapsed_time: 500]

  setup do
    port = Application.fetch_env!(:mint_otlp, :test_server_port)
    client = OTLP.new(:http, "localhost", port, headers: [{"x-new", "yes"}], retry: @retry)
    %{client: client, port: port}
  end

  describe "export/4" do
    test "exports traces", %{client: client} do
      request = Requests.traces(3)
      {:ok, client, ref} = OTLP.export(client, :traces, request, headers: TestServer.headers())

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
      {:ok, client, metrics} = OTLP.export(client, :metrics, Requests.metrics(), headers: headers)
      {:ok, client, logs} = OTLP.export(client, :logs, Requests.logs(), headers: headers)

      {_client, responses} = await(client, [metrics, logs])
      assert {:done, ^metrics, %{rejected: 0}} = responses[metrics]
      assert {:done, ^logs, %{rejected: 0}} = responses[logs]

      assert_receive {:test_server,
                      "/opentelemetry.proto.collector.metrics.v1.MetricsService/Export", _, _}

      assert_receive {:test_server, "/opentelemetry.proto.collector.logs.v1.LogsService/Export",
                      _, _}
    end

    test "accepts the size", %{client: client} do
      request = Requests.traces(10)
      size = IO.iodata_length(request)

      {:ok, client, ref} =
        OTLP.export(client, :traces, request, size: size, headers: TestServer.headers())

      {_client, %{^ref => {:done, ^ref, _}}} = await(client, ref)
      assert_receive {:test_server, _, _, <<0, ^size::32, _::binary-size(^size)>>}
    end

    test "sends large requests as the windows allow", %{client: client} do
      # About 1MB, while windows start at 64KB
      request = Requests.traces(10_000)
      size = IO.iodata_length(request)
      assert size > 1_000_000

      headers = TestServer.headers("sleep:50:ok")
      {:ok, client, ref} = OTLP.export(client, :traces, request, headers: headers)
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
            OTLP.export(client, :traces, request, headers: TestServer.headers())

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
            OTLP.export(client, :traces, request, headers: TestServer.headers())

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

  describe "responses" do
    test "partial success", %{client: client} do
      headers = TestServer.headers("partial:3:invalid span")
      {:ok, client, ref} = OTLP.export(client, :traces, Requests.traces(5), headers: headers)

      {_client, %{^ref => response}} = await(client, ref)
      assert response == {:done, ref, %{rejected: 3, error_message: "invalid span"}}
    end

    test "retries retryable statuses", %{client: client} do
      headers = TestServer.headers("status:14:try again, later", 2)
      {:ok, client, ref} = OTLP.export(client, :traces, Requests.traces(1), headers: headers)

      {_client, %{^ref => {:done, ^ref, _}}} = await(client, ref)
      assert requests() == 3
    end

    test "gives up after the maximum elapsed time", %{client: client} do
      headers = TestServer.headers("status:14:try again, later")
      {:ok, client, ref} = OTLP.export(client, :traces, Requests.traces(1), headers: headers)

      log =
        ExUnit.CaptureLog.capture_log(fn ->
          {_client, %{^ref => {:error, ^ref, error}}} = await(client, ref)
          send(self(), {:error, error})
        end)

      assert log =~
               ~r"\[info\] Backing off OTLP exports for \d+ms after: OTLP export failed with unavailable"

      assert log =~ "[error] Discarding OTLP export: OTLP export failed with unavailable"
      assert_received {:error, error}
      assert %Mint.OTLP.Error{transport: Mint.OTLP.GRPC, code: :unavailable} = error
      assert error.message == "try again, later"
      assert Exception.message(error) == "OTLP export failed with unavailable: try again, later"
      assert requests() > 2
    end

    test "does not retry when disabled", %{port: port} do
      client = OTLP.new(:http, "localhost", port, retry: false)
      headers = TestServer.headers("status:14:try again, later", 1)
      {:ok, client, ref} = OTLP.export(client, :traces, Requests.traces(1), headers: headers)

      {_client, %{^ref => {:error, ^ref, %{code: :unavailable}}}} = await(client, ref)
      assert requests() == 1
    end

    test "does not retry other statuses", %{client: client} do
      headers = TestServer.headers("status:3:bad request", 1)
      {:ok, client, ref} = OTLP.export(client, :traces, Requests.traces(1), headers: headers)

      {_client, %{^ref => {:error, ^ref, error}}} = await(client, ref)
      assert {error.code, error.message} == {:invalid_argument, "bad request"}
      assert requests() == 1
    end

    test "backs off the whole client on retryable errors", %{port: port} do
      # Between 100 and 200ms
      client = OTLP.new(:http, "localhost", port, retry: [initial_backoff: 200, max_backoff: 200])
      a_headers = TestServer.headers("status:14:try again, later", 1)
      {:ok, client, a} = OTLP.export(client, :traces, Requests.traces(1), headers: a_headers)
      client = stream_while(client, &(&1.backoff_until == nil))
      a_id = List.keyfind(a_headers, "x-test-id", 0)
      assert_received {:test_server, _, headers, _}
      assert List.keyfind(headers, "x-test-id", 0) == a_id

      # B is exported after A failed, so it waits for the backoff too
      b_headers = TestServer.headers()
      {:ok, client, b} = OTLP.export(client, :traces, Requests.traces(1), headers: b_headers)
      client = stream_for(client, 50)
      refute_received {:test_server, _, _, _}

      {_client, responses} = await(client, [a, b])
      assert {:done, ^a, _} = responses[a]
      assert {:done, ^b, _} = responses[b]

      ids =
        for _ <- 1..2 do
          assert_receive {:test_server, _, headers, _}
          List.keyfind(headers, "x-test-id", 0)
        end

      b_id = List.keyfind(b_headers, "x-test-id", 0)
      assert Enum.sort(ids) == Enum.sort([a_id, b_id])
    end

    test "backs off exponentially", %{port: port} do
      retry = [initial_backoff: 50, max_backoff: 2000, max_elapsed_time: 1000]
      client = OTLP.new(:http, "localhost", port, retry: retry)
      headers = TestServer.headers("status:14:try again, later")
      {:ok, client, ref} = OTLP.export(client, :traces, Requests.traces(1), headers: headers)

      {_client, %{^ref => {:error, ^ref, %{code: :unavailable}}}} = await(client, ref, 2000)

      # Even with the shortest jitter, the delays 25, 37, 56, 84, 126, 190,
      # and 284ms add up to almost 1s, while 50ms each would be 20 attempts
      assert requests() <= 9
    end

    test "retries resource exhausted only with a retry delay", %{client: client} do
      headers = TestServer.headers("retry:8:200", 1)
      {:ok, client, ref} = OTLP.export(client, :traces, Requests.traces(1), headers: headers)

      {time, {client, %{^ref => {:done, ^ref, _}}}} = :timer.tc(fn -> await(client, ref) end)
      assert time >= 200_000
      assert requests() == 2

      headers = TestServer.headers("status:8:slow down", 1)
      {:ok, client, ref} = OTLP.export(client, :traces, Requests.traces(1), headers: headers)

      {_client, %{^ref => {:error, ^ref, error}}} = await(client, ref)

      assert {error.code, error.message, error.retry_after} ==
               {:resource_exhausted, "slow down", nil}

      assert requests() == 1
    end

    test "maps HTTP statuses", %{client: client} do
      headers = TestServer.headers("http:503", 1)
      {:ok, client, ref} = OTLP.export(client, :traces, Requests.traces(1), headers: headers)
      {client, %{^ref => {:done, ^ref, _}}} = await(client, ref)
      assert requests() == 2

      headers = TestServer.headers("http:401", 1)
      {:ok, client, ref} = OTLP.export(client, :traces, Requests.traces(1), headers: headers)

      {_client, %{^ref => {:error, ^ref, error}}} = await(client, ref)
      assert {error.code, error.message} == {:unauthenticated, "HTTP status 401"}
    end

    test "times out exports without affecting others", %{port: port} do
      client = OTLP.new(:http, "localhost", port, timeout: 300, retry: false)
      headers = TestServer.headers("sleep:1000:ok")
      {:ok, client, slow} = OTLP.export(client, :traces, Requests.traces(1), headers: headers)
      client = stream_for(client, 150)

      # The fast one shows the connection is alive and the other one is
      # still in flight when the slow one times out
      {:ok, client, fast} =
        OTLP.export(client, :traces, Requests.traces(1), headers: TestServer.headers())

      headers = TestServer.headers("sleep:200:ok")
      {:ok, client, other} = OTLP.export(client, :traces, Requests.traces(1), headers: headers)

      {_client, responses} = await(client, [slow, fast, other])
      assert {:error, ^slow, %Mint.OTLP.Error{code: :deadline_exceeded}} = responses[slow]
      assert {:done, ^fast, _} = responses[fast]
      assert {:done, ^other, _} = responses[other]
    end

    test "retries attempts that time out", %{port: port} do
      client = OTLP.new(:http, "localhost", port, timeout: 100, retry: @retry)
      headers = TestServer.headers("sleep:2000:ok", 1)
      {:ok, client, ref} = OTLP.export(client, :traces, Requests.traces(1), headers: headers)

      # Only the second attempt can finish in time
      {_client, %{^ref => {:done, ^ref, _}}} = await(client, ref, 1000)
      assert_receive {:test_server, _, headers, _}
      assert {"grpc-timeout", "100m"} in headers
    end
  end

  describe "connection" do
    test "connects on the first export" do
      port = unused_port()
      client = OTLP.new(:http, "localhost", port, retry: @retry)

      {_pid, ^port} = TestServer.start(port)
      {:ok, client, ref} = OTLP.export(client, :traces, Requests.traces(1))
      {_client, %{^ref => {:done, ^ref, _}}} = await(client, ref)
    end

    test "fails exports when it cannot connect" do
      client = OTLP.new(:http, "localhost", unused_port(), retry: @retry)
      {:ok, client, ref} = OTLP.export(client, :traces, Requests.traces(1))

      {_client, %{^ref => {:error, ^ref, %Mint.TransportError{reason: :econnrefused}}}} =
        await(client, ref)
    end

    test "fails exports after the timeout when it cannot connect and retries are disabled" do
      client = OTLP.new(:http, "localhost", unused_port(), retry: false, timeout: 100)
      {:ok, client, ref} = OTLP.export(client, :traces, Requests.traces(1))

      {time, {_client, responses}} = :timer.tc(fn -> await(client, ref) end)
      assert {:error, ^ref, %Mint.TransportError{reason: :econnrefused}} = responses[ref]
      assert time >= 100_000
    end

    test "reconnects until the server is up" do
      port = unused_port()
      client = OTLP.new(:http, "localhost", port, retry: [initial_backoff: 20, max_backoff: 20])
      {:ok, client, ref} = OTLP.export(client, :traces, Requests.traces(1))

      # Several connection attempts fail before the server is up
      spawn(fn ->
        Process.sleep(200)
        TestServer.start(port)
      end)

      {_client, %{^ref => {:done, ^ref, _}}} = await(client, ref)
    end

    # Killing the server logs errors
    @tag :capture_log
    test "resends exports when the connection drops" do
      {pid, port} = TestServer.start()
      client = OTLP.new(:http, "localhost", port, retry: [initial_backoff: 50, max_backoff: 50])

      headers = TestServer.headers("sleep:5000:ok", 1)
      {:ok, client, ref} = OTLP.export(client, :traces, Requests.traces(1), headers: headers)

      # Let the request reach the server, then drop it
      client = stream_for(client, 100)
      Process.exit(pid, :kill)

      wait_until_free(port)
      {_pid, ^port} = TestServer.start(port)
      {_client, %{^ref => {:done, ^ref, _}}} = await(client, ref)
    end

    test "resends exports when the server closes the connection before they are sent" do
      {:ok, listen} = :gen_tcp.listen(0, [:binary, active: false, ip: :loopback, reuseaddr: true])
      {:ok, port} = :inet.port(listen)

      # Accepts the connection, reads the first request, and closes it
      spawn_link(fn ->
        {:ok, socket} = :gen_tcp.accept(listen)
        {:ok, _} = :gen_tcp.recv(socket, 0)
        Process.sleep(100)
        :gen_tcp.close(socket)
      end)

      client = OTLP.new(:http, "localhost", port, retry: @retry)
      {:ok, client, a} = OTLP.export(client, :traces, Requests.traces(1))
      client = stream_for(client, 50)

      # B is only sent after the server closed the connection
      {:ok, client, b} = OTLP.export(client, :traces, Requests.traces(1))
      Process.sleep(300)

      :ok = :gen_tcp.close(listen)
      wait_until_free(port)
      {_pid, ^port} = TestServer.start(port)

      {_client, responses} = await(client, [a, b])
      assert {:done, ^a, _} = responses[a]
      assert {:done, ^b, _} = responses[b]
    end

    test "drops the connection right away after a GOAWAY with an error" do
      {listen, port} = RawServer.listen()

      spawn_link(fn ->
        socket = RawServer.accept(listen)
        stream = RawServer.receive_request(socket)

        # Mint stops reading after a GOAWAY with ENHANCE_YOUR_CALM
        RawServer.goaway(socket, stream, 11)
        Process.sleep(100)
        RawServer.respond(socket, stream)

        socket = RawServer.accept(listen)
        stream = RawServer.receive_request(socket)
        RawServer.respond(socket, stream)
        Process.sleep(1000)
      end)

      client = OTLP.new(:http, "localhost", port, timeout: 5000, retry: @retry)
      {:ok, client, ref} = OTLP.export(client, :traces, Requests.traces(1))
      {_client, %{^ref => {:done, ^ref, _}}} = await(client, ref, 1000)
    end

    test "drops connections where the server stopped responding" do
      {listen, port} = RawServer.listen()
      parent = self()

      # Never responds, not even to pings
      spawn_link(fn ->
        socket = RawServer.accept(listen)
        _stream = RawServer.receive_request(socket)
        send(parent, {:test_server, :closed, RawServer.await_close(socket)})
      end)

      # The first attempt times out, but the server sent its settings,
      # so the connection is found dead when the second times out
      retry = [initial_backoff: 10, max_backoff: 10, max_elapsed_time: 600]
      client = OTLP.new(:http, "localhost", port, timeout: 200, retry: retry)
      {:ok, client, ref} = OTLP.export(client, :traces, Requests.traces(1))

      {client, %{^ref => {:error, ^ref, error}}} = await(client, ref)

      # Either the connection is found dead first or the export times out first
      assert match?(%Mint.TransportError{reason: :timeout}, error) or
               match?(%Mint.OTLP.Error{code: :deadline_exceeded}, error)

      stream_for(client, 500)
      assert_receive {:test_server, :closed, {:error, :closed}}
    end

    test "fails queued exports that cannot start" do
      {listen, port} = RawServer.listen()

      # SETTINGS_MAX_CONCURRENT_STREAMS of 0
      spawn_link(fn ->
        socket = RawServer.accept(listen, [{3, 0}])
        RawServer.await_close(socket)
      end)

      client = OTLP.new(:http, "localhost", port, timeout: 200, retry: false)

      # The first export is sent before the server SETTINGS arrive
      {:ok, client, first} = OTLP.export(client, :traces, Requests.traces(1))
      client = stream_for(client, 50)
      exported_at = System.monotonic_time(:millisecond)
      {:ok, client, second} = OTLP.export(client, :traces, Requests.traces(1))

      # The first one times out, which may also find the connection dead
      {_client, responses} = await(client, [first, second])
      assert {:error, ^first, _error} = responses[first]
      assert {:error, ^second, _error} = responses[second]
      assert System.monotonic_time(:millisecond) - exported_at >= 200
    end

    test "backs off when the server refuses requests" do
      {listen, port} = RawServer.listen()
      parent = self()

      spawn_link(fn ->
        socket = RawServer.accept(listen)

        refuse = fn refuse ->
          stream = RawServer.receive_request(socket)
          send(parent, {:test_server, :refused, nil, nil})
          # REFUSED_STREAM
          RawServer.reset(socket, stream, 7)
          refuse.(refuse)
        end

        refuse.(refuse)
      end)

      retry = [initial_backoff: 50, max_backoff: 50, max_elapsed_time: 500]
      client = OTLP.new(:http, "localhost", port, retry: retry)
      {:ok, client, ref} = OTLP.export(client, :traces, Requests.traces(1))

      {_client, %{^ref => {:error, ^ref, %Mint.HTTPError{}}}} = await(client, ref)

      # Backing off between 25 and 50ms, instead of thousands of attempts
      assert requests() <= 21
    end

    test "resends requests left unprocessed by a GOAWAY right away" do
      {listen, port} = RawServer.listen()

      spawn_link(fn ->
        socket = RawServer.accept(listen)
        first = RawServer.receive_request(socket)
        second = RawServer.receive_request(socket)
        RawServer.respond(socket, first)
        RawServer.goaway(socket, first, 0)

        socket = RawServer.accept(listen)
        RawServer.respond(socket, RawServer.receive_request(socket))
        Process.sleep(1000)
        second
      end)

      # Waiting for the backoff would take seconds
      client = OTLP.new(:http, "localhost", port, retry: [initial_backoff: 2000])
      {:ok, client, a} = OTLP.export(client, :traces, Requests.traces(1))
      {:ok, client, b} = OTLP.export(client, :traces, Requests.traces(1))

      {_client, responses} = await(client, [a, b], 1000)
      assert {:done, ^a, _} = responses[a]
      assert {:done, ^b, _} = responses[b]
    end

    test "close fails exports in flight", %{client: client} do
      headers = TestServer.headers("sleep:5000:ok")
      {:ok, client, ref} = OTLP.export(client, :traces, Requests.traces(1), headers: headers)
      client = stream_for(client, 100)

      {:responses, client, responses} = OTLP.close(client)
      assert responses == [{:error, ref, %Mint.TransportError{reason: :closed}}]

      {:ok, client, ref} = OTLP.export(client, :traces, Requests.traces(1))
      {_client, %{^ref => {:done, ^ref, _}}} = await(client, ref)
    end

    test "close fails pending exports", %{client: client} do
      headers = TestServer.headers("sleep:5000:ok")
      {:ok, client, ref} = OTLP.export(client, :traces, Requests.traces(1), headers: headers)

      {:responses, client, responses} = OTLP.close(client)
      assert responses == [{:error, ref, %Mint.TransportError{reason: :closed}}]

      # And the client can be used again
      {:ok, client, ref} = OTLP.export(client, :traces, Requests.traces(1))
      {_client, %{^ref => {:done, ^ref, _}}} = await(client, ref)
    end

    test "sends the user agent", %{client: client, port: port} do
      request = Requests.traces(1)
      {:ok, client, ref} = OTLP.export(client, :traces, request, headers: TestServer.headers())
      await(client, ref)
      assert_receive {:test_server, _, headers, _}
      assert {"user-agent", "mint-otlp/" <> _} = List.keyfind(headers, "user-agent", 0)

      client = OTLP.new(:http, "localhost", port, user_agent: "my-sdk/1.0")
      {:ok, client, ref} = OTLP.export(client, :traces, request, headers: TestServer.headers())
      await(client, ref)
      assert_receive {:test_server, _, headers, _}
      assert {"user-agent", "my-sdk/1.0"} in headers
    end

    test "ignores unknown messages", %{client: client} do
      assert OTLP.stream(client, :unknown) == :unknown
    end
  end

  # Requests the test server received
  defp requests(count \\ 0) do
    receive do
      {:test_server, _, _, _} -> requests(count + 1)
    after
      100 -> count
    end
  end

  defp unused_port do
    {:ok, socket} = :gen_tcp.listen(0, ip: :loopback)
    {:ok, port} = :inet.port(socket)
    :ok = :gen_tcp.close(socket)
    port
  end

  defp wait_until_free(port) do
    case :gen_tcp.listen(port, ip: :loopback, reuseaddr: true) do
      {:ok, socket} ->
        :gen_tcp.close(socket)

      {:error, :eaddrinuse} ->
        Process.sleep(10)
        wait_until_free(port)
    end
  end

  # Gives the client its messages while the condition holds, expecting no responses
  defp stream_while(client, condition) do
    if condition.(client) do
      receive do
        message when not (is_tuple(message) and elem(message, 0) == :test_server) ->
          {:responses, client, []} = OTLP.stream(client, message)
          stream_while(client, condition)
      after
        5000 -> flunk("condition did not change")
      end
    else
      client
    end
  end

  # Gives the client its messages for the given time, expecting no responses
  defp stream_for(client, time) do
    stream_until(client, System.monotonic_time(:millisecond) + time)
  end

  defp stream_until(client, deadline) do
    timeout = max(deadline - System.monotonic_time(:millisecond), 0)

    receive do
      message when not (is_tuple(message) and elem(message, 0) == :test_server) ->
        {:responses, client, []} = OTLP.stream(client, message)
        stream_until(client, deadline)
    after
      timeout -> client
    end
  end
end
