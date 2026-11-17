defmodule Mint.OTLP.HTTP2Test do
  use Mint.OTLP.Case, async: true

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
    {:ok, client, ref} = OTLP.export(client, {:traces, Requests.traces(1)})
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
    {:ok, client, ref} = OTLP.export(client, {:traces, Requests.traces(1)})

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
    {:ok, client, first} = OTLP.export(client, {:traces, Requests.traces(1)})
    client = stream_for(client, 50)
    exported_at = System.monotonic_time(:millisecond)
    {:ok, client, second} = OTLP.export(client, {:traces, Requests.traces(1)})

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
    {:ok, client, ref} = OTLP.export(client, {:traces, Requests.traces(1)})

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
    {:ok, client, a} = OTLP.export(client, {:traces, Requests.traces(1)})
    {:ok, client, b} = OTLP.export(client, {:traces, Requests.traces(1)})

    {_client, responses} = await(client, [a, b], 1000)
    assert {:done, ^a, _} = responses[a]
    assert {:done, ^b, _} = responses[b]
  end

  test "resends requests that were being sent when the server sent a GOAWAY" do
    {listen, port} = RawServer.listen()

    # Only the first 64KB fit the windows, which the server never updates
    spawn_link(fn ->
      socket = RawServer.accept(listen)
      RawServer.goaway(socket, RawServer.receive_stream(socket), 0)

      socket = RawServer.accept(listen)
      RawServer.respond(socket, RawServer.receive_stream(socket))
      Process.sleep(1000)
    end)

    # Waiting for the backoff would take seconds
    client = OTLP.new(:http, "localhost", port, retry: [initial_backoff: 2000])
    {:ok, client, ref} = OTLP.export(client, {:traces, :binary.copy("a", 200_000)})
    {_client, %{^ref => {:done, ^ref, _}}} = await(client, ref, 1000)
  end

  test "retries retryable responses that arrive while the request is being sent" do
    {listen, port} = RawServer.listen()

    # The second request cannot send its body, as the first one
    # exhausted the connection window
    spawn_link(fn ->
      socket = RawServer.accept(listen)
      RawServer.respond(socket, RawServer.receive_stream(socket), "14")
      RawServer.respond(socket, RawServer.receive_stream(socket))
      Process.sleep(1000)
    end)

    client = OTLP.new(:http, "localhost", port, retry: @retry)
    {:ok, client, ref} = OTLP.export(client, {:traces, :binary.copy("a", 200_000)})
    {_client, %{^ref => {:done, ^ref, _}}} = await(client, ref)
  end

  test "backs off and retries requests cancelled by the server" do
    {listen, port} = RawServer.listen()

    spawn_link(fn ->
      socket = RawServer.accept(listen)
      # CANCEL
      RawServer.reset(socket, RawServer.receive_request(socket), 8)
      RawServer.respond(socket, RawServer.receive_request(socket))
      Process.sleep(1000)
    end)

    client = OTLP.new(:http, "localhost", port, retry: @retry)
    {:ok, client, ref} = OTLP.export(client, {:traces, Requests.traces(1)})

    {{_client, %{^ref => {:done, ^ref, _}}}, log} =
      ExUnit.CaptureLog.with_log(fn -> await(client, ref) end)

    assert log =~ ~r"Backing off OTLP exports for \d+ms after: .+ :cancel"
  end

  test "fails requests left unprocessed by a GOAWAY when retries are disabled" do
    {listen, port} = RawServer.listen()

    spawn_link(fn ->
      socket = RawServer.accept(listen)
      first = RawServer.receive_request(socket)
      _second = RawServer.receive_request(socket)
      RawServer.respond(socket, first)
      RawServer.goaway(socket, first, 0)
      Process.sleep(1000)
    end)

    client = OTLP.new(:http, "localhost", port, retry: false)
    {:ok, client, a} = OTLP.export(client, {:traces, Requests.traces(1)})
    {:ok, client, b} = OTLP.export(client, {:traces, Requests.traces(1)})

    {_client, responses} = await(client, [a, b])
    assert {:done, ^a, _} = responses[a]
    assert {:error, ^b, %Mint.HTTPError{reason: :unprocessed}} = responses[b]
  end

  test "starts queued exports as others finish, up to the server limit" do
    {listen, port} = RawServer.listen()
    parent = self()

    # SETTINGS_MAX_CONCURRENT_STREAMS of 1. The test responds to requests.
    spawn_link(fn ->
      socket = RawServer.accept(listen, [{3, 1}])

      loop = fn loop ->
        send(parent, {:test_server, :request, socket, RawServer.receive_request(socket)})
        loop.(loop)
      end

      loop.(loop)
    end)

    # The first export is sent before the server SETTINGS arrive
    client = OTLP.new(:http, "localhost", port, retry: false)
    {:ok, client, a} = OTLP.export(client, {:traces, Requests.traces(1)})
    client = stream_for(client, 100)
    assert_received {:test_server, :request, socket, stream}

    {:ok, client, b} = OTLP.export(client, {:traces, Requests.traces(1)})
    {:ok, client, c} = OTLP.export(client, {:traces, Requests.traces(1)})
    client = stream_for(client, 100)
    refute_received {:test_server, :request, _, _}

    RawServer.respond(socket, stream)
    {client, %{^a => {:done, ^a, _}}} = await(client, a)
    assert_receive {:test_server, :request, ^socket, stream}
    refute_received {:test_server, :request, _, _}

    RawServer.respond(socket, stream)
    {client, %{^b => {:done, ^b, _}}} = await(client, b)
    assert_receive {:test_server, :request, ^socket, stream}

    RawServer.respond(socket, stream)
    {_client, %{^c => {:done, ^c, _}}} = await(client, c)
  end
end
