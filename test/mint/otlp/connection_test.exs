defmodule Mint.OTLP.ConnectionTest do
  use Mint.OTLP.Case, async: true

  test "connects on the first export" do
    port = unused_port()
    client = OTLP.new(:http, "localhost", port, retry: @retry)

    {_pid, ^port} = TestServer.start(port)
    {:ok, client, ref} = OTLP.export(client, {:traces, Requests.traces(1)})
    {_client, %{^ref => {:done, ^ref, _}}} = await(client, ref)
  end

  test "fails exports when it cannot connect" do
    retry = [initial_backoff: 10, max_backoff: 50, max_elapsed_time: 200]
    client = OTLP.new(:http, "localhost", unused_port(), retry: retry)
    {:ok, client, ref} = OTLP.export(client, {:traces, Requests.traces(1)})

    {_client, %{^ref => {:error, ^ref, %Mint.TransportError{reason: :econnrefused}}}} =
      await(client, ref)
  end

  test "fails exports after the timeout when it cannot connect and retries are disabled" do
    client = OTLP.new(:http, "localhost", unused_port(), retry: false, timeout: 100)
    {:ok, client, ref} = OTLP.export(client, {:traces, Requests.traces(1)})

    {time, {_client, responses}} = :timer.tc(fn -> await(client, ref) end)
    assert {:error, ^ref, %Mint.TransportError{reason: :econnrefused}} = responses[ref]
    assert time >= 100_000
  end

  test "reconnects until the server is up" do
    port = unused_port()
    client = OTLP.new(:http, "localhost", port, retry: [initial_backoff: 20, max_backoff: 20])
    {:ok, client, ref} = OTLP.export(client, {:traces, Requests.traces(1)})

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
    {:ok, client, ref} = OTLP.export(client, {:traces, Requests.traces(1)}, headers: headers)

    # Let the request reach the server, then drop it
    client = stream_for(client, 0)
    wait_until(fn -> TestServer.count(headers) == 1 end)
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
    {:ok, client, a} = OTLP.export(client, {:traces, Requests.traces(1)})
    client = stream_for(client, 50)

    # B is only sent after the server closed the connection
    {:ok, client, b} = OTLP.export(client, {:traces, Requests.traces(1)})
    Process.sleep(300)

    :ok = :gen_tcp.close(listen)
    wait_until_free(port)
    {_pid, ^port} = TestServer.start(port)

    {_client, responses} = await(client, [a, b])
    assert {:done, ^a, _} = responses[a]
    assert {:done, ^b, _} = responses[b]
  end

  test "exports over TLS" do
    {server_opts, client_opts} = tls_options()
    {pid, port} = TestServer.start(0, server_opts)
    retry = [initial_backoff: 50, max_backoff: 50]
    client = OTLP.new(:https, "localhost", port, transport_opts: client_opts, retry: retry)

    {:ok, client, ref} = OTLP.export(client, {:traces, Requests.traces(1)})
    {client, %{^ref => {:done, ^ref, _}}} = await(client, ref)

    # Resends exports when the connection drops
    headers = TestServer.headers("sleep:5000:ok", 1)
    {:ok, client, ref} = OTLP.export(client, {:traces, Requests.traces(1)}, headers: headers)
    client = stream_for(client, 0)
    wait_until(fn -> TestServer.count(headers) == 1 end)
    Process.exit(pid, :kill)

    wait_until_free(port)
    {_pid, ^port} = TestServer.start(port, server_opts)
    {client, %{^ref => {:done, ^ref, _}}} = await(client, ref)

    # Closing removes the messages of the socket, such as a response
    # that arrived before closing
    headers = TestServer.headers("sleep:50:ok")
    {:ok, client, ref} = OTLP.export(client, {:traces, Requests.traces(1)}, headers: headers)
    client = stream_for(client, 0)
    Process.sleep(200)
    {:responses, _client, [{:error, ^ref, _}]} = OTLP.close(client)
    refute_received {:ssl, _, _}
    refute_received {:ssl_closed, _}
  end

  test "close fails exports in flight", %{client: client} do
    headers = TestServer.headers("sleep:5000:ok")
    {:ok, client, ref} = OTLP.export(client, {:traces, Requests.traces(1)}, headers: headers)
    client = stream_for(client, 100)

    {:responses, client, responses} = OTLP.close(client)
    assert responses == [{:error, ref, %Mint.TransportError{reason: :closed}}]

    {:ok, client, ref} = OTLP.export(client, {:traces, Requests.traces(1)})
    {_client, %{^ref => {:done, ^ref, _}}} = await(client, ref)
  end

  test "close fails pending exports", %{client: client} do
    headers = TestServer.headers("sleep:5000:ok")
    {:ok, client, ref} = OTLP.export(client, {:traces, Requests.traces(1)}, headers: headers)

    {:responses, client, responses} = OTLP.close(client)
    assert responses == [{:error, ref, %Mint.TransportError{reason: :closed}}]

    # And the client can be used again
    {:ok, client, ref} = OTLP.export(client, {:traces, Requests.traces(1)})
    {_client, %{^ref => {:done, ^ref, _}}} = await(client, ref)
  end

  test "sends the user agent", %{client: client, port: port} do
    request = Requests.traces(1)
    {:ok, client, ref} = OTLP.export(client, {:traces, request}, headers: TestServer.headers())
    await(client, ref)
    assert_receive {:test_server, _, headers, _}
    assert {"user-agent", "mint-otlp/" <> _} = List.keyfind(headers, "user-agent", 0)

    client = OTLP.new(:http, "localhost", port, user_agent: "my-sdk/1.0")
    {:ok, client, ref} = OTLP.export(client, {:traces, request}, headers: TestServer.headers())
    await(client, ref)
    assert_receive {:test_server, _, headers, _}
    assert {"user-agent", "my-sdk/1.0"} in headers
  end

  test "ignores unknown messages", %{client: client} do
    assert OTLP.stream(client, :unknown) == :unknown
  end
end
