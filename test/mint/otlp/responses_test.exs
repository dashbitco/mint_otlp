defmodule Mint.OTLP.ResponsesTest do
  use Mint.OTLP.Case, async: true

  test "partial success", %{client: client} do
    headers = TestServer.headers("partial:3:invalid span")
    {:ok, client, ref} = OTLP.export(client, {:traces, Requests.traces(5)}, headers: headers)

    {_client, %{^ref => response}} = await(client, ref)
    assert response == {:done, ref, %{rejected: 3, error_message: "invalid span"}}
  end

  test "maps HTTP statuses", %{client: client} do
    headers = TestServer.headers("http:503", 1)
    {:ok, client, ref} = OTLP.export(client, {:traces, Requests.traces(1)}, headers: headers)
    {client, %{^ref => {:done, ^ref, _}}} = await(client, ref)
    assert requests() == 2

    headers = TestServer.headers("http:401", 1)
    {:ok, client, ref} = OTLP.export(client, {:traces, Requests.traces(1)}, headers: headers)

    {_client, %{^ref => {:error, ^ref, error}}} = await(client, ref)
    assert {error.code, error.message} == {:unauthenticated, "HTTP status 401"}
  end

  test "times out exports without affecting others", %{port: port} do
    client = OTLP.new(:http, "localhost", port, timeout: 600, retry: false)
    headers = TestServer.headers("sleep:5000:ok")
    {:ok, client, slow} = OTLP.export(client, {:traces, Requests.traces(1)}, headers: headers)
    client = stream_for(client, 300)

    # The fast one shows the connection is alive and the other one is
    # still in flight when the slow one times out
    {:ok, client, fast} =
      OTLP.export(client, {:traces, Requests.traces(1)}, headers: TestServer.headers())

    headers = TestServer.headers("block:ok")
    {:ok, client, other} = OTLP.export(client, {:traces, Requests.traces(1)}, headers: headers)

    {client, responses} = await(client, [slow, fast])
    assert {:error, ^slow, %Mint.OTLP.Error{code: :deadline_exceeded}} = responses[slow]
    assert {:done, ^fast, _} = responses[fast]

    assert_receive {:test_server, :blocked, handler}
    send(handler, :continue)
    {_client, %{^other => {:done, ^other, _}}} = await(client, other)
  end

  test "retries attempts that time out", %{port: port} do
    client = OTLP.new(:http, "localhost", port, timeout: 100, retry: @retry)
    headers = TestServer.headers("sleep:2000:ok", 1)
    {:ok, client, ref} = OTLP.export(client, {:traces, Requests.traces(1)}, headers: headers)

    # Only the second attempt can finish in time
    {_client, %{^ref => {:done, ^ref, _}}} = await(client, ref, 1000)
    assert_receive {:test_server, _, headers, _}
    assert {"grpc-timeout", "100m"} in headers
  end
end
