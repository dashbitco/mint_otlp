defmodule Mint.OTLP.RetriesTest do
  use Mint.OTLP.Case, async: true

  test "retries retryable statuses", %{client: client} do
    headers = TestServer.headers("status:14:try again, later", 2)
    {:ok, client, ref} = OTLP.export(client, {:traces, Requests.traces(1)}, headers: headers)

    {_client, %{^ref => {:done, ^ref, _}}} = await(client, ref)
    assert requests() == 3
  end

  test "gives up after the maximum elapsed time", %{port: port} do
    retry = [initial_backoff: 10, max_backoff: 50, max_elapsed_time: 300]
    client = OTLP.new(:http, "localhost", port, retry: retry)
    headers = TestServer.headers("status:14:giving up")
    {:ok, client, ref} = OTLP.export(client, {:traces, Requests.traces(1)}, headers: headers)

    # Logs from other async tests may be captured too
    {{_client, %{^ref => {:error, ^ref, error}}}, log} =
      ExUnit.CaptureLog.with_log(fn -> await(client, ref) end)

    assert log =~
             ~r"\[info\] Backing off OTLP exports for \d+ms after: OTLP export failed with unavailable: giving up"

    assert log =~ "[error] Discarding OTLP export: OTLP export failed with unavailable: giving up"
    assert %Mint.OTLP.Error{transport: :grpc, code: :unavailable} = error
    assert error.message == "giving up"
    assert Exception.message(error) == "OTLP export failed with unavailable: giving up"
  end

  test "does not retry when disabled", %{port: port} do
    client = OTLP.new(:http, "localhost", port, retry: false)
    headers = TestServer.headers("status:14:try again, later", 1)
    {:ok, client, ref} = OTLP.export(client, {:traces, Requests.traces(1)}, headers: headers)

    {_client, %{^ref => {:error, ^ref, %{code: :unavailable}}}} = await(client, ref)
    assert requests() == 1
  end

  test "does not retry other statuses", %{client: client} do
    headers = TestServer.headers("status:3:bad request", 1)
    {:ok, client, ref} = OTLP.export(client, {:traces, Requests.traces(1)}, headers: headers)

    {_client, %{^ref => {:error, ^ref, error}}} = await(client, ref)
    assert {error.code, error.message} == {:invalid_argument, "bad request"}
    assert requests() == 1
  end

  test "backs off the whole client on retryable errors", %{port: port} do
    # Between 100 and 200ms
    client = OTLP.new(:http, "localhost", port, retry: [initial_backoff: 200, max_backoff: 200])
    a_headers = TestServer.headers("status:14:try again, later", 1)
    {:ok, client, a} = OTLP.export(client, {:traces, Requests.traces(1)}, headers: a_headers)
    client = stream_while(client, &(&1.backoff_until == nil))
    a_id = List.keyfind(a_headers, "x-test-id", 0)
    assert_received {:test_server, _, headers, _}
    assert List.keyfind(headers, "x-test-id", 0) == a_id

    # B is exported after A failed, so it waits for the backoff too
    b_headers = TestServer.headers()
    {:ok, client, b} = OTLP.export(client, {:traces, Requests.traces(1)}, headers: b_headers)
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
    retry = [initial_backoff: 20, max_backoff: 2000, max_elapsed_time: 400]
    client = OTLP.new(:http, "localhost", port, retry: retry)
    headers = TestServer.headers("status:14:try again, later")
    {:ok, client, ref} = OTLP.export(client, {:traces, Requests.traces(1)}, headers: headers)

    {_client, %{^ref => {:error, ^ref, %{code: :unavailable}}}} = await(client, ref, 2000)

    # Even with the shortest jitter, the delays 10, 15, 22, 34, 51, 76,
    # 114, and 171ms add up to almost 500ms, while 20ms each would be 40
    assert requests() <= 9
  end

  test "retries resource exhausted only with a retry delay", %{client: client} do
    headers = TestServer.headers("retry:8:200", 1)
    {:ok, client, ref} = OTLP.export(client, {:traces, Requests.traces(1)}, headers: headers)

    {time, {client, %{^ref => {:done, ^ref, _}}}} = :timer.tc(fn -> await(client, ref) end)
    assert time >= 200_000
    assert requests() == 2

    headers = TestServer.headers("status:8:slow down", 1)
    {:ok, client, ref} = OTLP.export(client, {:traces, Requests.traces(1)}, headers: headers)

    {_client, %{^ref => {:error, ^ref, error}}} = await(client, ref)

    assert {error.code, error.message, error.retry_after} ==
             {:resource_exhausted, "slow down", nil}

    assert requests() == 1
  end
end
