defmodule Mint.OTLP.TestServer do
  @moduledoc """
  An OTLP/gRPC server for tests, on top of Bandit with HTTP/2 prior knowledge.

  Every request is sent to the test process given in the `x-test-pid`
  header as `{:test_server, path, headers, body}`. The `x-test` header
  chooses the response:

    * `ok` (default) - an empty `Export*ServiceResponse`
    * `partial:REJECTED:MESSAGE` - a partial success
    * `status:CODE:MESSAGE` - a gRPC status
    * `retry:CODE:MILLISECONDS` - a gRPC status with a `RetryInfo`
    * `http:STATUS` - an HTTP status without gRPC status
    * `sleep:MILLISECONDS:RESPONSE` - waits before reading the body,
      so flow control windows are not refilled meanwhile

  When `headers/2` is given a number of times, the response is only
  used that many times for the export, and `ok` afterwards.

  Plug does not support trailers, so the gRPC status is always sent in
  the headers, as in gRPC "Trailers-Only" responses.
  """
  @behaviour Plug

  import Bitwise

  @doc """
  Starts a server and returns its pid and port.

  `opts` are given to `Bandit.start_link/1`.
  """
  def start(port \\ 0, opts \\ []) do
    # Counts requests per export, owned by the first caller, test_helper.exs
    if :ets.whereis(__MODULE__) == :undefined do
      :ets.new(__MODULE__, [:named_table, :public, :set])
    end

    {:ok, pid} =
      Bandit.start_link([plug: __MODULE__, port: port, ip: :loopback, startup_log: false] ++ opts)

    Process.unlink(pid)
    {:ok, {_ip, port}} = ThousandIsland.listener_info(pid)
    {pid, port}
  end

  @doc """
  Headers that make the server send requests to the calling process
  and respond with `response`, at most `times` for this export.
  """
  def headers(response \\ "ok", times \\ :infinity) do
    pid = self() |> :erlang.term_to_binary() |> Base.encode64()
    id = make_ref() |> :erlang.term_to_binary() |> Base.encode64()
    [{"x-test-pid", pid}, {"x-test", response}, {"x-test-id", id}, {"x-test-times", "#{times}"}]
  end

  @impl true
  def init(opts), do: opts

  @impl true
  def call(conn, _opts) do
    response =
      case Plug.Conn.get_req_header(conn, "x-test") do
        [response] -> limit(conn, response)
        [] -> "ok"
      end

    response =
      case String.split(response, ":", parts: 3) do
        ["sleep", ms, response] ->
          Process.sleep(String.to_integer(ms))
          response

        _ ->
          response
      end

    {body, conn} = read_body(conn, [])

    with [pid] <- Plug.Conn.get_req_header(conn, "x-test-pid") do
      pid = pid |> Base.decode64!() |> :erlang.binary_to_term()
      send(pid, {:test_server, conn.request_path, conn.req_headers, body})
    end

    respond(conn, String.split(response, ":", parts: 3))
  end

  defp limit(conn, response) do
    [id] = Plug.Conn.get_req_header(conn, "x-test-id")

    case Plug.Conn.get_req_header(conn, "x-test-times") do
      ["infinity"] ->
        response

      [times] ->
        count = :ets.update_counter(__MODULE__, id, 1, {id, 0})
        if count > String.to_integer(times), do: "ok", else: response
    end
  end

  defp read_body(conn, acc) do
    case Plug.Conn.read_body(conn, length: 64_000_000) do
      {:ok, data, conn} -> {IO.iodata_to_binary([acc | data]), conn}
      {:more, data, conn} -> read_body(conn, [acc | data])
    end
  end

  defp respond(conn, ["ok"]), do: grpc(conn, "0", [], <<0, 0::32>>)

  defp respond(conn, ["partial", rejected, message]) do
    partial = [0x08, varint(String.to_integer(rejected)), field(2, message)]
    response = IO.iodata_to_binary(field(1, partial))
    grpc(conn, "0", [], <<0, byte_size(response)::32, response::binary>>)
  end

  defp respond(conn, ["status", code, message]) do
    grpc(conn, code, [{"grpc-message", URI.encode(message)}], "")
  end

  defp respond(conn, ["retry", code, ms]) do
    ms = String.to_integer(ms)
    duration = [0x08, varint(div(ms, 1000)), 0x10, varint(rem(ms, 1000) * 1_000_000)]
    retry_info = field(1, duration)
    any = [field(1, "type.googleapis.com/google.rpc.RetryInfo"), field(2, retry_info)]
    status = [0x08, varint(String.to_integer(code)), field(3, any)]
    details = status |> IO.iodata_to_binary() |> Base.encode64(padding: false)
    grpc(conn, code, [{"grpc-status-details-bin", details}], "")
  end

  defp respond(conn, ["http", status]) do
    Plug.Conn.send_resp(conn, String.to_integer(status), "")
  end

  defp grpc(conn, status, headers, body) do
    conn
    |> Plug.Conn.put_resp_header("content-type", "application/grpc")
    |> Plug.Conn.put_resp_header("grpc-status", status)
    |> Plug.Conn.merge_resp_headers(headers)
    |> Plug.Conn.send_resp(200, body)
  end

  defp field(number, iodata),
    do: [varint(number <<< 3 ||| 2), varint(IO.iodata_length(iodata)), iodata]

  defp varint(n) when n < 128, do: <<n>>
  defp varint(n), do: <<1::1, n &&& 127::7, varint(n >>> 7)::binary>>
end
