defmodule Mint.OTLP.RawServer do
  @moduledoc """
  A scripted HTTP/2 server for tests that need frames Bandit does not
  send, such as GOAWAY with an error code, or no responses at all.
  """
  import Bitwise

  def listen do
    {:ok, listen} = :gen_tcp.listen(0, [:binary, active: false, ip: :loopback, reuseaddr: true])
    {:ok, port} = :inet.port(listen)
    {listen, port}
  end

  @doc """
  Accepts a connection and sends the given SETTINGS.
  """
  def accept(listen, settings \\ []) do
    {:ok, socket} = :gen_tcp.accept(listen)
    {:ok, "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"} = :gen_tcp.recv(socket, 24)
    send_frame(socket, 4, 0, 0, for({id, value} <- settings, do: <<id::16, value::32>>))
    socket
  end

  @doc """
  Receives frames until a request ends, returning its stream id.
  """
  def receive_request(socket) do
    case receive_frame(socket) do
      {type, flags, stream, _payload} when type in [0, 1] and (flags &&& 1) == 1 -> stream
      {_type, _flags, _stream, _payload} -> receive_request(socket)
    end
  end

  @doc """
  Receives frames until a request starts, returning its stream id,
  without waiting for its body.
  """
  def receive_stream(socket) do
    case receive_frame(socket) do
      {1, _flags, stream, _payload} -> stream
      {_type, _flags, _stream, _payload} -> receive_stream(socket)
    end
  end

  defp receive_frame(socket) do
    {:ok, <<size::24, type::8, flags::8, _::1, stream::31>>} = :gen_tcp.recv(socket, 9, 5000)
    payload = if size > 0, do: elem(:gen_tcp.recv(socket, size), 1), else: ""

    # Acknowledge SETTINGS
    if type == 4 and (flags &&& 1) == 0, do: send_frame(socket, 4, 1, 0, "")
    {type, flags, stream, payload}
  end

  @doc """
  Responds to a request with a gRPC status, without a body.
  """
  def respond(socket, stream, status \\ "0") do
    headers = [{":status", "200"}, {"content-type", "application/grpc"}, {"grpc-status", status}]

    {block, _table} =
      HPAX.encode(Enum.map(headers, &{:no_store, elem(&1, 0), elem(&1, 1)}), HPAX.new(4096))

    send_frame(socket, 1, 0x5, stream, block)
  end

  def reset(socket, stream, code) do
    send_frame(socket, 3, 0, stream, <<code::32>>)
  end

  def goaway(socket, last_stream, code) do
    send_frame(socket, 7, 0, 0, <<0::1, last_stream::31, code::32>>)
  end

  defp send_frame(socket, type, flags, stream, payload) do
    payload = IO.iodata_to_binary(payload)
    frame = <<byte_size(payload)::24, type::8, flags::8, 0::1, stream::31, payload::binary>>

    # The client may have closed the connection already
    _ = :gen_tcp.send(socket, frame)
  end

  @doc """
  Reads from the socket until the client closes it.
  """
  def await_close(socket) do
    case :gen_tcp.recv(socket, 0, 5000) do
      {:ok, _data} -> await_close(socket)
      other -> other
    end
  end
end
