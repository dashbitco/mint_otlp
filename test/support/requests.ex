defmodule Mint.OTLP.Requests do
  @moduledoc """
  Encodes minimal OTLP export requests for tests.
  """
  import Bitwise

  @doc """
  An `ExportTraceServiceRequest` with `count` spans, as a list of binaries.
  """
  def traces(count, opts \\ []) do
    name = Keyword.get(opts, :name, "GET /users/:id")
    spans = for _ <- 1..count//1, do: field(2, span(name))
    [field(1, [resource(), field(2, [scope(), spans])])] |> to_iovec()
  end

  @doc """
  An `ExportMetricsServiceRequest` with a gauge.
  """
  def metrics do
    now = System.os_time(:nanosecond)
    data_point = [<<0x19, now::little-64>>, <<0x31, 42::little-64>>]
    metric = [string(1, "mint_otlp.test"), field(5, field(1, data_point))]
    [field(1, [resource(), field(2, [scope(), field(2, metric)])])] |> to_iovec()
  end

  @doc """
  An `ExportLogsServiceRequest` with a log record.
  """
  def logs do
    now = System.os_time(:nanosecond)
    record = [<<0x09, now::little-64>>, <<0x10, 9>>, field(5, string(1, "hello"))]
    [field(1, [resource(), field(2, [scope(), field(2, record)])])] |> to_iovec()
  end

  defp span(name) do
    now = System.os_time(:nanosecond)

    [
      bytes(1, :rand.bytes(16)),
      bytes(2, :rand.bytes(8)),
      string(5, name),
      <<0x30, 2>>,
      <<0x39, now - 1_000_000::little-64, 0x41, now::little-64>>,
      field(9, [string(1, "http.route"), field(2, string(1, "/users/:id"))]),
      field(9, [string(1, "http.response.status_code"), field(2, [0x18, varint(200)])])
    ]
  end

  defp resource do
    field(1, field(1, [string(1, "service.name"), field(2, string(1, "mint_otlp_test"))]))
  end

  defp scope, do: field(1, [string(1, "mint_otlp"), string(2, "0.1.0")])

  defp to_iovec(iodata), do: :erlang.iolist_to_iovec(iodata)

  defp field(number, iodata),
    do: [varint(number <<< 3 ||| 2), varint(IO.iodata_length(iodata)), iodata]

  defp bytes(number, binary), do: field(number, binary)
  defp string(number, binary), do: field(number, binary)

  defp varint(n) when n < 128, do: <<n>>
  defp varint(n), do: <<1::1, n &&& 127::7, varint(n >>> 7)::binary>>
end
