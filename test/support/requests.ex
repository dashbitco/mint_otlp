defmodule Mint.OTLP.Requests do
  @moduledoc """
  Encodes minimal OTLP export requests for tests.
  """
  import Bitwise

  alias Mint.OTLP.Encoder

  @doc """
  An `ExportTraceServiceRequest` with `count` spans, as a list of binaries.
  """
  def traces(count, opts \\ []) do
    name = Keyword.get(opts, :name, "GET /users/:id")
    now = System.os_time(:nanosecond)
    attributes = [{"http.route", "/users/:id"}, {"http.response.status_code", 200}]

    spans =
      for _ <- 1..count//1 do
        trace_id = :rand.bytes(16)
        span_id = :rand.bytes(8)
        Encoder.span(trace_id, span_id, name, now - 1_000_000, now, attributes: attributes)
      end

    {:traces, request} = Encoder.traces(resource(), [{scope(), spans}])
    to_iovec(request)
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

  defp resource, do: Encoder.resource(%{"service.name" => "mint_otlp_test"})
  defp scope, do: Encoder.scope("mint_otlp", version: "0.1.0")

  defp to_iovec(iodata), do: :erlang.iolist_to_iovec(iodata)

  defp field(number, iodata),
    do: [varint(number <<< 3 ||| 2), varint(IO.iodata_length(iodata)), iodata]

  defp string(number, binary), do: field(number, binary)

  defp varint(n) when n < 128, do: <<n>>
  defp varint(n), do: <<1::1, n &&& 127::7, varint(n >>> 7)::binary>>
end
