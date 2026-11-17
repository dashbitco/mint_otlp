defmodule Mint.OTLP.Requests do
  @moduledoc """
  Encodes minimal OTLP export requests for tests.
  """
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
    data_point = Encoder.number_data_point(System.os_time(:nanosecond), 42.0)
    metric = Encoder.gauge("mint_otlp.test", [data_point])
    {:metrics, request} = Encoder.metrics(resource(), [{scope(), [metric]}])
    to_iovec(request)
  end

  @doc """
  An `ExportLogsServiceRequest` with a log record.
  """
  def logs do
    log_record = Encoder.log_record(System.os_time(:nanosecond), "hello", severity: :info)
    {:logs, request} = Encoder.logs(resource(), [{scope(), [log_record]}])
    to_iovec(request)
  end

  defp resource, do: Encoder.resource(%{"service.name" => "mint_otlp_test"})
  defp scope, do: Encoder.scope("mint_otlp", version: "0.1.0")

  defp to_iovec(iodata), do: :erlang.iolist_to_iovec(iodata)
end
