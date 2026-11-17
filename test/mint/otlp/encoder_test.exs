defmodule Mint.OTLP.EncoderTest do
  use ExUnit.Case, async: true

  alias Mint.OTLP.Encoder

  @trace_id :binary.copy(<<1>>, 16)
  @span_id :binary.copy(<<2>>, 8)

  test "resource/1" do
    assert Encoder.resource([]) == <<0x0A, 0>>

    # attributes = 1 { key = 1, value = 2 { string_value = 1 } }
    assert Encoder.resource(%{"a" => "b"}) ==
             <<0x0A, 10, 0x0A, 8, 0x0A, 1, "a", 0x12, 3, 0x0A, 1, "b">>
  end

  test "scope/2" do
    assert Encoder.scope("lib") == <<0x0A, 5, 0x0A, 3, "lib">>
    assert Encoder.scope("lib", version: "1.0") == <<0x0A, 10, 0x0A, 3, "lib", 0x12, 3, "1.0">>
  end

  test "attribute values" do
    assert value("") == <<0x0A, 0>>
    assert value(:atom) == <<0x0A, 4, "atom">>
    assert value(true) == <<0x10, 1>>
    assert value(false) == <<0x10, 0>>
    assert value(300) == <<0x18, 0xAC, 0x02>>
    assert value(-1) == <<0x18, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01>>
    assert value(1.5) == <<0x21, 1.5::little-float-64>>
    assert value([1, "a"]) == <<0x2A, 9, 0x0A, 2, 0x18, 1, 0x0A, 3, 0x0A, 1, "a">>
    assert value(%{"k" => 1}) == <<0x32, 9, 0x0A, 7, 0x0A, 1, "k", 0x12, 2, 0x18, 1>>
  end

  # The value of the only attribute of a resource
  defp value(value) do
    <<0x0A, _, 0x0A, _, 0x0A, 1, "k", 0x12, size, value::binary-size(size)>> =
      Encoder.resource([{:k, value}])

    value
  end

  test "span/6" do
    span = Encoder.span(@trace_id, @span_id, "op", 1, 2)

    assert span ==
             <<0x12, 50, 0x0A, 16, @trace_id::binary, 0x12, 8, @span_id::binary, 0x2A, 2, "op",
               0x39, 1::little-64, 0x41, 2::little-64>>

    span =
      Encoder.span(@trace_id, @span_id, "op", 1, 2,
        parent_span_id: @span_id,
        trace_state: "a=b",
        flags: 0x301,
        kind: :client,
        status: {:error, "failed"}
      )

    assert <<0x12, _size, rest::binary>> = span
    assert rest =~ <<0x1A, 3, "a=b">>
    assert rest =~ <<0x22, 8, @span_id::binary>>
    assert rest =~ <<0x30, 3>>
    assert rest =~ <<0x7A, 10, 0x12, 6, "failed", 0x18, 2>>
    assert rest =~ <<0x85, 0x01, 0x301::little-32>>

    assert Encoder.span(@trace_id, @span_id, "op", 1, 2, status: :ok) =~ <<0x7A, 2, 0x18, 1>>
  end

  test "event/3 and link/3 are embedded in spans" do
    event = Encoder.event(1, "boom", %{"k" => "v"})
    assert <<0x5A, size, _::binary-size(size)>> = event

    link = Encoder.link(@trace_id, @span_id, trace_state: "a=b", flags: 1)
    assert <<0x6A, size, _::binary-size(size)>> = link
    assert link =~ <<0x35, 1::little-32>>

    span = Encoder.span(@trace_id, @span_id, "op", 1, 2, events: [event], links: [link])
    assert span =~ event
    assert span =~ link
  end

  test "traces/2" do
    resource = Encoder.resource([])
    scope = Encoder.scope("lib")
    span = Encoder.span(@trace_id, @span_id, "op", 1, 2)
    {:traces, request} = Encoder.traces(resource, [{scope, [span, span]}, {scope, []}])

    scope_spans = <<0x12, 7 + 2 * 52, scope::binary, span::binary, span::binary>>

    assert IO.iodata_to_binary(request) ==
             <<0x0A, 2 + byte_size(scope_spans) + 9, resource::binary, scope_spans::binary, 0x12,
               7, scope::binary>>
  end
end
