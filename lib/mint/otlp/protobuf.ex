defmodule Mint.OTLP.Protobuf do
  # Just enough protobuf decoding for OTLP and gRPC responses.
  @moduledoc false

  import Bitwise

  @retry_info "type.googleapis.com/google.rpc.RetryInfo"

  @doc """
  Decodes an `Export*ServiceResponse`, which has the same shape for
  all signals, except for the name of the rejected field:

      message ExportTraceServiceResponse {
        ExportTracePartialSuccess partial_success = 1;
      }

      message ExportTracePartialSuccess {
        // rejected_data_points for metrics, rejected_log_records for logs
        int64 rejected_spans = 1;
        string error_message = 2;
      }
  """
  def decode_export_response(message) do
    case List.keyfind(decode_fields(message), 1, 0) do
      {1, partial_success} ->
        fields = decode_fields(partial_success)
        {1, rejected} = List.keyfind(fields, 1, 0, {1, 0})
        {2, error_message} = List.keyfind(fields, 2, 0, {2, ""})
        %{rejected: rejected, error_message: error_message}

      nil ->
        %{rejected: 0, error_message: ""}
    end
  end

  @doc """
  Decodes the retry delay, in milliseconds, from a `google.rpc.Status`
  with a `google.rpc.RetryInfo` in its details:

      message Status {
        int32 code = 1;
        string message = 2;
        repeated google.protobuf.Any details = 3;
      }

      message Any {
        string type_url = 1;
        bytes value = 2;
      }

      message RetryInfo {
        google.protobuf.Duration retry_delay = 1;
      }

      message Duration {
        int64 seconds = 1;
        int32 nanos = 2;
      }

  Returns `nil` if there is no `RetryInfo`.
  """
  def decode_retry_after(status) do
    Enum.find_value(decode_fields(status), fn
      {3, any} -> decode_retry_info(decode_fields(any))
      _ -> nil
    end)
  end

  defp decode_retry_info(any) do
    case List.keyfind(any, 1, 0) do
      {1, @retry_info} ->
        {2, retry_info} = List.keyfind(any, 2, 0, {2, ""})
        {1, duration} = List.keyfind(decode_fields(retry_info), 1, 0, {1, ""})
        fields = decode_fields(duration)
        {1, seconds} = List.keyfind(fields, 1, 0, {1, 0})
        {2, nanos} = List.keyfind(fields, 2, 0, {2, 0})
        seconds * 1000 + div(nanos, 1_000_000)

      _ ->
        nil
    end
  end

  # Decodes a message into a list of {field, value}, where value is an
  # integer for varint and fixed fields and a binary for length-delimited
  # fields.
  defp decode_fields(binary), do: decode_fields(binary, [])

  defp decode_fields(<<>>, acc), do: Enum.reverse(acc)

  defp decode_fields(binary, acc) do
    {key, rest} = decode_varint(binary)
    {value, rest} = decode_value(key &&& 7, rest)
    decode_fields(rest, [{key >>> 3, value} | acc])
  end

  defp decode_value(0, binary), do: decode_varint(binary)
  defp decode_value(1, <<value::little-64, rest::binary>>), do: {value, rest}
  defp decode_value(5, <<value::little-32, rest::binary>>), do: {value, rest}

  defp decode_value(2, binary) do
    {size, rest} = decode_varint(binary)
    <<value::binary-size(^size), rest::binary>> = rest
    {value, rest}
  end

  defp decode_varint(binary), do: decode_varint(binary, 0, 0)

  defp decode_varint(<<1::1, value::7, rest::binary>>, shift, acc),
    do: decode_varint(rest, shift + 7, acc ||| value <<< shift)

  defp decode_varint(<<0::1, value::7, rest::binary>>, shift, acc),
    do: {acc ||| value <<< shift, rest}
end
