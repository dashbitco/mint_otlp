defmodule Mint.OTLP.Protobuf do
  @moduledoc false

  # Just enough protobuf decoding for OTLP and gRPC responses.

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

  Returns `:error` if the message is malformed.
  """
  def decode_export_response(message) do
    with {:ok, fields} <- decode_fields(message) do
      case List.keyfind(fields, 1, 0) do
        {1, partial_success} ->
          with {:ok, fields} <- decode_fields(partial_success) do
            {1, rejected} = List.keyfind(fields, 1, 0, {1, 0})
            {2, error_message} = List.keyfind(fields, 2, 0, {2, ""})
            {:ok, %{rejected: rejected, error_message: error_message}}
          end

        nil ->
          {:ok, %{rejected: 0, error_message: ""}}
      end
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

  Returns `nil` if there is no retry delay.
  """
  def decode_retry_after(status) do
    case decode_fields(status) do
      {:ok, fields} ->
        Enum.find_value(fields, fn
          {3, any} -> decode_retry_info(any)
          _ -> nil
        end)

      :error ->
        nil
    end
  end

  defp decode_retry_info(any) do
    with {:ok, fields} <- decode_fields(any),
         {1, @retry_info} <- List.keyfind(fields, 1, 0),
         {2, retry_info} <- List.keyfind(fields, 2, 0),
         {:ok, fields} <- decode_fields(retry_info),
         {1, duration} <- List.keyfind(fields, 1, 0),
         {:ok, fields} <- decode_fields(duration) do
      {1, seconds} = List.keyfind(fields, 1, 0, {1, 0})
      {2, nanos} = List.keyfind(fields, 2, 0, {2, 0})
      seconds * 1000 + div(nanos, 1_000_000)
    else
      _ -> nil
    end
  end

  # Decodes a message into a list of {field, value}, where value is an
  # integer for varint and fixed fields and a binary for length-delimited
  # fields, or :error if the message is malformed.
  defp decode_fields(binary), do: decode_fields(binary, [])

  defp decode_fields(<<>>, acc), do: {:ok, Enum.reverse(acc)}

  defp decode_fields(binary, acc) do
    with {:ok, key, rest} <- decode_varint(binary),
         {:ok, value, rest} <- decode_value(key &&& 7, rest) do
      decode_fields(rest, [{key >>> 3, value} | acc])
    end
  end

  defp decode_value(0, binary), do: decode_varint(binary)
  defp decode_value(1, <<value::little-64, rest::binary>>), do: {:ok, value, rest}
  defp decode_value(5, <<value::little-32, rest::binary>>), do: {:ok, value, rest}

  defp decode_value(2, binary) do
    with {:ok, size, rest} <- decode_varint(binary),
         <<value::binary-size(^size), rest::binary>> <- rest do
      {:ok, value, rest}
    else
      _ -> :error
    end
  end

  defp decode_value(_type, _binary), do: :error

  defp decode_varint(binary), do: decode_varint(binary, 0, 0)

  defp decode_varint(<<1::1, value::7, rest::binary>>, shift, acc) when shift < 64,
    do: decode_varint(rest, shift + 7, acc ||| value <<< shift)

  defp decode_varint(<<0::1, value::7, rest::binary>>, shift, acc),
    do: {:ok, acc ||| value <<< shift, rest}

  defp decode_varint(_binary, _shift, _acc), do: :error
end
