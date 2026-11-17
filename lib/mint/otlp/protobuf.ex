defmodule Mint.OTLP.Protobuf do
  @moduledoc false

  # Just enough protobuf decoding for OTLP and gRPC responses.

  import Bitwise

  @doc """
  Decodes a message into a list of `{field, value}`, where `value` is an
  integer for varint and fixed fields and a binary for length-delimited
  fields. Returns `:error` if the message is malformed.
  """
  def decode(binary), do: decode(binary, [])

  defp decode(<<>>, acc), do: {:ok, Enum.reverse(acc)}

  defp decode(binary, acc) do
    with {:ok, key, rest} <- decode_varint(binary),
         {:ok, value, rest} <- decode_value(key &&& 7, rest) do
      decode(rest, [{key >>> 3, value} | acc])
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

  @doc """
  Decodes a varint.
  """
  def decode_varint(binary), do: decode_varint(binary, 0, 0)

  defp decode_varint(<<1::1, value::7, rest::binary>>, shift, acc) when shift < 64,
    do: decode_varint(rest, shift + 7, acc ||| value <<< shift)

  defp decode_varint(<<0::1, value::7, rest::binary>>, shift, acc),
    do: {:ok, acc ||| value <<< shift, rest}

  defp decode_varint(_binary, _shift, _acc), do: :error
end
