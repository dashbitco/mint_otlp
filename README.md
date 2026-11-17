# Mint.OTLP

Small and composable [OpenTelemetry Protocol (OTLP)](https://opentelemetry.io/docs/specs/otlp/) client, built on top of [Mint](https://github.com/elixir-mint/mint).

Like Mint, it is processless: a client is a data structure owned by a process, which gives the messages it receives to `Mint.OTLP.stream/2`. It does not encode telemetry, it exports requests already encoded as protobuf. It connects lazily, retries failed exports, and reconnects, as defined by the OTLP specification.

```elixir
client = Mint.OTLP.new(:http, "localhost", 4317)
{:ok, client, ref} = Mint.OTLP.export(client, :traces, encoded_request)

# In a loop, until responses has {:done, ^ref, %{rejected: 0, error_message: ""}}
receive do
  message ->
    {:responses, client, responses} = Mint.OTLP.stream(client, message)
end
```

Currently, only OTLP/gRPC is supported.

## Tests

    mix test

Integration tests run against the OpenTelemetry Collector:

    docker compose up -d
    mix test --include integration

## License

Copyright 2026 Dashbit

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
