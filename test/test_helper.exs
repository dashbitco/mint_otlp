Application.put_env(:mint_otlp, :test_server_port, Mint.OTLP.TestServer.start())

# Integration tests run against the OpenTelemetry Collector, see docker-compose.yml:
#
#     docker compose up -d
#     mix test --include integration
#
ExUnit.start(exclude: [:integration])
