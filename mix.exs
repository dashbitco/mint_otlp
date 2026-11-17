defmodule Mint.OTLP.MixProject do
  use Mix.Project

  @version "0.1.0-dev"
  @repo_url "https://github.com/elixir-mint/mint_otlp"

  def project do
    [
      app: :mint_otlp,
      version: @version,
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env()),
      deps: deps(),

      # Hex
      package: package(),
      description: "Small and composable OpenTelemetry Protocol (OTLP) client.",

      # Docs
      name: "Mint.OTLP",
      docs: [
        source_ref: "v#{@version}",
        source_url: @repo_url
      ]
    ]
  end

  def application do
    [
      extra_applications: [:logger, :ssl]
    ]
  end

  defp package do
    [
      licenses: ["Apache-2.0"],
      links: %{"GitHub" => @repo_url}
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_env), do: ["lib"]

  defp deps do
    [
      {:mint, "~> 1.7"},

      # Test dependencies
      {:bandit, "~> 1.6", only: :test},
      {:ex_doc, "~> 0.34", only: :dev, runtime: false}
    ]
  end
end
