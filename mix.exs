defmodule Dataset.MixProject do
  use Mix.Project

  def project do
    [
      app: :dataset,
      version: "0.6.0",
      elixir: "~> 1.8",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: description(),
      package: package(),
      name: "Dataset",
      source_url: "https://github.com/edw/elixir-dataset"
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:ex_doc, ">= 0.0.0", only: :dev},
      {:relate, "~> 0.4.0"}
    ]
  end

  defp description() do
    "Dataset provides a simple abstraction for managing tabular sets of data."
  end

  defp package() do
    [
      licenses: ["Apache 2.0"],
      links: %{"GitHub" => "https://github.com/edw/elixir-dataset"}
    ]
  end

end
