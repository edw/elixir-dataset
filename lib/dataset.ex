defmodule Dataset do
  defstruct rows: [], labels: {}

  @moduledoc ~S"""

  Datasets represent labeled tabular data.   Datasets are enumerable:

      iex> Dataset.new([{:a, :b, :c},
      ...>              {:A, :B, :C},
      ...>              {:i, :ii, :iii},
      ...>              {:I, :II, :III}],
      ...>             {"one", "two", "three"})
      ...> |> Enum.map(&elem(&1, 2))
      [:c, :C, :iii, :III]

  Datasets are also collectable:

      iex> for x <- 0..10, into: Dataset.empty({:n}), do: x
      %Dataset{labels: {:n}, rows: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]}

  """

  @doc ~S"""

  Construct a new dataset. A dataset is a list of tuples. With no
  arguments, an empty dataset with zero columns is constructed. Withf
  one argument a dataset is constructed with the passed object
  interpreted as rows and labels beginning with `0` are generated, the
  number of which are determined by size of the first tuple in the
  data.


      iex> Dataset.new()
      %Dataset{rows: [], labels: {}}

      iex> Dataset.new([{:foo, :bar}, {:eggs, :ham}])
      %Dataset{rows: [foo: :bar, eggs: :ham], labels: {0, 1}}

      iex> Dataset.new([{0,0}, {1, 1}, {2, 4}, {3, 9}],
      ...>             {:x, :x_squared})
      %Dataset{labels: {:x, :x_squared}, rows: [{0, 0}, {1, 1}, {2, 4}, {3, 9}]}

  """

  def new(rows \\ [], labels \\ nil)

  def new(rows, nil) do
    labels = default_labels(rows)
    new(rows, labels)
  end

  def new(rows, labels) do
    %Dataset{rows: rows, labels: labels}
  end

  def empty(labels \\ nil) do
    new([], labels)
  end

  defp default_labels([]), do: {}

  defp default_labels([h | _t]) do
    0..(tuple_size(h) - 1) |> Enum.to_list() |> List.to_tuple()
  end

  def to_map_list(_ds = %Dataset{rows: rows, labels: labels}) do
    for row <- rows do
      Map.new(tuple_zip(labels, row))
    end
  end

  defp tuple_zip(t1, t2) do
    for i <- 0..(tuple_size(t1) - 1) do
      {elem(t1, i), elem(t2, i)}
    end
  end

  @doc ~S"""

      iex> Dataset.new([{:a,:b,:c},
      ...>              {:A, :B, :C},
      ...>              {:i, :ii, :iii},
      ...>              {:I, :II, :III}])
      ...> |> Dataset.rotate()
      %Dataset{
        labels: {0, 1, 2, 3},
        rows: [{:a, :A, :i, :I},
               {:b, :B, :ii, :II},
               {:c, :C, :iii, :III}]
      }

  """
  def rotate(%Dataset{rows: in_rows}) do
    for i <- 0..(tuple_size(List.first(in_rows)) - 1) do
      List.to_tuple(for in_row <- in_rows, do: elem(in_row, i))
    end
    |> Dataset.new()
  end
end

defimpl Enumerable, for: Dataset do
  def reduce(_list, {:halt, acc}, _fun), do: {:halted, acc}

  def reduce(ds = %Dataset{}, {:suspend, acc}, fun) do
    {:suspended, acc, &reduce(ds, &1, fun)}
  end

  def reduce(%Dataset{rows: []}, {:cont, acc}, _fun) do
    {:done, acc}
  end

  def reduce(%Dataset{rows: [h | t], labels: ls}, {:cont, acc}, fun) do
    reduce(%Dataset{rows: t, labels: ls}, fun.(h, acc), fun)
  end

  def count(%Dataset{rows: []}), do: {:ok, 0}
  def count(%Dataset{rows: rows}), do: {:ok, Enum.count(rows)}

  def member?(_ds = %Dataset{}, _el), do: {:error, __MODULE__}
  def slice(_ds = %Dataset{}), do: {:error, __MODULE__}
end

defimpl Collectable, for: Dataset do
  def into(o) do
    collector = fn
      %Dataset{rows: rows, labels: labels}, {:cont, el} ->
        Dataset.new([el | rows], labels)

      %Dataset{rows: rows, labels: labels}, :done ->
        %Dataset{rows: Enum.reverse(rows), labels: labels}

      _set, :halt ->
        :ok
    end

    {o, collector}
  end
end
