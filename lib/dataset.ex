defmodule Dataset do
  defstruct rows: [], labels: {}

  @moduledoc ~S"""

  Datasets represent labeled tabular data.

  Datasets are enumerable:

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

  @doc """

  Return a dataset with no rows and labels specified by the tuple
  passed as `label`. If label is not specified, return an empty
  dataset with zero columns.

  """

  def empty(labels \\ nil) do
    new([], labels)
  end

  defp default_labels([]), do: {}

  defp default_labels([h | _t]) do
    exc_range(tuple_size(h)) |> Enum.to_list() |> List.to_tuple()
  end

  def exc_range(base \\ 0, count),
    do: Stream.drop((base - 1)..(base + count - 1), 1)

  @doc """

  Return the contents of `_ds` as a list of maps.

  """

  def to_map_list(_ds = %Dataset{rows: rows, labels: labels}) do
    l_list = Tuple.to_list(labels)

    for row <- rows do
      for k <- l_list,
          v <- Tuple.to_list(row),
          into: %{} do
        {k, v}
      end
    end
  end

  @doc """

  Return the elements in `row` tuple as a map with keys matching the
  labels of `_ds`.

  """

  def row_to_map(row, _ds = %Dataset{labels: labels})
      when is_tuple(row) do
    for k <- Tuple.to_list(labels),
        v <- Tuple.to_list(row),
        into: %{} do
      {k, v}
    end
  end

  @doc ~S"""

  Returns a dataset with each value in row _i_ and column _j_
  transposed into row _j_ and column _i_. The dataset is labelled with
  integer indicies beginning with zero.

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

  @doc ~S"""

  Return the result of performing an inner join on datasets `ds1` and
  `ds2`, using `k1` and `k2` as the key labels on each respective
  dataset. The returned dataset will contain columns for each label
  specified in `out_labels`, which is a keyword list of the form
  `[left_or_right: label, ...]`.

      iex> iso_countries =
      ...>   Dataset.new(
      ...>     [
      ...>       {"us", "United States"},
      ...>       {"uk", "United Kingdom"},
      ...>       {"ca", "Canada"},
      ...>       {"de", "Germany"},
      ...>       {"nl", "Netherlands"},
      ...>       {"sg", "Singapore"}
      ...>     ],
      ...>     {:iso_country, :country_name}
      ...>   )
      ...>
      ...> country_clicks =
      ...>   Dataset.new(
      ...>     [
      ...>       {"United States", "13"},
      ...>       {"United Kingdom", "11"},
      ...>       {"Canada", "4"},
      ...>       {"Germany", "4"},
      ...>       {"France", "2"}
      ...>     ],
      ...>     {:country_name, :clicks}
      ...>   )
      ...>
      ...> Dataset.inner_join(country_clicks, iso_countries, :country_name,
      ...>   right: :iso_country,
      ...>   left: :clicks
      ...> )
      %Dataset{
        labels: {:iso_country, :clicks},
        rows: [{"ca", "4"}, {"de", "4"}, {"uk", "11"}, {"us", "13"}]
      }

  """

  def inner_join(ds1, ds2, k1, k2 \\ nil, out_labels),
    do:
      perform_join(
        &Relate.inner_join/4,
        ds1,
        ds2,
        k1,
        k2,
        out_labels
      )

  @doc ~S"""

  Return the result of performing an outer join on datasets `ds1` and
  `ds2`, using `k1` and `k2` as the key labels on each respective
  dataset. The returned dataset will contain columns for each label
  specified in `out_labels`, which is a keyword list of the form
  `[left_or_right: label, ...]`.

      iex> iso_countries =
      ...>   Dataset.new(
      ...>     [
      ...>       {"us", "United States"},
      ...>       {"uk", "United Kingdom"},
      ...>       {"ca", "Canada"},
      ...>       {"de", "Germany"},
      ...>       {"nl", "Netherlands"},
      ...>       {"sg", "Singapore"}
      ...>     ],
      ...>     {:iso_country, :country_name}
      ...>   )
      ...>
      ...> country_clicks =
      ...>   Dataset.new(
      ...>     [
      ...>       {"United States", "13"},
      ...>       {"United Kingdom", "11"},
      ...>       {"Canada", "4"},
      ...>       {"Germany", "4"},
      ...>       {"France", "2"}
      ...>     ],
      ...>     {:country_name, :clicks}
      ...>   )
      ...>
      ...>  Dataset.outer_join(country_clicks, iso_countries, :country_name,
      ...>    right: :iso_country,
      ...>    left: :clicks
      ...>  )
      %Dataset{
        labels: {:iso_country, :clicks},
        rows: [
          {"ca", "4"},
          {nil, "2"},
          {"de", "4"},
          {"nl", nil},
          {"sg", nil},
          {"uk", "11"},
          {"us", "13"}
        ]
      }

  """

  def outer_join(ds1, ds2, k1, k2 \\ nil, out_labels),
    do:
      perform_join(
        &Relate.outer_join/4,
        ds1,
        ds2,
        k1,
        k2,
        out_labels
      )

  @doc ~S"""

  Return the result of performing a left join on datasets `ds1` and
  `ds2`, using `k1` and `k2` as the key labels on each respective
  dataset. The returned dataset will contain columns for each label
  specified in `out_labels`, which is a keyword list of the form
  `[left_or_right: label, ...]`.

      iex> iso_countries =
      ...>   Dataset.new(
      ...>     [
      ...>       {"us", "United States"},
      ...>       {"uk", "United Kingdom"},
      ...>       {"ca", "Canada"},
      ...>       {"de", "Germany"},
      ...>       {"nl", "Netherlands"},
      ...>       {"sg", "Singapore"}
      ...>     ],
      ...>     {:iso_country, :country_name}
      ...>   )
      ...>
      ...> country_clicks =
      ...>   Dataset.new(
      ...>     [
      ...>       {"United States", "13"},
      ...>       {"United Kingdom", "11"},
      ...>       {"Canada", "4"},
      ...>       {"Germany", "4"},
      ...>       {"France", "2"}
      ...>     ],
      ...>     {:country_name, :clicks}
      ...>   )
      ...>
      ...>  Dataset.left_join(country_clicks, iso_countries, :country_name,
      ...>    right: :iso_country,
      ...>    left: :clicks
      ...>  )
      %Dataset{
        labels: {:iso_country, :clicks},
        rows: [{"ca", "4"}, {nil, "2"}, {"de", "4"}, {"uk", "11"}, {"us", "13"}]
      }

  """

  def left_join(ds1, ds2, k1, k2 \\ nil, out_labels),
    do:
      perform_join(
        &Relate.left_join/4,
        ds1,
        ds2,
        k1,
        k2,
        out_labels
      )

  @doc ~S"""

  Return the result of performing a right join on datasets `ds1` and
  `ds2`, using `k1` and `k2` as the key labels on each respective
  dataset. The returned dataset will contain columns for each label
  specified in `out_labels`, which is a keyword list of the form
  `[left_or_right: label, ...]`.

      iex> iso_countries =
      ...>   Dataset.new(
      ...>     [
      ...>       {"us", "United States"},
      ...>       {"uk", "United Kingdom"},
      ...>       {"ca", "Canada"},
      ...>       {"de", "Germany"},
      ...>       {"nl", "Netherlands"},
      ...>       {"sg", "Singapore"}
      ...>     ],
      ...>     {:iso_country, :country_name}
      ...>   )
      ...>
      ...> country_clicks =
      ...>   Dataset.new(
      ...>     [
      ...>       {"United States", "13"},
      ...>       {"United Kingdom", "11"},
      ...>       {"Canada", "4"},
      ...>       {"Germany", "4"},
      ...>       {"France", "2"}
      ...>     ],
      ...>     {:country_name, :clicks}
      ...>   )
      ...>
      ...>  Dataset.right_join(country_clicks, iso_countries, :country_name,
      ...>    right: :iso_country,
      ...>    left: :clicks
      ...>  )
      %Dataset{
        labels: {:iso_country, :clicks},
        rows: [
          {"ca", "4"},
          {"de", "4"},
          {"nl", nil},
          {"sg", nil},
          {"uk", "11"},
          {"us", "13"}
        ]
      }

  """

  def right_join(ds1, ds2, k1, k2 \\ nil, out_labels),
    do:
      perform_join(
        &Relate.right_join/4,
        ds1,
        ds2,
        k1,
        k2,
        out_labels
      )

  @doc ~S"""

  Return a new dataset with columns chosen from the input dataset `ds`.

      iex> Dataset.new([{:a,:b,:c},
      ...>              {:A, :B, :C},
      ...>              {:i, :ii, :iii},
      ...>              {:I, :II, :III}],
      ...>             {"first", "second", "third"})
      ...> |> Dataset.select(["second"])
      %Dataset{rows: [{:b}, {:B}, {:ii}, {:II}], labels: {"second"}}

  """

  def select(_ds = %Dataset{rows: rows, labels: labels}, out_labels) do
    columns =
      for l <- out_labels do
        {:left, label_index(labels, l)}
      end

    for row <- rows do
      {row, {}}
    end
    |> Relate.select(columns)
    |> Dataset.new(List.to_tuple(out_labels))
  end

  @doc ~S"""

  Return a tuple of lists containing columnar data from `ds`, one list
  for each passed element of the `column_labels` list. Lists are
  returned in the tuple in the same order in which they appear in
  `column_labels`. Labels may appear more than once.

      iex> iso_countries = %Dataset{
      ...>   labels: {:iso_country, :country_name},
      ...>   rows: [
      ...>     {"us", "United States"},
      ...>     {"uk", "United Kingdom"},
      ...>     {"ca", "Canada"},
      ...>     {"de", "Germany"},
      ...>     {"nl", "Netherlands"},
      ...>     {"sg", "Singapore"}
      ...>   ]
      ...> }
      ...>  Dataset.columns(iso_countries, [:iso_country, :iso_country])
      {["us", "uk", "ca", "de", "nl", "sg"],
       ["us", "uk", "ca", "de", "nl", "sg"]}

  """

  def columns(_ds = %Dataset{}, []), do: {}

  def columns(ds = %Dataset{}, column_labels)
      when is_list(column_labels) do
    rotated = Enum.to_list(Dataset.rotate(ds)) |> List.to_tuple()

    column_set =
      for l <- column_labels do
        label_index(List.to_tuple(column_labels), l)
      end

    for c <- column_set do
      Tuple.to_list(elem(rotated, c))
    end
    |> List.to_tuple()
  end

  defp perform_join(
         join_func,
         %Dataset{rows: rows1, labels: labels1},
         %Dataset{rows: rows2, labels: labels2},
         k1,
         k2,
         out_labels
       ) do
    kf1 = key_func(labels1, k1)
    kf2 = key_func(labels2, k2 || k1)

    select_columns =
      Enum.map(out_labels, fn
        {:left, label} -> {:left, label_index(labels1, label)}
        {:right, label} -> {:right, label_index(labels2, label)}
      end)

    new_labels =
      Enum.map(out_labels, fn {_, label} -> label end)
      |> List.to_tuple()

    join_func.(rows1, rows2, kf1, kf2)
    |> Relate.select(select_columns)
    |> Dataset.new(new_labels)
  end

  defp key_func(labels, k) do
    i = label_index(labels, k)
    fn t -> elem(t, i) end
  end

  defp label_index(labels, k),
    do: labels |> Tuple.to_list() |> Enum.find_index(&(&1 == k))
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
