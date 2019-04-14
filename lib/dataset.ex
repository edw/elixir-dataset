defmodule Dataset do
  defstruct rows: [], labels: {}

  def new(rows \\ [], labels \\ {}) do
    %Dataset{rows: rows, labels: labels}
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
        labels: {},
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
