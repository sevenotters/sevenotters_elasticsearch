defmodule SevenottersElasticsearch.Storage do
  @moduledoc false

  @behaviour SevenottersPersistence.Storage

  def start_link(_opts \\ []) do
    # Mongo.start_link(opts ++ [name: __MODULE__, pool_size: @pool_size])
  end

  @spec insert(String.t(), Map.t()) :: any
  def insert(_collection, _value) do
    # {:ok, _id} = Mongo.insert_one(__MODULE__, collection, value)
  end

  @spec new_id :: any
  def new_id, do: nil #Mongo.object_id()

  @spec printable_id(any) :: String.t()
  # def printable_id(%BSON.ObjectId{} = id), do: BSON.ObjectId.encode!(id)
  def printable_id(id) when is_bitstring(id), do: id

  @spec object_id(String.t()) :: any
  def object_id(_id) do
    # {_, bin} = Base.decode16(id, case: :mixed)
    # %BSON.ObjectId{value: bin}
  end

  @spec is_valid_id?(any) :: Boolean.t()
  def is_valid_id?(_id), do: true
    # do: Regex.match?(@bson_value_format, BSON.ObjectId.encode!(id))

  @spec max_in_collection(String.t(), atom) :: Int.t()
  def max_in_collection(_collection, _field) do
    # Mongo.find(
    #   __MODULE__,
    #   collection,
    #   %{},
    #   sort: %{field => -1},
    #   limit: 1
    # )
    # |> Enum.to_list()
    # |> calculate_max(Atom.to_string(field))
  end

  @spec content_of(String.t(), Map.t(), Map.t()) :: List.t()
  def content_of(_collection, _filter, _sort) do
    # Mongo.find(__MODULE__, collection, filter, sort: sort)
    # |> Enum.to_list()
  end

  @spec drop_collections(List.t()) :: any
  def drop_collections(_collections) do
    # collections
    # |> Enum.each(fn c ->
    #   Mongo.command(__MODULE__, %{:drop => c}, pool: DBConnection.Poolboy)
    # end)
  end

  @spec sort_expression() :: any
  def sort_expression(), do: %{counter: 1}

  @spec type_expression([String.t()]) :: any
  def type_expression(types), do: %{type: %{"$in" => types}}

  # @spec calculate_max(List.t(), String.t()) :: Int.t()
  # defp calculate_max([], _field), do: 0
  # defp calculate_max([e], field), do: e[field]
end
