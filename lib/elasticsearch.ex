defmodule SevenottersElasticsearch.Storage do
  @moduledoc false

  use GenServer
  require Logger

  @behaviour SevenottersPersistence.Storage

  @type_name "_doc"
  @id_regex ~r/^[A-Fa-f0-9\-]{24}$/

  defstruct url: nil

  @spec start_link(keyword) :: :ignore | {:error, any} | {:ok, pid}
  def start_link(opts \\ []) do
    url = Keyword.get(opts, :url) || raise "missing elastic url"

    port =
      case Keyword.get(opts, :port) do
        nil -> nil
        port -> ":#{port}"
      end

    url = "#{url}#{port}"
    Logger.info("Elasticsearch url: #{url}")

    GenServer.start_link(__MODULE__, %{url: url}, name: __MODULE__)
  end

  @spec init(any) :: {:ok, any}
  def init(state), do: {:ok, state}

  #
  # API
  #

  @spec initialize(String.t()) :: any
  def initialize(collection), do: GenServer.call(__MODULE__, {:initialize, collection})

  @spec insert(String.t(), Map.t()) :: any
  def insert(collection, value), do: GenServer.cast(__MODULE__, {:insert, [collection, value]})

  @spec new_id :: any
  def new_id, do: UUID.uuid4(:hex)

  @spec printable_id(any) :: String.t()
  def printable_id(id) when is_bitstring(id), do: id

  @spec object_id(String.t()) :: any
  def object_id(id), do: id

  @spec is_valid_id?(any) :: Boolean.t()
  def is_valid_id?(id) when is_bitstring(id), do: Regex.match?(@id_regex, id)

  @spec max_in_collection(String.t(), atom) :: Int.t()
  def max_in_collection(collection, field),
    do: GenServer.call(__MODULE__, {:max_in_collection, [collection, field]})

  @spec content(String.t()) :: List.t()
  def content(collection), do: GenServer.call(__MODULE__, {:content, collection})

  @spec content_by_correlation_id(String.t(), String.t(), atom()) :: List.t()
  def content_by_correlation_id(collection, correlation_id, sort),
    do:
      GenServer.call(__MODULE__, {:content_by_correlation_id, [collection, correlation_id, sort]})

  @spec content_by_types(String.t(), [String.t()], atom()) :: List.t()
  def content_by_types(collection, types, sort),
    do: GenServer.call(__MODULE__, {:content_by_types, [collection, types, sort]})

  @spec drop_collections(List.t()) :: any
  def drop_collections(collections) do
    GenServer.call(__MODULE__, {:drop_collections, collections})
  end

  @spec sort_expression() :: any
  def sort_expression(), do: :counter

  #
  # Callback
  #

  def handle_call({:content, collection}, _from, %{url: url} = state) do
    {:ok, %{body: %{hits: %{hits: hits}}}} =
      Elastix.Search.search(url, collection, [@type_name], %{})

    hits = hits |> Enum.map(fn h -> Map.get(h, :_source) end)
    {:reply, hits, state}
  end

  def handle_call({:content_by_types, [collection, types, sort]}, _from, %{url: url} = state) do
    sort_expression = [%{} |> Map.put(sort, "asc")]
    filter = %{type: types}

    {:ok, %{body: %{hits: %{hits: hits}}}} =
      Elastix.Search.search(url, collection, [@type_name], %{
        query: %{constant_score: %{filter: %{terms: filter}}},
        sort: sort_expression
      })

    hits = hits |> Enum.map(fn h -> Map.get(h, :_source) end)
    {:reply, hits, state}
  end

  def handle_call(
        {:content_by_correlation_id, [collection, correlation_id, sort]},
        _from,
        %{url: url} = state
      ) do
    sort_expression = [%{} |> Map.put(sort, "asc")]
    filter = %{correlation_id: correlation_id}

    {:ok, %{body: %{hits: %{hits: hits}}}} =
      Elastix.Search.search(url, collection, [@type_name], %{
        query: %{constant_score: %{filter: %{term: filter}}},
        sort: sort_expression
      })

    hits = hits |> Enum.map(fn h -> Map.get(h, :_source) end)
    {:reply, hits, state}
  end

  def handle_call({:max_in_collection, [collection, field]}, _from, %{url: url} = state) do
    {:ok, %{body: %{aggregations: %{max_counter: %{value: value}}}}} =
      Elastix.Search.search(
        url,
        collection,
        [@type_name],
        %{aggs: %{max_counter: %{max: %{field: field}}}},
        size: 0
      )

    {:reply, read_max_value(value), state}
  end

  def handle_call({:initialize, collection}, _from, %{url: url} = state) do
    Elastix.Index.exists?(url, collection) |> create_index(url, collection)
    {:reply, nil, state}
  end

  def handle_call({:drop_collections, collections}, _from, %{url: url} = state) do
    collections
    |> Enum.each(fn collection ->
      Elastix.Index.exists?(url, collection) |> delete_index(url, collection)
      create_index(url, collection)
    end)

    {:reply, nil, state}
  end

  def handle_cast({:insert, [collection, value]}, %{url: url} = state) do
    Elastix.Document.index(url, collection, "_create", value.id, value)
    {:noreply, state}
  end

  #
  # Privates
  #
  defp read_max_value(nil), do: 0
  defp read_max_value(v) when is_float(v), do: trunc(v)

  defp create_index({:ok, false}, url, collection), do: create_index(url, collection)
  defp create_index({:ok, true}, _url, _collection), do: nil

  defp create_index(url, collection) do
    Logger.info("#{collection} will be created")

    Elastix.Index.create(url, collection, %{
      mappings: %{
        properties: %{
          counter: %{type: "long"},
          correlation_id: %{type: "keyword"},
          correlation_module: %{type: "keyword"},
          type: %{type: "keyword"}
        }
      }
    })
  end

  defp delete_index({:ok, true}, url, collection), do: delete_index(url, collection)
  defp delete_index({:ok, false}, _url, _collection), do: nil

  defp delete_index(url, collection) do
    Logger.info("#{collection} will be deleted")
    Elastix.Index.delete(url, collection)
  end
end
