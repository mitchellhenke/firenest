defmodule Firenest.Topology.Redis do
  @behaviour Firenest.Topology
  @timeout 5000

  defdelegate child_spec(opts), to: Firenest.Topology.Redis.Server

  def broadcast(topology, name, :default, message) do
    GenServer.call(topology, {:broadcast, name, message})
  end

  def send(topology, {node, _} = node_ref, name, :default, message) do
    if node == Kernel.node() or node_ref_connected?(topology, node_ref) do
      # TODO
    else
      {:error, :noconnection}
    end
  end

  def sync_named(topology, pid) do
    case Process.info(pid, :registered_name) do
      {:registered_name, []} ->
        raise ArgumentError,
              "cannot sync process #{inspect(pid)} because it hasn't been registered"

      {:registered_name, name} ->
        :ok
        GenServer.call(topology, {:sync_named, pid, name}, @timeout)
    end
  end

  def node(topology) do
    :ets.lookup_element(topology, :node, 2)
  end

  def nodes(topology) do
    :ets.lookup_element(topology, :nodes, 2)
  end

  defp node_ref_connected?(topology, node_ref) do
    ms = [{{{:connected, node_ref}, :"$1"}, [], [:"$1"]}]
    :ets.select_count(topology, ms) == 1
  end
end

defmodule Firenest.Topology.Redis.Server do
  @moduledoc false

  use GenServer
  require Logger

  def start_link(opts) do
    topology = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: topology)
  end

  def init(opts) do
    Process.flag(:trap_exit, true)
    topology = Keyword.fetch!(opts, :name)
    broadcast_period = opts[:broadcast_period] || 1_500
    down_period = opts[:down_period] || broadcast_period * 20
    permdown_period = opts[:permdown_period] || 1_200_000

    # Setup the topology ets table contract.
    ^topology = :ets.new(topology, [:set, :public, :named_table, read_concurrency: true])

    id = id()
    # Some additional data is stored in the ETS table for fast lookups.
    # We need to do this before we write the adapter because once
    # the adapter is written the table is considered as ready.
    true = :ets.insert(topology, [{:node, {Kernel.node(), id}}, {:nodes, [], []}])
    true = :ets.insert(topology, {:adapter, Firenest.Topology.Redis})

    # We generate a unique ID to be used alongside the node name
    # to guarantee uniqueness in case of restarts.
    state = %{
      id: id,
      heartbeat_channel: "heartbeat",
      topology: topology,
      redix: nil,
      redix_pub_sub: nil,
      monitors: %{},
      nodes: %{},
      local_names: %{},
      broadcast_period: broadcast_period,
      down_period: down_period,
      permdown_period: permdown_period
    }

    {:ok, establish_connections(state)}
  end

  def handle_call({:broadcast, topic, message}, _from, state) do
    case Redix.command(state.redix, ["PUBLISH", topic, message]) do
      {:ok, _} ->
        {:reply, :ok, state}

      {:error, reason} ->
        Logger.error("failed to publish broadcast due to #{inspect(reason)}")
        {:reply, reason, state}
    end
  end

  def handle_call({:sync_named, _pid, _name}, _from, state) do
    # TODO
    {:reply, :ok, state}
  end

  def handle_info(:heartbeat, state) do
    {:noreply,
     state
     |> broadcast_heartbeat()
     |> detect_downs()
     |> schedule_next_heartbeat()}
  end

  def handle_info({:redix_pubsub, _pid, _ref, :subscribed, %{channel: channel}}, state) do
    Logger.info("SUBSCRIBED TO #{channel}")
    {:noreply, state}
  end

  def handle_info({:redix_pubsub, _pid, _ref, :disconnected, %{error: reason}}, state) do
    Logger.error(
      "Redix.PubSub disconnected from Redis with reason #{inspect(reason)} (awaiting reconnection)"
    )

    {:noreply, state}
  end

  def handle_info({:redix_pubsub, _pid, _ref, :message, %{payload: msg, channel: channel}}, state) do
    if state.heartbeat_channel == channel do
      {:noreply, handle_heartbeat(state, msg)}
    else
      Logger.info("RECEIVED #{msg} on #{channel}")
      {:noreply, state}
    end
  end

  ## Helpers

  defp id() do
    {:crypto.strong_rand_bytes(4), System.system_time()}
  end

  defp establish_failed(state) do
    Logger.error("unable to establish initial Redis connection. Attempting to reconnect...")
    %{state | redix: nil, redix_pub_sub: nil}
  end

  defp establish_success(state) do
    {:ok, _ref} = Redix.PubSub.subscribe(state.redix_pub_sub, state.heartbeat_channel)
    send_stuttered_heartbeat(self(), state.broadcast_period)
    state
  end

  defp establish_connections(state) do
    with {:ok, redix_pub_sub_pid} <- Redix.PubSub.start_link(sync_connect: true),
         {:ok, redix_pid} <- Redix.start_link(sync_connect: true) do
      establish_success(%{state | redix: redix_pid, redix_pub_sub: redix_pub_sub_pid})
    else
      {:error, _} ->
        establish_failed(state)
    end
  end

  defp send_stuttered_heartbeat(pid, interval) do
    Process.send_after(pid, :heartbeat, Enum.random(0..trunc(interval * 0.25)))
  end

  defp broadcast_heartbeat(state) do
    Redix.command(state.redix, [
      "PUBLISH",
      state.heartbeat_channel,
      :erlang.term_to_binary(state.id)
    ])

    state
  end

  defp handle_heartbeat(state, msg) do
    node_id = :erlang.binary_to_term(msg)

    if node_id != state.id do
      Logger.info("Received heartbeat from #{inspect(node_id)}")
      node = %{id: node_id, last_heartbeat_at: now_ms(), status: :up}
      nodes = Map.put(state.nodes, node_id, node)
      # TODO: PERSIST
      # :ets.insert(topology, [{:nodes, node_refs, node_names}, {{:connected, node_ref}, true}])
      %{state | nodes: nodes}
    else
      state
    end
  end

  defp detect_downs(%{permdown_period: perm_int, down_period: temp_int} = state) do
    nodes =
      Enum.reduce(state.nodes, state.nodes, fn {_name, node}, acc ->
        downtime = now_ms() - node.last_heartbeat_at

        cond do
          downtime > perm_int ->
            Logger.warn("#{inspect(node.id)} is permdown")
            # TODO: PERSIST
            # :ets.insert(topology, [{:nodes, node_refs, node_names}, {{:connected, node_ref}, false}])
            # :ets.delete(topology, {:connected, node_ref})
            Map.delete(acc, node.id)

          downtime > temp_int ->
            Logger.warn("#{inspect(node.id)} is down")
            # TODO: Persist?
            Map.put(acc, node.id, %{node | status: :down})

          true ->
            acc
        end
      end)

    %{state | nodes: nodes}
  end

  defp schedule_next_heartbeat(state) do
    Process.send_after(self(), :heartbeat, state.broadcast_period)
    state
  end

  defp now_ms, do: System.system_time(:millisecond)
end
