defmodule DBConnection.SingleConnection do
  @moduledoc """
  A single-connection pool for constrained runtimes.

  Unlike `DBConnection.ConnectionPool`, this pool keeps exactly one connection,
  does not use CoDel queueing, idle polling, or `DynamicSupervisor`.

  Pass this module as the `:pool` option to `DBConnection.start_link/2`.
  The default pool remains `DBConnection.ConnectionPool`. `:pool_size` is
  ignored except that values below 1 raise, same as the default pool.
  """

  use GenServer
  alias DBConnection.Holder
  alias DBConnection.Util

  @behaviour DBConnection.Pool

  @doc false
  def start_link({mod, opts}) do
    GenServer.start_link(__MODULE__, {mod, opts}, start_opts(opts))
  end

  @doc false
  @impl DBConnection.Pool
  def checkout(pool, callers, opts) do
    Holder.checkout(pool, callers, opts)
  end

  @doc false
  @impl DBConnection.Pool
  def disconnect_all(pool, interval, _opts) do
    GenServer.call(pool, {:disconnect_all, interval}, :infinity)
  end

  @doc false
  @impl DBConnection.Pool
  def get_connection_metrics(pool) do
    GenServer.call(pool, :get_connection_metrics, :infinity)
  end

  @impl GenServer
  def init({mod, opts}) do
    Process.flag(:trap_exit, true)
    DBConnection.register_as_pool(mod)

    size = Keyword.get(opts, :pool_size, 1)

    if size < 1 do
      raise ArgumentError, "pool size must be greater or equal to 1, got #{size}"
    end

    tag = make_ref()

    state = %{
      mod: mod,
      opts: opts,
      tag: tag,
      holder: nil,
      waiters: :queue.new(),
      ts: {nil, max_lifetime(opts)},
      conn: nil,
      restarts: [],
      max_restarts: Keyword.get(opts, :max_restarts, 3),
      max_seconds: Keyword.get(opts, :max_seconds, 5)
    }

    {:ok, pid} = start_connection(state)
    {:ok, %{state | conn: pid}}
  end

  @impl GenServer
  def handle_call(:get_connection_metrics, _from, state) do
    metrics = %{
      source: {:pool, self()},
      ready_conn_count: if(state.holder, do: 1, else: 0),
      checkout_queue_length: :queue.len(state.waiters)
    }

    {:reply, [metrics], state}
  end

  def handle_call({:disconnect_all, interval}, _from, state) do
    {_, max_lifetime} = state.ts
    ts = {{System.monotonic_time(), interval}, max_lifetime}
    {:reply, :ok, %{state | ts: ts}}
  end

  @impl GenServer
  def handle_info({:db_connection, from, {:checkout, _callers, _now, queue?}}, state) do
    {:noreply, checkout_request(from, queue?, state)}
  end

  def handle_info({:"ETS-TRANSFER", holder, _pid, {msg, tag, extra}}, %{tag: tag} = state) do
    case msg do
      :checkin ->
        owner = self()

        case :ets.info(holder, :owner) do
          ^owner ->
            {interval, max_lifetime} = state.ts

            if Holder.maybe_disconnect(holder, interval, max_lifetime) do
              {:noreply, %{state | holder: nil}}
            else
              handle_checkin(holder, extra, state)
            end

          :undefined ->
            {:noreply, state}
        end

      :disconnect ->
        Holder.handle_disconnect(holder, extra)
        {:noreply, %{state | holder: nil}}

      :stop ->
        Holder.handle_stop(holder, extra)
        {:noreply, %{state | holder: nil}}
    end
  end

  def handle_info({:"ETS-TRANSFER", holder, pid, tag}, %{tag: tag} = state) do
    message = "client #{Util.inspect_pid(pid)} exited"
    err = DBConnection.ConnectionError.exception(message: message, severity: :info)
    Holder.handle_disconnect(holder, err)
    {:noreply, %{state | holder: nil}}
  end

  def handle_info({:timeout, deadline, {tag, holder, pid, len}}, %{tag: tag} = state) do
    if Holder.handle_deadline(holder, deadline) do
      message =
        "client #{Util.inspect_pid(pid)} timed out because " <>
          "it queued and checked out the connection for longer than #{len}ms"

      exc =
        case Process.info(pid, :current_stacktrace) do
          {:current_stacktrace, stacktrace} ->
            message <>
              "\n\n#{Util.inspect_pid(pid)} was at location:\n\n" <>
              Exception.format_stacktrace(stacktrace)

          _ ->
            message
        end
        |> DBConnection.ConnectionError.exception()

      Holder.handle_disconnect(holder, exc)
    end

    {:noreply, state}
  end

  def handle_info({:EXIT, pid, reason}, %{conn: pid} = state) do
    restart_connection(reason, state)
  end

  def handle_info({:EXIT, _pid, reason}, state) do
    {:stop, reason, state}
  end

  @impl GenServer
  def terminate(_reason, %{conn: pid}) when is_pid(pid) do
    Process.exit(pid, :shutdown)
    :ok
  end

  def terminate(_reason, _state), do: :ok

  defp checkout_request(from, queue?, %{holder: {holder, checked_in_at}} = state) do
    if Holder.handle_checkout(holder, from, state.tag, checked_in_at) do
      %{state | holder: nil}
    else
      checkout_request(from, queue?, %{state | holder: nil})
    end
  end

  defp checkout_request(from, true, state) do
    %{state | waiters: :queue.in(from, state.waiters)}
  end

  defp checkout_request(from, false, state) do
    message = "connection not available and queuing is disabled"
    Holder.reply_error(from, DBConnection.ConnectionError.exception(message))
    state
  end

  defp handle_checkin(holder, now_in_native, state) do
    case :queue.out(state.waiters) do
      {:empty, waiters} ->
        {:noreply, %{state | holder: {holder, now_in_native}, waiters: waiters}}

      {{:value, from}, waiters} ->
        state = %{state | waiters: waiters}

        if Holder.handle_checkout(holder, from, state.tag, now_in_native) do
          {:noreply, %{state | holder: nil}}
        else
          handle_checkin(holder, now_in_native, state)
        end
    end
  end

  defp restart_connection(reason, state) do
    now = System.monotonic_time(:second)
    cutoff = now - state.max_seconds
    restarts = Enum.filter(state.restarts, &(&1 >= cutoff))

    if length(restarts) >= state.max_restarts do
      {:stop, reason, state}
    else
      {:ok, pid} = start_connection(state)
      {:noreply, %{state | conn: pid, holder: nil, restarts: [now | restarts]}}
    end
  end

  defp start_connection(%{mod: mod, opts: opts, tag: tag}) do
    DBConnection.Connection.start_link(mod, Keyword.put(opts, :pool_index, 1), self(), tag)
  end

  defp max_lifetime(opts) do
    case Keyword.fetch(opts, :max_lifetime) do
      {:ok, %Range{first: first, last: last, step: 1}} when first >= 0 and last >= first ->
        {System.convert_time_unit(first, :millisecond, :native), last - first}

      {:ok, invalid} ->
        raise ArgumentError,
              "invalid value for :max_lifetime, expected a non-negative step-1 range, got: #{inspect(invalid)}"

      :error ->
        nil
    end
  end

  defp start_opts(opts) do
    Keyword.take(opts, [:name, :spawn_opt])
  end
end
