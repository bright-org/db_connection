defmodule DBConnection.Connection do
  @moduledoc false

  use GenServer

  require Logger
  alias DBConnection.Backoff
  alias DBConnection.Holder
  alias DBConnection.Util

  @timeout 15_000

  @doc false
  def start_link(mod, opts, pool, tag) do
    start_opts = Keyword.take(opts, [:debug, :spawn_opt])
    sensitive_options = %DBConnection.SensitiveData{data: opts}
    GenServer.start_link(__MODULE__, {mod, sensitive_options, pool, tag}, start_opts)
  end

  @doc false
  def child_spec(mod, opts, pool, tag, child_opts) do
    Supervisor.child_spec(
      %{id: __MODULE__, start: {__MODULE__, :start_link, [mod, opts, pool, tag]}},
      child_opts
    )
  end

  @doc false
  def disconnect({pid, ref}, err, state) do
    GenServer.cast(pid, {:disconnect, ref, err, state})
  end

  @doc false
  def stop({pid, ref}, err, state) do
    GenServer.cast(pid, {:stop, ref, err, state})
  end

  @doc false
  def ping({pid, ref}, state) do
    GenServer.cast(pid, {:ping, ref, state})
  end

  @impl GenServer
  def init({mod, %DBConnection.SensitiveData{data: opts}, pool, tag}) do
    pool_index = Keyword.get(opts, :pool_index)
    label = if pool_index, do: "db_conn_#{pool_index}", else: "db_conn"
    Util.set_label(label)

    s = %{
      mod: mod,
      opts: opts,
      state: nil,
      client: :closed,
      pool: pool,
      tag: tag,
      timer: nil,
      backoff_ref: nil,
      connected_at: nil,
      backoff: Backoff.new(opts),
      connection_listeners: Keyword.get(opts, :connection_listeners, []),
      after_connect: Keyword.get(opts, :after_connect),
      after_connect_timeout: Keyword.get(opts, :after_connect_timeout, @timeout)
    }

    {:ok, s, {:continue, {:connect, :init}}}
  end

  @impl GenServer
  def handle_continue({:connect, _info}, s) do
    %{mod: mod, opts: opts, backoff: backoff, after_connect: after_connect} = s

    try do
      apply(mod, :connect, [connect_opts(opts)])
    rescue
      e ->
        {e, stack} = maybe_sanitize_exception(e, __STACKTRACE__, opts)
        reraise e, stack
    else
      {:ok, state} when after_connect != nil ->
        ref = make_ref()
        connected_at = System.monotonic_time()
        GenServer.cast(self(), {:after_connect, ref})
        s = cancel_backoff(s)

        {:noreply, %{s | state: state, client: {ref, :connect}, connected_at: connected_at}}

      {:ok, state} ->
        backoff = backoff && Backoff.reset(backoff)
        ref = make_ref()
        connected_at = System.monotonic_time()
        GenServer.cast(self(), {:connected, ref})
        s = cancel_backoff(s)

        {:noreply,
         %{
           s
           | state: state,
             client: {ref, :connect},
             backoff: backoff,
             connected_at: connected_at
         }}

      {:error, err} when is_nil(backoff) ->
        Logger.error(
          fn ->
            [
              inspect(mod),
              " (",
              Util.inspect_pid(self()),
              ") failed to connect: " | Exception.format_banner(:error, err, [])
            ]
          end,
          crash_reason: {err, []}
        )

        raise err

      {:error, err} ->
        Logger.error(
          fn ->
            [
              inspect(mod),
              ?\s,
              ?(,
              Util.inspect_pid(self()),
              ") failed to connect: "
              | Exception.format_banner(:error, err, [])
            ]
          end,
          crash_reason: {err, []}
        )

        {:noreply, schedule_backoff(s)}
    end
  end

  def handle_continue({:disconnect, {log, err}}, %{mod: mod} = s) do
    if log == :log do
      severity =
        case err do
          %DBConnection.ConnectionError{severity: severity} -> severity
          _ -> :error
        end

      Logger.log(severity, fn ->
        [
          inspect(mod),
          ?\s,
          ?(,
          Util.inspect_pid(self()),
          ") disconnected: " | Exception.format_banner(:error, err, [])
        ]
      end)

      :ok
    end

    %{state: state, client: client, timer: timer, backoff: backoff} = s
    demonitor(client)
    cancel_timer(timer)
    :ok = apply(mod, :disconnect, [err, state])
    s = %{s | state: nil, client: :closed, timer: nil, connected_at: nil}

    notify_connection_listeners(:disconnected, s)

    case client do
      _ when backoff == nil ->
        {:stop, {:shutdown, err}, s}

      {_, :after_connect} ->
        {:noreply, schedule_backoff(s)}

      _ ->
        {:noreply, s, {:continue, {:connect, :disconnect}}}
    end
  end

  @impl GenServer
  def handle_cast({:ping, ref, state}, %{client: {ref, :pool}, mod: mod} = s) do
    case apply(mod, :ping, [state]) do
      {:ok, state} ->
        pool_update(state, s)

      {:disconnect, err, state} ->
        {:noreply, %{s | state: state}, {:continue, {:disconnect, {:log, err}}}}
    end
  end

  def handle_cast({:disconnect, ref, err, state}, %{client: {ref, _}} = s) do
    {:noreply, %{s | state: state}, {:continue, {:disconnect, {:log, err}}}}
  end

  def handle_cast({:stop, ref, err, state}, %{client: {ref, _}} = s) do
    {_, stack} = :erlang.process_info(self(), :current_stacktrace)

    case err do
      ok when ok in [:normal, :shutdown] ->
        :ok

      {:shutdown, _term} ->
        :ok

      _ ->
        reason =
          case err do
            %{__exception__: true} -> Exception.format_banner(:error, err, stack)
            _other -> "** #{inspect(err)}"
          end

        format =
          ~c"** State machine ~p terminating~n" ++
            ~c"** Reason for termination ==~n" ++
            ~c"~s~n"

        :error_logger.format(format, [self(), reason])
    end

    {:stop, {err, stack}, %{s | state: state}}
  end

  def handle_cast({tag, _, _, _}, s) when tag in [:disconnect, :stop] do
    {:noreply, s}
  end

  def handle_cast({:after_connect, ref}, %{client: {ref, :connect}} = s) do
    %{
      mod: mod,
      state: state,
      after_connect: after_connect,
      after_connect_timeout: timeout,
      opts: opts
    } = s

    notify_connection_listeners(:connected, s)

    case apply(mod, :checkout, [state]) do
      {:ok, state} ->
        opts = [timeout: timeout] ++ opts
        {pid, ref} = DBConnection.Task.run_child(mod, state, after_connect, opts)
        timer = start_timer(pid, timeout)
        s = %{s | client: {ref, :after_connect}, timer: timer, state: state}
        {:noreply, s}

      {:disconnect, err, state} ->
        {:noreply, %{s | state: state}, {:continue, {:disconnect, {:log, err}}}}
    end
  end

  def handle_cast({:after_connect, _}, s) do
    {:noreply, s}
  end

  def handle_cast({:connected, ref}, %{client: {ref, :connect}} = s) do
    %{mod: mod, state: state} = s

    notify_connection_listeners(:connected, s)

    case apply(mod, :checkout, [state]) do
      {:ok, state} ->
        pool_update(state, s)

      {:disconnect, err, state} ->
        {:noreply, %{s | state: state}, {:continue, {:disconnect, {:log, err}}}}
    end
  end

  def handle_cast({:connected, _}, s) do
    {:noreply, s}
  end

  @impl GenServer
  def handle_info(:backoff, s) do
    {:noreply, %{s | backoff_ref: nil}, {:continue, {:connect, :backoff}}}
  end

  def handle_info({:DOWN, ref, _, pid, reason}, %{client: {ref, :after_connect}} = s) do
    message =
      "client #{Util.inspect_pid(pid)} exited: " <> Exception.format_exit(reason)

    err = DBConnection.ConnectionError.exception(message)

    {:noreply, %{s | client: {nil, :after_connect}},
     {:continue, {:disconnect, {down_log(reason), err}}}}
  end

  def handle_info({:DOWN, mon, _, pid, reason}, %{client: {ref, mon}} = s) do
    message =
      "client #{Util.inspect_pid(pid)} exited: " <> Exception.format_exit(reason)

    err = DBConnection.ConnectionError.exception(message)

    {:noreply, %{s | client: {ref, nil}}, {:continue, {:disconnect, {down_log(reason), err}}}}
  end

  def handle_info({:timeout, timer, {__MODULE__, pid, timeout}}, %{timer: timer} = s)
      when is_reference(timer) do
    message =
      "client #{Util.inspect_pid(pid)} timed out because it checked out " <>
        "the connection for longer than #{timeout}ms"

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

    {:noreply, %{s | timer: nil}, {:continue, {:disconnect, {:log, exc}}}}
  end

  def handle_info(
        {:"ETS-TRANSFER", holder, _pid, {msg, ref, extra}},
        %{client: {ref, :after_connect}, timer: timer} = s
      ) do
    {_, state} = Holder.delete(holder)
    cancel_timer(timer)
    s = %{s | timer: nil}

    case msg do
      :checkin -> handle_checkin(state, s)
      :disconnect -> handle_cast({:disconnect, ref, extra, state}, s)
      :stop -> handle_cast({:stop, ref, extra, state}, s)
    end
  end

  def handle_info({:EXIT, _, _}, s) do
    {:noreply, s}
  end

  def handle_info(msg, %{mod: mod} = s) do
    Logger.info(fn ->
      [inspect(mod), ?\s, ?(, Util.inspect_pid(self()), ") missed message: " | inspect(msg)]
    end)

    {:noreply, s}
  end

  @impl GenServer
  def terminate(_reason, %{client: :closed}), do: :ok

  def terminate(reason, s) do
    %{mod: mod, state: state} = s
    msg = "connection exited: " <> Exception.format_exit(reason)

    msg
    |> DBConnection.ConnectionError.exception()
    |> mod.disconnect(state)
  end

  @impl GenServer
  def format_status(:normal, [_pdict, %{client: :closed, mod: mod}]) do
    [{:data, [{~c"Module", mod}]}]
  end

  def format_status(:terminate, [_pdict, %{client: :closed, mod: mod}]) do
    mod
  end

  def format_status(info, [pdict, %{mod: mod, state: state}]) do
    case function_exported?(mod, :format_status, 2) do
      true when info == :normal ->
        normal_status(mod, pdict, state)

      false when info == :normal ->
        normal_status_default(mod, state)

      true when info == :terminate ->
        {mod, terminate_status(mod, pdict, state)}

      false when info == :terminate ->
        {mod, state}
    end
  end

  defp maybe_sanitize_exception(e, stack, opts) do
    if Keyword.get(opts, :show_sensitive_data_on_connection_error, false) do
      {e, stack}
    else
      message =
        "connect raised #{inspect(e.__struct__)} exception#{sanitized_message(e)}. " <>
          "The exception details are hidden, as they may contain sensitive data such as " <>
          "database credentials. You may set :show_sensitive_data_on_connection_error " <>
          "to true when starting your connection if you wish to see all of the details"

      {RuntimeError.exception(message), cleanup_stacktrace(stack)}
    end
  end

  defp sanitized_message(%KeyError{} = e), do: ": #{Exception.message(%{e | term: nil})}"
  defp sanitized_message(_), do: ""

  defp connect_opts(opts) do
    case Keyword.get(opts, :configure) do
      {mod, fun, args} ->
        apply(mod, fun, [opts | args])

      fun when is_function(fun, 1) ->
        fun.(opts)

      nil ->
        opts
    end
  end

  defp down_log(:normal), do: :nolog
  defp down_log(:shutdown), do: :nolog
  defp down_log({:shutdown, _}), do: :nolog
  defp down_log(_), do: :log

  defp demonitor({_, mon}) when is_reference(mon) do
    Process.demonitor(mon, [:flush])
  end

  defp demonitor({mon, :after_connect}) when is_reference(mon) do
    Process.demonitor(mon, [:flush])
  end

  defp demonitor({_, _}), do: true
  defp demonitor(nil), do: true

  defp start_timer(_, :infinity), do: nil

  defp start_timer(pid, timeout) do
    :erlang.start_timer(timeout, self(), {__MODULE__, pid, timeout})
  end

  defp cancel_timer(nil), do: :ok

  defp cancel_timer(timer) do
    case :erlang.cancel_timer(timer) do
      false -> flush_timer(timer)
      _ -> :ok
    end
  end

  defp flush_timer(timer) do
    receive do
      {:timeout, ^timer, {__MODULE__, _, _}} ->
        :ok
    after
      0 ->
        raise ArgumentError, "timer #{inspect(timer)} does not exist"
    end
  end

  defp schedule_backoff(%{backoff: backoff} = s) do
    {timeout, backoff} = Backoff.backoff(backoff)
    s = cancel_backoff(s)
    ref = Process.send_after(self(), :backoff, timeout)
    %{s | backoff: backoff, backoff_ref: ref}
  end

  defp cancel_backoff(%{backoff_ref: ref} = s) when is_reference(ref) do
    _ = Process.cancel_timer(ref)
    %{s | backoff_ref: nil}
  end

  defp cancel_backoff(s), do: s

  defp handle_checkin(state, s) do
    %{backoff: backoff, client: client} = s
    backoff = backoff && Backoff.reset(backoff)
    demonitor(client)
    pool_update(state, %{s | client: nil, backoff: backoff})
  end

  defp pool_update(state, %{pool: pool, tag: tag, mod: mod, connected_at: connected_at} = s) do
    case Holder.update(pool, tag, mod, state, connected_at) do
      {:ok, ref} ->
        {:noreply, %{s | client: {ref, :pool}, state: state}}

      :error ->
        {:stop, {:shutdown, :no_more_pool}, s}
    end
  end

  defp normal_status(mod, pdict, state) do
    try do
      mod.format_status(:normal, [pdict, state])
    catch
      _, _ ->
        normal_status_default(mod, state)
    else
      status ->
        status
    end
  end

  defp normal_status_default(mod, state) do
    [{:data, [{~c"Module", mod}, {~c"State", state}]}]
  end

  defp terminate_status(mod, pdict, state) do
    try do
      mod.format_status(:terminate, [pdict, state])
    catch
      _, _ ->
        state
    else
      status ->
        status
    end
  end

  defp cleanup_stacktrace(stack) do
    case stack do
      [{_, _, arity, _} | _rest] = stacktrace when is_integer(arity) ->
        stacktrace

      [{mod, fun, args, info} | rest] when is_list(args) ->
        [{mod, fun, length(args), info} | rest]
    end
  end

  defp notify_connection_listeners(action, %{} = state) do
    %{connection_listeners: connection_listeners} = state

    {listeners, message} =
      case connection_listeners do
        listeners when is_list(listeners) ->
          {listeners, {action, self()}}

        {listeners, tag} when is_list(listeners) ->
          {listeners, {action, self(), tag}}
      end

    Enum.each(listeners, &send(&1, message))
  end
end
