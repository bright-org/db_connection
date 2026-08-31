defmodule DBConnection.TaskSupervisor do
  @moduledoc false

  use Supervisor

  def child_spec(opts) do
    name = Keyword.fetch!(opts, :name)

    %{
      id: name,
      start: {__MODULE__, :start_link, [opts]},
      type: :supervisor
    }
  end

  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    Supervisor.start_link(__MODULE__, :ok, name: name)
  end

  def start_child(supervisor, module, function_name, args) do
    spec = %{
      id: make_ref(),
      start: {__MODULE__, :start_task, [module, function_name, args]},
      restart: :temporary
    }

    Supervisor.start_child(supervisor, spec)
  end

  def start_task(module, function_name, args) do
    {:ok, :proc_lib.spawn_link(module, function_name, args)}
  end

  @impl true
  def init(:ok) do
    Supervisor.init([], strategy: :one_for_one)
  end
end
