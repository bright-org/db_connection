defmodule DBConnection.DynamicSupervisor do
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
    Supervisor.start_link(__MODULE__, opts, name: name)
  end

  def start_child(supervisor, child_spec) do
    spec = Supervisor.child_spec(child_spec, id: make_ref())
    Supervisor.start_child(supervisor, spec)
  end

  def which_children(supervisor) do
    Supervisor.which_children(supervisor)
  end

  @impl true
  def init(opts) do
    Supervisor.init([], strategy: Keyword.get(opts, :strategy, :one_for_one))
  end
end
