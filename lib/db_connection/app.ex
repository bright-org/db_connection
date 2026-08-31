defmodule DBConnection.App do
  @moduledoc false
  use Application

  @impl true
  def start(_type, _args) do
    children = [
      {DBConnection.TaskSupervisor, name: DBConnection.Task},
      {DBConnection.DynamicSupervisor,
       name: DBConnection.Ownership.Supervisor, strategy: :one_for_one},
      {DBConnection.DynamicSupervisor,
       name: DBConnection.ConnectionPool.Supervisor, strategy: :one_for_one},
      DBConnection.Watcher
    ]

    Supervisor.start_link(children, strategy: :one_for_all, name: __MODULE__)
  end
end
