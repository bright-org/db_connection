defmodule DBConnection.SingleConnectionTest do
  use ExUnit.Case, async: true

  alias TestSingleConnection, as: P
  alias TestAgent, as: A
  alias TestQuery, as: Q
  alias TestResult, as: R

  test "start_link workflow with unregistered name" do
    stack = [{:ok, :state}]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, pool_size: 1]
    {:ok, conn} = P.start_link(opts)

    {:links, links} = Process.info(self(), :links)
    assert conn in links

    _ = :sys.get_state(conn)
    assert [{:connect, [connect_opts]}] = A.record(agent)
    assert Keyword.get(connect_opts, :pool_index) == 1
    assert Keyword.get(connect_opts, :agent) == agent
  end

  test "start_link workflow with registered name", %{test: name} do
    stack = [{:ok, :state}]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, name: name, pool_size: 1]
    {:ok, conn} = P.start_link(opts)

    assert Process.info(conn, :registered_name) == {:registered_name, name}

    _ = :sys.get_state(conn)
    assert [{:connect, [connect_opts]}] = A.record(agent)
    assert Keyword.get(connect_opts, :pool_index) == 1
  end

  test "connection_module/1 returns the connection module" do
    {:ok, agent} = A.start_link([{:ok, :state}, {:idle, :state}, {:idle, :state}])
    {:ok, pool} = P.start_link(agent: agent)

    assert {:ok, TestConnection} = DBConnection.connection_module(pool)

    P.run(pool, fn conn ->
      assert {:ok, TestConnection} = DBConnection.connection_module(conn)
    end)
  end

  test "checkout and checkin via run" do
    stack = [{:ok, :state}] ++ List.duplicate({:idle, :state}, 8)
    {:ok, agent} = A.start_link(stack)
    {:ok, pool} = P.start_link(agent: agent)

    assert P.run(pool, fn _conn -> :result end) == :result
    assert P.run(pool, fn _conn -> :again end) == :again
  end

  test "execute after checkout" do
    stack = [
      {:ok, :state},
      {:ok, %Q{}, %R{}, :state},
      {:ok, %Q{}, %R{}, :state}
    ]

    {:ok, agent} = A.start_link(stack)
    {:ok, pool} = P.start_link(agent: agent)

    assert P.execute(pool, %Q{}, [:param1]) == {:ok, %Q{}, %R{}}
    assert P.execute(pool, %Q{}, [:param2]) == {:ok, %Q{}, %R{}}

    assert [
             connect: [_],
             handle_execute: [%Q{}, [:param1], _, :state],
             handle_execute: [%Q{}, [:param2], _, :state]
           ] = A.record(agent)
  end

  test "queue: false raises when the connection is busy" do
    stack = [{:ok, :state}, {:idle, :state}, {:idle, :state}]
    {:ok, agent} = A.start_link(stack)
    {:ok, pool} = P.start_link(agent: agent)

    P.run(pool, fn _ ->
      {queue_time, _} =
        :timer.tc(fn ->
          opts = [queue: false]

          assert_raise DBConnection.ConnectionError,
                       ~r"connection not available and queuing is disabled",
                       fn -> P.run(pool, fn _ -> flunk("got connection") end, opts) end
        end)

      assert queue_time <= 1_000_000, "request was queued"
    end)
  end

  test "queues waiters and serves them in FIFO order" do
    stack = [{:ok, :state}] ++ List.duplicate({:idle, :state}, 20)
    {:ok, agent} = A.start_link(stack)
    {:ok, pool} = P.start_link(agent: agent)

    parent = self()

    holder =
      spawn_link(fn ->
        Process.put(:agent, agent)

        P.run(pool, fn _ ->
          send(parent, {:held, self()})

          receive do
            {:release, ^parent} -> :ok
          end
        end)
      end)

    assert_receive {:held, ^holder}

    first =
      Task.async(fn ->
        Process.put(:agent, agent)
        P.run(pool, fn _ -> send(parent, {:ran, 1}) end)
        :ok
      end)

    TestHelpers.poll(fn ->
      assert [%{checkout_queue_length: 1, ready_conn_count: 0}] =
               P.get_connection_metrics(pool)
    end)

    second =
      Task.async(fn ->
        Process.put(:agent, agent)
        P.run(pool, fn _ -> send(parent, {:ran, 2}) end)
        :ok
      end)

    TestHelpers.poll(fn ->
      assert [%{checkout_queue_length: 2, ready_conn_count: 0}] =
               P.get_connection_metrics(pool)
    end)

    send(holder, {:release, self()})
    assert_receive {:ran, 1}
    assert_receive {:ran, 2}

    assert :ok = Task.await(first)
    assert :ok = Task.await(second)
  end

  test "reports connection metrics" do
    stack = [
      {:ok, :state},
      fn _, _, _, _ ->
        receive do
          :continue -> {:ok, %Q{}, %R{}, :state}
        end
      end
    ]

    {:ok, agent} = A.start_link(stack)
    {:ok, pool} = P.start_link(agent: agent)

    TestHelpers.poll(fn ->
      assert [%{source: {:pool, ^pool}, checkout_queue_length: 0, ready_conn_count: 1}] =
               P.get_connection_metrics(pool)
    end)

    query =
      spawn_link(fn ->
        Process.put(:agent, agent)
        assert P.execute(pool, %Q{}, [:client])
      end)

    TestHelpers.poll(fn ->
      assert [%{source: {:pool, ^pool}, checkout_queue_length: 0, ready_conn_count: 0}] =
               P.get_connection_metrics(pool)
    end)

    send(query, :continue)

    TestHelpers.poll(fn ->
      assert [%{source: {:pool, ^pool}, checkout_queue_length: 0, ready_conn_count: 1}] =
               P.get_connection_metrics(pool)
    end)
  end

  test "disconnect_all disconnects on checkin" do
    stack = [
      {:ok, :state},
      {:ok, %Q{}, %R{}, :new_state1},
      {:ok, %Q{}, %R{}, :new_state2},
      :ok,
      {:ok, :final_state},
      {:ok, %Q{}, %R{}, :final_state1},
      {:ok, %Q{}, %R{}, :final_state2}
    ]

    {:ok, agent} = A.start_link(stack)
    {:ok, pool} = P.start_link(agent: agent)

    assert P.execute(pool, %Q{}, [:param]) == {:ok, %Q{}, %R{}}
    P.disconnect_all(pool, 0)
    assert P.execute(pool, %Q{}, [:param]) == {:ok, %Q{}, %R{}}
    assert P.execute(pool, %Q{}, [:param]) == {:ok, %Q{}, %R{}}
    assert P.execute(pool, %Q{}, [:param]) == {:ok, %Q{}, %R{}}

    err = %DBConnection.ConnectionError{message: "disconnect_all requested", severity: :debug}

    assert [
             connect: [_],
             handle_execute: [_, _, _, :state],
             handle_execute: [_, _, _, :new_state1],
             disconnect: [^err, :new_state2],
             connect: [_],
             handle_execute: [_, _, _, :final_state],
             handle_execute: [_, _, _, :final_state1]
           ] = A.record(agent)
  end

  test "reconnects when the client exits" do
    stack = [
      {:ok, :state},
      {:idle, :state},
      :ok,
      fn opts ->
        send(opts[:parent], :reconnected)
        {:ok, :state}
      end,
      {:idle, :state},
      {:idle, :state}
    ]

    {:ok, agent} = A.start_link(stack)
    {:ok, pool} = P.start_link(agent: agent, parent: self())

    _ =
      spawn(fn ->
        _ = Process.put(:agent, agent)

        P.run(pool, fn _ ->
          Process.exit(self(), :shutdown)
        end)
      end)

    assert_receive :reconnected
    assert P.run(pool, fn _ -> :result end) == :result
  end
end
