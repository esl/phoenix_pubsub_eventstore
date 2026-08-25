defmodule Phoenix.PubSub.EventStoreTest.MarkerSerializer do
  alias Phoenix.PubSub.EventStore.Serializer.Base64

  def serialize(term),
    do: %Base64{payload: {:custom, term} |> :erlang.term_to_binary() |> Base.encode64()}

  def deserialize(%Base64{payload: p}) do
    {:custom, term} = p |> Base.decode64!() |> :erlang.binary_to_term()
    term
  end
end

defmodule Phoenix.PubSub.EventStoreTest do
  use ExUnit.Case

  @eventstore Phoenix.PubSub.EventStoreTest.TestApp.EventStore

  setup_all do
    case Phoenix.PubSub.EventStoreTest.TestApp.Application.start(nil, nil) do
      {:ok, _} -> :ok
      {:error, {:already_started, _}} -> :ok
    end

    # Verify the EventStore DB is actually functional. Without this check,
    # tests can pass for the wrong reason: append_to_stream silently fails,
    # the adapter's local_broadcast still fires, and tests see 1 delivery
    # without ever touching the database.
    event = %EventStore.EventData{
      event_type: "HealthCheck",
      data: %{},
      metadata: %{}
    }

    case @eventstore.append_to_stream("health-check", :any_version, [event]) do
      :ok ->
        :ok

      {:error, reason} ->
        flunk("""
        EventStore is not functional. Run `MIX_ENV=test mix event_store.init` to set up the database.
        Error: #{inspect(reason)}
        """)
    end
  end

  describe "subscriptions" do
    test "I can broadcast a message on a topic" do
      Phoenix.PubSub.subscribe(EventStoreTest.PubSub, "test.topic")

      :ok = Phoenix.PubSub.broadcast(EventStoreTest.PubSub, "test.topic", %{hello: :world})

      assert_receive %{hello: :world}, 5000
    end

    test "broadcast delivers message exactly once to local subscriber" do
      Phoenix.PubSub.subscribe(EventStoreTest.PubSub, "test.topic")

      :ok = Phoenix.PubSub.broadcast(EventStoreTest.PubSub, "test.topic", %{hello: :world})

      assert_receive %{hello: :world}, 5000
      refute_receive %{hello: :world}, 200
    end

    test "local subscriber receives exactly one copy of broadcast" do
      Phoenix.PubSub.subscribe(EventStoreTest.PubSub, "test.topic")
      :ok = Phoenix.PubSub.broadcast(EventStoreTest.PubSub, "test.topic", :count_me)

      assert_receive :count_me, 5000

      # Sleep to allow any duplicate (EventStore round-trip or double-dispatch) to arrive
      Process.sleep(500)
      {:message_queue_len, message_queue_len} = Process.info(self(), :message_queue_len)

      assert message_queue_len == 0,
             "expected no duplicate deliveries, got #{message_queue_len} extra"
    end

    def spawn_forwarder(label, topic) do
      parent = self()

      Task.async(fn ->
        Phoenix.PubSub.subscribe(EventStoreTest.PubSub, topic)
        send(parent, {:subscribed, label})

        receive do
          msg -> send(parent, {:received, label, msg})
        after
          5000 ->
            IO.puts("WARNING: Forwarder didn't receieve any messages after timeout!")
        end
      end)
    end

    test "all subscribers on a topic receive the broadcast" do
      subscribers =
        Enum.map(1..3, fn i ->
          spawn_forwarder(i, "test.topic")
        end)

      Enum.each(1..3, fn i -> assert_receive {:subscribed, ^i}, 1000 end)

      :ok = Phoenix.PubSub.broadcast(EventStoreTest.PubSub, "test.topic", %{hello: :world})

      Task.await_many(subscribers, 6000)

      Enum.each(1..3, fn i -> assert_receive {:received, ^i, %{hello: :world}}, 100 end)
    end

    test "subscriber on topic A does not receive broadcasts on topic B" do
      subscriber = spawn_forwarder(:a, "topic.a")

      assert_receive {:subscribed, :a}, 1000

      :ok = Phoenix.PubSub.broadcast(EventStoreTest.PubSub, "topic.b", %{hello: :world})

      Task.await(subscriber, 6000)

      refute_receive {:received, :a, _}
    end

    test "unsubscribed process does not receive messages" do
      :ok = Phoenix.PubSub.broadcast(EventStoreTest.PubSub, "test.topic", %{hello: :world})

      refute_receive _, 500
    end

    test "broadcast returns :ok" do
      assert :ok = Phoenix.PubSub.broadcast(EventStoreTest.PubSub, "test.topic", %{hello: :world})
    end

    test "messages are received in order when multiple broadcasts sent" do
      Phoenix.PubSub.subscribe(EventStoreTest.PubSub, "test.topic")

      Enum.each(1..5, fn i ->
        Phoenix.PubSub.broadcast(EventStoreTest.PubSub, "test.topic", {:msg, i})
      end)

      messages =
        Enum.map(1..5, fn _ ->
          receive do
            msg -> msg
          after
            5000 -> flunk("timed out waiting for message")
          end
        end)

      assert messages == Enum.map(1..5, &{:msg, &1})
    end
  end

  describe "direct_broadcast" do
    test "direct_broadcast to current node delivers message to local subscriber" do
      Phoenix.PubSub.subscribe(EventStoreTest.PubSub, "test.topic")

      Phoenix.PubSub.direct_broadcast(node(), EventStoreTest.PubSub, "test.topic", %{
        hello: :world
      })

      assert_receive %{hello: :world}, 5000
    end

    test "direct_broadcast to different node does not deliver locally" do
      Phoenix.PubSub.subscribe(EventStoreTest.PubSub, "test.topic")

      Phoenix.PubSub.direct_broadcast(:fake@node, EventStoreTest.PubSub, "test.topic", %{
        hello: :world
      })

      refute_receive _, 500
    end

    test "direct_broadcast delivers exactly once to local subscriber" do
      Phoenix.PubSub.subscribe(EventStoreTest.PubSub, "test.topic")

      Phoenix.PubSub.direct_broadcast(node(), EventStoreTest.PubSub, "test.topic", :once)

      assert_receive :once, 5000

      Process.sleep(500)
      {:message_queue_len, len} = Process.info(self(), :message_queue_len)
      assert len == 0, "expected no duplicate deliveries, got #{len} extra"
    end
  end

  describe "message types" do
    test "broadcast works with atom message" do
      Phoenix.PubSub.subscribe(EventStoreTest.PubSub, "test.topic")
      :ok = Phoenix.PubSub.broadcast(EventStoreTest.PubSub, "test.topic", :hello)
      assert_receive :hello, 5000
    end

    test "broadcast works with string message" do
      Phoenix.PubSub.subscribe(EventStoreTest.PubSub, "test.topic")
      :ok = Phoenix.PubSub.broadcast(EventStoreTest.PubSub, "test.topic", "hello")
      assert_receive "hello", 5000
    end

    test "broadcast works with list message" do
      Phoenix.PubSub.subscribe(EventStoreTest.PubSub, "test.topic")
      :ok = Phoenix.PubSub.broadcast(EventStoreTest.PubSub, "test.topic", [1, 2, 3])
      assert_receive [1, 2, 3], 5000
    end

    test "broadcast works with integer message" do
      Phoenix.PubSub.subscribe(EventStoreTest.PubSub, "test.topic")
      :ok = Phoenix.PubSub.broadcast(EventStoreTest.PubSub, "test.topic", 42)
      assert_receive 42, 5000
    end
  end

  describe "custom serializer" do
    test "custom serializer module is used for encode/decode" do
      # Two separate PubSub instances force the message through the EventStore
      # round-trip: A serializes, B deserializes. If either step used the wrong
      # serializer the message would not arrive correctly.
      topic = "custom_serializer_topic"

      pubsub_a_name = :"custom_serializer_a_#{System.unique_integer()}"
      pubsub_b_name = :"custom_serializer_b_#{System.unique_integer()}"

      start_supervised!(
        {Phoenix.PubSub,
         [
           name: pubsub_a_name,
           adapter: Phoenix.PubSub.EventStore,
           eventstore: @eventstore,
           serializer: Phoenix.PubSub.EventStoreTest.MarkerSerializer
         ]},
        id: :pubsub_a
      )

      start_supervised!(
        {Phoenix.PubSub,
         [
           name: pubsub_b_name,
           adapter: Phoenix.PubSub.EventStore,
           eventstore: @eventstore,
           serializer: Phoenix.PubSub.EventStoreTest.MarkerSerializer
         ]},
        id: :pubsub_b
      )

      Phoenix.PubSub.subscribe(pubsub_b_name, topic)
      Phoenix.PubSub.broadcast(pubsub_a_name, topic, %{hello: :world})

      assert_receive %{hello: :world}, 5000
    end
  end

  describe "unique_id_fn option" do
    test "custom unique_id_fn is called with pubsub name on init" do
      test_pid = self()
      pubsub_name = :"custom_id_fn_#{System.unique_integer()}"

      unique_id_fn = fn name ->
        send(test_pid, {:unique_id_called, name})
        "test-unique-id"
      end

      start_supervised!(
        {Phoenix.PubSub,
         [
           name: pubsub_name,
           adapter: Phoenix.PubSub.EventStore,
           eventstore: @eventstore,
           unique_id_fn: unique_id_fn
         ]}
      )

      assert_receive {:unique_id_called, ^pubsub_name}, 1000
    end
  end
end
