# How to write a Phoenix PubSub adapter. Tutorial example based on EventStore

In distributed systems there is usually a need for the asynchronous transmission of messages to one or more services or processes. If you have used Phoenix you might have discovered that it provides a flexible way of solving this problem through a built-in pubsub framework called [Phoenix PubSub](https://hexdocs.pm/phoenix_pubsub/Phoenix.PubSub.html). Currently, it officially supports pubsub based on PG2 and Redis. It uses so called adapters to provide a pluggable interface for different pubsub implementations.

This blog post covers the main steps of implementing an adapter for Phoenix PubSub. This will be an adapter for sending PubSub broadcasts with [EventStore](https://hexdocs.pm/eventstore/EventStore.html), an event sourcing library for Elixir that persists events to a PostgreSQL database as an append-only log.

Using EventStore as a PubSub backend can give you a few advantages over the default PG2 adapter:

- **No Erlang distribution required**: nodes communicate through the shared database rather than through the Erlang cluster, so you can run multiple nodes without configuring Erlang node connectivity.
- **Persistence**: every broadcast is stored and can be replayed or audited later.

The tradeoffs are the need for storage and the additional latency of a database round-trip per broadcast, making it best suited for lower-throughput messaging where persistence and cross-node decoupling matter more than raw speed.

A full implementation of the adapter can be found [on Github](https://github.com/laszlohegedus/phoenix_pubsub_eventstore).

To learn more about Elixir and related technologies you might want to check out [ElixirConf EU Virtual](https://virtual.elixirconf.eu/) taking place 18-19 June.

## Phoenix.PubSub.Adapter in a nutshell

A Phoenix PubSub adapter has to implement a few callbacks that are specified in the behaviour [`Phoenix.PubSub.Adapter`](https://phoenix-pubsub.hexdocs.pm/Phoenix.PubSub.Adapter.html):

### `node_name(adapter_name)`

This function should return the node name as an atom or a binary. There are not too many uses for it, apart from the module `Phoenix.Tracker` and its implementations.

In most cases the following implementation should suffice:

```elixir
def node_name(nil), do: node()
def node_name(configured_name), do: configured_name
```

### `child_spec(keyword)`

This callback is used to generate the child spec for the adapter. Note that it is a default implementation for each GenServer, so usually it is not necessary to overwrite it.

### `broadcast(adapter_name, topic, message, dispatcher)`

This is called when a message is broadcast through `Phoenix.PubSub.broadcast`. The first parameter `adapter_name` is derived from the name specified for the PubSub — set as an atom or module name when initializing the PubSub system. Note that the name of the PubSub is treated as a valid (not necessarily existing) module name, so it is better to follow the corresponding naming convention. The name of the adapter will come from the PubSub name with the suffix `.Adapter` added (e.g. a `name` of `MyApp.PubSub` would have an `adapter_name` of `MyApp.PubSub.Adapter`).

The `topic` and `message` parameters are self explanatory. The `dispatcher` is a module that is responsible for the local delivery of messages. It implements a `dispatch/3` function that will forward the messages to the subscribed processes.

### `direct_broadcast(adapter_name, node_name, topic, message, dispatcher)`

This is similar to `broadcast/4` with an additional `node_name` parameter. When `direct_broadcast` is called, the message should only be broadcast to subscribers on the given node.

## The EventStore adapter

This section walks through a possible implementation of a Phoenix PubSub adapter that uses EventStore to distribute messages between nodes. This gives a solution that does not depend on Erlang/Elixir distribution, and an event log is stored in case further analysis is needed.

Note that no load tests were performed on this solution and it is not production-ready, mainly a proof of concept and an aid for demonstration.

### Phoenix.PubSub

To understand how the adapter should work, it is worth looking into the code of the module `Phoenix.PubSub`. It is well documented and clean, so it doesn't take too long to understand what each function does.

`Phoenix.PubSub` makes use of Elixir's [Registry](https://hexdocs.pm/elixir/Registry.html) module. Each subscription is an entry under the corresponding key in the registry associated with the PubSub adapter. That is, when calling `Phoenix.PubSub.subscribe(pubsub, topic, opts \\ [])`, a new entry is added to the registry with `Registry.register(pubsub, topic, opts[:metadata])`.

Duplicate subscriptions are allowed, but they will lead to duplicate delivery of messages. Unsubscribing from a topic removes all entries for the process under that topic.

The main functionality covered here is `Phoenix.PubSub.broadcast` and the similar `Phoenix.PubSub.direct_broadcast`. Whenever these functions are called, two main things happen:

1) The `broadcast` or `direct_broadcast` callback is called on the corresponding PubSub adapter and
2) if successful, the message is dispatched to local processes through the default or overridden dispatch method.

This means that the main goal of the adapter's `broadcast` function is to make sure that the message gets delivered to the other nodes. In the case of a `direct_broadcast` the message should only be received by the subscribers on the given node and not others.

## The implementation

First, the adapter creates a GenServer called `Phoenix.PubSub.EventStore` to stitch into the PubSub supervision tree. To allow flexibility over which EventStore to use, an `eventstore` option is exposed to pass the desired EventStore module to the PubSub. This is stored in the state along with the name of the current adapter instance (the option `name` in the pubsub config) — both are needed later.

To use this PubSub, it is added to the supervision tree:

```elixir
{Phoenix.PubSub,
  [name: MyApp.PubSub,
   adapter: Phoenix.PubSub.EventStore,
   eventstore: MyApp.EventStore]
}
```

Then storing the desired values can be done in the GenServer's `init` callback:

```elixir
defmodule Phoenix.PubSub.EventStore do
  @behaviour Phoenix.PubSub.Adapter
  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: opts[:adapter_name])
  end

  def init(opts) do
    {:ok,
     %{
       eventstore: opts[:eventstore],
       pubsub_name: opts[:name]
     }}
  end
  #... implementation will come here ...#
end
```

Note the difference between `opts[:name]` and `opts[:adapter_name]`. The former is the name of the PubSub as a whole and is reserved for the Registry. Publishers use it when broadcasting messages. `opts[:adapter_name]` can be used as the name of the GenServer.

The implementation is fairly simple. The adapter must be able to broadcast messages to all subscribers, using the event store.

## Distributing a message as an event

The first thing the GenServer must do is append a new message to the event store when `broadcast/4` is called.

```elixir
def broadcast(server, topic, message, dispatcher, metadata \\ %{}) do
  metadata = Map.put(metadata, :dispatcher, dispatcher)
  GenServer.call(server, {:broadcast, topic, message, metadata})
end

def handle_call(
      {:broadcast, topic, message, metadata},
      _from_pid,
      %{id: id, eventstore: eventstore, serializer: serializer, pubsub_name: pubsub_name} = state
    ) do
  event = %EventStore.EventData{...}

  res = eventstore.append_to_stream(topic, :any_version, [event])

  # For direct_broadcast targeting the current node, the framework does not
  # call local dispatch, so the adapter must do it. For regular broadcast,
  # the framework handles local dispatch after adapter.broadcast returns :ok.
  current_node = to_string(node())
  destination_node = Map.get(metadata, :destination_node)

  if destination_node == current_node do
    dispatcher = Map.get(metadata, :dispatcher, Phoenix.PubSub)
    Phoenix.PubSub.local_broadcast(pubsub_name, topic, message, dispatcher)
  end

  {:reply, res, state}
end
```

The key decision here is how to wrap the message inside an `%EventStore.EventData{}` struct. Serialization is handled by a pluggable module (defaulting to `Phoenix.PubSub.EventStore.Serializer.Base64`) so the adapter is not tied to a specific encoding. The default serializer base64-encodes `:erlang.term_to_binary/1` output — this is necessary because EventStore stores data as JSON and raw binaries would be invalid, and because JSON cannot distinguish atoms from strings so a round-trip through term serialization preserves type fidelity.

```elixir
event = %EventStore.EventData{
  event_type: to_string(serializer),
  data: serializer.serialize(message)
}
```

A custom serializer can be provided via the `serializer` option as long as it implements `serialize/1` and `deserialize/1`.

A custom ID generator can be provided via `unique_id_fn` — a function that receives the PubSub name and returns a unique string. Useful when UUID is unavailable or when a deterministic ID is needed for testing.

## Handling events, local distribution

Now that events are in the event store, any subscribed process will receive them. The GenServer (`Phoenix.PubSub.EventStore`) must subscribe to all topics (`"$all"`). If the event store is also used for another purpose, it's best to have a separate one for pubsub. The subscription is set up via `handle_continue/2`, which runs immediately after `init/1` completes, before any other messages can be processed.

```elixir
def init(opts) do
  {:ok,
   %{
     eventstore: opts[:eventstore],
     pubsub_name: opts[:name]
   }, {:continue, :subscribe}}
end

#...#

def handle_continue(:subscribe, %{eventstore: eventstore} = state) do
  eventstore.subscribe("$all")

  {:noreply, state}
end

def handle_info({:subscribed, _subscription}, state), do: {:noreply, state}
```

A transient subscription is used since previous messages are not needed. The event store replies with a `{:subscribed, subscription}` message, which must also be handled. After this, the server will start receiving `{:events, events}` messages.

In Phoenix.PubSub 2.x the adapter owns local dispatch — it must call `Phoenix.PubSub.local_broadcast` itself rather than relying on the framework to do it after `broadcast/4` returns.

To avoid dispatching a local message twice (once from `broadcast/4` and once when the event arrives back from EventStore), a unique ID is added to the process state:

```elixir
def init(opts) do
  {:ok,
   %{
     id: generate_unique_id(opts),
     eventstore: opts[:eventstore],
     pubsub_name: opts[:name],
     serializer: opts[:serializer] || Phoenix.PubSub.EventStore.Serializer.Base64
   }, {:continue, :subscribe}}
end

defp generate_unique_id(opts) do
  unique_id_fn = opts[:unique_id_fn] || fn _name -> UUID.uuid4() end
  unique_id_fn.(opts[:name])
end
```

The `id` is added to the event's `metadata` field as `source_id`, keeping it separate from the message data. Serialization is delegated to the configurable `serializer` module. The `handle_call` for `:broadcast` becomes:

```elixir
event = %EventStore.EventData{
  event_type: to_string(serializer),
  data: serializer.serialize(message),
  metadata: Map.put(metadata, :source_id, id)
}
```

Where the value of `id` and `serializer` come from the state, and `metadata` already contains `dispatcher` and any `destination_node` for direct broadcasts. When an event arrives back, `source_id` identifies the origin node so duplicates can be skipped:

```elixir
def handle_info({:events, events}, state) do
  Enum.each(events, &local_broadcast_event(&1, state))

  {:noreply, state}
end

defp local_broadcast_event(
       %EventStore.RecordedEvent{
         data: data,
         metadata: metadata,
         stream_uuid: topic,
         event_type: event_type
       },
       %{id: id, serializer: serializer, pubsub_name: pubsub_name} = _state
     ) do
  current_node = to_string(node())

  %{source_id: source_id, destination_node: destination_node, dispatcher: dispatcher} =
    convert_metadata_keys_to_atoms(metadata)

  is_destination? = is_nil(destination_node) or destination_node == current_node

  if not is_nil(dispatcher) and is_destination? and source_id != id and
       event_type == to_string(serializer) do
    Phoenix.PubSub.local_broadcast(
      pubsub_name,
      topic,
      serializer.deserialize(data),
      maybe_convert_to_existing_atom(dispatcher)
    )
  end
end
```

That's it — a complete implementation of Phoenix PubSub using EventStore, including support for `direct_broadcast` via the `destination_node` metadata field and pluggable serialization.

The complete implementation can be found at [laszlohegedus/phoenix_pubsub_eventstore](https://github.com/laszlohegedus/phoenix_pubsub_eventstore).
