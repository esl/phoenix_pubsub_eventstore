# How to write a Phoenix PubSub adapter. Tutorial example based on EventStore

In distributed systems there is usually a need for the asynchronous transmission of messages to one or more services or processes. If you have used Phoenix you might have discovered that it provides a flexible way of solving this problem through a built-in pubsub framework called [Phoenix PubSub](https://hexdocs.pm/phoenix_pubsub/Phoenix.PubSub.html). Currently, it officially supports pubsub based on PG2 and Redis. It uses so called adapters to provide a pluggable interface for different pubsub implementations.

In this blog post, we are going to show you the main steps of implementing an adapter for Phoenix PubSub. The example is written using version 2.0.0.

A full implementation of the adapter which is based on [EventStore](https://hexdocs.pm/eventstore/EventStore.html) can be found on Github at [laszlohegedus/phoenix_pubsub_eventstore](https://github.com/laszlohegedus/phoenix_pubsub_eventstore). The code discussed in this post is available on the master branch, while a version that works with phoenix_pubsub 1.1.2 is on branch v1.1.

To learn more about Elixir and related technologies you might want to check out [ElixirConf EU Virtual](https://virtual.elixirconf.eu/) taking pace 18-19 June.

## Phoenix.PubSub.Adapter in a nutshell

A Phoenix PubSub adapter has to implement a few callbacks that are specified in the behaviour `Phoenix.PubSub.Adapter`:

- `node_name(adapter_name)`
- `child_spec(keyword)`
- `broadcast(adapter_name, topic, message, dispatcher)`
- `direct_broadcast(adapter_name, node_name, topic, message, dispatcher)`

### Node name

This function should return the node name as an atom or a binary. We did not discover too many uses for it, apart from the module `Phoenix.Tracker` and its implementations.

In most cases the following implementation should suffice:

```elixir
def node_name(nil), do: node()
def node_name(configured_name), do: configured_name
```

### Child spec

This function is used to generate the child spec for our adapter. Note that it is a default implementation for each GenServer, so usually it is not necessary to overwrite it.

### Broadcast

The function `broadcast` is called when a message is broadcast through `Phoenix.PubSub.broadcast`. The first paramater `adapter_name` is derived from the name we specify for the PubSub. We set the name (as an atom or module name) of the PubSub system when we initialize it. Note that the name of the PubSub is treated as a valid (not necessarily existing) module name, so it is better to follow the corresponding naming convention. The name of the adapter will come from the PubSub name with the suffix `.Adapter` added.

The `topic` and `message` parameters are self explanatory. The `dispatcher` is a module that is responsible for the local delivery of messages. It implements a `dispatch/3` function that will forward the messages to the subscribed processes.

### Direct broadcast

Direct Broadcast is similar to `broadcast` with an additional `node_name` parameter. When `direct_broadcast` is called, the message should only be broadcasted to subscribers on a given node.

## EventStore adapter

We will walk through a possible implementation of a Phoenix PubSub adapter that uses EventStore to distribute the messages between nodes. This gives us a solution that does not depend on Erlang/Elixir distribution. Additionally, we'll have an event log stored in case further analysis is needed.

Note that I did not perform any load tests on this solution and it is not production-ready, mainly a proof of concept and an aid for demonstration.

I mentioned above that we are going to use the latest master of [phoenixframework/phoenix_pubsub](https://github.com/phoenixframework/phoenix_pubsub) since it is cleaner and easier to use than the previous versions.

### Phoenix.PubSub

In order to know how our adapter should work, it is worth looking into the code of the module `Phoenix.Pubsub`. It is well documented and clean, so it doesn't take too long to understand what each function does.

The latest master version of Phoenix Pubsub makes use of [Registry](https://hexdocs.pm/elixir/Registry.html). Each subscription is an entry under the corresponding key in the registry associated with our PubSub adapter. That is, when we call `Phoenix.PubSub.subscribe(pubsub, topic, opts \\ [])`, a new entry is added to the registry with `Registry.register(pubsub, topic, opts[:metadata])`.

Duplicate subscriptions are allowed, but they will lead to duplicate delivery of messages. Unsubscribing from a topic removes all entries for the process under that topic.

The main functionality we are going to deal with is `Phoenix.Pubsub.broadcast` and the similar `Phoenix.Pubsub.direct_broadcast`. Whenever these functions are called, two main things happen:

1) The `broadcast` or `direct_broadcast` function is called on the corresponding PubSub adapter and
2) if successful, the message is dispatched to local processes through the default or overridden dispatch method.

This means that the main goal of our adapter's `broadcast` function is to make sure that the message gets delivered to the other nodes. In the case of a `direct_broadcast` the message should only be received by the subscribers on the given node and not others.

## The implementation

First, we create a GenServer called Phoenix.PubSub.EventStore so we can easily stitch it into the PubSub supervision tree. We want to give the user the flexibility to specify which EventStore to use. To do this, we will expose an eventstore option to pass the desired EventStore module to the PubSub. We are going to store this in the state along with the name of the current instance of the pubsub adapter (the option `name` in the pubsub config). We will need both in the future.

In order to use our PubSub we have to add it to our supervision tree. This can be done as follows:

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

Note the difference between `opts[:name]` and `opts[:adapter_name]`. The former is the name of the PubSub as a whole and is reserved for the Registry. Publishers use it when broadcasting messages. We can use `opts[:adapter_name]` as the name of our GenServer.

The implementation is fairly simple. We have to make sure that our adapter can be used to broadcast messages to all subscribers. For this, we will make use of the event store.

## Distributing a message as an event

The first thing our GenServer has to do is append a new message to the event store when `broadcast/4` is called.

```elixir
def broadcast(server, topic, message, dispatcher) do
  GenServer.call(server, {:broadcast, topic, message})
end

def handle_call(
      {:broadcast, topic, message},
      _from_pid,
      %{eventstore: eventstore} = state
    ) do
  event = %EventStore.EventData{...}

  res = eventstore.append_to_stream(topic, :any_version, [event])

  {:reply, res, state}
end
```

This is where we have to decide how we want to wrap the message inside an `%EventStore.EventData{}` struct. An easy solution is to serialise the message as one field. For this, we'll have to introduce a struct that will hold this field:

```elixir
defmodule Phoenix.PubSub.EventStore.Data do
  defstruct [:payload]
end
```

Then our message can be written as:

```elixir
event = %EventStore.EventData{
  event_type: "Elixir.Phoenix.PubSub.EventStore.Data",
  data: %Phoenix.PubSub.EventStore.Data{
    payload: Base.encode64(:erlang.term_to_binary(message))
  }
}
```

Note that EventStore converts the data to JSON and if we only used `:erlang.term_to_binary` then we would likely have invalid JSONs, so an additional base64 encoding is required. You may wonder why we need to convert the payload to a binary. This is needed because JSON cannot differentiate between atoms and strings, so each atom would appear as a string in the published message. If we want consistency, we have to make sure to serialise and deserialise the payload.

Another way to partially solve this would be to force the user to use structs when sending messages. That way EventStore would be able to do ser-des. Note that the keys remain atoms, but any value that was originally an atom will be converted to a string, so some post processing is still required.

Unless the messages are huge the base64 encoded binary should suffice.

## Handling events, local distribution

Now that the events are in the event store, any process that is subscribed to corresponding topics will receive them. First, we have to make sure that our GenServer (`Phoenix.PubSub.EventStore`) subscribes to all topics (`"$all"`). If you also want to use an event store for a different purpose, it's best to have a separate one for pubsub. We can easily do the subscription by sending a message to `self()`, then handling it in `handle_info` right after the server starts.

```elixir
def init(opts) do
  send(self(), :subscribe)

  {:ok,
   %{
     eventstore: opts[:eventstore],
     pubsub_name: opts[:name]
   }}
end

#...#

def handle_info(:subscribe, %{eventstore: eventstore} = state) do
  eventstore.subscribe("$all")

  {:noreply, state}
end

def handle_info({:subscribed, _subscription}, state), do: {:noreply, state}
```

We use a transient subscription, because we do not care about previous messages. The event store will reply with a `{:subscribed, subscription}` message, which we'll also have to handle. After this, the server will start receiving `{:events, events}` messages.

Note that when a message is broadcast on a node, it will be distributed to local subscribers by `Phoenix.PubSub` right after our adapter returns from `broadcast/4` as seen in the implementation:

```elixir
# from Phoenix.PubSub #
def broadcast(pubsub, topic, message, dispatcher \\ __MODULE__)
    when is_atom(pubsub) and is_binary(topic) and is_atom(dispatcher) do
  {:ok, {adapter, name}} = Registry.meta(pubsub, :pubsub)

  with :ok <- adapter.broadcast(name, topic, message, dispatcher) do
    dispatch(pubsub, :none, topic, message, dispatcher)
  end
end
```

So we have to make sure that a local message is not dispatched twice. For that we can add a unique ID (I went with `UUID.uuid1()`) to the process state:

```elixir
def init(opts) do
  send(self(), :subscribe)

  {:ok,
   %{
     id: UUID.uuid1(),
     eventstore: opts[:eventstore],
     pubsub_name: opts[:name]
   }}
end
```

Now, we can just add the `id` to the event before publishing it into the event store. I chose to put it in the `metadata` field, but we could also wrap it inside `data`. Although it's best to keep this information separate from the actual message. Finally, we'll have to change the `handle_call` for `:broadcast` and add the `id` to the event:

```elixir
event = %EventStore.EventData{
  event_type: "Elixir.Phoenix.PubSub.EventStore.Data",
  data: %Phoenix.PubSub.EventStore.Data{
    payload: Base.encode64(:erlang.term_to_binary(message))
  }
  metadata: %{source: id}
}
```

Where the value of `id` comes from the state. Now when we recive an event we know where it came from and we can decide whether it is needed to be dispatched to local subscribers.

```elixir
def handle_info({:events, events}, state) do
  Enum.each(events, &local_broadcast_event(&1, state))

  {:noreply, state}
end

defp local_broadcast_event(
       %EventStore.RecordedEvent{
         event_type: "Elixir.Phoenix.PubSub.EventStore.Data",
         data: %Phoenix.PubSub.EventStore.Data{
           payload: payload
         },
         metadata: metadata,
         stream_uuid: topic
       },
       %{id: id, pubsub_name: pubsub_name} = _state
     ) do
  case metadata do
    %{"source" => ^id} ->
      # This node is the source, nothing to do, because local dispatch already
      # happened.
      :ok

    _not_local ->
      # Otherwise broadcast locally
      message = :erlang.binary_to_term(Base.decode64(payload))

      Phoenix.PubSub.local_broadcast(
        pubsub_name,
        topic,
        message
      )
  end
end
```

That's it. This should give us enough to have a simple implementation of Phoenix Pubsub using EventStore. Note that the implementation of direct broadcast is still missing, which I solved by adding the destination node to the metadata field and extending the function `local_broadcast_event` to handle it. I also added support for handling the `dispatch` field during broadcasts.

For my complete implementation consult [laszlohegedus/phoenix_pubsub_eventstore](https://github.com/laszlohegedus/phoenix_pubsub_eventstore).
