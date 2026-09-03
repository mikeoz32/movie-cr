# Cluster Receptionist

[Documentation index](README.md) · [Cluster membership](cluster.md) · [Cluster sharding](sharding.md) · [Cluster singleton](singleton.md)

`Movie::ClusterReceptionist` is an Akka-style typed service-discovery extension above remoting and cluster membership. It replicates actor registrations across member incarnations and returns typed references without deploying actors, choosing an owner, or changing cluster membership.

Use the receptionist when a logical service may have zero, one, or many interchangeable actor instances and callers need to discover the current set. Use [cluster sharding](sharding.md) for one logical owner per entity id, and [cluster singleton](singleton.md) for exactly one active cluster-wide coordinator.

## Register and discover

Enable remoting and cluster membership first. Every node that discovers or registers a key must use the same key name and concrete wire message type:

```crystal
Movie::Remote::MessageRegistry.register(WorkerCommand)
Movie::Remote::MessageRegistry.register(WorkerReply)

system.enable_remoting("0.0.0.0", 2551)
cluster = system.enable_cluster(cluster_settings)
cluster.await_up

workers = Movie::Cluster::ServiceKey(WorkerCommand).new("workers")
receptionist = Movie::ClusterReceptionist.get(system)
worker = system.spawn(Worker.new, name: "worker-1")
receptionist.register(workers, worker)

listing = receptionist.find(workers)
listing.services.each { |service| service << WorkerCommand.new("job-42") }
```

`ServiceRef(T)` supports typed `<<`, `tell_from`, `ask`, and supported system messages for both local and remote actors. Listings are defensive and sorted by canonical actor path. `register` and `deregister` return false for an already satisfied operation. Reusing one key name with another message type raises `ReceptionistConfigurationError` locally and incompatible replicated state is rejected.

Only local actor refs may be registered. The extension watches each registered actor and removes all of its registrations after termination. Registrations are ephemeral: applications recreate actors and register them again after process restart.

## Subscribe to listings

A local typed subscriber receives the current listing immediately and every subsequent effective registry or membership change:

```crystal
class WorkerListingObserver < Movie::AbstractBehavior(
  Movie::Cluster::ReceptionistListing(WorkerCommand)
)
  def receive(message, context)
    puts "available workers: #{message.services.size}"
    Movie::Behaviors(Movie::Cluster::ReceptionistListing(WorkerCommand)).same
  end
end

observer = system.spawn(WorkerListingObserver.new, name: "worker-listings")
receptionist.subscribe(workers, observer)
receptionist.unsubscribe(workers, observer)
```

Stopped subscribers are removed automatically. Listing delivery is local actor delivery and may coalesce only at the application level; the receptionist publishes each effective update it observes.

## Replication and failure semantics

Each node owns a complete local registration set with a monotonically increasing revision. Changes are broadcast immediately and a 250 ms anti-entropy round retransmits the current full state. Incoming state is accepted only when its cluster name, daemon actor path, transport address, process UID, registration owner, capacity, and service-key message types agree. Equal revisions with different contents fail closed.

The per-node state is bounded at 10,000 registrations; each process accepts at most 100,000 known registrations, 100,000 remembered service keys, and 10,000 local subscriptions. Service keys are limited to 256 bytes, message type names to 512 bytes, and actor paths to 4,096 bytes. Capacity overflow fails closed. Wire envelopes use the remoting registered direct JSON path.

Listings include only members currently observed as `Up` and reachable from the querying node. An unreachable service disappears locally without being deleted or causing automatic downing, and it may return when reachability is restored. `Leaving` members stop appearing in new listings; `Down` and `Removed` incarnations are purged. A process restarted at the same host and port has a new node UID and cannot resurrect the old incarnation's registrations.

Discovery does not make actor delivery reliable. Remote tells remain at-most-once and ask timeouts remain ambiguous. Use `remoting.shared-secret` outside isolated networks so association identity is authenticated, and retain the cluster rule that reachability alone never decides which partition may continue.

## Telemetry and verification

`receptionist.stats` reports current local registrations, replicated node states, visible services, subscribers, registration/deregistration totals, sync rounds and messages, listing updates, purged nodes, and rejected protocol state.

```bash
crystal spec spec/movie/cluster/receptionist_spec.cr \
  -Dpreview_mt -Dexecution_context

MOVIE_RECEPTIONIST_STRESS=1 crystal spec \
  spec/movie/cluster/receptionist_stress_spec.cr \
  -Dpreview_mt -Dexecution_context
```

The opt-in real-process scenario covers remote discovery and ask, temporary unreachability plus restoration without re-registration, graceful removal, abrupt loss plus explicit downing, and same-address restart with a new UID. See [`examples/cluster_receptionist_example.cr`](../../examples/cluster_receptionist_example.cr) for a runnable two-node example.
