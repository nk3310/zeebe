package io.zeebe.broker.clustering2.base.topology;

import io.zeebe.broker.clustering2.base.topology.Topology.MemberInfo;
import io.zeebe.broker.transport.cfg.TransportComponentCfg;
import io.zeebe.gossip.Gossip;
import io.zeebe.raft.Raft;
import io.zeebe.servicecontainer.*;
import io.zeebe.transport.SocketAddress;

public class TopologyManagerService implements Service<TopologyManager>
{
    private TopologyManager topologyManager;

    private final Injector<Gossip> gossipInjector = new Injector<>();

    private final ServiceGroupReference<Raft> raftReference = ServiceGroupReference.<Raft>create()
        .onAdd((name, raft) -> topologyManager.onRaftStarted(raft))
        .onRemove((name, raft) -> topologyManager.onRaftStopped(raft))
        .build();

    private final MemberInfo localBrokerInfo;

    public TopologyManagerService(TransportComponentCfg cfg)
    {
        final String defaultHost = cfg.host;

        final SocketAddress managementApi = cfg.managementApi.toSocketAddress(defaultHost);
        final SocketAddress clientApi = cfg.clientApi.toSocketAddress(defaultHost);
        final SocketAddress replicationApi = cfg.replicationApi.toSocketAddress(defaultHost);

        localBrokerInfo = new MemberInfo(clientApi, managementApi, replicationApi);
    }

    @Override
    public void start(ServiceStartContext startContext)
    {
        final Gossip gossip = gossipInjector.getValue();

        topologyManager = new TopologyManager(gossip, localBrokerInfo);

        startContext.async(startContext.getScheduler().submitActor(topologyManager));
    }

    @Override
    public void stop(ServiceStopContext stopContext)
    {
        stopContext.async(topologyManager.close());
    }

    @Override
    public TopologyManager get()
    {
        return topologyManager;
    }

    public ServiceGroupReference<Raft> getRaftReference()
    {
        return raftReference;
    }

    public Injector<Gossip> getGossipInjector()
    {
        return gossipInjector;
    }
}
