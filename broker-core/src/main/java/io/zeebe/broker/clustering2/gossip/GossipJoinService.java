package io.zeebe.broker.clustering2.gossip;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import io.zeebe.broker.transport.cfg.TransportComponentCfg;
import io.zeebe.gossip.Gossip;
import io.zeebe.servicecontainer.*;
import io.zeebe.transport.SocketAddress;

/**
 * Join / leave cluster on broker start / stop
 */
public class GossipJoinService implements Service<Object>
{
    private final Injector<Gossip> gossipInjector = new Injector<>();
    private final TransportComponentCfg config;
    private Gossip gossip;

    public GossipJoinService(TransportComponentCfg config)
    {
        this.config = config;
    }

    @Override
    public void start(ServiceStartContext startContext)
    {
        gossip = gossipInjector.getValue();

        final List<SocketAddress> initalContactPoints = Arrays.stream(config.gossip.initialContactPoints)
            .map(SocketAddress::from)
            .collect(Collectors.toList());

        startContext.async(gossip.join(initalContactPoints));
    }

    @Override
    public void stop(ServiceStopContext stopContext)
    {
        stopContext.async(gossip.leave());
    }

    @Override
    public Object get()
    {
        return null;
    }

    public Injector<Gossip> getGossipInjector()
    {
        return gossipInjector;
    }

}
