package io.zeebe.broker.clustering2.base;

import static io.zeebe.broker.clustering2.base.ClusterBaseLayerServiceNames.*;

import java.util.Arrays;
import java.util.List;

import io.zeebe.broker.Loggers;
import io.zeebe.servicecontainer.*;
import org.slf4j.Logger;

/**
 * Marker service which depends on all services installed during
 * the bootstrapping of the cluster base layer:
 *
 * <ul>
 * <li>Gossip started (but not joined yet)</li>
 * <li>Rafts are started from configuration (does not block on join)</li>
 * <li>Topology manager is started</li>
 * <li>Remote address manager is started</li>
 * </ul>
 *
 * You can put a dependency on this service if your service should be available after the clustering
 * base infrastructure is available. Example: clustering base infra should be bootstrapped before we
 * start accepting requests on the management api.
 */
public class ClusterBaseLayerBootstrap implements Service<ClusterBaseLayerBootstrap>
{
    public static final List<ServiceName<?>> DEPENDENCIES = Arrays.asList(GOSSIP_SERVICE,
        REMOTE_ADDRESS_MANAGER_SERVICE,
        RAFT_BOOTSTRAP_SERVICE,
        TOPOLOGY_MANAGER_SERVICE);

    private static final Logger LOG = Loggers.CLUSTERING_LOGGER;

    @Override
    public void start(ServiceStartContext startContext)
    {
        LOG.info("Successfully started all clustering services");
    }

    @Override
    public void stop(ServiceStopContext stopContext)
    {
        // no-op
    }

    @Override
    public ClusterBaseLayerBootstrap get()
    {
        return null;
    }

    public static void addDepenedencies(ServiceBuilder<ClusterBaseLayerBootstrap> builder)
    {
        for (ServiceName<?> serviceName : DEPENDENCIES)
        {
            builder.dependency(serviceName);
        }
    }

}
