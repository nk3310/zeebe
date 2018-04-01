/*
 * Zeebe Broker Core
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.zeebe.broker.clustering2;

import static io.zeebe.broker.clustering2.base.ClusterBaseLayerServiceNames.*;

import io.zeebe.broker.clustering2.base.ClusterBaseLayerBootstrap;
import io.zeebe.broker.clustering2.base.connections.RemoteAddressManager;
import io.zeebe.broker.clustering2.base.gossip.GossipJoinService;
import io.zeebe.broker.clustering2.base.gossip.GossipService;
import io.zeebe.broker.clustering2.base.raft.RaftBootstrapService;
import io.zeebe.broker.clustering2.base.raft.config.RaftPersistentConfigurationManagerService;
import io.zeebe.broker.clustering2.base.topology.TopologyManagerService;
import io.zeebe.broker.system.*;
import io.zeebe.broker.transport.TransportServiceNames;
import io.zeebe.broker.transport.cfg.TransportComponentCfg;
import io.zeebe.servicecontainer.*;

public class ClusterComponent implements Component
{
    @Override
    public void init(final SystemContext context)
    {
        final ServiceContainer serviceContainer = context.getServiceContainer();
        final ConfigurationManager configurationManager = context.getConfigurationManager();
        final TransportComponentCfg config = configurationManager.readEntry("network", TransportComponentCfg.class);

        initClusteringBaseLayer(context, serviceContainer, config);
    }

    private void initClusteringBaseLayer(final SystemContext context, final ServiceContainer serviceContainer, final TransportComponentCfg config)
    {
        final ClusterBaseLayerBootstrap clusteringBootstrap = new ClusterBaseLayerBootstrap();
        final ServiceBuilder<ClusterBaseLayerBootstrap> bootstrapBuilder = serviceContainer.createService(CLUSTERING_BOOTSTRAP, clusteringBootstrap);
        ClusterBaseLayerBootstrap.addDepenedencies(bootstrapBuilder);
        context.addRequiredStartAction(bootstrapBuilder.install());

        final TopologyManagerService topologyManagerService = new TopologyManagerService(config);
        serviceContainer.createService(TOPOLOGY_MANAGER_SERVICE, topologyManagerService)
            .dependency(GOSSIP_SERVICE, topologyManagerService.getGossipInjector())
            .groupReference(RAFT_SERVICE_GROUP, topologyManagerService.getRaftReference())
            .install();

        final RemoteAddressManager remoteAddressManager = new RemoteAddressManager();
        serviceContainer.createService(REMOTE_ADDRESS_MANAGER_SERVICE, remoteAddressManager)
            .dependency(TOPOLOGY_MANAGER_SERVICE, remoteAddressManager.getTopologyManagerInjector())
            .install();

        initGossip(serviceContainer, config);
        initRaft(serviceContainer, config);
    }

    private void initGossip(final ServiceContainer serviceContainer, final TransportComponentCfg config)
    {
        final GossipService gossipService = new GossipService(config);
        serviceContainer.createService(GOSSIP_SERVICE, gossipService)
            .dependency(TransportServiceNames.clientTransport(TransportServiceNames.MANAGEMENT_API_CLIENT_NAME), gossipService.getClientTransportInjector())
            .dependency(TransportServiceNames.bufferingServerTransport(TransportServiceNames.MANAGEMENT_API_SERVER_NAME), gossipService.getBufferingServerTransportInjector())
            .install();

        final GossipJoinService gossipJoinService = new GossipJoinService(config);
        serviceContainer.createService(GOSSIP_JOIN_SERVICE, gossipJoinService)
            .dependency(GOSSIP_SERVICE, gossipJoinService.getGossipInjector())
            .install();
    }

    private void initRaft(final ServiceContainer serviceContainer, final TransportComponentCfg config)
    {
        final RaftPersistentConfigurationManagerService raftConfigurationManagerService = new RaftPersistentConfigurationManagerService(config);
        serviceContainer.createService(RAFT_CONFIGURATION_MANAGER, raftConfigurationManagerService)
            .install();

        final RaftBootstrapService raftBootstrapService = new RaftBootstrapService();
        serviceContainer.createService(RAFT_BOOTSTRAP_SERVICE, raftBootstrapService)
            .dependency(RAFT_CONFIGURATION_MANAGER, raftBootstrapService.getConfigurationManagerInjector())
            .install();
    }


}
