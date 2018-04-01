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
package io.zeebe.broker.clustering2.base;

import io.zeebe.broker.clustering2.base.raft.config.RaftPersistentConfigurationManager;
import io.zeebe.broker.clustering2.base.topology.TopologyManager;
import io.zeebe.gossip.Gossip;
import io.zeebe.raft.Raft;
import io.zeebe.servicecontainer.ServiceName;
import io.zeebe.transport.RemoteAddress;
import io.zeebe.transport.SocketAddress;

public class ClusterBaseLayerServiceNames
{
    public static final ServiceName<ClusterBaseLayerBootstrap> CLUSTERING_BOOTSTRAP = ServiceName.newServiceName("cluster.base.bootstrapped", ClusterBaseLayerBootstrap.class);

    public static final ServiceName<TopologyManager> TOPOLOGY_MANAGER_SERVICE = ServiceName.newServiceName("cluster.base.topologyManager", TopologyManager.class);
    public static final ServiceName<Object> REMOTE_ADDRESS_MANAGER_SERVICE = ServiceName.newServiceName("cluster.base.remoteAddrManager", Object.class);

    public static final ServiceName<Gossip> GOSSIP_SERVICE = ServiceName.newServiceName("cluster.base.gossip", Gossip.class);
    public static final ServiceName<Object> GOSSIP_JOIN_SERVICE = ServiceName.newServiceName("cluster.base.gossip.join", Object.class);

    public static final ServiceName<Object> RAFT_BOOTSTRAP_SERVICE = ServiceName.newServiceName("cluster.base.raft.bootstrap", Object.class);
    public static final ServiceName<RaftPersistentConfigurationManager> RAFT_CONFIGURATION_MANAGER = ServiceName.newServiceName("cluster.base.raft.configurationManager", RaftPersistentConfigurationManager.class);
    public static final ServiceName<Raft> RAFT_SERVICE_GROUP = ServiceName.newServiceName("cluster.base.raft.service", Raft.class);

    public static ServiceName<RemoteAddress> remoteAddressServiceName(SocketAddress socketAddress)
    {
        return ServiceName.newServiceName(String.format("cluster.base.remoteAddress.%s", socketAddress.toString()), RemoteAddress.class);
    }

    public static ServiceName<Raft> raftServiceName(final String topicName, int partitionId)
    {
        return ServiceName.newServiceName(String.format("cluster.base.raft.%s-%d", topicName, partitionId), Raft.class);
    }

}
