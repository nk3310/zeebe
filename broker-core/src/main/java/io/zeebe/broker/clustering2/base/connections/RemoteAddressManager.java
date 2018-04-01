package io.zeebe.broker.clustering2.base.connections;

import static io.zeebe.broker.clustering2.base.ClusterBaseLayerServiceNames.remoteAddressServiceName;
import static io.zeebe.broker.transport.TransportServiceNames.*;

import io.zeebe.broker.clustering2.base.topology.Topology;
import io.zeebe.broker.clustering2.base.topology.Topology.MemberInfo;
import io.zeebe.broker.clustering2.base.topology.TopologyManager;
import io.zeebe.broker.clustering2.base.topology.TopologyManager.TopologyMemberListener;
import io.zeebe.servicecontainer.*;
import io.zeebe.transport.RemoteAddress;

/**
 * Listens to topology member changes and adds / removes the remote addresses of the member's
 * management and replication apis on the corresponding client transports.
 */
public class RemoteAddressManager implements Service<Object>
{
    private final Injector<TopologyManager> topologyManagerInjector = new Injector<>();
    private BrokerMembershipListener membershipListener;

    @Override
    public void start(ServiceStartContext startContext)
    {
        membershipListener = new BrokerMembershipListener(startContext);

        topologyManagerInjector.getValue()
            .addTopologyMemberListener(membershipListener);
    }

    @Override
    public void stop(ServiceStopContext stopContext)
    {
        topologyManagerInjector.getValue()
            .removeTopologyMemberListener(membershipListener);
    }

    @Override
    public Object get()
    {
        return null;
    }

    public Injector<TopologyManager> getTopologyManagerInjector()
    {
        return topologyManagerInjector;
    }

    class BrokerMembershipListener implements TopologyMemberListener
    {
        private final ServiceStartContext serviceContext;

        BrokerMembershipListener(ServiceStartContext serviceContext)
        {
            this.serviceContext = serviceContext;
        }

        @Override
        public void onMemberAdded(MemberInfo memberInfo, Topology topology)
        {
            final ServiceName<RemoteAddress> managementRemoteAddrServiceName = remoteAddressServiceName(memberInfo.getManagementPort());
            final RemoteAddressService managementRemoteAddrService = new RemoteAddressService(memberInfo.getManagementPort());
            serviceContext.createService(managementRemoteAddrServiceName, managementRemoteAddrService)
                .dependency(clientTransport(MANAGEMENT_API_CLIENT_NAME), managementRemoteAddrService.getClientTransportInjector())
                .install();

            final ServiceName<RemoteAddress> replicationRemoteAddrServiceName = remoteAddressServiceName(memberInfo.getReplicationPort());
            final RemoteAddressService replicationRemoteAddrService = new RemoteAddressService(memberInfo.getReplicationPort());
            serviceContext.createService(replicationRemoteAddrServiceName, replicationRemoteAddrService)
                .dependency(clientTransport(REPLICATION_API_CLIENT_NAME), replicationRemoteAddrService.getClientTransportInjector())
                .install();
        }

        @Override
        public void onMemberRemoved(MemberInfo memberInfo, Topology topology)
        {
            final ServiceName<RemoteAddress> managementRemoteAddrServiceName = remoteAddressServiceName(memberInfo.getManagementPort());
            serviceContext.removeService(managementRemoteAddrServiceName);

            final ServiceName<RemoteAddress> replicationRemoteAddrServiceName = remoteAddressServiceName(memberInfo.getReplicationPort());
            serviceContext.removeService(replicationRemoteAddrServiceName);
        }
    }

}
