package io.zeebe.broker.clustering2.base.connections;

import io.zeebe.servicecontainer.*;
import io.zeebe.transport.*;

/**
 * Registers / retires remote addresses as members / join leave the cluster
 */
public class RemoteAddressService implements Service<RemoteAddress>
{
    private final Injector<ClientTransport> clientTransportInjector = new Injector<>();
    private final SocketAddress socketAddress;
    private ClientTransport clientTransport;
    private RemoteAddress remoteAddress;

    public RemoteAddressService(SocketAddress socketAddress)
    {
        this.socketAddress = socketAddress;
    }

    @Override
    public void start(ServiceStartContext startContext)
    {
        clientTransport = clientTransportInjector.getValue();
        remoteAddress = clientTransport.registerRemoteAddress(socketAddress);
    }

    @Override
    public void stop(ServiceStopContext stopContext)
    {
        clientTransport.deactivateRemoteAddress(remoteAddress);
    }

    @Override
    public RemoteAddress get()
    {
        return remoteAddress;
    }

    public Injector<ClientTransport> getClientTransportInjector()
    {
        return clientTransportInjector;
    }
}
