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
package io.zeebe.broker.task;

import io.zeebe.broker.logstreams.processor.StreamProcessorServiceFactory;
import io.zeebe.logstreams.log.LogStream;
import io.zeebe.servicecontainer.*;
import io.zeebe.transport.ServerTransport;
import io.zeebe.util.sched.ActorScheduler;
import io.zeebe.util.sched.future.ActorFuture;

public class TaskSubscriptionManagerService implements Service<TaskSubscriptionManager>
{
    protected final Injector<ServerTransport> transportInjector = new Injector<>();
    protected final Injector<StreamProcessorServiceFactory> streamProcessorServiceFactoryInjector = new Injector<>();

    protected final ServiceContainer serviceContainer;

    protected TaskSubscriptionManager service;

    protected final ServiceGroupReference<LogStream> logStreamsGroupReference = ServiceGroupReference.<LogStream>create()
        .onAdd((name, stream) -> service.addStream(stream, name))
        .onRemove((name, stream) -> service.removeStream(stream))
        .build();

    public TaskSubscriptionManagerService(ServiceContainer serviceContainer)
    {
        this.serviceContainer = serviceContainer;
    }

    @Override
    public void start(ServiceStartContext startContext)
    {
        final ServerTransport clientApiTransport = transportInjector.getValue();
        final StreamProcessorServiceFactory streamProcessorServiceFactory = streamProcessorServiceFactoryInjector.getValue();

        final ActorScheduler actorScheduler = startContext.getScheduler();
        service = new TaskSubscriptionManager(serviceContainer, streamProcessorServiceFactory, clientApiTransport);
        actorScheduler.submitActor(service);

        final ActorFuture<Void> transportRegistration = clientApiTransport.registerChannelListener(service);
        startContext.async(transportRegistration);
    }

    @Override
    public void stop(ServiceStopContext stopContext)
    {
    }

    @Override
    public TaskSubscriptionManager get()
    {
        return service;
    }

    public ServiceGroupReference<LogStream> getLogStreamsGroupReference()
    {
        return logStreamsGroupReference;
    }

    public Injector<ServerTransport> getClientApiTransportInjector()
    {
        return transportInjector;
    }

    public Injector<StreamProcessorServiceFactory> getStreamProcessorServiceFactoryInjector()
    {
        return streamProcessorServiceFactoryInjector;
    }

}
