package io.zeebe.broker.clustering2.base.raft;

import java.util.List;

import io.zeebe.broker.clustering2.base.raft.config.RaftPersistentConfiguration;
import io.zeebe.broker.clustering2.base.raft.config.RaftPersistentConfigurationManager;
import io.zeebe.servicecontainer.*;

/**
 * Starts all locally available Rafts on broker start
 */
public class RaftBootstrapService implements Service<Object>
{
    private final Injector<RaftPersistentConfigurationManager> configurationManagerInjector = new Injector<>();

    @Override
    public void start(ServiceStartContext startContext)
    {
        final RaftPersistentConfigurationManager configurationManager = configurationManagerInjector.getValue();

        startContext.run(() ->
        {
            final List<RaftPersistentConfiguration> configurations = configurationManager.getConfigurations().join();

            for (RaftPersistentConfiguration configuration : configurations)
            {

            }
        });
    }

    @Override
    public void stop(ServiceStopContext stopContext)
    {

    }

    @Override
    public Object get()
    {
        return null;
    }

    public Injector<RaftPersistentConfigurationManager> getConfigurationManagerInjector()
    {
        return configurationManagerInjector;
    }
}
