package io.zeebe.broker.clustering2.base.raft.config;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import io.zeebe.broker.transport.cfg.TransportComponentCfg;
import io.zeebe.servicecontainer.*;

public class RaftPersistentConfigurationManagerService implements Service<RaftPersistentConfigurationManager>
{
    private final TransportComponentCfg config;
    private RaftPersistentConfigurationManager service;

    public RaftPersistentConfigurationManagerService(TransportComponentCfg config)
    {
        this.config = config;
    }

    @Override
    public void start(ServiceStartContext startContext)
    {
        final File configDirectory = new File(config.management.directory);

        if (!configDirectory.exists())
        {
            try
            {
                configDirectory.getParentFile().mkdirs();
                Files.createDirectory(configDirectory.toPath());
            }
            catch (final IOException e)
            {
                throw new RuntimeException("Unable to create directory " + configDirectory, e);
            }
        }

        service = new RaftPersistentConfigurationManager(configDirectory);

        startContext.async(startContext.getScheduler().submitActor(service));
    }

    @Override
    public void stop(ServiceStopContext stopContext)
    {
        stopContext.async(service.close());
    }

    @Override
    public RaftPersistentConfigurationManager get()
    {
        return service;
    }

}
