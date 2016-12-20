package org.neo4j.asyncaction;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.lifecycle.Lifecycle;

/**
 * @author Stefan Armbruster
 */
public class AsyncQueueHolderKernelExtensionFactory extends KernelExtensionFactory<AsyncQueueHolderKernelExtensionFactory.Dependencies> {

    public interface Dependencies {
        GraphDatabaseService getGraphDatabaseService();
//        Config getConfig();
        LogService getLogService();
    }

    public AsyncQueueHolderKernelExtensionFactory() {
        super("AsyncQueueHolder");
    }

    @Override
    public Lifecycle newInstance(KernelContext context, Dependencies dependencies) throws Throwable {
        return new AsyncQueueHolder(dependencies.getGraphDatabaseService(), dependencies.getLogService());
    }

}
