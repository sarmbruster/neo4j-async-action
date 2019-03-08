package org.neo4j.asyncaction;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.ExtensionType;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.internal.LogService;

/**
 * @author Stefan Armbruster
 */
public class AsyncQueueHolderKernelExtensionFactory extends KernelExtensionFactory<AsyncQueueHolderKernelExtensionFactory.Dependencies> {

    public interface Dependencies {
        GraphDatabaseAPI graphdatabaseAPI();
        Config config();
        LogService log();
        ThreadToStatementContextBridge threadToStatementContextBridge();
    }

    public AsyncQueueHolderKernelExtensionFactory() {
        super( ExtensionType.DATABASE, "AsyncQueueHolder");
    }

    @Override
    public Lifecycle newInstance(KernelContext context, Dependencies dependencies) {
        return new AsyncQueueHolder(
                dependencies.graphdatabaseAPI(),
                dependencies.config(),
                dependencies.log(),
                dependencies.threadToStatementContextBridge()
        );
    }

}
