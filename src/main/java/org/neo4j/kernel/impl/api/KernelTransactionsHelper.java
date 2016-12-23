package org.neo4j.kernel.impl.api;

import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.KernelTransactionHandle;

/**
 * since {@link KernelTransactions#createHandle(KernelTransactionImplementation)} is package scoped we've put that helper
 * into the same package to work around that limitation without requiring to use reflection
 */
public abstract class KernelTransactionsHelper {

    public static KernelTransactionHandle getHandle(KernelTransactions kernelTransactions, KernelTransaction kernelTransaction) {
        return kernelTransactions.createHandle((KernelTransactionImplementation) kernelTransaction);
    }

}