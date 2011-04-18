/*
 * Copyright (C) 2005-2010 Alfresco Software Limited.
 *
 * This file is part of Alfresco
 *
 * Alfresco is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Alfresco is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with Alfresco. If not, see <http://www.gnu.org/licenses/>.
 */
package com.github.rholder.spring.transaction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.orm.hibernate3.SessionFactoryUtils;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationAdapter;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * Helper class to manage transaction synchronization. This provides helpers to
 * ensure that the necessary <code>TransactionSynchronization</code> instances
 * are registered on behalf of the application code.
 * 
 * @author Derek Hulley
 * @author Ray Holder, derived work modifications
 */
public abstract class TransactionBindingSupport
{    
    /**
     * The order of synchronization set to be 100 less than the Hibernate synchronization order
     */
    public static final int SESSION_SYNCHRONIZATION_ORDER =
        SessionFactoryUtils.SESSION_SYNCHRONIZATION_ORDER - 100;

    /** resource key to store the transaction synchronizer instance */
    private static final String RESOURCE_KEY_TXN_SYNCH = "txnSynch";
    
    private static Log logger = LogFactory.getLog(TransactionBindingSupport.class);
    
    /**
     * @return Returns the system time when the transaction started, or -1 if there is no current transaction.
     */
    public static long getTransactionStartTime()
    {
        /*
         * This method can be called outside of a transaction, so we can go direct to the synchronizations.
         */
        TransactionSynchronizationImpl txnSynch =
            (TransactionSynchronizationImpl) TransactionSynchronizationManager.getResource(RESOURCE_KEY_TXN_SYNCH);
        if (txnSynch == null)
        {
            if (TransactionSynchronizationManager.isSynchronizationActive())
            {
                // need to lazily register synchronizations
                return registerSynchronizations().getTransactionStartTime();
            }
            else
            {
                return -1;   // not in a transaction
            }
        }
        else
        {
            return txnSynch.getTransactionStartTime();
        }
    }
    
    /**
     * Get a unique identifier associated with each transaction of each thread.  Null is returned if
     * no transaction is currently active.
     * 
     * @return Returns the transaction ID, or null if no transaction is present
     */
    public static String getTransactionId()
    {
        /*
         * Go direct to the synchronizations as we don't want to register a resource if one doesn't exist.
         * This method is heavily used, so the simple Map lookup on the ThreadLocal is the fastest.
         */
        
        TransactionSynchronizationImpl txnSynch =
                (TransactionSynchronizationImpl) TransactionSynchronizationManager.getResource(RESOURCE_KEY_TXN_SYNCH);
        if (txnSynch == null)
        {
            if (TransactionSynchronizationManager.isSynchronizationActive())
            {
                // need to lazily register synchronizations
                return registerSynchronizations().getTransactionId();
            }
            else
            {
                return null;   // not in a transaction
            }
        }
        else
        {
            return txnSynch.getTransactionId();
        }
    }
    
    /**
     * 
     * @author Derek Hulley
     * @since 2.1.4
     */
    public static enum TxnReadState
    {
        /** No transaction is active */
        TXN_NONE,
        /** The current transaction is read-only */
        TXN_READ_ONLY,
        /** The current transaction supports writes */
        TXN_READ_WRITE
    }
    
    /**
     * @return      Returns the read-write state of the current transaction
     * @since 2.1.4
     */
    public static TxnReadState getTransactionReadState()
    {
        if (!TransactionSynchronizationManager.isSynchronizationActive())
        {
            return TxnReadState.TXN_NONE;
        }
        // Find the read-write state of the txn
        if (TransactionSynchronizationManager.isCurrentTransactionReadOnly())
        {
            return TxnReadState.TXN_READ_ONLY;
        }
        else
        {
            return TxnReadState.TXN_READ_WRITE;
        }
    }
    
    /**
     * Checks the state of the current transaction and throws an exception if a transaction
     * is not present or if the transaction is not read-write, if required.
     * 
     * @param requireReadWrite          <tt>true</tt> if the transaction must be read-write
     * 
     * @since 3.2
     */
    public static void checkTransactionReadState(boolean requireReadWrite)
    {
        if (!TransactionSynchronizationManager.isSynchronizationActive())
        {
            throw new IllegalStateException(
                    "The current operation requires an active " +
                    (requireReadWrite ? "read-write" : "") +
                    "transaction.");
        }
        if (TransactionSynchronizationManager.isCurrentTransactionReadOnly() && requireReadWrite)
        {
            throw new IllegalStateException("The current operation requires an active read-write transaction.");
        }
    }
    
    /**
     * Gets a resource associated with the current transaction, which must be active.
     * <p>
     * All necessary synchronization instances will be registered automatically, if required.
     * 
     *  
     * @param key the thread resource map key
     * @return Returns a thread resource of null if not present
     * 
     * @see TransactionalResourceHelper         for helper methods to create and bind common collection types
     */
    @SuppressWarnings("unchecked")
    public static <R extends Object> R getResource(Object key)
    {
        // get the synchronization
        TransactionSynchronizationImpl txnSynch = getSynchronization();
        // get the resource
        Object resource = txnSynch.resources.get(key);
        // done
        if (logger.isDebugEnabled())
        {
            logger.debug("Fetched resource: \n" +
                    "   key: " + key + "\n" +
                    "   resource: " + resource);
        }
        return (R) resource;
    }
    
    /**
     * Binds a resource to the current transaction, which must be active.
     * <p>
     * All necessary synchronization instances will be registered automatically, if required.
     * 
     * @param key
     * @param resource
     */
    public static void bindResource(Object key, Object resource)
    {
        // get the synchronization
        TransactionSynchronizationImpl txnSynch = getSynchronization();
        // bind the resource
        txnSynch.resources.put(key, resource);
        // done
        if (logger.isDebugEnabled())
        {
            logger.debug("Bound resource: \n" +
                    "   key: " + key + "\n" +
                    "   resource: " + resource);
        }
    }
    
    /**
     * Unbinds a resource from the current transaction, which must be active.
     * <p>
     * All necessary synchronization instances will be registered automatically, if required.
     * 
     * @param key
     */
    public static void unbindResource(Object key)
    {
        // get the synchronization
        TransactionSynchronizationImpl txnSynch = getSynchronization();
        // remove the resource
        txnSynch.resources.remove(key);
        // done
        if (logger.isDebugEnabled())
        {
            logger.debug("Unbound resource: \n" +
                    "   key: " + key);
        }
    }
    
    /**
     * Method that registers a <tt>LuceneIndexerAndSearcherFactory</tt> against
     * the transaction.
     * <p>
     * Setting this will ensure that the pre- and post-commit operations perform
     * the necessary cleanups against the <tt>LuceneIndexerAndSearcherFactory</tt>.
     * <p>
     * Although bound within a <tt>Set</tt>, it would still be better for the caller
     * to only bind once per transaction, if possible.
     * 
     * @param indexerAndSearcher the Lucene indexer to perform transaction completion
     *      tasks on
     */
    public static void bindListener(TransactionListener listener)
    {
        // get transaction-local synchronization
        TransactionSynchronizationImpl synch = getSynchronization();
        
        // bind the service in
        boolean bound = synch.addListener(listener);
        
        // done
        if (logger.isDebugEnabled())
        {
            logBoundService(listener, bound); 
        }
    }
    
    /**
     * Use as part of a debug statement
     * 
     * @param service the service to report 
     * @param bound true if the service was just bound; false if it was previously bound
     */
    private static void logBoundService(Object service, boolean bound)
    {
        if (bound)
        {
            logger.debug("Bound service: \n" +
                    "   transaction: " + getTransactionId() + "\n" +
                    "   service: " + service);
        }
        else
        {
            logger.debug("Service already bound: \n" +
                    "   transaction: " + getTransactionId() + "\n" +
                    "   service: " + service);
        }
    }

    /**
     * Gets the current transaction synchronization instance, which contains the locally bound
     * resources that are available to {@link #getResource(Object) retrieve} or
     * {@link #bindResource(Object, Object) add to}.
     * <p>
     * This method also ensures that the transaction binding has been performed.
     * 
     * @return Returns the common synchronization instance used
     */
    private static TransactionSynchronizationImpl getSynchronization()
    {
        // ensure synchronizations
        return registerSynchronizations();
    }
    
    /**
     * Binds the Alfresco-specific to the transaction resources
     * 
     * @return Returns the current or new synchronization implementation
     */
    private static TransactionSynchronizationImpl registerSynchronizations()
    {
        /*
         * No thread synchronization or locking required as the resources are all threadlocal
         */
        if (!TransactionSynchronizationManager.isSynchronizationActive())
        {
            Thread currentThread = Thread.currentThread();
            throw new RuntimeException("Transaction must be active and synchronization is required: " + currentThread);
        }
        TransactionSynchronizationImpl txnSynch =
            (TransactionSynchronizationImpl) TransactionSynchronizationManager.getResource(RESOURCE_KEY_TXN_SYNCH);
        if (txnSynch != null)
        {
            // synchronization already registered
            return txnSynch;
        }
        // we need a unique ID for the transaction
        String txnId = UUID.randomUUID().toString();
        // register the synchronization
        txnSynch = new TransactionSynchronizationImpl(txnId);
        TransactionSynchronizationManager.registerSynchronization(txnSynch);
        // register the resource that will ensure we don't duplication the synchronization
        TransactionSynchronizationManager.bindResource(RESOURCE_KEY_TXN_SYNCH, txnSynch);
        // done
        if (logger.isDebugEnabled())
        {
            logger.debug("Bound txn synch: " + txnSynch);
        }
        return txnSynch;
    }
    
    /**
     * Cleans out transaction resources if present
     */
    private static void clearSynchronization()
    {
        if (TransactionSynchronizationManager.hasResource(RESOURCE_KEY_TXN_SYNCH))
        {
            Object txnSynch = TransactionSynchronizationManager.unbindResource(RESOURCE_KEY_TXN_SYNCH);
            // done
            if (logger.isDebugEnabled())
            {
                logger.debug("Unbound txn synch:" + txnSynch);
            }
        }
    }
    
    /**
     * Helper method to rebind the synchronization to the transaction
     * 
     * @param txnSynch
     */
    private static void rebindSynchronization(TransactionSynchronizationImpl txnSynch)
    {
        TransactionSynchronizationManager.bindResource(RESOURCE_KEY_TXN_SYNCH, txnSynch);
        if (logger.isDebugEnabled())
        {
            logger.debug("Bound txn synch: " + txnSynch);
        }
    }
    
    /**
     * Handler of txn synchronization callbacks specific to internal
     * application requirements
     */
    private static class TransactionSynchronizationImpl extends TransactionSynchronizationAdapter
    {
        private long txnStartTime;
        private final String txnId;
        private final LinkedHashSet<TransactionListener> listeners;
        private final Map<Object, Object> resources;
        
        /**
         * Sets up the resource map
         * 
         * @param txnId
         */
        public TransactionSynchronizationImpl(String txnId)
        {
            this.txnStartTime = System.currentTimeMillis();
            this.txnId = txnId;
            listeners = new LinkedHashSet<TransactionListener>(5);
            resources = new HashMap<Object, Object>(17);
        }
        
        public long getTransactionStartTime()
        {
            return txnStartTime;
        }

        public String getTransactionId()
        {
            return txnId;
        }
        
        /**
         * @return Returns a set of <tt>TransactionListener<tt> instances that will be called
         *      during end-of-transaction processing
         */
        public boolean addListener(TransactionListener listener)
        {
            return listeners.add(listener);
        }
        
        /**
         * @return Returns the listeners in a list disconnected from the original set
         */
        private List<TransactionListener> getListenersIterable()
        {
            return new ArrayList<TransactionListener>(listeners);
        }

        public String toString()
        {
            StringBuilder sb = new StringBuilder(50);
            sb.append("TransactionSychronizationImpl")
              .append("[ txnId=").append(txnId)
              .append(", resources=").append(resources)
              .append("]");
            return sb.toString();
        }

        /**
         * @see TransactionBindingSupport#SESSION_SYNCHRONIZATION_ORDER
         */
        @Override
        public int getOrder()
        {
            return TransactionBindingSupport.SESSION_SYNCHRONIZATION_ORDER;
        }

        @Override
        public void suspend()
        {
            if (logger.isDebugEnabled())
            {
                logger.debug("Suspending transaction: " + this);
            }
            TransactionBindingSupport.clearSynchronization();
        }

        @Override
        public void resume()
        {
            if (logger.isDebugEnabled())
            {
                logger.debug("Resuming transaction: " + this);
            }
            TransactionBindingSupport.rebindSynchronization(this);
        }

        /**
         * Pre-commit cleanup.
         * <p>
         * Ensures that the session transaction listeners are property executed.
         * The Lucene indexes are then prepared.
         */
        @Override
        public void beforeCommit(boolean readOnly)
        {
            if (logger.isDebugEnabled())
            {
                logger.debug("Before commit " + (readOnly ? "read-only" : "" ) + ": " + this);
            }
            // get the txn ID
            TransactionSynchronizationImpl synch = (TransactionSynchronizationImpl)
                    TransactionSynchronizationManager.getResource(RESOURCE_KEY_TXN_SYNCH);
            if (synch == null)
            {
                throw new RuntimeException("No synchronization bound to thread");
            }

            // These are still considered part of the transaction so are executed here
            doBeforeCommit(readOnly);
            
        }
        
        /**
         * Execute the beforeCommit event handlers for the registered listeners
         * 
         * @param readOnly  is read only
         */
        private void doBeforeCommit(boolean readOnly)
        {
            doBeforeCommit(new HashSet<TransactionListener>(listeners.size()), readOnly);
        }
        
        /**
         * Executes the beforeCommit event handlers for the outstanding listeners.
         * This process is iterative as the process of calling listeners may lead to more listeners
         * being added.  The new listeners will be processed until there no listeners remaining.
         * 
         * @param visitedListeners  a set containing the already visited listeners
         * @param readOnly          is read only
         */
        private void doBeforeCommit(Set<TransactionListener> visitedListeners, boolean readOnly)
        {
            Set<TransactionListener> pendingListeners = new HashSet<TransactionListener>(listeners);
            pendingListeners.removeAll(visitedListeners);
            
            if (pendingListeners.size() != 0)
            {
                for (TransactionListener listener : pendingListeners) 
                {
                    listener.beforeCommit(readOnly);
                    visitedListeners.add(listener);
                }
                
                doBeforeCommit(visitedListeners, readOnly);
            }
        }
        
        @Override
        public void beforeCompletion()
        {
            if (logger.isDebugEnabled())
            {
                logger.debug("Before completion: " + this);
            }
            // notify listeners
            for (TransactionListener listener : getListenersIterable())
            {
                listener.beforeCompletion();
            }
        }
               

        @Override
        public void afterCompletion(int status)
        {
            String statusStr = "unknown";
            switch (status)
            {
                case TransactionSynchronization.STATUS_COMMITTED:
                    statusStr = "committed";
                    break;
                case TransactionSynchronization.STATUS_ROLLED_BACK:
                    statusStr = "rolled-back";
                    break;
                default:
            }
            if (logger.isDebugEnabled())
            {
                logger.debug("After completion (" + statusStr + "): " + this);
            }
                        
            List<TransactionListener> iterableListeners = getListenersIterable();
            // notify listeners
            if (status  == TransactionSynchronization.STATUS_COMMITTED)
            {
                for (TransactionListener listener : iterableListeners)
                {
                    try
                    {
                        listener.afterCommit();
                    }
                    catch (RuntimeException e)
                    {
                        logger.error("After completion (" + statusStr + ") listener exception: \n" +
                                "   listener: " + listener,
                                e);
                    }
                }
            }
            else
            {
                for (TransactionListener listener : iterableListeners)
                {
                    try
                    {
                        listener.afterRollback();
                    }
                    catch (RuntimeException e)
                    {
                        logger.error("After completion (" + statusStr + ") listener exception: \n" +
                                "   listener: " + listener,
                                e);
                    }
                }
            }
            
            // clear the thread's registrations and synchronizations
            TransactionBindingSupport.clearSynchronization();
        }
    }
}
