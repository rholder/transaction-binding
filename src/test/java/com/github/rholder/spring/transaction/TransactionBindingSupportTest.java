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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

import com.github.rholder.spring.transaction.TransactionBindingSupport.TxnReadState;

/**
 * Tests integration between our <tt>UserTransaction</tt> implementation and
 * our <tt>TransactionManager</tt>.
 * 
 * @see org.alfresco.repo.transaction.AlfrescoTransactionManager
 * @see org.alfresco.util.transaction.SpringAwareUserTransaction
 * 
 * @author Derek Hulley
 * @author Ray Holder, derived work modifications
 */
public class TransactionBindingSupportTest {

    private static PlatformTransactionManager transactionManager;
    
    private TransactionTemplate transactionTemplate;
    
    private String txnId;
    private String txnIdCheck;
    
    // TODO clean up these lazy init hacks with proper wiring
    @BeforeClass
    public static void beforeThisClass() {
        ApplicationContext applicationContext = new ClassPathXmlApplicationContext("/applicationContext-test.xml");        
        transactionManager = (PlatformTransactionManager) applicationContext.getBean("transactionManager");
    }
    
    @Before
    public void beforeEachTest() {
        transactionTemplate = new TransactionTemplate(transactionManager);
    }

    @Test
    public void testTransactionId() throws Exception {

        // turn on nested transactions for this one
        transactionTemplate.setPropagationBehavior(TransactionTemplate.PROPAGATION_REQUIRES_NEW);
        
        Assert.assertNull("Thread shouldn't have a txn ID", TransactionBindingSupport.getTransactionId());
        Assert.assertEquals("No transaction start time expected", -1, TransactionBindingSupport.getTransactionStartTime());

        transactionTemplate.execute(new TransactionCallback() {
            
            public Object doInTransaction(TransactionStatus status) {
                // begin the txn
                txnId = TransactionBindingSupport.getTransactionId();
                Assert.assertNotNull("Expected thread to have a txn id", txnId);
                final long txnStartTime = TransactionBindingSupport.getTransactionStartTime();
                Assert.assertTrue("Expected a transaction start time", txnStartTime > 0);
                
                // check that the txn id and time doesn't change
                txnIdCheck = TransactionBindingSupport.getTransactionId();
                Assert.assertEquals("Transaction ID changed on same thread", txnId, txnIdCheck);
                long txnStartTimeCheck = TransactionBindingSupport.getTransactionStartTime();
                Assert.assertEquals("Transaction start time changed on same thread", txnStartTime, txnStartTimeCheck);
                
                // begin a new, inner transaction
                {
                    transactionTemplate.execute(new TransactionCallback() {
                        
                        public Object doInTransaction(TransactionStatus status) {
                            // check the ID for the outer transaction
                            String txnIdInner = TransactionBindingSupport.getTransactionId();
                            Assert.assertNotSame("Inner txn ID must be different from outer txn ID", txnIdInner, txnId);
                            // Check the time against the outer transaction
                            long txnStartTimeInner = TransactionBindingSupport.getTransactionStartTime();
                            Assert.assertTrue(
                                    "Inner transaction start time should be greater or equal (accuracy) to the outer's",
                                    txnStartTime <= txnStartTimeInner);
                            // rollback the nested txn
                            status.setRollbackOnly();
                            return null;
                        }
                    });
                    
                    txnIdCheck = TransactionBindingSupport.getTransactionId();
                    Assert.assertEquals("Txn ID not popped inner txn completion", txnId, txnIdCheck);
                }
                
                // rollback the outer transaction
                status.setRollbackOnly();
                return null;
            }
        });

        Assert.assertNull("Thread shouldn't have a txn ID after rollback", TransactionBindingSupport.getTransactionId());
        
        // start a new transaction
        transactionTemplate.execute(new TransactionCallback() {
            
            public Object doInTransaction(TransactionStatus status) {
                txnIdCheck = TransactionBindingSupport.getTransactionId();
                Assert.assertNotSame("New transaction has same ID", txnId, txnIdCheck);

                // rollback
                status.setRollbackOnly();
                return null;
            }
        });
        
        Assert.assertNull("Thread shouldn't have a txn ID after rollback", TransactionBindingSupport.getTransactionId());
    }
    
    @Test
    public void testListener() throws Exception {
        final List<String> strings = new ArrayList<String>(1);

        // anonymous inner class to test it
        final TransactionListener listener = new TransactionListener() {
            public void beforeCommit(boolean readOnly) {
                strings.add("beforeCommit");
            }

            public void beforeCompletion() {
                strings.add("beforeCompletion");
            }

            public void afterCommit() {
                strings.add("afterCommit");
            }

            public void afterRollback() {
                strings.add("afterRollback");
            }
        };
        
        // begin a transaction
        transactionTemplate.execute(new TransactionCallback() {
            
            public Object doInTransaction(TransactionStatus status) {
                // register it
                TransactionBindingSupport.bindListener(listener);
                return null;
            }
        });
        
        // test commit after return
        Assert.assertTrue("beforeCommit not called on listener", strings.contains("beforeCommit"));
        Assert.assertTrue("beforeCompletion not called on listener", strings.contains("beforeCompletion"));
        Assert.assertTrue("afterCommit not called on listener", strings.contains("afterCommit"));
        Assert.assertEquals("Unexpected size for collected listener logging", 3, strings.size());
        
        // reset logging
        strings.clear();
        
        // begin a transaction
        transactionTemplate.execute(new TransactionCallback() {
            
            public Object doInTransaction(TransactionStatus status) {
                // register it
                TransactionBindingSupport.bindListener(listener);
                
                // rollback
                status.setRollbackOnly();
                return null;
            }
        });
        
        // test rollback after return
        Assert.assertTrue("beforeCompletion not called on listener", strings.contains("beforeCompletion"));
        Assert.assertTrue("afterRollback not called on listener", strings.contains("afterRollback"));
        Assert.assertEquals("Unexpected size for collected listener logging", 2, strings.size());

    }
    
    /**
     * Tests the condition whereby a listener can cause failure by attempting to bind itself to
     * the transaction in the pre-commit callback.  This is caused by the listener set being
     * modified during calls to the listeners.
     */
    @Test
    public void testPreCommitListenerBinding() throws Exception {
        final String beforeCommit = "beforeCommit";
        final String afterCommitInner = "afterCommit - inner";
        final String afterCommitOuter = "afterCommit = outer";
        
        // the listeners will play with this
        final List<String> testList = new ArrayList<String>(1);
        testList.add(beforeCommit);
        testList.add(afterCommitInner);
        testList.add(afterCommitOuter);
        
        final TransactionListener listener = new TransactionListenerAdapter() {
            @Override
            public int hashCode() {
                // force this listener to be first in the bound set
                return 100;
            }

            @Override
            public void beforeCommit(boolean readOnly) {
                testList.remove(beforeCommit);
                TransactionListener postCommitListener = new TransactionListenerAdapter() {
                    @Override
                    public void afterCommit() {
                        testList.remove(afterCommitInner);
                    }
                };
                // register bogus on the transaction
                TransactionBindingSupport.bindListener(postCommitListener);
            }

            @Override
            public void afterCommit() {
                testList.remove(afterCommitOuter);
            }
        };
        
        final TransactionListener dummyListener = new TransactionListenerAdapter() {
            @Override
            public int hashCode() {
                // force the dummy listener to be AFTER the binding listener
                return 200;
            }
        };
        
        // start a transaction and kick it off
        transactionTemplate.execute(new TransactionCallback() {
            
            public Object doInTransaction(TransactionStatus status) {
                // just bind the listener to the transaction
                TransactionBindingSupport.bindListener(dummyListener);
                TransactionBindingSupport.bindListener(listener);
                return null;
            }
        });
        
        // make sure that the binding all worked
        Assert.assertTrue("Expected callbacks not all processed: " + testList, testList.size() == 0);
    }
    
    @Test
    @Ignore("TransactionTemplate use here doesn't exactly match semantics of RetryingTransactionCallback yet")
    public void testReadWriteStateRetrieval() throws Exception {
        final TxnReadState[] postCommitReadState = new TxnReadState[1];
        final TransactionListenerAdapter getReadStatePostCommit = new TransactionListenerAdapter() {
            @Override
            public void afterCommit() {
                postCommitReadState[0] = TransactionBindingSupport.getTransactionReadState();
            }
        };        
        
        TransactionCallback callback = new TransactionCallback() {
            
            public Object doInTransaction(TransactionStatus status) {
                // Register to list to post-commit
                TransactionBindingSupport.bindListener(getReadStatePostCommit);
                
                return TransactionBindingSupport.getTransactionReadState();
            }
        };
        
        // TODO fix this behavior
        //TxnReadState a = (TxnReadState) transactionTemplate.execute();
        
//        RetryingTransactionCallback<TxnReadState> getReadStateWork = new RetryingTransactionCallback<TxnReadState>()
//        {
//            public TxnReadState execute() throws Exception
//            {
//                // Register to list to post-commit
//                TransactionBindingSupport.bindListener(getReadStatePostCommit);
//                
//                return TransactionBindingSupport.getTransactionReadState();
//            }
//        };

        // Check TXN_NONE
        TxnReadState checkTxnReadState = TransactionBindingSupport.getTransactionReadState();
        Assert.assertEquals("Expected 'no transaction'", TxnReadState.TXN_NONE, checkTxnReadState);
        Assert.assertNull("Expected no post-commit read state", postCommitReadState[0]);
        // Check TXN_READ_ONLY
        transactionTemplate.setReadOnly(true);
        checkTxnReadState = (TxnReadState) transactionTemplate.execute(callback);
        Assert.assertEquals("Expected 'read-only transaction'", TxnReadState.TXN_READ_ONLY, checkTxnReadState);
        Assert.assertEquals("Expected 'no transaction'", TxnReadState.TXN_NONE, postCommitReadState[0]);
        // check TXN_READ_WRITE
        transactionTemplate.setReadOnly(false);
        checkTxnReadState = (TxnReadState) transactionTemplate.execute(callback);
        Assert.assertEquals("Expected 'read-write transaction'", TxnReadState.TXN_READ_WRITE, checkTxnReadState);
        Assert.assertEquals("Expected 'no transaction'", TxnReadState.TXN_NONE, postCommitReadState[0]);
    }

    @Test
    public void testResourceHelperMap() throws Exception {

        // run this in a transaction        
        transactionTemplate.execute(new TransactionCallback() {
            
            public Object doInTransaction(TransactionStatus status) {
                Map<String, String> map = TransactionalResourceHelper.getMap("abc");
                Assert.assertNotNull("Map not created", map);
                map.put("1", "ONE");
                Map<String, String> mapCheck = TransactionalResourceHelper.getMap("abc");
                Assert.assertTrue("Same map not retrieved", map == mapCheck);
                return null;
            }
        });        
    }
    
    @Test
    public void testResourceHelperList() throws Exception {

        // run this in a transaction        
        transactionTemplate.execute(new TransactionCallback() {
            
            public Object doInTransaction(TransactionStatus status) {
                List<String> list = TransactionalResourceHelper.getList("abc");
                Assert.assertNotNull("List not created", list);
                list.add("ONE");
                
                List<String> listCheck = TransactionalResourceHelper.getList("abc");
                Assert.assertTrue("Same list not retrieved", list == listCheck);
                return null;
            }
        });        
    }

    @Test
    public void testResourceHelperSet() throws Exception {

        // run this in a transaction        
        transactionTemplate.execute(new TransactionCallback() {
            
            public Object doInTransaction(TransactionStatus status) {
                Set<String> set = TransactionalResourceHelper.getSet("abc");
                Assert.assertNotNull("Set not created", set);
                set.add("ONE");
                Set<String> setCheck = TransactionalResourceHelper.getSet("abc");
                Assert.assertTrue("Same map not retrieved", set == setCheck);
                return null;
            }
        });        
    }

    @Test
    public void testResourceHelperTreeSet() throws Exception {

        // run this in a transaction        
        transactionTemplate.execute(new TransactionCallback() {
            
            public Object doInTransaction(TransactionStatus status) {
                TreeSet<String> treeSet = TransactionalResourceHelper.getTreeSet("abc");
                Assert.assertNotNull("Map not created", treeSet);
                treeSet.add("ONE");
                TreeSet<String> treeSetCheck = TransactionalResourceHelper.getTreeSet("abc");
                Assert.assertTrue("Same map not retrieved", treeSet == treeSetCheck);
                return null;
            }
        });        
    }
}
