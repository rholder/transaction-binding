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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Helper class that will look up or create transactional resources.
 * This shortcuts some of the "<i>if not existing, then create</i>" code.
 * 
 * @author Derek Hulley
 * @author Ray Holder, derived work modifications
 * @since 3.2
 */
public abstract class TransactionalResourceHelper
{
    /**
     * Support method to retrieve or create and bind a <tt>HashMap</tt> to the current transaction.
     * 
     * @param <K>           the map key type
     * @param <V>           the map value type
     * @param resourceKey   the key under which the resource will be stored
     * @return              Returns an previously-bound <tt>Map</tt> or else a newly-bound <tt>HashMap</tt>
     */
    public static final <K,V> Map<K,V> getMap(Object resourceKey)
    {
        Map<K,V> map = TransactionBindingSupport.<Map<K,V>>getResource(resourceKey);
        if (map == null)
        {
            map = new HashMap<K, V>(29);
            TransactionBindingSupport.bindResource(resourceKey, map);
        }
        return map;
    }

    /**
     * Support method to retrieve or create and bind a <tt>HashSet</tt> to the current transaction.
     * 
     * @param <V>           the set value type
     * @param resourceKey   the key under which the resource will be stored
     * @return              Returns an previously-bound <tt>Set</tt> or else a newly-bound <tt>HashSet</tt>
     */
    public static final <V> Set<V> getSet(Object resourceKey)
    {
        Set<V> set = TransactionBindingSupport.<Set<V>>getResource(resourceKey);
        if (set == null)
        {
            set = new HashSet<V>(29);
            TransactionBindingSupport.bindResource(resourceKey, set);
        }
        return set;
    }

    /**
     * Support method to retrieve or create and bind a <tt>TreeSet</tt> to the current transaction.
     * 
     * @param <V>           the set value type
     * @param resourceKey   the key under which the resource will be stored
     * @return              Returns an previously-bound <tt>TreeSet</tt> or else a newly-bound <tt>TreeSet</tt>
     */
    public static final <V> TreeSet<V> getTreeSet(Object resourceKey)
    {
        TreeSet<V> set = TransactionBindingSupport.<TreeSet<V>>getResource(resourceKey);
        if (set == null)
        {
            set = new TreeSet<V>();
            TransactionBindingSupport.bindResource(resourceKey, set);
        }
        return set;
    }

    /**
     * Support method to retrieve or create and bind a <tt>ArrayList</tt> to the current transaction.
     * 
     * @param <V>           the list value type
     * @param resourceKey   the key under which the resource will be stored
     * @return              Returns an previously-bound <tt>List</tt> or else a newly-bound <tt>ArrayList</tt>
     */
    public static final <V> List<V> getList(Object resourceKey)
    {
        List<V> list = TransactionBindingSupport.<List<V>>getResource(resourceKey);
        if (list == null)
        {
            list = new ArrayList<V>(29);
            TransactionBindingSupport.bindResource(resourceKey, list);
        }
        return list;
    }
    
    /**
     * Support method to set a boolean (true) value in the current transaction.
     * @param resourceKey   the key under which the resource will be stored
     * @return true - the value of resourceKey, was set to true, false - the value was already true
     */
    public static final boolean setBoolean(Object resourceKey)
    {
        Boolean value = TransactionBindingSupport.getResource(resourceKey);
        if(value == null)
        {
            TransactionBindingSupport.bindResource(resourceKey, Boolean.TRUE);
            return true;
        }
       
        return false;
    }
    
    /**
     * Support method to reset (make false) a boolean value in the current transaction.
     * @param resourceKey   the key under which the resource is stored.
     */
    public static final void resetBoolean(Object resourceKey)
    {
        Boolean value = TransactionBindingSupport.getResource(resourceKey);
        if(value == null)
        {
            TransactionBindingSupport.unbindResource(resourceKey);
        }
    }
    
    /**
     * Is there a boolean value in the current transaction
     * @param resourceKey   the key under which the resource will be stored
     * @return true - thre is, false no.
     */
    public static final boolean testBoolean(Object resourceKey)
    {
        Boolean value = TransactionBindingSupport.getResource(resourceKey);
        if(value == null)
        {
            return false;
        }
        else
        {
            return true;
        }
    }
}
