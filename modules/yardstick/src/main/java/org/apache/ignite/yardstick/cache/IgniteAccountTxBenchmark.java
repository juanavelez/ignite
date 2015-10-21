/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.yardstick.cache;

import java.util.Map;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.yardstick.cache.model.Account;

import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 *
 */
public class IgniteAccountTxBenchmark extends IgniteAccountTxAbstractBenchmark {
    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        int key1 = nextRandom(args.range());

        int key2;

        do {
            key2 = nextRandom(args.range());
        }
        while (key2 == key1);

        if (key2 > key1) {
            int tmp = key2;
            key2 = key1;
            key1 = tmp;
        }

        try (Transaction tx = txs.txStart(PESSIMISTIC, REPEATABLE_READ)) {
            Account a1 = (Account)cache.get(key1);

            if (a1 == null)
                throw new Exception("Failed to find account " + key1);

            Account a2 = (Account)cache.get(key2);

            if (a2 == null)
                throw new Exception("Failed to find account " + key2);

            if (a1.value() > 0) {
                cache.put(key1, new Account(a1.value() - 1));
                cache.put(key2, new Account(a2.value() + 1));
            }

            tx.commit();
        }

        return true;
    }
}