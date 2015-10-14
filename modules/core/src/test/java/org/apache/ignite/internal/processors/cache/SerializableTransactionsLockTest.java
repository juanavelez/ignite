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

package org.apache.ignite.internal.processors.cache;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteReducer;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class SerializableTransactionsLockTest extends GridCommonAbstractTest {
    /** */
    private static final boolean TRY_LOCK = false;

    /** */
    private List<Node> nodes = new ArrayList<>();

    /** */
    private AtomicLong txIdGen = new AtomicLong();

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 6 * 60_000;
    }

    /**
     * @param cnt Number of nodes.
     */
    private void createNodes(int cnt) {
        for (int i = 0; i < cnt; i++)
            nodes.add(new Node("n-" + i));
    }

    /**
     * @throws Exception If failed.
     */
    public void testConflict() throws Exception {
        createNodes(5);

        final AtomicBoolean stop = new AtomicBoolean();

        final AtomicInteger totalCnt = new AtomicInteger();

        final AtomicInteger rollbackCnt = new AtomicInteger();

        IgniteInternalFuture<?> fut = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                int iter = 0;

                List<Integer> keys = new ArrayList<>();

                for (int i = 0; i < 100; i++)
                    keys.add(i);

                while (!stop.get()) {
                    Collections.shuffle(keys);

                    boolean commit = executeTransaction(txIdGen.incrementAndGet(), keys);

                    totalCnt.incrementAndGet();

                    if (!commit)
                        rollbackCnt.incrementAndGet();

                    if (iter % 1000 == 0)
                        log.info("Iteration: " + iter);

                    iter++;
                }

                return null;
            }
        }, 5, "tx-thread");

        Thread.sleep(5_000);

        stop.set(true);

        fut.get();

        log.info("Result [total=" + totalCnt.get() +
            ", rollback=" + rollbackCnt.get() +
            ", rollbackPercent=" + rollbackCnt.get() / (float)totalCnt.get() * 100 + "%]");
    }

    /**
     * @throws Exception If failed.
     */
    public void testNoDeadlock() throws Exception {
        createNodes(10);

        final AtomicBoolean stop = new AtomicBoolean();

        final AtomicInteger totalCnt = new AtomicInteger();

        final AtomicInteger rollbackCnt = new AtomicInteger();

        IgniteInternalFuture<?> fut = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                ThreadLocalRandom rnd = ThreadLocalRandom.current();

                int iter = 0;

                while (!stop.get()) {
                    int keysNum = rnd.nextInt(1, 30);

                    Set<Integer> keys = new LinkedHashSet<>();

                    while (keys.size() < keysNum)
                        keys.add(rnd.nextInt(100));

                    boolean commit = executeTransaction(txIdGen.incrementAndGet(), keys);

                    totalCnt.incrementAndGet();

                    if (!commit)
                        rollbackCnt.incrementAndGet();

                    if (iter % 5000 == 0)
                        log.info("Iteration: " + iter);

                    iter++;
                }

                return null;
            }
        }, 30, "tx-thread");

        Thread.sleep(5 * 60_000);

        stop.set(true);

        fut.get();

        log.info("Result [total=" + totalCnt.get() +
            ", rollback=" + rollbackCnt.get() +
            ", rollbackPercent=" + rollbackCnt.get() / (float)totalCnt.get() * 100 + "%]");
    }

    /**
     * @param txId Transaction ID.
     * @param keys Keys.
     * @return {@code True} if transaction acquired all locks.
     * @throws Exception If failed.
     */
    private boolean executeTransaction(Long txId, Collection<Integer> keys) throws Exception {
        Map<Node, Set<Integer>> txMap = new HashMap<>();

        for (Integer key : keys) {
            Node node =  map(key);

            Set<Integer> nodeKeys = txMap.get(node);

            if (nodeKeys == null)
                txMap.put(node, nodeKeys = new HashSet<>());

            nodeKeys.add(key);
        }

        Map<Node, IgniteInternalFuture<Boolean>> futs = new HashMap<>();

        for (Map.Entry<Node, Set<Integer>> e : txMap.entrySet()) {
            Node node = e.getKey();

            futs.put(node, node.prepare(txId, e.getValue()));
        }

        boolean prepared = true;

        for (Map.Entry<Node, IgniteInternalFuture<Boolean>> e : futs.entrySet()) {
            boolean nodePrepared = e.getValue().get();

            if (!nodePrepared) {
                prepared = false;

                txMap.remove(e.getKey());
            }
        }

        GridCompoundFuture<Void, Void> finishFut = new GridCompoundFuture<>();

        for (Map.Entry<Node, Set<Integer>> e : txMap.entrySet()) {
            Node node = e.getKey();

            finishFut.add(node.finish(txId, e.getValue(), prepared));
        }

        finishFut.markInitialized();

        finishFut.get();

        return prepared;
    }

    /**
     * @param key Key.
     * @return Node.
     */
    private Node map(Integer key) {
        int idx = U.safeAbs(key.hashCode()) % nodes.size();

        return nodes.get(idx);
    }

    /**
     *
     */
    static class Node {
        /** */
        private ConcurrentHashMap<Integer, Entry> map = new ConcurrentHashMap<>();

        /** */
        private final String id;

        /** */
        IgniteThreadPoolExecutor execSvc;

        /**
         * @param id ID.
         */
        public Node(String id) {
            this.id = id;

            execSvc = new IgniteThreadPoolExecutor(
                "pool-" + id,
                IgniteConfiguration.DFLT_SYSTEM_CORE_THREAD_CNT,
                IgniteConfiguration.DFLT_SYSTEM_CORE_THREAD_CNT,
                0,
                new LinkedBlockingQueue<Runnable>(Integer.MAX_VALUE));
        }

        /**
         * @param txId Transaction ID.
         * @param keys Keys.
         * @return Prepare future.
         */
        public IgniteInternalFuture<Boolean> prepare(final Long txId, final Collection<Integer> keys) {
            final GridCompoundFuture<Boolean, Boolean> fut =
                new GridCompoundFuture<>(new IgniteReducer<Boolean, Boolean>() {
                    /** */
                    private volatile boolean res = true;

                    @Override public boolean collect(Boolean prepared) {
                        if (!prepared)
                            res = false;

                        return true;
                    }

                    @Override public Boolean reduce() {
                        return res;
                    }
                }
            );

            execSvc.submit(new Runnable() {
                @Override public void run() {
                    List<Entry> locked = new ArrayList<>(keys.size());

                    for (Integer key : keys) {
                        Entry e = entry(key);

                        IgniteInternalFuture<Boolean> keyFut = e.lock(txId);

                        if (keyFut == null) {
                            for (Entry l : locked)
                                l.unlock(txId);

                            fut.onDone(false);

                            return;
                        }

                        locked.add(e);

                        fut.add(keyFut);
                    }

                    fut.markInitialized();

                    for (Entry e : locked)
                        e.txReady(txId);
                }
            });

            return fut;
        }

        /**
         * @param txId Transaction ID.
         * @param keys Transaction keys
         * @param commit Commit flag.
         * @return Commit future.
         */
        public IgniteInternalFuture<Void> finish(final Long txId, final Collection<Integer> keys, boolean commit) {
            final GridFutureAdapter<Void> fut = new GridFutureAdapter<>();

            execSvc.submit(new Runnable() {
                @Override public void run() {
                    for (Integer key : keys) {
                        Entry e = entry(key);

                        e.unlock(txId);
                    }

                    fut.onDone();
                }
            });

            return fut;
        }

        /**
         * @param key Key.
         * @return Entry.
         */
        private Entry entry(Integer key) {
            Entry e = map.get(key);

            if (e == null) {
                Entry old = map.putIfAbsent(key, e = new Entry(key));

                if (old != null)
                    e = old;
            }

            return e;
        }

        /**
         * @return Node ID.
         */
        String id() {
            return id;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            Node node = (Node)o;

            return id.equals(node.id);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return id.hashCode();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Node [id=" + id + ']';
        }
    }

    /**
     *
     */
    static class Entry {
        /** */
        private final Deque<Candidate> cands = new ArrayDeque<>();

        /** */
        private final Integer key;

        /**
         * @param key Key.
         */
        public Entry(Integer key) {
            this.key = key;
        }

        /**
         * @return Key.
         */
        Integer key() {
            return key;
        }

        /**
         *
         */
        static class Candidate {
            /** */
            private Long txId;

            /** */
            private GridFutureAdapter<Boolean> fut;

            /** */
            private boolean ready;

            /**
             * @param txId Transaction ID.
             * @param fut Future.
             */
            public Candidate(Long txId, GridFutureAdapter<Boolean> fut) {
                this.txId = txId;
                this.fut = fut;
            }
        }

        /**
         * @param txId Transaction ID.
         * @return Future.
         */
        synchronized IgniteInternalFuture<Boolean> lock(Long txId) {
            Candidate last = cands.peekLast();

            if (TRY_LOCK) {
                if (last != null)
                    return null;
            }
            else {
                if (last != null && last.txId > txId)
                    return null;
            }

            GridFutureAdapter<Boolean> fut = new GridFutureAdapter<>();

            cands.addLast(new Candidate(txId, fut));

            return fut;
        }

        /**
         * @param txId Transaction ID.
         */
        void txReady(Long txId) {
            Candidate owner;

            synchronized (this) {
                Candidate cand = candidate(txId);

                cand.ready = true;

                owner = assignNewOwner();
            }

            if (owner != null)
                owner.fut.onDone(true);
        }

        /**
         * @param txId Transaction ID.
         */
        void unlock(Long txId) {
             Candidate owner;

             synchronized (this) {
                 Candidate cand = candidate(txId);

                 cands.remove(cand);

                 owner = assignNewOwner();
             }

             if (owner != null)
                 owner.fut.onDone(true);
        }

        /**
         * @return New owner.
         */
        @Nullable private Candidate assignNewOwner() {
            Candidate cand = cands.peekFirst();

            if (cand != null && cand.ready)
                return cand;

            return cand;
        }

        /**
         * @param txId Transaction ID.
         * @return Candidate for transaction.
         */
        private Candidate candidate(Long txId) {
            Candidate txCand = null;

            for (Candidate cand : cands) {
                if (cand.txId.equals(txId)) {
                    txCand = cand;

                    break;
                }
            }

            assert txCand != null;

            return txCand;
        }

        /** {@inheritDoc} */
        public String toString() {
            return S.toString(Entry.class, this);
        }
    }
}
