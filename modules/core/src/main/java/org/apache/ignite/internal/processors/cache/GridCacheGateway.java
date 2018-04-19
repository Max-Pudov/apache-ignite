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

import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.cache.CacheException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteClientDisconnectedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.GridSpinReadWriteLock;
import org.apache.ignite.internal.util.future.IgniteFutureImpl;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Cache gateway.
 */
@GridToStringExclude
public class GridCacheGateway<K, V> {
    /** Context. */
    private final GridCacheContext<K, V> ctx;

    /** Stopped flag for dynamic caches. */
    private final AtomicReference<State> _state = new AtomicReference<>(State.STARTED);
    private final IgniteLogger log;

    /** */
    private IgniteFuture<?> reconnectFut;

    /** */
    private GridSpinReadWriteLock rwLock = new GridSpinReadWriteLock();

    /**
     * @param ctx Cache context.
     */
    public GridCacheGateway(GridCacheContext<K, V> ctx) {
        assert ctx != null;

        this.ctx = ctx;

        this.log = ctx.logger(this.getClass());
    }

    /**
     * Enter a cache call.
     */
    public void enter() {
        if (ctx.deploymentEnabled())
            ctx.deploy().onEnter();

        rwLock.readLock();

        checkState(true, true);
    }

    /**
     * @param lock {@code True} if lock is held.
     * @param stopErr {@code True} if throw exception if stopped.
     * @return {@code True} if cache is in started state.
     */
    private boolean checkState(boolean lock, boolean stopErr) {
        State state = this._state.get();

        if (state != State.STARTED) {
            if (lock)
                rwLock.readUnlock();

            if (state == State.STOPPED) {
                if (stopErr)
                    throw new IllegalStateException("Cache has been stopped: " + ctx.name());
                else
                    return false;
            }
            else {
                assert reconnectFut != null;

                dump("throw IgniteClientDisconnectedException");
                logMsg(String.format("checkState throw IgniteClientDisconnectedException reconFut = [%s] internal = [%s]",
                        reconnectFut.hashCode(),
                        ((IgniteFutureImpl)reconnectFut).internalFuture().hashCode()));

                throw new CacheException(
                    new IgniteClientDisconnectedException(proxy(reconnectFut), "Client node disconnected: " + ctx.gridName()));
            }
        }

        return true;
    }

    private IgniteFuture<?> proxy(final IgniteFuture<?> rf) {
        return new IgniteFuture() {

            @Override
            public Object get() throws IgniteException {
                logMsg(String.format("return future get reconFut = [%s] internal = [%s]",
                        rf.hashCode(),
                        ((IgniteFutureImpl)rf).internalFuture().hashCode()));
                return rf.get();
            }

            @Override
            public Object get(long timeout) throws IgniteException {
                logMsg(String.format("return future get(%s) reconFut = [%s] internal = [%s]",
                        timeout,
                        rf.hashCode(),
                        ((IgniteFutureImpl)rf).internalFuture().hashCode()));
                return rf.get(timeout);
            }

            @Override
            public Object get(long timeout, TimeUnit unit) throws IgniteException {
                logMsg(String.format("return future get(%s, %s) reconFut = [%s] internal = [%s]",
                        timeout, unit,
                        rf.hashCode(),
                        ((IgniteFutureImpl)rf).internalFuture().hashCode()));
                return rf.get(timeout, unit);
            }

            @Override
            public boolean cancel() throws IgniteException {
                logMsg(String.format("return future cancel() reconFut = [%s] internal = [%s]",
                        rf.hashCode(),
                        ((IgniteFutureImpl)rf).internalFuture().hashCode()));

                return rf.cancel();
            }

            @Override
            public boolean isCancelled() {
                logMsg(String.format("return future isCancelled() reconFut = [%s] internal = [%s]",
                        rf.hashCode(),
                        ((IgniteFutureImpl)rf).internalFuture().hashCode()));

                return rf.isCancelled();
            }

            @Override
            public boolean isDone() {
                logMsg(String.format("return future isDone() reconFut = [%s] internal = [%s]",
                        rf.hashCode(),
                        ((IgniteFutureImpl)rf).internalFuture().hashCode()));

                return rf.isDone();
            }

            @Override
            public long startTime() {
                logMsg(String.format("return future startTime() reconFut = [%s] internal = [%s]",
                        rf.hashCode(),
                        ((IgniteFutureImpl)rf).internalFuture().hashCode()));

                return rf.startTime();
            }

            @Override
            public long duration() {
                logMsg(String.format("return future duration() reconFut = [%s] internal = [%s]",
                        rf.hashCode(),
                        ((IgniteFutureImpl)rf).internalFuture().hashCode()));

                return rf.duration();
            }

            @Override
            public IgniteFuture chainAsync(IgniteClosure doneCb, Executor exec) {
                logMsg(String.format("return future chainAsync() reconFut = [%s] internal = [%s]",
                        rf.hashCode(),
                        ((IgniteFutureImpl)rf).internalFuture().hashCode()));

                return rf.chainAsync(doneCb, exec);
            }

            @Override
            public IgniteFuture chain(IgniteClosure doneCb) {
                logMsg(String.format("return future chain() reconFut = [%s] internal = [%s]",
                        rf.hashCode(),
                        ((IgniteFutureImpl)rf).internalFuture().hashCode()));

                return rf.chain(doneCb);
            }

            @Override
            public void listenAsync(IgniteInClosure lsnr, Executor exec) {
                logMsg(String.format("return future listenAsync() reconFut = [%s] internal = [%s]",
                        rf.hashCode(),
                        ((IgniteFutureImpl)rf).internalFuture().hashCode()));

                rf.listenAsync(lsnr, exec);
            }

            @Override
            public void listen(IgniteInClosure lsnr) {
                logMsg(String.format("return future listen() reconFut = [%s] internal = [%s]",
                        rf.hashCode(),
                        ((IgniteFutureImpl)rf).internalFuture().hashCode()));

                rf.listen(lsnr);

            }
        };
    }

    /**
     * Enter a cache call.
     *
     * @return {@code True} if enter successful, {@code false} if the cache or the node was stopped.
     */
    public boolean enterIfNotStopped() {
        onEnter();

        // Must unlock in case of unexpected errors to avoid deadlocks during kernal stop.
        rwLock.readLock();

        return checkState(true, false);
    }

    /**
     * Enter a cache call without lock.
     *
     * @return {@code True} if enter successful, {@code false} if the cache or the node was stopped.
     */
    public boolean enterIfNotStoppedNoLock() {
        onEnter();

        return checkState(false, false);
    }

    /**
     * Leave a cache call entered by {@link #enterNoLock} method.
     */
    public void leaveNoLock() {
        ctx.tm().resetContext();
        ctx.mvcc().contextReset();

        // Unwind eviction notifications.
        if (!ctx.shared().closed(ctx))
            CU.unwindEvicts(ctx);
    }

    /**
     * Leave a cache call entered by {@link #enter()} method.
     */
    public void leave() {
        try {
           leaveNoLock();
        }
        finally {
            rwLock.readUnlock();
        }
    }

    /**
     * @param opCtx Cache operation context to guard.
     * @return Previous operation context set on this thread.
     */
    @Nullable public CacheOperationContext enter(@Nullable CacheOperationContext opCtx) {
        try {
            GridCacheAdapter<K, V> cache = ctx.cache();

            GridCachePreloader preldr = cache != null ? cache.preloader() : null;

            if (preldr == null)
                throw new IllegalStateException("Cache has been closed or destroyed: " + ctx.name());

            preldr.startFuture().get();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to wait for cache preloader start [cacheName=" +
                ctx.name() + "]", e);
        }

        onEnter();

        rwLock.readLock();

        checkState(true, true);

        // Must unlock in case of unexpected errors to avoid
        // deadlocks during kernal stop.
        try {
            return setOperationContextPerCall(opCtx);
        }
        catch (Throwable e) {
            rwLock.readUnlock();

            throw e;
        }
    }

    /**
     * @param opCtx Operation context to guard.
     * @return Previous operation context set on this thread.
     */
    @Nullable public CacheOperationContext enterNoLock(@Nullable CacheOperationContext opCtx) {
        onEnter();

        checkState(false, false);

        return setOperationContextPerCall(opCtx);
    }

    /**
     * Set thread local operation context per call.
     *
     * @param opCtx Operation context to guard.
     * @return Previous operation context set on this thread.
     */
    private CacheOperationContext setOperationContextPerCall(@Nullable CacheOperationContext opCtx) {
        CacheOperationContext prev = ctx.operationContextPerCall();

        if (prev != null || opCtx != null)
            ctx.operationContextPerCall(opCtx);

        return prev;
    }

    /**
     * @param prev Previous.
     */
    public void leave(CacheOperationContext prev) {
        try {
            leaveNoLock(prev);
        }
        finally {
            rwLock.readUnlock();
        }
    }

    /**
     * @param prev Previous.
     */
    public void leaveNoLock(CacheOperationContext prev) {
        ctx.tm().resetContext();
        ctx.mvcc().contextReset();

        // Unwind eviction notifications.
        CU.unwindEvicts(ctx);

        // Return back previous thread local operation context per call.
        ctx.operationContextPerCall(prev);
    }

    /**
     *
     */
    private void onEnter() {
        ctx.itHolder().checkWeakQueue();

        if (ctx.deploymentEnabled())
            ctx.deploy().onEnter();
    }

    /**
     *
     */
    public void stopped() {
        _state.set(State.STOPPED);
    }

    /**
     * @param reconnectFut Reconnect future.
     */
    public void onDisconnected(IgniteFuture<?> reconnectFut) {
        assert reconnectFut != null;

        dump("GridCacheGateway.onDisconnected()");

        logMsg(String.format("GridCacheGateway.onDisconnected() reconFut = [%s] internal = [%s]",
                reconnectFut.hashCode(),
                ((IgniteFutureImpl)reconnectFut).internalFuture().hashCode()));

        this.reconnectFut = reconnectFut;

        logMsg("try... _state.compareAndSet(State.STARTED, State.DISCONNECTED)");
        if(!_state.compareAndSet(State.STARTED, State.DISCONNECTED))
            logMsg("!_state.compareAndSet(State.STARTED, State.DISCONNECTED)");
        else
            logMsg("DONE _state.compareAndSet(State.STARTED, State.DISCONNECTED)");
    }

    private void logMsg(String msg) {
        log.info(getLogPrefix() + msg);
    }

    private void dump(String msg) {
        U.dumpStack(log, getLogPrefix() + msg);
    }

    private String getLogPrefix() {
        return String.format("[CG][%s][%s][%s]", Thread.currentThread().getName(), this.ctx.name(), this.hashCode());
    }

    /**
     *
     */
    public void writeLock(){
        rwLock.writeLock();
    }

    /**
     *
     */
    public void writeUnlock() {
        rwLock.writeUnlock();
    }

    /**
     * @param stopped Cache stopped flag.
     */
    public void reconnected(boolean stopped) {
        State newState = stopped ? State.STOPPED : State.STARTED;

        dump("GridCacheGateway.reconnected()");

        logMsg(String.format("try... _state.compareAndSet(State.DISCONNECTED, %s)", newState));
        if(!_state.compareAndSet(State.DISCONNECTED, newState))
            logMsg(String.format("!_state.compareAndSet(State.DISCONNECTED, %s)", newState));
        else
            logMsg(String.format("DONE _state.compareAndSet(State.DISCONNECTED, %s)", newState));
    }


    /**
     *
     */
    public void onStopped() {
        boolean interrupted = false;

        while (true) {
            if (rwLock.tryWriteLock())
                break;
            else {
                try {
                    U.sleep(200);
                }
                catch (IgniteInterruptedCheckedException ignore) {
                    interrupted = true;
                }
            }
        }

        if (interrupted)
            Thread.currentThread().interrupt();

        try {
            _state.set(State.STOPPED);
        }
        finally {
            rwLock.writeUnlock();
        }
    }

    /**
     *
     */
    private enum State {
        /** */
        STARTED,

        /** */
        DISCONNECTED,

        /** */
        STOPPED
    }
}