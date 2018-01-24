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

package org.apache.ignite.internal.processors.schedule;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObjectAdapter;
import org.apache.ignite.internal.util.future.AsyncFutureListener;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.future.IgniteFutureImpl;
import org.apache.ignite.internal.util.lang.GridClosureException;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteFutureCancelledException;
import org.apache.ignite.lang.IgniteFutureTimeoutException;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.scheduler.SchedulerFuture;
import org.jetbrains.annotations.Nullable;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Implementation of {@link org.apache.ignite.scheduler.SchedulerFuture} interface.
 */
class ScheduleFutureImpl<R> implements SchedulerFuture<R> {
    /** Empty time array. */
    private static final long[] EMPTY_TIMES = new long[] {};

    /** No next execution time constant. **/
    private static final long NO_NEXT_EXECUTION_TIME = 0;

    /** Identifier generated by cron scheduler. */
    private volatile String id;

    /** Scheduling pattern. */
    private String pat;

    /** Scheduling delay in seconds parsed from pattern. */
    private int delay;

    /** Number of maximum task calls parsed from pattern. */
    private int maxCalls;

    /** Mere cron pattern parsed from extended pattern. */
    private String cron;

    /** Cancelled flag. */
    private boolean cancelled;

    /** Done flag. */
    private boolean done;

    /** Task calls counter. */
    private int callCnt;

    /** De-schedule flag. */
    private final AtomicBoolean descheduled = new AtomicBoolean(true);

    /** Listeners. */
    private Collection<IgniteInClosure<? super IgniteFuture<R>>> lsnrs = new ArrayList<>(1);

    /** Statistics. */
    @SuppressWarnings({"FieldAccessedSynchronizedAndUnsynchronized"})
    private GridScheduleStatistics stats = new GridScheduleStatistics();

    /** Latch synchronizing fetch of the next execution result. */
    @GridToStringExclude
    private CountDownLatch resLatch = new CountDownLatch(1);

    /** Cron scheduler. */
    @GridToStringExclude
    private SpringScheduler sched;

    /** Processor registry. */
    @GridToStringExclude
    private GridKernalContext ctx;

    /** Execution task. */
    @GridToStringExclude
    private Callable<R> task;

    /** Result of the last execution of scheduled task. */
    @GridToStringExclude
    private R lastRes;

    /** Keeps last execution exception or {@code null} if the last execution was successful. */
    @GridToStringExclude
    private Throwable lastErr;

    /** Listener call count. */
    private int lastLsnrExecCnt;

    /** Mutex. */
    private final Object mux = new Object();

    /** Grid logger. */
    private IgniteLogger log;

    /** Runnable object to schedule with cron scheduler. */
    private final Runnable run = new Runnable() {
        @Nullable private CountDownLatch onStart() {
            synchronized (mux) {
                if (done || cancelled)
                    return null;

                if (stats.isRunning()) {
                    U.warn(log, "Task got scheduled while previous was not finished: " + this);

                    return null;
                }

                if (callCnt == maxCalls && maxCalls > 0)
                    return null;

                callCnt++;

                stats.onStart();

                assert resLatch != null;

                return resLatch;
            }
        }

        @SuppressWarnings({"ErrorNotRethrown"})
        @Override public void run() {
            CountDownLatch latch = onStart();

            if (latch == null)
                return;

            R res = null;

            Throwable err = null;

            try {
                res = task.call();
            }
            catch (Exception e) {
                err = e;
            }
            catch (Error e) {
                err = e;

                U.error(log, "Error occurred while executing scheduled task: " + this, e);
            }
            finally {
                if (!onEnd(latch, res, err, false))
                    deschedule();
            }
        }
    };

    /**
     * Creates descriptor for task scheduling. To start scheduling call {@link #schedule(Callable)}.
     *
     * @param sched Cron scheduler.
     * @param ctx Kernal context.
     * @param pat Cron pattern.
     */
    ScheduleFutureImpl(SpringScheduler sched, GridKernalContext ctx, String pat) {
        assert sched != null;
        assert ctx != null;
        assert pat != null;

        this.sched = sched;
        this.ctx = ctx;
        this.pat = pat.trim();

        log = ctx.log(getClass());

        try {
            parsePatternParameters();
        }
        catch (IgniteCheckedException e) {
            onEnd(resLatch, null, e, true);
        }
    }

    /**
     * @param latch Latch.
     * @param res Result.
     * @param err Error.
     * @param initErr Init error flag.
     * @return {@code False} if future should be unscheduled.
     */
    private boolean onEnd(CountDownLatch latch, R res, Throwable err, boolean initErr) {
        assert latch != null;

        boolean notifyLsnr = false;

        CountDownLatch resLatchCp = null;

        try {
            synchronized (mux) {
                lastRes = res;
                lastErr = err;

                if (initErr) {
                    assert err != null;

                    notifyLsnr = true;
                }
                else {
                    stats.onEnd();

                    int cnt = stats.getExecutionCount();

                    if (lastLsnrExecCnt != cnt) {
                        notifyLsnr = true;

                        lastLsnrExecCnt = cnt;
                    }
                }

                if ((callCnt == maxCalls && maxCalls > 0) || cancelled || initErr) {
                    done = true;

                    resLatchCp = resLatch;

                    resLatch = null;

                    return false;
                }

                resLatch = new CountDownLatch(1);

                return true;
            }
        }
        finally {
            // Unblock all get() invocations.
            latch.countDown();

            // Make sure that none will be blocked on new latch if this
            // future will not be executed any more.
            if (resLatchCp != null)
                resLatchCp.countDown();

            if (notifyLsnr)
                notifyListeners(res, err);
        }
    }

    /**
     * Sets execution task.
     *
     * @param task Execution task.
     */
    void schedule(Callable<R> task) {
        assert task != null;
        assert this.task == null;

        // Done future on this step means that there was error on init.
        if (isDone())
            return;

        this.task = task;

        ((IgniteScheduleProcessor)ctx.schedule()).onScheduled(this);

        if (delay > 0) {
            // Schedule after delay.
            ctx.timeout().addTimeoutObject(new GridTimeoutObjectAdapter(delay * 1000) {
                @Override public void onTimeout() {
                    schedule0();
                }
            });
        }
        else
            schedule0();
    }

    /**
     * Schedules executing task.
     */
    private void schedule0() {
        assert id == null;

        try {
            id = sched.schedule(cron, run);

            boolean b = descheduled.compareAndSet(true, false);

            assert b;
        }
        catch (IgniteException e) {
            // This should never happen as we validated the pattern during parsing.
            U.error(log, "Invalid scheduling pattern: " + cron, e);

            assert false : "Invalid scheduling pattern: " + cron;
        }
    }

    /**
     * De-schedules scheduled task.
     */
    void deschedule() {
        if (descheduled.compareAndSet(false, true)) {
            sched.deschedule(id);

            ((IgniteScheduleProcessor)ctx.schedule()).onDescheduled(this);
        }
    }

    /**
     * Parse delay, number of task calls and mere cron expression from extended pattern
     *  that looks like  "{n1,n2} * * * * *".
     * @throws IgniteCheckedException Thrown if pattern is invalid.
     */
    private void parsePatternParameters() throws IgniteCheckedException {
        assert pat != null;

        String regEx = "(\\{(\\*|\\d+),\\s*(\\*|\\d+)\\})?(.*)";

        Matcher matcher = Pattern.compile(regEx).matcher(pat.trim());

        if (matcher.matches()) {
            String delayStr = matcher.group(2);

            if (delayStr != null)
                if ("*".equals(delayStr))
                    delay = 0;
                else
                    try {
                        delay = Integer.valueOf(delayStr);
                    }
                    catch (NumberFormatException e) {
                        throw new IgniteCheckedException("Invalid delay parameter in schedule pattern [delay=" +
                            delayStr + ", pattern=" + pat + ']', e);
                    }

            String numOfCallsStr = matcher.group(3);

            if (numOfCallsStr != null) {
                int maxCalls0;

                if ("*".equals(numOfCallsStr))
                    maxCalls0 = 0;
                else {
                    try {
                        maxCalls0 = Integer.valueOf(numOfCallsStr);
                    }
                    catch (NumberFormatException e) {
                        throw new IgniteCheckedException("Invalid number of calls parameter in schedule pattern [numOfCalls=" +
                            numOfCallsStr + ", pattern=" + pat + ']', e);
                    }

                    if (maxCalls0 <= 0)
                        throw new IgniteCheckedException("Number of calls must be greater than 0 or must be equal to \"*\"" +
                            " in schedule pattern [numOfCalls=" + maxCalls0 + ", pattern=" + pat + ']');
                }

                synchronized (mux) {
                    maxCalls = maxCalls0;
                }
            }

            cron = matcher.group(4);

            if (cron != null)
                cron = cron.trim();

            sched.validate(cron);
        }
        else
            throw new IgniteCheckedException("Invalid schedule pattern: " + pat);
    }

    /** {@inheritDoc} */
    @Override public String pattern() {
        return pat;
    }

    /** {@inheritDoc} */
    @Override public String id() {
        return id;
    }

    /** {@inheritDoc} */
    @Override public long[] nextExecutionTimes(int cnt, long start) {
        assert cnt > 0;
        assert start > 0;

        if (isDone() || isCancelled())
            return EMPTY_TIMES;

        synchronized (mux) {
            if (maxCalls > 0)
                cnt = Math.min(cnt, maxCalls);
        }

        if (start < createTime() + delay * 1000)
            start = createTime() + delay * 1000;

        return sched.getNextExecutionTimes(cron, cnt, start);
    }

    /** {@inheritDoc} */
    @Override public long nextExecutionTime() {
        long[] execTimes = nextExecutionTimes(1, U.currentTimeMillis());

        return execTimes == EMPTY_TIMES ? NO_NEXT_EXECUTION_TIME : execTimes[0];
    }

    /** {@inheritDoc} */
    @Override public boolean cancel() {
        synchronized (mux) {
            if (done)
                return false;

            if (cancelled)
                return true;

            if (!stats.isRunning())
                done = true;

            cancelled = true;
        }

        deschedule();

        return true;
    }

    /** {@inheritDoc} */
    @Override public long createTime() {
        synchronized (mux) {
            return stats.getCreateTime();
        }
    }

    /** {@inheritDoc} */
    @Override public long lastStartTime() {
        synchronized (mux) {
            return stats.getLastStartTime();
        }
    }

    /** {@inheritDoc} */
    @Override public long lastFinishTime() {
        synchronized (mux) {
            return stats.getLastEndTime();
        }
    }

    /** {@inheritDoc} */
    @Override public double averageExecutionTime() {
        synchronized (mux) {
            return stats.getLastExecutionTime();
        }
    }

    /** {@inheritDoc} */
    @Override public long lastIdleTime() {
        synchronized (mux) {
            return stats.getLastIdleTime();
        }
    }

    /** {@inheritDoc} */
    @Override public double averageIdleTime() {
        synchronized (mux) {
            return stats.getAverageIdleTime();
        }
    }

    /** {@inheritDoc} */
    @Override public int count() {
        synchronized (mux) {
            return stats.getExecutionCount();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isRunning() {
        synchronized (mux) {
            return stats.isRunning();
        }
    }

    /** {@inheritDoc} */
    @Override public R last() throws IgniteException {
        synchronized (mux) {
            if (lastErr != null)
                throw U.convertException(U.cast(lastErr));

            return lastRes;
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isCancelled() {
        synchronized (mux) {
            return cancelled;
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isDone() {
        synchronized (mux) {
            return done;
        }
    }

    /** {@inheritDoc} */
    @Override public void listen(IgniteInClosure<? super IgniteFuture<R>> lsnr) {
        A.notNull(lsnr, "lsnr");

        Throwable err;
        R res;

        boolean notifyLsnr = false;

        synchronized (mux) {
            lsnrs.add(lsnr);

            err = lastErr;
            res = lastRes;

            int cnt = stats.getExecutionCount();

            if (cnt > 0 && lastLsnrExecCnt != cnt) {
                lastLsnrExecCnt = cnt;

                notifyLsnr = true;
            }
        }

        // Avoid race condition in case if listener was added after
        // first execution completed.
        if (notifyLsnr)
            notifyListener(lsnr, res, err);
    }

    /** {@inheritDoc} */
    @Override public void listenAsync(IgniteInClosure<? super IgniteFuture<R>> lsnr, Executor exec) {
        A.notNull(lsnr, "lsnr");
        A.notNull(exec, "exec");

        listen(new AsyncFutureListener<>(lsnr, exec));
    }

    /** {@inheritDoc} */
    @SuppressWarnings("ExternalizableWithoutPublicNoArgConstructor")
    @Override public <T> IgniteFuture<T> chain(final IgniteClosure<? super IgniteFuture<R>, T> doneCb) {
        A.notNull(doneCb, "doneCb");

        return chain(doneCb, null);
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteFuture<T> chainAsync(IgniteClosure<? super IgniteFuture<R>, T> doneCb, Executor exec) {
        A.notNull(doneCb, "");
        A.notNull(exec, "exec");

        return chain(doneCb, exec);
    }

    /**
     * @param doneCb Done callback.
     * @param exec Executor.
     * @return Chained future.
     */
    private <T> IgniteFuture<T> chain(final IgniteClosure<? super IgniteFuture<R>, T> doneCb, @Nullable Executor exec) {
        final GridFutureAdapter<T> fut = new GridFutureAdapter<T>() {
            @Override public String toString() {
                return "ChainFuture[orig=" + ScheduleFutureImpl.this + ", doneCb=" + doneCb + ']';
            }
        };

        IgniteInClosure<? super IgniteFuture<R>> lsnr = new CI1<IgniteFuture<R>>() {
            @Override public void apply(IgniteFuture<R> fut0) {
                try {
                    fut.onDone(doneCb.apply(fut0));
                }
                catch (GridClosureException e) {
                    fut.onDone(e.unwrap());
                }
                catch (IgniteException e) {
                    fut.onDone(e);
                }
                catch (RuntimeException | Error e) {
                    U.warn(null, "Failed to notify chained future (is grid stopped?) [igniteInstanceName=" +
                        ctx.igniteInstanceName() + ", doneCb=" + doneCb + ", err=" + e.getMessage() + ']');

                    fut.onDone(e);

                    throw e;
                }
            }
        };

        if (exec != null)
            lsnr = new AsyncFutureListener<>(lsnr, exec);

        listen(lsnr);

        return new IgniteFutureImpl<>(fut);
    }

    /**
     * @param lsnr Listener to notify.
     * @param res Last execution result.
     * @param err Last execution error.
     */
    private void notifyListener(final IgniteInClosure<? super IgniteFuture<R>> lsnr, R res, Throwable err) {
        assert lsnr != null;
        assert !Thread.holdsLock(mux);
        assert ctx != null;

        lsnr.apply(snapshot(res, err));
    }

    /**
     * @param res Last execution result.
     * @param err Last execution error.
     */
    private void notifyListeners(R res, Throwable err) {
        final Collection<IgniteInClosure<? super IgniteFuture<R>>> tmp;

        synchronized (mux) {
            tmp = new ArrayList<>(lsnrs);
        }

        final SchedulerFuture<R> snapshot = snapshot(res, err);

        for (IgniteInClosure<? super IgniteFuture<R>> lsnr : tmp)
            lsnr.apply(snapshot);
    }

    /**
     * Checks that the future is in valid state for get operation.
     *
     * @return Latch or {@code null} if future has been finished.
     * @throws IgniteFutureCancelledException If was cancelled.
     */
    @Nullable private CountDownLatch ensureGet() throws IgniteFutureCancelledException {
        synchronized (mux) {
            if (cancelled)
                throw new IgniteFutureCancelledException("Scheduling has been cancelled: " + this);

            if (done)
                return null;

            return resLatch;
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public R get() {
        CountDownLatch latch = ensureGet();

        if (latch != null) {
            try {
                latch.await();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();

                if (isCancelled())
                    throw new IgniteFutureCancelledException(e);

                if (isDone())
                    return last();

                throw new IgniteInterruptedException(e);
            }
        }

        return last();
    }

    /** {@inheritDoc} */
    @Override public R get(long timeout) {
        return get(timeout, MILLISECONDS);
    }

    /** {@inheritDoc} */
    @Nullable @Override public R get(long timeout, TimeUnit unit) throws IgniteException {
        CountDownLatch latch = ensureGet();

        if (latch != null) {
            try {
                if (latch.await(timeout, unit))
                    return last();
                else
                    throw new IgniteFutureTimeoutException("Timed out waiting for completion of next " +
                        "scheduled computation: " + this);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();

                if (isCancelled())
                    throw new IgniteFutureCancelledException(e);

                if (isDone())
                    return last();

                throw new IgniteInterruptedException(e);
            }
        }

        return last();
    }

    /**
     * Creates a snapshot of this future with fixed last result.
     *
     * @param res Last result.
     * @param err Last error.
     * @return Future snapshot.
     */
    private SchedulerFuture<R> snapshot(R res, Throwable err) {
        return new ScheduleFutureSnapshot<>(this, res, err);
    }

    /**
     * Future snapshot.
     *
     * @param <R>
     */
    private static class ScheduleFutureSnapshot<R> implements SchedulerFuture<R> {
        /** */
        private ScheduleFutureImpl<R> ref;

        /** */
        private R res;

        /** */
        private Throwable err;

        /**
         *
         * @param ref Referenced implementation.
         * @param res Last result.
         * @param err Throwable.
         */
        ScheduleFutureSnapshot(ScheduleFutureImpl<R> ref, R res, Throwable err) {
            assert ref != null;

            this.ref = ref;
            this.res = res;
            this.err = err;
        }

        /** {@inheritDoc} */
        @Override public R last() {
            if (err != null)
                throw U.convertException(U.cast(err));

            return res;
        }

        /** {@inheritDoc} */
        @Override public String id() {
            return ref.id();
        }

        /** {@inheritDoc} */
        @Override public String pattern() {
            return ref.pattern();
        }

        /** {@inheritDoc} */
        @Override public long createTime() {
            return ref.createTime();
        }

        /** {@inheritDoc} */
        @Override public long lastStartTime() {
            return ref.lastStartTime();
        }

        /** {@inheritDoc} */
        @Override public long lastFinishTime() {
            return ref.lastFinishTime();
        }

        /** {@inheritDoc} */
        @Override public double averageExecutionTime() {
            return ref.averageExecutionTime();
        }

        /** {@inheritDoc} */
        @Override public long lastIdleTime() {
            return ref.lastIdleTime();
        }

        /** {@inheritDoc} */
        @Override public double averageIdleTime() {
            return ref.averageIdleTime();
        }

        /** {@inheritDoc} */
        @Override public long[] nextExecutionTimes(int cnt, long start) {
            return ref.nextExecutionTimes(cnt, start);
        }

        /** {@inheritDoc} */
        @Override public int count() {
            return ref.count();
        }

        /** {@inheritDoc} */
        @Override public boolean isRunning() {
            return ref.isRunning();
        }

        /** {@inheritDoc} */
        @Override public long nextExecutionTime() {
            return ref.nextExecutionTime();
        }

        /** {@inheritDoc} */
        @Nullable @Override public R get() {
            return ref.get();
        }

        /** {@inheritDoc} */
        @Override public R get(long timeout) {
            return ref.get(timeout);
        }

        /** {@inheritDoc} */
        @Nullable @Override public R get(long timeout, TimeUnit unit) {
            return ref.get(timeout, unit);
        }

        /** {@inheritDoc} */
        @Override public boolean cancel() {
            return ref.cancel();
        }

        /** {@inheritDoc} */
        @Override public boolean isDone() {
            return ref.isDone();
        }

        /** {@inheritDoc} */
        @Override public boolean isCancelled() {
            return ref.isCancelled();
        }

        /** {@inheritDoc} */
        @Override public void listen(IgniteInClosure<? super IgniteFuture<R>> lsnr) {
            ref.listen(lsnr);
        }

        /** {@inheritDoc} */
        @Override public void listenAsync(IgniteInClosure<? super IgniteFuture<R>> lsnr, Executor exec) {
            ref.listenAsync(lsnr, exec);
        }

        /** {@inheritDoc} */
        @Override public <T> IgniteFuture<T> chain(IgniteClosure<? super IgniteFuture<R>, T> doneCb) {
            return ref.chain(doneCb);
        }

        /** {@inheritDoc} */
        @Override public <T> IgniteFuture<T> chainAsync(IgniteClosure<? super IgniteFuture<R>, T> doneCb,
            Executor exec) {
            return ref.chainAsync(doneCb, exec);
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ScheduleFutureImpl.class, this);
    }
}
