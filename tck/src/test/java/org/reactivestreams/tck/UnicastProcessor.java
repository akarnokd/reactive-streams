/************************************************************************
 * Licensed under Public Domain (CC0)                                    *
 *                                                                       *
 * To the extent possible under law, the person who associated CC0 with  *
 * this code has waived all copyright and related or neighboring         *
 * rights to this code.                                                  *
 *                                                                       *
 * You should have received a copy of the CC0 legalcode along with this  *
 * work. If not, see <http://creativecommons.org/publicdomain/zero/1.0/>.*
 ************************************************************************/

package org.reactivestreams.tck;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.*;

import org.reactivestreams.*;

/**
 * A {@link Processor} implementation that allows only one {@link Subscriber}
 * during its lifetime and buffers upstream events until this single Subscriber
 * is ready to receive the items, optionally delaying an error signal.
 *
 * @param <T>
 */
final class UnicastProcessor<T> implements Processor<T, T> {

    static final class EmptySubscription implements Subscription {
        @Override
        public void request(long n) {
        }
        @Override
        public void cancel() {
        }
    }
    
    static final Subscription CANCELLED = new EmptySubscription();
    
    final Queue<T> queue;

    final AtomicBoolean once;

    final AtomicReference<Subscriber<? super T>> actual;

    final boolean delayError;

    final AtomicLong requested;

    final AtomicReference<Throwable> error;

    final AtomicReference<Subscription> upstream;
    
    final AtomicInteger wip;

    long emitted;

    volatile boolean done;

    volatile boolean cancelled;

    UnicastProcessor(boolean delayError) {
        this.queue = new ConcurrentLinkedQueue<T>();
        this.once = new AtomicBoolean();
        this.actual = new AtomicReference<Subscriber<? super T>>();
        this.delayError = delayError;
        this.requested = new AtomicLong();
        this.error = new AtomicReference<Throwable>();
        this.upstream = new AtomicReference<Subscription>();
        this.wip = new AtomicInteger();
    }

    public void start() {
        onSubscribe(new EmptySubscription());
    }

    @Override
    public void onSubscribe(Subscription s) {
        if (!upstream.compareAndSet(null, s)) {
            s.cancel();
        } else {
            s.request(Long.MAX_VALUE);
        }
    }

    @Override
    public void onNext(T t) {
        if (t == null) {
            throw new NullPointerException("onNext: t is null");
        }
        if (!done && !cancelled) {
            queue.offer(t);
            drain();
        }
    }

    @Override
    public void onError(Throwable t) {
        if (t == null) {
            throw new NullPointerException("onError: t is null");
        }
        if (!done && !cancelled) {
            if (error.compareAndSet(null, t)) {
                done = true;
                drain();
            }
        }
    }

    @Override
    public void onComplete() {
        if (!done && !cancelled) {
            done = true;
            drain();
        }
    }

    @Override
    public void subscribe(Subscriber<? super T> s) {
        if (once.compareAndSet(false, true)) {
            s.onSubscribe(new Subscription() {
                @Override
                public void request(long n) {
                    innerRequest(n);
                }
                
                @Override
                public void cancel() {
                    innerCancel();
                }
            });
            actual.set(s);
            if (cancelled) {
                actual.lazySet(null);
                return;
            }
            drain();
        } else {
            s.onSubscribe(new EmptySubscription());
            s.onError(new IllegalStateException("This Processor accepts only one Subscriber."));
        }
    }
    
    public boolean hasSubscriber() {
        return actual.get() != null;
    }
    
    public long requested() {
        return requested.get();
    }
    
    void drain() {
        if (wip.getAndIncrement() != 0) {
            return;
        }
        
        int missed = 1;
        Subscriber<? super T> a = actual.get();
        long e = emitted;
        boolean delayError = this.delayError;
        Queue<T> q = queue;
        
        for (;;) {

            if (a != null) {

                long r = requested.get();

                while (e != r) {
                    if (cancelled) {
                        actual.lazySet(null);
                        q.clear();
                        return;
                    }
                    
                    boolean d = done;
                    if (d && !delayError) {
                        Throwable ex = error.get();
                        if (ex != null) {
                            actual.lazySet(null);
                            q.clear();
                            a.onError(ex);
                            return;
                        }
                    }
                    
                    T v = queue.poll();
                    boolean empty = v == null;

                    if (d && empty) {
                        actual.lazySet(null);
                        Throwable ex = error.get();
                        if (ex != null) {
                            a.onError(ex);
                        } else {
                            a.onComplete();
                        }
                        return;
                    }
                    
                    if (empty) {
                        break;
                    }
                    
                    a.onNext(v);

                    e++;
                }
                
                if (e == r) {
                    if (cancelled) {
                        actual.lazySet(null);
                        q.clear();
                        return;
                    }
                    boolean d = done;
                    
                    if (d && !delayError) {
                        Throwable ex = error.get();
                        if (ex != null) {
                            actual.lazySet(null);
                            q.clear();
                            a.onError(ex);
                            return;
                        }
                    }
                    
                    boolean empty = q.isEmpty();
                    
                    if (d && empty) {
                        actual.lazySet(null);
                        Throwable ex = error.get();
                        if (ex != null) {
                            a.onError(ex);
                        } else {
                            a.onComplete();
                        }
                        return;
                    }
                }
            }

            int w = wip.get();
            if (w == missed) {
                emitted = e;
                missed = wip.addAndGet(-missed);
                if (missed == 0) {
                    break;
                }
            } else {
                missed = w;
            }
            if (a == null) {
                a = actual.get();
            }
        }
    }

    void innerRequest(long n) {
        if (n <= 0L) {
            Subscription u = upstream.getAndSet(CANCELLED);
            if (u != null) {
                u.cancel();
            }
            onError(new IllegalArgumentException("§3.9 violated: positive request amount required but it was " + n));
        } else {
            for (;;) {
                long r = requested.get();
                long u = r + n;
                if (u < 0L) {
                    u = Long.MAX_VALUE;
                }
                if (requested.compareAndSet(r, u)) {
                    drain();
                    break;
                }
            }
        }
    }

    void innerCancel() {
        if (!cancelled) {
            cancelled = true;
            Subscription u = upstream.getAndSet(CANCELLED);
            if (u != null) {
                u.cancel();
            }
            if (wip.getAndIncrement() == 0) {
                actual.lazySet(null);
                queue.clear();
            }
        }
    }
}
