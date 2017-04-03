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

import java.util.concurrent.*;

import org.reactivestreams.Publisher;
import org.testng.annotations.*;

public class UnicastProcessorDelayErrorAsPublisherTest extends PublisherVerification<Integer> {

    ExecutorService ex;

    public UnicastProcessorDelayErrorAsPublisherTest() {
        super(new TestEnvironment());
    }

    @BeforeClass
    void before() {
        ex = Executors.newCachedThreadPool();
    }

    @AfterClass
    void after() {
        if (ex != null) {
            ex.shutdown();
        }
    }

    @Override
    public Publisher<Integer> createPublisher(final long elements) {
        final UnicastProcessor<Integer> up = new UnicastProcessor<Integer>(true);
        up.start();

        ex.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    long start = System.currentTimeMillis();
                    while (!up.hasSubscriber()) {
                        Thread.sleep(1);
                        if (System.currentTimeMillis() - start > 200) {
                            return;
                        }
                    }
                    
                    for (int i = 0; i < elements; i++) {
                        start = System.currentTimeMillis();
                        while (up.requested() <= i) {
                            if (!up.hasSubscriber()) {
                                return;
                            }
                            Thread.sleep(1);
                            if (System.currentTimeMillis() - start > 300) {
                                return;
                            }
                        }
                        up.onNext(i);
                    }
                    up.onComplete();
                } catch (InterruptedException ex) {
                }
            }
        });
        
        return up;
    }

    @Override
    public Publisher<Integer> createFailedPublisher() {
        return null;
    }

    @Override
    public long maxElementsFromPublisher() {
        return 1024;
    }
}
