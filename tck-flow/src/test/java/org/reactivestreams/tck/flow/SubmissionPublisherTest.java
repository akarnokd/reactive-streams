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

package org.reactivestreams.tck.flow;

import org.reactivestreams.tck.TestEnvironment;

import java.util.concurrent.*;

public class SubmissionPublisherTest extends FlowPublisherVerification<Integer> {

    public SubmissionPublisherTest() {
        super(new TestEnvironment());
    }

    @Override
    public Flow.Publisher<Integer> createFlowPublisher(long elements) {
        SubmissionPublisher<Integer> sp = new SubmissionPublisher<>();

        ForkJoinPool.commonPool().submit(() -> {
            while (sp.getNumberOfSubscribers() == 0) {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException ex) {
                    return;
                }
            }

            for (int i = 0; i < elements; i++) {
                sp.submit(i);
            }

            sp.close();
        });

        return sp;
    }

    @Override
    public Flow.Publisher<Integer> createFailedFlowPublisher() {
        SubmissionPublisher<Integer> sp = new SubmissionPublisher<>();
        sp.closeExceptionally(new Exception());
        return sp;
    }
}
