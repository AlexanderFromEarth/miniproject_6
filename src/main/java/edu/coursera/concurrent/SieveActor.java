package edu.coursera.concurrent;

import edu.rice.pcdp.Actor;
import static edu.rice.pcdp.PCDP.finish;

/**
 * An actor-based implementation of the Sieve of Eratosthenes.
 */
public final class SieveActor extends Sieve {
    /**
     * {@inheritDoc}
     */
    @Override
    public int countPrimes(final int limit) {
        final SieveActorActor slave = new SieveActorActor();

        finish(() -> {
            for (int i = 3; i <= limit; i += 2) {
                slave.send(i);
            }
        });

        return slave.primesCount() + 1;
    }

    /**
     * An actor class that helps implement the Sieve of Eratosthenes in parallel.
     */
    public static final class SieveActorActor extends Actor {
        private SieveActorActor slave;
        private int primesCount;
        private final int[] localPrimes;

        public SieveActorActor(final int batchSize) {
            this.primesCount = 0;
            this.localPrimes = new int[batchSize];
        }

        public SieveActorActor() {
            this(100);
        }

        public int primesCount() {
            return this.primesCount + (this.slave != null ? this.slave.primesCount() : 0);
        }

        /**
         * Process a single message sent to this actor.
         *
         * @param msg Received message
         */
        @Override
        public void process(final Object msg) {
            Integer candidate = (Integer) msg;

            if (!this.isPrime(candidate)) {
                return;
            }
            if (this.primesCount < this.localPrimes.length) {
                this.localPrimes[this.primesCount++] = candidate;
            } else {
                if (this.slave == null) {
                    this.slave = new SieveActorActor(this.localPrimes.length);
                }

                this.slave.send(candidate);
            }
        }

        private boolean isPrime(final int candidateNumber) {
            for (int i = 0; i < this.primesCount; i++) {
                if (candidateNumber % this.localPrimes[i] == 0) {
                    return false;
                }
            }

            return true;
        }
    }
}
