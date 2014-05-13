package test;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.tests.AbstractTest;
import com.hazelcast.stabilizer.tests.TestRunner;
import com.hazelcast.stabilizer.worker.ExceptionReporter;
import test.processors.ActualPropertiesSetter;
import test.processors.RequestedPropertiesSetter;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class HazelnutsTest extends AbstractTest {

    private final static ILogger log = Logger.getLogger(HazelnutsTest.class);

    private IAtomicLong totalCounter;
    private AtomicLong operations = new AtomicLong();
    private IAtomicLong counter;

    //props
    public int threadCount = 1;
    public int itemCount = 1500000;
    public int logFrequency = 10000;
    public int performanceUpdateFrequency = 10000;

    @Override
    public void localSetup() throws Exception {
        HazelcastInstance targetInstance = getTargetInstance();

        totalCounter = targetInstance.getAtomicLong(getTestId() + ":TotalCounter");
        counter = targetInstance.getAtomicLong("counter");

        preloadData();
    }

    private void preloadData() {
        final ExecutorService executorService = Executors.newFixedThreadPool(threadCount);

        long chunk = itemCount / threadCount;
        for (int k = 0; k < threadCount; k++) {
            executorService.submit(new PreloadWorker(0, k * chunk, chunk));
        }

        // Wait for completion of all submitted tasks
        executorService.shutdown();
        while (!stopped()) {
            try {
                if (executorService.awaitTermination(1, TimeUnit.SECONDS)) {
                    break;
                }
            } catch (InterruptedException e) {
                break;
            }
        }

        log.info("Finished preloading data");
    }

    @Override
    public void createTestThreads() {

        long chunk = itemCount / threadCount;

        for (int k = 0; k < threadCount; k++) {
            spawn(new TestWorker(0, k * chunk, chunk));
        }

        spawn(new ProgressMonitor());
    }

    @Override
    public void globalVerify() {
        long expectedCount = totalCounter.get();
        long foundCount = counter.get();

        if (expectedCount != foundCount) {
//            throw new TestFailureException("Expected count: " + expectedCount + " but found count was: " + foundCount);
        }
    }

    @Override
    public void globalTearDown() throws Exception {
        counter.destroy();
        totalCounter.destroy();
    }

    @Override
    public long getOperationCount() {
        return operations.get();
    }

    private class PreloadWorker implements Runnable {

        private Date time = new Date();
        private String requestId = UUID.randomUUID().toString();

        private long baseId;
        private long firstItem;
        private long itemCount;
        private IMap storages;
        private HazelcastInstance targetInstance;

        private PreloadWorker(final long baseId, final long firstItem, final long itemCount) {
            this.baseId = baseId;
            this.firstItem = firstItem;
            this.itemCount = itemCount;
        }

        private void preloadData() {

            final Map<String, String> data = TestDataFactory.generateInitialData();

            for (long i = 0; i < itemCount; i++) {

                if (stopped()) {
                    return;
                }

                long itemId = firstItem + i;
                final long id = baseId + itemId / 10;

                final String storageId = "" + id;
                final String containerId = String.format("xxxxxxxxxxxxxx_%04x", itemId % 10);
                final Integer position = null;

                final String key = storageId + "@" + containerId;
                storages.executeOnKey(key, new ActualPropertiesSetter(time, requestId, position, data));
                storages.executeOnKey(key, new RequestedPropertiesSetter(time, requestId, position, data));

//                counter.incrementAndGet();
            }
        }

        @Override
        public void run() {

            // "10.104.13.221"
            final String server = "10.210.182.71";

            log.info(Thread.currentThread().getName() + " preloading");

            ClientConfig clientConfig = new ClientConfig();
            clientConfig.getNetworkConfig().addAddress(server);
            targetInstance = HazelcastClient.newHazelcastClient(clientConfig);

            storages = targetInstance.getMap("storages");

            preloadData();

            log.info(Thread.currentThread().getName() + " preloading done");

            targetInstance.shutdown();
        }
    }

    private class TestWorker implements Runnable {
        private final Random random = new Random();

        private Date time = new Date();
        private String requestId = UUID.randomUUID().toString();

        private long baseId;
        private long firstItem;
        private long itemCount;
        private IMap storages;
        private HazelcastInstance targetInstance;

        private TestWorker(final long baseId, final long firstItem, final long itemCount) {
            this.baseId = baseId;
            this.firstItem = firstItem;
            this.itemCount = itemCount;
        }

        private void updateContinuously() {

            final Map<String, String> data = TestDataFactory.generateInitialData();

            while (!stopped()) {
                for (long i = 0; i < itemCount; i++) {

                    if (stopped()) {
                        return;
                    }

                    long itemId = firstItem + i;
                    final long id = baseId + itemId / 10;

                    final String storageId = "" + id;
                    final String containerId = String.format("xxxxxxxxxxxxxx_%04x", itemId % 10);
                    final Integer position = null;

                    final String key = storageId + "@" + containerId;
                    storages.executeOnKey(key, new ActualPropertiesSetter(time, requestId, position, data));

                    operations.incrementAndGet();
                }
            }
        }

        @Override
        public void run() {

            // "10.104.13.221"
            final String server = "10.210.182.71";

            log.info(Thread.currentThread().getName() + " starting continuous update");

            ClientConfig clientConfig = new ClientConfig();
            clientConfig.getNetworkConfig().addAddress(server);
            targetInstance = HazelcastClient.newHazelcastClient(clientConfig);

            storages = targetInstance.getMap("storages");

            updateContinuously();

            log.info(Thread.currentThread().getName() + " continuous update finished");

            targetInstance.shutdown();
        }
    }

    private class ProgressMonitor implements Runnable {

        private final static long WINDOW_SIZE = 20;

        private List<Long> rpsHistory = new ArrayList<Long>();

        @Override
        public void run() {

            long baselineRps = 0;
            long lastOps = operations.get();
            while (!stopped()) {

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    break;
                }

                final long ops = operations.get();

                long rps = ops - lastOps;
                lastOps = ops;

                log.info("RPS=" + rps);

                rpsHistory.add(rps);
                while (rpsHistory.size() > WINDOW_SIZE) {
                    rpsHistory.remove(0);
                }

                if (rpsHistory.size() < WINDOW_SIZE) {
                    continue;
                }

                long avgRps = 0;
                for (final Long value : rpsHistory) {
                    avgRps += value;
                }
                avgRps /= rpsHistory.size();

                if (baselineRps < avgRps) {
                    baselineRps = avgRps;
                }

                log.info("average RPS=" + avgRps + ", baseline=" + baselineRps);
                if (avgRps < baselineRps / 4) {
                    log.severe("average RPS is significantly below baseline");
                    ExceptionReporter.report(new Exception("average RPS is significantly below baseline"));
                }
            }
        }
    }


    public static void main(String[] args) throws Exception {
        HazelnutsTest test = new HazelnutsTest();
        new TestRunner().run(test, 720);
    }
}

