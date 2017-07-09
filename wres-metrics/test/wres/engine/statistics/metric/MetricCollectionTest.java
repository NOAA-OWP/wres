package wres.engine.statistics.metric;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.function.BiPredicate;

import org.junit.Test;

import wres.datamodel.metric.DefaultMetricOutputFactory;
import wres.datamodel.metric.DichotomousPairs;
import wres.datamodel.metric.DiscreteProbabilityPairs;
import wres.datamodel.metric.MetricOutputCollection;
import wres.datamodel.metric.MetricOutputFactory;
import wres.datamodel.metric.MulticategoryPairs;
import wres.datamodel.metric.ScalarOutput;
import wres.datamodel.metric.SingleValuedPairs;
import wres.datamodel.metric.VectorOutput;
import wres.engine.statistics.metric.MetricCollection.MetricCollectionBuilder;

/**
 * Tests the {@link MetricCollection}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public class MetricCollectionTest
{

    /**
     * Construct a collection of metrics that consume single-valued pairs and produce scalar outputs. Compute and check
     * the results.
     * 
     * @throws ExecutionException
     * @throws InterruptedException
     * @throws MetricCalculationException
     */

    @Test
    public void test1OfSingleValuedScalar() throws MetricCalculationException, InterruptedException, ExecutionException
    {
        //Generate some data
        final SingleValuedPairs input = MetricTestDataFactory.getSingleValuedPairsOne();

        //Create a collection of metrics that consume single-valued pairs and produce a scalar output
        final MetricCollectionBuilder<SingleValuedPairs, ScalarOutput> n = MetricCollectionBuilder.of();
        final MetricOutputFactory outF = DefaultMetricOutputFactory.getInstance();
        final MetricFactory metF = MetricFactory.getInstance(outF);
        n.setOutputFactory(outF);
        
        //Add some appropriate metrics to the collection
        n.add(metF.ofMeanError()); //Should be -200.55
        n.add(metF.ofMeanAbsoluteError()); //Should be 201.37
        n.add(metF.ofRootMeanSquareError()); //Should be 632.4586381732801

        //Finalize
        final MetricCollection<SingleValuedPairs, ScalarOutput> collection = n.build();

        //Compute them
        final MetricOutputCollection<ScalarOutput> d = collection.apply(input);

        //Print them
        //d.stream().forEach(g -> System.out.println(g.getData()));

        //Check them   
        final Double expectedFirst = -200.55;
        final Double expectedSecond = 201.37;
        final Double expectedThird = 632.4586381732801;
        final Double actualFirst = d.get(0).getData();
        final Double actualSecond = d.get(1).getData();
        final Double actualThird = d.get(2).getData();

        final BiPredicate<Double, Double> testMe = FunctionFactory.doubleEquals();

        assertTrue("Expected value: " + expectedFirst + ". Actual value: " + actualFirst + ".",
                   testMe.test(actualFirst, expectedFirst));
        assertTrue("Expected value: " + expectedSecond + ". Actual value: " + actualSecond + ".",
                   testMe.test(actualSecond, expectedSecond));
        assertTrue("Expected value: " + expectedThird + ". Actual value: " + actualThird + ".",
                   testMe.test(actualThird, expectedThird));
    }

    /**
     * Construct a collection of metrics that consume dichotomous pairs and produce scalar outputs. Compute and check
     * the results.
     */

    @Test
    public void test2OfDichotomousScalar()
    {
        //Generate some data
        final DichotomousPairs input = MetricTestDataFactory.getDichotomousPairsOne();

        //Create a collection of dichotomous metrics that produce a scalar output. Since all scores implement 
        //Collectable, they make efficient use of common intermediate data. In this case, all scores require the 2x2
        //Contingency Table, which is computed only once
        final MetricCollectionBuilder<DichotomousPairs, ScalarOutput> m = MetricCollectionBuilder.of();
        final MetricOutputFactory outF = DefaultMetricOutputFactory.getInstance();
        final MetricFactory metF = MetricFactory.getInstance(outF);
        m.setOutputFactory(outF);
        //Add some appropriate metrics to the collection     
        m.add(metF.ofCriticalSuccessIndex()); //Should be 0.5734265734265734
        m.add(metF.ofProbabilityOfDetection()); //Should be 0.780952380952381
        m.add(metF.ofProbabilityOfFalseDetection()); //Should be 0.14615384615384616
        m.add(metF.ofPeirceSkillScore()); //Should be 0.6347985347985348
        m.add(metF.ofEquitableThreatScore()); //Should be 0.43768152544513195

        //Finalize
        final MetricCollection<DichotomousPairs, ScalarOutput> collection = m.build();

        //Compute them
        final MetricOutputCollection<ScalarOutput> c = collection.apply(input);

        //Print them
        //c.stream().forEach(g -> System.out.println(g.getData().doubleValue()));

        //Check them
        final Double expectedFirst = 0.5734265734265734;
        final Double expectedSecond = 0.780952380952381;
        final Double expectedThird = 0.14615384615384616;
        final Double expectedFourth = 0.6347985347985348;
        final Double expectedFifth = 0.43768152544513195;
        final Double actualFirst = c.get(0).getData();
        final Double actualSecond = c.get(1).getData();
        final Double actualThird = c.get(2).getData();
        final Double actualFourth = c.get(3).getData();
        final Double actualFifth = c.get(4).getData();

        final BiPredicate<Double, Double> testMe = FunctionFactory.doubleEquals();

        assertTrue("Expected value: " + expectedFirst + ". Actual value: " + actualFirst + ".",
                   testMe.test(actualFirst, expectedFirst));
        assertTrue("Expected value: " + expectedSecond + ". Actual value: " + actualSecond + ".",
                   testMe.test(actualSecond, expectedSecond));
        assertTrue("Expected value: " + expectedThird + ". Actual value: " + actualThird + ".",
                   testMe.test(actualThird, expectedThird));
        assertTrue("Expected value: " + expectedFourth + ". Actual value: " + actualFourth + ".",
                   testMe.test(actualFourth, expectedFourth));
        assertTrue("Expected value: " + expectedFifth + ". Actual value: " + actualFifth + ".",
                   testMe.test(actualFifth, expectedFifth));
    }

    /**
     * Construct a collection of metrics that consume discrete probability pairs and produce vector outputs. Compute and
     * check the results.
     */

    @Test
    public void test3OfDiscreteProbabilityVector()
    {
        //Generate some data
        final DiscreteProbabilityPairs input = MetricTestDataFactory.getDiscreteProbabilityPairsTwo();

        //Create a collection metrics that consume probabilistic pairs and generate vector outputs
        final MetricCollectionBuilder<DiscreteProbabilityPairs, VectorOutput> n = MetricCollectionBuilder.of();
        final MetricOutputFactory outF = DefaultMetricOutputFactory.getInstance();
        final MetricFactory metF = MetricFactory.getInstance(outF);
        n.setOutputFactory(outF);
        
        //Add some appropriate metrics to the collection
        n.add(metF.ofBrierScore()); //Should be 0.26
        n.add(metF.ofBrierSkillScore()); //Should be 0.11363636363636376

        //Finalize
        final MetricCollection<DiscreteProbabilityPairs, VectorOutput> collection = n.build();

        //Compute them
        final MetricOutputCollection<VectorOutput> d = collection.apply(input);

        //Print them
        //d.stream().forEach(g -> System.out.println(((ScalarOutput)g).getData().valueOf()));

        //Check them
        final Double expectedFirst = 0.26;
        final Double expectedSecond = 0.11363636363636376;
        final Double actualFirst = d.get(0).getData().getDoubles()[0];
        final Double actualSecond = d.get(1).getData().getDoubles()[0];

        final BiPredicate<Double, Double> testMe = FunctionFactory.doubleEquals();

        assertTrue("Expected value: " + expectedFirst + ". Actual value: " + actualFirst + ".",
                   testMe.test(expectedFirst, actualFirst));
        assertTrue("Expected value: " + expectedSecond + ". Actual value: " + actualSecond + ".",
                   testMe.test(expectedFirst, actualFirst));
    }

    /**
     * Construct a collection of metrics that consume single-valued pairs and produce vector outputs. Compute and check
     * the results.
     */

    @Test
    public void test4OfSingleValuedVector()
    {
        //Generate some data
        final SingleValuedPairs input = MetricTestDataFactory.getSingleValuedPairsTwo();

        //Create a collection metrics that consume single-valued pairs and produce vector outputs
        final MetricCollectionBuilder<SingleValuedPairs, VectorOutput> n = MetricCollectionBuilder.of();
        final MetricOutputFactory outF = DefaultMetricOutputFactory.getInstance();
        final MetricFactory metF = MetricFactory.getInstance(outF);
        n.setOutputFactory(outF);
        
        //Add some appropriate metrics to the collection
        n.add(metF.ofMeanSquareError()); //Should be 400003.929
        n.add(metF.ofMeanSquareErrorSkillScore()); //Should be 0.8007025335093799

        //Finalize
        final MetricCollection<SingleValuedPairs, VectorOutput> collection = n.build();

        //Compute them
        final MetricOutputCollection<VectorOutput> d = collection.apply(input);

        //Print them
        //d.stream().forEach(g -> System.out.println(((ScalarOutput)g).getData().valueOf()));

        //Check them
        final Double expectedFirst = 400003.929;
        final Double expectedSecond = 0.8007025335093799;
        final Double actualFirst = d.get(0).getData().getDoubles()[0];
        final Double actualSecond = d.get(1).getData().getDoubles()[0];

        final BiPredicate<Double, Double> testMe = FunctionFactory.doubleEquals();

        assertTrue("Expected value: " + expectedFirst + ". Actual value: " + actualFirst + ".",
                   testMe.test(expectedFirst, actualFirst));
        assertTrue("Expected value: " + expectedSecond + ". Actual value: " + actualSecond + ".",
                   testMe.test(expectedFirst, actualFirst));
    }

    /**
     * Construct a collection of metrics that consume multicategory pairs and produce scalar outputs. Compute and check
     * the results.
     */

    @Test
    public void test5OfMulticategoryScalar()
    {
        //Generate some data
        final MulticategoryPairs input = MetricTestDataFactory.getMulticategoryPairsOne();

        //Create a collection of multicategory metrics that produce a scalar output. 
        final MetricCollectionBuilder<MulticategoryPairs, ScalarOutput> n = MetricCollectionBuilder.of();
        final MetricOutputFactory outF = DefaultMetricOutputFactory.getInstance();
        final MetricFactory metF = MetricFactory.getInstance(outF);
        n.setOutputFactory(outF);
        
        //Add some appropriate metrics to the collection
        n.add(metF.ofPeirceSkillScoreMulti()); //Should be 0.05057466520850963

        //Finalize
        final MetricCollection<MulticategoryPairs, ScalarOutput> collection = n.build();

        //Compute them
        final MetricOutputCollection<ScalarOutput> c = collection.apply(input);

        //Print them
        //c.stream().forEach(g -> System.out.println(g.getData().doubleValue()));

        //Check them
        final Double expectedFirst = 0.05057466520850963;
        final Double actualFirst = c.get(0).getData();

        final BiPredicate<Double, Double> testMe = FunctionFactory.doubleEquals();

        assertTrue("Expected value: " + expectedFirst + ". Actual value: " + actualFirst + ".",
                   testMe.test(actualFirst, expectedFirst));
    }

    /**
     * Tests the exceptions associated with {@link MetricCollection}.
     * 
     * @throws ExecutionException
     * @throws InterruptedException
     * @throws MetricCalculationException
     */

    @Test
    public void test6ExceptionTests() throws MetricCalculationException, InterruptedException, ExecutionException
    {

        final ExecutorService metricPool = Executors.newSingleThreadExecutor();

        try
        {

            //Generate some data
            final SingleValuedPairs input = MetricTestDataFactory.getSingleValuedPairsOne();

            //Create a collection of metrics that consume single-valued pairs and produce a scalar output
            final MetricCollectionBuilder<SingleValuedPairs, ScalarOutput> n = MetricCollectionBuilder.of();
            final MetricOutputFactory outF = DefaultMetricOutputFactory.getInstance();
            final MetricFactory metF = MetricFactory.getInstance(outF);
            n.setOutputFactory(outF);
            //Add some appropriate metrics to the collection
            n.add(metF.ofMeanError());

            //Wrap an input in a future
            final FutureTask<SingleValuedPairs> futureInput =
                                                            new FutureTask<SingleValuedPairs>(new Callable<SingleValuedPairs>()
                                                            {
                                                                public SingleValuedPairs call()
                                                                {
                                                                    return input;
                                                                }
                                                            });

            //Add the data
            n.setMetricInput(futureInput);

            //Set an executor
            n.setExecutorService(metricPool);

            //Finalize
            final MetricCollection<SingleValuedPairs, ScalarOutput> collection = n.build();

            //Calling apply should generate an exception
            try
            {
                collection.apply(input);
                fail("Expected a checked exception on calling apply with a new input.");
            }
            catch(final Exception e)
            {
            }
            //Try to build with no metrics
            try
            {
                final MetricCollectionBuilder<SingleValuedPairs, ScalarOutput> m = MetricCollectionBuilder.of();
                m.build();
                fail("Expected a checked exception on constructing a metric collection with no metrics.");
            }
            catch(final Exception e)
            {
            }
            //Try to call with no input
            try
            {
                final MetricCollectionBuilder<SingleValuedPairs, ScalarOutput> m = MetricCollectionBuilder.of();
                m.add(metF.ofMeanError());
                final MetricCollection<SingleValuedPairs, ScalarOutput> cTest = m.build();
                cTest.call();
                fail("Expected a checked exception on calling a metric collection without an input.");
            }
            catch(final Exception e)
            {
            }
        }
        finally
        {
            metricPool.shutdown();
        }
    }

    /**
     * Tests a {@link MetricCollection} as an implementation of {@link Callable}. Specifically, tests a collection of
     * {@link Collectable}, each of which is contained in a {@link CollectableTask}.
     * 
     * @throws ExecutionException if the execution fails
     * @throws InterruptedException if the execution is interrupted
     * @throws MetricCalculationException if the metric calculation fails
     */

    @Test
    public void test7Callable() throws MetricCalculationException, InterruptedException, ExecutionException
    {

        //Set the input and an executor service
        final ExecutorService pairPool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        final ExecutorService metricPool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        //Construct the factories
        final MetricOutputFactory outF = DefaultMetricOutputFactory.getInstance();
        final MetricFactory metF = MetricFactory.getInstance(outF);
        try
        {

            //Get the future input
            final FutureTask<DichotomousPairs> futureInput =
                                                           new FutureTask<DichotomousPairs>(new Callable<DichotomousPairs>()
                                                           {
                                                               public DichotomousPairs call()
                                                               {
                                                                   return MetricTestDataFactory.getDichotomousPairsOne();
                                                               }
                                                           });

            //Create an immutable collection of metrics that take dichotomous pairs and produce a scalar output 
            final MetricCollectionBuilder<DichotomousPairs, ScalarOutput> m = MetricCollectionBuilder.of();
            final MetricCollection<DichotomousPairs, ScalarOutput> collection =
                                                                              m.add(metF.ofCriticalSuccessIndex())
                                                                               .add(metF.ofProbabilityOfDetection())
                                                                               .add(metF.ofProbabilityOfFalseDetection())
                                                                               .add(metF.ofPeirceSkillScore())
                                                                               .add(metF.ofEquitableThreatScore())
                                                                               .setMetricInput(futureInput)
                                                                               .setExecutorService(metricPool)
                                                                               .setOutputFactory(outF)
                                                                               .build();

            //Compute the pairs
            pairPool.submit(futureInput);

            //Compute the metric
            final MetricOutputCollection<ScalarOutput> d = collection.call();

            //Check them
            final Double expectedFirst = 0.5734265734265734;
            final Double expectedSecond = 0.780952380952381;
            final Double expectedThird = 0.14615384615384616;
            final Double expectedFourth = 0.6347985347985348;
            final Double expectedFifth = 0.43768152544513195;
            final Double actualFirst = d.get(0).getData();
            final Double actualSecond = d.get(1).getData();
            final Double actualThird = d.get(2).getData();
            final Double actualFourth = d.get(3).getData();
            final Double actualFifth = d.get(4).getData();

            final BiPredicate<Double, Double> testMe = FunctionFactory.doubleEquals();

            assertTrue("Expected value: " + expectedFirst + ". Actual value: " + actualFirst + ".",
                       testMe.test(actualFirst, expectedFirst));
            assertTrue("Expected value: " + expectedSecond + ". Actual value: " + actualSecond + ".",
                       testMe.test(actualSecond, expectedSecond));
            assertTrue("Expected value: " + expectedThird + ". Actual value: " + actualThird + ".",
                       testMe.test(actualThird, expectedThird));
            assertTrue("Expected value: " + expectedFourth + ". Actual value: " + actualFourth + ".",
                       testMe.test(actualFourth, expectedFourth));
            assertTrue("Expected value: " + expectedFifth + ". Actual value: " + actualFifth + ".",
                       testMe.test(actualFifth, expectedFifth));
        }
        finally
        {
            pairPool.shutdown();
            metricPool.shutdown();
        }
    }

    /**
     * Tests a {@link MetricCollection} as an implementation of {@link Callable}. Specifically, tests a collection of
     * metrics that do not implement {@link Collectable}, each of which is contained in a {@link MetricTask}.
     * 
     * @throws ExecutionException if the execution fails
     * @throws InterruptedException if the execution is interrupted
     * @throws MetricCalculationException if the metric calculation fails
     */

    @Test
    public void test8Callable() throws MetricCalculationException, InterruptedException, ExecutionException
    {
        final ExecutorService metricPool = Executors.newSingleThreadExecutor();
        final ExecutorService pairPool = Executors.newSingleThreadExecutor();

        try
        {

            //Generate some data
            final SingleValuedPairs input = MetricTestDataFactory.getSingleValuedPairsOne();

            //Create a collection of metrics that consume single-valued pairs and produce a scalar output
            final MetricCollectionBuilder<SingleValuedPairs, ScalarOutput> n = MetricCollectionBuilder.of();
            final MetricOutputFactory outF = DefaultMetricOutputFactory.getInstance();
            final MetricFactory metF = MetricFactory.getInstance(outF);
            n.setOutputFactory(outF);
            
            //Add some appropriate metrics to the collection
            n.add(metF.ofMeanError());

            //Wrap an input in a future
            final FutureTask<SingleValuedPairs> futureInput =
                                                            new FutureTask<SingleValuedPairs>(new Callable<SingleValuedPairs>()
                                                            {
                                                                public SingleValuedPairs call()
                                                                {
                                                                    return input;
                                                                }
                                                            });

            //Set an executor
            n.setExecutorService(metricPool);

            //Compute the pairs
            pairPool.submit(futureInput);

            //Add the data
            n.setMetricInput(futureInput);

            //Finalize
            final MetricCollection<SingleValuedPairs, ScalarOutput> collection = n.build();
            //Compute
            final MetricOutputCollection<ScalarOutput> d = collection.call();
            //Check the results
            //Check them   
            final Double expectedFirst = -200.55;
            final Double actualFirst = d.get(0).getData();
            final BiPredicate<Double, Double> testMe = FunctionFactory.doubleEquals();
            assertTrue("Expected value: " + expectedFirst + ". Actual value: " + actualFirst + ".",
                       testMe.test(actualFirst, expectedFirst));
        }
        finally
        {
            pairPool.shutdown();
            metricPool.shutdown();
        }
    }

}
