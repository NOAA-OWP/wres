package wres.engine.statistics.metric;

import static org.junit.Assert.assertTrue;

import java.util.concurrent.ExecutionException;
import java.util.function.BiPredicate;

import org.junit.Test;

import wres.engine.statistics.metric.MetricCollection.MetricCollectionBuilder;
import wres.engine.statistics.metric.inputs.DichotomousPairs;
import wres.engine.statistics.metric.inputs.DiscreteProbabilityPairs;
import wres.engine.statistics.metric.inputs.MulticategoryPairs;
import wres.engine.statistics.metric.inputs.SingleValuedPairs;
import wres.engine.statistics.metric.outputs.MetricOutputCollection;
import wres.engine.statistics.metric.outputs.ScalarOutput;
import wres.engine.statistics.metric.outputs.VectorOutput;

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

        //Add some appropriate metrics to the collection
        n.add(MetricFactory.ofMeanError()); //Should be -200.55
        n.add(MetricFactory.ofMeanAbsoluteError()); //Should be 201.37
        n.add(MetricFactory.ofRootMeanSquareError()); //Should be 632.4586381732801

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

        //Add some appropriate metrics to the collection
        m.add(MetricFactory.ofCriticalSuccessIndex()); //Should be 0.5734265734265734
        m.add(MetricFactory.ofProbabilityOfDetection()); //Should be 0.780952380952381
        m.add(MetricFactory.ofProbabilityOfFalseDetection()); //Should be 0.14615384615384616
        m.add(MetricFactory.ofPeirceSkillScore()); //Should be 0.6347985347985348
        m.add(MetricFactory.ofEquitableThreatScore()); //Should be 0.43768152544513195

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
        //Add some appropriate metrics to the collection
        n.add(MetricFactory.ofBrierScore()); //Should be 0.26
        n.add(MetricFactory.ofBrierSkillScore()); //Should be 0.11363636363636376

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
        //Add some appropriate metrics to the collection
        n.add(MetricFactory.ofMeanSquareError()); //Should be 400003.929
        n.add(MetricFactory.ofMeanSquareErrorSkillScore()); //Should be 0.8007025335093799

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

        //Add some appropriate metrics to the collection
        n.add(MetricFactory.ofPeirceSkillScoreMulti()); //Should be 0.05057466520850963

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

//    /**
//     * Construct a collection of metrics that consume single-valued pairs and produce scalar outputs. Compute and check
//     * the results.
//     * 
//     * @throws ExecutionException
//     * @throws InterruptedException
//     * @throws MetricCalculationException
//     */
//
//    @Test
//    public void test6OfSingleValuedScalar() throws MetricCalculationException, InterruptedException, ExecutionException
//    {
//
//        //Create a collection of metrics that consume single-valued pairs and produce a scalar output
//        final MetricCollectionBuilder<SingleValuedPairs, ScalarOutput> n = MetricCollectionBuilder.of();
//
//        //final MetricCollection<SingleValuedPairs, ScalarOutput> n = MetricCollection.ofSingleValuedScalar();
//
//        //Add some appropriate metrics to the collection
//        n.add(MetricFactory.ofMeanError()); //Should be -200.55
//        n.add(MetricFactory.ofMeanAbsoluteError()); //Should be 201.37
//        n.add(MetricFactory.ofRootMeanSquareError()); //Should be 632.4586381732801
//
//        //Set the input and an executor service
//        final ExecutorService pairPool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
//        final ExecutorService metricPool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
//
//        //Get the future input
//
//        final SingleValuedPairs returnMe = MetricTestDataFactory.getSingleValuedPairsOne();
//
//        final FutureTask<SingleValuedPairs> futureInput =
//                                                        new FutureTask<SingleValuedPairs>(new Callable<SingleValuedPairs>()
//                                                        {
//                                                            public SingleValuedPairs call()
//                                                            {
//                                                                return returnMe;
//                                                            }
//                                                        });
//        n.setMetricInput(futureInput);
//        n.setExecutorService(metricPool);
//
//        //Finalize
//        final MetricCollection<SingleValuedPairs, ScalarOutput> collection = n.build();
//
////        //Add a method ofMetricTask() to the metric factory to wrap a metric in a metric task
////
////        final List<MetricTask<SingleValuedPairs, ScalarOutput>> tasks = new ArrayList<>();
////        final Metric<SingleValuedPairs, ScalarOutput> me = MetricFactory.ofMeanError();
////        final Metric<SingleValuedPairs, ScalarOutput> mae = MetricFactory.ofMeanAbsoluteError();
////        final Metric<SingleValuedPairs, ScalarOutput> rmse = MetricFactory.ofRootMeanSquareError();
////
////        final FutureTask<SingleValuedPairs> input = new FutureTask<SingleValuedPairs>(new Callable<SingleValuedPairs>()
////        {
////            public SingleValuedPairs call()
////            {
////                return MetricTestDataFactory.getSingleValuedPairsOne();
////            }
////        });
////
////        final ExecutorService pairPool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
////        final ExecutorService metricPool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
////
////        tasks.add(new MetricTask<>(me, input));
////        tasks.add(new MetricTask<>(mae, input));
////        tasks.add(new MetricTask<>(rmse, input));
////
////        //final Could wrap the final input in a <Future>
////
////        List<Future<ScalarOutput>> result;
////        try
////        {
////            pairPool.submit(input); //Get the pairs
////            result = metricPool.invokeAll(tasks);
////            result.forEach(g -> {
////                try
////                {
////                    System.out.println(g.get().getData());
////                }
////                catch(InterruptedException | ExecutionException e)
////                {
////                    e.printStackTrace();
////                }
////            });
////        }
////        catch(final InterruptedException e)
////        {
////            e.printStackTrace();
////        }
//
//        //Compute them
//        //final MetricOutputCollection<ScalarOutput> d = collection.apply(input);
//
//        //Get the pairs
//        pairPool.submit(futureInput); //Get the pairs
//
//        //Compute
//        final MetricOutputCollection<ScalarOutput> d = collection.call();
//
//        //Print them
//        d.stream().forEach(g -> System.out.println(g.getData()));
//
//        //Check them   
//        final Double expectedFirst = -200.55;
//        final Double expectedSecond = 201.37;
//        final Double expectedThird = 632.4586381732801;
//        final Double actualFirst = d.get(0).getData();
//        final Double actualSecond = d.get(1).getData();
//        final Double actualThird = d.get(2).getData();
//
//        final BiPredicate<Double, Double> testMe = FunctionFactory.doubleEquals();
//
//        assertTrue("Expected value: " + expectedFirst + ". Actual value: " + actualFirst + ".",
//                   testMe.test(actualFirst, expectedFirst));
//        assertTrue("Expected value: " + expectedSecond + ". Actual value: " + actualSecond + ".",
//                   testMe.test(actualSecond, expectedSecond));
//        assertTrue("Expected value: " + expectedThird + ". Actual value: " + actualThird + ".",
//                   testMe.test(actualThird, expectedThird));
//    }
//
//    /**
//     * Construct a collection of metrics that consume single-valued pairs and produce scalar outputs. Compute and check
//     * the results.
//     * 
//     * @throws ExecutionException
//     * @throws InterruptedException
//     * @throws MetricCalculationException
//     */
//
//    @Test
//    public void test7OfDichotomousScalar() throws MetricCalculationException, InterruptedException, ExecutionException
//    {
//
//        //Set the input and an executor service
//        final ExecutorService pairPool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
//        final ExecutorService metricPool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
//
//        //Get the future input
//        final FutureTask<DichotomousPairs> futureInput =
//                                                       new FutureTask<DichotomousPairs>(new Callable<DichotomousPairs>()
//                                                       {
//                                                           public DichotomousPairs call()
//                                                           {
//                                                               return MetricTestDataFactory.getDichotomousPairsOne();
//                                                           }
//                                                       });
//
//        //Create an immutable collection of metrics that take dichotomous pairs and produce a scalar output 
//        final MetricCollectionBuilder<DichotomousPairs, ScalarOutput> m = MetricCollectionBuilder.of();
//        final MetricCollection<DichotomousPairs, ScalarOutput> collection =
//                                                                          m.add(MetricFactory.ofCriticalSuccessIndex())
//                                                                           .add(MetricFactory.ofProbabilityOfDetection())
//                                                                           .add(MetricFactory.ofProbabilityOfFalseDetection())
//                                                                           .add(MetricFactory.ofPeirceSkillScore())
//                                                                           .add(MetricFactory.ofEquitableThreatScore())
//                                                                           .setMetricInput(futureInput)
//                                                                           .setExecutorService(metricPool)
//                                                                           .build();
//
//        //Compute the pairs
//        pairPool.submit(futureInput);
//
//        //Compute the metric
//        final MetricOutputCollection<ScalarOutput> d = collection.call();
//
//        //Print them
//        d.stream().forEach(g -> System.out.println(g.getData()));
//
//    }

}
