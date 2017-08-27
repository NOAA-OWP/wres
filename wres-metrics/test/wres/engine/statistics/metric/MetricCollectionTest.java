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

import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.DichotomousPairs;
import wres.datamodel.DiscreteProbabilityPairs;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricOutputMapByMetric;
import wres.datamodel.MulticategoryPairs;
import wres.datamodel.ScalarOutput;
import wres.datamodel.SingleValuedPairs;
import wres.datamodel.VectorOutput;
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
     */

    @Test
    public void test1OfSingleValuedScalar()
    {
        //Generate some data
        final SingleValuedPairs input = MetricTestDataFactory.getSingleValuedPairsOne();

        //Create a collection of metrics that consume single-valued pairs and produce a scalar output
        final DataFactory outF = DefaultDataFactory.getInstance();
        final MetricFactory metF = MetricFactory.getInstance( outF );

        //Finalize
        final MetricCollection<SingleValuedPairs, ScalarOutput> collection =
                metF.ofSingleValuedScalarCollection( MetricConstants.MEAN_ERROR,
                                                     MetricConstants.MEAN_ABSOLUTE_ERROR,
                                                     MetricConstants.ROOT_MEAN_SQUARE_ERROR );

        //Compute them
        final MetricOutputMapByMetric<ScalarOutput> d = collection.apply( input );

        //Print them
        //d.stream().forEach(g -> System.out.println(g.getData()));

        //Check them   
        final Double expectedFirst = -200.55;
        final Double expectedSecond = 201.37;
        final Double expectedThird = 632.4586381732801;
        final Double actualFirst = d.get( MetricConstants.MEAN_ERROR ).getData();
        final Double actualSecond = d.get( MetricConstants.MEAN_ABSOLUTE_ERROR ).getData();
        final Double actualThird = d.get( MetricConstants.ROOT_MEAN_SQUARE_ERROR ).getData();

        final BiPredicate<Double, Double> testMe = FunctionFactory.doubleEquals();

        assertTrue( "Expected value: " + expectedFirst
                    + ". Actual value: "
                    + actualFirst
                    + ".",
                    testMe.test( actualFirst, expectedFirst ) );
        assertTrue( "Expected value: " + expectedSecond
                    + ". Actual value: "
                    + actualSecond
                    + ".",
                    testMe.test( actualSecond, expectedSecond ) );
        assertTrue( "Expected value: " + expectedThird
                    + ". Actual value: "
                    + actualThird
                    + ".",
                    testMe.test( actualThird, expectedThird ) );
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
        final DataFactory outF = DefaultDataFactory.getInstance();
        final MetricFactory metF = MetricFactory.getInstance( outF );
        m.setOutputFactory( outF );
        //Add some appropriate metrics to the collection     
        m.add( metF.ofCriticalSuccessIndex() ); //Should be 0.5734265734265734
        m.add( metF.ofProbabilityOfDetection() ); //Should be 0.780952380952381
        m.add( metF.ofProbabilityOfFalseDetection() ); //Should be 0.14615384615384616
        m.add( metF.ofPeirceSkillScore() ); //Should be 0.6347985347985348
        m.add( metF.ofEquitableThreatScore() ); //Should be 0.43768152544513195

        //Finalize
        final MetricCollection<DichotomousPairs, ScalarOutput> collection = m.build();

        //Compute them
        final MetricOutputMapByMetric<ScalarOutput> c = collection.apply( input );

        //Print them
        //c.stream().forEach(g -> System.out.println(g.getData().doubleValue()));

        //Check them
        final Double expectedFirst = 0.5734265734265734;
        final Double expectedSecond = 0.780952380952381;
        final Double expectedThird = 0.14615384615384616;
        final Double expectedFourth = 0.6347985347985348;
        final Double expectedFifth = 0.43768152544513195;
        final Double actualFirst = c.get( MetricConstants.CRITICAL_SUCCESS_INDEX ).getData();
        final Double actualSecond = c.get( MetricConstants.PROBABILITY_OF_DETECTION ).getData();
        final Double actualThird = c.get( MetricConstants.PROBABILITY_OF_FALSE_DETECTION ).getData();
        final Double actualFourth = c.get( MetricConstants.PEIRCE_SKILL_SCORE ).getData();
        final Double actualFifth = c.get( MetricConstants.EQUITABLE_THREAT_SCORE ).getData();

        final BiPredicate<Double, Double> testMe = FunctionFactory.doubleEquals();

        assertTrue( "Expected value: " + expectedFirst
                    + ". Actual value: "
                    + actualFirst
                    + ".",
                    testMe.test( actualFirst, expectedFirst ) );
        assertTrue( "Expected value: " + expectedSecond
                    + ". Actual value: "
                    + actualSecond
                    + ".",
                    testMe.test( actualSecond, expectedSecond ) );
        assertTrue( "Expected value: " + expectedThird
                    + ". Actual value: "
                    + actualThird
                    + ".",
                    testMe.test( actualThird, expectedThird ) );
        assertTrue( "Expected value: " + expectedFourth
                    + ". Actual value: "
                    + actualFourth
                    + ".",
                    testMe.test( actualFourth, expectedFourth ) );
        assertTrue( "Expected value: " + expectedFifth
                    + ". Actual value: "
                    + actualFifth
                    + ".",
                    testMe.test( actualFifth, expectedFifth ) );
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
        final DataFactory outF = DefaultDataFactory.getInstance();
        final MetricFactory metF = MetricFactory.getInstance( outF );
        n.setOutputFactory( outF );

        //Add some appropriate metrics to the collection
        n.add( metF.ofBrierScore() ); //Should be 0.26
        n.add( metF.ofBrierSkillScore() ); //Should be 0.11363636363636376

        //Finalize
        final MetricCollection<DiscreteProbabilityPairs, VectorOutput> collection = n.build();

        //Compute them
        final MetricOutputMapByMetric<VectorOutput> d = collection.apply( input );

        //Print them
        //d.stream().forEach(g -> System.out.println(((ScalarOutput)g).getData().valueOf()));

        //Check them
        final Double expectedFirst = 0.26;
        final Double expectedSecond = 0.11363636363636376;
        final Double actualFirst = d.get( MetricConstants.BRIER_SCORE, MetricConstants.NONE ).getData().getDoubles()[0];
        final Double actualSecond =
                d.get( MetricConstants.BRIER_SKILL_SCORE, MetricConstants.NONE ).getData().getDoubles()[0];

        final BiPredicate<Double, Double> testMe = FunctionFactory.doubleEquals();

        assertTrue( "Expected value: " + expectedFirst
                    + ". Actual value: "
                    + actualFirst
                    + ".",
                    testMe.test( expectedFirst, actualFirst ) );
        assertTrue( "Expected value: " + expectedSecond
                    + ". Actual value: "
                    + actualSecond
                    + ".",
                    testMe.test( expectedFirst, actualFirst ) );
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
        final DataFactory outF = DefaultDataFactory.getInstance();
        final MetricFactory metF = MetricFactory.getInstance( outF );
        n.setOutputFactory( outF );

        //Add some appropriate metrics to the collection
        n.add( metF.ofMeanSquareError() ); //Should be 400003.929
        n.add( metF.ofMeanSquareErrorSkillScore() ); //Should be 0.8007025335093799

        //Finalize
        final MetricCollection<SingleValuedPairs, VectorOutput> collection = n.build();

        //Compute them
        final MetricOutputMapByMetric<VectorOutput> d = collection.apply( input );

        //Print them
        //d.stream().forEach(g -> System.out.println(((ScalarOutput)g).getData().valueOf()));

        //Check them
        final Double expectedFirst = 400003.929;
        final Double expectedSecond = 0.8007025335093799;
        final Double actualFirst =
                d.get( MetricConstants.MEAN_SQUARE_ERROR, MetricConstants.NONE ).getData().getDoubles()[0];
        final Double actualSecond =
                d.get( MetricConstants.MEAN_SQUARE_ERROR_SKILL_SCORE, MetricConstants.NONE ).getData().getDoubles()[0];

        final BiPredicate<Double, Double> testMe = FunctionFactory.doubleEquals();

        assertTrue( "Expected value: " + expectedFirst
                    + ". Actual value: "
                    + actualFirst
                    + ".",
                    testMe.test( expectedFirst, actualFirst ) );
        assertTrue( "Expected value: " + expectedSecond
                    + ". Actual value: "
                    + actualSecond
                    + ".",
                    testMe.test( expectedFirst, actualFirst ) );
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
        final DataFactory outF = DefaultDataFactory.getInstance();
        final MetricFactory metF = MetricFactory.getInstance( outF );
        n.setOutputFactory( outF );

        //Add some appropriate metrics to the collection
        n.add( metF.ofMulticategoryScalar( MetricConstants.PEIRCE_SKILL_SCORE ) ); //Should be 0.05057466520850963

        //Finalize
        final MetricCollection<MulticategoryPairs, ScalarOutput> collection = n.build();

        //Compute them
        final MetricOutputMapByMetric<ScalarOutput> c = collection.apply( input );

        //Print them
        //c.stream().forEach(g -> System.out.println(g.getData().doubleValue()));

        //Check them
        final Double expectedFirst = 0.05057466520850963;
        final Double actualFirst = c.get( MetricConstants.PEIRCE_SKILL_SCORE ).getData();

        final BiPredicate<Double, Double> testMe = FunctionFactory.doubleEquals();

        assertTrue( "Expected value: " + expectedFirst
                    + ". Actual value: "
                    + actualFirst
                    + ".",
                    testMe.test( actualFirst, expectedFirst ) );
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
            final DataFactory outF = DefaultDataFactory.getInstance();
            final MetricFactory metF = MetricFactory.getInstance( outF );
            n.setOutputFactory( outF );
            //Add some appropriate metrics to the collection
            n.add( metF.ofMeanError() );

            //Wrap an input in a future
            final FutureTask<SingleValuedPairs> futureInput =
                    new FutureTask<SingleValuedPairs>( new Callable<SingleValuedPairs>()
                    {
                        public SingleValuedPairs call()
                        {
                            return input;
                        }
                    } );

            //Add the data
            n.setMetricInput( futureInput );

            //Set an executor
            n.setExecutorService( metricPool );

            //Finalize
            final MetricCollection<SingleValuedPairs, ScalarOutput> collection = n.build();

            //Calling apply should generate an exception
            try
            {
                collection.apply( input );
                fail( "Expected a checked exception on calling apply with a new input." );
            }
            catch ( final Exception e )
            {
            }
            //Try to build with an empty output factory
            try
            {
                final MetricCollectionBuilder<SingleValuedPairs, ScalarOutput> m = MetricCollectionBuilder.of();
                m.build();
                fail( "Expected a checked exception on constructing a metric collection with no output factory." );
            }
            catch ( final Exception e )
            {
            }
            //Try to build with no metrics
            try
            {
                MetricCollectionBuilder.of().setOutputFactory( outF ).build();
                fail( "Expected a checked exception on constructing a metric collection with no metrics." );
            }
            catch ( final Exception e )
            {
            }
            //Try to call with no input
            try
            {
                final MetricCollectionBuilder<SingleValuedPairs, ScalarOutput> m = MetricCollectionBuilder.of();
                m.setOutputFactory( outF ).add( metF.ofMeanError() );
                final MetricCollection<SingleValuedPairs, ScalarOutput> cTest = m.build();
                cTest.call();
                fail( "Expected a checked exception on calling a metric collection without an input." );
            }
            catch ( final Exception e )
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
        final ExecutorService pairPool = Executors.newFixedThreadPool( Runtime.getRuntime().availableProcessors() );
        final ExecutorService metricPool = Executors.newFixedThreadPool( Runtime.getRuntime().availableProcessors() );
        //Construct the factories
        final DataFactory outF = DefaultDataFactory.getInstance();
        final MetricFactory metF = MetricFactory.getInstance( outF );
        try
        {

            //Get the future input
            final FutureTask<DichotomousPairs> futureInput =
                    new FutureTask<DichotomousPairs>( new Callable<DichotomousPairs>()
                    {
                        public DichotomousPairs call()
                        {
                            return MetricTestDataFactory.getDichotomousPairsOne();
                        }
                    } );

            //Create an immutable collection of metrics that take dichotomous pairs and produce a scalar output 
            final MetricCollectionBuilder<DichotomousPairs, ScalarOutput> m = MetricCollectionBuilder.of();
            final MetricCollection<DichotomousPairs, ScalarOutput> collection = m.add( metF.ofCriticalSuccessIndex() )
                                                                                 .add( metF.ofProbabilityOfDetection() )
                                                                                 .add( metF.ofProbabilityOfFalseDetection() )
                                                                                 .add( metF.ofPeirceSkillScore() )
                                                                                 .add( metF.ofEquitableThreatScore() )
                                                                                 .setMetricInput( futureInput )
                                                                                 .setExecutorService( metricPool )
                                                                                 .setOutputFactory( outF )
                                                                                 .build();

            //Compute the pairs
            pairPool.submit( futureInput );

            //Compute the metric
            final MetricOutputMapByMetric<ScalarOutput> d = collection.call();

            //Check them
            final Double expectedFirst = 0.5734265734265734;
            final Double expectedSecond = 0.780952380952381;
            final Double expectedThird = 0.14615384615384616;
            final Double expectedFourth = 0.6347985347985348;
            final Double expectedFifth = 0.43768152544513195;
            final Double actualFirst = d.get( MetricConstants.CRITICAL_SUCCESS_INDEX ).getData();
            final Double actualSecond = d.get( MetricConstants.PROBABILITY_OF_DETECTION ).getData();
            final Double actualThird = d.get( MetricConstants.PROBABILITY_OF_FALSE_DETECTION ).getData();
            final Double actualFourth = d.get( MetricConstants.PEIRCE_SKILL_SCORE ).getData();
            final Double actualFifth = d.get( MetricConstants.EQUITABLE_THREAT_SCORE ).getData();

            final BiPredicate<Double, Double> testMe = FunctionFactory.doubleEquals();

            assertTrue( "Expected value: " + expectedFirst
                        + ". Actual value: "
                        + actualFirst
                        + ".",
                        testMe.test( actualFirst, expectedFirst ) );
            assertTrue( "Expected value: " + expectedSecond
                        + ". Actual value: "
                        + actualSecond
                        + ".",
                        testMe.test( actualSecond, expectedSecond ) );
            assertTrue( "Expected value: " + expectedThird
                        + ". Actual value: "
                        + actualThird
                        + ".",
                        testMe.test( actualThird, expectedThird ) );
            assertTrue( "Expected value: " + expectedFourth
                        + ". Actual value: "
                        + actualFourth
                        + ".",
                        testMe.test( actualFourth, expectedFourth ) );
            assertTrue( "Expected value: " + expectedFifth
                        + ". Actual value: "
                        + actualFifth
                        + ".",
                        testMe.test( actualFifth, expectedFifth ) );
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
            final DataFactory outF = DefaultDataFactory.getInstance();
            final MetricFactory metF = MetricFactory.getInstance( outF );
            n.setOutputFactory( outF );

            //Add some appropriate metrics to the collection
            n.add( metF.ofMeanError() );

            //Wrap an input in a future
            final FutureTask<SingleValuedPairs> futureInput =
                    new FutureTask<SingleValuedPairs>( new Callable<SingleValuedPairs>()
                    {
                        public SingleValuedPairs call()
                        {
                            return input;
                        }
                    } );

            //Set an executor
            n.setExecutorService( metricPool );

            //Compute the pairs
            pairPool.submit( futureInput );

            //Add the data
            n.setMetricInput( futureInput );

            //Finalize
            final MetricCollection<SingleValuedPairs, ScalarOutput> collection = n.build();
            //Compute
            final MetricOutputMapByMetric<ScalarOutput> d = collection.call();
            //Check the results
            //Check them   
            final Double expectedFirst = -200.55;
            final Double actualFirst = d.get( MetricConstants.MEAN_ERROR ).getData();
            final BiPredicate<Double, Double> testMe = FunctionFactory.doubleEquals();
            assertTrue( "Expected value: " + expectedFirst
                        + ". Actual value: "
                        + actualFirst
                        + ".",
                        testMe.test( actualFirst, expectedFirst ) );
        }
        finally
        {
            pairPool.shutdown();
            metricPool.shutdown();
        }
    }

    /**
     * Construct a collection of metrics that consume single-valued pairs and produce scalar outputs. Tests a pair of
     * metrics that implement {@link Collectable}.
     */

    @Test
    public void test9OfSingleValuedScalar()
    {
        //Generate some data
        final SingleValuedPairs input = MetricTestDataFactory.getSingleValuedPairsOne();

        //Create a collection of metrics that consume single-valued pairs and produce a scalar output
        final DataFactory outF = DefaultDataFactory.getInstance();
        final MetricFactory metF = MetricFactory.getInstance( outF );

        //Add some appropriate metrics to the collection
        final MetricCollection<SingleValuedPairs, ScalarOutput> n =
                metF.ofSingleValuedScalarCollection( MetricConstants.CORRELATION_PEARSONS,
                                                     MetricConstants.COEFFICIENT_OF_DETERMINATION );
        //Compute them
        final MetricOutputMapByMetric<ScalarOutput> d = n.apply( input );

        //Print them
        //d.stream().forEach(g -> System.out.println(g.getData()));

        //Check them   
        final Double expectedFirst = 0.9999999910148981;
        final Double expectedSecond = Math.pow( expectedFirst, 2 );
        final Double actualFirst = d.get( MetricConstants.CORRELATION_PEARSONS ).getData();
        final Double actualSecond = d.get( MetricConstants.COEFFICIENT_OF_DETERMINATION ).getData();

        final BiPredicate<Double, Double> testMe = FunctionFactory.doubleEquals();

        assertTrue( "Expected value: " + expectedFirst
                    + ". Actual value: "
                    + actualFirst
                    + ".",
                    testMe.test( actualFirst, expectedFirst ) );
        assertTrue( "Expected value: " + expectedSecond
                    + ". Actual value: "
                    + actualSecond
                    + ".",
                    testMe.test( actualSecond, expectedSecond ) );
    }

}
