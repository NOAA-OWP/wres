package wres.engine.statistics.metric;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.FutureTask;
import java.util.function.BiPredicate;

import org.junit.Test;

import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.inputs.pairs.DichotomousPairs;
import wres.datamodel.inputs.pairs.DiscreteProbabilityPairs;
import wres.datamodel.inputs.pairs.MulticategoryPairs;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.outputs.MetricOutput;
import wres.datamodel.outputs.MetricOutputMapByMetric;
import wres.datamodel.outputs.MultiValuedScoreOutput;
import wres.datamodel.outputs.ScalarOutput;
import wres.engine.statistics.metric.MetricCollection.MetricCollectionBuilder;
import wres.engine.statistics.metric.singlevalued.DoubleErrorScore;

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
     * @throws MetricParameterException if the metric construction fails 
     */

    @Test
    public void test1OfSingleValuedScalar() throws MetricParameterException
    {
        //Generate some data
        final SingleValuedPairs input = MetricTestDataFactory.getSingleValuedPairsOne();

        //Create a collection of metrics that consume single-valued pairs and produce a scalar output
        final DataFactory outF = DefaultDataFactory.getInstance();
        final MetricFactory metF = MetricFactory.getInstance( outF );

        //Finalize
        final MetricCollection<SingleValuedPairs, ScalarOutput, ScalarOutput> collection =
                metF.ofSingleValuedScalarCollection( ForkJoinPool.commonPool(),
                                                     MetricConstants.MEAN_ERROR,
                                                     MetricConstants.MEAN_ABSOLUTE_ERROR,
                                                     MetricConstants.ROOT_MEAN_SQUARE_ERROR );

        //Compute them
        final MetricOutputMapByMetric<ScalarOutput> d = collection.apply( input );

        //Print them
        //d.stream().forEach(g -> System.out.println(g.getData()));

        //Check them   
        final Double expectedFirst = 200.55;
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
     * @throws MetricParameterException if the metric construction fails 
     */

    @Test
    public void test2OfDichotomousScalar() throws MetricParameterException
    {
        //Generate some data
        final DichotomousPairs input = MetricTestDataFactory.getDichotomousPairsOne();

        //Create a collection of dichotomous metrics that produce a scalar output. Since all scores implement 
        //Collectable, they make efficient use of common intermediate data. In this case, all scores require the 2x2
        //Contingency Table, which is computed only once
        final MetricCollectionBuilder<DichotomousPairs, MetricOutput<?>, ScalarOutput> m = MetricCollectionBuilder.of();
        final DataFactory outF = DefaultDataFactory.getInstance();
        final MetricFactory metF = MetricFactory.getInstance( outF );
        m.setOutputFactory( outF );
        m.setExecutorService( ForkJoinPool.commonPool() );
        //Add some appropriate metrics to the collection     
        m.add( metF.ofCriticalSuccessIndex() ); //Should be 0.5734265734265734
        m.add( metF.ofProbabilityOfDetection() ); //Should be 0.780952380952381
        m.add( metF.ofProbabilityOfFalseDetection() ); //Should be 0.14615384615384616
        m.add( metF.ofPeirceSkillScore() ); //Should be 0.6347985347985348
        m.add( metF.ofEquitableThreatScore() ); //Should be 0.43768152544513195

        //Finalize
        final MetricCollection<DichotomousPairs, MetricOutput<?>, ScalarOutput> collection = m.build();

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
     * @throws MetricParameterException if the metric construction fails
     */

    @Test
    public void test3OfDiscreteProbabilityVector() throws MetricParameterException
    {
        //Generate some data
        final DiscreteProbabilityPairs input = MetricTestDataFactory.getDiscreteProbabilityPairsTwo();

        //Create a collection metrics that consume probabilistic pairs and generate vector outputs
        final MetricCollectionBuilder<DiscreteProbabilityPairs, MetricOutput<?>, MultiValuedScoreOutput> n =
                MetricCollectionBuilder.of();
        final DataFactory outF = DefaultDataFactory.getInstance();
        final MetricFactory metF = MetricFactory.getInstance( outF );
        n.setOutputFactory( outF );
        n.setExecutorService( ForkJoinPool.commonPool() );

        //Add some appropriate metrics to the collection
        n.add( metF.ofBrierScore() ); //Should be 0.26
        n.add( metF.ofBrierSkillScore() ); //Should be 0.11363636363636376

        //Finalize
        final MetricCollection<DiscreteProbabilityPairs, MetricOutput<?>, MultiValuedScoreOutput> collection =
                n.build();

        //Compute them
        final MetricOutputMapByMetric<MultiValuedScoreOutput> d = collection.apply( input );

        //Print them
        //d.stream().forEach(g -> System.out.println(((ScalarOutput)g).getData().valueOf()));

        //Check them
        final Double expectedFirst = 0.26;
        final Double expectedSecond = 0.11363636363636376;
        final Double actualFirst = d.get( MetricConstants.BRIER_SCORE ).getData().getDoubles()[0];
        final Double actualSecond =
                d.get( MetricConstants.BRIER_SKILL_SCORE ).getData().getDoubles()[0];

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
     * @throws MetricParameterException if the metric construction fails
     */

    @Test
    public void test4OfSingleValuedVector() throws MetricParameterException
    {
        //Generate some data
        final SingleValuedPairs input = MetricTestDataFactory.getSingleValuedPairsTwo();

        //Create a collection metrics that consume single-valued pairs and produce vector outputs
        final MetricCollectionBuilder<SingleValuedPairs, MetricOutput<?>, MultiValuedScoreOutput> n =
                MetricCollectionBuilder.of();
        final DataFactory outF = DefaultDataFactory.getInstance();
        final MetricFactory metF = MetricFactory.getInstance( outF );
        n.setOutputFactory( outF );
        n.setExecutorService( ForkJoinPool.commonPool() );

        //Add some appropriate metrics to the collection
        n.add( metF.ofMeanSquareError() ); //Should be 400003.929
        n.add( metF.ofMeanSquareErrorSkillScore() ); //Should be 0.8007025335093799

        //Finalize
        final MetricCollection<SingleValuedPairs, MetricOutput<?>, MultiValuedScoreOutput> collection = n.build();

        //Compute them
        final MetricOutputMapByMetric<MultiValuedScoreOutput> d = collection.apply( input );

        //Print them
        //d.stream().forEach(g -> System.out.println(((ScalarOutput)g).getData().valueOf()));

        //Check them
        final Double expectedFirst = 400003.929;
        final Double expectedSecond = 0.8007025335093799;
        final Double actualFirst =
                d.get( MetricConstants.MEAN_SQUARE_ERROR ).getData().getDoubles()[0];
        final Double actualSecond =
                d.get( MetricConstants.MEAN_SQUARE_ERROR_SKILL_SCORE ).getData().getDoubles()[0];

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
     * @throws MetricParameterException if the metric construction fails 
     */

    @Test
    public void test5OfMulticategoryScalar() throws MetricParameterException
    {
        //Generate some data
        final MulticategoryPairs input = MetricTestDataFactory.getMulticategoryPairsOne();

        //Create a collection of multicategory metrics that produce a scalar output. 
        final MetricCollectionBuilder<MulticategoryPairs, MetricOutput<?>, ScalarOutput> n =
                MetricCollectionBuilder.of();
        final DataFactory outF = DefaultDataFactory.getInstance();
        final MetricFactory metF = MetricFactory.getInstance( outF );
        n.setOutputFactory( outF );
        n.setExecutorService( ForkJoinPool.commonPool() );

        //Add some appropriate metrics to the collection
        n.add( metF.ofMulticategoryScalar( MetricConstants.PEIRCE_SKILL_SCORE ) ); //Should be 0.05057466520850963

        //Finalize
        final MetricCollection<MulticategoryPairs, MetricOutput<?>, ScalarOutput> collection = n.build();

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
     * @throws ExecutionException if the execution fails
     * @throws InterruptedException if the calculation is interrupted
     * @throws MetricCalculationException if the metric calculation fails
     * @throws MetricParameterException if the metric construction fails
     */

    @Test
    public void test6ExceptionTests()
            throws MetricCalculationException, InterruptedException, ExecutionException, MetricParameterException
    {

        final ExecutorService metricPool = Executors.newSingleThreadExecutor();

        try
        {

            //Generate some data
            final SingleValuedPairs input = MetricTestDataFactory.getSingleValuedPairsOne();

            //Create a collection of metrics that consume single-valued pairs and produce a scalar output
            final MetricCollectionBuilder<SingleValuedPairs, MetricOutput<?>, ScalarOutput> n =
                    MetricCollectionBuilder.of();
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
            final MetricCollection<SingleValuedPairs, MetricOutput<?>, ScalarOutput> collection = n.build();

            //Calling apply should generate an exception
            try
            {
                collection.apply( input );
                fail( "Expected a checked exception on calling apply with a new input." );
            }
            catch ( final Exception e )
            {
            }
            //Null input
            try
            {
                collection.apply( null );
                fail( "Expected a checked exception on calling apply with a null input." );
            }
            catch ( final Exception e )
            {
            }
            //Try to build with an empty output factory
            try
            {
                final MetricCollectionBuilder<SingleValuedPairs, MetricOutput<?>, ScalarOutput> m =
                        MetricCollectionBuilder.of();
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
            //Check for interruptions and failed executions
            final MetricCollectionBuilder<SingleValuedPairs, MetricOutput<?>, ScalarOutput> m =
                    MetricCollectionBuilder.of();
            m.setOutputFactory( outF );
            m.add( new MeanErrorException() );
            try
            {
                m.build().apply( input );
                fail( "Expected an unchecked exception on calling apply." );
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
     * Construct a collection of metrics that consume single-valued pairs and produce scalar outputs. Tests a pair of
     * metrics that implement {@link Collectable}.
     * @throws MetricParameterException if the metric construction fails 
     */

    @Test
    public void test9OfSingleValuedScalar() throws MetricParameterException
    {
        //Generate some data
        final SingleValuedPairs input = MetricTestDataFactory.getSingleValuedPairsOne();

        //Create a collection of metrics that consume single-valued pairs and produce a scalar output
        final DataFactory outF = DefaultDataFactory.getInstance();
        final MetricFactory metF = MetricFactory.getInstance( outF );

        //Add some appropriate metrics to the collection
        final MetricCollection<SingleValuedPairs, ScalarOutput, ScalarOutput> n =
                metF.ofSingleValuedScalarCollection( ForkJoinPool.commonPool(),
                                                     MetricConstants.PEARSON_CORRELATION_COEFFICIENT,
                                                     MetricConstants.COEFFICIENT_OF_DETERMINATION );
        //Compute them
        final MetricOutputMapByMetric<ScalarOutput> d = n.apply( input );

        //Print them
        //d.forEach( ( a, b ) -> System.out.println( a.getKey() + " " + b.getData() ) );

        //Check them   
        final Double expectedFirst = 0.9999999910148981;
        final Double expectedSecond = Math.pow( expectedFirst, 2 );
        final Double actualFirst = d.get( MetricConstants.PEARSON_CORRELATION_COEFFICIENT ).getData();
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

    /**
     * Class for testing runtime exceptions.
     */

    private static class MeanErrorException extends DoubleErrorScore<SingleValuedPairs>
    {
        private MeanErrorException() throws MetricParameterException
        {
            super( (MeanErrorExceptionBuilder) new MeanErrorExceptionBuilder().setOutputFactory( DefaultDataFactory.getInstance() ) );
        }

        @Override
        public ScalarOutput apply( SingleValuedPairs input )
        {
            throw new IllegalArgumentException();
        }

        private static class MeanErrorExceptionBuilder extends DoubleErrorScoreBuilder<SingleValuedPairs>
        {
            @Override
            public MeanErrorException build() throws MetricParameterException
            {
                return new MeanErrorException();
            }
        }

        @Override
        public boolean isSkillScore()
        {
            return false;
        }

        @Override
        public MetricConstants getID()
        {
            return MetricConstants.MEAN_ERROR;
        }

        @Override
        public boolean hasRealUnits()
        {
            return false;
        }
    }

}
