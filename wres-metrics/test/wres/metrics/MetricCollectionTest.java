package wres.metrics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.function.BiPredicate;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import wres.datamodel.Probability;
import wres.datamodel.Slicer;
import wres.datamodel.metrics.MetricConstants;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.datamodel.statistics.Statistic;
import wres.metrics.MetricCollection.Builder;
import wres.metrics.categorical.EquitableThreatScore;
import wres.metrics.categorical.PeirceSkillScore;
import wres.metrics.categorical.ProbabilityOfDetection;
import wres.metrics.categorical.ProbabilityOfFalseDetection;
import wres.metrics.categorical.ThreatScore;
import wres.metrics.discreteprobability.BrierScore;
import wres.metrics.discreteprobability.BrierSkillScore;
import wres.metrics.singlevalued.MeanError;
import wres.metrics.singlevalued.MeanSquareError;
import wres.metrics.singlevalued.MeanSquareErrorSkillScore;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

/**
 * Tests the {@link MetricCollection}.
 * 
 * @author James Brown
 */
public class MetricCollectionTest
{
    private ExecutorService metricPool;

    @Before
    public void setupBeforeEachTest()
    {
        this.metricPool = Executors.newSingleThreadExecutor();
    }

    @Test
    public void testApplyWithSingleValuedPairs() throws MetricParameterException
    {
        //Generate some data
        final Pool<Pair<Double, Double>> input = MetricTestDataFactory.getSingleValuedPairsOne();

        //Finalize
        final MetricCollection<Pool<Pair<Double, Double>>, DoubleScoreStatisticOuter, DoubleScoreStatisticOuter> collection =
                MetricFactory.ofSingleValuedScoreCollection( ForkJoinPool.commonPool(),
                                                             MetricConstants.MEAN_ERROR,
                                                             MetricConstants.MEAN_ABSOLUTE_ERROR,
                                                             MetricConstants.ROOT_MEAN_SQUARE_ERROR );

        //Compute them
        final List<DoubleScoreStatisticOuter> d = collection.apply( input );

        //Check them   
        final Double expectedFirst = 200.55;
        final Double expectedSecond = 201.37;
        final Double expectedThird = 632.4586381732801;
        final Double actualFirst = Slicer.filter( d, MetricConstants.MEAN_ERROR )
                                         .get( 0 )
                                         .getComponent( MetricConstants.MAIN )
                                         .getData()
                                         .getValue();
        final Double actualSecond =
                Slicer.filter( d, MetricConstants.MEAN_ABSOLUTE_ERROR )
                      .get( 0 )
                      .getComponent( MetricConstants.MAIN )
                      .getData()
                      .getValue();
        final Double actualThird =
                Slicer.filter( d, MetricConstants.ROOT_MEAN_SQUARE_ERROR )
                      .get( 0 )
                      .getComponent( MetricConstants.MAIN )
                      .getData()
                      .getValue();

        final BiPredicate<Double, Double> testMe = FunctionFactory.doubleEquals();

        assertTrue( testMe.test( actualFirst, expectedFirst ) );
        assertTrue( testMe.test( actualSecond, expectedSecond ) );
        assertTrue( testMe.test( actualThird, expectedThird ) );
    }

    @Test
    public void testApplyWithSingleValuedPairsTwo() throws MetricParameterException
    {
        //Generate some data
        final Pool<Pair<Double, Double>> input = MetricTestDataFactory.getSingleValuedPairsTwo();

        //Create a collection of metrics that consume single-valued pairs
        final Builder<Pool<Pair<Double, Double>>, DoubleScoreStatisticOuter, DoubleScoreStatisticOuter> n =
                Builder.of();

        n.setExecutorService( ForkJoinPool.commonPool() );

        //Add some appropriate metrics to the collection
        n.addMetric( MeanSquareError.of() ); //Should be 400003.929
        n.addMetric( MeanSquareErrorSkillScore.of() ); //Should be 0.8007025335093799

        //Finalize
        final MetricCollection<Pool<Pair<Double, Double>>, DoubleScoreStatisticOuter, DoubleScoreStatisticOuter> collection =
                n.build();

        //Compute them
        final List<DoubleScoreStatisticOuter> d = collection.apply( input );

        //Check them
        final Double expectedFirst = 400003.929;
        final Double expectedSecond = 0.8007025335093799;
        final Double actualFirst =
                Slicer.filter( d, MetricConstants.MEAN_SQUARE_ERROR )
                      .get( 0 )
                      .getComponent( MetricConstants.MAIN )
                      .getData()
                      .getValue();
        final Double actualSecond =
                Slicer.filter( d, MetricConstants.MEAN_SQUARE_ERROR_SKILL_SCORE )
                      .get( 0 )
                      .getComponent( MetricConstants.MAIN )
                      .getData()
                      .getValue();

        final BiPredicate<Double, Double> testMe = FunctionFactory.doubleEquals();

        assertTrue( testMe.test( expectedFirst, actualFirst ) );
        assertTrue( testMe.test( expectedSecond, actualSecond ) );
    }

    @Test
    public void testApplyWithDichotomousPairs() throws MetricParameterException
    {
        //Generate some data
        final Pool<Pair<Boolean, Boolean>> input = MetricTestDataFactory.getDichotomousPairsOne();

        //Create a collection of dichotomous metrics that produce a scalar output. Since all scores implement 
        //Collectable, they make efficient use of common intermediate data. In this case, all scores require the 2x2
        //Contingency Table, which is computed only once
        final Builder<Pool<Pair<Boolean, Boolean>>, Statistic<?>, DoubleScoreStatisticOuter> m =
                Builder.of();

        m.setExecutorService( ForkJoinPool.commonPool() );

        //Add some appropriate metrics to the collection     
        m.addMetric( ThreatScore.of() ); //Should be 0.5734265734265734
        m.addMetric( ProbabilityOfDetection.of() ); //Should be 0.780952380952381
        m.addMetric( ProbabilityOfFalseDetection.of() ); //Should be 0.14615384615384616
        m.addMetric( PeirceSkillScore.of() ); //Should be 0.6347985347985348
        m.addMetric( EquitableThreatScore.of() ); //Should be 0.43768152544513195

        //Finalize
        final MetricCollection<Pool<Pair<Boolean, Boolean>>, Statistic<?>, DoubleScoreStatisticOuter> collection =
                m.build();

        //Compute them
        final List<DoubleScoreStatisticOuter> c = collection.apply( input );

        //Check them
        final Double expectedFirst = 0.5734265734265734;
        final Double expectedSecond = 0.780952380952381;
        final Double expectedThird = 0.14615384615384616;
        final Double expectedFourth = 0.6347985347985348;
        final Double expectedFifth = 0.43768152544513195;
        final Double actualFirst = Slicer.filter( c, MetricConstants.THREAT_SCORE )
                                         .get( 0 )
                                         .getComponent( MetricConstants.MAIN )
                                         .getData()
                                         .getValue();
        final Double actualSecond =
                Slicer.filter( c, MetricConstants.PROBABILITY_OF_DETECTION )
                      .get( 0 )
                      .getComponent( MetricConstants.MAIN )
                      .getData()
                      .getValue();
        final Double actualThird =
                Slicer.filter( c, MetricConstants.PROBABILITY_OF_FALSE_DETECTION )
                      .get( 0 )
                      .getComponent( MetricConstants.MAIN )
                      .getData()
                      .getValue();
        final Double actualFourth = Slicer.filter( c, MetricConstants.PEIRCE_SKILL_SCORE )
                                          .get( 0 )
                                          .getComponent( MetricConstants.MAIN )
                                          .getData()
                                          .getValue();
        final Double actualFifth =
                Slicer.filter( c, MetricConstants.EQUITABLE_THREAT_SCORE )
                      .get( 0 )
                      .getComponent( MetricConstants.MAIN )
                      .getData()
                      .getValue();

        final BiPredicate<Double, Double> testMe = FunctionFactory.doubleEquals();

        assertTrue( testMe.test( actualFirst, expectedFirst ) );
        assertTrue( testMe.test( actualSecond, expectedSecond ) );
        assertTrue( testMe.test( actualThird, expectedThird ) );
        assertTrue( testMe.test( actualFourth, expectedFourth ) );
        assertTrue( testMe.test( actualFifth, expectedFifth ) );
    }

    @Test
    public void testApplyWithDiscreteProbabilityPairs() throws MetricParameterException
    {
        //Generate some data
        final Pool<Pair<Probability, Probability>> input = MetricTestDataFactory.getDiscreteProbabilityPairsTwo();

        //Create a collection metrics that consume probabilistic pairs and generate vector outputs
        final Builder<Pool<Pair<Probability, Probability>>, Statistic<?>, DoubleScoreStatisticOuter> n =
                Builder.of();

        n.setExecutorService( ForkJoinPool.commonPool() );

        //Add some appropriate metrics to the collection
        n.addMetric( BrierScore.of() ); //Should be 0.26
        n.addMetric( BrierSkillScore.of() ); //Should be 0.11363636363636376

        //Finalize
        final MetricCollection<Pool<Pair<Probability, Probability>>, Statistic<?>, DoubleScoreStatisticOuter> collection =
                n.build();

        //Compute them
        final List<DoubleScoreStatisticOuter> d = collection.apply( input );

        //Check them
        final Double expectedFirst = 0.26;
        final Double expectedSecond = 0.11363636363636376;
        final Double actualFirst = Slicer.filter( d, MetricConstants.BRIER_SCORE )
                                         .get( 0 )
                                         .getComponent( MetricConstants.MAIN )
                                         .getData()
                                         .getValue();
        final Double actualSecond =
                Slicer.filter( d, MetricConstants.BRIER_SKILL_SCORE )
                      .get( 0 )
                      .getComponent( MetricConstants.MAIN )
                      .getData()
                      .getValue();

        final BiPredicate<Double, Double> testMe = FunctionFactory.doubleEquals();

        assertTrue( testMe.test( expectedFirst, actualFirst ) );
        assertTrue( testMe.test( expectedSecond, actualSecond ) );
    }

    @Test
    public void testApplyWithSubsetOfMetrics() throws MetricParameterException
    {
        Pool<Pair<Double, Double>> input = MetricTestDataFactory.getSingleValuedPairsOne();

        MetricCollection<Pool<Pair<Double, Double>>, DoubleScoreStatisticOuter, DoubleScoreStatisticOuter> collection =
                MetricFactory.ofSingleValuedScoreCollection( ForkJoinPool.commonPool(),
                                                             MetricConstants.MEAN_ERROR,
                                                             MetricConstants.SAMPLE_SIZE );

        // Compute the subset
        List<DoubleScoreStatisticOuter> actualList = collection.apply( input, Set.of( MetricConstants.SAMPLE_SIZE ) );

        assertEquals( 1, actualList.size() );

        DoubleScoreStatisticOuter actual = actualList.get( 0 );

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setMetric( SampleSize.MAIN )
                                                                               .setValue( 10 )
                                                                               .build();

        DoubleScoreStatistic score = DoubleScoreStatistic.newBuilder()
                                                         .setMetric( SampleSize.BASIC_METRIC )
                                                         .addStatistics( component )
                                                         .build();

        DoubleScoreStatisticOuter expected = DoubleScoreStatisticOuter.of( score, PoolMetadata.of() );

        assertEquals( expected, actual );
    }

    /**
     * Expects a {@link MetricCalculationException} when calling 
     * {@link MetricCollection#apply(wres.datamodel.pools.Pool)} with null input.
     * 
     * @throws MetricCalculationException if the execution fails
     * @throws MetricParameterException if the metric construction fails
     */

    @Test
    public void testApplyWithNullInput() throws MetricParameterException
    {

        //Create a collection of metrics that consume single-valued pairs and produce a scalar output
        final Builder<Pool<Pair<Double, Double>>, Statistic<?>, DoubleScoreStatisticOuter> n =
                Builder.of();

        //Add some appropriate metrics to the collection
        n.addMetric( MeanError.of() );

        //Set an executor
        n.setExecutorService( metricPool );

        //Finalize
        final MetricCollection<Pool<Pair<Double, Double>>, Statistic<?>, DoubleScoreStatisticOuter> collection =
                n.build();

        //Null input
        NullPointerException expected =
                assertThrows( NullPointerException.class, () -> collection.apply( null ) );

        assertEquals( "Specify non-null input to the metric collection.", expected.getMessage() );
    }

    /**
     * Expects a {@link MetricParameterException} when building a {@link MetricCollection} without an 
     * {@link ExecutorService}.
     * 
     * @throws MetricParameterException if the metric construction fails
     */

    @Test
    public void testBuildWithNoExecutorService() throws MetricParameterException
    {
        //No output factory            
        final Builder<Pool<Pair<Double, Double>>, Statistic<?>, DoubleScoreStatisticOuter> m =
                Builder.of();

        MetricParameterException expected =
                assertThrows( MetricParameterException.class,
                              () -> m.build() );

        assertEquals( "Cannot construct the metric collection without an executor service.", expected.getMessage() );
    }

    /**
     * Expects a {@link MetricParameterException} when building a {@link MetricCollection} without any metrics.
     * 
     * @throws MetricParameterException if the metric construction fails
     */

    @Test
    public void testBuildWithNoMetrics() throws MetricParameterException
    {
        Builder<Pool<Pair<Double, Double>>, Statistic<?>, DoubleScoreStatisticOuter> collection =
                new Builder<Pool<Pair<Double, Double>>, Statistic<?>, DoubleScoreStatisticOuter>().setExecutorService( this.metricPool );

        MetricParameterException expected =
                assertThrows( MetricParameterException.class,
                              () -> collection.build() );

        assertEquals( "Cannot construct a metric collection without any metrics.", expected.getMessage() );
    }

    /**
     * Tests that {@link MetricCollection#apply(Pool, Set)} throws an expected exception when cancelled.
     * 
     * @throws MetricCalculationException if the execution fails
     * @throws MetricParameterException if the metric construction fails
     * @throws InvocationTargetException if the underlying method throws an exception
     * @throws IllegalArgumentException if the method inputs are unexpected
     * @throws IllegalAccessException if the method is inaccessible
     * @throws SecurityException if the request is denied
     * @throws NoSuchMethodException if the method does not exist
     */

    @Test
    @Ignore( "Until new way of interrupting with mockito is found." )
    public void testApplyThrowsExceptionWhenInterrupted() throws MetricParameterException, IllegalAccessException,
            InvocationTargetException, NoSuchMethodException
    {
        InvocationTargetException expected =
                assertThrows( InvocationTargetException.class,
                              () -> Builder.of().setExecutorService( metricPool ).build() );
        assertEquals( expected.getCause().getClass(), MetricCalculationException.class );

        MetricCollection<Pool<Pair<Double, Double>>, DoubleScoreStatisticOuter, DoubleScoreStatisticOuter> collection =
                MetricFactory.ofSingleValuedScoreCollection( MetricConstants.MEAN_ERROR );
        Method method = collection.getClass().getDeclaredMethod( "apply", Pool.class, Set.class );
        method.setAccessible( true );

        Set<?> ignore = Mockito.mock( Set.class );
        Mockito.when( ignore.size() ).thenThrow( InterruptedException.class );

        method.invoke( collection, MetricTestDataFactory.getSingleValuedPairsOne(), ignore );
    }

    /**
     * Expects a {@link MetricCalculationException} from a metric within a {@link MetricCollection}.
     * 
     * @throws MetricCalculationException if the execution fails
     * @throws MetricParameterException if the metric construction fails
     */

    @Test
    public void testExceptionOnFailedExecution() throws MetricParameterException
    {

        //Generate some data
        Pool<Pair<Double, Double>> input = MetricTestDataFactory.getSingleValuedPairsOne();

        MeanError meanError = Mockito.mock( MeanError.class );
        Mockito.when( meanError.getMetricName() ).thenReturn( MetricConstants.MEAN_ERROR );
        Mockito.when( meanError.apply( input ) ).thenThrow( IllegalArgumentException.class );

        MetricCollection<Pool<Pair<Double, Double>>, Statistic<?>, DoubleScoreStatisticOuter> collection =
                new Builder<Pool<Pair<Double, Double>>, Statistic<?>, DoubleScoreStatisticOuter>().setExecutorService( metricPool )
                                                                                                  .addMetric( meanError )
                                                                                                  .build();

        MetricCalculationException expected = assertThrows( MetricCalculationException.class,
                                                            () -> collection.apply( input ) );

        assertEquals( "Computation of the metric collection failed: ", expected.getMessage() );
    }

    @Test
    public void testOfSingleValuedPairsWithCollectableMetrics() throws MetricParameterException
    {
        //Generate some data
        final Pool<Pair<Double, Double>> input = MetricTestDataFactory.getSingleValuedPairsOne();

        //Create a collection of metrics that consume single-valued pairs and produce a scalar output
        //Add some appropriate metrics to the collection
        final MetricCollection<Pool<Pair<Double, Double>>, DoubleScoreStatisticOuter, DoubleScoreStatisticOuter> n =
                MetricFactory.ofSingleValuedScoreCollection( ForkJoinPool.commonPool(),
                                                             MetricConstants.PEARSON_CORRELATION_COEFFICIENT,
                                                             MetricConstants.COEFFICIENT_OF_DETERMINATION,
                                                             MetricConstants.SUM_OF_SQUARE_ERROR,
                                                             MetricConstants.MEAN_SQUARE_ERROR,
                                                             MetricConstants.ROOT_MEAN_SQUARE_ERROR );

        //Compute them
        final List<DoubleScoreStatisticOuter> d = n.apply( input );

        //Check them   
        final Double expectedFirst = 0.9999999910148981;
        final Double expectedSecond = Math.pow( expectedFirst, 2 );
        final Double expectedThird = 4000039.29;
        final Double expectedFourth = 400003.929;
        final Double expectedFifth = 632.4586381732801;

        final Double actualFirst =
                Slicer.filter( d, MetricConstants.PEARSON_CORRELATION_COEFFICIENT )
                      .get( 0 )
                      .getComponent( MetricConstants.MAIN )
                      .getData()
                      .getValue();
        final Double actualSecond =
                Slicer.filter( d, MetricConstants.COEFFICIENT_OF_DETERMINATION )
                      .get( 0 )
                      .getComponent( MetricConstants.MAIN )
                      .getData()
                      .getValue();
        final Double actualThird = Slicer.filter( d, MetricConstants.SUM_OF_SQUARE_ERROR )
                                         .get( 0 )
                                         .getComponent( MetricConstants.MAIN )
                                         .getData()
                                         .getValue();
        final Double actualFourth = Slicer.filter( d, MetricConstants.MEAN_SQUARE_ERROR )
                                          .get( 0 )
                                          .getComponent( MetricConstants.MAIN )
                                          .getData()
                                          .getValue();
        final Double actualFifth =
                Slicer.filter( d, MetricConstants.ROOT_MEAN_SQUARE_ERROR )
                      .get( 0 )
                      .getComponent( MetricConstants.MAIN )
                      .getData()
                      .getValue();

        final BiPredicate<Double, Double> testMe = FunctionFactory.doubleEquals();

        assertTrue( testMe.test( actualFirst, expectedFirst ) );

        assertTrue( testMe.test( actualSecond, expectedSecond ) );

        assertTrue( testMe.test( actualThird, expectedThird ) );

        assertTrue( testMe.test( actualFourth, expectedFourth ) );

        assertTrue( testMe.test( actualFifth, expectedFifth ) );

    }

    @After
    public void tearDownAfterEachTest()
    {
        this.metricPool.shutdownNow();

        // Return the interrupted status of the thread running the test
        Thread.interrupted();
    }

}
