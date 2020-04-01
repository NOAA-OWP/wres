package wres.engine.statistics.metric;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
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
import org.slf4j.Logger;

import wres.datamodel.MetricConstants;
import wres.datamodel.Probability;
import wres.datamodel.Slicer;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.statistics.DoubleScoreStatistic;
import wres.datamodel.statistics.Statistic;
import wres.datamodel.statistics.StatisticMetadata;
import wres.engine.statistics.metric.MetricCollection.MetricCollectionBuilder;
import wres.engine.statistics.metric.categorical.EquitableThreatScore;
import wres.engine.statistics.metric.categorical.PeirceSkillScore;
import wres.engine.statistics.metric.categorical.ProbabilityOfDetection;
import wres.engine.statistics.metric.categorical.ProbabilityOfFalseDetection;
import wres.engine.statistics.metric.categorical.ThreatScore;
import wres.engine.statistics.metric.discreteprobability.BrierScore;
import wres.engine.statistics.metric.discreteprobability.BrierSkillScore;
import wres.engine.statistics.metric.singlevalued.MeanError;
import wres.engine.statistics.metric.singlevalued.MeanSquareError;
import wres.engine.statistics.metric.singlevalued.MeanSquareErrorSkillScore;

/**
 * Tests the {@link MetricCollection}.
 * 
 * @author james.brown@hydrosolved.com
 */
public class MetricCollectionTest
{
    private ExecutorService metricPool;

    @Before
    public void setupBeforeEachTest()
    {
        metricPool = Executors.newSingleThreadExecutor();
    }

    /**
     * Construct a collection of metrics that consume single-valued pairs and produce scalar outputs. Compute and check
     * the results.
     * @throws MetricParameterException if the metric construction fails 
     */

    @Test
    public void testOfSingleValuedScalar() throws MetricParameterException
    {
        //Generate some data
        final SampleData<Pair<Double, Double>> input = MetricTestDataFactory.getSingleValuedPairsOne();

        //Finalize
        final MetricCollection<SampleData<Pair<Double, Double>>, DoubleScoreStatistic, DoubleScoreStatistic> collection =
                MetricFactory.ofSingleValuedScoreCollection( ForkJoinPool.commonPool(),
                                                             MetricConstants.MEAN_ERROR,
                                                             MetricConstants.MEAN_ABSOLUTE_ERROR,
                                                             MetricConstants.ROOT_MEAN_SQUARE_ERROR );

        //Compute them
        final List<DoubleScoreStatistic> d = collection.apply( input );

        //Check them   
        final Double expectedFirst = 200.55;
        final Double expectedSecond = 201.37;
        final Double expectedThird = 632.4586381732801;
        final Double actualFirst = Slicer.filter( d, MetricConstants.MEAN_ERROR ).get( 0 ).getData();
        final Double actualSecond =
                Slicer.filter( d, MetricConstants.MEAN_ABSOLUTE_ERROR ).get( 0 ).getData();
        final Double actualThird =
                Slicer.filter( d, MetricConstants.ROOT_MEAN_SQUARE_ERROR ).get( 0 ).getData();

        final BiPredicate<Double, Double> testMe = FunctionFactory.doubleEquals();

        assertTrue( testMe.test( actualFirst, expectedFirst ) );
        assertTrue( testMe.test( actualSecond, expectedSecond ) );
        assertTrue( testMe.test( actualThird, expectedThird ) );
    }

    /**
     * Construct a collection of metrics that consume dichotomous pairs and produce scalar outputs. Compute and check
     * the results.
     * @throws MetricParameterException if the metric construction fails 
     */

    @Test
    public void testOfDichotomousScalar() throws MetricParameterException
    {
        //Generate some data
        final SampleData<Pair<Boolean, Boolean>> input = MetricTestDataFactory.getDichotomousPairsOne();

        //Create a collection of dichotomous metrics that produce a scalar output. Since all scores implement 
        //Collectable, they make efficient use of common intermediate data. In this case, all scores require the 2x2
        //Contingency Table, which is computed only once
        final MetricCollectionBuilder<SampleData<Pair<Boolean, Boolean>>, Statistic<?>, DoubleScoreStatistic> m =
                MetricCollectionBuilder.of();

        m.setExecutorService( ForkJoinPool.commonPool() );

        //Add some appropriate metrics to the collection     
        m.addMetric( ThreatScore.of() ); //Should be 0.5734265734265734
        m.addMetric( ProbabilityOfDetection.of() ); //Should be 0.780952380952381
        m.addMetric( ProbabilityOfFalseDetection.of() ); //Should be 0.14615384615384616
        m.addMetric( PeirceSkillScore.of() ); //Should be 0.6347985347985348
        m.addMetric( EquitableThreatScore.of() ); //Should be 0.43768152544513195

        //Finalize
        final MetricCollection<SampleData<Pair<Boolean, Boolean>>, Statistic<?>, DoubleScoreStatistic> collection =
                m.build();

        //Compute them
        final List<DoubleScoreStatistic> c = collection.apply( input );

        //Check them
        final Double expectedFirst = 0.5734265734265734;
        final Double expectedSecond = 0.780952380952381;
        final Double expectedThird = 0.14615384615384616;
        final Double expectedFourth = 0.6347985347985348;
        final Double expectedFifth = 0.43768152544513195;
        final Double actualFirst = Slicer.filter( c, MetricConstants.THREAT_SCORE ).get( 0 ).getData();
        final Double actualSecond =
                Slicer.filter( c, MetricConstants.PROBABILITY_OF_DETECTION ).get( 0 ).getData();
        final Double actualThird =
                Slicer.filter( c, MetricConstants.PROBABILITY_OF_FALSE_DETECTION ).get( 0 ).getData();
        final Double actualFourth = Slicer.filter( c, MetricConstants.PEIRCE_SKILL_SCORE ).get( 0 ).getData();
        final Double actualFifth =
                Slicer.filter( c, MetricConstants.EQUITABLE_THREAT_SCORE ).get( 0 ).getData();

        final BiPredicate<Double, Double> testMe = FunctionFactory.doubleEquals();

        assertTrue( testMe.test( actualFirst, expectedFirst ) );
        assertTrue( testMe.test( actualSecond, expectedSecond ) );
        assertTrue( testMe.test( actualThird, expectedThird ) );
        assertTrue( testMe.test( actualFourth, expectedFourth ) );
        assertTrue( testMe.test( actualFifth, expectedFifth ) );
    }

    /**
     * Construct a collection of metrics that consume discrete probability pairs and produce vector outputs. Compute and
     * check the results.
     * @throws MetricParameterException if the metric construction fails
     */

    @Test
    public void testOfDiscreteProbabilityVector() throws MetricParameterException
    {
        //Generate some data
        final SampleData<Pair<Probability, Probability>> input = MetricTestDataFactory.getDiscreteProbabilityPairsTwo();

        //Create a collection metrics that consume probabilistic pairs and generate vector outputs
        final MetricCollectionBuilder<SampleData<Pair<Probability, Probability>>, Statistic<?>, DoubleScoreStatistic> n =
                MetricCollectionBuilder.of();

        n.setExecutorService( ForkJoinPool.commonPool() );

        //Add some appropriate metrics to the collection
        n.addMetric( BrierScore.of() ); //Should be 0.26
        n.addMetric( BrierSkillScore.of() ); //Should be 0.11363636363636376

        //Finalize
        final MetricCollection<SampleData<Pair<Probability, Probability>>, Statistic<?>, DoubleScoreStatistic> collection =
                n.build();

        //Compute them
        final List<DoubleScoreStatistic> d = collection.apply( input );

        //Check them
        final Double expectedFirst = 0.26;
        final Double expectedSecond = 0.11363636363636376;
        final Double actualFirst = Slicer.filter( d, MetricConstants.BRIER_SCORE ).get( 0 ).getData();
        final Double actualSecond =
                Slicer.filter( d, MetricConstants.BRIER_SKILL_SCORE ).get( 0 ).getData();

        final BiPredicate<Double, Double> testMe = FunctionFactory.doubleEquals();

        assertTrue( testMe.test( expectedFirst, actualFirst ) );
        assertTrue( testMe.test( expectedSecond, actualSecond ) );
    }

    /**
     * Construct a collection of metrics that consume single-valued pairs and produce vector outputs. Compute and check
     * the results.
     * @throws MetricParameterException if the metric construction fails
     */

    @Test
    public void testOfSingleValuedVector() throws MetricParameterException
    {
        //Generate some data
        final SampleData<Pair<Double, Double>> input = MetricTestDataFactory.getSingleValuedPairsTwo();

        //Create a collection of metrics that consume single-valued pairs and produce vector outputs
        final MetricCollectionBuilder<SampleData<Pair<Double, Double>>, DoubleScoreStatistic, DoubleScoreStatistic> n =
                MetricCollectionBuilder.of();

        n.setExecutorService( ForkJoinPool.commonPool() );

        //Add some appropriate metrics to the collection
        n.addMetric( MeanSquareError.of() ); //Should be 400003.929
        n.addMetric( MeanSquareErrorSkillScore.of() ); //Should be 0.8007025335093799

        //Finalize
        final MetricCollection<SampleData<Pair<Double, Double>>, DoubleScoreStatistic, DoubleScoreStatistic> collection =
                n.build();

        //Compute them
        final List<DoubleScoreStatistic> d = collection.apply( input );

        //Check them
        final Double expectedFirst = 400003.929;
        final Double expectedSecond = 0.8007025335093799;
        final Double actualFirst =
                Slicer.filter( d, MetricConstants.MEAN_SQUARE_ERROR ).get( 0 ).getData();
        final Double actualSecond =
                Slicer.filter( d, MetricConstants.MEAN_SQUARE_ERROR_SKILL_SCORE ).get( 0 ).getData();

        final BiPredicate<Double, Double> testMe = FunctionFactory.doubleEquals();

        assertTrue( testMe.test( expectedFirst, actualFirst ) );
        assertTrue( testMe.test( expectedSecond, actualSecond ) );
    }

    /**
     * Expects a {@link MetricCalculationException} when calling 
     * {@link MetricCollection#apply(wres.datamodel.sampledata.SampleData)} with null input.
     * 
     * @throws MetricCalculationException if the execution fails
     * @throws MetricParameterException if the metric construction fails
     */

    @Test
    public void testApplyWithNullInput() throws MetricParameterException
    {

        //Create a collection of metrics that consume single-valued pairs and produce a scalar output
        final MetricCollectionBuilder<SampleData<Pair<Double, Double>>, Statistic<?>, DoubleScoreStatistic> n =
                MetricCollectionBuilder.of();

        //Add some appropriate metrics to the collection
        n.addMetric( MeanError.of() );

        //Set an executor
        n.setExecutorService( metricPool );

        //Finalize
        final MetricCollection<SampleData<Pair<Double, Double>>, Statistic<?>, DoubleScoreStatistic> collection =
                n.build();

        //Null input
        MetricCalculationException expected =
                assertThrows( MetricCalculationException.class, () -> collection.apply( null ) );
        
        assertEquals( "Specify non-null input to the metric collection.", expected.getMessage() );
    }

    /**
     * Expects a {@link MetricCalculationException} when calling 
     * {@link MetricCollection#apply(wres.datamodel.sampledata.SampleData, Set)} with a null set of metrics to ignore.
     * 
     * @throws MetricCalculationException if the execution fails
     * @throws MetricParameterException if the metric construction fails
     */

    @Test
    public void testApplyWithNullSetToIgnore() throws MetricParameterException
    {

        //Create a collection of metrics that consume single-valued pairs and produce a scalar output
        final MetricCollectionBuilder<SampleData<Pair<Double, Double>>, Statistic<?>, DoubleScoreStatistic> n =
                MetricCollectionBuilder.of();

        final SampleData<Pair<Double, Double>> input = MetricTestDataFactory.getSingleValuedPairsOne();

        final MetricCollection<SampleData<Pair<Double, Double>>, Statistic<?>, DoubleScoreStatistic> collection =
                n.addMetric( MeanError.of() ).setExecutorService( metricPool ).build();

        //Null input
        MetricCalculationException expected =
                assertThrows( MetricCalculationException.class, () -> collection.apply( input, null ) );
        
        assertEquals( "Specify a non-null set of metrics to ignore, such as the empty set.", expected.getMessage() );
    }

    /**
     * Expects a {@link MetricCalculationException} when calling 
     * {@link MetricCollection#apply(wres.datamodel.sampledata.SampleData, Set)} with a set of metrics to ignore that 
     * includes all metrics in the collection.
     * 
     * @throws MetricCalculationException if the execution fails
     * @throws MetricParameterException if the metric construction fails
     */

    @Test
    public void testApplyWithAllMetricsToIgnore() throws MetricParameterException
    {

        //Create a collection of metrics that consume single-valued pairs and produce a scalar output
        final MetricCollectionBuilder<SampleData<Pair<Double, Double>>, Statistic<?>, DoubleScoreStatistic> n =
                MetricCollectionBuilder.of();

        final SampleData<Pair<Double, Double>> input = MetricTestDataFactory.getSingleValuedPairsOne();

        final MetricCollection<SampleData<Pair<Double, Double>>, Statistic<?>, DoubleScoreStatistic> collection =
                n.addMetric( MeanError.of() ).setExecutorService( metricPool ).build();

        //Null input
        MetricCalculationException expected =
                assertThrows( MetricCalculationException.class,
                              () -> collection.apply( input, Collections.singleton( MetricConstants.MEAN_ERROR ) ) );
        
        assertEquals( "Cannot ignore all metrics in the store: specify some metrics to process. The store contains "
                + "[MEAN ERROR] and the ignored metrics are [MEAN ERROR].",
                      expected.getMessage() );
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
        final MetricCollectionBuilder<SampleData<Pair<Double,Double>>, Statistic<?>, DoubleScoreStatistic> m =
                MetricCollectionBuilder.of();

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
        MetricParameterException expected =
                assertThrows( MetricParameterException.class,
                              () -> MetricCollectionBuilder.of().setExecutorService( metricPool ).build() );
        
        assertEquals( "Cannot construct a metric collection without any metrics.", expected.getMessage() );
    }

    /**
     * Tests that logging occurs at the start of a calculation when required.
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
    public void testLogStartOfCalculation() throws MetricParameterException, IllegalAccessException,
            InvocationTargetException, NoSuchMethodException
    {
        MetricCollection<SampleData<Pair<Double, Double>>, DoubleScoreStatistic, DoubleScoreStatistic> collection =
                MetricFactory.ofSingleValuedScoreCollection( MetricConstants.PEARSON_CORRELATION_COEFFICIENT );
        Method logStart = collection.getClass().getDeclaredMethod( "logStartOfCalculation", Logger.class );
        logStart.setAccessible( true );

        Logger logger = Mockito.mock( Logger.class );
        Mockito.when( logger.isTraceEnabled() ).thenReturn( true );

        logStart.invoke( collection, logger );

        Mockito.verify( logger )
               .trace( "Attempting to compute metrics for a collection that contains {} "
                       + "ordinary metric(s) and {} collectable metric(s). The metrics include {}.",
                       0,
                       1,
                       Collections.singleton( MetricConstants.PEARSON_CORRELATION_COEFFICIENT ) );
    }

    /**
     * Tests that logging occurs at the end of a calculation when required.
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
    public void testLogEndOfCalculation() throws MetricParameterException, IllegalAccessException,
            InvocationTargetException, NoSuchMethodException
    {
        MetricCollection<SampleData<Pair<Double, Double>>, DoubleScoreStatistic, DoubleScoreStatistic> collection =
                MetricFactory.ofSingleValuedScoreCollection( MetricConstants.PEARSON_CORRELATION_COEFFICIENT );
        Method logEnd = collection.getClass()
                                  .getDeclaredMethod( "logEndOfCalculation", Logger.class, List.class );
        logEnd.setAccessible( true );

        Logger logger = Mockito.mock( Logger.class );
        Mockito.when( logger.isTraceEnabled() ).thenReturn( true );

        logEnd.invoke( collection, logger, List.of() );

        Mockito.verify( logger )
               .trace( "Finished computing metrics for a collection that contains {} "
                       + "ordinary metric(s) and {} collectable metric(s). Obtained {} result(s) of "
                       + "the {} result(s) expected. Results were obtained for these metrics {}.",
                       0,
                       1,
                       0,
                       1,
                       Collections.emptySet() );
    }

    /**
     * Tests that {@link MetricCollection#apply(SampleData, Set)} throws an expected exception when cancelled.
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
    @Ignore // Until new way of interrupting with mockito is found
    public void testApplyThrowsExceptionWhenInterrupted() throws MetricParameterException, IllegalAccessException,
            InvocationTargetException, NoSuchMethodException
    {
        InvocationTargetException expected =
                assertThrows( InvocationTargetException.class,
                              () -> MetricCollectionBuilder.of().setExecutorService( metricPool ).build() );
        assertEquals( expected.getCause().getClass(), MetricCalculationException.class );

        MetricCollection<SampleData<Pair<Double, Double>>, DoubleScoreStatistic, DoubleScoreStatistic> collection =
                MetricFactory.ofSingleValuedScoreCollection( MetricConstants.MEAN_ERROR );
        Method method = collection.getClass().getDeclaredMethod( "apply", SampleData.class, Set.class );
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
        SampleData<Pair<Double, Double>> input = MetricTestDataFactory.getSingleValuedPairsOne();

        MeanError meanError = Mockito.mock( MeanError.class );
        Mockito.when( meanError.getID() ).thenReturn( MetricConstants.MEAN_ERROR );
        Mockito.when( meanError.apply( input ) ).thenThrow( IllegalArgumentException.class );

        MetricCollectionBuilder<SampleData<Pair<Double, Double>>, Statistic<?>, DoubleScoreStatistic> failed =
                MetricCollectionBuilder.of();

        MetricCalculationException expected = assertThrows( MetricCalculationException.class,
                                                            () -> failed.setExecutorService( metricPool )
                                                                        .addMetric( meanError )
                                                                        .build()
                                                                        .apply( input ) );

        assertEquals( "Computation of the metric collection failed: ", expected.getMessage() );
    }

    /**
     * Construct a collection of metrics that consume single-valued pairs and produce scalar outputs. Tests a pair of
     * metrics that implement {@link Collectable}.
     * @throws MetricParameterException if the metric construction fails 
     */

    @Test
    public void testOfSingleValuedScalarCollectable() throws MetricParameterException
    {
        //Generate some data
        final SampleData<Pair<Double, Double>> input = MetricTestDataFactory.getSingleValuedPairsOne();

        //Create a collection of metrics that consume single-valued pairs and produce a scalar output
        //Add some appropriate metrics to the collection
        final MetricCollection<SampleData<Pair<Double, Double>>, DoubleScoreStatistic, DoubleScoreStatistic> n =
                MetricFactory.ofSingleValuedScoreCollection( ForkJoinPool.commonPool(),
                                                             MetricConstants.PEARSON_CORRELATION_COEFFICIENT,
                                                             MetricConstants.COEFFICIENT_OF_DETERMINATION,
                                                             MetricConstants.SUM_OF_SQUARE_ERROR,
                                                             MetricConstants.MEAN_SQUARE_ERROR,
                                                             MetricConstants.ROOT_MEAN_SQUARE_ERROR );

        //Compute them
        final List<DoubleScoreStatistic> d = n.apply( input );

        //Check them   
        final Double expectedFirst = 0.9999999910148981;
        final Double expectedSecond = Math.pow( expectedFirst, 2 );
        final Double expectedThird = 4000039.29;
        final Double expectedFourth = 400003.929;
        final Double expectedFifth = 632.4586381732801;

        final Double actualFirst =
                Slicer.filter( d, MetricConstants.PEARSON_CORRELATION_COEFFICIENT ).get( 0 ).getData();
        final Double actualSecond =
                Slicer.filter( d, MetricConstants.COEFFICIENT_OF_DETERMINATION ).get( 0 ).getData();
        final Double actualThird = Slicer.filter( d, MetricConstants.SUM_OF_SQUARE_ERROR ).get( 0 ).getData();
        final Double actualFourth = Slicer.filter( d, MetricConstants.MEAN_SQUARE_ERROR ).get( 0 ).getData();
        final Double actualFifth =
                Slicer.filter( d, MetricConstants.ROOT_MEAN_SQUARE_ERROR ).get( 0 ).getData();

        final BiPredicate<Double, Double> testMe = FunctionFactory.doubleEquals();

        assertTrue( testMe.test( actualFirst, expectedFirst ) );

        assertTrue( testMe.test( actualSecond, expectedSecond ) );

        assertTrue( testMe.test( actualThird, expectedThird ) );

        assertTrue( testMe.test( actualFourth, expectedFourth ) );

        assertTrue( testMe.test( actualFifth, expectedFifth ) );

    }

    /**
     * Construct a collection of metrics that consume single-valued pairs and produce scalar outputs. Computes and 
     * benchmarks the output when specifying a non-empty set of metrics to ignore for
     * {@link MetricCollection#apply(wres.datamodel.sampledata.SampleData, Set)}.
     * @throws MetricParameterException if the metric construction fails 
     */

    @Test
    public void testOfSingleValuedScalarWithIgnore() throws MetricParameterException
    {
        //Generate some data
        final SampleData<Pair<Double, Double>> input = MetricTestDataFactory.getSingleValuedPairsOne();

        //Add some appropriate metrics to the collection
        final MetricCollection<SampleData<Pair<Double, Double>>, DoubleScoreStatistic, DoubleScoreStatistic> collection =
                MetricFactory.ofSingleValuedScoreCollection( ForkJoinPool.commonPool(),
                                                             MetricConstants.PEARSON_CORRELATION_COEFFICIENT,
                                                             MetricConstants.MEAN_SQUARE_ERROR,
                                                             MetricConstants.COEFFICIENT_OF_DETERMINATION );
        //Compute them, ignoring two metrics
        Set<MetricConstants> ignore = new HashSet<>( Arrays.asList( MetricConstants.COEFFICIENT_OF_DETERMINATION,
                                                                    MetricConstants.MEAN_SQUARE_ERROR ) );
        final List<DoubleScoreStatistic> actual = collection.apply( input, ignore );
        StatisticMetadata outM =
                StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of() ),
                                      10,
                                      MeasurementUnit.of(),
                                      MetricConstants.PEARSON_CORRELATION_COEFFICIENT,
                                      MetricConstants.MAIN );
        List<DoubleScoreStatistic> expected = Arrays.asList( DoubleScoreStatistic.of( 0.9999999910148981, outM ) );
        
        //Check them   
        assertEquals( expected, actual );
    }

    @After
    public void tearDownAfterEachTest()
    {
        metricPool.shutdownNow();

        // Return the interrupted status of the thread running the test
        Thread.interrupted();
    }

}
