package wres.engine.statistics.metric;

import static org.junit.Assert.assertTrue;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.function.BiPredicate;

import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;
import org.slf4j.Logger;

import wres.datamodel.DataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.inputs.MetricInput;
import wres.datamodel.inputs.pairs.DichotomousPairs;
import wres.datamodel.inputs.pairs.DiscreteProbabilityPairs;
import wres.datamodel.inputs.pairs.MulticategoryPairs;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.metadata.MeasurementUnit;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.datamodel.outputs.MetricOutput;
import wres.datamodel.outputs.MetricOutputMapByMetric;
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

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    private ExecutorService metricPool;

    @Before
    public void setupBeforeEachTest() throws MetricParameterException
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
        final SingleValuedPairs input = MetricTestDataFactory.getSingleValuedPairsOne();

        //Finalize
        final MetricCollection<SingleValuedPairs, DoubleScoreOutput, DoubleScoreOutput> collection =
                MetricFactory.ofSingleValuedScoreCollection( ForkJoinPool.commonPool(),
                                                             MetricConstants.MEAN_ERROR,
                                                             MetricConstants.MEAN_ABSOLUTE_ERROR,
                                                             MetricConstants.ROOT_MEAN_SQUARE_ERROR );

        //Compute them
        final MetricOutputMapByMetric<DoubleScoreOutput> d = collection.apply( input );

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
    public void testOfDichotomousScalar() throws MetricParameterException
    {
        //Generate some data
        final DichotomousPairs input = MetricTestDataFactory.getDichotomousPairsOne();

        //Create a collection of dichotomous metrics that produce a scalar output. Since all scores implement 
        //Collectable, they make efficient use of common intermediate data. In this case, all scores require the 2x2
        //Contingency Table, which is computed only once
        final MetricCollectionBuilder<DichotomousPairs, MetricOutput<?>, DoubleScoreOutput> m =
                MetricCollectionBuilder.of();

        m.setExecutorService( ForkJoinPool.commonPool() );

        //Add some appropriate metrics to the collection     
        m.addMetric( ThreatScore.of() ); //Should be 0.5734265734265734
        m.addMetric( ProbabilityOfDetection.of() ); //Should be 0.780952380952381
        m.addMetric( ProbabilityOfFalseDetection.of() ); //Should be 0.14615384615384616
        m.addMetric( PeirceSkillScore.of() ); //Should be 0.6347985347985348
        m.addMetric( EquitableThreatScore.of() ); //Should be 0.43768152544513195

        //Finalize
        final MetricCollection<DichotomousPairs, MetricOutput<?>, DoubleScoreOutput> collection = m.build();

        //Compute them
        final MetricOutputMapByMetric<DoubleScoreOutput> c = collection.apply( input );

        //Print them
        //c.stream().forEach(g -> System.out.println(g.getData().doubleValue()));

        //Check them
        final Double expectedFirst = 0.5734265734265734;
        final Double expectedSecond = 0.780952380952381;
        final Double expectedThird = 0.14615384615384616;
        final Double expectedFourth = 0.6347985347985348;
        final Double expectedFifth = 0.43768152544513195;
        final Double actualFirst = c.get( MetricConstants.THREAT_SCORE ).getData();
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
    public void testOfDiscreteProbabilityVector() throws MetricParameterException
    {
        //Generate some data
        final DiscreteProbabilityPairs input = MetricTestDataFactory.getDiscreteProbabilityPairsTwo();

        //Create a collection metrics that consume probabilistic pairs and generate vector outputs
        final MetricCollectionBuilder<DiscreteProbabilityPairs, MetricOutput<?>, DoubleScoreOutput> n =
                MetricCollectionBuilder.of();

        n.setExecutorService( ForkJoinPool.commonPool() );

        //Add some appropriate metrics to the collection
        n.addMetric( BrierScore.of() ); //Should be 0.26
        n.addMetric( BrierSkillScore.of() ); //Should be 0.11363636363636376

        //Finalize
        final MetricCollection<DiscreteProbabilityPairs, MetricOutput<?>, DoubleScoreOutput> collection =
                n.build();

        //Compute them
        final MetricOutputMapByMetric<DoubleScoreOutput> d = collection.apply( input );

        //Print them
        //d.stream().forEach(g -> System.out.println(((ScalarOutput)g).getData().valueOf()));

        //Check them
        final Double expectedFirst = 0.26;
        final Double expectedSecond = 0.11363636363636376;
        final Double actualFirst = d.get( MetricConstants.BRIER_SCORE ).getData();
        final Double actualSecond =
                d.get( MetricConstants.BRIER_SKILL_SCORE ).getData();

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
    public void testOfSingleValuedVector() throws MetricParameterException
    {
        //Generate some data
        final SingleValuedPairs input = MetricTestDataFactory.getSingleValuedPairsTwo();

        //Create a collection of metrics that consume single-valued pairs and produce vector outputs
        final MetricCollectionBuilder<SingleValuedPairs, DoubleScoreOutput, DoubleScoreOutput> n =
                MetricCollectionBuilder.of();

        n.setExecutorService( ForkJoinPool.commonPool() );

        //Add some appropriate metrics to the collection
        n.addMetric( MeanSquareError.of() ); //Should be 400003.929
        n.addMetric( MeanSquareErrorSkillScore.of() ); //Should be 0.8007025335093799

        //Finalize
        final MetricCollection<SingleValuedPairs, DoubleScoreOutput, DoubleScoreOutput> collection = n.build();

        //Compute them
        final MetricOutputMapByMetric<DoubleScoreOutput> d = collection.apply( input );

        //Print them
        //d.stream().forEach(g -> System.out.println(((ScalarOutput)g).getData().valueOf()));

        //Check them
        final Double expectedFirst = 400003.929;
        final Double expectedSecond = 0.8007025335093799;
        final Double actualFirst =
                d.get( MetricConstants.MEAN_SQUARE_ERROR ).getData();
        final Double actualSecond =
                d.get( MetricConstants.MEAN_SQUARE_ERROR_SKILL_SCORE ).getData();

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
    public void testOfMulticategoryScalar() throws MetricParameterException
    {
        //Generate some data
        final MulticategoryPairs input = MetricTestDataFactory.getMulticategoryPairsOne();

        //Create a collection of multicategory metrics that produce a scalar output. 
        final MetricCollectionBuilder<MulticategoryPairs, MetricOutput<?>, DoubleScoreOutput> n =
                MetricCollectionBuilder.of();

        n.setExecutorService( ForkJoinPool.commonPool() );

        //Add some appropriate metrics to the collection
        n.addMetric( MetricFactory.ofMulticategoryScore( MetricConstants.PEIRCE_SKILL_SCORE ) ); //Should be 0.05057466520850963

        //Finalize
        final MetricCollection<MulticategoryPairs, MetricOutput<?>, DoubleScoreOutput> collection = n.build();

        //Compute them
        final MetricOutputMapByMetric<DoubleScoreOutput> c = collection.apply( input );

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
     * Expects a {@link MetricCalculationException} when calling 
     * {@link MetricCollection#apply(wres.datamodel.inputs.MetricInput)} with null input.
     * 
     * @throws MetricCalculationException if the execution fails
     * @throws MetricParameterException if the metric construction fails
     */

    @Test
    public void testApplyWithNullInput() throws MetricParameterException
    {

        //Create a collection of metrics that consume single-valued pairs and produce a scalar output
        final MetricCollectionBuilder<SingleValuedPairs, MetricOutput<?>, DoubleScoreOutput> n =
                MetricCollectionBuilder.of();

        //Add some appropriate metrics to the collection
        n.addMetric( MeanError.of() );

        //Set an executor
        n.setExecutorService( metricPool );

        //Finalize
        final MetricCollection<SingleValuedPairs, MetricOutput<?>, DoubleScoreOutput> collection = n.build();

        //Null input
        exception.expect( MetricCalculationException.class );
        exception.expectMessage( "Specify non-null input to the metric collection." );

        collection.apply( null );
    }

    /**
     * Expects a {@link MetricCalculationException} when calling 
     * {@link MetricCollection#apply(wres.datamodel.inputs.MetricInput, Set)} with a null set of metrics to ignore.
     * 
     * @throws MetricCalculationException if the execution fails
     * @throws MetricParameterException if the metric construction fails
     */

    @Test
    public void testApplyWithNullSetToIgnore() throws MetricParameterException
    {

        //Create a collection of metrics that consume single-valued pairs and produce a scalar output
        final MetricCollectionBuilder<SingleValuedPairs, MetricOutput<?>, DoubleScoreOutput> n =
                MetricCollectionBuilder.of();

        final SingleValuedPairs input = MetricTestDataFactory.getSingleValuedPairsOne();

        final MetricCollection<SingleValuedPairs, MetricOutput<?>, DoubleScoreOutput> collection =
                n.addMetric( MeanError.of() ).setExecutorService( metricPool ).build();

        //Null input
        exception.expect( MetricCalculationException.class );
        exception.expectMessage( "Specify a non-null set of metrics to ignore, such as the empty set." );

        collection.apply( input, null );
    }

    /**
     * Expects a {@link MetricCalculationException} when calling 
     * {@link MetricCollection#apply(wres.datamodel.inputs.MetricInput, Set)} with a set of metrics to ignore that 
     * includes all metrics in the collection.
     * 
     * @throws MetricCalculationException if the execution fails
     * @throws MetricParameterException if the metric construction fails
     */

    @Test
    public void testApplyWithAllMetricsToIgnore() throws MetricParameterException
    {

        //Create a collection of metrics that consume single-valued pairs and produce a scalar output
        final MetricCollectionBuilder<SingleValuedPairs, MetricOutput<?>, DoubleScoreOutput> n =
                MetricCollectionBuilder.of();

        final SingleValuedPairs input = MetricTestDataFactory.getSingleValuedPairsOne();

        final MetricCollection<SingleValuedPairs, MetricOutput<?>, DoubleScoreOutput> collection =
                n.addMetric( MeanError.of() ).setExecutorService( metricPool ).build();

        //Null input
        exception.expect( MetricCalculationException.class );
        exception.expectMessage( "Cannot ignore all metrics in the store: specify some metrics to process." );

        collection.apply( input, Collections.singleton( MetricConstants.MEAN_ERROR ) );
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
        exception.expect( MetricParameterException.class );
        exception.expectMessage( "Cannot construct the metric collection without an executor service." );

        //No output factory            
        final MetricCollectionBuilder<SingleValuedPairs, MetricOutput<?>, DoubleScoreOutput> m =
                MetricCollectionBuilder.of();
        m.build();
    }

    /**
     * Expects a {@link MetricParameterException} when building a {@link MetricCollection} without any metrics.
     * 
     * @throws MetricParameterException if the metric construction fails
     */

    @Test
    public void testBuildWithNoMetrics() throws MetricParameterException
    {
        exception.expect( MetricParameterException.class );
        exception.expectMessage( "Cannot construct a metric collection without any metrics." );

        //Try to build with no metrics
        MetricCollectionBuilder.of().setExecutorService( metricPool ).build();
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
            IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException
    {
        MetricCollection<SingleValuedPairs, DoubleScoreOutput, DoubleScoreOutput> collection =
                MetricFactory.ofSingleValuedScoreCollection( MetricConstants.PEARSON_CORRELATION_COEFFICIENT );
        Method logStart = collection.getClass().getDeclaredMethod( "logStartOfCalculation", Logger.class );
        logStart.setAccessible( true );

        Logger logger = Mockito.mock( Logger.class );
        Mockito.when( logger.isTraceEnabled() ).thenReturn( true );

        logStart.invoke( collection, logger );

        Mockito.verify( logger ).trace( "Attempting to compute metrics for a collection that contains {} "
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
            IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException
    {
        MetricCollection<SingleValuedPairs, DoubleScoreOutput, DoubleScoreOutput> collection =
                MetricFactory.ofSingleValuedScoreCollection( MetricConstants.PEARSON_CORRELATION_COEFFICIENT );
        Method logEnd = collection.getClass().getDeclaredMethod( "logEndOfCalculation", Logger.class, Map.class );
        logEnd.setAccessible( true );

        Logger logger = Mockito.mock( Logger.class );
        Mockito.when( logger.isTraceEnabled() ).thenReturn( true );

        logEnd.invoke( collection, logger, Collections.emptyMap() );

        Mockito.verify( logger ).trace( "Finished computing metrics for a collection that contains {} "
                                        + "ordinary metric(s) and {} collectable metric(s). Obtained {} result(s) of "
                                        + "the {} result(s) expected. Results were obtained for these metrics {}.",
                                        0,
                                        1,
                                        0,
                                        1,
                                        Collections.emptySet() );
    }

    /**
     * Tests that {@link MetricCollection#apply(MetricInput, Set)} throws an expected exception when cancelled.
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
    public void testApplyThrowsExceptionWhenInterrupted() throws MetricParameterException, IllegalAccessException,
            IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException
    {
        exception.expect( InvocationTargetException.class );
        exception.expectCause( CoreMatchers.isA( MetricCalculationException.class ) );

        MetricCollection<SingleValuedPairs, DoubleScoreOutput, DoubleScoreOutput> collection =
                MetricFactory.ofSingleValuedScoreCollection( MetricConstants.MEAN_ERROR );
        Method method = collection.getClass().getDeclaredMethod( "apply", MetricInput.class, Set.class );
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
        SingleValuedPairs input = MetricTestDataFactory.getSingleValuedPairsOne();

        MeanError meanError = Mockito.mock( MeanError.class );
        Mockito.when( meanError.getID() ).thenReturn( MetricConstants.MEAN_ERROR );
        Mockito.when( meanError.apply( input ) ).thenThrow( IllegalArgumentException.class );

        exception.expect( MetricCalculationException.class );
        exception.expectMessage( "Computation of the metric collection failed: " );

        MetricCollectionBuilder<SingleValuedPairs, MetricOutput<?>, DoubleScoreOutput> failed =
                MetricCollectionBuilder.of();
        failed.setExecutorService( metricPool )
              .addMetric( meanError )
              .build()
              .apply( input );
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
        final SingleValuedPairs input = MetricTestDataFactory.getSingleValuedPairsOne();

        //Create a collection of metrics that consume single-valued pairs and produce a scalar output
        //Add some appropriate metrics to the collection
        final MetricCollection<SingleValuedPairs, DoubleScoreOutput, DoubleScoreOutput> n =
                MetricFactory.ofSingleValuedScoreCollection( ForkJoinPool.commonPool(),
                                                             MetricConstants.PEARSON_CORRELATION_COEFFICIENT,
                                                             MetricConstants.COEFFICIENT_OF_DETERMINATION,
                                                             MetricConstants.SUM_OF_SQUARE_ERROR,
                                                             MetricConstants.MEAN_SQUARE_ERROR,
                                                             MetricConstants.ROOT_MEAN_SQUARE_ERROR );

        //Compute them
        final MetricOutputMapByMetric<DoubleScoreOutput> d = n.apply( input );

        //Print them
        //d.forEach( ( a, b ) -> System.out.println( a.getKey() + " " + b.getData() ) );

        //Check them   
        final Double expectedFirst = 0.9999999910148981;
        final Double expectedSecond = Math.pow( expectedFirst, 2 );
        final Double expectedThird = 4000039.29;
        final Double expectedFourth = 400003.929;
        final Double expectedFifth = 632.4586381732801;

        final Double actualFirst = d.get( MetricConstants.PEARSON_CORRELATION_COEFFICIENT ).getData();
        final Double actualSecond = d.get( MetricConstants.COEFFICIENT_OF_DETERMINATION ).getData();
        final Double actualThird = d.get( MetricConstants.SUM_OF_SQUARE_ERROR ).getData();
        final Double actualFourth = d.get( MetricConstants.MEAN_SQUARE_ERROR ).getData();
        final Double actualFifth = d.get( MetricConstants.ROOT_MEAN_SQUARE_ERROR ).getData();

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
     * Construct a collection of metrics that consume single-valued pairs and produce scalar outputs. Computes and 
     * benchmarks the output when specifying a non-empty set of metrics to ignore for
     * {@link MetricCollection#apply(wres.datamodel.inputs.MetricInput, Set)}.
     * @throws MetricParameterException if the metric construction fails 
     */

    @Test
    public void testOfSingleValuedScalarWithIgnore() throws MetricParameterException
    {
        //Generate some data
        final SingleValuedPairs input = MetricTestDataFactory.getSingleValuedPairsOne();

        //Add some appropriate metrics to the collection
        final MetricCollection<SingleValuedPairs, DoubleScoreOutput, DoubleScoreOutput> collection =
                MetricFactory.ofSingleValuedScoreCollection( ForkJoinPool.commonPool(),
                                                             MetricConstants.PEARSON_CORRELATION_COEFFICIENT,
                                                             MetricConstants.MEAN_SQUARE_ERROR,
                                                             MetricConstants.COEFFICIENT_OF_DETERMINATION );
        //Compute them, ignoring two metrics
        Set<MetricConstants> ignore = new HashSet<>( Arrays.asList( MetricConstants.COEFFICIENT_OF_DETERMINATION,
                                                                    MetricConstants.MEAN_SQUARE_ERROR ) );
        final MetricOutputMapByMetric<DoubleScoreOutput> actual = collection.apply( input, ignore );
        MetricOutputMetadata outM =
                MetricOutputMetadata.of( 10,
                                                   MeasurementUnit.of(),
                                                   MeasurementUnit.of(),
                                                   MetricConstants.PEARSON_CORRELATION_COEFFICIENT );
        MetricOutputMapByMetric<DoubleScoreOutput> expected =
                DataFactory.ofMetricOutputMapByMetric( Collections.singletonMap( MetricConstants.PEARSON_CORRELATION_COEFFICIENT,
                                                                                 DoubleScoreOutput.of( 0.9999999910148981, outM ) ) );
        //Check them   
        assertTrue( "Difference between the actual and expected output when ignoring some metrics in the "
                    + "collection.",
                    actual.equals( expected ) );
    }

    @After
    public void tearDownAfterEachTest()
    {
        metricPool.shutdownNow();

        // Return the interrupted status of the thread running the test
        Thread.interrupted();
    }

}
