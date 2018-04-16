package wres.engine.statistics.metric;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.ForkJoinPool;

import org.hamcrest.CoreMatchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.config.ProjectConfigPlus;
import wres.config.generated.ProjectConfig;
import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MetricConstants;
import wres.engine.statistics.metric.categorical.ContingencyTable;
import wres.engine.statistics.metric.categorical.EquitableThreatScore;
import wres.engine.statistics.metric.categorical.PeirceSkillScore;
import wres.engine.statistics.metric.categorical.ProbabilityOfDetection;
import wres.engine.statistics.metric.categorical.ProbabilityOfFalseDetection;
import wres.engine.statistics.metric.categorical.ThreatScore;
import wres.engine.statistics.metric.discreteprobability.BrierScore;
import wres.engine.statistics.metric.discreteprobability.BrierSkillScore;
import wres.engine.statistics.metric.discreteprobability.RelativeOperatingCharacteristicDiagram;
import wres.engine.statistics.metric.discreteprobability.RelativeOperatingCharacteristicScore;
import wres.engine.statistics.metric.discreteprobability.ReliabilityDiagram;
import wres.engine.statistics.metric.ensemble.BoxPlotErrorByForecast;
import wres.engine.statistics.metric.ensemble.BoxPlotErrorByObserved;
import wres.engine.statistics.metric.ensemble.ContinuousRankedProbabilityScore;
import wres.engine.statistics.metric.ensemble.ContinuousRankedProbabilitySkillScore;
import wres.engine.statistics.metric.ensemble.RankHistogram;
import wres.engine.statistics.metric.processing.MetricProcessorByTimeEnsemblePairs;
import wres.engine.statistics.metric.processing.MetricProcessorByTimeSingleValuedPairs;
import wres.engine.statistics.metric.processing.MetricProcessorException;
import wres.engine.statistics.metric.processing.MetricProcessorForProject;
import wres.engine.statistics.metric.singlevalued.BiasFraction;
import wres.engine.statistics.metric.singlevalued.CoefficientOfDetermination;
import wres.engine.statistics.metric.singlevalued.CorrelationPearsons;
import wres.engine.statistics.metric.singlevalued.IndexOfAgreement;
import wres.engine.statistics.metric.singlevalued.KlingGuptaEfficiency;
import wres.engine.statistics.metric.singlevalued.MeanAbsoluteError;
import wres.engine.statistics.metric.singlevalued.MeanError;
import wres.engine.statistics.metric.singlevalued.MeanSquareError;
import wres.engine.statistics.metric.singlevalued.MeanSquareErrorSkillScore;
import wres.engine.statistics.metric.singlevalued.QuantileQuantileDiagram;
import wres.engine.statistics.metric.singlevalued.RootMeanSquareError;
import wres.engine.statistics.metric.timeseries.TimeToPeakError;
import wres.engine.statistics.metric.timeseries.TimeToPeakRelativeError;
import wres.engine.statistics.metric.timeseries.TimingErrorDurationStatistics;

/**
 * Tests the {@link MetricFactory}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class MetricFactoryTest
{

    /**
     * Output factory.
     */

    private DataFactory outF;

    /**
     * Metric factory.
     */

    private MetricFactory metF;

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Before
    public void setupBeforeEachTest()
    {
        outF = DefaultDataFactory.getInstance();
        metF = MetricFactory.getInstance( outF );
    }

    /**
     * Tests {@link MetricFactory#ofSingleValuedScore(MetricConstants)}. 
     * @throws MetricParameterException if the metric construction fails
     */
    @Test
    public void testOfSingleValuedScore() throws MetricParameterException
    {
        assertTrue( metF.ofSingleValuedScore( MetricConstants.BIAS_FRACTION ) instanceof BiasFraction );
        assertTrue( metF.ofSingleValuedScore( MetricConstants.MEAN_ABSOLUTE_ERROR ) instanceof MeanAbsoluteError );
        assertTrue( metF.ofSingleValuedScore( MetricConstants.MEAN_ERROR ) instanceof MeanError );
        assertTrue( metF.ofSingleValuedScore( MetricConstants.ROOT_MEAN_SQUARE_ERROR ) instanceof RootMeanSquareError );
        assertTrue( metF.ofSingleValuedScore( MetricConstants.PEARSON_CORRELATION_COEFFICIENT ) instanceof CorrelationPearsons );
        assertTrue( metF.ofSingleValuedScore( MetricConstants.COEFFICIENT_OF_DETERMINATION ) instanceof CoefficientOfDetermination );
        assertTrue( metF.ofSingleValuedScore( MetricConstants.INDEX_OF_AGREEMENT ) instanceof IndexOfAgreement );
        assertTrue( metF.ofSingleValuedScore( MetricConstants.SAMPLE_SIZE ) instanceof SampleSize );
        assertTrue( metF.ofSingleValuedScore( MetricConstants.MEAN_SQUARE_ERROR ) instanceof MeanSquareError );
        assertTrue( metF.ofSingleValuedScore( MetricConstants.MEAN_SQUARE_ERROR_SKILL_SCORE ) instanceof MeanSquareErrorSkillScore );
        assertTrue( metF.ofSingleValuedScore( MetricConstants.KLING_GUPTA_EFFICIENCY ) instanceof KlingGuptaEfficiency );

        // Unrecognized metric
        exception.expect( IllegalArgumentException.class );
        exception.expectMessage( "Unrecognized metric for identifier. 'MAIN'." );
        metF.ofSingleValuedScore( MetricConstants.MAIN );
    }

    /**
     * Tests {@link MetricFactory#ofDiscreteProbabilityScore(MetricConstants)} 
     * @throws MetricParameterException if the metric construction fails 
     */
    @Test
    public void testOfDiscreteProbabilityScore() throws MetricParameterException
    {
        assertTrue( metF.ofDiscreteProbabilityScore( MetricConstants.BRIER_SCORE ) instanceof BrierScore );
        assertTrue( metF.ofDiscreteProbabilityScore( MetricConstants.BRIER_SKILL_SCORE ) instanceof BrierSkillScore );
        assertTrue( metF.ofDiscreteProbabilityScore( MetricConstants.RELATIVE_OPERATING_CHARACTERISTIC_SCORE ) instanceof RelativeOperatingCharacteristicScore );

        // Unrecognized metric
        exception.expect( IllegalArgumentException.class );
        exception.expectMessage( "Unrecognized metric for identifier. 'MAIN'." );
        metF.ofDiscreteProbabilityScore( MetricConstants.MAIN );
    }

    /**
     * Tests {@link MetricFactory#ofDichotomousScore(MetricConstants)}. 
     * @throws MetricParameterException if the metric construction fails 
     */
    @Test
    public void testOfDichotomousScore() throws MetricParameterException
    {
        assertTrue( metF.ofDichotomousScore( MetricConstants.THREAT_SCORE ) instanceof ThreatScore );
        assertTrue( metF.ofDichotomousScore( MetricConstants.EQUITABLE_THREAT_SCORE ) instanceof EquitableThreatScore );
        assertTrue( metF.ofDichotomousScore( MetricConstants.PEIRCE_SKILL_SCORE ) instanceof PeirceSkillScore );
        assertTrue( metF.ofDichotomousScore( MetricConstants.PROBABILITY_OF_DETECTION ) instanceof ProbabilityOfDetection );
        assertTrue( metF.ofDichotomousScore( MetricConstants.PROBABILITY_OF_FALSE_DETECTION ) instanceof ProbabilityOfFalseDetection );

        // Unrecognized metric
        exception.expect( IllegalArgumentException.class );
        exception.expectMessage( "Unrecognized metric for identifier. 'MAIN'." );
        metF.ofDichotomousScore( MetricConstants.MAIN );
    }

    /**
     * Tests {@link MetricFactory#ofMulticategoryScore(MetricConstants)}. 
     * @throws MetricParameterException if the metric construction fails 
     */
    @Test
    public void testOfMulticategoryScore() throws MetricParameterException
    {
        assertTrue( metF.ofMulticategoryScore( MetricConstants.PEIRCE_SKILL_SCORE ) instanceof PeirceSkillScore );

        // Unrecognized metric
        exception.expect( IllegalArgumentException.class );
        exception.expectMessage( "Unrecognized metric for identifier. 'MAIN'." );
        metF.ofMulticategoryScore( MetricConstants.MAIN );
    }

    /**
     * Tests {@link MetricFactory#ofEnsembleScore(MetricConstants)}. 
     * @throws MetricParameterException if the metric construction fails
     */
    @Test
    public void testOfEnsembleScore() throws MetricParameterException
    {
        assertTrue( metF.ofEnsembleScore( MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SCORE ) instanceof ContinuousRankedProbabilityScore );
        assertTrue( metF.ofEnsembleScore( MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE ) instanceof ContinuousRankedProbabilitySkillScore );
        assertTrue( metF.ofEnsembleScore( MetricConstants.SAMPLE_SIZE ) instanceof SampleSize );

        // Unrecognized metric
        exception.expect( IllegalArgumentException.class );
        exception.expectMessage( "Unrecognized metric for identifier. 'MAIN'." );
        metF.ofEnsembleScore( MetricConstants.MAIN );
    }

    /**
     * Tests {@link MetricFactory#ofDichotomousMatrix(MetricConstants)}. 
     * @throws MetricParameterException if the metric construction fails 
     */
    @Test
    public void testOfMulticategoryMatrix() throws MetricParameterException
    {
        assertTrue( metF.ofDichotomousMatrix( MetricConstants.CONTINGENCY_TABLE ) instanceof ContingencyTable );

        // Unrecognized metric
        exception.expect( IllegalArgumentException.class );
        exception.expectMessage( "Unrecognized metric for identifier. 'MAIN'." );
        metF.ofDichotomousMatrix( MetricConstants.MAIN );
    }

    /**
     * Tests {@link MetricFactory#ofSingleValuedMultiVector(MetricConstants)}. 
     * @throws MetricParameterException if the metric construction fails 
     */
    @Test
    public void testOfSingleValuedMultiVector() throws MetricParameterException
    {
        assertTrue( metF.ofSingleValuedMultiVector( MetricConstants.QUANTILE_QUANTILE_DIAGRAM ) instanceof QuantileQuantileDiagram );

        // Unrecognized metric
        exception.expect( IllegalArgumentException.class );
        exception.expectMessage( "Unrecognized metric for identifier. 'MAIN'." );
        metF.ofSingleValuedMultiVector( MetricConstants.MAIN );
    }

    /**
     * Tests {@link MetricFactory#ofSingleValuedMultiVector(MetricConstants)}. 
     * @throws MetricParameterException if the metric construction fails 
     */
    @Test
    public void testOfDiscreteProbabilityMultiVector() throws MetricParameterException
    {
        assertTrue( metF.ofDiscreteProbabilityMultiVector( MetricConstants.RELIABILITY_DIAGRAM ) instanceof ReliabilityDiagram );
        assertTrue( metF.ofDiscreteProbabilityMultiVector( MetricConstants.RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM ) instanceof RelativeOperatingCharacteristicDiagram );

        // Unrecognized metric
        exception.expect( IllegalArgumentException.class );
        exception.expectMessage( "Unrecognized metric for identifier. 'MAIN'." );
        metF.ofDiscreteProbabilityMultiVector( MetricConstants.MAIN );
    }

    /**
     * Tests {@link MetricFactory#ofEnsembleMultiVector(MetricConstants)}. 
     * @throws MetricParameterException if the metric construction fails 
     */
    @Test
    public void testOfEnsembleMultiVector() throws MetricParameterException
    {
        assertTrue( metF.ofEnsembleMultiVector( MetricConstants.RANK_HISTOGRAM ) instanceof RankHistogram );

        // Unrecognized metric
        exception.expect( IllegalArgumentException.class );
        exception.expectMessage( "Unrecognized metric for identifier. 'MAIN'." );
        metF.ofEnsembleMultiVector( MetricConstants.MAIN );
    }

    /**
     * Tests {@link MetricFactory#ofEnsembleBoxPlot(MetricConstants)}. 
     * @throws MetricParameterException if the metric construction fails 
     */
    @Test
    public void testOfEnsembleBoxPlot() throws MetricParameterException
    {
        assertTrue( metF.ofEnsembleBoxPlot( MetricConstants.BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE ) instanceof BoxPlotErrorByObserved );
        assertTrue( metF.ofEnsembleBoxPlot( MetricConstants.BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE ) instanceof BoxPlotErrorByForecast );

        // Unrecognized metric
        exception.expect( IllegalArgumentException.class );
        exception.expectMessage( "Unrecognized metric for identifier. 'MAIN'." );
        metF.ofEnsembleBoxPlot( MetricConstants.MAIN );
    }

    /**
     * Tests {@link MetricFactory#ofSingleValuedTimeSeries(MetricConstants)}. 
     * @throws MetricParameterException if the metric construction fails 
     */
    @Test
    public void testSingleValuedTimeSeries() throws MetricParameterException
    {
        assertTrue( metF.ofSingleValuedTimeSeries( MetricConstants.TIME_TO_PEAK_ERROR ) instanceof TimeToPeakError );
        assertTrue( metF.ofSingleValuedTimeSeries( MetricConstants.TIME_TO_PEAK_RELATIVE_ERROR ) instanceof TimeToPeakRelativeError );

        // Unrecognized metric
        exception.expect( IllegalArgumentException.class );
        exception.expectMessage( "Unrecognized metric for identifier. 'MAIN'." );
        metF.ofSingleValuedTimeSeries( MetricConstants.MAIN );
    }

    /**
     * Tests {@link MetricFactory#ofSingleValuedScoreCollection(MetricConstants...)}. 
     * @throws MetricParameterException if the metric construction fails
     */
    @Test
    public void testOfSingleValuedScoreCollection() throws MetricParameterException
    {
        assertTrue( Objects.nonNull( metF.ofSingleValuedScoreCollection( MetricConstants.MEAN_ABSOLUTE_ERROR ) ) );
    }

    /**
     * Tests {@link MetricFactory#ofDiscreteProbabilityScoreCollection(MetricConstants...)}. 
     * @throws MetricParameterException if the metric construction fails 
     */
    @Test
    public void testOfDiscreteProbabilityVectorCollection() throws MetricParameterException
    {
        assertTrue( Objects.nonNull( metF.ofDiscreteProbabilityScoreCollection( MetricConstants.BRIER_SCORE ) ) );
    }

    /**
     * Tests {@link MetricFactory#ofDichotomousScoreCollection(MetricConstants...)}. 
     * @throws MetricParameterException if the metric construction fails 
     */
    @Test
    public void testOfDichotomousScoreCollection() throws MetricParameterException
    {
        assertTrue( Objects.nonNull( metF.ofDichotomousScoreCollection( MetricConstants.THREAT_SCORE ) ) );
        assertTrue( Objects.nonNull( metF.ofDichotomousScoreCollection( MetricConstants.FREQUENCY_BIAS ) ) );
    }

    /**
     * Tests {@link MetricFactory#ofSingleValuedMultiVectorCollection(MetricConstants...)}. 
     * @throws MetricParameterException if the metric construction fails 
     */
    @Test
    public void testOfSingleValuedMultiVectorCollection() throws MetricParameterException
    {
        assertTrue( Objects.nonNull( metF.ofSingleValuedMultiVectorCollection( MetricConstants.QUANTILE_QUANTILE_DIAGRAM ) ) );
    }

    /**
     * Tests {@link MetricFactory#ofDiscreteProbabilityMultiVectorCollection(MetricConstants...)}. 
     * @throws MetricParameterException if the metric collection could not be constructed
     */
    @Test
    public void testOfDiscreteProbabilityMultiVectorCollection() throws MetricParameterException
    {
        assertTrue( Objects.nonNull( metF.ofDiscreteProbabilityMultiVectorCollection( MetricConstants.RELIABILITY_DIAGRAM ) ) );
        assertTrue( Objects.nonNull( metF.ofDiscreteProbabilityMultiVectorCollection( MetricConstants.RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM ) ) );
    }

    /**
     * Tests {@link MetricFactory#ofDichotomousMatrixCollection(MetricConstants...)}. 
     * @throws MetricParameterException if the metric construction fails 
     */
    @Test
    public void testOfMulticategoryMatrixCollection() throws MetricParameterException
    {
        assertTrue( Objects.nonNull( metF.ofDichotomousMatrixCollection( MetricConstants.CONTINGENCY_TABLE ) ) );
    }

    /**
     * Tests {@link MetricFactory#ofEnsembleScoreCollection(MetricConstants...)}. 
     * @throws MetricParameterException if the metric construction fails 
     */
    @Test
    public void testOfEnsembleScoreCollection() throws MetricParameterException
    {
        assertTrue( Objects.nonNull( metF.ofEnsembleScoreCollection( MetricConstants.SAMPLE_SIZE ) ) );
        assertTrue( Objects.nonNull( metF.ofEnsembleScoreCollection( MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SCORE ) ) );
        assertTrue( Objects.nonNull( metF.ofEnsembleScoreCollection( MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE ) ) );
    }

    /**
     * Tests {@link MetricFactory#ofEnsembleMultiVectorCollection(MetricConstants...)}. 
     * @throws MetricParameterException if the metric construction fails 
     */
    @Test
    public void testOfEnsembleMultiVectorCollection() throws MetricParameterException
    {
        assertTrue( Objects.nonNull( metF.ofEnsembleMultiVectorCollection( MetricConstants.RANK_HISTOGRAM ) ) );
    }

    /**
     * Tests {@link MetricFactory#ofEnsembleBoxPlotCollection(MetricConstants...)}. 
     * @throws MetricParameterException if the metric construction fails 
     */
    @Test
    public void testOfEnsembleBoxPlotCollection() throws MetricParameterException
    {
        assertTrue( Objects.nonNull( metF.ofEnsembleBoxPlotCollection( MetricConstants.BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE ) ) );
        assertTrue( Objects.nonNull( metF.ofEnsembleBoxPlotCollection( MetricConstants.BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE ) ) );
    }

    /**
     * Tests {@link MetricFactory#ofSingleValuedTimeSeriesCollection(MetricConstants...)}. 
     * @throws MetricParameterException if the metric construction fails 
     */
    @Test
    public void testOfSingleValuedTimeSeriesCollection() throws MetricParameterException
    {
        assertTrue( Objects.nonNull( metF.ofSingleValuedTimeSeriesCollection( MetricConstants.TIME_TO_PEAK_ERROR ) ) );
        assertTrue( Objects.nonNull( metF.ofSingleValuedTimeSeriesCollection( MetricConstants.TIME_TO_PEAK_RELATIVE_ERROR ) ) );
    }

    /**
     * Tests for exceptions in {@link MetricFactory}.
     * 
     * @throws SecurityException if reflection fails
     * @throws NoSuchMethodException if reflection fails
     * @throws InstantiationException if reflection fails
     * @throws IllegalAccessException if reflection fails
     * @throws InvocationTargetException if reflection fails
     */

    @Test
    public void testExceptions() throws InstantiationException,
            IllegalAccessException,
            InvocationTargetException,
            NoSuchMethodException,
            SecurityException
    {
        Constructor<MetricFactory> cons = MetricFactory.class.getDeclaredConstructor( DataFactory.class );
        cons.setAccessible( true );

        exception.expectCause( CoreMatchers.instanceOf( IllegalArgumentException.class ) );
        cons.newInstance( (DataFactory) null );
    }

    /**
     * Tests {@link MetricFactory#ofSummaryStatisticsForTimingErrorMetric(MetricConstants)}. 
     * @throws MetricParameterException if the metric construction fails 
     */
    @Test
    public void testOfSummaryStatisticsForTimingErrorMetric() throws MetricParameterException
    {
        assertTrue( Objects.nonNull( MetricFactory.ofSummaryStatisticsForTimingErrorMetric( MetricConstants.TIME_TO_PEAK_ERROR ) ) );
        assertTrue( Objects.nonNull( MetricFactory.ofSummaryStatisticsForTimingErrorMetric( MetricConstants.TIME_TO_PEAK_RELATIVE_ERROR ) ) );
        assertTrue( Objects.isNull( MetricFactory.ofSummaryStatisticsForTimingErrorMetric( MetricConstants.MAIN ) ) );
    }

    /**
     * Tests the {@link MetricFactory#ofTimingErrorDurationStatistics(MetricConstants, java.util.Set)}.
     * @throws MetricParameterException if the metric construction fails 
     */

    @Test
    public void testOfTimingErrorDurationStatistics() throws MetricParameterException
    {
        assertTrue( metF.ofTimingErrorDurationStatistics( MetricConstants.TIME_TO_PEAK_ERROR,
                                                          Collections.singleton( MetricConstants.MEAN ) ) instanceof TimingErrorDurationStatistics );
    }

    /**
     * Tests the {@link MetricFactory#ofMetricProcessorForProject(ProjectConfig, 
     * wres.datamodel.ThresholdsByMetric, java.util.concurrent.ExecutorService, java.util.concurrent.ExecutorService)}. 
     * @throws IOException if the input configuration could not be read
     * @throws MetricProcessorException if the metric processor could not be constructed
     */

    @Test
    public void testOfMetricProcessorByProject() throws IOException, MetricProcessorException
    {
        String configPathSingleValued =
                "testinput/metricProcessorSingleValuedPairsByTimeTest/testApplyWithoutThresholds.xml";

        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPathSingleValued ) ).getProjectConfig();
        assertTrue( MetricFactory.getInstance( DefaultDataFactory.getInstance() )
                                 .ofMetricProcessorForProject( config,
                                                               null,
                                                               ForkJoinPool.commonPool(),
                                                               ForkJoinPool.commonPool() ) instanceof MetricProcessorForProject );
    }

    /**
     * Tests the {@link MetricFactory#ofMetricProcessorByTimeSingleValuedPairs(ProjectConfig, java.util.Set)}. 
     * @throws IOException if the input configuration could not be read
     * @throws MetricProcessorException if the metric processor could not be constructed
     */

    @Test
    public void testOfMetricProcessorByTimeSingleValuedPairs() throws IOException, MetricProcessorException
    {
        String configPathSingleValued =
                "testinput/metricProcessorSingleValuedPairsByTimeTest/testApplyWithoutThresholds.xml";

        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPathSingleValued ) ).getProjectConfig();
        assertTrue( MetricFactory.getInstance( DefaultDataFactory.getInstance() )
                                 .ofMetricProcessorByTimeSingleValuedPairs( config,
                                                                            null ) instanceof MetricProcessorByTimeSingleValuedPairs );
    }

    /**
     * Tests the {@link MetricFactory#ofMetricProcessorByTimeEnsemblePairs(ProjectConfig, 
     * wres.datamodel.ThresholdsByMetric, java.util.Set)}. 
     * @throws IOException if the input configuration could not be read
     * @throws MetricProcessorException if the metric processor could not be constructed
     */

    @Test
    public void testOfMetricProcessorByTimeEnsemblePairs() throws IOException, MetricProcessorException
    {
        String configPathEnsemble = "testinput/metricProcessorEnsemblePairsByTimeTest/testApplyWithoutThresholds.xml";
        ProjectConfig configTwo = ProjectConfigPlus.from( Paths.get( configPathEnsemble ) ).getProjectConfig();
        assertTrue( MetricFactory.getInstance( DefaultDataFactory.getInstance() )
                                 .ofMetricProcessorByTimeEnsemblePairs( configTwo,
                                                                        null ) instanceof MetricProcessorByTimeEnsemblePairs );
    }

    /**
     * Tests the {@link MetricFactory#ofMetricProcessorByTimeSingleValuedPairs(ProjectConfig, 
     * wres.datamodel.ThresholdsByMetric, java.util.Set)}. 
     * @throws IOException if the input configuration could not be read
     * @throws MetricProcessorException if the metric processor could not be constructed
     */

    @Test
    public void testOfMetricProcessorByTimeSingleValuedPairsWithExternalThresholds()
            throws IOException, MetricProcessorException
    {
        String configPathSingleValued =
                "testinput/metricProcessorSingleValuedPairsByTimeTest/testApplyWithThresholds.xml";

        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPathSingleValued ) ).getProjectConfig();
        assertTrue( MetricFactory.getInstance( DefaultDataFactory.getInstance() )
                                 .ofMetricProcessorByTimeSingleValuedPairs( config,
                                                                            null,
                                                                            null ) instanceof MetricProcessorByTimeSingleValuedPairs );
    }

    /**
     * Tests the {@link MetricFactory#ofMetricProcessorByTimeEnsemblePairs(ProjectConfig, 
     * wres.datamodel.ThresholdsByMetric, java.util.Set)}. 
     * @throws IOException if the input configuration could not be read
     * @throws MetricProcessorException if the metric processor could not be constructed
     */

    @Test
    public void testOfMetricProcessorByTimeEnsemblePairsWithExternalThresholds()
            throws IOException, MetricProcessorException
    {
        String configPathSingleValued =
                "testinput/metricProcessorEnsemblePairsByTimeTest/testApplyWithValueThresholds.xml";

        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPathSingleValued ) ).getProjectConfig();
        assertTrue( MetricFactory.getInstance( DefaultDataFactory.getInstance() )
                                 .ofMetricProcessorByTimeEnsemblePairs( config,
                                                                        null,
                                                                        null ) instanceof MetricProcessorByTimeEnsemblePairs );
    }


    /**
     * Tests the {@link MetricFactory#ofMetricProcessorByTimeSingleValuedPairs(ProjectConfig, 
     * wres.datamodel.ThresholdsByMetric, java.util.concurrent.ExecutorService, java.util.concurrent.ExecutorService)}. 
     * @throws IOException if the input configuration could not be read
     * @throws MetricProcessorException if the metric processor could not be constructed
     */

    @Test
    public void testOfMetricProcessorByTimeSingleValuedPairsWithExternalThresholdsAndExecutors()
            throws IOException, MetricProcessorException
    {
        String configPathSingleValued =
                "testinput/metricProcessorSingleValuedPairsByTimeTest/testApplyWithThresholds.xml";

        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPathSingleValued ) ).getProjectConfig();
        assertTrue( MetricFactory.getInstance( DefaultDataFactory.getInstance() )
                                 .ofMetricProcessorByTimeSingleValuedPairs( config,
                                                                            null,
                                                                            ForkJoinPool.commonPool(),
                                                                            ForkJoinPool.commonPool() ) instanceof MetricProcessorByTimeSingleValuedPairs );
    }

    /**
     * Tests the {@link MetricFactory#ofMetricProcessorByTimeEnsemblePairs(ProjectConfig, 
     * wres.datamodel.ThresholdsByMetric, java.util.concurrent.ExecutorService, java.util.concurrent.ExecutorService)}. 
     * @throws IOException if the input configuration could not be read
     * @throws MetricProcessorException if the metric processor could not be constructed
     */

    @Test
    public void testOfMetricProcessorByTimeEnsemblePairsWithExternalThresholdsAndExecutors()
            throws IOException, MetricProcessorException
    {
        String configPathSingleValued =
                "testinput/metricProcessorEnsemblePairsByTimeTest/testApplyWithValueThresholds.xml";

        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPathSingleValued ) ).getProjectConfig();
        assertTrue( MetricFactory.getInstance( DefaultDataFactory.getInstance() )
                                 .ofMetricProcessorByTimeEnsemblePairs( config,
                                                                        null,
                                                                        ForkJoinPool.commonPool(),
                                                                        ForkJoinPool.commonPool() ) instanceof MetricProcessorByTimeEnsemblePairs );
    }


}
