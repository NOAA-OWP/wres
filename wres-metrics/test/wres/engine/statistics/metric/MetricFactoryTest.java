package wres.engine.statistics.metric;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.junit.Before;
import org.junit.Test;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.DatasourceType;
import wres.config.generated.DestinationConfig;
import wres.config.generated.MetricConfig;
import wres.config.generated.MetricConfigName;
import wres.config.generated.MetricsConfig;
import wres.config.generated.OutputTypeSelection;
import wres.config.generated.ProjectConfig;
import wres.config.generated.ProjectConfig.Inputs;
import wres.config.generated.ProjectConfig.Outputs;
import wres.config.generated.ThresholdDataType;
import wres.config.generated.ThresholdOperator;
import wres.config.generated.ThresholdType;
import wres.config.generated.ThresholdsConfig;
import wres.datamodel.MetricConstants;
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
import wres.engine.statistics.metric.singlevalued.SumOfSquareError;
import wres.engine.statistics.metric.timeseries.TimeToPeakError;
import wres.engine.statistics.metric.timeseries.TimeToPeakRelativeError;

/**
 * Tests the {@link MetricFactory}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class MetricFactoryTest
{
    /**
     * Expected error message.
     */

    private static final String UNRECOGNIZED_METRIC_FOR_IDENTIFIER_MAIN =
            "Unrecognized metric for identifier. 'MAIN'.";

    /**
     * Mocked single-valued configuration.
     */

    private ProjectConfig mockSingleValued;

    /**
     * Mocked single-valued configuration.
     */

    private ProjectConfig mockEnsemble;

    @Before
    public void setupBeforeEachTest()
    {
        setMockConfiguration();
    }

    /**
     * Tests {@link MetricFactory#ofSingleValuedScore(MetricConstants)}. 
     */
    @Test
    public void testOfSingleValuedScore()
    {
        assertTrue( MetricFactory.ofSingleValuedScore( MetricConstants.BIAS_FRACTION ) instanceof BiasFraction );
        assertTrue( MetricFactory.ofSingleValuedScore( MetricConstants.MEAN_ABSOLUTE_ERROR ) instanceof MeanAbsoluteError );
        assertTrue( MetricFactory.ofSingleValuedScore( MetricConstants.MEAN_ERROR ) instanceof MeanError );
        assertTrue( MetricFactory.ofSingleValuedScore( MetricConstants.ROOT_MEAN_SQUARE_ERROR ) instanceof RootMeanSquareError );
        assertTrue( MetricFactory.ofSingleValuedScore( MetricConstants.PEARSON_CORRELATION_COEFFICIENT ) instanceof CorrelationPearsons );
        assertTrue( MetricFactory.ofSingleValuedScore( MetricConstants.COEFFICIENT_OF_DETERMINATION ) instanceof CoefficientOfDetermination );
        assertTrue( MetricFactory.ofSingleValuedScore( MetricConstants.INDEX_OF_AGREEMENT ) instanceof IndexOfAgreement );
        assertTrue( MetricFactory.ofSingleValuedScore( MetricConstants.SAMPLE_SIZE ) instanceof SampleSize );
        assertTrue( MetricFactory.ofSingleValuedScore( MetricConstants.MEAN_SQUARE_ERROR ) instanceof MeanSquareError );
        assertTrue( MetricFactory.ofSingleValuedScore( MetricConstants.MEAN_SQUARE_ERROR_SKILL_SCORE ) instanceof MeanSquareErrorSkillScore );
        assertTrue( MetricFactory.ofSingleValuedScore( MetricConstants.KLING_GUPTA_EFFICIENCY ) instanceof KlingGuptaEfficiency );
        assertTrue( MetricFactory.ofSingleValuedScore( MetricConstants.SUM_OF_SQUARE_ERROR ) instanceof SumOfSquareError );

        // Unrecognized metric
        IllegalArgumentException expected = assertThrows( IllegalArgumentException.class,
                                                          () -> MetricFactory.ofSingleValuedScore( MetricConstants.MAIN ) );
        assertEquals( UNRECOGNIZED_METRIC_FOR_IDENTIFIER_MAIN, expected.getMessage() );
    }

    /**
     * Tests {@link MetricFactory#ofSingleValuedScoreCollectable(MetricConstants)}. 
     */
    @Test
    public void testOfSingleValuedScoreCollectable()
    {
        assertTrue( MetricFactory.ofSingleValuedScoreCollectable( MetricConstants.ROOT_MEAN_SQUARE_ERROR ) instanceof RootMeanSquareError );
        assertTrue( MetricFactory.ofSingleValuedScoreCollectable( MetricConstants.PEARSON_CORRELATION_COEFFICIENT ) instanceof CorrelationPearsons );
        assertTrue( MetricFactory.ofSingleValuedScoreCollectable( MetricConstants.COEFFICIENT_OF_DETERMINATION ) instanceof CoefficientOfDetermination );
        assertTrue( MetricFactory.ofSingleValuedScoreCollectable( MetricConstants.MEAN_SQUARE_ERROR ) instanceof MeanSquareError );

        // Unrecognized metric
        IllegalArgumentException expected = assertThrows( IllegalArgumentException.class,
                                                          () -> MetricFactory.ofSingleValuedScoreCollectable( MetricConstants.MAIN ) );
        assertEquals( UNRECOGNIZED_METRIC_FOR_IDENTIFIER_MAIN, expected.getMessage() );
    }

    /**
     * Tests {@link MetricFactory#ofDiscreteProbabilityScore(MetricConstants)} 
     */
    @Test
    public void testOfDiscreteProbabilityScore()
    {
        assertTrue( MetricFactory.ofDiscreteProbabilityScore( MetricConstants.BRIER_SCORE ) instanceof BrierScore );
        assertTrue( MetricFactory.ofDiscreteProbabilityScore( MetricConstants.BRIER_SKILL_SCORE ) instanceof BrierSkillScore );
        assertTrue( MetricFactory.ofDiscreteProbabilityScore( MetricConstants.RELATIVE_OPERATING_CHARACTERISTIC_SCORE ) instanceof RelativeOperatingCharacteristicScore );

        // Unrecognized metric
        IllegalArgumentException expected = assertThrows( IllegalArgumentException.class,
                                                          () -> MetricFactory.ofDiscreteProbabilityScore( MetricConstants.MAIN ) );
        assertEquals( UNRECOGNIZED_METRIC_FOR_IDENTIFIER_MAIN, expected.getMessage() );
    }

    /**
     * Tests {@link MetricFactory#ofDichotomousScore(MetricConstants)}. 
     */
    @Test
    public void testOfDichotomousScore()
    {
        assertTrue( MetricFactory.ofDichotomousScore( MetricConstants.THREAT_SCORE ) instanceof ThreatScore );
        assertTrue( MetricFactory.ofDichotomousScore( MetricConstants.EQUITABLE_THREAT_SCORE ) instanceof EquitableThreatScore );
        assertTrue( MetricFactory.ofDichotomousScore( MetricConstants.PEIRCE_SKILL_SCORE ) instanceof PeirceSkillScore );
        assertTrue( MetricFactory.ofDichotomousScore( MetricConstants.PROBABILITY_OF_DETECTION ) instanceof ProbabilityOfDetection );
        assertTrue( MetricFactory.ofDichotomousScore( MetricConstants.PROBABILITY_OF_FALSE_DETECTION ) instanceof ProbabilityOfFalseDetection );

        // Unrecognized metric
        IllegalArgumentException expected = assertThrows( IllegalArgumentException.class,
                                                          () -> MetricFactory.ofDichotomousScore( MetricConstants.MAIN ) );
        assertEquals( UNRECOGNIZED_METRIC_FOR_IDENTIFIER_MAIN, expected.getMessage() );
    }

    /**
     * Tests {@link MetricFactory#ofEnsembleScore(MetricConstants)}. 
     */
    @Test
    public void testOfEnsembleScore()
    {
        assertTrue( MetricFactory.ofEnsembleScore( MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SCORE ) instanceof ContinuousRankedProbabilityScore );
        assertTrue( MetricFactory.ofEnsembleScore( MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE ) instanceof ContinuousRankedProbabilitySkillScore );
        assertTrue( MetricFactory.ofEnsembleScore( MetricConstants.SAMPLE_SIZE ) instanceof SampleSize );

        // Unrecognized metric
        IllegalArgumentException expected = assertThrows( IllegalArgumentException.class,
                                                          () -> MetricFactory.ofEnsembleScore( MetricConstants.MAIN ) );
        assertEquals( UNRECOGNIZED_METRIC_FOR_IDENTIFIER_MAIN, expected.getMessage() );
    }

    /**
     * Tests {@link MetricFactory#ofSingleValuedMultiVector(MetricConstants)}. 
     */
    @Test
    public void testOfSingleValuedMultiVector()
    {
        assertTrue( MetricFactory.ofSingleValuedMultiVector( MetricConstants.QUANTILE_QUANTILE_DIAGRAM ) instanceof QuantileQuantileDiagram );

        // Unrecognized metric
        IllegalArgumentException expected = assertThrows( IllegalArgumentException.class,
                                                          () -> MetricFactory.ofSingleValuedMultiVector( MetricConstants.MAIN ) );
        assertEquals( UNRECOGNIZED_METRIC_FOR_IDENTIFIER_MAIN, expected.getMessage() );
    }

    /**
     * Tests {@link MetricFactory#ofSingleValuedMultiVector(MetricConstants)}. 
     * @throws MetricParameterException if the metric construction fails 
     */
    @Test
    public void testOfDiscreteProbabilityMultiVector() throws MetricParameterException
    {
        assertTrue( MetricFactory.ofDiscreteProbabilityMultiVector( MetricConstants.RELIABILITY_DIAGRAM ) instanceof ReliabilityDiagram );
        assertTrue( MetricFactory.ofDiscreteProbabilityMultiVector( MetricConstants.RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM ) instanceof RelativeOperatingCharacteristicDiagram );

        // Unrecognized metric
        IllegalArgumentException expected = assertThrows( IllegalArgumentException.class,
                                                          () -> MetricFactory.ofDiscreteProbabilityMultiVector( MetricConstants.MAIN ) );
        assertEquals( UNRECOGNIZED_METRIC_FOR_IDENTIFIER_MAIN, expected.getMessage() );
    }

    /**
     * Tests {@link MetricFactory#ofEnsembleMultiVector(MetricConstants)}. 
     */
    @Test
    public void testOfEnsembleMultiVector()
    {
        assertTrue( MetricFactory.ofEnsembleMultiVector( MetricConstants.RANK_HISTOGRAM ) instanceof RankHistogram );

        // Unrecognized metric
        IllegalArgumentException expected = assertThrows( IllegalArgumentException.class,
                                                          () -> MetricFactory.ofEnsembleMultiVector( MetricConstants.MAIN ) );
        assertEquals( UNRECOGNIZED_METRIC_FOR_IDENTIFIER_MAIN, expected.getMessage() );
    }

    /**
     * Tests {@link MetricFactory#ofEnsembleBoxPlot(MetricConstants)}.  
     */
    @Test
    public void testOfEnsembleBoxPlot()
    {
        assertTrue( MetricFactory.ofEnsembleBoxPlot( MetricConstants.BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE ) instanceof BoxPlotErrorByObserved );
        assertTrue( MetricFactory.ofEnsembleBoxPlot( MetricConstants.BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE ) instanceof BoxPlotErrorByForecast );

        // Unrecognized metric
        IllegalArgumentException expected = assertThrows( IllegalArgumentException.class,
                                                          () -> MetricFactory.ofEnsembleBoxPlot( MetricConstants.MAIN ) );
        assertEquals( UNRECOGNIZED_METRIC_FOR_IDENTIFIER_MAIN, expected.getMessage() );
    }

    /**
     * Tests {@link MetricFactory#ofSingleValuedTimeSeries(MetricConstants)}. 
     */
    @Test
    public void testSingleValuedTimeSeries()
    {
        assertTrue( MetricFactory.ofSingleValuedTimeSeries( MetricConstants.TIME_TO_PEAK_ERROR ) instanceof TimeToPeakError );
        assertTrue( MetricFactory.ofSingleValuedTimeSeries( MetricConstants.TIME_TO_PEAK_RELATIVE_ERROR ) instanceof TimeToPeakRelativeError );

        // Unrecognized metric
        IllegalArgumentException expected = assertThrows( IllegalArgumentException.class,
                                                          () -> MetricFactory.ofSingleValuedTimeSeries( MetricConstants.MAIN ) );
        assertEquals( UNRECOGNIZED_METRIC_FOR_IDENTIFIER_MAIN, expected.getMessage() );
    }

    /**
     * Tests {@link MetricFactory#ofSingleValuedScoreCollection(MetricConstants...)}. 
     * @throws MetricParameterException if the metric construction fails
     */
    @Test
    public void testOfSingleValuedScoreCollection() throws MetricParameterException
    {
        assertTrue( Objects.nonNull( MetricFactory.ofSingleValuedScoreCollection( MetricConstants.MEAN_ABSOLUTE_ERROR ) ) );
    }

    /**
     * Tests {@link MetricFactory#ofDiscreteProbabilityScoreCollection(MetricConstants...)}. 
     * @throws MetricParameterException if the metric construction fails 
     */
    @Test
    public void testOfDiscreteProbabilityVectorCollection() throws MetricParameterException
    {
        assertTrue( Objects.nonNull( MetricFactory.ofDiscreteProbabilityScoreCollection( MetricConstants.BRIER_SCORE ) ) );
    }

    /**
     * Tests {@link MetricFactory#ofDichotomousScoreCollection(MetricConstants...)}. 
     * @throws MetricParameterException if the metric construction fails 
     */
    @Test
    public void testOfDichotomousScoreCollection() throws MetricParameterException
    {
        assertTrue( Objects.nonNull( MetricFactory.ofDichotomousScoreCollection( MetricConstants.THREAT_SCORE ) ) );
        assertTrue( Objects.nonNull( MetricFactory.ofDichotomousScoreCollection( MetricConstants.FREQUENCY_BIAS ) ) );
    }

    /**
     * Tests {@link MetricFactory#ofSingleValuedMultiVectorCollection(MetricConstants...)}. 
     * @throws MetricParameterException if the metric construction fails 
     */
    @Test
    public void testOfSingleValuedMultiVectorCollection() throws MetricParameterException
    {
        assertTrue( Objects.nonNull( MetricFactory.ofSingleValuedMultiVectorCollection( MetricConstants.QUANTILE_QUANTILE_DIAGRAM ) ) );
    }

    /**
     * Tests {@link MetricFactory#ofDiscreteProbabilityMultiVectorCollection(MetricConstants...)}. 
     * @throws MetricParameterException if the metric collection could not be constructed
     */
    @Test
    public void testOfDiscreteProbabilityMultiVectorCollection() throws MetricParameterException
    {
        assertTrue( Objects.nonNull( MetricFactory.ofDiscreteProbabilityMultiVectorCollection( MetricConstants.RELIABILITY_DIAGRAM ) ) );
        assertTrue( Objects.nonNull( MetricFactory.ofDiscreteProbabilityMultiVectorCollection( MetricConstants.RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM ) ) );
    }

    /**
     * Tests {@link MetricFactory#ofEnsembleScoreCollection(MetricConstants...)}. 
     * @throws MetricParameterException if the metric construction fails 
     */
    @Test
    public void testOfEnsembleScoreCollection() throws MetricParameterException
    {
        assertTrue( Objects.nonNull( MetricFactory.ofEnsembleScoreCollection( MetricConstants.SAMPLE_SIZE ) ) );
        assertTrue( Objects.nonNull( MetricFactory.ofEnsembleScoreCollection( MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SCORE ) ) );
        assertTrue( Objects.nonNull( MetricFactory.ofEnsembleScoreCollection( MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE ) ) );
    }

    /**
     * Tests {@link MetricFactory#ofEnsembleMultiVectorCollection(MetricConstants...)}. 
     * @throws MetricParameterException if the metric construction fails 
     */
    @Test
    public void testOfEnsembleMultiVectorCollection() throws MetricParameterException
    {
        assertTrue( Objects.nonNull( MetricFactory.ofEnsembleMultiVectorCollection( MetricConstants.RANK_HISTOGRAM ) ) );
    }

    /**
     * Tests {@link MetricFactory#ofEnsembleBoxPlotCollection(MetricConstants...)}. 
     * @throws MetricParameterException if the metric construction fails 
     */
    @Test
    public void testOfEnsembleBoxPlotCollection() throws MetricParameterException
    {
        assertTrue( Objects.nonNull( MetricFactory.ofEnsembleBoxPlotCollection( MetricConstants.BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE ) ) );
        assertTrue( Objects.nonNull( MetricFactory.ofEnsembleBoxPlotCollection( MetricConstants.BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE ) ) );
    }

    /**
     * Tests {@link MetricFactory#ofSingleValuedTimeSeriesCollection(MetricConstants...)}. 
     * @throws MetricParameterException if the metric construction fails 
     */
    @Test
    public void testOfSingleValuedTimeSeriesCollection() throws MetricParameterException
    {
        assertTrue( Objects.nonNull( MetricFactory.ofSingleValuedTimeSeriesCollection( MetricConstants.TIME_TO_PEAK_ERROR ) ) );
        assertTrue( Objects.nonNull( MetricFactory.ofSingleValuedTimeSeriesCollection( MetricConstants.TIME_TO_PEAK_RELATIVE_ERROR ) ) );
    }

    /**
     * Tests {@link MetricFactory#ofSummaryStatisticsForTimingErrorMetric(MetricConstants)}. 
     */
    @Test
    public void testOfSummaryStatisticsForTimingErrorMetric()
    {
        assertTrue( Objects.nonNull( MetricFactory.ofSummaryStatisticsForTimingErrorMetric( MetricConstants.TIME_TO_PEAK_ERROR ) ) );
        assertTrue( Objects.nonNull( MetricFactory.ofSummaryStatisticsForTimingErrorMetric( MetricConstants.TIME_TO_PEAK_RELATIVE_ERROR ) ) );
        assertTrue( Objects.isNull( MetricFactory.ofSummaryStatisticsForTimingErrorMetric( MetricConstants.MAIN ) ) );
    }

    /**
     * Tests the {@link MetricFactory#ofMetricProcessorForSingleValuedPairs(ProjectConfig, java.util.Set)}. 
     * @throws MetricParameterException if one or more metric parameters is set incorrectly
     */

    @Test
    public void testOfMetricProcessorByTimeSingleValuedPairs() throws MetricParameterException
    {
        assertTrue( MetricFactory.ofMetricProcessorForSingleValuedPairs( mockSingleValued,
                                                                            null ) instanceof MetricProcessorByTimeSingleValuedPairs );
    }

    /**
     * Tests the {@link MetricFactory#ofMetricProcessorForEnsemblePairs(ProjectConfig, 
     * wres.datamodel.ThresholdsByMetric, java.util.Set)}. 
     * @throws MetricParameterException if one or more metric parameters is set incorrectly
     */

    @Test
    public void testOfMetricProcessorByTimeEnsemblePairs() throws MetricParameterException
    {
        assertTrue( MetricFactory.ofMetricProcessorForEnsemblePairs( mockEnsemble,
                                                                        null ) instanceof MetricProcessorByTimeEnsemblePairs );
    }

    /**
     * Tests the {@link MetricFactory#ofMetricProcessorForSingleValuedPairs(ProjectConfig, 
     * wres.datamodel.ThresholdsByMetric, java.util.Set)}. 
     * @throws IOException if the input configuration could not be read
     * @throws MetricParameterException if one or more metric parameters is set incorrectly
     */

    @Test
    public void testOfMetricProcessorByTimeSingleValuedPairsWithExternalThresholds()
            throws MetricParameterException
    {
        assertTrue( MetricFactory.ofMetricProcessorForSingleValuedPairs( mockSingleValued,
                                                                            null,
                                                                            null ) instanceof MetricProcessorByTimeSingleValuedPairs );
    }

    /**
     * Tests the {@link MetricFactory#ofMetricProcessorForEnsemblePairs(ProjectConfig, 
     * wres.datamodel.ThresholdsByMetric, java.util.Set)}. 
     * @throws MetricParameterException if one or more metric parameters is set incorrectly
     */

    @Test
    public void testOfMetricProcessorByTimeEnsemblePairsWithExternalThresholds()
            throws MetricParameterException
    {
        assertTrue( MetricFactory.ofMetricProcessorForEnsemblePairs( mockEnsemble,
                                                                        null,
                                                                        null ) instanceof MetricProcessorByTimeEnsemblePairs );
    }

    /**
     * Generates mock configuration for testing.
     */

    private void setMockConfiguration()
    {
        // Mock several project configurations
        List<MetricConfig> metrics = new ArrayList<>();
        metrics.add( new MetricConfig( null, null, MetricConfigName.BIAS_FRACTION ) );
        metrics.add( new MetricConfig( null, null, MetricConfigName.VOLUMETRIC_EFFICIENCY ) );
        metrics.add( new MetricConfig( null, null, MetricConfigName.SUM_OF_SQUARE_ERROR ) );
        metrics.add( new MetricConfig( null, null, MetricConfigName.MEAN_SQUARE_ERROR ) );
        metrics.add( new MetricConfig( null, null, MetricConfigName.ROOT_MEAN_SQUARE_ERROR ) );
        metrics.add( new MetricConfig( null, null, MetricConfigName.PEARSON_CORRELATION_COEFFICIENT ) );
        metrics.add( new MetricConfig( null, null, MetricConfigName.COEFFICIENT_OF_DETERMINATION ) );

        mockSingleValued =
                new ProjectConfig( new Inputs( null,
                                               new DataSourceConfig( DatasourceType.SINGLE_VALUED_FORECASTS,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null ),
                                               null ),
                                   null,
                                   Arrays.asList( new MetricsConfig( null, metrics, null ) ),
                                   null,
                                   null,
                                   null );

        // Add a threshold-dependent metric for the ensemble mock
        metrics.add( new MetricConfig( null, null, MetricConfigName.RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM ) );

        // Mock some thresholds
        List<ThresholdsConfig> thresholds = new ArrayList<>();
        thresholds.add( new ThresholdsConfig( ThresholdType.PROBABILITY,
                                              ThresholdDataType.LEFT,
                                              "0.1,0.2,0.3",
                                              ThresholdOperator.GREATER_THAN ) );

        mockEnsemble =
                new ProjectConfig( new Inputs( null,
                                               new DataSourceConfig( DatasourceType.ENSEMBLE_FORECASTS,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null ),
                                               null ),
                                   null,
                                   Arrays.asList( new MetricsConfig( thresholds, metrics, null ) ),
                                   new Outputs( Arrays.asList( new DestinationConfig( OutputTypeSelection.THRESHOLD_LEAD,
                                                                                      null,
                                                                                      null,
                                                                                      null,
                                                                                      null ) ),
                                                null ),
                                   null,
                                   null );
    }

}
