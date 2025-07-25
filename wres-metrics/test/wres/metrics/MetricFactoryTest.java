package wres.metrics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.Objects;
import org.junit.Test;

import wres.config.MetricConstants;
import wres.metrics.categorical.EquitableThreatScore;
import wres.metrics.categorical.PeirceSkillScore;
import wres.metrics.categorical.ProbabilityOfDetection;
import wres.metrics.categorical.ProbabilityOfFalseDetection;
import wres.metrics.categorical.ThreatScore;
import wres.metrics.discreteprobability.BrierScore;
import wres.metrics.discreteprobability.BrierSkillScore;
import wres.metrics.discreteprobability.RelativeOperatingCharacteristicDiagram;
import wres.metrics.discreteprobability.RelativeOperatingCharacteristicScore;
import wres.metrics.discreteprobability.ReliabilityDiagram;
import wres.metrics.ensemble.BoxPlotErrorByForecast;
import wres.metrics.ensemble.BoxPlotErrorByObserved;
import wres.metrics.ensemble.ContinuousRankedProbabilityScore;
import wres.metrics.ensemble.ContinuousRankedProbabilitySkillScore;
import wres.metrics.ensemble.RankHistogram;
import wres.metrics.singlevalued.BiasFraction;
import wres.metrics.singlevalued.CoefficientOfDetermination;
import wres.metrics.singlevalued.CorrelationPearsons;
import wres.metrics.singlevalued.IndexOfAgreement;
import wres.metrics.singlevalued.KlingGuptaEfficiency;
import wres.metrics.singlevalued.MeanAbsoluteError;
import wres.metrics.singlevalued.MeanError;
import wres.metrics.singlevalued.MeanSquareError;
import wres.metrics.singlevalued.MeanSquareErrorSkillScore;
import wres.metrics.singlevalued.QuantileQuantileDiagram;
import wres.metrics.singlevalued.RootMeanSquareError;
import wres.metrics.singlevalued.ScatterPlot;
import wres.metrics.singlevalued.SumOfSquareError;
import wres.metrics.timeseries.TimeToPeakError;
import wres.metrics.timeseries.TimeToPeakRelativeError;

/**
 * Tests the {@link MetricFactory}.
 * 
 * @author James Brown
 */
public final class MetricFactoryTest
{
    /**
     * Expected error message.
     */

    private static final String UNRECOGNIZED_METRIC_FOR_IDENTIFIER_MAIN =
            "Unrecognized metric for identifier. 'MAIN'.";

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
     * Tests {@link MetricFactory#ofSingleValuedDiagram(MetricConstants)}. 
     */
    @Test
    public void testOfSingleValuedDiagram()
    {
        assertTrue( MetricFactory.ofSingleValuedDiagram( MetricConstants.QUANTILE_QUANTILE_DIAGRAM ) instanceof QuantileQuantileDiagram );
        assertTrue( MetricFactory.ofSingleValuedDiagram( MetricConstants.SCATTER_PLOT ) instanceof ScatterPlot );

        // Unrecognized metric
        IllegalArgumentException expected = assertThrows( IllegalArgumentException.class,
                                                          () -> MetricFactory.ofSingleValuedDiagram( MetricConstants.MAIN ) );
        assertEquals( UNRECOGNIZED_METRIC_FOR_IDENTIFIER_MAIN, expected.getMessage() );
    }

    /**
     * Tests {@link MetricFactory#ofSingleValuedDiagram(MetricConstants)}. 
     * @throws MetricParameterException if the metric construction fails 
     */
    @Test
    public void testOfDiscreteProbabilityDiagram() throws MetricParameterException
    {
        assertTrue( MetricFactory.ofDiscreteProbabilityDiagram( MetricConstants.RELIABILITY_DIAGRAM ) instanceof ReliabilityDiagram );
        assertTrue( MetricFactory.ofDiscreteProbabilityDiagram( MetricConstants.RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM ) instanceof RelativeOperatingCharacteristicDiagram );

        // Unrecognized metric
        IllegalArgumentException expected = assertThrows( IllegalArgumentException.class,
                                                          () -> MetricFactory.ofDiscreteProbabilityDiagram( MetricConstants.MAIN ) );
        assertEquals( UNRECOGNIZED_METRIC_FOR_IDENTIFIER_MAIN, expected.getMessage() );
    }

    /**
     * Tests {@link MetricFactory#ofEnsembleDiagram(MetricConstants)}. 
     */
    @Test
    public void testOfEnsembleDiagram()
    {
        assertTrue( MetricFactory.ofEnsembleDiagram( MetricConstants.RANK_HISTOGRAM ) instanceof RankHistogram );

        // Unrecognized metric
        IllegalArgumentException expected = assertThrows( IllegalArgumentException.class,
                                                          () -> MetricFactory.ofEnsembleDiagram( MetricConstants.MAIN ) );
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
     * Tests {@link MetricFactory#ofSingleValuedScores(MetricConstants...)}.
     * @throws MetricParameterException if the metric construction fails
     */
    @Test
    public void testOfSingleValuedScoreCollection() throws MetricParameterException
    {
        assertTrue( Objects.nonNull( MetricFactory.ofSingleValuedScores( MetricConstants.MEAN_ABSOLUTE_ERROR ) ) );
    }

    /**
     * Tests {@link MetricFactory#ofDiscreteProbabilityScores(MetricConstants...)}.
     * @throws MetricParameterException if the metric construction fails 
     */
    @Test
    public void testOfDiscreteProbabilityVectorCollection() throws MetricParameterException
    {
        assertTrue( Objects.nonNull( MetricFactory.ofDiscreteProbabilityScores( MetricConstants.BRIER_SCORE ) ) );
    }

    /**
     * Tests {@link MetricFactory#ofDichotomousScores(MetricConstants...)}.
     * @throws MetricParameterException if the metric construction fails 
     */
    @Test
    public void testOfDichotomousScoreCollection() throws MetricParameterException
    {
        assertTrue( Objects.nonNull( MetricFactory.ofDichotomousScores( MetricConstants.THREAT_SCORE ) ) );
        assertTrue( Objects.nonNull( MetricFactory.ofDichotomousScores( MetricConstants.FREQUENCY_BIAS ) ) );
    }

    /**
     * Tests {@link MetricFactory#ofSingleValuedDiagrams(MetricConstants...)}.
     * @throws MetricParameterException if the metric construction fails 
     */
    @Test
    public void testOfSingleValuedDiagramCollection() throws MetricParameterException
    {
        assertTrue( Objects.nonNull( MetricFactory.ofSingleValuedDiagrams( MetricConstants.QUANTILE_QUANTILE_DIAGRAM ) ) );
    }

    /**
     * Tests {@link MetricFactory#ofDiscreteProbabilityDiagrams(MetricConstants...)}.
     * @throws MetricParameterException if the metric collection could not be constructed
     */
    @Test
    public void testOfDiscreteProbabilityDiagramCollection() throws MetricParameterException
    {
        assertTrue( Objects.nonNull( MetricFactory.ofDiscreteProbabilityDiagrams( MetricConstants.RELIABILITY_DIAGRAM ) ) );
        assertTrue( Objects.nonNull( MetricFactory.ofDiscreteProbabilityDiagrams( MetricConstants.RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM ) ) );
    }

    /**
     * Tests {@link MetricFactory#ofEnsembleScores(MetricConstants...)}.
     * @throws MetricParameterException if the metric construction fails 
     */
    @Test
    public void testOfEnsembleScoreCollection() throws MetricParameterException
    {
        assertTrue( Objects.nonNull( MetricFactory.ofEnsembleScores( MetricConstants.SAMPLE_SIZE ) ) );
        assertTrue( Objects.nonNull( MetricFactory.ofEnsembleScores( MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SCORE ) ) );
        assertTrue( Objects.nonNull( MetricFactory.ofEnsembleScores( MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE ) ) );
    }

    /**
     * Tests {@link MetricFactory#ofEnsembleDiagrams(MetricConstants...)}.
     * @throws MetricParameterException if the metric construction fails 
     */
    @Test
    public void testOfEnsembleDiagramCollection() throws MetricParameterException
    {
        assertTrue( Objects.nonNull( MetricFactory.ofEnsembleDiagrams( MetricConstants.RANK_HISTOGRAM ) ) );
    }

    /**
     * Tests {@link MetricFactory#ofEnsembleBoxplots(MetricConstants...)}.
     * @throws MetricParameterException if the metric construction fails 
     */
    @Test
    public void testOfEnsembleBoxPlotCollection() throws MetricParameterException
    {
        assertTrue( Objects.nonNull( MetricFactory.ofEnsembleBoxplots( MetricConstants.BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE ) ) );
        assertTrue( Objects.nonNull( MetricFactory.ofEnsembleBoxplots( MetricConstants.BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE ) ) );
    }

    /**
     * Tests {@link MetricFactory#ofSingleValuedTimeSeriesMetrics(MetricConstants...)}.
     * @throws MetricParameterException if the metric construction fails 
     */
    @Test
    public void testOfSingleValuedTimeSeriesCollection() throws MetricParameterException
    {
        assertTrue( Objects.nonNull( MetricFactory.ofSingleValuedTimeSeriesMetrics( MetricConstants.TIME_TO_PEAK_ERROR ) ) );
        assertTrue( Objects.nonNull( MetricFactory.ofSingleValuedTimeSeriesMetrics( MetricConstants.TIME_TO_PEAK_RELATIVE_ERROR ) ) );
    }

}
