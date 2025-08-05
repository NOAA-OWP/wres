package wres.metrics;

import java.util.Objects;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
import wres.metrics.timeseries.SingleValuedTimeSeriesPlot;
import wres.metrics.timeseries.SpaghettiPlot;
import wres.metrics.timeseries.TimeToPeakError;
import wres.metrics.timeseries.TimeToPeakRelativeError;

/**
 * Tests the {@link MetricFactory}.
 *
 * @author James Brown
 */
final class MetricFactoryTest
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
    void testOfSingleValuedScore()
    {
        assertInstanceOf( BiasFraction.class, MetricFactory.ofSingleValuedScore( MetricConstants.BIAS_FRACTION ) );
        assertInstanceOf( MeanAbsoluteError.class,
                          MetricFactory.ofSingleValuedScore( MetricConstants.MEAN_ABSOLUTE_ERROR ) );
        assertInstanceOf( MeanError.class, MetricFactory.ofSingleValuedScore( MetricConstants.MEAN_ERROR ) );
        assertInstanceOf( RootMeanSquareError.class,
                          MetricFactory.ofSingleValuedScore( MetricConstants.ROOT_MEAN_SQUARE_ERROR ) );
        assertInstanceOf( CorrelationPearsons.class,
                          MetricFactory.ofSingleValuedScore( MetricConstants.PEARSON_CORRELATION_COEFFICIENT ) );
        assertInstanceOf( CoefficientOfDetermination.class,
                          MetricFactory.ofSingleValuedScore( MetricConstants.COEFFICIENT_OF_DETERMINATION ) );
        assertInstanceOf( IndexOfAgreement.class,
                          MetricFactory.ofSingleValuedScore( MetricConstants.INDEX_OF_AGREEMENT ) );
        assertInstanceOf( SampleSize.class, MetricFactory.ofSingleValuedScore( MetricConstants.SAMPLE_SIZE ) );
        assertInstanceOf( MeanSquareError.class,
                          MetricFactory.ofSingleValuedScore( MetricConstants.MEAN_SQUARE_ERROR ) );
        assertInstanceOf( MeanSquareErrorSkillScore.class,
                          MetricFactory.ofSingleValuedScore( MetricConstants.MEAN_SQUARE_ERROR_SKILL_SCORE ) );
        assertInstanceOf( KlingGuptaEfficiency.class,
                          MetricFactory.ofSingleValuedScore( MetricConstants.KLING_GUPTA_EFFICIENCY ) );
        assertInstanceOf( SumOfSquareError.class,
                          MetricFactory.ofSingleValuedScore( MetricConstants.SUM_OF_SQUARE_ERROR ) );

        // Unrecognized metric
        IllegalArgumentException expected = assertThrows( IllegalArgumentException.class,
                                                          () -> MetricFactory.ofSingleValuedScore( MetricConstants.MAIN ) );
        assertEquals( UNRECOGNIZED_METRIC_FOR_IDENTIFIER_MAIN, expected.getMessage() );
    }

    /**
     * Tests {@link MetricFactory#ofDiscreteProbabilityScore(MetricConstants)}
     */
    @Test
    void testOfDiscreteProbabilityScore()
    {
        assertInstanceOf( BrierScore.class, MetricFactory.ofDiscreteProbabilityScore( MetricConstants.BRIER_SCORE ) );
        assertInstanceOf( BrierSkillScore.class,
                          MetricFactory.ofDiscreteProbabilityScore( MetricConstants.BRIER_SKILL_SCORE ) );
        assertInstanceOf( RelativeOperatingCharacteristicScore.class,
                          MetricFactory.ofDiscreteProbabilityScore( MetricConstants.RELATIVE_OPERATING_CHARACTERISTIC_SCORE ) );

        // Unrecognized metric
        IllegalArgumentException expected = assertThrows( IllegalArgumentException.class,
                                                          () -> MetricFactory.ofDiscreteProbabilityScore(
                                                                  MetricConstants.MAIN ) );
        assertEquals( UNRECOGNIZED_METRIC_FOR_IDENTIFIER_MAIN, expected.getMessage() );
    }

    /**
     * Tests {@link MetricFactory#ofDichotomousScore(MetricConstants)}. 
     */
    @Test
    void testOfDichotomousScore()
    {
        assertInstanceOf( ThreatScore.class, MetricFactory.ofDichotomousScore( MetricConstants.THREAT_SCORE ) );
        assertInstanceOf( EquitableThreatScore.class,
                          MetricFactory.ofDichotomousScore( MetricConstants.EQUITABLE_THREAT_SCORE ) );
        assertInstanceOf( PeirceSkillScore.class,
                          MetricFactory.ofDichotomousScore( MetricConstants.PEIRCE_SKILL_SCORE ) );
        assertInstanceOf( ProbabilityOfDetection.class,
                          MetricFactory.ofDichotomousScore( MetricConstants.PROBABILITY_OF_DETECTION ) );
        assertInstanceOf( ProbabilityOfFalseDetection.class,
                          MetricFactory.ofDichotomousScore( MetricConstants.PROBABILITY_OF_FALSE_DETECTION ) );

        // Unrecognized metric
        IllegalArgumentException expected = assertThrows( IllegalArgumentException.class,
                                                          () -> MetricFactory.ofDichotomousScore( MetricConstants.MAIN ) );
        assertEquals( UNRECOGNIZED_METRIC_FOR_IDENTIFIER_MAIN, expected.getMessage() );
    }

    /**
     * Tests {@link MetricFactory#ofEnsembleScore(MetricConstants)}. 
     */
    @Test
    void testOfEnsembleScore()
    {
        assertInstanceOf( ContinuousRankedProbabilityScore.class,
                          MetricFactory.ofEnsembleScore( MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SCORE ) );
        assertInstanceOf( ContinuousRankedProbabilitySkillScore.class,
                          MetricFactory.ofEnsembleScore( MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE ) );
        assertInstanceOf( SampleSize.class, MetricFactory.ofEnsembleScore( MetricConstants.SAMPLE_SIZE ) );

        // Unrecognized metric
        IllegalArgumentException expected = assertThrows( IllegalArgumentException.class,
                                                          () -> MetricFactory.ofEnsembleScore( MetricConstants.MAIN ) );
        assertEquals( UNRECOGNIZED_METRIC_FOR_IDENTIFIER_MAIN, expected.getMessage() );
    }

    /**
     * Tests {@link MetricFactory#ofSingleValuedDiagram(MetricConstants)}. 
     */
    @Test
    void testOfSingleValuedDiagram()
    {
        assertInstanceOf( QuantileQuantileDiagram.class,
                          MetricFactory.ofSingleValuedDiagram( MetricConstants.QUANTILE_QUANTILE_DIAGRAM ) );
        assertInstanceOf( ScatterPlot.class, MetricFactory.ofSingleValuedDiagram( MetricConstants.SCATTER_PLOT ) );

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
    void testOfDiscreteProbabilityDiagram() throws MetricParameterException
    {
        assertInstanceOf( ReliabilityDiagram.class,
                          MetricFactory.ofDiscreteProbabilityDiagram( MetricConstants.RELIABILITY_DIAGRAM ) );
        assertInstanceOf( RelativeOperatingCharacteristicDiagram.class,
                          MetricFactory.ofDiscreteProbabilityDiagram( MetricConstants.RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM ) );

        // Unrecognized metric
        IllegalArgumentException expected = assertThrows( IllegalArgumentException.class,
                                                          () -> MetricFactory.ofDiscreteProbabilityDiagram(
                                                                  MetricConstants.MAIN ) );
        assertEquals( UNRECOGNIZED_METRIC_FOR_IDENTIFIER_MAIN, expected.getMessage() );
    }

    /**
     * Tests {@link MetricFactory#ofEnsembleDiagram(MetricConstants)}. 
     */
    @Test
    void testOfEnsembleDiagram()
    {
        assertInstanceOf( RankHistogram.class, MetricFactory.ofEnsembleDiagram( MetricConstants.RANK_HISTOGRAM ) );

        // Unrecognized metric
        IllegalArgumentException expected = assertThrows( IllegalArgumentException.class,
                                                          () -> MetricFactory.ofEnsembleDiagram( MetricConstants.MAIN ) );
        assertEquals( UNRECOGNIZED_METRIC_FOR_IDENTIFIER_MAIN, expected.getMessage() );
    }

    /**
     * Tests {@link MetricFactory#ofEnsembleBoxPlot(MetricConstants)}.  
     */
    @Test
    void testOfEnsembleBoxPlot()
    {
        assertInstanceOf( BoxPlotErrorByObserved.class,
                          MetricFactory.ofEnsembleBoxPlot( MetricConstants.BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE ) );
        assertInstanceOf( BoxPlotErrorByForecast.class,
                          MetricFactory.ofEnsembleBoxPlot( MetricConstants.BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE ) );

        // Unrecognized metric
        IllegalArgumentException expected = assertThrows( IllegalArgumentException.class,
                                                          () -> MetricFactory.ofEnsembleBoxPlot( MetricConstants.MAIN ) );
        assertEquals( UNRECOGNIZED_METRIC_FOR_IDENTIFIER_MAIN, expected.getMessage() );
    }

    /**
     * Tests {@link MetricFactory#ofSingleValuedTimeSeries(MetricConstants)}. 
     */
    @Test
    void testSingleValuedTimeSeries()
    {
        assertInstanceOf( TimeToPeakError.class,
                          MetricFactory.ofSingleValuedTimeSeries( MetricConstants.TIME_TO_PEAK_ERROR ) );
        assertInstanceOf( TimeToPeakRelativeError.class,
                          MetricFactory.ofSingleValuedTimeSeries( MetricConstants.TIME_TO_PEAK_RELATIVE_ERROR ) );

        // Unrecognized metric
        IllegalArgumentException expected = assertThrows( IllegalArgumentException.class,
                                                          () -> MetricFactory.ofSingleValuedTimeSeries( MetricConstants.MAIN ) );
        assertEquals( UNRECOGNIZED_METRIC_FOR_IDENTIFIER_MAIN, expected.getMessage() );
    }

    /**
     * Tests {@link MetricFactory#ofSingleValuedPairs(MetricConstants)}.
     */
    @Test
    void testSingleValuedPairs()
    {
        assertInstanceOf( SingleValuedTimeSeriesPlot.class,
                          MetricFactory.ofSingleValuedPairs( MetricConstants.TIME_SERIES_PLOT ) );

        // Unrecognized metric
        IllegalArgumentException expected = assertThrows( IllegalArgumentException.class,
                                                          () -> MetricFactory.ofSingleValuedPairs( MetricConstants.MAIN ) );
        assertEquals( UNRECOGNIZED_METRIC_FOR_IDENTIFIER_MAIN, expected.getMessage() );
    }

    /**
     * Tests {@link MetricFactory#ofEnsemblePairs(MetricConstants)}.
     */
    @Test
    void testEnsemblePairs()
    {
        assertInstanceOf( SpaghettiPlot.class,
                          MetricFactory.ofEnsemblePairs( MetricConstants.SPAGHETTI_PLOT ) );

        // Unrecognized metric
        IllegalArgumentException expected = assertThrows( IllegalArgumentException.class,
                                                          () -> MetricFactory.ofEnsemblePairs( MetricConstants.MAIN ) );
        assertEquals( UNRECOGNIZED_METRIC_FOR_IDENTIFIER_MAIN, expected.getMessage() );
    }

    /**
     * Tests {@link MetricFactory#ofSingleValuedScores(MetricConstants...)}.
     * @throws MetricParameterException if the metric construction fails
     */
    @Test
    void testOfSingleValuedScoreCollection() throws MetricParameterException
    {
        assertTrue( Objects.nonNull( MetricFactory.ofSingleValuedScores( MetricConstants.MEAN_ABSOLUTE_ERROR ) ) );
    }

    /**
     * Tests {@link MetricFactory#ofDiscreteProbabilityScores(MetricConstants...)}.
     * @throws MetricParameterException if the metric construction fails 
     */
    @Test
    void testOfDiscreteProbabilityVectorCollection() throws MetricParameterException
    {
        assertTrue( Objects.nonNull( MetricFactory.ofDiscreteProbabilityScores( MetricConstants.BRIER_SCORE ) ) );
    }

    /**
     * Tests {@link MetricFactory#ofDichotomousScores(MetricConstants...)}.
     * @throws MetricParameterException if the metric construction fails 
     */
    @Test
    void testOfDichotomousScoreCollection() throws MetricParameterException
    {
        assertTrue( Objects.nonNull( MetricFactory.ofDichotomousScores( MetricConstants.THREAT_SCORE ) ) );
        assertTrue( Objects.nonNull( MetricFactory.ofDichotomousScores( MetricConstants.FREQUENCY_BIAS ) ) );
    }

    /**
     * Tests {@link MetricFactory#ofSingleValuedDiagrams(MetricConstants...)}.
     * @throws MetricParameterException if the metric construction fails 
     */
    @Test
    void testOfSingleValuedDiagramCollection() throws MetricParameterException
    {
        assertTrue( Objects.nonNull( MetricFactory.ofSingleValuedDiagrams( MetricConstants.QUANTILE_QUANTILE_DIAGRAM ) ) );
    }

    /**
     * Tests {@link MetricFactory#ofDiscreteProbabilityDiagrams(MetricConstants...)}.
     * @throws MetricParameterException if the metric collection could not be constructed
     */
    @Test
    void testOfDiscreteProbabilityDiagramCollection() throws MetricParameterException
    {
        assertTrue( Objects.nonNull( MetricFactory.ofDiscreteProbabilityDiagrams( MetricConstants.RELIABILITY_DIAGRAM ) ) );
        assertTrue( Objects.nonNull( MetricFactory.ofDiscreteProbabilityDiagrams( MetricConstants.RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM ) ) );
    }

    /**
     * Tests {@link MetricFactory#ofEnsembleScores(MetricConstants...)}.
     * @throws MetricParameterException if the metric construction fails 
     */
    @Test
    void testOfEnsembleScoreCollection() throws MetricParameterException
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
    void testOfEnsembleDiagramCollection() throws MetricParameterException
    {
        assertTrue( Objects.nonNull( MetricFactory.ofEnsembleDiagrams( MetricConstants.RANK_HISTOGRAM ) ) );
    }

    /**
     * Tests {@link MetricFactory#ofEnsembleBoxplots(MetricConstants...)}.
     * @throws MetricParameterException if the metric construction fails 
     */
    @Test
    void testOfEnsembleBoxPlotCollection() throws MetricParameterException
    {
        assertTrue( Objects.nonNull( MetricFactory.ofEnsembleBoxplots( MetricConstants.BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE ) ) );
        assertTrue( Objects.nonNull( MetricFactory.ofEnsembleBoxplots( MetricConstants.BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE ) ) );
    }

    /**
     * Tests {@link MetricFactory#ofSingleValuedTimeSeriesMetrics(MetricConstants...)}.
     * @throws MetricParameterException if the metric construction fails 
     */
    @Test
    void testOfSingleValuedTimeSeriesCollection() throws MetricParameterException
    {
        assertTrue( Objects.nonNull( MetricFactory.ofSingleValuedTimeSeriesMetrics( MetricConstants.TIME_TO_PEAK_ERROR ) ) );
        assertTrue( Objects.nonNull( MetricFactory.ofSingleValuedTimeSeriesMetrics( MetricConstants.TIME_TO_PEAK_RELATIVE_ERROR ) ) );
    }

    /**
     * Tests {@link MetricFactory#ofSingleValuedPairsMetrics(MetricConstants...)}.
     * @throws MetricParameterException if the metric construction fails
     */
    @Test
    void testOfSingleValuedPairsCollection() throws MetricParameterException
    {
        assertTrue( Objects.nonNull( MetricFactory.ofSingleValuedPairsMetrics( MetricConstants.TIME_SERIES_PLOT ) ) );
    }

    /**
     * Tests {@link MetricFactory#ofEnsemblePairsMetrics(MetricConstants...)}.
     * @throws MetricParameterException if the metric construction fails
     */
    @Test
    void testOfEnsemblePairsCollection() throws MetricParameterException
    {
        assertTrue( Objects.nonNull( MetricFactory.ofEnsemblePairsMetrics( MetricConstants.SPAGHETTI_PLOT ) ) );
    }

}
