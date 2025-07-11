package wres.config;

import java.util.Set;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static wres.config.MetricConstants.FALSE_NEGATIVES;
import static wres.config.MetricConstants.FALSE_POSITIVES;
import static wres.config.MetricConstants.TIME_TO_PEAK_ERROR_STATISTIC;
import static wres.config.MetricConstants.TRUE_NEGATIVES;
import static wres.config.MetricConstants.TRUE_POSITIVES;

/**
 * Tests the {@link MetricConstants}.
 *
 * @author James Brown
 */
class MetricConstantsTest
{
    @Test
    void testIsSamplingUncertaintyAllowed()
    {
        assertAll( () -> assertTrue( MetricConstants.MEAN.isSamplingUncertaintyAllowed() ),
                   () -> assertFalse( MetricConstants.SCATTER_PLOT.isSamplingUncertaintyAllowed() ),
                   () -> assertFalse( MetricConstants.BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE.isSamplingUncertaintyAllowed() ),
                   () -> assertFalse( MetricConstants.BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE.isSamplingUncertaintyAllowed() ) );
    }

    @Test
    void testIsContinuous()
    {
        assertAll( () -> assertTrue( MetricConstants.MEAN.isContinuous() ),
                   () -> assertTrue( MetricConstants.ROOT_MEAN_SQUARE_ERROR.isContinuous() ),
                   () -> assertFalse( MetricConstants.BRIER_SCORE.isContinuous() ),
                   () -> assertFalse( MetricConstants.PROBABILITY_OF_FALSE_DETECTION.isContinuous() ) );
    }

    @Test
    void testIsAThresholdMetric()
    {
        assertAll( () -> assertTrue( MetricConstants.BRIER_SCORE.isAThresholdMetric() ),
                   () -> assertFalse( MetricConstants.BOX_PLOT_OF_ERRORS.isAThresholdMetric() ),
                   () -> assertFalse( MetricConstants.SCATTER_PLOT.isAThresholdMetric() ),
                   () -> assertFalse( MetricConstants.ENSEMBLE_QUANTILE_QUANTILE_DIAGRAM.isAThresholdMetric() ) );
    }

    @Test
    void testIsSkillMetric()
    {
        assertAll( () -> assertTrue( MetricConstants.BRIER_SKILL_SCORE.isSkillMetric() ),
                   () -> assertTrue( MetricConstants.MEAN_SQUARE_ERROR_SKILL_SCORE.isSkillMetric() ),
                   () -> assertFalse( MetricConstants.SCATTER_PLOT.isSkillMetric() ),
                   () -> assertFalse( MetricConstants.ENSEMBLE_QUANTILE_QUANTILE_DIAGRAM.isSkillMetric() ) );
    }

    @Test
    void testIsDifferenceMetric()
    {
        assertAll( () -> assertTrue( MetricConstants.BRIER_SCORE_DIFFERENCE.isDifferenceMetric() ),
                   () -> assertTrue( MetricConstants.MEAN_ERROR_DIFFERENCE.isDifferenceMetric() ),
                   () -> assertFalse( MetricConstants.SCATTER_PLOT.isDifferenceMetric() ),
                   () -> assertFalse( MetricConstants.ENSEMBLE_QUANTILE_QUANTILE_DIAGRAM.isDifferenceMetric() ) );
    }

    @Test
    void testIsExplicitBaselineRequired()
    {
        assertAll( () -> assertTrue( MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE.isExplicitBaselineRequired() ),
                   () -> assertFalse( MetricConstants.BRIER_SKILL_SCORE.isExplicitBaselineRequired() ) );
    }

    @Test
    void testIsInGroupWithSampleDataGroup()
    {
        assertAll( () -> assertTrue( MetricConstants.MEAN_ERROR.isInGroup( MetricConstants.SampleDataGroup.SINGLE_VALUED ) ),
                   () -> assertFalse( MetricConstants.BRIER_SKILL_SCORE.isInGroup( MetricConstants.SampleDataGroup.ENSEMBLE ) ) );
    }

    @Test
    void testIsInGroupWithStatisticType()
    {
        assertAll( () -> assertTrue( MetricConstants.MEAN_ERROR.isInGroup( MetricConstants.StatisticType.DOUBLE_SCORE ) ),
                   () -> assertFalse( MetricConstants.BRIER_SKILL_SCORE.isInGroup( MetricConstants.StatisticType.DURATION_SCORE ) ) );
    }

    @Test
    void testIsInGroupWithMetricGroup()
    {
        assertAll( () -> assertTrue( MetricConstants.MEAN.isInGroup( MetricConstants.MetricGroup.UNIVARIATE_STATISTIC ) ),
                   () -> assertTrue( MetricConstants.MEAN_SQUARE_ERROR.isInGroup( MetricConstants.MetricGroup.NONE ) ),
                   () -> assertFalse( MetricConstants.MEAN_ERROR.isInGroup( MetricConstants.MetricGroup.CR ) ) );
    }

    @Test
    void testIsInGroupWithSampleDataGroupAndStatisticType()
    {
        assertAll( () -> assertTrue( MetricConstants.MEAN_ERROR.isInGroup( MetricConstants.SampleDataGroup.SINGLE_VALUED,
                                                                           MetricConstants.StatisticType.DOUBLE_SCORE ) ),
                   () -> assertFalse( MetricConstants.BRIER_SKILL_SCORE.isInGroup( MetricConstants.SampleDataGroup.ENSEMBLE,
                                                                                   MetricConstants.StatisticType.DOUBLE_SCORE ) ) );
    }

    @Test
    void testGetParent()
    {
        assertAll( () -> assertEquals( MetricConstants.TIME_TO_PEAK_ERROR,
                                       MetricConstants.TIME_TO_PEAK_ERROR_MEDIAN.getParent() ),
                   () -> assertEquals( MetricConstants.CONTINGENCY_TABLE, TRUE_POSITIVES.getParent() ) );
    }

    @Test
    void testGetChild()
    {
        assertEquals( MetricConstants.MEDIAN, MetricConstants.TIME_TO_PEAK_ERROR_MEDIAN.getChild() );
    }

    @Test
    void testGetChildren()
    {
        assertEquals( Set.of( TRUE_POSITIVES,
                              TRUE_NEGATIVES,
                              FALSE_POSITIVES,
                              FALSE_NEGATIVES ),
                      MetricConstants.CONTINGENCY_TABLE.getChildren() );
    }

    @Test
    void testGetCollection()
    {
        assertEquals( TIME_TO_PEAK_ERROR_STATISTIC, MetricConstants.TIME_TO_PEAK_ERROR_MEDIAN.getCollection() );
    }
}
