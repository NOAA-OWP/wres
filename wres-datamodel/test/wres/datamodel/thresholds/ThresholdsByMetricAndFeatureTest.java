package wres.datamodel.thresholds;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.metrics.MetricConstants;
import wres.datamodel.space.FeatureGroup;
import wres.datamodel.space.FeatureKey;
import wres.datamodel.space.FeatureTuple;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdGroup;
import wres.datamodel.thresholds.ThresholdsByMetric.Builder;

/**
 * Tests the {@link ThresholdsByMetricAndFeature}.
 * @author James Brown
 */

class ThresholdsByMetricAndFeatureTest
{

    /** Instance to test. */
    private ThresholdsByMetricAndFeature testInstance;

    @BeforeEach
    private void runBeforeEachTest()
    {
        // Value thresholds
        Map<MetricConstants, Set<ThresholdOuter>> values = new EnumMap<>( MetricConstants.class );
        values.put( MetricConstants.MEAN_ERROR,
                    new HashSet<>( Arrays.asList( ThresholdOuter.of( OneOrTwoDoubles.of( 0.2 ),
                                                                     Operator.GREATER,
                                                                     ThresholdDataType.LEFT ) ) ) );

        ThresholdsByMetric thresholdsByMetric = new Builder().addThresholds( values, ThresholdGroup.VALUE )
                                                             .build();

        Map<FeatureTuple, ThresholdsByMetric> thresholds =
                Map.of( new FeatureTuple( FeatureKey.of( "a" ), FeatureKey.of( "a" ), null ), thresholdsByMetric );

        this.testInstance = ThresholdsByMetricAndFeature.of( thresholds, 7 );
    }

    @Test
    void testGetMinimumSampleSize()
    {
        assertEquals( 7, this.testInstance.getMinimumSampleSize() );
    }

    @Test
    void testGetMetrics()
    {
        assertEquals( Set.of( MetricConstants.MEAN_ERROR ), this.testInstance.getMetrics() );
    }

    @Test
    void testGetThresholdsByMetricAndFeature()
    {
        Map<FeatureTuple, ThresholdsByMetric> actual = this.testInstance.getThresholdsByMetricAndFeature();

        Map<MetricConstants, Set<ThresholdOuter>> values = new EnumMap<>( MetricConstants.class );
        values.put( MetricConstants.MEAN_ERROR,
                    new HashSet<>( Arrays.asList( ThresholdOuter.of( OneOrTwoDoubles.of( 0.2 ),
                                                                     Operator.GREATER,
                                                                     ThresholdDataType.LEFT ) ) ) );

        ThresholdsByMetric thresholdsByMetric = new Builder().addThresholds( values, ThresholdGroup.VALUE )
                                                             .build();

        Map<FeatureTuple, ThresholdsByMetric> expected =
                Map.of( new FeatureTuple( FeatureKey.of( "a" ), FeatureKey.of( "a" ), null ), thresholdsByMetric );

        assertEquals( expected, actual );
    }

    @Test
    void testGetThresholdsByMetricAndFeatureByFeatureGroup()
    {
        FeatureTuple featureTuple = new FeatureTuple( FeatureKey.of( "a" ), FeatureKey.of( "a" ), null );
        FeatureGroup featureGroup = FeatureGroup.of( featureTuple );
        ThresholdsByMetricAndFeature actual = this.testInstance.getThresholdsByMetricAndFeature( featureGroup );
        assertEquals( this.testInstance, actual );
    }

}
