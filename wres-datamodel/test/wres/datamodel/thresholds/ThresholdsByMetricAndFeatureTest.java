package wres.datamodel.thresholds;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import wres.config.yaml.components.ThresholdOperator;
import wres.config.yaml.components.ThresholdOrientation;
import wres.config.yaml.components.ThresholdType;
import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.messages.MessageFactory;
import wres.config.MetricConstants;
import wres.datamodel.space.FeatureGroup;
import wres.datamodel.space.FeatureTuple;
import wres.datamodel.thresholds.ThresholdsByMetric.Builder;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.GeometryGroup;
import wres.statistics.generated.GeometryTuple;

/**
 * Tests the {@link ThresholdsByMetricAndFeature}.
 * @author James Brown
 */

class ThresholdsByMetricAndFeatureTest
{

    /** Instance to test. */
    private ThresholdsByMetricAndFeature testInstance;

    /** Feature tuple. */
    private FeatureTuple featureTuple;

    @BeforeEach
    void runBeforeEachTest()
    {
        // Value thresholds
        Map<MetricConstants, Set<ThresholdOuter>> values = new EnumMap<>( MetricConstants.class );
        values.put( MetricConstants.MEAN_ERROR,
                    new HashSet<>( Arrays.asList( ThresholdOuter.of( OneOrTwoDoubles.of( 0.2 ),
                                                                     ThresholdOperator.GREATER,
                                                                     ThresholdOrientation.LEFT ) ) ) );

        ThresholdsByMetric thresholdsByMetric = new Builder().addThresholds( values, ThresholdType.VALUE )
                                                             .build();

        Geometry geometry = MessageFactory.getGeometry( "a" );
        GeometryTuple geoTuple = MessageFactory.getGeometryTuple( geometry, geometry, null );
        this.featureTuple = FeatureTuple.of( geoTuple );

        Map<FeatureTuple, ThresholdsByMetric> thresholds = Map.of( this.featureTuple,
                                                                   thresholdsByMetric );

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
                                                                     ThresholdOperator.GREATER,
                                                                     ThresholdOrientation.LEFT ) ) ) );

        ThresholdsByMetric thresholdsByMetric = new Builder().addThresholds( values, ThresholdType.VALUE )
                                                             .build();

        Map<FeatureTuple, ThresholdsByMetric> expected =
                Map.of( this.featureTuple,
                        thresholdsByMetric );

        assertEquals( expected, actual );
    }

    @Test
    void testGetThresholdsByMetricAndFeatureByFeatureGroup()
    {
        GeometryGroup geoGroup = MessageFactory.getGeometryGroup( this.featureTuple );
        FeatureGroup featureGroup = FeatureGroup.of( geoGroup );
        ThresholdsByMetricAndFeature actual = this.testInstance.getThresholdsByMetricAndFeature( featureGroup );
        assertEquals( this.testInstance, actual );
    }

}
