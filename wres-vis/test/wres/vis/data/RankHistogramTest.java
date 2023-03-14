package wres.vis.data;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;

import wres.config.MetricConstants.MetricDimension;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.statistics.DiagramStatisticOuter;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.datamodel.time.TimeWindowOuter;
import wres.statistics.generated.DiagramMetric;
import wres.statistics.generated.DiagramStatistic;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.TimeWindow;
import wres.statistics.generated.DiagramMetric.DiagramMetricComponent;
import wres.statistics.generated.DiagramMetric.DiagramMetricComponent.DiagramComponentName;
import wres.statistics.generated.DiagramStatistic.DiagramStatisticComponent;

/**
 * Tests the {@link RankHistogram}.
 * @author James Brown
 */

class RankHistogramTest
{
    /** An instance to test. */
    private Diagram diagram;

    @BeforeEach
    void runBeforeEachTest()
    {
        DiagramMetricComponent rankOrder = DiagramMetricComponent.newBuilder()
                                                                 .setName( DiagramComponentName.RANK_ORDER )
                                                                 .setMinimum( 1 )
                                                                 .setMaximum( Double.POSITIVE_INFINITY )
                                                                 .setUnits( "COUNT" )
                                                                 .build();

        DiagramMetricComponent observedRelativeFrequency = DiagramMetricComponent.newBuilder()
                                                                                 .setName( DiagramComponentName.OBSERVED_RELATIVE_FREQUENCY )
                                                                                 .setMinimum( 0 )
                                                                                 .setMaximum( 1 )
                                                                                 .setUnits( "PROBABILITY" )
                                                                                 .build();

        DiagramMetric metric = DiagramMetric.newBuilder()
                                            .addComponents( rankOrder )
                                            .addComponents( observedRelativeFrequency )
                                            .setName( MetricName.RANK_HISTOGRAM )
                                            .build();

        List<Double> ranks = new ArrayList<>();
        List<Double> cumProbs = new ArrayList<>();

        for ( int i = 1; i < 11; i++ )
        {
            double next = i;
            ranks.add( next );
            cumProbs.add( i / 10.0 );
        }

        DiagramStatisticComponent x =
                DiagramStatisticComponent.newBuilder()
                                         .setMetric( rankOrder )
                                         .addAllValues( ranks )
                                         .build();

        DiagramStatisticComponent y =
                DiagramStatisticComponent.newBuilder()
                                         .setMetric( observedRelativeFrequency )
                                         .addAllValues( cumProbs )
                                         .build();

        DiagramStatistic diagram = DiagramStatistic.newBuilder()
                                                   .setMetric( metric )
                                                   .addStatistics( x )
                                                   .addStatistics( y )
                                                   .build();

        // Set metadata with minimum content
        TimeWindow timeWindow = TimeWindow.newBuilder()
                                          .setEarliestLeadDuration( Duration.newBuilder().setSeconds( 0 ) )
                                          .setLatestLeadDuration( Duration.newBuilder().setSeconds( 33 ) )
                                          .setEarliestReferenceTime( Timestamp.getDefaultInstance() )
                                          .setLatestReferenceTime( Timestamp.getDefaultInstance() )
                                          .setEarliestValidTime( Timestamp.getDefaultInstance() )
                                          .setLatestValidTime( Timestamp.getDefaultInstance() )
                                          .build();

        OneOrTwoThresholds thresholds = OneOrTwoThresholds.of( ThresholdOuter.ALL_DATA );
        PoolMetadata meta = PoolMetadata.of( PoolMetadata.of(), TimeWindowOuter.of( timeWindow ), thresholds );

        DiagramStatisticOuter outerDiagram = DiagramStatisticOuter.of( diagram, meta );

        this.diagram = Diagram.ofLeadThreshold( List.of( outerDiagram ),
                                                MetricDimension.RANK_ORDER,
                                                MetricDimension.OBSERVED_RELATIVE_FREQUENCY,
                                                ChronoUnit.SECONDS );
    }

    @Test
    void testGetItemCount()
    {
        assertEquals( 10, this.diagram.getItemCount( 0 ) );
    }

    @Test
    void testGetSeriesCount()
    {
        assertEquals( 1, this.diagram.getSeriesCount() );
    }

    @Test
    void testGetX()
    {
        assertEquals( 5.0, this.diagram.getX( 0, 4 ) );
    }

    @Test
    void testGetY()
    {
        assertEquals( 0.7, this.diagram.getY( 0, 6 ) );
    }

    @Test
    void testGetSeriesKey()
    {
        assertEquals( ThresholdOuter.ALL_DATA.toString(), this.diagram.getSeriesKey( 0 ) );
    }
}
