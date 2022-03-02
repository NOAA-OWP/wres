package wres.vis.data;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;

import wres.datamodel.metrics.MetricConstants.MetricDimension;
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
 * Tests the {@link Diagram}.
 * @author James Brown
 */

class DiagramTest
{
    /** An instance to test. */
    private Diagram diagram;

    @BeforeEach
    void runBeforeEachTest()
    {
        DiagramMetricComponent observedQuantiles = DiagramMetricComponent.newBuilder()
                                                                         .setName( DiagramComponentName.OBSERVED_QUANTILES )
                                                                         .build();

        DiagramMetricComponent predictedQuantiles = DiagramMetricComponent.newBuilder()
                                                                          .setName( DiagramComponentName.PREDICTED_QUANTILES )
                                                                          .build();

        DiagramMetric metric = DiagramMetric.newBuilder()
                                            .addComponents( observedQuantiles )
                                            .addComponents( predictedQuantiles )
                                            .setName( MetricName.QUANTILE_QUANTILE_DIAGRAM )
                                            .build();

        List<Double> observedQ = new ArrayList<>();
        List<Double> predictedQ = new ArrayList<>();

        for ( int i = 1; i < 11; i++ )
        {
            double next = i;
            observedQ.add( next );
            predictedQ.add( next );
        }

        DiagramStatisticComponent oqs =
                DiagramStatisticComponent.newBuilder()
                                         .setMetric( observedQuantiles )
                                         .addAllValues( observedQ )
                                         .build();

        DiagramStatisticComponent pqs =
                DiagramStatisticComponent.newBuilder()
                                         .setMetric( predictedQuantiles )
                                         .addAllValues( predictedQ )
                                         .build();

        DiagramStatistic diagram = DiagramStatistic.newBuilder()
                                                   .setMetric( metric )
                                                   .addStatistics( oqs )
                                                   .addStatistics( pqs )
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
                                                MetricDimension.PREDICTED_QUANTILES,
                                                MetricDimension.OBSERVED_QUANTILES,
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
        assertEquals( 7.0, this.diagram.getY( 0, 6 ) );
    }

    @Test
    void testGetSeriesKey()
    {
        assertEquals( ThresholdOuter.ALL_DATA.toString(), this.diagram.getSeriesKey( 0 ) );
    }
}
