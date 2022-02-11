package wres.vis.data;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.temporal.ChronoUnit;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;

import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.statistics.DoubleScoreStatisticOuter.DoubleScoreComponentOuter;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.datamodel.time.TimeWindowOuter;
import wres.statistics.generated.TimeWindow;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent.ComponentName;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

/**
 * Tests the {@link ScoreByThresholdAndLead}.
 * @author James Brown
 */

class ScoreByThresholdAndLeadTest
{
    /** An instance to test. */
    private ScoreByThresholdAndLead score;

    @BeforeEach
    void runBeforeEachTest()
    {
        DoubleScoreMetricComponent metric = DoubleScoreMetricComponent.newBuilder()
                                                                      .setName( ComponentName.MAIN )
                                                                      .build();

        DoubleScoreStatisticComponent scoreOne = DoubleScoreStatisticComponent.newBuilder()
                                                                              .setMetric( metric )
                                                                              .setValue( 2.3 )
                                                                              .build();

        DoubleScoreStatisticComponent scoreTwo = DoubleScoreStatisticComponent.newBuilder()
                                                                              .setMetric( metric )
                                                                              .setValue( 2.7 )
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

        ThresholdOuter first = ThresholdOuter.of( OneOrTwoDoubles.of( 23.0 ),
                                                  Operator.GREATER,
                                                  ThresholdDataType.LEFT );
        ThresholdOuter second = ThresholdOuter.of( OneOrTwoDoubles.of( 29.0 ),
                                                   Operator.GREATER,
                                                   ThresholdDataType.LEFT );
        PoolMetadata metaOne =
                PoolMetadata.of( PoolMetadata.of(), TimeWindowOuter.of( timeWindow ), OneOrTwoThresholds.of( first ) );
        PoolMetadata metaTwo =
                PoolMetadata.of( PoolMetadata.of(), TimeWindowOuter.of( timeWindow ), OneOrTwoThresholds.of( second ) );

        DoubleScoreComponentOuter outerScoreOne = DoubleScoreComponentOuter.of( scoreOne, metaOne );
        DoubleScoreComponentOuter outerScoreTwo = DoubleScoreComponentOuter.of( scoreTwo, metaTwo );

        this.score = ScoreByThresholdAndLead.of( List.of( outerScoreOne, outerScoreTwo ),
                                                 ChronoUnit.SECONDS );
    }

    @Test
    void testGetItemCount()
    {
        assertEquals( 2, this.score.getItemCount( 0 ) );
    }

    @Test
    void testGetSeriesCount()
    {
        assertEquals( 1, this.score.getSeriesCount() );
    }

    @Test
    void testGetX()
    {
        assertEquals( 29.0, this.score.getX( 0, 1 ) );
    }

    @Test
    void testGetY()
    {
        assertEquals( 2.3, this.score.getY( 0, 0 ) );
    }

    @Test
    void testGetSeriesKey()
    {
        assertEquals( "33", this.score.getSeriesKey( 0 ) );
    }
}
