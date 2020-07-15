package wres.datamodel.statistics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import wres.datamodel.DatasetIdentifier;
import wres.datamodel.MetricConstants;
import wres.datamodel.sampledata.Location;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.statistics.DoubleScoreStatisticOuter.DoubleScoreComponentOuter;
import wres.statistics.generated.DoubleScoreMetric;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent.ComponentName;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

/**
 * Tests the {@link ScoreStatistic}.
 * 
 * @author james.brown@hydrosolveDataFactory.com
 */
public final class ScoreStatisticOuterTest
{

    /**
     * Instant to use when testing.
     */

    private final DoubleScoreStatistic one =
            DoubleScoreStatistic.newBuilder()
                                .setMetric( DoubleScoreMetric.newBuilder().setName( MetricName.MEAN_ERROR ) )
                                .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                             .setValue( 1.0 )
                                                                             .setName( ComponentName.MAIN ) )
                                .build();

    /**
     * Constructs a {@link DoubleScoreStatisticOuter} and tests for equality with another {@link DoubleScoreStatisticOuter}.
     */

    @Test
    public void testEquals()
    {
        Location l1 = Location.of( "A" );
        StatisticMetadata m1 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                        DatasetIdentifier.of( l1,
                                                                                              "B",
                                                                                              "C" ) ),
                                                     10,
                                                     MeasurementUnit.of(),
                                                     MetricConstants.MEAN_ERROR,
                                                     MetricConstants.MAIN );
        Location l2 = Location.of( "A" );
        StatisticMetadata m2 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                        DatasetIdentifier.of( l2,
                                                                                              "B",
                                                                                              "C" ) ),
                                                     11,
                                                     MeasurementUnit.of(),
                                                     MetricConstants.MEAN_ERROR,
                                                     MetricConstants.MAIN );
        Location l3 = Location.of( "B" );
        StatisticMetadata m3 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                        DatasetIdentifier.of( l3,
                                                                                              "B",
                                                                                              "C" ) ),
                                                     10,
                                                     MeasurementUnit.of(),
                                                     MetricConstants.MEAN_ERROR,
                                                     MetricConstants.MAIN );

        ScoreStatistic<DoubleScoreStatistic, DoubleScoreComponentOuter> s =
                DoubleScoreStatisticOuter.of( this.one, m1 );
        ScoreStatistic<DoubleScoreStatistic, DoubleScoreComponentOuter> t =
                DoubleScoreStatisticOuter.of( this.one, m1 );
        assertTrue( s.equals( t ) );
        assertNotEquals( null, s );
        assertNotEquals( Double.valueOf( 1.0 ), s );

        DoubleScoreStatistic two =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( DoubleScoreMetric.newBuilder().setName( MetricName.MEAN_ERROR ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 2.0 )
                                                                                 .setName( ComponentName.MAIN ) )
                                    .build();

        assertNotEquals( DoubleScoreStatisticOuter.of( two, m1 ), s );
        assertNotEquals( DoubleScoreStatisticOuter.of( this.one, m2 ), s );
        ScoreStatistic<DoubleScoreStatistic, DoubleScoreComponentOuter> q =
                DoubleScoreStatisticOuter.of( this.one, m2 );
        ScoreStatistic<DoubleScoreStatistic, DoubleScoreComponentOuter> r =
                DoubleScoreStatisticOuter.of( this.one, m3 );
        assertNotEquals( q, s );
        assertEquals( q, q );
        assertNotEquals( q, r );
    }

    /**
     * Constructs a {@link DoubleScoreStatisticOuter} and checks the {@link DoubleScoreStatisticOuter#toString()} representation.
     */

    @Test
    public void testToString()
    {
        Location l1 = Location.of( "A" );
        StatisticMetadata m1 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                        DatasetIdentifier.of( l1,
                                                                                              "B",
                                                                                              "C" ) ),
                                                     10,
                                                     MeasurementUnit.of(),
                                                     MetricConstants.MEAN_ERROR,
                                                     MetricConstants.MAIN );

        ScoreStatistic<DoubleScoreStatistic, DoubleScoreComponentOuter> s =
                DoubleScoreStatisticOuter.of( this.one, m1 );
        ScoreStatistic<DoubleScoreStatistic, DoubleScoreComponentOuter> t =
                DoubleScoreStatisticOuter.of( this.one, m1 );
        assertEquals( "Expected equal string representations.", s.toString(), t.toString() );
    }

    /**
     * Constructs a {@link DoubleScoreStatisticOuter} and checks the {@link DoubleScoreStatisticOuter#getMetadata()}.
     */

    @Test
    public void testGetMetadata()
    {
        Location l1 = Location.of( "A" );
        StatisticMetadata m1 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                        DatasetIdentifier.of( l1,
                                                                                              "B",
                                                                                              "C" ) ),
                                                     10,
                                                     MeasurementUnit.of(),
                                                     MetricConstants.MEAN_ERROR,
                                                     MetricConstants.MAIN );
        Location l2 = Location.of( "B" );
        StatisticMetadata m2 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                        DatasetIdentifier.of( l2,
                                                                                              "B",
                                                                                              "C" ) ),
                                                     10,
                                                     MeasurementUnit.of(),
                                                     MetricConstants.MEAN_ERROR,
                                                     MetricConstants.MAIN );

        ScoreStatistic<DoubleScoreStatistic, DoubleScoreComponentOuter> q =
                DoubleScoreStatisticOuter.of( this.one, m1 );
        ScoreStatistic<DoubleScoreStatistic, DoubleScoreComponentOuter> r =
                DoubleScoreStatisticOuter.of( this.one, m2 );
        assertNotEquals( "Equal metadata.", q.getMetadata(), r.getMetadata() );
    }

    /**
     * Constructs a {@link DoubleScoreStatisticOuter} and checks the {@link DoubleScoreStatisticOuter#hashCode()}.
     */

    @Test
    public void testHashCode()
    {
        Location l1 = Location.of( "A" );
        StatisticMetadata m1 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                        DatasetIdentifier.of( l1,
                                                                                              "B",
                                                                                              "C" ) ),
                                                     10,
                                                     MeasurementUnit.of(),
                                                     MetricConstants.MEAN_ERROR,
                                                     MetricConstants.MAIN );
        Location l2 = Location.of( "A" );
        StatisticMetadata m2 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                        DatasetIdentifier.of( l2,
                                                                                              "B",
                                                                                              "C" ) ),
                                                     10,
                                                     MeasurementUnit.of(),
                                                     MetricConstants.MEAN_ERROR,
                                                     MetricConstants.MAIN );
        Location l3 = Location.of( "B" );
        StatisticMetadata m3 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                        DatasetIdentifier.of( l3,
                                                                                              "B",
                                                                                              "C" ) ),
                                                     10,
                                                     MeasurementUnit.of(),
                                                     MetricConstants.MEAN_ERROR,
                                                     MetricConstants.MAIN );

        ScoreStatistic<DoubleScoreStatistic, DoubleScoreComponentOuter> q =
                DoubleScoreStatisticOuter.of( this.one, m1 );
        ScoreStatistic<DoubleScoreStatistic, DoubleScoreComponentOuter> r =
                DoubleScoreStatisticOuter.of( this.one, m2 );
        assertEquals( "Expected equal hash codes.", q.hashCode(), r.hashCode() );
    }

}
