package wres.datamodel.statistics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import com.google.protobuf.Timestamp;

import wres.datamodel.DatasetIdentifier;
import wres.datamodel.FeatureKey;
import wres.datamodel.FeatureTuple;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.sampledata.SampleMetadata;
import wres.statistics.generated.DurationDiagramMetric;
import wres.statistics.generated.DurationDiagramStatistic;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.DurationDiagramStatistic.PairOfInstantAndDuration;

/**
 * Tests the {@link DurationDiagramStatisticOuter}.
 * 
 * @author james.brown@hydrosolveDataFactory.com
 */
public final class DurationDiagramStatisticOuterTest
{

    /**
     * Default instance for testing.
     */

    private DurationDiagramStatistic defaultInstance;

    @Before
    public void runBeforeEachTest()
    {
        DurationDiagramMetric metric = DurationDiagramMetric.newBuilder()
                                                            .setName( MetricName.TIME_TO_PEAK_ERROR )
                                                            .build();

        PairOfInstantAndDuration one = PairOfInstantAndDuration.newBuilder()
                                                               .setTime( Timestamp.newBuilder()
                                                                                  .setSeconds( 123456 )
                                                                                  .setNanos( 789 ) )
                                                               .setDuration( com.google.protobuf.Duration.newBuilder()
                                                                                                         .setSeconds( 13 ) )
                                                               .build();

        this.defaultInstance = DurationDiagramStatistic.newBuilder()
                                                       .setMetric( metric )
                                                       .addStatistics( one )
                                                       .build();
    }

    /**
     * Location for testing.
     */

    private final FeatureKey l1 = FeatureKey.of( "A" );

    /**
     * Metadata for testing.
     */

    private final SampleMetadata m1 = SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                         DatasetIdentifier.of( new FeatureTuple( l1, l1, l1 ),
                                                                               "B",
                                                                               "C" ) );

    /**
     * Constructs a {@link DurationDiagramStatisticOuter} and tests for equality with another 
     * {@link DurationDiagramStatisticOuter}.
     */

    @Test
    public void testEquals()
    {
        FeatureKey l2 = FeatureKey.of( "A" );
        SampleMetadata m2 = SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                               DatasetIdentifier.of( new FeatureTuple( l2, l2, l2),
                                                                     "B",
                                                                     "C" ) );
        FeatureKey l3 = FeatureKey.of( "B" );
        SampleMetadata m3 = SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                               DatasetIdentifier.of( new FeatureTuple( l3, l3, l3),
                                                                     "B",
                                                                     "C" ) );

        DurationDiagramStatisticOuter one = DurationDiagramStatisticOuter.of( this.defaultInstance, m2 );

        assertEquals( one, one );

        assertNotEquals( null, one );

        assertNotEquals( Double.valueOf( 1.0 ), one );

        DurationDiagramMetric metric = DurationDiagramMetric.newBuilder()
                                                            .setName( MetricName.TIME_TO_PEAK_ERROR )
                                                            .build();

        PairOfInstantAndDuration first = PairOfInstantAndDuration.newBuilder()
                                                                 .setTime( Timestamp.newBuilder()
                                                                                    .setSeconds( 123456 )
                                                                                    .setNanos( 689 ) )
                                                                 .setDuration( com.google.protobuf.Duration.newBuilder()
                                                                                                           .setSeconds( 13 ) )
                                                                 .build();

        DurationDiagramStatistic next = DurationDiagramStatistic.newBuilder()
                                                                .setMetric( metric )
                                                                .addStatistics( first )
                                                                .build();

        DurationDiagramStatisticOuter another = DurationDiagramStatisticOuter.of( next, this.m1 );

        assertNotEquals( one, another );
        assertNotEquals( one, DurationDiagramStatisticOuter.of( this.defaultInstance, m3 ) );


        PairOfInstantAndDuration second = PairOfInstantAndDuration.newBuilder()
                                                                  .setTime( Timestamp.newBuilder()
                                                                                     .setSeconds( 123457 )
                                                                                     .setNanos( 689 ) )
                                                                  .setDuration( com.google.protobuf.Duration.newBuilder()
                                                                                                            .setSeconds( 13 ) )
                                                                  .build();

        DurationDiagramStatistic anotherNext = DurationDiagramStatistic.newBuilder()
                                                                       .setMetric( metric )
                                                                       .addStatistics( second )
                                                                       .build();

        DurationDiagramStatisticOuter yetAnother = DurationDiagramStatisticOuter.of( anotherNext, m2 );
        DurationDiagramStatisticOuter oneMore = DurationDiagramStatisticOuter.of( anotherNext, m3 );

        assertNotEquals( another, yetAnother );
        assertNotEquals( yetAnother, another );
        assertEquals( yetAnother, yetAnother );
        assertNotEquals( yetAnother, oneMore );
    }

    /**
     * Constructs a {@link DurationDiagramStatisticOuter} and checks the {@link DurationDiagramStatisticOuter#toString()} representation.
     */

    @Test
    public void testToString()
    {
        String expectedStart = "DurationDiagramStatisticOuter[";
        String expectedContainsMetric = "metric=TIME_TO_PEAK_ERROR";
        String expectedContainsStatistic = "statistic=";
        String expectedContainsInstantAndDuration = "1970-01-02T10:17:36.000000789Z,PT13S";
        String expectedContainsMetadata = "metadata=SampleMetadata[";
        String expectedContainsMeasurementUnit = "measurementUnit=CMS";

        String m1ToString = DurationDiagramStatisticOuter.of( this.defaultInstance, this.m1 ).toString();
        assertTrue( m1ToString.startsWith( expectedStart ) );
        assertTrue( m1ToString.contains( expectedContainsMetric ) );
        assertTrue( m1ToString.contains( expectedContainsStatistic ) );
        assertTrue( m1ToString.contains( expectedContainsInstantAndDuration ) );
        assertTrue( m1ToString.contains( expectedContainsMetadata ) );
        assertTrue( m1ToString.contains( expectedContainsMeasurementUnit ) );
    }

    /**
     * Constructs a {@link DurationDiagramStatisticOuter} and checks the {@link DurationDiagramStatisticOuter#getMetadata()}.
     */

    @Test
    public void testGetMetadata()
    {

        FeatureKey l2 = FeatureKey.of( "B" );
        SampleMetadata m2 = SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                               DatasetIdentifier.of( new FeatureTuple( l2, l2, l2),
                                                                     "B",
                                                                     "C" ) );

        assertEquals( m2, DurationDiagramStatisticOuter.of( this.defaultInstance, m2 ).getMetadata() );
    }

    /**
     * Constructs a {@link DurationDiagramStatisticOuter} and checks the {@link DurationDiagramStatisticOuter#hashCode()}.
     */

    @Test
    public void testHashCode()
    {
        FeatureKey l2 = FeatureKey.of( "A" );
        SampleMetadata m2 = SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                               DatasetIdentifier.of( new FeatureTuple( l2, l2, l2),
                                                                     "B",
                                                                     "C" ) );

        DurationDiagramStatisticOuter anInstance = DurationDiagramStatisticOuter.of( this.defaultInstance, m2 );

        // Equality and consistency
        for ( int i = 0; i < 100; i++ )
        {
            assertEquals( anInstance.hashCode(), anInstance.hashCode() );
        }

    }

    @Test
    public void testExceptionOnConstructionWithNullData()
    {
        assertThrows( StatisticException.class, () -> DurationDiagramStatisticOuter.of( null, this.m1 ) );
    }

    @Test
    public void testExceptionOnConstructionWithNullMetadata()
    {
        assertThrows( StatisticException.class, () -> DurationDiagramStatisticOuter.of( this.defaultInstance, null ) );
    }

}
