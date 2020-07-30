package wres.datamodel.statistics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import com.google.protobuf.Timestamp;

import wres.datamodel.FeatureKey;
import wres.datamodel.FeatureTuple;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.sampledata.SampleMetadata;
import wres.statistics.generated.DurationDiagramMetric;
import wres.statistics.generated.DurationDiagramStatistic;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.Pool;
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

    /**
     * Metadata for testing.
     */

    private SampleMetadata metadata;

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

        FeatureKey feature = FeatureKey.of( "A" );

        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightDataName( "B" )
                                          .setBaselineDataName( "C" )
                                          .setMeasurementUnit( "CMS" )
                                          .build();


        Pool pool = MessageFactory.parse( new FeatureTuple( feature, feature, feature ),
                                          null,
                                          null,
                                          null,
                                          false );

        this.metadata = SampleMetadata.of( evaluation, pool );
    }

    /**
     * Constructs a {@link DurationDiagramStatisticOuter} and tests for equality with another 
     * {@link DurationDiagramStatisticOuter}.
     */

    @Test
    public void testEquals()
    {
        FeatureKey l2 = FeatureKey.of( "A" );

        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightDataName( "B" )
                                          .setBaselineDataName( "C" )
                                          .setMeasurementUnit( "CMS" )
                                          .build();


        Pool pool = MessageFactory.parse( new FeatureTuple( l2, l2, l2 ),
                                          null,
                                          null,
                                          null,
                                          false );

        SampleMetadata m2 = SampleMetadata.of( evaluation, pool );
        FeatureKey l3 = FeatureKey.of( "B" );

        Pool poolTwo = MessageFactory.parse( new FeatureTuple( l3, l3, l3 ),
                                             null,
                                             null,
                                             null,
                                             false );

        SampleMetadata m3 = SampleMetadata.of( evaluation, poolTwo );

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

        DurationDiagramStatisticOuter another = DurationDiagramStatisticOuter.of( next, this.metadata );

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

        String m1ToString = DurationDiagramStatisticOuter.of( this.defaultInstance, this.metadata ).toString();
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
        assertEquals( SampleMetadata.of(),
                      DurationDiagramStatisticOuter.of( this.defaultInstance,
                                                        SampleMetadata.of() )
                                                   .getMetadata() );
    }

    /**
     * Constructs a {@link DurationDiagramStatisticOuter} and checks the {@link DurationDiagramStatisticOuter#hashCode()}.
     */

    @Test
    public void testHashCode()
    {
        FeatureKey l2 = FeatureKey.of( "A" );

        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightDataName( "B" )
                                          .setBaselineDataName( "C" )
                                          .setMeasurementUnit( "CMS" )
                                          .build();

        Pool pool = MessageFactory.parse( new FeatureTuple( l2, l2, l2 ),
                                          null,
                                          null,
                                          null,
                                          false );

        SampleMetadata m2 = SampleMetadata.of( evaluation, pool );

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
        assertThrows( StatisticException.class, () -> DurationDiagramStatisticOuter.of( null, this.metadata ) );
    }

    @Test
    public void testExceptionOnConstructionWithNullMetadata()
    {
        assertThrows( StatisticException.class, () -> DurationDiagramStatisticOuter.of( this.defaultInstance, null ) );
    }

}
