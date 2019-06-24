package wres.datamodel.statistics;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.datamodel.MetricConstants;
import wres.datamodel.sampledata.DatasetIdentifier;
import wres.datamodel.sampledata.Location;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.sampledata.SampleMetadata;

/**
 * Tests the {@link PairedStatistic}.
 * 
 * @author james.brown@hydrosolveDataFactory.com
 */
public final class PairedStatisticTest
{

    private static final String FIRST_TIME = "1985-01-01T00:00:00Z";

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    /**
     * Location for testing.
     */

    private final Location l1 = Location.of( "A" );

    /**
     * Metadata for testing.
     */

    private final StatisticMetadata m1 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                                  DatasetIdentifier.of( l1,
                                                                                                        "B",
                                                                                                        "C" ) ),
                                                               10,
                                                               MeasurementUnit.of(),
                                                               MetricConstants.TIME_TO_PEAK_ERROR,
                                                               MetricConstants.MAIN );

    /**
     * Constructs a {@link PairedStatistic} and tests for equality with another {@link PairedStatistic}.
     */

    @Test
    public void testEquals()
    {
        final Location l2 = Location.of( "A" );
        final StatisticMetadata m2 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                              DatasetIdentifier.of( l2,
                                                                                                    "B",
                                                                                                    "C" ) ),
                                                           11,
                                                           MeasurementUnit.of(),
                                                           MetricConstants.TIME_TO_PEAK_ERROR,
                                                           MetricConstants.MAIN );
        final Location l3 = Location.of( "B" );
        final StatisticMetadata m3 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                              DatasetIdentifier.of( l3,
                                                                                                    "B",
                                                                                                    "C" ) ),
                                                           10,
                                                           MeasurementUnit.of(),
                                                           MetricConstants.TIME_TO_PEAK_ERROR,
                                                           MetricConstants.MAIN );
        List<Pair<Instant, Duration>> input = new ArrayList<>();
        input.add( Pair.of( Instant.parse( FIRST_TIME ), Duration.ofHours( 1 ) ) );
        final PairedStatistic<Instant, Duration> s = PairedStatistic.of( input, m1 );
        final PairedStatistic<Instant, Duration> t = PairedStatistic.of( input, m1 );
        assertTrue( s.getData().size() == t.getData().size() );

        // Iterate the pairs
        for ( Pair<Instant, Duration> next : s )
        {
            assertTrue( t.getData().contains( next ) );
        }

        assertTrue( s.equals( t ) );
        assertNotEquals( null, s );
        assertNotEquals( Double.valueOf( 1.0 ), s );
        List<Pair<Instant, Duration>> inputSecond = new ArrayList<>();
        inputSecond.add( Pair.of( Instant.parse( FIRST_TIME ), Duration.ofHours( 2 ) ) );
        assertTrue( !s.equals( PairedStatistic.of( inputSecond, m1 ) ) );
        assertTrue( !s.equals( PairedStatistic.of( input, m2 ) ) );
        List<Pair<Instant, Duration>> inputThird = new ArrayList<>();
        inputThird.add( Pair.of( Instant.parse( FIRST_TIME ), Duration.ofHours( 1 ) ) );
        final PairedStatistic<Instant, Duration> q = PairedStatistic.of( inputThird, m2 );
        final PairedStatistic<Instant, Duration> r = PairedStatistic.of( inputThird, m3 );
        assertTrue( !s.equals( q ) );
        assertTrue( q.equals( q ) );
        assertTrue( !q.equals( s ) );
        assertTrue( !q.equals( r ) );
    }

    /**
     * Constructs a {@link PairedStatistic} and checks the {@link PairedStatistic#toString()} representation.
     */

    @Test
    public void testToString()
    {
        List<Pair<Instant, Duration>> input = new ArrayList<>();
        input.add( Pair.of( Instant.parse( FIRST_TIME ), Duration.ofHours( 1 ) ) );
        input.add( Pair.of( Instant.parse( FIRST_TIME ), Duration.ofHours( 1 ) ) );
        final PairedStatistic<Instant, Duration> s = PairedStatistic.of( input, m1 );
        final PairedStatistic<Instant, Duration> t = PairedStatistic.of( input, m1 );
        assertTrue( "Expected equal string representations.", s.toString().equals( t.toString() ) );
    }

    /**
     * Constructs a {@link PairedStatistic} and checks the {@link PairedStatistic#getMetadata()}.
     */

    @Test
    public void testGetMetadata()
    {

        final Location l2 = Location.of( "B" );
        final StatisticMetadata m2 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                              DatasetIdentifier.of( l2,
                                                                                                    "B",
                                                                                                    "C" ) ),
                                                           10,
                                                           MeasurementUnit.of(),
                                                           MetricConstants.TIME_TO_PEAK_ERROR,
                                                           MetricConstants.MAIN );
        List<Pair<Instant, Duration>> inputThird = new ArrayList<>();
        inputThird.add( Pair.of( Instant.parse( FIRST_TIME ), Duration.ofHours( 1 ) ) );
        final PairedStatistic<Instant, Duration> q = PairedStatistic.of( inputThird, m1 );
        final PairedStatistic<Instant, Duration> r = PairedStatistic.of( inputThird, m2 );
        assertTrue( "Unequal metadata.", !q.getMetadata().equals( r.getMetadata() ) );
    }

    /**
     * Constructs a {@link PairedStatistic} and checks the {@link PairedStatistic#hashCode()}.
     */

    @Test
    public void testHashCode()
    {

        final Location l2 = Location.of( "A" );
        final StatisticMetadata m2 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                              DatasetIdentifier.of( l2,
                                                                                                    "B",
                                                                                                    "C" ) ),
                                                           10,
                                                           MeasurementUnit.of(),
                                                           MetricConstants.TIME_TO_PEAK_ERROR,
                                                           MetricConstants.MAIN );
        final Location l3 = Location.of( "B" );
        final StatisticMetadata m3 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                              DatasetIdentifier.of( l3,
                                                                                                    "B",
                                                                                                    "C" ) ),
                                                           10,
                                                           MeasurementUnit.of(),
                                                           MetricConstants.TIME_TO_PEAK_ERROR,
                                                           MetricConstants.MAIN );
        List<Pair<Instant, Duration>> inputThird = new ArrayList<>();
        inputThird.add( Pair.of( Instant.parse( FIRST_TIME ), Duration.ofHours( 1 ) ) );
        final PairedStatistic<Instant, Duration> q = PairedStatistic.of( inputThird, m1 );
        final PairedStatistic<Instant, Duration> r = PairedStatistic.of( inputThird, m2 );
        final PairedStatistic<Instant, Duration> s = PairedStatistic.of( inputThird, m3 );
        assertTrue( q.hashCode() == r.hashCode() );
        assertTrue( q.hashCode() != s.hashCode() );
    }

    @Test
    public void testExceptionOnConstructionWithNullData()
    {
        exception.expect( StatisticException.class );

        PairedStatistic.of( null, m1 );
    }

    @Test
    public void testExceptionOnConstructionWithNullMetadata()
    {
        List<Pair<Instant, Duration>> input = new ArrayList<>();
        input.add( Pair.of( Instant.parse( FIRST_TIME ), Duration.ofHours( 1 ) ) );

        exception.expect( StatisticException.class );

        PairedStatistic.of( input, null );

    }

    @Test
    public void testExceptionOnConstructionWithNullPair()
    {
        List<Pair<Instant, Duration>> input = new ArrayList<>();
        input.add( Pair.of( Instant.parse( FIRST_TIME ), Duration.ofHours( 1 ) ) );
        input.add( null );

        exception.expect( StatisticException.class );

        PairedStatistic.of( input, m1 );

    }

    @Test
    public void testExceptionOnConstructionWithNullLeftSide()
    {
        List<Pair<Instant, Duration>> input = new ArrayList<>();
        input.add( Pair.of( Instant.parse( FIRST_TIME ), Duration.ofHours( 1 ) ) );

        input.add( Pair.of( null, Duration.ofHours( 1 ) ) );

        exception.expect( StatisticException.class );

        PairedStatistic.of( input, m1 );

    }

    @Test
    public void testExceptionOnConstructionWithNullRightSide()
    {
        List<Pair<Instant, Duration>> input = new ArrayList<>();
        input.add( Pair.of( Instant.parse( FIRST_TIME ), Duration.ofHours( 1 ) ) );

        input.add( Pair.of( Instant.parse( FIRST_TIME ), null ) );


        exception.expect( StatisticException.class );

        PairedStatistic.of( input, m1 );

    }

}
