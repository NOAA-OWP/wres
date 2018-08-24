package wres.datamodel.statistics;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import wres.datamodel.MetricConstants;
import wres.datamodel.metadata.DatasetIdentifier;
import wres.datamodel.metadata.Location;
import wres.datamodel.metadata.MeasurementUnit;
import wres.datamodel.metadata.StatisticMetadata;
import wres.datamodel.statistics.StatisticException;
import wres.datamodel.statistics.PairedStatistic;

/**
 * Tests the {@link PairedStatistic}.
 * 
 * @author james.brown@hydrosolveDataFactory.com
 */
public final class PairedStatisticTest
{

    /**
     * Constructs a {@link PairedStatistic} and tests for equality with another {@link PairedStatistic}.
     */

    @Test
    public void test1Equals()
    {
        final Location l1 = Location.of( "A" );
        final StatisticMetadata m1 = StatisticMetadata.of( 10,
                                                                 MeasurementUnit.of(),
                                                                 MeasurementUnit.of( "CMS" ),
                                                                 MetricConstants.TIME_TO_PEAK_ERROR,
                                                                 MetricConstants.MAIN,
                                                                 DatasetIdentifier.of( l1,
                                                                                       "B",
                                                                                       "C" ) );
        final Location l2 = Location.of( "A" );
        final StatisticMetadata m2 = StatisticMetadata.of( 11,
                                                                 MeasurementUnit.of(),
                                                                 MeasurementUnit.of( "CMS" ),
                                                                 MetricConstants.TIME_TO_PEAK_ERROR,
                                                                 MetricConstants.MAIN,
                                                                 DatasetIdentifier.of( l2,
                                                                                       "B",
                                                                                       "C" ) );
        final Location l3 = Location.of( "B" );
        final StatisticMetadata m3 = StatisticMetadata.of( 10,
                                                                 MeasurementUnit.of(),
                                                                 MeasurementUnit.of( "CMS" ),
                                                                 MetricConstants.TIME_TO_PEAK_ERROR,
                                                                 MetricConstants.MAIN,
                                                                 DatasetIdentifier.of( l3,
                                                                                       "B",
                                                                                       "C" ) );
        List<Pair<Instant, Duration>> input = new ArrayList<>();
        input.add( Pair.of( Instant.parse( "1985-01-01T00:00:00Z" ), Duration.ofHours( 1 ) ) );
        final PairedStatistic<Instant, Duration> s = PairedStatistic.of( input, m1 );
        final PairedStatistic<Instant, Duration> t = PairedStatistic.of( input, m1 );
        assertTrue( "Expected outputs of equal size", s.getData().size() == t.getData().size() );
        // Iterate the pairs
        for ( Pair<Instant, Duration> next : s )
        {
            assertTrue( "Expected equal pairs.", t.getData().contains( next ) );
        }
        assertTrue( "Expected equal outputs.", s.equals( t ) );
        assertTrue( "Expected non-equal outputs.", !s.equals( null ) );
        assertTrue( "Expected non-equal outputs.", !s.equals( new Double( 1.0 ) ) );
        List<Pair<Instant, Duration>> inputSecond = new ArrayList<>();
        inputSecond.add( Pair.of( Instant.parse( "1985-01-01T00:00:00Z" ), Duration.ofHours( 2 ) ) );
        assertTrue( "Expected non-equal outputs.", !s.equals( PairedStatistic.of( inputSecond, m1 ) ) );
        assertTrue( "Expected non-equal outputs.", !s.equals( PairedStatistic.of( input, m2 ) ) );
        List<Pair<Instant, Duration>> inputThird = new ArrayList<>();
        inputThird.add( Pair.of( Instant.parse( "1985-01-01T00:00:00Z" ), Duration.ofHours( 1 ) ) );
        final PairedStatistic<Instant, Duration> q = PairedStatistic.of( inputThird, m2 );
        final PairedStatistic<Instant, Duration> r = PairedStatistic.of( inputThird, m3 );
        assertTrue( "Expected non-equal outputs.", !s.equals( q ) );
        assertTrue( "Expected equal outputs.", q.equals( q ) );
        assertTrue( "Expected non-equal outputs.", !q.equals( s ) );
        assertTrue( "Expected non-equal outputs.", !q.equals( r ) );
    }

    /**
     * Constructs a {@link PairedStatistic} and checks the {@link PairedStatistic#toString()} representation.
     */

    @Test
    public void test2ToString()
    {
        final Location l1 = Location.of( "A" );
        final StatisticMetadata m1 = StatisticMetadata.of( 10,
                                                                 MeasurementUnit.of(),
                                                                 MeasurementUnit.of( "CMS" ),
                                                                 MetricConstants.TIME_TO_PEAK_ERROR,
                                                                 MetricConstants.MAIN,
                                                                 DatasetIdentifier.of( l1,
                                                                                       "B",
                                                                                       "C" ) );
        List<Pair<Instant, Duration>> input = new ArrayList<>();
        input.add( Pair.of( Instant.parse( "1985-01-01T00:00:00Z" ), Duration.ofHours( 1 ) ) );
        input.add( Pair.of( Instant.parse( "1985-01-01T00:00:00Z" ), Duration.ofHours( 1 ) ) );
        final PairedStatistic<Instant, Duration> s = PairedStatistic.of( input, m1 );
        final PairedStatistic<Instant, Duration> t = PairedStatistic.of( input, m1 );
        assertTrue( "Expected equal string representations.", s.toString().equals( t.toString() ) );
    }

    /**
     * Constructs a {@link PairedStatistic} and checks the {@link PairedStatistic#getMetadata()}.
     */

    @Test
    public void test3GetMetadata()
    {
        final Location l1 = Location.of( "A" );
        final StatisticMetadata m1 = StatisticMetadata.of( 10,
                                                                 MeasurementUnit.of(),
                                                                 MeasurementUnit.of( "CMS" ),
                                                                 MetricConstants.TIME_TO_PEAK_ERROR,
                                                                 MetricConstants.MAIN,
                                                                 DatasetIdentifier.of( l1,
                                                                                       "B",
                                                                                       "C" ) );
        final Location l2 = Location.of( "B" );
        final StatisticMetadata m2 = StatisticMetadata.of( 10,
                                                                 MeasurementUnit.of(),
                                                                 MeasurementUnit.of( "CMS" ),
                                                                 MetricConstants.TIME_TO_PEAK_ERROR,
                                                                 MetricConstants.MAIN,
                                                                 DatasetIdentifier.of( l2,
                                                                                       "B",
                                                                                       "C" ) );
        List<Pair<Instant, Duration>> inputThird = new ArrayList<>();
        inputThird.add( Pair.of( Instant.parse( "1985-01-01T00:00:00Z" ), Duration.ofHours( 1 ) ) );
        final PairedStatistic<Instant, Duration> q = PairedStatistic.of( inputThird, m1 );
        final PairedStatistic<Instant, Duration> r = PairedStatistic.of( inputThird, m2 );
        assertTrue( "Unequal metadata.", !q.getMetadata().equals( r.getMetadata() ) );
    }

    /**
     * Constructs a {@link PairedStatistic} and checks the {@link PairedStatistic#hashCode()}.
     */

    @Test
    public void test4HashCode()
    {
        final Location l1 = Location.of( "A" );
        final StatisticMetadata m1 = StatisticMetadata.of( 10,
                                                                 MeasurementUnit.of(),
                                                                 MeasurementUnit.of( "CMS" ),
                                                                 MetricConstants.CONTINGENCY_TABLE,
                                                                 MetricConstants.MAIN,
                                                                 DatasetIdentifier.of( l1,
                                                                                       "B",
                                                                                       "C" ) );
        final Location l2 = Location.of( "A" );
        final StatisticMetadata m2 = StatisticMetadata.of( 10,
                                                                 MeasurementUnit.of(),
                                                                 MeasurementUnit.of( "CMS" ),
                                                                 MetricConstants.CONTINGENCY_TABLE,
                                                                 MetricConstants.MAIN,
                                                                 DatasetIdentifier.of( l2,
                                                                                       "B",
                                                                                       "C" ) );
        final Location l3 = Location.of( "B" );
        final StatisticMetadata m3 = StatisticMetadata.of( 10,
                                                                 MeasurementUnit.of(),
                                                                 MeasurementUnit.of( "CMS" ),
                                                                 MetricConstants.CONTINGENCY_TABLE,
                                                                 MetricConstants.MAIN,
                                                                 DatasetIdentifier.of( l3,
                                                                                       "B",
                                                                                       "C" ) );
        List<Pair<Instant, Duration>> inputThird = new ArrayList<>();
        inputThird.add( Pair.of( Instant.parse( "1985-01-01T00:00:00Z" ), Duration.ofHours( 1 ) ) );
        final PairedStatistic<Instant, Duration> q = PairedStatistic.of( inputThird, m1 );
        final PairedStatistic<Instant, Duration> r = PairedStatistic.of( inputThird, m2 );
        final PairedStatistic<Instant, Duration> s = PairedStatistic.of( inputThird, m3 );
        assertTrue( "Expected equal hash codes.", q.hashCode() == r.hashCode() );
        assertTrue( "Expected unequal hash codes.", q.hashCode() != s.hashCode() );
    }

    /**
     * Checks for expected exceptions when constructing a {@link PairedStatistic}.
     */

    @Test
    public void test6Exceptions()
    {
        final Location l1 = Location.of( "A" );
        final StatisticMetadata m1 = StatisticMetadata.of( 10,
                                                                 MeasurementUnit.of(),
                                                                 MeasurementUnit.of( "CMS" ),
                                                                 MetricConstants.CONTINGENCY_TABLE,
                                                                 MetricConstants.MAIN,
                                                                 DatasetIdentifier.of( l1,
                                                                                       "B",
                                                                                       "C" ) );
        List<Pair<Instant, Duration>> input = new ArrayList<>();
        input.add( Pair.of( Instant.parse( "1985-01-01T00:00:00Z" ), Duration.ofHours( 1 ) ) );
        // Null output
        try
        {
            PairedStatistic.of( null, m1 );
            fail( "Expected a checked exception on attempting to construct a paired output with null input." );
        }
        catch ( StatisticException e )
        {
        }
        // Null metadata
        try
        {
            PairedStatistic.of( input, null );
            fail( "Expected a checked exception on attempting to construct a paired output with null metadata." );
        }
        catch ( StatisticException e )
        {
        }
        // Null pair
        try
        {
            input.add( null );
            PairedStatistic.of( input, m1 );
            fail( "Expected a checked exception on attempting to construct a paired output with a null pair." );
        }
        catch ( StatisticException e )
        {
        }
        // Pair with null left
        try
        {
            input.remove( 1 );
            input.add( Pair.of( null, Duration.ofHours( 1 ) ) );
            PairedStatistic.of( input, m1 );
            fail( "Expected a checked exception on attempting to construct a paired output with a pair that has a "
                  + "null left side." );
        }
        catch ( StatisticException e )
        {
        }
        // Pair with null right
        try
        {
            input.remove( 1 );
            input.add( Pair.of( Instant.parse( "1985-01-01T00:00:00Z" ), null ) );
            PairedStatistic.of( input, m1 );
            fail( "Expected a checked exception on attempting to construct a paired output with a pair that has a "
                  + "null right side." );
        }
        catch ( StatisticException e )
        {
        }
    }

}
