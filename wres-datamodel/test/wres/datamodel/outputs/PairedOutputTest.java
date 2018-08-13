package wres.datamodel.outputs;

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
import wres.datamodel.metadata.MetricOutputMetadata;

/**
 * Tests the {@link PairedOutput}.
 * 
 * @author james.brown@hydrosolveDataFactory.com
 */
public final class PairedOutputTest
{

    /**
     * Constructs a {@link PairedOutput} and tests for equality with another {@link PairedOutput}.
     */

    @Test
    public void test1Equals()
    {
        final Location l1 = Location.of( "A" );
        final MetricOutputMetadata m1 = MetricOutputMetadata.of( 10,
                                                                 MeasurementUnit.of(),
                                                                 MeasurementUnit.of( "CMS" ),
                                                                 MetricConstants.TIME_TO_PEAK_ERROR,
                                                                 MetricConstants.MAIN,
                                                                 DatasetIdentifier.of( l1,
                                                                                       "B",
                                                                                       "C" ) );
        final Location l2 = Location.of( "A" );
        final MetricOutputMetadata m2 = MetricOutputMetadata.of( 11,
                                                                 MeasurementUnit.of(),
                                                                 MeasurementUnit.of( "CMS" ),
                                                                 MetricConstants.TIME_TO_PEAK_ERROR,
                                                                 MetricConstants.MAIN,
                                                                 DatasetIdentifier.of( l2,
                                                                                       "B",
                                                                                       "C" ) );
        final Location l3 = Location.of( "B" );
        final MetricOutputMetadata m3 = MetricOutputMetadata.of( 10,
                                                                 MeasurementUnit.of(),
                                                                 MeasurementUnit.of( "CMS" ),
                                                                 MetricConstants.TIME_TO_PEAK_ERROR,
                                                                 MetricConstants.MAIN,
                                                                 DatasetIdentifier.of( l3,
                                                                                       "B",
                                                                                       "C" ) );
        List<Pair<Instant, Duration>> input = new ArrayList<>();
        input.add( Pair.of( Instant.parse( "1985-01-01T00:00:00Z" ), Duration.ofHours( 1 ) ) );
        final PairedOutput<Instant, Duration> s = PairedOutput.of( input, m1 );
        final PairedOutput<Instant, Duration> t = PairedOutput.of( input, m1 );
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
        assertTrue( "Expected non-equal outputs.", !s.equals( PairedOutput.of( inputSecond, m1 ) ) );
        assertTrue( "Expected non-equal outputs.", !s.equals( PairedOutput.of( input, m2 ) ) );
        List<Pair<Instant, Duration>> inputThird = new ArrayList<>();
        inputThird.add( Pair.of( Instant.parse( "1985-01-01T00:00:00Z" ), Duration.ofHours( 1 ) ) );
        final PairedOutput<Instant, Duration> q = PairedOutput.of( inputThird, m2 );
        final PairedOutput<Instant, Duration> r = PairedOutput.of( inputThird, m3 );
        assertTrue( "Expected non-equal outputs.", !s.equals( q ) );
        assertTrue( "Expected equal outputs.", q.equals( q ) );
        assertTrue( "Expected non-equal outputs.", !q.equals( s ) );
        assertTrue( "Expected non-equal outputs.", !q.equals( r ) );
    }

    /**
     * Constructs a {@link PairedOutput} and checks the {@link PairedOutput#toString()} representation.
     */

    @Test
    public void test2ToString()
    {
        final Location l1 = Location.of( "A" );
        final MetricOutputMetadata m1 = MetricOutputMetadata.of( 10,
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
        final PairedOutput<Instant, Duration> s = PairedOutput.of( input, m1 );
        final PairedOutput<Instant, Duration> t = PairedOutput.of( input, m1 );
        assertTrue( "Expected equal string representations.", s.toString().equals( t.toString() ) );
    }

    /**
     * Constructs a {@link PairedOutput} and checks the {@link PairedOutput#getMetadata()}.
     */

    @Test
    public void test3GetMetadata()
    {
        final Location l1 = Location.of( "A" );
        final MetricOutputMetadata m1 = MetricOutputMetadata.of( 10,
                                                                 MeasurementUnit.of(),
                                                                 MeasurementUnit.of( "CMS" ),
                                                                 MetricConstants.TIME_TO_PEAK_ERROR,
                                                                 MetricConstants.MAIN,
                                                                 DatasetIdentifier.of( l1,
                                                                                       "B",
                                                                                       "C" ) );
        final Location l2 = Location.of( "B" );
        final MetricOutputMetadata m2 = MetricOutputMetadata.of( 10,
                                                                 MeasurementUnit.of(),
                                                                 MeasurementUnit.of( "CMS" ),
                                                                 MetricConstants.TIME_TO_PEAK_ERROR,
                                                                 MetricConstants.MAIN,
                                                                 DatasetIdentifier.of( l2,
                                                                                       "B",
                                                                                       "C" ) );
        List<Pair<Instant, Duration>> inputThird = new ArrayList<>();
        inputThird.add( Pair.of( Instant.parse( "1985-01-01T00:00:00Z" ), Duration.ofHours( 1 ) ) );
        final PairedOutput<Instant, Duration> q = PairedOutput.of( inputThird, m1 );
        final PairedOutput<Instant, Duration> r = PairedOutput.of( inputThird, m2 );
        assertTrue( "Unequal metadata.", !q.getMetadata().equals( r.getMetadata() ) );
    }

    /**
     * Constructs a {@link PairedOutput} and checks the {@link PairedOutput#hashCode()}.
     */

    @Test
    public void test4HashCode()
    {
        final Location l1 = Location.of( "A" );
        final MetricOutputMetadata m1 = MetricOutputMetadata.of( 10,
                                                                 MeasurementUnit.of(),
                                                                 MeasurementUnit.of( "CMS" ),
                                                                 MetricConstants.CONTINGENCY_TABLE,
                                                                 MetricConstants.MAIN,
                                                                 DatasetIdentifier.of( l1,
                                                                                       "B",
                                                                                       "C" ) );
        final Location l2 = Location.of( "A" );
        final MetricOutputMetadata m2 = MetricOutputMetadata.of( 10,
                                                                 MeasurementUnit.of(),
                                                                 MeasurementUnit.of( "CMS" ),
                                                                 MetricConstants.CONTINGENCY_TABLE,
                                                                 MetricConstants.MAIN,
                                                                 DatasetIdentifier.of( l2,
                                                                                       "B",
                                                                                       "C" ) );
        final Location l3 = Location.of( "B" );
        final MetricOutputMetadata m3 = MetricOutputMetadata.of( 10,
                                                                 MeasurementUnit.of(),
                                                                 MeasurementUnit.of( "CMS" ),
                                                                 MetricConstants.CONTINGENCY_TABLE,
                                                                 MetricConstants.MAIN,
                                                                 DatasetIdentifier.of( l3,
                                                                                       "B",
                                                                                       "C" ) );
        List<Pair<Instant, Duration>> inputThird = new ArrayList<>();
        inputThird.add( Pair.of( Instant.parse( "1985-01-01T00:00:00Z" ), Duration.ofHours( 1 ) ) );
        final PairedOutput<Instant, Duration> q = PairedOutput.of( inputThird, m1 );
        final PairedOutput<Instant, Duration> r = PairedOutput.of( inputThird, m2 );
        final PairedOutput<Instant, Duration> s = PairedOutput.of( inputThird, m3 );
        assertTrue( "Expected equal hash codes.", q.hashCode() == r.hashCode() );
        assertTrue( "Expected unequal hash codes.", q.hashCode() != s.hashCode() );
    }

    /**
     * Checks for expected exceptions when constructing a {@link PairedOutput}.
     */

    @Test
    public void test6Exceptions()
    {
        final Location l1 = Location.of( "A" );
        final MetricOutputMetadata m1 = MetricOutputMetadata.of( 10,
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
            PairedOutput.of( null, m1 );
            fail( "Expected a checked exception on attempting to construct a paired output with null input." );
        }
        catch ( MetricOutputException e )
        {
        }
        // Null metadata
        try
        {
            PairedOutput.of( input, null );
            fail( "Expected a checked exception on attempting to construct a paired output with null metadata." );
        }
        catch ( MetricOutputException e )
        {
        }
        // Null pair
        try
        {
            input.add( null );
            PairedOutput.of( input, m1 );
            fail( "Expected a checked exception on attempting to construct a paired output with a null pair." );
        }
        catch ( MetricOutputException e )
        {
        }
        // Pair with null left
        try
        {
            input.remove( 1 );
            input.add( Pair.of( null, Duration.ofHours( 1 ) ) );
            PairedOutput.of( input, m1 );
            fail( "Expected a checked exception on attempting to construct a paired output with a pair that has a "
                  + "null left side." );
        }
        catch ( MetricOutputException e )
        {
        }
        // Pair with null right
        try
        {
            input.remove( 1 );
            input.add( Pair.of( Instant.parse( "1985-01-01T00:00:00Z" ), null ) );
            PairedOutput.of( input, m1 );
            fail( "Expected a checked exception on attempting to construct a paired output with a pair that has a "
                  + "null right side." );
        }
        catch ( MetricOutputException e )
        {
        }
    }

}
