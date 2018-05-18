package wres.datamodel;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.outputs.MetricOutputException;
import wres.datamodel.outputs.PairedOutput;

/**
 * Tests the {@link SafePairedOutput}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.4
 * @since 0.1
 */
public final class SafePairedOutputTest
{

    /**
     * Constructs a {@link SafePairedOutput} and tests for equality with another {@link SafePairedOutput}.
     */

    @Test
    public void test1Equals()
    {
        final DataFactory d = DefaultDataFactory.getInstance();
        final MetadataFactory metaFac = d.getMetadataFactory();
        final Location l1 = metaFac.getLocation( "A" );
        final MetricOutputMetadata m1 = metaFac.getOutputMetadata( 10,
                                                                   metaFac.getDimension(),
                                                                   metaFac.getDimension( "CMS" ),
                                                                   MetricConstants.TIME_TO_PEAK_ERROR,
                                                                   MetricConstants.MAIN,
                                                                   metaFac.getDatasetIdentifier( l1, "B", "C" ) );
        final Location l2 = metaFac.getLocation( "A" );
        final MetricOutputMetadata m2 = metaFac.getOutputMetadata( 11,
                                                                   metaFac.getDimension(),
                                                                   metaFac.getDimension( "CMS" ),
                                                                   MetricConstants.TIME_TO_PEAK_ERROR,
                                                                   MetricConstants.MAIN,
                                                                   metaFac.getDatasetIdentifier( l2, "B", "C" ) );
        final Location l3 = metaFac.getLocation( "B" );
        final MetricOutputMetadata m3 = metaFac.getOutputMetadata( 10,
                                                                   metaFac.getDimension(),
                                                                   metaFac.getDimension( "CMS" ),
                                                                   MetricConstants.TIME_TO_PEAK_ERROR,
                                                                   MetricConstants.MAIN,
                                                                   metaFac.getDatasetIdentifier( l3, "B", "C" ) );
        List<Pair<Instant, Duration>> input = new ArrayList<>();
        input.add( Pair.of( Instant.parse( "1985-01-01T00:00:00Z" ), Duration.ofHours( 1 ) ) );
        final PairedOutput<Instant, Duration> s = d.ofPairedOutput( input, m1 );
        final PairedOutput<Instant, Duration> t = d.ofPairedOutput( input, m1 );
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
        assertTrue( "Expected non-equal outputs.", !s.equals( d.ofPairedOutput( inputSecond, m1 ) ) );
        assertTrue( "Expected non-equal outputs.", !s.equals( d.ofPairedOutput( input, m2 ) ) );
        List<Pair<Instant, Duration>> inputThird = new ArrayList<>();
        inputThird.add( Pair.of( Instant.parse( "1985-01-01T00:00:00Z" ), Duration.ofHours( 1 ) ) );
        final PairedOutput<Instant, Duration> q = d.ofPairedOutput( inputThird, m2 );
        final PairedOutput<Instant, Duration> r = d.ofPairedOutput( inputThird, m3 );
        assertTrue( "Expected non-equal outputs.", !s.equals( q ) );
        assertTrue( "Expected equal outputs.", q.equals( q ) );
        assertTrue( "Expected non-equal outputs.", !q.equals( s ) );
        assertTrue( "Expected non-equal outputs.", !q.equals( r ) );
    }

    /**
     * Constructs a {@link SafePairedOutput} and checks the {@link SafePairedOutput#toString()} representation.
     */

    @Test
    public void test2ToString()
    {
        final DataFactory d = DefaultDataFactory.getInstance();
        final MetadataFactory metaFac = d.getMetadataFactory();
        final Location l1 = metaFac.getLocation( "A" );
        final MetricOutputMetadata m1 = metaFac.getOutputMetadata( 10,
                                                                   metaFac.getDimension(),
                                                                   metaFac.getDimension( "CMS" ),
                                                                   MetricConstants.TIME_TO_PEAK_ERROR,
                                                                   MetricConstants.MAIN,
                                                                   metaFac.getDatasetIdentifier( l1, "B", "C" ) );
        List<Pair<Instant, Duration>> input = new ArrayList<>();
        input.add( Pair.of( Instant.parse( "1985-01-01T00:00:00Z" ), Duration.ofHours( 1 ) ) );
        input.add( Pair.of( Instant.parse( "1985-01-01T00:00:00Z" ), Duration.ofHours( 1 ) ) );
        final PairedOutput<Instant, Duration> s = d.ofPairedOutput( input, m1 );
        final PairedOutput<Instant, Duration> t = d.ofPairedOutput( input, m1 );
        assertTrue( "Expected equal string representations.", s.toString().equals( t.toString() ) );
    }

    /**
     * Constructs a {@link SafePairedOutput} and checks the {@link SafePairedOutput#getMetadata()}.
     */

    @Test
    public void test3GetMetadata()
    {
        final DataFactory d = DefaultDataFactory.getInstance();
        final MetadataFactory metaFac = d.getMetadataFactory();
        final Location l1 = metaFac.getLocation( "A" );
        final MetricOutputMetadata m1 = metaFac.getOutputMetadata( 10,
                                                                   metaFac.getDimension(),
                                                                   metaFac.getDimension( "CMS" ),
                                                                   MetricConstants.TIME_TO_PEAK_ERROR,
                                                                   MetricConstants.MAIN,
                                                                   metaFac.getDatasetIdentifier( l1, "B", "C" ) );
        final Location l2 = metaFac.getLocation( "B" );
        final MetricOutputMetadata m2 = metaFac.getOutputMetadata( 10,
                                                                   metaFac.getDimension(),
                                                                   metaFac.getDimension( "CMS" ),
                                                                   MetricConstants.TIME_TO_PEAK_ERROR,
                                                                   MetricConstants.MAIN,
                                                                   metaFac.getDatasetIdentifier( l2, "B", "C" ) );
        List<Pair<Instant, Duration>> inputThird = new ArrayList<>();
        inputThird.add( Pair.of( Instant.parse( "1985-01-01T00:00:00Z" ), Duration.ofHours( 1 ) ) );
        final PairedOutput<Instant, Duration> q = d.ofPairedOutput( inputThird, m1 );
        final PairedOutput<Instant, Duration> r = d.ofPairedOutput( inputThird, m2 );
        assertTrue( "Unequal metadata.", !q.getMetadata().equals( r.getMetadata() ) );
    }

    /**
     * Constructs a {@link SafePairedOutput} and checks the {@link SafePairedOutput#hashCode()}.
     */

    @Test
    public void test4HashCode()
    {
        final DataFactory d = DefaultDataFactory.getInstance();
        final MetadataFactory metaFac = d.getMetadataFactory();
        final Location l1 = metaFac.getLocation( "A" );
        final MetricOutputMetadata m1 = metaFac.getOutputMetadata( 10,
                                                                   metaFac.getDimension(),
                                                                   metaFac.getDimension( "CMS" ),
                                                                   MetricConstants.CONTINGENCY_TABLE,
                                                                   MetricConstants.MAIN,
                                                                   metaFac.getDatasetIdentifier( l1, "B", "C" ) );
        final Location l2 = metaFac.getLocation( "A" );
        final MetricOutputMetadata m2 = metaFac.getOutputMetadata( 10,
                                                                   metaFac.getDimension(),
                                                                   metaFac.getDimension( "CMS" ),
                                                                   MetricConstants.CONTINGENCY_TABLE,
                                                                   MetricConstants.MAIN,
                                                                   metaFac.getDatasetIdentifier( l2, "B", "C" ) );
        final Location l3 = metaFac.getLocation( "B" );
        final MetricOutputMetadata m3 = metaFac.getOutputMetadata( 10,
                                                                   metaFac.getDimension(),
                                                                   metaFac.getDimension( "CMS" ),
                                                                   MetricConstants.CONTINGENCY_TABLE,
                                                                   MetricConstants.MAIN,
                                                                   metaFac.getDatasetIdentifier( l3, "B", "C" ) );
        List<Pair<Instant, Duration>> inputThird = new ArrayList<>();
        inputThird.add( Pair.of( Instant.parse( "1985-01-01T00:00:00Z" ), Duration.ofHours( 1 ) ) );
        final PairedOutput<Instant, Duration> q = d.ofPairedOutput( inputThird, m1 );
        final PairedOutput<Instant, Duration> r = d.ofPairedOutput( inputThird, m2 );
        final PairedOutput<Instant, Duration> s = d.ofPairedOutput( inputThird, m3 );
        assertTrue( "Expected equal hash codes.", q.hashCode() == r.hashCode() );
        assertTrue( "Expected unequal hash codes.", q.hashCode() != s.hashCode() );
    }

    /**
     * Checks for expected exceptions when constructing a {@link SafePairedOutput}.
     */

    @Test
    public void test6Exceptions()
    {
        final DataFactory d = DefaultDataFactory.getInstance();
        final MetadataFactory metaFac = d.getMetadataFactory();
        final Location l1 = metaFac.getLocation( "A" );
        final MetricOutputMetadata m1 = metaFac.getOutputMetadata( 10,
                                                                   metaFac.getDimension(),
                                                                   metaFac.getDimension( "CMS" ),
                                                                   MetricConstants.CONTINGENCY_TABLE,
                                                                   MetricConstants.MAIN,
                                                                   metaFac.getDatasetIdentifier( l1, "B", "C" ) );
        List<Pair<Instant, Duration>> input = new ArrayList<>();
        input.add( Pair.of( Instant.parse( "1985-01-01T00:00:00Z" ), Duration.ofHours( 1 ) ) );
        // Null output
        try
        {
            d.ofPairedOutput( null, m1 );
            fail( "Expected a checked exception on attempting to construct a paired output with null input." );
        }
        catch ( MetricOutputException e )
        {
        }
        // Null metadata
        try
        {
            d.ofPairedOutput( input, null );
            fail( "Expected a checked exception on attempting to construct a paired output with null metadata." );
        }
        catch ( MetricOutputException e )
        {
        }
        // Null pair
        try
        {
            input.add( null );
            d.ofPairedOutput( input, m1 );
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
            d.ofPairedOutput( input, m1 );
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
            d.ofPairedOutput( input, m1 );
            fail( "Expected a checked exception on attempting to construct a paired output with a pair that has a "
                    + "null right side." );
        }
        catch ( MetricOutputException e )
        {
        }
    }

}
