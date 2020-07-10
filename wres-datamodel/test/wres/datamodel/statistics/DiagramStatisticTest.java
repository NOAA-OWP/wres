package wres.datamodel.statistics;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.util.EnumMap;
import java.util.Map;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.datamodel.DatasetIdentifier;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.sampledata.Location;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.sampledata.SampleMetadata;

/**
 * Tests the {@link DiagramStatisticOuter}.
 * 
 * @author james.brown@hydrosolveDataFactory.com
 */
public final class DiagramStatisticTest
{

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    /**
     * Constructs a {@link DiagramStatisticOuter} and tests for equality with another {@link DiagramStatisticOuter}.
     */

    @Test
    public void testEquals()
    {
        final Location l1 = Location.of( "A" );
        final StatisticMetadata m1 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                              DatasetIdentifier.of( l1,
                                                                                                    "B",
                                                                                                    "C" ) ),
                                                           10,
                                                           MeasurementUnit.of(),
                                                           MetricConstants.RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM,
                                                           MetricConstants.MAIN );
        final Location l2 = Location.of( "A" );
        final StatisticMetadata m2 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                              DatasetIdentifier.of( l2,
                                                                                                    "B",
                                                                                                    "C" ) ),
                                                           11,
                                                           MeasurementUnit.of(),
                                                           MetricConstants.RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM,
                                                           MetricConstants.MAIN );
        final Location l3 = Location.of( "B" );
        final StatisticMetadata m3 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                              DatasetIdentifier.of( l3,
                                                                                                    "B",
                                                                                                    "C" ) ),
                                                           10,
                                                           MeasurementUnit.of(),
                                                           MetricConstants.RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM,
                                                           MetricConstants.MAIN );
        Map<MetricDimension, VectorOfDoubles> mva = new EnumMap<>( MetricDimension.class );
        Map<MetricDimension, VectorOfDoubles> mvb = new EnumMap<>( MetricDimension.class );
        Map<MetricDimension, VectorOfDoubles> mvc = new EnumMap<>( MetricDimension.class );
        mva.put( MetricDimension.PROBABILITY_OF_DETECTION, VectorOfDoubles.of( 0.1, 0.2, 0.3, 0.4 ) );
        mva.put( MetricDimension.PROBABILITY_OF_FALSE_DETECTION, VectorOfDoubles.of( 0.1, 0.2, 0.3, 0.4 ) );
        mvb.put( MetricDimension.PROBABILITY_OF_DETECTION, VectorOfDoubles.of( 0.1, 0.2, 0.3, 0.4 ) );
        mvb.put( MetricDimension.PROBABILITY_OF_FALSE_DETECTION, VectorOfDoubles.of( 0.1, 0.2, 0.3, 0.4 ) );
        mvc.put( MetricDimension.PROBABILITY_OF_DETECTION, VectorOfDoubles.of( 0.1, 0.2, 0.3, 0.4, 0.5 ) );
        mvc.put( MetricDimension.PROBABILITY_OF_FALSE_DETECTION, VectorOfDoubles.of( 0.1, 0.2, 0.3, 0.4, 0.5 ) );
        final DiagramStatisticOuter s = DiagramStatisticOuter.of( mva, m1 );
        final DiagramStatisticOuter t = DiagramStatisticOuter.of( mvb, m1 );
        assertTrue( s.equals( t ) );
        assertNotEquals( null, s );
        assertNotEquals( Double.valueOf( 1.0 ), s );
        assertTrue( !s.equals( DiagramStatisticOuter.of( mvc, m1 ) ) );
        assertTrue( !s.equals( DiagramStatisticOuter.of( mvc, m2 ) ) );
        final DiagramStatisticOuter q = DiagramStatisticOuter.of( mva, m2 );
        final DiagramStatisticOuter r = DiagramStatisticOuter.of( mvb, m3 );
        assertTrue( q.equals( q ) );
        assertFalse( s.equals( q ) );
        assertFalse( q.equals( s ) );
        assertFalse( q.equals( r ) );
    }

    /**
     * Constructs a {@link DiagramStatisticOuter} and checks the {@link DiagramStatisticOuter#getMetadata()}.
     */

    @Test
    public void testGetMetadata()
    {
        final Location l1 = Location.of( "A" );
        final StatisticMetadata m1 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                              DatasetIdentifier.of( l1,
                                                                                                    "B",
                                                                                                    "C" ) ),
                                                           10,
                                                           MeasurementUnit.of(),
                                                           MetricConstants.RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM,
                                                           MetricConstants.MAIN );
        final Location l2 = Location.of( "B" );
        final StatisticMetadata m2 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                              DatasetIdentifier.of( l2,
                                                                                                    "B",
                                                                                                    "C" ) ),
                                                           10,
                                                           MeasurementUnit.of(),
                                                           MetricConstants.RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM,
                                                           MetricConstants.MAIN );
        Map<MetricDimension, VectorOfDoubles> mva = new EnumMap<>( MetricDimension.class );
        mva.put( MetricDimension.PROBABILITY_OF_DETECTION, VectorOfDoubles.of( 0.1, 0.2, 0.3, 0.4 ) );
        mva.put( MetricDimension.PROBABILITY_OF_FALSE_DETECTION, VectorOfDoubles.of( 0.1, 0.2, 0.3, 0.4 ) );
        final DiagramStatisticOuter q = DiagramStatisticOuter.of( mva, m1 );
        final DiagramStatisticOuter r = DiagramStatisticOuter.of( mva, m2 );
        assertTrue( "Expected unequal dimensions.", !q.getMetadata().equals( r.getMetadata() ) );
    }

    /**
     * Constructs a {@link DiagramStatisticOuter} and checks the {@link DiagramStatisticOuter#hashCode()}.
     */

    @Test
    public void testHashCode()
    {
        final Location l1 = Location.of( "A" );
        final StatisticMetadata m1 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                              DatasetIdentifier.of( l1,
                                                                                                    "B",
                                                                                                    "C" ) ),
                                                           10,
                                                           MeasurementUnit.of(),
                                                           MetricConstants.RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM,
                                                           MetricConstants.MAIN );
        final Location l2 = Location.of( "A" );
        final StatisticMetadata m2 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                              DatasetIdentifier.of( l2,
                                                                                                    "B",
                                                                                                    "C" ) ),
                                                           10,
                                                           MeasurementUnit.of(),
                                                           MetricConstants.RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM,
                                                           MetricConstants.MAIN );
        final Location l3 = Location.of( "B" );
        final StatisticMetadata m3 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                              DatasetIdentifier.of( l3,
                                                                                                    "B",
                                                                                                    "C" ) ),
                                                           10,
                                                           MeasurementUnit.of(),
                                                           MetricConstants.RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM,
                                                           MetricConstants.MAIN );

        Map<MetricDimension, VectorOfDoubles> mva = new EnumMap<>( MetricDimension.class );
        Map<MetricDimension, VectorOfDoubles> mvb = new EnumMap<>( MetricDimension.class );
        Map<MetricDimension, VectorOfDoubles> mvc = new EnumMap<>( MetricDimension.class );
        mva.put( MetricDimension.PROBABILITY_OF_DETECTION, VectorOfDoubles.of( 0.1, 0.2, 0.3, 0.4 ) );
        mva.put( MetricDimension.PROBABILITY_OF_FALSE_DETECTION, VectorOfDoubles.of( 0.1, 0.2, 0.3, 0.4 ) );
        mvb.put( MetricDimension.PROBABILITY_OF_DETECTION, VectorOfDoubles.of( 0.1, 0.2, 0.3, 0.4 ) );
        mvb.put( MetricDimension.PROBABILITY_OF_FALSE_DETECTION, VectorOfDoubles.of( 0.1, 0.2, 0.3, 0.4 ) );
        mvc.put( MetricDimension.PROBABILITY_OF_DETECTION, VectorOfDoubles.of( 0.1, 0.2, 0.3, 0.4, 0.5 ) );
        mvc.put( MetricDimension.PROBABILITY_OF_FALSE_DETECTION, VectorOfDoubles.of( 0.1, 0.2, 0.3, 0.4, 0.5 ) );
        final DiagramStatisticOuter q = DiagramStatisticOuter.of( mva, m1 );
        final DiagramStatisticOuter r = DiagramStatisticOuter.of( mvb, m2 );
        final DiagramStatisticOuter s = DiagramStatisticOuter.of( mvc, m3 );
        assertTrue( "Expected equal hash codes.", q.hashCode() == r.hashCode() );
        assertTrue( "Expected unequal hash codes.", q.hashCode() != s.hashCode() );
    }

    /**
     * Constructs a {@link DiagramStatisticOuter} and checks the accessor methods for correct operation.
     */

    @Test
    public void testAccessors()
    {
        final Location l1 = Location.of( "A" );
        final StatisticMetadata m1 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                              DatasetIdentifier.of( l1,
                                                                                                    "B",
                                                                                                    "C" ) ),
                                                           10,
                                                           MeasurementUnit.of(),
                                                           MetricConstants.RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM,
                                                           MetricConstants.MAIN );
        Map<MetricDimension, VectorOfDoubles> mva = new EnumMap<>( MetricDimension.class );
        mva.put( MetricDimension.PROBABILITY_OF_DETECTION, VectorOfDoubles.of( 0.1, 0.2, 0.3, 0.4 ) );
        mva.put( MetricDimension.PROBABILITY_OF_FALSE_DETECTION, VectorOfDoubles.of( 0.1, 0.2, 0.3, 0.4 ) );
        final DiagramStatisticOuter s = DiagramStatisticOuter.of( mva, m1 );
        assertTrue( "Expected a " + MetricDimension.PROBABILITY_OF_DETECTION
                    + ".",
                    s.containsKey( MetricDimension.PROBABILITY_OF_DETECTION ) );
        assertTrue( "Expected a " + MetricDimension.PROBABILITY_OF_DETECTION
                    + ".",
                    s.get( MetricDimension.PROBABILITY_OF_DETECTION ) != null );
        assertTrue( "Expected a map of data.", !s.getData().isEmpty() );
    }


    @Test
    public void testExceptionOnNullData()
    {
        final Location l1 = Location.of( "A" );
        final StatisticMetadata m1 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                              DatasetIdentifier.of( l1,
                                                                                                    "B",
                                                                                                    "C" ) ),
                                                           10,
                                                           MeasurementUnit.of(),
                                                           MetricConstants.RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM,
                                                           MetricConstants.MAIN );
        Map<MetricDimension, VectorOfDoubles> mva = new EnumMap<>( MetricDimension.class );
        mva.put( MetricDimension.PROBABILITY_OF_DETECTION,
                 VectorOfDoubles.of( 0.1, 0.2, 0.3, 0.4 ) );
        mva.put( MetricDimension.PROBABILITY_OF_FALSE_DETECTION,
                 VectorOfDoubles.of( 0.1, 0.2, 0.3, 0.4 ) );

        exception.expect( StatisticException.class );

        DiagramStatisticOuter.of( null, m1 );

    }

    @Test
    public void testExceptionOnNullMetadata()
    {

        Map<MetricDimension, VectorOfDoubles> mva = new EnumMap<>( MetricDimension.class );
        mva.put( MetricDimension.PROBABILITY_OF_DETECTION,
                 VectorOfDoubles.of( 0.1, 0.2, 0.3, 0.4 ) );
        mva.put( MetricDimension.PROBABILITY_OF_FALSE_DETECTION,
                 VectorOfDoubles.of( 0.1, 0.2, 0.3, 0.4 ) );

        exception.expect( StatisticException.class );

        DiagramStatisticOuter.of( mva, null );

    }

    @Test
    public void testExceptionOnEnptyData()
    {
        final Location l1 = Location.of( "A" );
        final StatisticMetadata m1 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                              DatasetIdentifier.of( l1,
                                                                                                    "B",
                                                                                                    "C" ) ),
                                                           10,
                                                           MeasurementUnit.of(),
                                                           MetricConstants.RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM,
                                                           MetricConstants.MAIN );
        Map<MetricDimension, VectorOfDoubles> mva = new EnumMap<>( MetricDimension.class );

        exception.expect( StatisticException.class );

        DiagramStatisticOuter.of( mva, m1 );

    }

}
