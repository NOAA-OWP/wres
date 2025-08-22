package wres.vis.data;

import java.util.List;

import org.jfree.data.time.TimeSeries;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests the {@link PerformantTimeSeriesCollection}.
 *
 * @author James Brown
 */
class PerformantTimeSeriesCollectionTest
{
    @Test
    void testGetSeries()
    {
        TimeSeries one = new TimeSeries( "foo" );
        TimeSeries two = new TimeSeries( "bar" );
        PerformantTimeSeriesCollection p = PerformantTimeSeriesCollection.of( List.of( one, two ) );
        assertAll( () -> assertEquals( one, p.getSeries( "foo" ) ),
                   () -> assertEquals( two, p.getSeries( "bar" ) ) );
    }

    @Test
    void testEquals()
    {
        TimeSeries one = new TimeSeries( "foo" );
        TimeSeries two = new TimeSeries( "bar" );
        PerformantTimeSeriesCollection p = PerformantTimeSeriesCollection.of( List.of( one, two ) );

        // Reflexive
        assertEquals( p, p );

        // Symmetric
        PerformantTimeSeriesCollection pTwo = PerformantTimeSeriesCollection.of( List.of( one, two ) );
        assertTrue( p.equals( pTwo ) && pTwo.equals( p ) );

        // Transitive
        PerformantTimeSeriesCollection pThree = PerformantTimeSeriesCollection.of( List.of( one, two ) );
        assertTrue( p.equals( pTwo )
                    && pTwo.equals( pThree )
                    && p.equals( pThree ) );

        // Consistent
        for ( int i = 0; i < 100; i++ )
        {
            assertEquals( p, pTwo );
        }

        // Unequal
        TimeSeries three = new TimeSeries( "baz" );
        PerformantTimeSeriesCollection pFour = PerformantTimeSeriesCollection.of( List.of( one, three ) );
        assertNotEquals( p, pFour );
    }

    // Hashcode of the superclass has not been implemented correctly and unit tests in this context will fail. Yuck.
}
