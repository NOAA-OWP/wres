package wres.config;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import wres.config.generated.Circle;
import wres.config.generated.CoordinateSelection;
import wres.config.generated.Feature;
import wres.config.generated.Polygon;

/**
 * Class that tests {@link FeaturePlus}.
 * 
 * @author james.brown@hydrosolved.com
 */

public class FeaturePlusTest
{

    /**
     * Tests {@link FeaturePlus#equals(Object)}.
     */

    @Test
    public void testEquals()
    {
        List<Polygon.Point> points = Arrays.asList(
                new Polygon.Point( 1, 2 ),
                new Polygon.Point( 3, 4 ),
                new Polygon.Point( 5, 6 )
        );

        // Reflexive
        Feature one =
                new Feature( Arrays.asList( "A" ),
                             new CoordinateSelection( 1.0f, 2.0f, 3.0f ),
                             new Polygon( points, BigInteger.valueOf( 4326 ) ),
                             new Circle( 1, 2, 100, BigInteger.valueOf( 4326 )),
                             "A",
                             "B",
                             12345L,
                             "D",
                             "E",
                             "F",
                             "G",
                             "H" );
        FeaturePlus first = FeaturePlus.of( one );

        assertTrue( "Expected equal features.", first.equals( first ) );

        // Symmetric
        Feature two =
                new Feature( Arrays.asList( "A" ),
                             new CoordinateSelection( 1.0f, 2.0f, 3.0f ),
                             new Polygon( points, BigInteger.valueOf( 4326 ) ),
                             new Circle( 1, 2, 100, BigInteger.valueOf( 4326 )),
                             "A",
                             "B",
                             12345L,
                             "D",
                             "E",
                             "F",
                             "G",
                             "H" );
        FeaturePlus second = FeaturePlus.of( two );

        assertTrue( "Expected equal features.", second.equals( first ) && first.equals( second ) );

        // Transitive
        Feature three =
                new Feature( Arrays.asList( "A" ),
                             new CoordinateSelection( 1.0f, 2.0f, 3.0f ),
                             new Polygon( points, BigInteger.valueOf( 4326 ) ),
                             new Circle( 1, 2, 100, BigInteger.valueOf( 4326 )),
                             "A",
                             "B",
                             12345L,
                             "D",
                             "E",
                             "F",
                             "G",
                             "H" );
        FeaturePlus third = FeaturePlus.of( three );

        assertTrue( "Expected equal features.",
                    first.equals( second ) && second.equals( third ) && first.equals( second ) );

        // Consistent
        for ( int i = 0; i < 100; i++ )
        {
            assertTrue( "Expected equal features.", first.equals( second ) );
        }

        // Nullity
        assertFalse( "Expected unequal features.", first.equals( null ) );

        // Unequal combinations based on different components
        Feature nullFeature =
                new Feature( null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null );

        FeaturePlus nullth = FeaturePlus.of( nullFeature );

        // Alias
        Feature four =
                new Feature( Arrays.asList( "A", "B" ),
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null );

        FeaturePlus fourth = FeaturePlus.of( four );

        assertFalse( "Expected unequal features.", first.equals( fourth ) );

        // Coordinate
        Feature five =
                new Feature( null,
                             new CoordinateSelection( 5.0f, 6.0f, 7.0f ),
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null );

        FeaturePlus fifth = FeaturePlus.of( five );

        Feature fiveLat =
                new Feature( null,
                             new CoordinateSelection( 6.0f, 6.0f, 7.0f ),
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null );

        FeaturePlus fifthLat = FeaturePlus.of( fiveLat );

        assertFalse( "Expected unequal features.", first.equals( fifthLat ) );
        assertFalse( "Expected unequal features.", fifth.equals( nullth ) );
        assertFalse( "Expected unequal features.", nullth.equals( fifth ) );
        assertFalse( "Expected unequal features.", fifth.equals( fifthLat ) );

        Feature fiveLong =
                new Feature( null,
                             new CoordinateSelection( 5.0f, 7.0f, 7.0f ),
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null );

        FeaturePlus fifthLong = FeaturePlus.of( fiveLong );

        assertFalse( "Expected unequal features.", fifthLong.equals( fifth ) );

        Feature fiveRange =
                new Feature( null,
                             new CoordinateSelection( 5.0f, 6.0f, 8.0f ),
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null );

        FeaturePlus fifthRange = FeaturePlus.of( fiveRange );

        assertFalse( "Expected unequal features.", fifthRange.equals( fifth ) );

        // Label
        Feature seven =
                new Feature( null,
                             null,
                             null,
                             null,
                             "B",
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null );

        FeaturePlus seventh = FeaturePlus.of( seven );

        assertFalse( "Expected unequal features.", first.equals( seventh ) );

        Feature eight =
                new Feature( null,
                             null,
                             null,
                             null,
                             "C",
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null );

        FeaturePlus eighth = FeaturePlus.of( eight );

        assertFalse( "Expected unequal features.", seventh.equals( eighth ) );

        // Location id        
        Feature nine =
                new Feature( null,
                             null,
                             null,
                             null,
                             null,
                             "B",
                             null,
                             null,
                             null,
                             null,
                             null,
                             null );

        FeaturePlus ninth = FeaturePlus.of( nine );

        assertFalse( "Expected unequal features.", first.equals( ninth ) );

        Feature ten =
                new Feature( null,
                             null,
                             null,
                             null,
                             null,
                             "C",
                             null,
                             null,
                             null,
                             null,
                             null,
                             null );

        FeaturePlus tenth = FeaturePlus.of( ten );

        assertFalse( "Expected unequal features.", ninth.equals( tenth ) );

        //Comid
        Feature eleven =
                new Feature( null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             123L,
                             null,
                             null,
                             null,
                             null,
                             null );

        FeaturePlus eleventh = FeaturePlus.of( eleven );

        assertFalse( "Expected unequal features.", first.equals( eleventh ) );

        Feature twelve =
                new Feature( null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             234L,
                             null,
                             null,
                             null,
                             null,
                             null );

        FeaturePlus twelfth = FeaturePlus.of( twelve );

        assertFalse( "Expected unequal features.", eleventh.equals( twelfth ) );

        // Gauge id              
        Feature thirteen =
                new Feature( null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             "B",
                             null,
                             null,
                             null,
                             null );

        FeaturePlus thirteenth = FeaturePlus.of( thirteen );

        assertFalse( "Expected unequal features.", first.equals( thirteenth ) );

        Feature fourteen =
                new Feature( null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             "C",
                             null,
                             null,
                             null,
                             null );

        FeaturePlus fourteenth = FeaturePlus.of( fourteen );

        assertFalse( "Expected unequal features.", fourteenth.equals( thirteenth ) );

        // HUC
        Feature fifteen =
                new Feature( null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             "B",
                             null,
                             null,
                             null );

        FeaturePlus fifteenth = FeaturePlus.of( fifteen );

        assertFalse( "Expected unequal features.", first.equals( fifteenth ) );

        Feature sixteen =
                new Feature( null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             "C",
                             null,
                             null,
                             null );

        FeaturePlus sixteenth = FeaturePlus.of( sixteen );

        assertFalse( "Expected unequal features.", fifteenth.equals( sixteenth ) );

        // name
        Feature seventeen =
                new Feature( null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             "B",
                             null,
                             null );

        FeaturePlus seventeenth = FeaturePlus.of( seventeen );

        assertFalse( "Expected unequal features.", first.equals( seventeenth ) );

        Feature eighteen =
                new Feature( null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             "C",
                             null,
                             null );

        FeaturePlus eighteenth = FeaturePlus.of( eighteen );

        assertFalse( "Expected unequal features.", seventeenth.equals( eighteenth ) );

        // RFC
        Feature nineteen =
                new Feature( null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             "B",
                             null );

        FeaturePlus nineteenth = FeaturePlus.of( nineteen );

        assertFalse( "Expected unequal features.", first.equals( nineteenth ) );

        Feature twenty =
                new Feature( null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             "C",
                             null );

        FeaturePlus twentieth = FeaturePlus.of( twenty );

        assertFalse( "Expected unequal features.", nineteenth.equals( twentieth ) );

        // Wkt
        Feature twentyOne =
                new Feature( null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             "B" );

        FeaturePlus twentyFirst = FeaturePlus.of( twentyOne );

        assertFalse( "Expected unequal features.", first.equals( twentyFirst ) );

        Feature twentyTwo =
                new Feature( null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             "C" );

        FeaturePlus twentySecond = FeaturePlus.of( twentyTwo );

        assertFalse( "Expected unequal features.", twentySecond.equals( twentyFirst ) );

        // Polygon

        Feature twentyThree = new Feature ( null,
                                            null,
                                            new Polygon(
                                                    Arrays.asList(new Polygon.Point( 1, 2 )),
                                                    BigInteger.valueOf(4326)
                                            ),
                                            null,
                                            null,
                                            null,
                                            null,
                                            null,
                                            null,
                                            null,
                                            null,
                                            null);

        FeaturePlus twentyThird = FeaturePlus.of(twentyThree);

        assertFalse( "Expected unequal features.", first.equals( twentyThird ) );

        Feature twentyFour = new Feature ( null,
                                            null,
                                            new Polygon(
                                                    Arrays.asList(new Polygon.Point( 3, 4 )),
                                                    BigInteger.valueOf(7)
                                            ),
                                            null,
                                            null,
                                            null,
                                            null,
                                            null,
                                            null,
                                            null,
                                            null,
                                            null);

        FeaturePlus twentyFourth = FeaturePlus.of(twentyFour);

        assertFalse( "Expected unequal features.", twentyFourth.equals( twentyThird ) );


        // Circle

        Feature twentyFive = new Feature (null,
                                          null,
                                          null,
                                          new Circle(1, 2, 100, BigInteger.valueOf( 3 )),
                                          null,
                                          null,
                                          null,
                                          null,
                                          null,
                                          null,
                                          null,
                                          null);

        FeaturePlus twentyFifth = FeaturePlus.of( twentyFive );

        assertFalse( "Expected unequal features.", first.equals( twentyFifth ) );

        Feature twentySix = new Feature (null,
                                          null,
                                          null,
                                          new Circle(7, 12, 103, BigInteger.valueOf( 4326 )),
                                          null,
                                          null,
                                          null,
                                          null,
                                          null,
                                          null,
                                          null,
                                          null);

        FeaturePlus twentySixth = FeaturePlus.of(twentySix);

        assertFalse( "Expected unequal features.", twentySixth.equals( twentyFifth ) );

    }

    /**
     * Tests {@link FeaturePlus#hashCode()}.
     */

    @Test
    public void testHashCode()
    {
        // Consistent over repeated calls
        Feature one =
                new Feature( Arrays.asList( "A" ),
                             new CoordinateSelection( 1.0f, 2.0f, 3.0f ),
                             new Polygon(
                                     Arrays.asList(new Polygon.Point( 3, 4 )),
                                     BigInteger.valueOf(7)
                             ),
                             new Circle(7, 12, 103, BigInteger.valueOf( 4326 )),
                             "A",
                             "B",
                             12345L,
                             "D",
                             "E",
                             "F",
                             "G",
                             "H" );
        FeaturePlus first = FeaturePlus.of( one );
        int code = first.hashCode();
        for ( int i = 0; i < 100; i++ )
        {
            assertTrue( "Expected equal hashcodes.", first.hashCode() == code );
        }

        // Consistent with equals
        Feature two =
                new Feature( Arrays.asList( "A" ),
                             new CoordinateSelection( 1.0f, 2.0f, 3.0f ),
                             new Polygon(
                                     Arrays.asList(new Polygon.Point( 3, 4 )),
                                     BigInteger.valueOf(7)
                             ),
                             new Circle(7, 12, 103, BigInteger.valueOf( 4326 )),
                             "A",
                             "B",
                             12345L,
                             "D",
                             "E",
                             "F",
                             "G",
                             "H" );
        FeaturePlus second = FeaturePlus.of( two );

        assertTrue( "Expected equal hashcodes.", first.hashCode() == second.hashCode() && first.equals( second ) );

        Feature three =
                new Feature( Arrays.asList( "A" ),
                             null,
                             null,
                             null,
                             "A",
                             "B",
                             12345L,
                             "D",
                             "E",
                             "F",
                             "G",
                             "H" );
        FeaturePlus third = FeaturePlus.of( three );

        // Consistent with equals
        Feature four =
                new Feature( Arrays.asList( "A" ),
                             null,
                             null,
                             null,
                             "A",
                             "B",
                             12345L,
                             "D",
                             "E",
                             "F",
                             "G",
                             "H" );
        FeaturePlus fourth = FeaturePlus.of( four );

        assertTrue( "Expected equal hashcodes.", third.hashCode() == fourth.hashCode() && third.equals( fourth ) );

    }

    /**
     * Tests {@link FeaturePlus#compareTo(FeaturePlus)}.
     */

    @Test
    public void testCompareTo()
    {
        // Reflexive
        Feature one =
                new Feature( Arrays.asList( "A" ),
                             new CoordinateSelection( 1.0f, 2.0f, 3.0f ),
                             new Polygon(
                                     Arrays.asList(new Polygon.Point( 3, 4 )),
                                     BigInteger.valueOf(7)
                             ),
                             new Circle(7, 12, 103, BigInteger.valueOf( 4326 )),
                             "A",
                             "B",
                             12345L,
                             "D",
                             "E",
                             "F",
                             "G",
                             "H" );
        FeaturePlus first = FeaturePlus.of( one );

        assertTrue( "Expected comparable features.", first.compareTo( first ) == 0 );

        // Anticommutative
        Feature two =
                new Feature( Arrays.asList( "A" ),
                             new CoordinateSelection( 3.0f, 2.0f, 3.0f ),
                             new Polygon(
                                     Arrays.asList(new Polygon.Point( 5, 4 )),
                                     BigInteger.valueOf(7)
                             ),
                             new Circle(7, 14, 1, BigInteger.valueOf( 7 )),
                             "A",
                             "Q",
                             12345L,
                             "D",
                             "E",
                             "R",
                             "G",
                             "H" );
        FeaturePlus second = FeaturePlus.of( two );

        assertTrue( "Expected anticommutative behaviour.",
                    Math.abs( second.compareTo( first ) ) == Math.abs( first.compareTo( second ) ) );

        // Symmetric 
        Feature three =
                new Feature( Arrays.asList( "A" ),
                             new CoordinateSelection( 3.0f, 2.0f, 3.0f ),
                             new Polygon(
                                     Arrays.asList(new Polygon.Point( 5, 4 )),
                                     BigInteger.valueOf(7)
                             ),
                             new Circle(7, 14, 1, BigInteger.valueOf( 7 )),
                             "A",
                             "Q",
                             12345L,
                             "D",
                             "E",
                             "R",
                             "G",
                             "H" );
        FeaturePlus third = FeaturePlus.of( three );


        assertTrue( "Expected symmetric comparability.",
                    second.compareTo( third ) == 0 && third.compareTo( second ) == 0 );

        // Transitive 
        Feature threeTrans =
                new Feature( Arrays.asList( "B" ),
                             new CoordinateSelection( 3.0f, 2.0f, 3.0f ),
                             new Polygon(
                                     Arrays.asList(new Polygon.Point( 5, 4 )),
                                     BigInteger.valueOf(7)
                             ),
                             new Circle(7, 14, 1, BigInteger.valueOf( 7 )),
                             "A",
                             "Q",
                             12345L,
                             "D",
                             "E",
                             "R",
                             "G",
                             "H" );
        FeaturePlus thirdTrans = FeaturePlus.of( threeTrans );

        assertTrue( "Expected transitive behaviour.",
                    thirdTrans.compareTo( third ) < 0 && third.compareTo( first ) < 0
                                                      && thirdTrans.compareTo( first ) < 0 );

        // Unequal combinations based on different components
        Feature nullFeature =
                new Feature( null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null );

        FeaturePlus nullth = FeaturePlus.of( nullFeature );

        // Alias
        Feature four =
                new Feature( Arrays.asList( "A", "B" ),
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null );

        FeaturePlus fourth = FeaturePlus.of( four );

        assertFalse( "Expected unequal features.", first.compareTo( fourth ) == 0 );
        assertFalse( "Expected unequal features.", fourth.compareTo( first ) == 0 );

        // Coordinate
        Feature five =
                new Feature( null,
                             new CoordinateSelection( 5.0f, 6.0f, 7.0f ),
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null );

        FeaturePlus fifth = FeaturePlus.of( five );

        Feature fiveLat =
                new Feature( null,
                             new CoordinateSelection( 6.0f, 6.0f, 7.0f ),
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null );

        FeaturePlus fifthLat = FeaturePlus.of( fiveLat );

        assertFalse( "Expected unequal features.", first.compareTo( fifthLat ) == 0 );
        assertFalse( "Expected unequal features.", fifth.compareTo( nullth ) == 0 );
        assertFalse( "Expected unequal features.", nullth.compareTo( fifth ) == 0 );
        assertFalse( "Expected unequal features.", fifth.compareTo( fifthLat ) == 0 );

        Feature fiveLong =
                new Feature( null,
                             new CoordinateSelection( 5.0f, 7.0f, 7.0f ),
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null );

        FeaturePlus fifthLong = FeaturePlus.of( fiveLong );

        assertFalse( "Expected unequal features.", fifthLong.compareTo( fifth ) == 0 );

        Feature fiveRange =
                new Feature( null,
                             new CoordinateSelection( 5.0f, 6.0f, 8.0f ),
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null );

        FeaturePlus fifthRange = FeaturePlus.of( fiveRange );

        assertFalse( "Expected unequal features.", fifthRange.compareTo( fifth ) == 0 );

        // Label
        Feature seven =
                new Feature( null,
                             null,
                             null,
                             null,
                             "B",
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null );

        FeaturePlus seventh = FeaturePlus.of( seven );

        assertFalse( "Expected unequal features.", first.compareTo( seventh ) == 0 );

        Feature eight =
                new Feature( null,
                             null,
                             null,
                             null,
                             "C",
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null );

        FeaturePlus eighth = FeaturePlus.of( eight );

        assertFalse( "Expected unequal features.", seventh.compareTo( eighth ) == 0 );

        // Location id        
        Feature nine =
                new Feature( null,
                             null,
                             null,
                             null,
                             null,
                             "B",
                             null,
                             null,
                             null,
                             null,
                             null,
                             null );

        FeaturePlus ninth = FeaturePlus.of( nine );

        assertFalse( "Expected unequal features.", first.compareTo( ninth ) == 0 );

        Feature ten =
                new Feature( null,
                             null,
                             null,
                             null,
                             null,
                             "C",
                             null,
                             null,
                             null,
                             null,
                             null,
                             null );

        FeaturePlus tenth = FeaturePlus.of( ten );

        assertFalse( "Expected unequal features.", ninth.compareTo( tenth ) == 0 );

        //Comid
        Feature eleven =
                new Feature( null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             123L,
                             null,
                             null,
                             null,
                             null,
                             null );

        FeaturePlus eleventh = FeaturePlus.of( eleven );

        assertFalse( "Expected unequal features.", first.compareTo( eleventh ) == 0 );

        Feature twelve =
                new Feature( null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             234L,
                             null,
                             null,
                             null,
                             null,
                             null );

        FeaturePlus twelfth = FeaturePlus.of( twelve );

        assertFalse( "Expected unequal features.", eleventh.compareTo( twelfth ) == 0 );

        // Gauge id              
        Feature thirteen =
                new Feature( null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             "B",
                             null,
                             null,
                             null,
                             null );

        FeaturePlus thirteenth = FeaturePlus.of( thirteen );

        assertFalse( "Expected unequal features.", first.compareTo( thirteenth ) == 0 );

        Feature fourteen =
                new Feature( null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             "C",
                             null,
                             null,
                             null,
                             null );

        FeaturePlus fourteenth = FeaturePlus.of( fourteen );

        assertFalse( "Expected unequal features.", fourteenth.compareTo( thirteenth ) == 0 );

        // HUC
        Feature fifteen =
                new Feature( null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             "B",
                             null,
                             null,
                             null );

        FeaturePlus fifteenth = FeaturePlus.of( fifteen );

        assertFalse( "Expected unequal features.", first.compareTo( fifteenth ) == 0 );

        Feature sixteen =
                new Feature( null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             "C",
                             null,
                             null,
                             null );

        FeaturePlus sixteenth = FeaturePlus.of( sixteen );

        assertFalse( "Expected unequal features.", fifteenth.compareTo( sixteenth ) == 0 );

        // name
        Feature seventeen =
                new Feature( null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             "B",
                             null,
                             null );

        FeaturePlus seventeenth = FeaturePlus.of( seventeen );

        assertFalse( "Expected unequal features.", first.compareTo( seventeenth ) == 0 );

        Feature eighteen =
                new Feature( null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             "C",
                             null,
                             null );

        FeaturePlus eighteenth = FeaturePlus.of( eighteen );

        assertFalse( "Expected unequal features.", seventeenth.compareTo( eighteenth ) == 0 );

        // RFC
        Feature nineteen =
                new Feature( null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             "B",
                             null );

        FeaturePlus nineteenth = FeaturePlus.of( nineteen );

        assertFalse( "Expected unequal features.", first.compareTo( nineteenth ) == 0 );

        Feature twenty =
                new Feature( null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             "C",
                             null );

        FeaturePlus twentieth = FeaturePlus.of( twenty );

        assertFalse( "Expected unequal features.", nineteenth.compareTo( twentieth ) == 0 );

        // Wkt
        Feature twentyOne =
                new Feature( null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             "B" );

        FeaturePlus twentyFirst = FeaturePlus.of( twentyOne );

        assertFalse( "Expected unequal features.", first.compareTo( twentyFirst ) == 0 );

        Feature twentyTwo =
                new Feature( null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             "C" );

        FeaturePlus twentySecond = FeaturePlus.of( twentyTwo );

        assertFalse( "Expected unequal features.", twentySecond.compareTo( twentyFirst ) == 0 );

        // Polygons

        Feature twentyThree =
                new Feature( null,
                             null,
                             new Polygon(Arrays.asList(new Polygon.Point( 1, 2)), BigInteger.valueOf( 100 )),
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null );

        FeaturePlus twentyThird = FeaturePlus.of(twentyThree);

        assertFalse("Expected unequal features.", first.compareTo( twentyThird ) == 0);

        Feature twentyFour =
                new Feature( null,
                             null,
                             new Polygon(Arrays.asList(new Polygon.Point( 3, 4)), BigInteger.valueOf( 100 )),
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null );

        FeaturePlus twentyFourth = FeaturePlus.of(twentyFour);

        assertFalse( "Expected unequal features.", twentyFourth.compareTo( twentyThird ) == 0 );

        // Circles

        Feature twentyFive =
                new Feature( null,
                             null,
                             null,
                             new Circle(1, 2, 3, BigInteger.valueOf(100)),
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null );

        FeaturePlus twentyFifth = FeaturePlus.of(twentyFive);

        assertFalse("Expected unequal features.", first.compareTo( twentyFifth ) == 0);

        Feature twentySix =
                new Feature( null,
                             null,
                             null,
                             new Circle(4, 5, 6, BigInteger.valueOf(100)),
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null );

        FeaturePlus twentySixth = FeaturePlus.of(twentySix);

        assertFalse( "Expected unequal features.", twentySixth.compareTo( twentyFifth ) == 0 );

    }

    /**
     * Tests {@link FeaturePlus#compareByLocationId(FeaturePlus, FeaturePlus)}.
     */

    @Test
    public void testCompareByLocationId()
    {
        Feature one =
                new Feature( null,
                             null,
                             null,
                             null,
                             null,
                             "A",
                             null,
                             null,
                             null,
                             null,
                             null,
                             null );

        FeaturePlus first = FeaturePlus.of( one );

        Feature two =
                new Feature( null,
                             null,
                             null,
                             null,
                             null,
                             "A",
                             null,
                             null,
                             null,
                             null,
                             null,
                             null );

        FeaturePlus second = FeaturePlus.of( two );

        assertTrue( "Expected equal features by location identifier.",
                    FeaturePlus.compareByLocationId( first, second ) == 0 );
    }

    /**
     * Tests the {@link FeaturePlus#toString()}.
     */

    @Test
    public void testToString()
    {
        Feature one =
                new Feature( Arrays.asList( "A" ),
                             new CoordinateSelection( 1.0f, 2.0f, 3.0f ),
                             new Polygon(Arrays.asList( new Polygon.Point( 1.0f, 2.0f ) ), BigInteger.valueOf( 4326 )),
                             new Circle(1.0f, 2.0f, 3.0f, BigInteger.valueOf( 4326 )),
                             "A",
                             "B",
                             12345L,
                             "D",
                             "E",
                             "F",
                             "G",
                             "H" );
        FeaturePlus first = FeaturePlus.of( one );
        
        assertTrue( "Unexpected string representation of feature.",
                    first.toString().equals( "{B,F,A,D,E,12345,G,H,2.0 1.0,'POLYGON (1.0 2.0)', SRID: 4326,CIRCLE '( (2.0, 1.0), 3.0) )', SRID: 4326,[A]}" ) );
        
        Feature two =
                new Feature( null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null,
                             null );

        FeaturePlus second = FeaturePlus.of( two );

        assertTrue( "Unexpected string representation of feature.",
                    second.toString().equals( "{null,null,null,null,null,null,null,null,null,null,null,[]}" ) );
        
    }

}
