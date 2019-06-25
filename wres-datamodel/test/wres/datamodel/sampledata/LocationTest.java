package wres.datamodel.sampledata;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * Tests the {@link Location}.
 * 
 * @author chris.tubbs
 * @author james.brown@hydrosolved.com
 */

public class LocationTest
{

    private static final String DRRC3 = "DRRC3";

    private static final String DRRC2 = "DRRC2";

    private static final String SECOND_LOCATION = "09165000";
    
    private static final String FIRST_LOCATION = "0916500";

    /**
     * Tests {@link Location#equals(Object)}
     */

    @Test
    public void testEquals()
    {
        Location all = Location.of( 18384141L, DRRC2, -108.06F, 37.6389F, SECOND_LOCATION );
        Location all2 = Location.of( 18384141L, DRRC2, -108.06F, 37.6389F, SECOND_LOCATION );
        Location all3 = Location.of( 18384141L, DRRC2, -108.06F, 37.6389F, SECOND_LOCATION );
        Location allDiffVectorID = Location.of( 1838141L, DRRC2, -108.06F, 37.6389F, SECOND_LOCATION );
        Location allDiffName = Location.of( 18384141L, DRRC3, -108.06F, 37.6389F, SECOND_LOCATION );
        Location allDiffCoordinates = Location.of( 18384141L, DRRC2, -108.106F, 37.63829F, SECOND_LOCATION );
        Location allDiffGage = Location.of( 18384141L, DRRC2, -108.06F, 37.6389F, FIRST_LOCATION );
        Location allDiff = Location.of( 1838425141L, DRRC3, -108.061F, 37.63859F, FIRST_LOCATION );

        Location name = Location.of( DRRC2 );
        Location name2 = Location.of( DRRC2 );
        Location name3 = Location.of( DRRC2 );
        Location diffName = Location.of( "DRRC5" );

        Location vID = Location.of( 18384141L );
        Location vID2 = Location.of( 18384141L );
        Location vID3 = Location.of( 18384141L );
        Location diffVID = Location.of( 1834584141L );

        Location latLon = Location.of( -108.06F, 37.6389F );
        Location latLon2 = Location.of( -108.06F, 37.6389F );
        Location latLon3 = Location.of( -108.06F, 37.6389F );
        Location diffLatLon = Location.of( -101.06F, 37.6389F );

        Location gage = Location.of( null, null, null, null, SECOND_LOCATION );
        Location gage2 = Location.of( null, null, null, null, SECOND_LOCATION );
        Location gage3 = Location.of( null, null, null, null, SECOND_LOCATION );
        Location diffGage = Location.of( null, null, null, null, "0916455000" );

        // Reflexive
        assertTrue( all.equals( all ) );
        assertTrue( name.equals( name ) );
        assertTrue( vID.equals( vID ) );
        assertTrue( gage.equals( gage ) );
        assertTrue( latLon.equals( latLon ) );

        // Symmetric
        assertTrue( all.equals( all2 ) );
        assertTrue( all2.equals( all ) );
        assertTrue( name.equals( name2 ) );
        assertTrue( name2.equals( name ) );
        assertTrue( vID.equals( vID2 ) );
        assertTrue( vID2.equals( vID ) );
        assertTrue( gage2.equals( gage ) );
        assertTrue( latLon.equals( latLon2 ) );
        assertTrue( latLon2.equals( latLon ) );

        // Transitive
        assertTrue( all.equals( all3 ) );
        assertTrue( all2.equals( all3 ) );

        assertTrue( name.equals( name3 ) );
        assertTrue( name2.equals( name3 ) );

        assertTrue( vID.equals( vID3 ) );
        assertTrue( vID2.equals( vID3 ) );

        assertTrue( gage.equals( gage3 ) );
        assertTrue( gage2.equals( gage3 ) );

        assertTrue( latLon.equals( latLon3 ) );
        assertTrue( latLon2.equals( latLon3 ) );

        // Consistent
        for ( int i = 0; i < 20; i++ )
        {
            assertTrue( all.equals( all2 ) );
        }

        // Unequal
        assertFalse( all.equals( allDiff ) );
        assertFalse( all.equals( allDiffName ) );
        assertFalse( all.equals( allDiffVectorID ) );
        assertFalse( all.equals( allDiffCoordinates ) );
        assertFalse( all.equals( allDiffGage ) );

        assertFalse( all.equals( name ) );

        assertFalse( all.equals( vID ) );

        assertFalse( all.equals( latLon ) );

        assertFalse( all.equals( gage ) );

        assertFalse( name.equals( diffName ) );

        assertFalse( vID.equals( diffVID ) );

        assertFalse( latLon.equals( diffLatLon ) );

        assertFalse( gage.equals( diffGage ) );

        // Null check
        assertNotEquals( null, all );

        // Other type check
        assertNotEquals( Double.valueOf( 2 ), all );
    }

    /**
     * Tests {@link Location#hashCode()}
     */
    @Test
    public void testHashCode()
    {
        // Equal
        Location all = Location.of( 18384141L, DRRC2, -108.06F, 37.6389F, SECOND_LOCATION );
        Location all2 = Location.of( 18384141L, DRRC2, -108.06F, 37.6389F, SECOND_LOCATION );
        assertTrue( all.hashCode() == all2.hashCode() );

        // Consistent
        for ( int i = 0; i < 20; i++ )
        {
            assertTrue( all.hashCode() == all2.hashCode() );
        }

        // Equal with Identifiers missing
        Location name = Location.of( DRRC2 );
        Location name2 = Location.of( DRRC2 );
        assertTrue( name.hashCode() == name2.hashCode() );

        Location vID = Location.of( 18384141L );
        Location vID2 = Location.of( 18384141L );
        assertTrue( vID.hashCode() == vID2.hashCode() );

        Location latLon = Location.of( -108.06F, 37.6389F );
        Location latLon2 = Location.of( -108.06F, 37.6389F );
        assertTrue( latLon.hashCode() == latLon2.hashCode() );

        Location gage = Location.of( null, null, null, null, SECOND_LOCATION );
        Location gage2 = Location.of( null, null, null, null, SECOND_LOCATION );
        assertTrue( gage.hashCode() == gage2.hashCode() );

        // Unequal
        Location allDiffVectorID = Location.of( 1838141L, DRRC2, -108.06F, 37.6389F, SECOND_LOCATION );
        assertFalse( all.hashCode() == allDiffVectorID.hashCode() );

        Location allDiffName = Location.of( 18384141L, DRRC3, -108.06F, 37.6389F, SECOND_LOCATION );
        assertFalse( all.hashCode() == allDiffName.hashCode() );

        Location allDiffCoordinates = Location.of( 18384141L, DRRC2, -108.106F, 37.63829F, SECOND_LOCATION );
        assertFalse( all.hashCode() == allDiffCoordinates.hashCode() );

        Location allDiffGage = Location.of( 18384141L, DRRC2, -108.06F, 37.6389F, FIRST_LOCATION );
        assertFalse( all.hashCode() == allDiffGage.hashCode() );

        Location allDiff = Location.of( 1838425141L, DRRC3, -108.061F, 37.63859F, FIRST_LOCATION );
        assertFalse( all.hashCode() == allDiff.hashCode() );


        // Unequal with some identifiers missing
        Location diffName = Location.of( "DRRC5" );
        assertFalse( name.hashCode() == diffName.hashCode() );

        Location diffVID = Location.of( 1834584141L );
        assertFalse( vID.hashCode() == diffVID.hashCode() );

        Location diffLatLon = Location.of( -101.06F, 37.6389F );
        assertFalse( latLon.hashCode() == diffLatLon.hashCode() );

        Location diffGage = Location.of( null, null, null, null, "0916455000" );
        assertFalse( gage.hashCode() == diffGage.hashCode() );

        // Unequal between partial and full objects
        assertFalse( all.hashCode() == gage.hashCode() );
        assertFalse( all.hashCode() == vID.hashCode() );
        assertFalse( all.hashCode() == name.hashCode() );
        assertFalse( all.hashCode() == latLon.hashCode() );

        // Other type check
        assertFalse( all.hashCode() == Double.valueOf( 2 ).hashCode() );
    }

}
