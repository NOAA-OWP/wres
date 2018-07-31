package wres.datamodel.metadata;

import static org.junit.Assert.assertFalse;
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

    /**
     * Tests {@link Location#equals(Object)}
     */
    @Test
    public void testEquals()
    {
        Location all = Location.of( 18384141L, "DRRC2", -108.06F, 37.6389F, "09165000" );
        Location all2 = Location.of( 18384141L, "DRRC2", -108.06F, 37.6389F, "09165000" );
        Location all3 = Location.of( 18384141L, "DRRC2", -108.06F, 37.6389F, "09165000" );
        Location allDiffVectorID = Location.of( 1838141L, "DRRC2", -108.06F, 37.6389F, "09165000" );
        Location allDiffName = Location.of( 18384141L, "DRRC3", -108.06F, 37.6389F, "09165000" );
        Location allDiffCoordinates = Location.of( 18384141L, "DRRC2", -108.106F, 37.63829F, "09165000" );
        Location allDiffGage = Location.of( 18384141L, "DRRC2", -108.06F, 37.6389F, "0916500" );
        Location allDiff = Location.of( 1838425141L, "DRRC3", -108.061F, 37.63859F, "0916500" );

        Location name = Location.of( "DRRC2" );
        Location name2 = Location.of( "DRRC2" );
        Location name3 = Location.of( "DRRC2" );
        Location diffName = Location.of( "DRRC5" );

        Location vID = Location.of( 18384141L );
        Location vID2 = Location.of( 18384141L );
        Location vID3 = Location.of( 18384141L );
        Location diffVID = Location.of( 1834584141L );

        Location latLon = Location.of( -108.06F, 37.6389F );
        Location latLon2 = Location.of( -108.06F, 37.6389F );
        Location latLon3 = Location.of( -108.06F, 37.6389F );
        Location diffLatLon = Location.of( -101.06F, 37.6389F );

        Location gage = Location.of( null, null, null, null, "09165000" );
        Location gage2 = Location.of( null, null, null, null, "09165000" );
        Location gage3 = Location.of( null, null, null, null, "09165000" );
        Location diffGage = Location.of( null, null, null, null, "0916455000" );

        // Reflexive
        assertTrue( "Unexpected inequality between two location identifier instances",
                    all.equals( all ) );
        assertTrue( "Unexpected inequality between two location identifier instances",
                    name.equals( name ) );
        assertTrue( "Unexpected inequality between two location identifier instances",
                    vID.equals( vID ) );
        assertTrue( "Unexpected inequality between two location identifier instances",
                    gage.equals( gage ) );
        assertTrue( "Unexpected inequality between two location identifier instances",
                    latLon.equals( latLon ) );

        // Symmetric
        assertTrue( "Unexpected inequality between two location identifier instances",
                    all.equals( all2 ) );
        assertTrue( "Unexpected inequality between two location identifier instances",
                    all2.equals( all ) );
        assertTrue( "Unexpected inequality between two location identifier instances",
                    name.equals( name2 ) );
        assertTrue( "Unexpected inequality between two location identifier instances",
                    name2.equals( name ) );
        assertTrue( "Unexpected inequality between two location identifier instances",
                    vID.equals( vID2 ) );
        assertTrue( "Unexpected inequality between two location identifier instances",
                    vID2.equals( vID ) );
        assertTrue( "Unexpected inequality between two location identifier instances",
                    gage2.equals( gage ) );
        assertTrue( "Unexpected inequality between two location identifier instances",
                    latLon.equals( latLon2 ) );
        assertTrue( "Unexpected inequality between two location identifier instances",
                    latLon2.equals( latLon ) );

        // Transitive
        assertTrue( "Unexpected inequality between two dataset identifier instances.",
                    all.equals( all3 ) );
        assertTrue( "Unexpected inequality between two dataset identifier instances.",
                    all2.equals( all3 ) );

        assertTrue( "Unexpected inequality between two dataset identifier instances.",
                    name.equals( name3 ) );
        assertTrue( "Unexpected inequality between two dataset identifier instances.",
                    name2.equals( name3 ) );

        assertTrue( "Unexpected inequality between two dataset identifier instances.",
                    vID.equals( vID3 ) );
        assertTrue( "Unexpected inequality between two dataset identifier instances.",
                    vID2.equals( vID3 ) );

        assertTrue( "Unexpected inequality between two dataset identifier instances.",
                    gage.equals( gage3 ) );
        assertTrue( "Unexpected inequality between two dataset identifier instances.",
                    gage2.equals( gage3 ) );

        assertTrue( "Unexpected inequality between two dataset identifier instances.",
                    latLon.equals( latLon3 ) );
        assertTrue( "Unexpected inequality between two dataset identifier instances.",
                    latLon2.equals( latLon3 ) );

        // Consistent
        for ( int i = 0; i < 20; i++ )
        {
            assertTrue( "Unexpected inequality between two dataset identifier instances.",
                        all.equals( all2 ) );
        }

        // Unequal
        assertFalse( "Unexpected equality between two dataset identifier instances.",
                     all.equals( allDiff ) );
        assertFalse( "Unexpected equality between two dataset identifier instances.",
                     all.equals( allDiffName ) );
        assertFalse( "Unexpected equality between two dataset identifier instances.",
                     all.equals( allDiffVectorID ) );
        assertFalse( "Unexpected equality between two dataset identifier instances.",
                     all.equals( allDiffCoordinates ) );
        assertFalse( "Unexpected equality between two dataset identifier instances.",
                     all.equals( allDiffGage ) );

        assertFalse( "Unexpected equality between two dataset identifier instances.",
                     all.equals( name ) );

        assertFalse( "Unexpected equality between two dataset identifier instances.",
                     all.equals( vID ) );

        assertFalse( "Unexpected equality between two dataset identifier instances.",
                     all.equals( latLon ) );

        assertFalse( "Unexpected equality between two dataset identifier instances.",
                     all.equals( gage ) );

        assertFalse( "Unexpected equality between two dataset identifier instances.",
                     name.equals( diffName ) );

        assertFalse( "Unexpected equality between two dataset identifier instances.",
                     vID.equals( diffVID ) );

        assertFalse( "Unexpected equality between two dataset identifier instances.",
                     latLon.equals( diffLatLon ) );

        assertFalse( "Unexpected equality between two dataset identifier instances.",
                     gage.equals( diffGage ) );

        // Null check
        assertFalse( "Unexpected equality between two dataset identifier instances.",
                     all.equals( null ) );

        // Other type check
        assertFalse( "Unexpected equality between two dataset identifier instances.",
                     all.equals( Double.valueOf( 2 ) ) );
    }

    /**
     * Tests {@link Location#hashCode()}
     */
    @Test
    public void testHashCode()
    {
        // Equal
        Location all = Location.of( 18384141L, "DRRC2", -108.06F, 37.6389F, "09165000" );
        Location all2 = Location.of( 18384141L, "DRRC2", -108.06F, 37.6389F, "09165000" );
        assertTrue( "Unexpected inequality between two location hashcodes.", all.hashCode() == all2.hashCode() );

        // Consistent
        for ( int i = 0; i < 20; i++ )
        {
            assertTrue( "Unexpected inequality between two location hashcodes.", all.hashCode() == all2.hashCode() );
        }

        // Equal with Identifiers missing
        Location name = Location.of( "DRRC2" );
        Location name2 = Location.of( "DRRC2" );
        assertTrue( "Unexpected inequality between two location hashcodes.", name.hashCode() == name2.hashCode() );

        Location vID = Location.of( 18384141L );
        Location vID2 = Location.of( 18384141L );
        assertTrue( "Unexpected inequality between two location hashcodes.", vID.hashCode() == vID2.hashCode() );

        Location latLon = Location.of( -108.06F, 37.6389F );
        Location latLon2 = Location.of( -108.06F, 37.6389F );
        assertTrue( "Unexpected inequality between two location hashcodes.", latLon.hashCode() == latLon2.hashCode() );

        Location gage = Location.of( null, null, null, null, "09165000" );
        Location gage2 = Location.of( null, null, null, null, "09165000" );
        assertTrue( "Unexpected inequality between two location hashcodes.",
                    gage.hashCode() == gage2.hashCode() );

        // Unequal
        Location allDiffVectorID = Location.of( 1838141L, "DRRC2", -108.06F, 37.6389F, "09165000" );
        assertFalse( "Unexpected equality between two location hashcodes.",
                     all.hashCode() == allDiffVectorID.hashCode() );

        Location allDiffName = Location.of( 18384141L, "DRRC3", -108.06F, 37.6389F, "09165000" );
        assertFalse( "Unexpected equality between two location hashcodes.",
                     all.hashCode() == allDiffName.hashCode() );

        Location allDiffCoordinates = Location.of( 18384141L, "DRRC2", -108.106F, 37.63829F, "09165000" );
        assertFalse( "Unexpected equality between two location hashcodes.",
                     all.hashCode() == allDiffCoordinates.hashCode() );

        Location allDiffGage = Location.of( 18384141L, "DRRC2", -108.06F, 37.6389F, "0916500" );
        assertFalse( "Unexpected equality between two location hashcodes.",
                     all.hashCode() == allDiffGage.hashCode() );

        Location allDiff = Location.of( 1838425141L, "DRRC3", -108.061F, 37.63859F, "0916500" );
        assertFalse( "Unexpected equality between two location hashcodes.",
                     all.hashCode() == allDiff.hashCode() );


        // Unequal with some identifiers missing
        Location diffName = Location.of( "DRRC5" );
        assertFalse( "Unexpected equality between two location hashcodes.",
                     name.hashCode() == diffName.hashCode() );

        Location diffVID = Location.of( 1834584141L );
        assertFalse( "Unexpected equality between two location hashcodes.",
                     vID.hashCode() == diffVID.hashCode() );

        Location diffLatLon = Location.of( -101.06F, 37.6389F );
        assertFalse( "Unexpected equality between two location hashcodes.",
                     latLon.hashCode() == diffLatLon.hashCode() );

        Location diffGage = Location.of( null, null, null, null, "0916455000" );
        assertFalse( "Unexpected equality between two location hashcodes.",
                     gage.hashCode() == diffGage.hashCode() );

        // Unequal between partial and full objects
        assertFalse( "Unexpected equality between two location hashcodes.",
                     all.hashCode() == gage.hashCode() );
        assertFalse( "Unexpected equality between two location hashcodes.",
                     all.hashCode() == vID.hashCode() );
        assertFalse( "Unexpected equality between two location hashcodes.",
                     all.hashCode() == name.hashCode() );
        assertFalse( "Unexpected equality between two location hashcodes.",
                     all.hashCode() == latLon.hashCode() );

        // Other type check
        assertFalse( "Unexpected equality between two location hashcodes",
                     all.hashCode() == Double.valueOf( 2 ).hashCode() );
    }

}
