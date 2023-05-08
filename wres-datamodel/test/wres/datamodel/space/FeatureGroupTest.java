package wres.datamodel.space;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import wres.datamodel.messages.MessageFactory;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.GeometryGroup;
import wres.statistics.generated.GeometryTuple;

/**
 * Tests the {@link FeatureGroup}.
 * 
 * @author James Brown
 */

class FeatureGroupTest
{

    /** A feature group to test. **/
    private FeatureGroup aGroup;

    /** A feature tuple to test. **/
    private FeatureTuple aTuple;

    /** Another feature tuple to test. **/
    private FeatureTuple anotherTuple;

    @BeforeEach
    void runBeforeEachTest()
    {
        Geometry keyOne = wres.statistics.MessageFactory.getGeometry( "A" );
        Geometry keyTwo = wres.statistics.MessageFactory.getGeometry( "B" );
        Geometry keyThree = wres.statistics.MessageFactory.getGeometry( "C" );
        GeometryTuple geoTuple = wres.statistics.MessageFactory.getGeometryTuple( keyOne, keyTwo, keyThree );
        this.aTuple = FeatureTuple.of( geoTuple );
        GeometryGroup geoGroup = wres.statistics.MessageFactory.getGeometryGroup( "aGroup", geoTuple );
        this.aGroup = FeatureGroup.of( geoGroup );

        Geometry keyFour = wres.statistics.MessageFactory.getGeometry( "A", "a feature", null, null );
        GeometryTuple anotherGeoTuple = wres.statistics.MessageFactory.getGeometryTuple( keyOne, keyTwo, keyFour );
        this.anotherTuple = FeatureTuple.of( anotherGeoTuple );
    }

    @Test
    void testEquals()
    {
        // Reflexive 
        assertEquals( this.aGroup, this.aGroup );

        // Symmetric
        GeometryGroup anotherGeoGroup = MessageFactory.getGeometryGroup( "aGroup", this.aTuple );
        FeatureGroup anotherGroup = FeatureGroup.of( anotherGeoGroup );

        assertTrue( anotherGroup.equals( this.aGroup )
                    && this.aGroup.equals( anotherGroup ) );

        // Transitive
        GeometryGroup yetAnotherGeoGroup = MessageFactory.getGeometryGroup( "aGroup", this.aTuple );
        FeatureGroup yetAnotherGroup = FeatureGroup.of( yetAnotherGeoGroup );

        assertTrue( this.aGroup.equals( anotherGroup )
                    && anotherGroup.equals( yetAnotherGroup )
                    && this.aGroup.equals( yetAnotherGroup ) );

        // Consistent
        for ( int i = 0; i < 100; i++ )
        {
            assertEquals( aGroup, anotherGroup );
        }

        // Nullity
        assertNotEquals( null, this.aGroup );
        assertNotEquals( this.aGroup, null );

        // Unequal cases
        GeometryGroup oneMoreGeo = MessageFactory.getGeometryGroup( "anotherGroup", this.aTuple );
        FeatureGroup oneMoreGroup = FeatureGroup.of( oneMoreGeo );

        assertNotEquals( this.aGroup, oneMoreGroup );

        GeometryGroup yetOneMoreGeoGroup =
                MessageFactory.getGeometryGroup( "aGroup", Set.of( this.aTuple, this.anotherTuple ) );
        FeatureGroup yetOneMoreGroup = FeatureGroup.of( yetOneMoreGeoGroup );

        assertNotEquals( yetOneMoreGroup, this.aGroup );

        GeometryGroup oneLastGeoGroup =
                MessageFactory.getGeometryGroup( "aGroup", Set.of( this.aTuple, this.anotherTuple ) );
        FeatureGroup oneLastGroup = FeatureGroup.of( oneLastGeoGroup );

        assertNotEquals( oneLastGroup, this.aGroup );
    }

    @Test
    void testHashcode()
    {
        // Equals consistent with hashcode
        GeometryGroup anotherGeoGroup = MessageFactory.getGeometryGroup( "aGroup", this.aTuple );
        FeatureGroup anotherFeatureGroup = FeatureGroup.of( anotherGeoGroup );
        assertEquals( this.aGroup, anotherFeatureGroup );

        // Consistent
        for ( int i = 0; i < 100; i++ )
        {
            assertEquals( this.aGroup.hashCode(), anotherFeatureGroup.hashCode() );
        }
    }

    @Test
    void testCompareTo()
    {
        // Consistent with equals
        GeometryGroup aGeoGroup = MessageFactory.getGeometryGroup( "aGroup", this.aTuple );
        FeatureGroup aFeatureGroup = FeatureGroup.of( aGeoGroup );
        assertEquals( this.aGroup, aFeatureGroup );
        assertEquals( 0, this.aGroup.compareTo( aFeatureGroup ) );

        GeometryGroup anotherGeoGroup = MessageFactory.getGeometryGroup( "anotherName", this.aTuple );
        FeatureGroup anotherGroup = FeatureGroup.of( anotherGeoGroup );
        assertNotEquals( 0, this.aGroup.compareTo( anotherGroup ) );

        // Unequal size
        FeatureGroup smallerGroup = FeatureGroup.of( MessageFactory.getGeometryGroup( Set.of( this.aTuple ) ) );
        FeatureGroup biggerGroup =
                FeatureGroup.of( MessageFactory.getGeometryGroup( Set.of( this.aTuple, this.anotherTuple ) ) );
        assertTrue( smallerGroup.compareTo( biggerGroup ) < 0 );

        // Equal size, equal group name, lesser tuple name        
        Geometry keyOne = wres.statistics.MessageFactory.getGeometry( "A" );
        Geometry keyTwo = wres.statistics.MessageFactory.getGeometry( "B" );
        GeometryTuple geoTuple = wres.statistics.MessageFactory.getGeometryTuple( keyOne, keyTwo, null );
        GeometryTuple geoTupleTwo = wres.statistics.MessageFactory.getGeometryTuple( keyTwo, keyTwo, null );
        FeatureTuple featureTuple = FeatureTuple.of( geoTuple );
        FeatureTuple featureTupleTwo = FeatureTuple.of( geoTupleTwo );

        FeatureGroup lesserGroup = FeatureGroup.of( MessageFactory.getGeometryGroup( featureTuple ) );
        FeatureGroup greaterGroup = FeatureGroup.of( MessageFactory.getGeometryGroup( featureTupleTwo ) );

        assertTrue( greaterGroup.compareTo( lesserGroup ) > 0 );
    }

    @Test
    void testGetName()
    {
        assertEquals( "aGroup", this.aGroup.getName() );
    }

    @Test
    void testGetFeatures()
    {
        assertEquals( Set.of( this.aTuple ), this.aGroup.getFeatures() );
    }

    @Test
    void testFeatureGroupThrowsExpectedExceptionWhenNameTooLong()
    {
        GeometryGroup geoGroup = MessageFactory.getGeometryGroup( new String( new char[99] ), this.aTuple );
        IllegalArgumentException exception =
                assertThrows( IllegalArgumentException.class,
                              () -> FeatureGroup.of( geoGroup ) );

        String actualMessage = exception.getMessage();
        String expectedMessageStartsWith = "A feature group name cannot be longer than";

        assertTrue( actualMessage.startsWith( expectedMessageStartsWith ) );
    }

}
