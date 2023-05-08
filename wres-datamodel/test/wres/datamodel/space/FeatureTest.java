package wres.datamodel.space;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import wres.statistics.generated.Geometry;

class FeatureTest
{
    @Test
    void testCompareTo()
    {
        Feature aFeature = Feature.of( Geometry.newBuilder().setName( "B" ).build() );
        Feature anotherFeature = Feature.of( Geometry.newBuilder().setName( "B" ).build() );
        assertEquals( aFeature, anotherFeature );
        assertEquals( 0, aFeature.compareTo( anotherFeature ) );
        Feature isGreater = Feature.of( Geometry.newBuilder().setName( "C" ).build() );
        assertTrue( isGreater.compareTo( aFeature ) > 0 );
        Feature isLess = Feature.of( Geometry.newBuilder().setName( "A" ).build() );
        assertTrue( isLess.compareTo( aFeature ) < 0 );
        Feature fullFeature = Feature.of( Geometry.newBuilder()
                                                  .setName( "B" )
                                                  .setWkt( "POINT ( 3.141592654 5.1 )" )
                                                  .setDescription( "Description" )
                                                  .setSrid( 4376 )
                                                  .build() );
        Feature anotherFullFeature = Feature.of( Geometry.newBuilder()
                                                  .setName( "B" )
                                                  .setWkt( "POINT ( 3.141592654 5.1 )" )
                                                  .setDescription( "Description" )
                                                  .setSrid( 4376 )
                                                  .build() );
        assertEquals( fullFeature, anotherFullFeature );
        assertEquals( 0, fullFeature.compareTo( anotherFullFeature ) );
        Feature aLesserFullFeature = Feature.of( Geometry.newBuilder()
                                                         .setName( "B" )
                                                         .setWkt( "POINT ( 3.141592654 5.1 )" )
                                                         .setDescription( "Description" )
                                                         .setSrid( 4375 )
                                                         .build() );
        assertTrue( aLesserFullFeature.compareTo( fullFeature ) < 0 );
    }
}
