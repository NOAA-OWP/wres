package wres.datamodel.space;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import wres.statistics.generated.Geometry;

class FeatureTest
{
    @Test
    void testCompareTo()
    {
        Feature aFeature = Feature.of( Geometry.newBuilder().setName( "B" ).build() );
        Feature anotherFeature = Feature.of( Geometry.newBuilder().setName( "B" ).build() );
        Assertions.assertEquals( aFeature, anotherFeature );
        Assertions.assertEquals( 0, aFeature.compareTo( anotherFeature ) );
        Feature isGreater = Feature.of( Geometry.newBuilder().setName( "C" ).build() );
        Assertions.assertTrue( isGreater.compareTo( aFeature ) > 0 );
        Feature isLess = Feature.of( Geometry.newBuilder().setName( "A" ).build() );
        Assertions.assertTrue( isLess.compareTo( aFeature ) < 0 );
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
        Assertions.assertEquals( fullFeature, anotherFullFeature );
        Assertions.assertEquals( 0, fullFeature.compareTo( anotherFullFeature ) );
        Feature aLesserFullFeature = Feature.of( Geometry.newBuilder()
                                                         .setName( "B" )
                                                         .setWkt( "POINT ( 3.141592654 5.1 )" )
                                                         .setDescription( "Description" )
                                                         .setSrid( 4375 )
                                                         .build() );
        Assertions.assertTrue( aLesserFullFeature.compareTo( fullFeature ) < 0 );
    }
}
