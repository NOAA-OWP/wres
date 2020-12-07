package wres.datamodel;

import org.junit.Test;

import static org.apache.commons.math3.util.Precision.EPSILON;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class FeatureKeyTest
{
    @Test
    public void testParsePointFromPointWkt()
    {
        String wkt = "POINT ( 3.141592654 5.1 )";
        FeatureKey.GeoPoint point = FeatureKey.getLonLatFromPointWkt( wkt );
        assertEquals( point.getX(), 3.141592654, EPSILON );
        assertEquals( point.getY(), 5.1, EPSILON );
    }


    @Test
    public void testParsePointFromPointWktNonStrict()
    {
        String wkt = "POINT ( 3.141592654 5.1 )";
        FeatureKey.GeoPoint point = FeatureKey.getLonLatOrNullFromWkt( wkt );
        assertNotNull( point );
        assertEquals( point.getX(), 3.141592654, EPSILON );
        assertEquals( point.getY(), 5.1, EPSILON );
    }
}
