package gov.noaa.wres.datamodel;

import org.junit.Test;

public class PointTest
{
    @Test
    public void twoIdenticalPointsAreEqual()
    {
        WresPoint pointA = WresPointFactory.of(5,6,7);
        WresPoint pointB = WresPointFactory.of(5,6,7);
        // both ways, equals should work
        assert(pointA.equals(pointB));
        assert(pointB.equals(pointA));
        // also, since these have the same values, the cache should
        // cause them to be the identical same reference
        assert(pointA == pointB);
    }

    @Test
    public void twoSimilarPointsAreNotEqual()
    {
        WresPoint pointA = WresPointFactory.of(5);
        WresPoint pointB = WresPointFactory.of(5, 0);
        assert(!pointA.equals(pointB));
        assert(!pointB.equals(pointA));
        assert(pointA != pointB);
    }
}
