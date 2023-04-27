package wres.datamodel.space;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import wres.datamodel.messages.MessageFactory;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.GeometryTuple;

/**
 * Tests the {@link FeatureCorrelator}.
 * @author James Brown
 */

class FeatureCorrelatorTest
{
    /** Test instance. */
    private FeatureCorrelator testInstance;

    private static final Geometry GEOMETRY = wres.statistics.MessageFactory.getGeometry( "DRRC2" );
    private static final Geometry ANOTHER_GEOMETRY = wres.statistics.MessageFactory.getGeometry( "DRRC3" );
    private static final Feature FEATURE = Feature.of( GEOMETRY );
    private static final Feature ANOTHER_FEATURE = Feature.of( ANOTHER_GEOMETRY );

    @BeforeEach
    void runBeforeEachTest()
    {
        Set<FeatureTuple> features =
                Set.of( FeatureTuple.of( wres.statistics.MessageFactory.getGeometryTuple( wres.statistics.MessageFactory.getGeometry( "a" ),
                                                                                          wres.statistics.MessageFactory.getGeometry( "b" ),
                                                                                          wres.statistics.MessageFactory.getGeometry( "c" ) ) ),
                        FeatureTuple.of( wres.statistics.MessageFactory.getGeometryTuple( wres.statistics.MessageFactory.getGeometry( "d" ),
                                                                                          wres.statistics.MessageFactory.getGeometry( "e" ),
                                                                                          null ) ),
                        FeatureTuple.of( wres.statistics.MessageFactory.getGeometryTuple( wres.statistics.MessageFactory.getGeometry( "f" ),
                                                                                          wres.statistics.MessageFactory.getGeometry( "g" ),
                                                                                          wres.statistics.MessageFactory.getGeometry( "h" ) ) ),
                        FeatureTuple.of( wres.statistics.MessageFactory.getGeometryTuple( wres.statistics.MessageFactory.getGeometry( "i" ),
                                                                                          wres.statistics.MessageFactory.getGeometry( "j" ),
                                                                                          null ) ),
                        FeatureTuple.of( wres.statistics.MessageFactory.getGeometryTuple( wres.statistics.MessageFactory.getGeometry( "k" ),
                                                                                          wres.statistics.MessageFactory.getGeometry( "l" ),
                                                                                          wres.statistics.MessageFactory.getGeometry( "m" ) ) ) );

        this.testInstance = FeatureCorrelator.of( features );
    }

    @Test
    void testGetLeftForRightFeature()
    {
        assertAll( () -> assertEquals( Set.of( Feature.of( wres.statistics.MessageFactory.getGeometry( "a" ) ) ),
                                       this.testInstance.getLeftForRightFeature( Feature.of( wres.statistics.MessageFactory.getGeometry( "b" ) ) ) ),
                   () -> assertEquals( Set.of( Feature.of( wres.statistics.MessageFactory.getGeometry( "d" ) ) ),
                                       this.testInstance.getLeftForRightFeature( Feature.of( wres.statistics.MessageFactory.getGeometry( "e" ) ) ) ),
                   () -> assertEquals( Set.of( Feature.of( wres.statistics.MessageFactory.getGeometry( "f" ) ) ),
                                       this.testInstance.getLeftForRightFeature( Feature.of( wres.statistics.MessageFactory.getGeometry( "g" ) ) ) ),
                   () -> assertEquals( Set.of( Feature.of( wres.statistics.MessageFactory.getGeometry( "i" ) ) ),
                                       this.testInstance.getLeftForRightFeature( Feature.of( wres.statistics.MessageFactory.getGeometry( "j" ) ) ) ),
                   () -> assertEquals( Set.of( Feature.of( wres.statistics.MessageFactory.getGeometry( "k" ) ) ),
                                       this.testInstance.getLeftForRightFeature( Feature.of( wres.statistics.MessageFactory.getGeometry( "l" ) ) ) ) );

    }

    @Test
    void testGetLeftForBaselineFeature()
    {
        assertAll( () -> assertEquals( Set.of( Feature.of( wres.statistics.MessageFactory.getGeometry( "a" ) ) ),
                                       this.testInstance.getLeftForBaselineFeature( Feature.of( wres.statistics.MessageFactory.getGeometry( "c" ) ) ) ),
                   () -> assertEquals( Set.of( Feature.of( wres.statistics.MessageFactory.getGeometry( "f" ) ) ),
                                       this.testInstance.getLeftForBaselineFeature( Feature.of( wres.statistics.MessageFactory.getGeometry( "h" ) ) ) ),
                   () -> assertEquals( Set.of( Feature.of( wres.statistics.MessageFactory.getGeometry( "k" ) ) ),
                                       this.testInstance.getLeftForBaselineFeature( Feature.of( wres.statistics.MessageFactory.getGeometry( "m" ) ) ) ) );

    }

    @Test
    void testGetFeatureTuplesForLeftFeature()
    {
        assertAll( () -> assertEquals( Set.of( FeatureTuple.of( wres.statistics.MessageFactory.getGeometryTuple( wres.statistics.MessageFactory.getGeometry( "f" ),
                                                                                                                 wres.statistics.MessageFactory.getGeometry( "g" ),
                                                                                                                 wres.statistics.MessageFactory.getGeometry( "h" ) ) ) ),
                                       this.testInstance.getFeatureTuplesForLeftFeature( Feature.of( wres.statistics.MessageFactory.getGeometry( "f" ) ) ) ) );

    }

    @Test
    void testGetLeftForRightFeatureWithDuplicateRightFeatures()
    {
        GeometryTuple geoTuple = MessageFactory.getGeometryTuple( FEATURE, FEATURE, FEATURE );
        FeatureTuple featureTuple = FeatureTuple.of( geoTuple );
        GeometryTuple anotherGeoTuple = MessageFactory.getGeometryTuple( ANOTHER_FEATURE, FEATURE, FEATURE );
        FeatureTuple anotherFeatureTuple = FeatureTuple.of( anotherGeoTuple );

        Set<FeatureTuple> featureTuples = Set.of( featureTuple, anotherFeatureTuple );

        FeatureCorrelator correlator = FeatureCorrelator.of( featureTuples );

        assertEquals( Set.of( FEATURE, ANOTHER_FEATURE ), correlator.getLeftForRightFeature( FEATURE ) );
    }

    @Test
    void testGetFeatureTuplesForLeftFeatureWithDuplicateRightFeatures()
    {
        GeometryTuple geoTuple = MessageFactory.getGeometryTuple( FEATURE, FEATURE, FEATURE );
        FeatureTuple featureTuple = FeatureTuple.of( geoTuple );
        GeometryTuple anotherGeoTuple = MessageFactory.getGeometryTuple( ANOTHER_FEATURE, FEATURE, FEATURE );
        FeatureTuple anotherFeatureTuple = FeatureTuple.of( anotherGeoTuple );

        Set<FeatureTuple> featureTuples = Set.of( featureTuple, anotherFeatureTuple );

        FeatureCorrelator correlator = FeatureCorrelator.of( featureTuples );

        assertEquals( Set.of( anotherFeatureTuple ), correlator.getFeatureTuplesForLeftFeature( ANOTHER_FEATURE ) );
    }

    @Test
    void testGetLeftForRightFeatureWithDuplicateLeftFeatures()
    {
        GeometryTuple geoTuple = MessageFactory.getGeometryTuple( FEATURE, FEATURE, FEATURE );
        FeatureTuple featureTuple = FeatureTuple.of( geoTuple );
        GeometryTuple anotherGeoTuple = MessageFactory.getGeometryTuple( FEATURE, ANOTHER_FEATURE, FEATURE );
        FeatureTuple anotherFeatureTuple = FeatureTuple.of( anotherGeoTuple );

        Set<FeatureTuple> featureTuples = Set.of( featureTuple, anotherFeatureTuple );

        FeatureCorrelator correlator = FeatureCorrelator.of( featureTuples );

        assertEquals( Set.of( FEATURE ), correlator.getLeftForRightFeature( FEATURE ) );
    }

    @Test
    void testGetFeatureTuplesForLeftFeatureWithDuplicateLeftFeatures()
    {
        GeometryTuple geoTuple = MessageFactory.getGeometryTuple( FEATURE, FEATURE, FEATURE );
        FeatureTuple featureTuple = FeatureTuple.of( geoTuple );
        GeometryTuple anotherGeoTuple = MessageFactory.getGeometryTuple( FEATURE, ANOTHER_FEATURE, FEATURE );
        FeatureTuple anotherFeatureTuple = FeatureTuple.of( anotherGeoTuple );

        Set<FeatureTuple> featureTuples = Set.of( featureTuple, anotherFeatureTuple );

        FeatureCorrelator correlator = FeatureCorrelator.of( featureTuples );

        assertEquals( Set.of( featureTuple, anotherFeatureTuple ),
                      correlator.getFeatureTuplesForLeftFeature( FEATURE ) );
    }

}
