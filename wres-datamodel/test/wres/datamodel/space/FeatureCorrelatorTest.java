package wres.datamodel.space;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import wres.datamodel.messages.MessageFactory;
import wres.datamodel.pools.PoolMetadata;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.GeometryGroup;
import wres.statistics.generated.GeometryTuple;

/**
 * Tests the {@link FeatureCorrelator}.
 * @author James Brown
 */

class FeatureCorrelatorTest
{
    /** Test instance. */
    private FeatureCorrelator testInstance;

    private static final Geometry GEOMETRY = MessageFactory.getGeometry( "DRRC2" );
    private static final Geometry ANOTHER_GEOMETRY = MessageFactory.getGeometry( "DRRC3" );
    private static final Feature FEATURE = Feature.of( GEOMETRY );
    private static final Feature ANOTHER_FEATURE = Feature.of( ANOTHER_GEOMETRY );

    @BeforeEach
    void runBeforeEachTest()
    {
        Set<FeatureTuple> features =
                Set.of( FeatureTuple.of( MessageFactory.getGeometryTuple( MessageFactory.getGeometry( "a" ),
                                                                          MessageFactory.getGeometry( "b" ),
                                                                          MessageFactory.getGeometry( "c" ) ) ),
                        FeatureTuple.of( MessageFactory.getGeometryTuple( MessageFactory.getGeometry( "d" ),
                                                                          MessageFactory.getGeometry( "e" ),
                                                                          null ) ),
                        FeatureTuple.of( MessageFactory.getGeometryTuple( MessageFactory.getGeometry( "f" ),
                                                                          MessageFactory.getGeometry( "g" ),
                                                                          MessageFactory.getGeometry( "h" ) ) ),
                        FeatureTuple.of( MessageFactory.getGeometryTuple( MessageFactory.getGeometry( "i" ),
                                                                          MessageFactory.getGeometry( "j" ),
                                                                          null ) ),
                        FeatureTuple.of( MessageFactory.getGeometryTuple( MessageFactory.getGeometry( "k" ),
                                                                          MessageFactory.getGeometry( "l" ),
                                                                          MessageFactory.getGeometry( "m" ) ) ) );

        this.testInstance = FeatureCorrelator.of( features );
    }

    @Test
    void testGetLeftForRightFeature()
    {
        assertAll( () -> assertEquals( Set.of( Feature.of( MessageFactory.getGeometry( "a" ) ) ),
                                       this.testInstance.getLeftForRightFeature( Feature.of( MessageFactory.getGeometry( "b" ) ) ) ),
                   () -> assertEquals( Set.of( Feature.of( MessageFactory.getGeometry( "d" ) ) ),
                                       this.testInstance.getLeftForRightFeature( Feature.of( MessageFactory.getGeometry( "e" ) ) ) ),
                   () -> assertEquals( Set.of( Feature.of( MessageFactory.getGeometry( "f" ) ) ),
                                       this.testInstance.getLeftForRightFeature( Feature.of( MessageFactory.getGeometry( "g" ) ) ) ),
                   () -> assertEquals( Set.of( Feature.of( MessageFactory.getGeometry( "i" ) ) ),
                                       this.testInstance.getLeftForRightFeature( Feature.of( MessageFactory.getGeometry( "j" ) ) ) ),
                   () -> assertEquals( Set.of( Feature.of( MessageFactory.getGeometry( "k" ) ) ),
                                       this.testInstance.getLeftForRightFeature( Feature.of( MessageFactory.getGeometry( "l" ) ) ) ) );

    }

    @Test
    void testGetLeftForBaselineFeature()
    {
        assertAll( () -> assertEquals( Set.of( Feature.of( MessageFactory.getGeometry( "a" ) ) ),
                                       this.testInstance.getLeftForBaselineFeature( Feature.of( MessageFactory.getGeometry( "c" ) ) ) ),
                   () -> assertEquals( Set.of( Feature.of( MessageFactory.getGeometry( "f" ) ) ),
                                       this.testInstance.getLeftForBaselineFeature( Feature.of( MessageFactory.getGeometry( "h" ) ) ) ),
                   () -> assertEquals( Set.of( Feature.of( MessageFactory.getGeometry( "k" ) ) ),
                                       this.testInstance.getLeftForBaselineFeature( Feature.of( MessageFactory.getGeometry( "m" ) ) ) ) );

    }

    @Test
    void testGetFeatureTuplesForLeftFeature()
    {
        assertAll( () -> assertEquals( Set.of( FeatureTuple.of( MessageFactory.getGeometryTuple( MessageFactory.getGeometry( "f" ),
                                                                                                 MessageFactory.getGeometry( "g" ),
                                                                                                 MessageFactory.getGeometry( "h" ) ) ) ),
                                       this.testInstance.getFeatureTuplesForLeftFeature( Feature.of( MessageFactory.getGeometry( "f" ) ) ) ) );

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
