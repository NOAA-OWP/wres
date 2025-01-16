package wres.datamodel.space;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import wres.datamodel.messages.MessageFactory;
import wres.statistics.MessageUtilities;
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

    private static final Geometry GEOMETRY = MessageUtilities.getGeometry( "DRRC2" );
    private static final Geometry ANOTHER_GEOMETRY = MessageUtilities.getGeometry( "DRRC3" );
    private static final Feature FEATURE = Feature.of( GEOMETRY );
    private static final Feature ANOTHER_FEATURE = Feature.of( ANOTHER_GEOMETRY );

    @BeforeEach
    void runBeforeEachTest()
    {
        Set<FeatureTuple> features =
                Set.of( FeatureTuple.of( MessageUtilities.getGeometryTuple( MessageUtilities.getGeometry( "a" ),
                                                                            MessageUtilities.getGeometry( "b" ),
                                                                            MessageUtilities.getGeometry( "c" ) ) ),
                        FeatureTuple.of( MessageUtilities.getGeometryTuple( MessageUtilities.getGeometry( "d" ),
                                                                            MessageUtilities.getGeometry( "e" ),
                                                                            null ) ),
                        FeatureTuple.of( MessageUtilities.getGeometryTuple( MessageUtilities.getGeometry( "f" ),
                                                                            MessageUtilities.getGeometry( "g" ),
                                                                            MessageUtilities.getGeometry( "h" ) ) ),
                        FeatureTuple.of( MessageUtilities.getGeometryTuple( MessageUtilities.getGeometry( "i" ),
                                                                            MessageUtilities.getGeometry( "j" ),
                                                                            null ) ),
                        FeatureTuple.of( MessageUtilities.getGeometryTuple( MessageUtilities.getGeometry( "k" ),
                                                                            MessageUtilities.getGeometry( "l" ),
                                                                            MessageUtilities.getGeometry( "m" ) ) ) );

        this.testInstance = FeatureCorrelator.of( features );
    }

    @Test
    void testGetLeftForRightFeature()
    {
        assertAll( () -> assertEquals( Set.of( Feature.of( MessageUtilities.getGeometry( "a" ) ) ),
                                       this.testInstance.getLeftForRightFeature( Feature.of( MessageUtilities.getGeometry( "b" ) ) ) ),
                   () -> assertEquals( Set.of( Feature.of( MessageUtilities.getGeometry( "d" ) ) ),
                                       this.testInstance.getLeftForRightFeature( Feature.of( MessageUtilities.getGeometry( "e" ) ) ) ),
                   () -> assertEquals( Set.of( Feature.of( MessageUtilities.getGeometry( "f" ) ) ),
                                       this.testInstance.getLeftForRightFeature( Feature.of( MessageUtilities.getGeometry( "g" ) ) ) ),
                   () -> assertEquals( Set.of( Feature.of( MessageUtilities.getGeometry( "i" ) ) ),
                                       this.testInstance.getLeftForRightFeature( Feature.of( MessageUtilities.getGeometry( "j" ) ) ) ),
                   () -> assertEquals( Set.of( Feature.of( MessageUtilities.getGeometry( "k" ) ) ),
                                       this.testInstance.getLeftForRightFeature( Feature.of( MessageUtilities.getGeometry( "l" ) ) ) ) );

    }

    @Test
    void testGetLeftForBaselineFeature()
    {
        assertAll( () -> assertEquals( Set.of( Feature.of( MessageUtilities.getGeometry( "a" ) ) ),
                                       this.testInstance.getLeftForBaselineFeature( Feature.of( MessageUtilities.getGeometry( "c" ) ) ) ),
                   () -> assertEquals( Set.of( Feature.of( MessageUtilities.getGeometry( "f" ) ) ),
                                       this.testInstance.getLeftForBaselineFeature( Feature.of( MessageUtilities.getGeometry( "h" ) ) ) ),
                   () -> assertEquals( Set.of( Feature.of( MessageUtilities.getGeometry( "k" ) ) ),
                                       this.testInstance.getLeftForBaselineFeature( Feature.of( MessageUtilities.getGeometry( "m" ) ) ) ) );

    }

    @Test
    void testGetFeatureTuplesForLeftFeature()
    {
        assertAll( () -> assertEquals( Set.of( FeatureTuple.of( MessageUtilities.getGeometryTuple( MessageUtilities.getGeometry( "f" ),
                                                                                                   MessageUtilities.getGeometry( "g" ),
                                                                                                   MessageUtilities.getGeometry( "h" ) ) ) ),
                                       this.testInstance.getFeatureTuplesForLeftFeature( Feature.of( MessageUtilities.getGeometry( "f" ) ) ) ) );

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
