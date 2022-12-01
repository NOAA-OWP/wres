package wres.datamodel.space;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import wres.datamodel.messages.MessageFactory;

/**
 * Tests the {@link FeatureCorrelator}.
 * @author James Brown
 */

class FeatureCorrelatorTest
{
    /** Test instance. */
    private FeatureCorrelator testInstance;

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
        assertAll( () -> assertEquals( Feature.of( MessageFactory.getGeometry( "a" ) ),
                                       this.testInstance.getLeftForRightFeature( Feature.of( MessageFactory.getGeometry( "b" ) ) ) ),
                   () -> assertEquals( Feature.of( MessageFactory.getGeometry( "d" ) ),
                                       this.testInstance.getLeftForRightFeature( Feature.of( MessageFactory.getGeometry( "e" ) ) ) ),
                   () -> assertEquals( Feature.of( MessageFactory.getGeometry( "f" ) ),
                                       this.testInstance.getLeftForRightFeature( Feature.of( MessageFactory.getGeometry( "g" ) ) ) ),
                   () -> assertEquals( Feature.of( MessageFactory.getGeometry( "i" ) ),
                                       this.testInstance.getLeftForRightFeature( Feature.of( MessageFactory.getGeometry( "j" ) ) ) ),
                   () -> assertEquals( Feature.of( MessageFactory.getGeometry( "k" ) ),
                                       this.testInstance.getLeftForRightFeature( Feature.of( MessageFactory.getGeometry( "l" ) ) ) ) );

    }

    @Test
    void testGetLeftForBaselineFeature()
    {
        assertAll( () -> assertEquals( Feature.of( MessageFactory.getGeometry( "a" ) ),
                                       this.testInstance.getLeftForBaselineFeature( Feature.of( MessageFactory.getGeometry( "c" ) ) ) ),
                   () -> assertEquals( Feature.of( MessageFactory.getGeometry( "f" ) ),
                                       this.testInstance.getLeftForBaselineFeature( Feature.of( MessageFactory.getGeometry( "h" ) ) ) ),
                   () -> assertEquals( Feature.of( MessageFactory.getGeometry( "k" ) ),
                                       this.testInstance.getLeftForBaselineFeature( Feature.of( MessageFactory.getGeometry( "m" ) ) ) ) );

    }

    @Test
    void testGetFeatureTupleForLeftFeature()
    {
        assertAll( () -> assertEquals( FeatureTuple.of( MessageFactory.getGeometryTuple( MessageFactory.getGeometry( "f" ),
                                                                                         MessageFactory.getGeometry( "g" ),
                                                                                         MessageFactory.getGeometry( "h" ) ) ),
                                       this.testInstance.getFeatureTupleForLeftFeature( Feature.of( MessageFactory.getGeometry( "f" ) ) ) ) );

    }

    @Test
    void testGetLeftByRightFeatures()
    {
        Map<Feature, Feature> actual = this.testInstance.getLeftByRightFeatures();
        Map<Feature, Feature> expected = Map.of( Feature.of( MessageFactory.getGeometry( "b" ) ),
                                                 Feature.of( MessageFactory.getGeometry( "a" ) ),
                                                 Feature.of( MessageFactory.getGeometry( "e" ) ),
                                                 Feature.of( MessageFactory.getGeometry( "d" ) ),
                                                 Feature.of( MessageFactory.getGeometry( "g" ) ),
                                                 Feature.of( MessageFactory.getGeometry( "f" ) ),
                                                 Feature.of( MessageFactory.getGeometry( "j" ) ),
                                                 Feature.of( MessageFactory.getGeometry( "i" ) ),
                                                 Feature.of( MessageFactory.getGeometry( "l" ) ),
                                                 Feature.of( MessageFactory.getGeometry( "k" ) ) );

        assertEquals( expected, actual );
    }

    @Test
    void testGetLeftByBaselineFeatures()
    {
        Map<Feature, Feature> actual = this.testInstance.getLeftByBaselineFeatures();
        Map<Feature, Feature> expected = Map.of( Feature.of( MessageFactory.getGeometry( "c" ) ),
                                                 Feature.of( MessageFactory.getGeometry( "a" ) ),
                                                 Feature.of( MessageFactory.getGeometry( "h" ) ),
                                                 Feature.of( MessageFactory.getGeometry( "f" ) ),
                                                 Feature.of( MessageFactory.getGeometry( "m" ) ),
                                                 Feature.of( MessageFactory.getGeometry( "k" ) ) );

        assertEquals( expected, actual );
    }
}
