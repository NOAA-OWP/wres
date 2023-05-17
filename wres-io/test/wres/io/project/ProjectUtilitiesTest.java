package wres.io.project;

import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import wres.config.yaml.components.EvaluationDeclaration;
import wres.config.yaml.components.EvaluationDeclarationBuilder;
import wres.config.yaml.components.FeatureGroups;
import wres.config.yaml.components.Features;
import wres.config.yaml.components.UnitAlias;
import wres.datamodel.space.FeatureGroup;
import wres.datamodel.space.FeatureTuple;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.GeometryGroup;
import wres.statistics.generated.GeometryTuple;

/**
 * @author James Brown
 */
class ProjectUtilitiesTest
{
    /**
     * Redmine #116312.
     */

    @Test
    void testGetFeatureGroups()
    {
        GeometryTuple leftRight = GeometryTuple.newBuilder()
                                               .setLeft( Geometry.newBuilder()
                                                                 .setName( "foo" ) )
                                               .setRight( Geometry.newBuilder()
                                                                  .setName( "foo" ) )
                                               .build();
        GeometryTuple leftRightBaseline = GeometryTuple.newBuilder()
                                                       .setLeft( Geometry.newBuilder()
                                                                         .setName( "foo" ) )
                                                       .setRight( Geometry.newBuilder()
                                                                          .setName( "foo" ) )
                                                       .setBaseline( Geometry.newBuilder()
                                                                             .setName( "bar" ) )
                                                       .build();
        GeometryTuple leftRightBaselineOther = GeometryTuple.newBuilder()
                                                       .setLeft( Geometry.newBuilder()
                                                                         .setName( "foo" ) )
                                                       .setRight( Geometry.newBuilder()
                                                                          .setName( "foo" ) )
                                                       .setBaseline( Geometry.newBuilder()
                                                                             .setName( "baz" ) )
                                                       .build();

        Set<GeometryTuple> geometries = Set.of( leftRight, leftRightBaseline, leftRightBaselineOther );
        GeometryGroup geoGroup = GeometryGroup.newBuilder()
                                              .addAllGeometryTuples( geometries )
                                              .build();
        FeatureGroups featureGroups = new FeatureGroups( Set.of( geoGroup ) );
        EvaluationDeclaration evaluation = EvaluationDeclarationBuilder.builder()
                                                                       .features( new Features( geometries ) )
                                                                       .featureGroups( featureGroups )
                                                                       .build();

        Set<FeatureTuple> featuresWithData = Set.of( FeatureTuple.of( leftRight ),
                                                     FeatureTuple.of( leftRightBaseline ),
                                                     FeatureTuple.of( leftRightBaselineOther ) );
        Set<FeatureTuple> groupedFeaturesWithData = Set.of( FeatureTuple.of( leftRight ),
                                                            FeatureTuple.of( leftRightBaseline ),
                                                            FeatureTuple.of( leftRightBaselineOther ) );

        Set<FeatureGroup> actual =
                ProjectUtilities.getFeatureGroups( featuresWithData, groupedFeaturesWithData, evaluation, 1 );

        assertEquals( 4, actual.size() );
    }
}
