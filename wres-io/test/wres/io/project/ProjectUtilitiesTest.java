package wres.io.project;

import java.util.Set;

import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;

import static org.junit.jupiter.api.Assertions.assertEquals;

import wres.config.yaml.components.EvaluationDeclaration;
import wres.config.yaml.components.EvaluationDeclarationBuilder;
import wres.config.yaml.components.FeatureGroups;
import wres.config.yaml.components.Features;
import wres.config.yaml.components.SpatialMask;
import wres.config.yaml.components.SpatialMaskBuilder;
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

    @Test
    void testFilterFeatures()
    {
        String wktOne = "POINT ( 1 1 ) ";
        Geometry geometryOne = Geometry.newBuilder()
                                       .setName( "foo" )
                                       .setWkt( wktOne )
                                       .build();
        GeometryTuple tupleOne = GeometryTuple.newBuilder()
                                              .setLeft( geometryOne )
                                              .setRight( geometryOne )
                                              .setBaseline( geometryOne )
                                              .build();
        String wktTwo = "POINT ( 2 2 ) ";
        Geometry geometryTwo = Geometry.newBuilder()
                                       .setName( "bar" )
                                       .setWkt( wktTwo )
                                       .build();
        GeometryTuple tupleTwo = GeometryTuple.newBuilder()
                                              .setLeft( geometryTwo )
                                              .setRight( geometryTwo )
                                              .setBaseline( geometryTwo )
                                              .build();
        String wktThree = "POINT ( 1 3 ) ";
        Geometry geometryThree = Geometry.newBuilder()
                                         .setName( "baz" )
                                         .setWkt( wktThree )
                                         .build();
        GeometryTuple tupleThree = GeometryTuple.newBuilder()
                                                .setLeft( geometryTwo )
                                                .setRight( geometryThree )
                                                .setBaseline( geometryTwo )
                                                .build();

        FeatureTuple featureTupleOne = FeatureTuple.of( tupleOne );
        FeatureTuple featureTupleTwo = FeatureTuple.of( tupleTwo );
        FeatureTuple featureTupleThree = FeatureTuple.of( tupleThree );

        GeometryFactory geometryFactory = new GeometryFactory();
        Coordinate one = new CoordinateXY( 1, 1 );
        Coordinate two = new CoordinateXY( 2, 1 );
        Coordinate three = new CoordinateXY( 2, 2 );
        Coordinate four = new CoordinateXY( 1, 2 );
        Coordinate five = new CoordinateXY( 1, 1 );
        Polygon mask = geometryFactory.createPolygon( new Coordinate[] { one, two, three, four, five } );
        SpatialMask spatialMask = SpatialMaskBuilder.builder()
                                                    .geometry( mask )
                                                    .build();

        Set<FeatureTuple> tuples = Set.of( featureTupleOne, featureTupleTwo, featureTupleThree );

        Set<FeatureTuple> actual = ProjectUtilities.filterFeatures( tuples, spatialMask );
        Set<FeatureTuple> expected = Set.of( featureTupleOne, featureTupleTwo );

        assertEquals( expected, actual );
    }

    @Test
    void testFilterFeatureGroups()
    {
        String wktOne = "POINT ( 1 1 ) ";
        Geometry geometryOne = Geometry.newBuilder()
                                       .setName( "foo" )
                                       .setWkt( wktOne )
                                       .build();
        GeometryTuple tupleOne = GeometryTuple.newBuilder()
                                              .setLeft( geometryOne )
                                              .setRight( geometryOne )
                                              .setBaseline( geometryOne )
                                              .build();
        String wktTwo = "POINT ( 2 2 ) ";
        Geometry geometryTwo = Geometry.newBuilder()
                                       .setName( "bar" )
                                       .setWkt( wktTwo )
                                       .build();
        GeometryTuple tupleTwo = GeometryTuple.newBuilder()
                                              .setLeft( geometryTwo )
                                              .setRight( geometryTwo )
                                              .setBaseline( geometryTwo )
                                              .build();
        String wktThree = "POINT ( 1 3 ) ";
        Geometry geometryThree = Geometry.newBuilder()
                                         .setName( "baz" )
                                         .setWkt( wktThree )
                                         .build();
        GeometryTuple tupleThree = GeometryTuple.newBuilder()
                                                .setLeft( geometryTwo )
                                                .setRight( geometryThree )
                                                .setBaseline( geometryTwo )
                                                .build();

        GeometryFactory geometryFactory = new GeometryFactory();
        Coordinate one = new CoordinateXY( 1, 1 );
        Coordinate two = new CoordinateXY( 2, 1 );
        Coordinate three = new CoordinateXY( 2, 2 );
        Coordinate four = new CoordinateXY( 1, 2 );
        Coordinate five = new CoordinateXY( 1, 1 );
        Polygon mask = geometryFactory.createPolygon( new Coordinate[] { one, two, three, four, five } );
        SpatialMask spatialMask = SpatialMaskBuilder.builder()
                                                    .geometry( mask )
                                                    .build();

        // Retained, unchanged
        GeometryGroup groupOne = GeometryGroup.newBuilder()
                                              .setRegionName( "foo" )
                                              .addGeometryTuples( tupleOne )
                                              .addGeometryTuples( tupleTwo )
                                              .build();

        // One feature removed
        GeometryGroup groupTwo = GeometryGroup.newBuilder()
                                              .setRegionName( "bar" )
                                              .addGeometryTuples( tupleOne )
                                              .addGeometryTuples( tupleTwo )
                                              .addGeometryTuples( tupleThree )
                                              .build();

        // Entire group removed
        GeometryGroup groupThree = GeometryGroup.newBuilder()
                                              .setRegionName( "baz" )
                                              .addGeometryTuples( tupleThree )
                                              .build();

        FeatureGroup featureGroupOne = FeatureGroup.of( groupOne );
        FeatureGroup featureGroupTwo = FeatureGroup.of( groupTwo );
        FeatureGroup featureGroupThree = FeatureGroup.of( groupThree );

        Set<FeatureGroup> groups = Set.of( featureGroupOne, featureGroupTwo, featureGroupThree );

        Set<FeatureGroup> actual = ProjectUtilities.filterFeatureGroups( groups, spatialMask );
        GeometryGroup expectedAdjusted = groupTwo.toBuilder()
                                                 .removeGeometryTuples( 2 )
                                                 .build();
        FeatureGroup expectedFeatureGroupAdjusted = FeatureGroup.of( expectedAdjusted );
        Set<FeatureGroup> expected = Set.of( featureGroupOne, expectedFeatureGroupAdjusted );

        assertEquals( expected, actual );
    }
}
