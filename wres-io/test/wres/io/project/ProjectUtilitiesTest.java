package wres.io.project;

import java.util.Collections;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;

import static org.junit.jupiter.api.Assertions.assertEquals;

import wres.config.VariableNames;
import wres.config.components.BaselineDataset;
import wres.config.components.BaselineDatasetBuilder;
import wres.config.components.Dataset;
import wres.config.components.DatasetBuilder;
import wres.config.components.EvaluationDeclaration;
import wres.config.components.EvaluationDeclarationBuilder;
import wres.config.components.FeatureGroups;
import wres.config.components.FeatureGroupsBuilder;
import wres.config.components.FeaturesBuilder;
import wres.config.components.SpatialMask;
import wres.config.components.SpatialMaskBuilder;
import wres.config.components.VariableBuilder;
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
        FeatureGroups featureGroups = FeatureGroupsBuilder.builder()
                                                          .geometryGroups( Collections.singleton( geoGroup ) )
                                                          .build();

        EvaluationDeclaration evaluation = EvaluationDeclarationBuilder.builder()
                                                                       .features( FeaturesBuilder.builder()
                                                                                                 .geometries( geometries )
                                                                                                 .build() )
                                                                       .featureGroups( featureGroups )
                                                                       .build();

        Set<FeatureTuple> featuresWithData = Set.of( FeatureTuple.of( leftRight ),
                                                     FeatureTuple.of( leftRightBaseline ),
                                                     FeatureTuple.of( leftRightBaselineOther ) );
        Set<FeatureTuple> groupedFeaturesWithData = Set.of( FeatureTuple.of( leftRight ),
                                                            FeatureTuple.of( leftRightBaseline ),
                                                            FeatureTuple.of( leftRightBaselineOther ) );

        ProjectUtilities.FeatureSets actual =
                ProjectUtilities.getFeatureGroups( featuresWithData, groupedFeaturesWithData, evaluation, 1 );

        assertEquals( 4, actual.featureGroups()
                               .size() );
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

    @Test
    void testGetVariableNamesWithObservedNameDeclaredAndPredictedAndBaselineNamesIntersected()
    {
        Set<String> observed = Set.of( "qux", "quux" );
        Set<String> predicted = Set.of( "foo", "bar" );
        Set<String> baseline = Set.of( "foo", "baz" );

        Dataset dataset = DatasetBuilder.builder()
                                        .variable( VariableBuilder.builder()
                                                                  .build() )
                                        .build();
        EvaluationDeclaration declaration
                = EvaluationDeclarationBuilder.builder()
                                              .left( DatasetBuilder.builder()
                                                                   .variable( VariableBuilder.builder()
                                                                                             .name( "qux" )
                                                                                             .build() )
                                                                   .build() )
                                              .right( dataset )
                                              .baseline( BaselineDatasetBuilder.builder()
                                                                               .dataset( dataset )
                                                                               .build() )
                                              .build();

        VariableNames actual = ProjectUtilities.getVariableNames( declaration,
                                                                  observed,
                                                                  predicted,
                                                                  baseline,
                                                                  Set.of() );

        VariableNames expected = new VariableNames( "qux", "foo", "foo", Set.of() );

        assertEquals( expected, actual );
    }

    @Test
    void testGetVariableNamesWithAllNamesIntersected()
    {
        Set<String> observed = Set.of( "foo", "qux" );
        Set<String> predicted = Set.of( "foo", "bar" );
        Set<String> baseline = Set.of( "foo", "baz" );

        Dataset dataset = DatasetBuilder.builder()
                                        .variable( VariableBuilder.builder()
                                                                  .build() )
                                        .build();
        EvaluationDeclaration declaration
                = EvaluationDeclarationBuilder.builder()
                                              .left( dataset )
                                              .right( dataset )
                                              .baseline( BaselineDatasetBuilder.builder()
                                                                               .dataset( dataset )
                                                                               .build() )
                                              .build();

        VariableNames actual = ProjectUtilities.getVariableNames( declaration,
                                                                  observed,
                                                                  predicted,
                                                                  baseline,
                                                                  Set.of() );

        VariableNames expected = new VariableNames( "foo", "foo", "foo", Set.of() );

        assertEquals( expected, actual );
    }

    @Test
    void testGetVariableNamesWithAllNamesIntersectedAndOneNameOnly()
    {
        Set<String> observed = Set.of( "foo" );
        Set<String> predicted = Set.of( "foo" );
        Set<String> baseline = Set.of( "foo" );

        Dataset dataset = DatasetBuilder.builder()
                                        .variable( VariableBuilder.builder()
                                                                  .build() )
                                        .build();
        EvaluationDeclaration declaration
                = EvaluationDeclarationBuilder.builder()
                                              .left( dataset )
                                              .right( dataset )
                                              .baseline( BaselineDatasetBuilder.builder()
                                                                               .dataset( dataset )
                                                                               .build() )
                                              .build();

        VariableNames actual = ProjectUtilities.getVariableNames( declaration,
                                                                  observed,
                                                                  predicted,
                                                                  baseline,
                                                                  Set.of() );

        VariableNames expected = new VariableNames( "foo", "foo", "foo", Set.of() );

        assertEquals( expected, actual );
    }

    @Test
    void testGetVariableNamesWithAllNamesDeclared()
    {
        Set<String> observed = Set.of( "foo" );
        Set<String> predicted = Set.of( "bar" );
        Set<String> baseline = Set.of( "baz" );

        BaselineDataset baselineDataset
                = BaselineDatasetBuilder.builder()
                                        .dataset( DatasetBuilder.builder()
                                                                .variable( VariableBuilder.builder()
                                                                                          .name( "baz" )
                                                                                          .build() )
                                                                .build() )
                                        .build();
        EvaluationDeclaration declaration
                = EvaluationDeclarationBuilder.builder()
                                              .left( DatasetBuilder.builder()
                                                                   .variable( VariableBuilder.builder()
                                                                                             .name( "foo" )
                                                                                             .build() )
                                                                   .build() )
                                              .right( DatasetBuilder.builder()
                                                                    .variable( VariableBuilder.builder()
                                                                                              .name( "bar" )
                                                                                              .build() )
                                                                    .build() )
                                              .baseline( baselineDataset )
                                              .build();

        VariableNames actual = ProjectUtilities.getVariableNames( declaration,
                                                                  observed,
                                                                  predicted,
                                                                  baseline,
                                                                  Set.of() );

        VariableNames expected = new VariableNames( "foo", "bar", "baz", Set.of() );

        assertEquals( expected, actual );
    }

    @Test
    void testGetVariableNamesWithAllNamesIntersectedAndEachNameUnique()
    {
        Set<String> observed = Set.of( "foo" );
        Set<String> predicted = Set.of( "bar" );
        Set<String> baseline = Set.of( "baz" );

        Dataset dataset = DatasetBuilder.builder()
                                        .variable( VariableBuilder.builder()
                                                                  .build() )
                                        .build();
        EvaluationDeclaration declaration
                = EvaluationDeclarationBuilder.builder()
                                              .left( dataset )
                                              .right( dataset )
                                              .baseline( BaselineDatasetBuilder.builder()
                                                                               .dataset( dataset )
                                                                               .build() )
                                              .build();

        VariableNames actual = ProjectUtilities.getVariableNames( declaration,
                                                                  observed,
                                                                  predicted,
                                                                  baseline,
                                                                  Set.of() );

        VariableNames expected = new VariableNames( "foo", "bar", "baz", Set.of() );

        assertEquals( expected, actual );
    }
}
