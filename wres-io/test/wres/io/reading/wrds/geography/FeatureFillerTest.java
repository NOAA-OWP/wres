package wres.io.reading.wrds.geography;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertEquals;

import wres.config.yaml.components.BaselineDataset;
import wres.config.yaml.components.BaselineDatasetBuilder;
import wres.config.yaml.components.Dataset;
import wres.config.yaml.components.DatasetBuilder;
import wres.config.yaml.components.EvaluationDeclaration;
import wres.config.yaml.components.EvaluationDeclarationBuilder;
import wres.config.yaml.components.FeatureAuthority;
import wres.config.yaml.components.FeatureService;
import wres.config.yaml.components.FeatureServiceGroup;
import wres.config.yaml.components.Features;
import wres.config.yaml.components.UnitAlias;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.GeometryGroup;
import wres.statistics.generated.GeometryTuple;

/**
 * Tests the {@link FeatureFiller}.
 */

class FeatureFillerTest
{
    private static final Dataset BOILERPLATE_LEFT_DATASOURCE_NO_DIMENSION = DatasetBuilder.builder()
                                                                                          .build();

    private static final Dataset BOILERPLATE_RIGHT_DATASOURCE_NO_DIMENSION = DatasetBuilder.builder()
                                                                                           .build();
    private static final BaselineDataset BOILERPLATE_BASELINE_DATASOURCE_NO_AUTHORITY =
            BaselineDatasetBuilder.builder()
                                  .dataset( DatasetBuilder.builder()
                                                          .build() )
                                  .build();
    private static final Dataset BOILERPLATE_DATASOURCE_USGS_SITE_CODE_AUTHORITY =
            DatasetBuilder.builder()
                          .featureAuthority( FeatureAuthority.USGS_SITE_CODE )
                          .build();
    private static final Dataset BOILERPLATE_DATASOURCE_NWS_LID_AUTHORITY =
            DatasetBuilder.builder()
                          .featureAuthority( FeatureAuthority.NWS_LID )
                          .build();

    private static final BaselineDataset BOILERPLATE_BASELINE_DATASOURCE_NWM_FEATURE_AUTHORITY =
            BaselineDatasetBuilder.builder()
                                  .dataset( DatasetBuilder.builder()
                                                          .featureAuthority( FeatureAuthority.NWM_FEATURE_ID )
                                                          .build() )
                                  .build();
    private static final GeometryTuple FULLY_DECLARED_FEATURE_ONE_NO_BASELINE =
            GeometryTuple.newBuilder()
                         .setLeft( Geometry.newBuilder()
                                           .setName( "02326550" ) )
                         .setRight( Geometry.newBuilder()
                                            .setName( "02326550" ) )
                         .build();
    private static final GeometryTuple FULLY_DECLARED_FEATURE_ONE_WITH_BASELINE =
            GeometryTuple.newBuilder()
                         .setLeft( Geometry.newBuilder()
                                           .setName( "02326550" ) )
                         .setRight( Geometry.newBuilder()
                                            .setName( "2287397" ) )
                         .setBaseline( Geometry.newBuilder()
                                               .setName( "NUTF1" ) )
                         .build();
    private static final GeometryTuple FULLY_DECLARED_FEATURE_TWO_NO_BASELINE =
            GeometryTuple.newBuilder()
                         .setLeft( Geometry.newBuilder()
                                           .setName( "09171100" ) )
                         .setRight( Geometry.newBuilder()
                                            .setName( "18382337" ) )
                         .build();
    private static final GeometryTuple FULLY_DECLARED_FEATURE_TWO_WITH_BASELINE =
            GeometryTuple.newBuilder()
                         .setLeft( Geometry.newBuilder()
                                           .setName( "09171100" ) )
                         .setRight( Geometry.newBuilder()
                                            .setName( "18382337" ) )
                         .setBaseline( Geometry.newBuilder()
                                               .setName( "DBDC2" ) )
                         .build();

    /**
     * When all features are fully declared (no baseline), meaning both left and right feature names are declared, the
     * resulting declaration should be the same as that passed in. No feature service required in this case.
     */

    @Test
    void testFullyDeclaredFeaturesPassesThrough()
    {
        Set<GeometryTuple> features = Set.of( FULLY_DECLARED_FEATURE_ONE_NO_BASELINE,
                                              FULLY_DECLARED_FEATURE_TWO_NO_BASELINE );
        EvaluationDeclaration expected
                = FeatureFillerTest.getBoilerplateEvaluationWith( features,
                                                                  null,
                                                                  BOILERPLATE_LEFT_DATASOURCE_NO_DIMENSION,
                                                                  BOILERPLATE_RIGHT_DATASOURCE_NO_DIMENSION,
                                                                  null );

        EvaluationDeclaration actual = FeatureFiller.fillFeatures( expected );
        assertEquals( expected, actual );
    }

    /**
     * When all features are fully declared with baseline, meaning left,right, and baseline feature names are declared,
     * the resulting declaration should be the same as that passed in. No feature service required in this case.
     */

    @Test
    void testFullyDeclaredFeaturesWithBaselinePassesThrough()
    {
        Set<GeometryTuple> features = Set.of( FULLY_DECLARED_FEATURE_ONE_WITH_BASELINE,
                                              FULLY_DECLARED_FEATURE_TWO_WITH_BASELINE );
        EvaluationDeclaration expected
                = FeatureFillerTest.getBoilerplateEvaluationWith( features,
                                                                  null,
                                                                  BOILERPLATE_LEFT_DATASOURCE_NO_DIMENSION,
                                                                  BOILERPLATE_RIGHT_DATASOURCE_NO_DIMENSION,
                                                                  BOILERPLATE_BASELINE_DATASOURCE_NO_AUTHORITY );

        EvaluationDeclaration actual = FeatureFiller.fillFeatures( expected );
        assertEquals( expected, actual );
    }

    @Test
    void testEvaluationWithoutExplicitFeaturesOrFeatureGroupsPassesThrough()
    {
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .label( "no features" )
                                                                        .build();

        assertEquals( declaration, FeatureFiller.fillFeatures( declaration ) );
    }

    @Test
    void testFillOutImplicitFeatureGroupUsingMockedFeatureService() throws URISyntaxException
    {
        URI uri = new URI( "https://some_fake_uri" );
        FeatureServiceGroup featureGroup = new FeatureServiceGroup( "state", "AL", true );
        FeatureService featureService = new FeatureService( uri, Set.of( featureGroup ) );

        EvaluationDeclaration evaluation
                = FeatureFillerTest.getBoilerplateEvaluationWith( null,
                                                                  featureService,
                                                                  BOILERPLATE_DATASOURCE_USGS_SITE_CODE_AUTHORITY,
                                                                  BOILERPLATE_DATASOURCE_NWS_LID_AUTHORITY,
                                                                  null );

        try ( MockedStatic<WrdsFeatureService> utilities = Mockito.mockStatic( WrdsFeatureService.class ) )
        {
            utilities.when( () -> WrdsFeatureService.read( Mockito.any() ) )
                     .thenReturn( List.of( new Location( "foo", "bar", "baz" ) ) );

            EvaluationDeclaration actualEvaluation = FeatureFiller.fillFeatures( evaluation );

            Set<GeometryGroup> actualGroup = actualEvaluation.featureGroups()
                                                             .geometryGroups();

            GeometryGroup expectedGroup =
                    GeometryGroup.newBuilder()
                                 .addAllGeometryTuples( Set.of( GeometryTuple.newBuilder()
                                                                             .setLeft( Geometry.newBuilder()
                                                                                               .setName( "bar" ) )
                                                                             .setRight( Geometry.newBuilder()
                                                                                                .setName( "baz" ) )
                                                                             .build() ) )
                                 .setRegionName( "AL" )
                                 .build();

            assertEquals( Set.of( expectedGroup ), actualGroup );
        }
    }

    @Test
    void testFillOutSparseFeaturesUsingMockedFeatureService() throws URISyntaxException
    {
        URI uri = new URI( "https://some_fake_uri" );
        FeatureService featureService = new FeatureService( uri, Set.of() );

        GeometryTuple left = GeometryTuple.newBuilder()
                                          .setLeft( Geometry.newBuilder()
                                                            .setName( "foo" ) )
                                          .build();
        GeometryTuple right = GeometryTuple.newBuilder()
                                           .setRight( Geometry.newBuilder()
                                                              .setName( "bar" ) )
                                           .build();
        GeometryTuple baseline = GeometryTuple.newBuilder()
                                              .setBaseline( Geometry.newBuilder()
                                                                    .setName( "baz" ) )
                                              .build();
        // No match for this one
        GeometryTuple anotherLeft = GeometryTuple.newBuilder()
                                          .setLeft( Geometry.newBuilder()
                                                            .setName( "foofoo" ) )
                                          .build();
        EvaluationDeclaration evaluation
                = FeatureFillerTest.getBoilerplateEvaluationWith( Set.of( left, right, baseline, anotherLeft ),
                                                                  featureService,
                                                                  BOILERPLATE_DATASOURCE_USGS_SITE_CODE_AUTHORITY,
                                                                  BOILERPLATE_DATASOURCE_NWS_LID_AUTHORITY,
                                                                  BOILERPLATE_BASELINE_DATASOURCE_NWM_FEATURE_AUTHORITY );

        try ( MockedStatic<WrdsFeatureService> utilities = Mockito.mockStatic( WrdsFeatureService.class ) )
        {
            // Mock the look-ups
            utilities.when( () -> WrdsFeatureService.bulkLookup( evaluation,
                                                                 featureService,
                                                                 FeatureAuthority.USGS_SITE_CODE,
                                                                 FeatureAuthority.NWS_LID,
                                                                 Set.of( "foo", "foofoo" ) ) )
                     .thenReturn( Map.of( "foo", "qux" ) );
            utilities.when( () -> WrdsFeatureService.bulkLookup( evaluation,
                                                                 featureService,
                                                                 FeatureAuthority.USGS_SITE_CODE,
                                                                 FeatureAuthority.NWM_FEATURE_ID,
                                                                 Set.of( "foo", "foofoo" ) ) )
                     .thenReturn( Map.of( "foo", "quux" ) );
            utilities.when( () -> WrdsFeatureService.bulkLookup( evaluation,
                                                                 featureService,
                                                                 FeatureAuthority.NWS_LID,
                                                                 FeatureAuthority.USGS_SITE_CODE,
                                                                 Set.of( "bar" ) ) )
                     .thenReturn( Map.of( "bar", "corge" ) );
            utilities.when( () -> WrdsFeatureService.bulkLookup( evaluation,
                                                                 featureService,
                                                                 FeatureAuthority.NWS_LID,
                                                                 FeatureAuthority.NWM_FEATURE_ID,
                                                                 Set.of( "bar" ) ) )
                     .thenReturn( Map.of( "bar", "grault" ) );
            utilities.when( () -> WrdsFeatureService.bulkLookup( evaluation,
                                                                 featureService,
                                                                 FeatureAuthority.NWM_FEATURE_ID,
                                                                 FeatureAuthority.USGS_SITE_CODE,
                                                                 Set.of( "baz" ) ) )
                     .thenReturn( Map.of( "baz", "garply" ) );
            utilities.when( () -> WrdsFeatureService.bulkLookup( evaluation,
                                                                 featureService,
                                                                 FeatureAuthority.NWM_FEATURE_ID,
                                                                 FeatureAuthority.NWS_LID,
                                                                 Set.of( "baz" ) ) )
                     .thenReturn( Map.of( "baz", "waldo" ) );
            utilities.when( () -> WrdsFeatureService.bulkLookup( evaluation,
                                                                 featureService,
                                                                 FeatureAuthority.USGS_SITE_CODE,
                                                                 FeatureAuthority.NWM_FEATURE_ID,
                                                                 Set.of( "foofoo" ) ) )
                     .thenReturn( Map.of() );

            EvaluationDeclaration actualEvaluation = FeatureFiller.fillFeatures( evaluation );

            Set<GeometryTuple> actual = actualEvaluation.features()
                                                        .geometries();

            GeometryTuple expectedOne = GeometryTuple.newBuilder()
                                                     .setLeft( Geometry.newBuilder()
                                                                       .setName( "foo" ) )
                                                     .setRight( Geometry.newBuilder()
                                                                        .setName( "qux" ) )
                                                     .setBaseline( Geometry.newBuilder()
                                                                           .setName( "quux" ) )
                                                     .build();
            GeometryTuple expectedTwo = GeometryTuple.newBuilder()
                                                     .setLeft( Geometry.newBuilder()
                                                                       .setName( "corge" ) )
                                                     .setRight( Geometry.newBuilder()
                                                                        .setName( "bar" ) )
                                                     .setBaseline( Geometry.newBuilder()
                                                                           .setName( "grault" ) )
                                                     .build();
            GeometryTuple expectedThree = GeometryTuple.newBuilder()
                                                     .setLeft( Geometry.newBuilder()
                                                                       .setName( "garply" ) )
                                                     .setRight( Geometry.newBuilder()
                                                                        .setName( "waldo" ) )
                                                     .setBaseline( Geometry.newBuilder()
                                                                           .setName( "baz" ) )
                                                     .build();
            Set<GeometryTuple> expected = Set.of( expectedOne, expectedTwo, expectedThree );

            assertEquals( expected, actual);
        }
    }

    /**
     * Generates a boilerplate evaluation declaration with the inputs.
     * @param features the features
     * @param featureService the feature service
     * @param left the left dataset
     * @param right the right dataset
     * @param baseline the baseline dataset
     * @return the evaluation declaration
     */

    private static EvaluationDeclaration getBoilerplateEvaluationWith( Set<GeometryTuple> features,
                                                                       FeatureService featureService,
                                                                       Dataset left,
                                                                       Dataset right,
                                                                       BaselineDataset baseline )
    {
        return EvaluationDeclarationBuilder.builder()
                                           .unit( "CMS" )
                                           .unitAliases( Set.of( new UnitAlias( "CMS", "m^3/s" ) ) )
                                           .featureService( featureService )
                                           .features( new Features( features ) )
                                           .left( left )
                                           .right( right )
                                           .baseline( baseline )
                                           .build();
    }
}
