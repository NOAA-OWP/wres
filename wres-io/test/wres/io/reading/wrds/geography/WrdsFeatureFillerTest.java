package wres.io.reading.wrds.geography;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
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
 * Tests the {@link WrdsFeatureFiller}.
 */

class WrdsFeatureFillerTest
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
    private static final Dataset BOILERPLATE_LEFT_DATASOURCE_USGS_SITE_CODE_AUTHORITY =
            DatasetBuilder.builder()
                          .featureAuthority( FeatureAuthority.USGS_SITE_CODE )
                          .build();

    private static final Dataset BOILERPLATE_RIGHT_DATASOURCE_NWS_LID_AUTHORITY =
            DatasetBuilder.builder()
                          .featureAuthority( FeatureAuthority.NWS_LID )
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
                = WrdsFeatureFillerTest.getBoilerplateEvaluationWith( features,
                                                                      null,
                                                                      BOILERPLATE_LEFT_DATASOURCE_NO_DIMENSION,
                                                                      BOILERPLATE_RIGHT_DATASOURCE_NO_DIMENSION,
                                                                      null );

        EvaluationDeclaration actual = WrdsFeatureFiller.fillFeatures( expected );
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
                = WrdsFeatureFillerTest.getBoilerplateEvaluationWith( features,
                                                                      null,
                                                                      BOILERPLATE_LEFT_DATASOURCE_NO_DIMENSION,
                                                                      BOILERPLATE_RIGHT_DATASOURCE_NO_DIMENSION,
                                                                      BOILERPLATE_BASELINE_DATASOURCE_NO_AUTHORITY );

        EvaluationDeclaration actual = WrdsFeatureFiller.fillFeatures( expected );
        assertEquals( expected, actual );
    }

    @Test
    void fillOutImplicitFeatureGroupUsingMockedFeatureService() throws URISyntaxException
    {
        URI uri = new URI( "https://some_fake_uri" );
        FeatureServiceGroup featureGroup = new FeatureServiceGroup( "state", "AL", true );
        FeatureService featureService = new FeatureService( uri, Set.of( featureGroup ) );

        EvaluationDeclaration evaluation
                = WrdsFeatureFillerTest.getBoilerplateEvaluationWith( null,
                                                                      featureService,
                                                                      BOILERPLATE_LEFT_DATASOURCE_USGS_SITE_CODE_AUTHORITY,
                                                                      BOILERPLATE_RIGHT_DATASOURCE_NWS_LID_AUTHORITY,
                                                                      null );

        try ( MockedStatic<WrdsFeatureService> utilities = Mockito.mockStatic( WrdsFeatureService.class ) )
        {
            utilities.when( () -> WrdsFeatureService.read( Mockito.any() ) )
                     .thenReturn( List.of( new WrdsLocation( "foo", "bar", "baz" ) ) );

            EvaluationDeclaration actualEvaluation = WrdsFeatureFiller.fillFeatures( evaluation );

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
