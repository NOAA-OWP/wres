package wres.reading.wrds.geography;

import java.io.BufferedWriter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
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
import wres.config.yaml.components.FeatureServiceGroup;
import wres.config.yaml.components.FeaturesBuilder;
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
        wres.config.yaml.components.FeatureService
                featureService = new wres.config.yaml.components.FeatureService( uri, Set.of( featureGroup ) );

        EvaluationDeclaration evaluation
                = FeatureFillerTest.getBoilerplateEvaluationWith( null,
                                                                  featureService,
                                                                  BOILERPLATE_DATASOURCE_USGS_SITE_CODE_AUTHORITY,
                                                                  BOILERPLATE_DATASOURCE_NWS_LID_AUTHORITY,
                                                                  null );

        try ( MockedStatic<FeatureService> utilities = Mockito.mockStatic( FeatureService.class ) )
        {
            utilities.when( () -> FeatureService.read( Mockito.any() ) )
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
    void testFillOutFeaturesUsingResponseFromFileSystem() throws IOException
    {
        try ( FileSystem fileSystem = Jimfs.newFileSystem( Configuration.unix() ) )
        {
            // Write a new csv file to an in-memory file system
            Path directory = fileSystem.getPath( "test" );
            Files.createDirectory( directory );
            Path pathToStore = fileSystem.getPath( "test", "test.json" );
            Path jsonPath = Files.createFile( pathToStore );

            try ( BufferedWriter writer = Files.newBufferedWriter( jsonPath ) )
            {
                String jsonString = """
                        {
                            "_metrics": {
                                "location_count": 4603,
                                "model_tracing_api_call": 0.10254240036010742,
                                "total_request_time": 19.812459230422974
                            },
                            "_warnings": [],
                            "_documentation": {
                                "swagger URL": "foo_docs"
                            },
                            "deployment": {
                                "api_url": "foo_url",
                                "stack": "prod",
                                "version": "v3.5.6",
                                "api_caller": "None"
                            },
                            "locations": [
                                {
                                    "identifiers": {
                                        "nws_lid": "baz",
                                        "usgs_site_code": "bar",
                                        "nwm_feature_id": "foo"
                                    }
                                }
                            ]
                        }
                        """;
                writer.write( jsonString );
            }

            URI serviceUri = jsonPath.toUri();
            wres.config.yaml.components.FeatureService
                    featureService = new wres.config.yaml.components.FeatureService( serviceUri, Set.of() );

            // Create a sparse feature to correlate
            GeometryTuple feature = GeometryTuple.newBuilder()
                                                 .setLeft( Geometry.newBuilder()
                                                                   .setName( "bar" ) )
                                                 .build();

            EvaluationDeclaration evaluation
                    = FeatureFillerTest.getBoilerplateEvaluationWith( Set.of( feature ),
                                                                      featureService,
                                                                      BOILERPLATE_DATASOURCE_USGS_SITE_CODE_AUTHORITY,
                                                                      BOILERPLATE_DATASOURCE_NWS_LID_AUTHORITY,
                                                                      null );

            EvaluationDeclaration actualEvaluation = FeatureFiller.fillFeatures( evaluation );

            GeometryTuple expectedFeature = GeometryTuple.newBuilder()
                                                         .setLeft( Geometry.newBuilder()
                                                                           .setName( "bar" ) )
                                                         .setRight( Geometry.newBuilder()
                                                                            .setName( "baz" ) )
                                                         .build();

            Set<GeometryTuple> expected = Set.of( expectedFeature );
            Set<GeometryTuple> actual = actualEvaluation.features()
                                                        .geometries();

            assertEquals( expected, actual );
        }
    }

    @Test
    void testFillOutImplicitFeatureGroupUsingMockedFeatureServiceAndNullBaseline() throws URISyntaxException
    {
        URI uri = new URI( "https://some_fake_uri" );
        FeatureServiceGroup featureGroup = new FeatureServiceGroup( "state", "AL", true );
        wres.config.yaml.components.FeatureService
                featureService = new wres.config.yaml.components.FeatureService( uri, Set.of( featureGroup ) );

        EvaluationDeclaration evaluation
                = FeatureFillerTest.getBoilerplateEvaluationWith( null,
                                                                  featureService,
                                                                  BOILERPLATE_DATASOURCE_USGS_SITE_CODE_AUTHORITY,
                                                                  BOILERPLATE_DATASOURCE_NWS_LID_AUTHORITY,
                                                                  BOILERPLATE_BASELINE_DATASOURCE_NWM_FEATURE_AUTHORITY );

        try ( MockedStatic<FeatureService> utilities = Mockito.mockStatic( FeatureService.class ) )
        {
            utilities.when( () -> FeatureService.read( Mockito.any() ) )
                     // Return one location with a missing NWM feature ID, which should be removed
                     .thenReturn( List.of( new Location( null, "bar", "baz" ),
                                           new Location( "qux", "quux", "corge" ) ) );

            EvaluationDeclaration actualEvaluation = FeatureFiller.fillFeatures( evaluation );

            Set<GeometryGroup> actualGroup = actualEvaluation.featureGroups()
                                                             .geometryGroups();

            GeometryGroup expectedGroup =
                    GeometryGroup.newBuilder()
                                 .addAllGeometryTuples( Set.of( GeometryTuple.newBuilder()
                                                                             .setLeft( Geometry.newBuilder()
                                                                                               .setName( "quux" ) )
                                                                             .setRight( Geometry.newBuilder()
                                                                                                .setName( "corge" ) )
                                                                             .setBaseline( Geometry.newBuilder()
                                                                                                   .setName( "qux" ) )
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
        wres.config.yaml.components.FeatureService
                featureService = new wres.config.yaml.components.FeatureService( uri, Set.of() );

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

        try ( MockedStatic<FeatureService> utilities = Mockito.mockStatic( FeatureService.class ) )
        {
            // Mock the look-ups
            utilities.when( () -> FeatureService.bulkLookup( evaluation,
                                                             featureService,
                                                             FeatureAuthority.USGS_SITE_CODE,
                                                             FeatureAuthority.NWS_LID,
                                                             Set.of( "foo", "foofoo" ) ) )
                     .thenReturn( Map.of( "foo", "qux" ) );
            utilities.when( () -> FeatureService.bulkLookup( evaluation,
                                                             featureService,
                                                             FeatureAuthority.USGS_SITE_CODE,
                                                             FeatureAuthority.NWM_FEATURE_ID,
                                                             Set.of( "foo", "foofoo" ) ) )
                     .thenReturn( Map.of( "foo", "quux" ) );
            utilities.when( () -> FeatureService.bulkLookup( evaluation,
                                                             featureService,
                                                             FeatureAuthority.NWS_LID,
                                                             FeatureAuthority.USGS_SITE_CODE,
                                                             Set.of( "bar" ) ) )
                     .thenReturn( Map.of( "bar", "corge" ) );
            utilities.when( () -> FeatureService.bulkLookup( evaluation,
                                                             featureService,
                                                             FeatureAuthority.NWS_LID,
                                                             FeatureAuthority.NWM_FEATURE_ID,
                                                             Set.of( "bar" ) ) )
                     .thenReturn( Map.of( "bar", "grault" ) );
            utilities.when( () -> FeatureService.bulkLookup( evaluation,
                                                             featureService,
                                                             FeatureAuthority.NWM_FEATURE_ID,
                                                             FeatureAuthority.USGS_SITE_CODE,
                                                             Set.of( "baz" ) ) )
                     .thenReturn( Map.of( "baz", "garply" ) );
            utilities.when( () -> FeatureService.bulkLookup( evaluation,
                                                             featureService,
                                                             FeatureAuthority.NWM_FEATURE_ID,
                                                             FeatureAuthority.NWS_LID,
                                                             Set.of( "baz" ) ) )
                     .thenReturn( Map.of( "baz", "waldo" ) );
            utilities.when( () -> FeatureService.bulkLookup( evaluation,
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

            assertEquals( expected, actual );
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
                                                                       wres.config.yaml.components.FeatureService featureService,
                                                                       Dataset left,
                                                                       Dataset right,
                                                                       BaselineDataset baseline )
    {
        return EvaluationDeclarationBuilder.builder()
                                           .unit( "CMS" )
                                           .unitAliases( Set.of( new UnitAlias( "CMS", "m^3/s" ) ) )
                                           .featureService( featureService )
                                           .features( FeaturesBuilder.builder()
                                                                     .geometries( features )
                                                                     .build() )
                                           .left( left )
                                           .right( right )
                                           .baseline( baseline )
                                           .build();
    }
}