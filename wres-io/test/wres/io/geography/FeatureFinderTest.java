package wres.io.geography;

import java.util.Collections;
import java.util.List;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import wres.config.generated.DataSourceBaselineConfig;
import wres.config.generated.DataSourceConfig;
import wres.config.generated.DatasourceType;
import wres.config.generated.Feature;
import wres.config.generated.FeatureDimension;
import wres.config.generated.FeatureService;
import wres.config.generated.PairConfig;
import wres.config.generated.ProjectConfig;

public class FeatureFinderTest
{
    private static final DataSourceConfig BOILERPLATE_LEFT_DATASOURCE_NO_DIMENSION =
            new DataSourceConfig( DatasourceType.SIMULATIONS,
                                  Collections.emptyList(),
                                  new DataSourceConfig.Variable( "discharge",
                                                                 null ),
                                  null,
                                  null,
                                  null,
                                  null,
                                  null,
                                  null,
                                  null,
                                  null );

    private static final DataSourceConfig BOILERPLATE_RIGHT_DATASOURCE_NO_DIMENSION =
            new DataSourceConfig( DatasourceType.SINGLE_VALUED_FORECASTS,
                                  Collections.emptyList(),
                                  new DataSourceConfig.Variable( "streamflow",
                                                                 null ),
                                  null,
                                  null,
                                  null,
                                  null,
                                  null,
                                  null,
                                  null,
                                  null );
    private static final DataSourceBaselineConfig BOILERPLATE_BASELINE_DATASOURCE_NO_DIMENSION =
            new DataSourceBaselineConfig( DatasourceType.SINGLE_VALUED_FORECASTS,
                                          Collections.emptyList(),
                                          new DataSourceConfig.Variable( "flow",
                                                                         null ),
                                          null,
                                          null,
                                          null,
                                          null,
                                          null,
                                          null,
                                          null,
                                          null,
                                          true );
    private static final DataSourceConfig BOILERPLATE_LEFT_DATASOURCE_CUSTOM_DIMENSION =
            new DataSourceConfig( DatasourceType.SIMULATIONS,
                                  Collections.emptyList(),
                                  new DataSourceConfig.Variable( "discharge",
                                                                 null ),
                                  null,
                                  null,
                                  null,
                                  null,
                                  null,
                                  null,
                                  null,
                                  FeatureDimension.CUSTOM );
    private static final DataSourceConfig BOILERPLATE_RIGHT_DATASOURCE_CUSTOM_DIMENSION =
            new DataSourceConfig( DatasourceType.SIMULATIONS,
                                  Collections.emptyList(),
                                  new DataSourceConfig.Variable( "streamflow",
                                                                 null ),
                                  null,
                                  null,
                                  null,
                                  null,
                                  null,
                                  null,
                                  null,
                                  FeatureDimension.CUSTOM );
    private static final DataSourceBaselineConfig BOILERPLATE_BASELINE_DATASOURCE_CUSTOM_DIMENSION =
            new DataSourceBaselineConfig( DatasourceType.SIMULATIONS,
                                          Collections.emptyList(),
                                          new DataSourceConfig.Variable( "flow",
                                                                         null ),
                                          null,
                                          null,
                                          null,
                                          null,
                                          null,
                                          null,
                                          null,
                                          FeatureDimension.CUSTOM,
                                          true );
    private static final ProjectConfig.Inputs BOILERPLATE_INPUTS_NO_BASELINE =
            new ProjectConfig.Inputs( BOILERPLATE_LEFT_DATASOURCE_NO_DIMENSION,
                                      BOILERPLATE_RIGHT_DATASOURCE_NO_DIMENSION,
                                      null );
    private static final ProjectConfig.Inputs BOILERPLATE_INPUTS_WITH_BASELINE =
            new ProjectConfig.Inputs( BOILERPLATE_LEFT_DATASOURCE_NO_DIMENSION,
                                      BOILERPLATE_BASELINE_DATASOURCE_NO_DIMENSION,
                                      BOILERPLATE_BASELINE_DATASOURCE_NO_DIMENSION );
    private static final Feature FULLY_DECLARED_FEATURE_ONE_NO_BASELINE =
            new Feature( "02326550", "2287397", null );
    private static final Feature FULLY_DECLARED_FEATURE_ONE_WITH_BASELINE =
            new Feature( "02326550", "2287397", "NUTF1" );
    private static final Feature FULLY_DECLARED_FEATURE_TWO_NO_BASELINE =
            new Feature( "09171100", "18382337", null );
    private static final Feature FULLY_DECLARED_FEATURE_TWO_WITH_BASELINE =
            new Feature( "09171100", "18382337", "DBDC2" );

    private static final String FEATURE_NAME_ONE = "CHICKEN";
    private static final Feature FULLY_DECLARED_FEATURE_ALL_NAME_ONE_NO_BASELINE =
            new Feature( FEATURE_NAME_ONE, FEATURE_NAME_ONE, null );
    private static final Feature FULLY_DECLARED_FEATURE_ALL_NAME_ONE_WITH_BASELINE =
            new Feature( FEATURE_NAME_ONE, FEATURE_NAME_ONE, FEATURE_NAME_ONE );
    private static final Feature LEFT_NAME_ONE_DECLARED_FEATURE =
            new Feature( FEATURE_NAME_ONE, null, null );
    private static final Feature RIGHT_NAME_ONE_DECLARED_FEATURE =
            new Feature( null, FEATURE_NAME_ONE, null );
    private static final Feature BASELINE_NAME_ONE_DECLARED_FEATURE =
            new Feature( null, null, FEATURE_NAME_ONE );

    private static final String FEATURE_NAME_TWO = "CHEESE";
    private static final Feature FULLY_DECLARED_FEATURE_ALL_NAME_TWO_NO_BASELINE =
            new Feature( FEATURE_NAME_TWO, FEATURE_NAME_TWO, null );
    private static final Feature FULLY_DECLARED_FEATURE_ALL_NAME_TWO_WITH_BASELINE =
            new Feature( FEATURE_NAME_TWO, FEATURE_NAME_TWO, FEATURE_NAME_TWO );

    private static final Feature LEFT_NAME_TWO_DECLARED_FEATURE =
            new Feature( FEATURE_NAME_TWO, null, null );
    private static final Feature RIGHT_NAME_TWO_DECLARED_FEATURE =
            new Feature( null, FEATURE_NAME_TWO, null );
    private static final Feature BASELINE_NAME_TWO_DECLARED_FEATURE =
            new Feature( null, null, FEATURE_NAME_TWO );


    private static final String FEATURE_NAME_THREE = "CHEESE";
    private static final Feature FULLY_DECLARED_FEATURE_ALL_NAME_THREE_WITH_BASELINE =
            new Feature( FEATURE_NAME_THREE, FEATURE_NAME_THREE, FEATURE_NAME_THREE );
    private static final Feature BASELINE_NAME_THREE_DECLARED_FEATURE =
            new Feature( null, null, FEATURE_NAME_THREE );

    private static PairConfig getBoilerplatePairConfigWith( List<Feature> features,
                                                            FeatureService featureService )
    {
        return new PairConfig( "CMS",
                               featureService,
                               features,
                               null,
                               null,
                               null,
                               null,
                               null,
                               null,
                               null,
                               null,
                               null,
                               null,
                               null,
                               null,
                               null,
                               null );
    }

    /**
     * When all features are fully declared (no baseline), meaning both left and
     * right feature names are declared, the resulting ProjectConfig should be
     * the same as what was passed in. No feature service required in this case.
     */

    @Test
    public void testFullyDeclaredFeaturesPassesThrough()
    {
        List<Feature> features = List.of( FULLY_DECLARED_FEATURE_ONE_NO_BASELINE,
                                          FULLY_DECLARED_FEATURE_TWO_NO_BASELINE );
        ProjectConfig projectConfig = new ProjectConfig( BOILERPLATE_INPUTS_NO_BASELINE,
                                                         getBoilerplatePairConfigWith( features,
                                                                                       null ),
                                                         null,
                                                         null,
                                                         null,
                                                         null );
        ProjectConfig result = FeatureFinder.fillFeatures( projectConfig );
        assertEquals( projectConfig, result );
    }


    /**
     * When all features are fully declared with baseline, meaning left,right,
     * and baseline feature names are declared, the resulting ProjectConfig
     * should be the same as what was passed in. No feature service required in
     * this case.
     */

    @Test
    public void testFullyDeclaredFeaturesWithBaselinePassesThrough()
    {
        List<Feature> features = List.of( FULLY_DECLARED_FEATURE_ONE_WITH_BASELINE,
                                          FULLY_DECLARED_FEATURE_TWO_WITH_BASELINE );
        ProjectConfig projectConfig = new ProjectConfig( BOILERPLATE_INPUTS_WITH_BASELINE,
                                                         getBoilerplatePairConfigWith( features,
                                                                                       null ),
                                                         null,
                                                         null,
                                                         null,
                                                         null );
        ProjectConfig result = FeatureFinder.fillFeatures( projectConfig );
        assertEquals( projectConfig, result );
    }

    /**
     * Test filling out left from declared right when both use same dimension.
     * No service required.
     */
    @Test
    public void fillOutLeftFromDeclaredRightWhenSameDimension()
    {
        ProjectConfig.Inputs inputs =
                new ProjectConfig.Inputs( BOILERPLATE_LEFT_DATASOURCE_CUSTOM_DIMENSION,
                                          BOILERPLATE_RIGHT_DATASOURCE_CUSTOM_DIMENSION,
                                          null );

        // Pass in sparsely declared features
        List<Feature> features = List.of( RIGHT_NAME_ONE_DECLARED_FEATURE,
                                          RIGHT_NAME_TWO_DECLARED_FEATURE );
        PairConfig pairConfig = FeatureFinderTest.getBoilerplatePairConfigWith( features,
                                                                                null );
        ProjectConfig projectConfig = new ProjectConfig( inputs,
                                                         pairConfig,
                                                         null,
                                                         null,
                                                         null,
                                                         null );

        ProjectConfig result = FeatureFinder.fillFeatures( projectConfig );

        List<Feature> resultFeatures = result.getPair()
                                             .getFeature();

        // Expect features fully declared.
        assertTrue( resultFeatures.contains( FULLY_DECLARED_FEATURE_ALL_NAME_ONE_NO_BASELINE ) );
        assertTrue( resultFeatures.contains( FULLY_DECLARED_FEATURE_ALL_NAME_TWO_NO_BASELINE ) );
        assertEquals( 2, resultFeatures.size() );
    }


    /**
     * Test filling out right from declared left when both use same dimension.
     * No service required.
     */
    @Test
    public void fillOutRightFromDeclaredLeftWhenSameDimension()
    {
        ProjectConfig.Inputs inputs =
                new ProjectConfig.Inputs( BOILERPLATE_LEFT_DATASOURCE_CUSTOM_DIMENSION,
                                          BOILERPLATE_RIGHT_DATASOURCE_CUSTOM_DIMENSION,
                                          null );

        // Pass in sparsely declared features
        List<Feature> features = List.of( LEFT_NAME_ONE_DECLARED_FEATURE,
                                          LEFT_NAME_TWO_DECLARED_FEATURE );
        PairConfig pairConfig = FeatureFinderTest.getBoilerplatePairConfigWith( features,
                                                                                null );
        ProjectConfig projectConfig = new ProjectConfig( inputs,
                                                         pairConfig,
                                                         null,
                                                         null,
                                                         null,
                                                         null );

        ProjectConfig result = FeatureFinder.fillFeatures( projectConfig );

        List<Feature> resultFeatures = result.getPair()
                                             .getFeature();

        // Expect features fully declared.
        assertTrue( resultFeatures.contains( FULLY_DECLARED_FEATURE_ALL_NAME_ONE_NO_BASELINE ) );
        assertTrue( resultFeatures.contains( FULLY_DECLARED_FEATURE_ALL_NAME_TWO_NO_BASELINE ) );
        assertEquals( 2, resultFeatures.size() );
    }

    /**
     * Test filling out baseline from left when all use same dimension. No
     * service required.
     */
    @Test
    public void fillOutRightAndBaselineFromDeclaredLeftWhenSameDimension()
    {
        ProjectConfig.Inputs inputs =
                new ProjectConfig.Inputs( BOILERPLATE_LEFT_DATASOURCE_CUSTOM_DIMENSION,
                                          BOILERPLATE_RIGHT_DATASOURCE_CUSTOM_DIMENSION,
                                          BOILERPLATE_BASELINE_DATASOURCE_CUSTOM_DIMENSION );

        // Pass in sparsely declared features
        List<Feature> features = List.of( LEFT_NAME_ONE_DECLARED_FEATURE,
                                          LEFT_NAME_TWO_DECLARED_FEATURE );
        PairConfig pairConfig = FeatureFinderTest.getBoilerplatePairConfigWith( features,
                                                                                null );
        ProjectConfig projectConfig = new ProjectConfig( inputs,
                                                         pairConfig,
                                                         null,
                                                         null,
                                                         null,
                                                         null );

        ProjectConfig result = FeatureFinder.fillFeatures( projectConfig );

        List<Feature> resultFeatures = result.getPair()
                                             .getFeature();

        // Expect features fully declared.
        assertTrue( resultFeatures.contains( FULLY_DECLARED_FEATURE_ALL_NAME_ONE_WITH_BASELINE ) );
        assertTrue( resultFeatures.contains( FULLY_DECLARED_FEATURE_ALL_NAME_TWO_WITH_BASELINE ) );
        assertEquals( 2, resultFeatures.size() );
    }


    /**
     * Test filling out left and baseline from right when all use same
     * dimension. No service required.
     */
    @Test
    public void fillOutLeftAndBaselineFromDeclaredRightWhenSameDimension()
    {
        ProjectConfig.Inputs inputs =
                new ProjectConfig.Inputs( BOILERPLATE_LEFT_DATASOURCE_CUSTOM_DIMENSION,
                                          BOILERPLATE_RIGHT_DATASOURCE_CUSTOM_DIMENSION,
                                          BOILERPLATE_BASELINE_DATASOURCE_CUSTOM_DIMENSION );

        // Pass in sparsely declared features
        List<Feature> features = List.of( BASELINE_NAME_ONE_DECLARED_FEATURE,
                                          BASELINE_NAME_TWO_DECLARED_FEATURE );
        PairConfig pairConfig = FeatureFinderTest.getBoilerplatePairConfigWith( features,
                                                                                null );
        ProjectConfig projectConfig = new ProjectConfig( inputs,
                                                         pairConfig,
                                                         null,
                                                         null,
                                                         null,
                                                         null );

        ProjectConfig result = FeatureFinder.fillFeatures( projectConfig );

        List<Feature> resultFeatures = result.getPair()
                                             .getFeature();

        // Expect features fully declared.
        assertTrue( resultFeatures.contains( FULLY_DECLARED_FEATURE_ALL_NAME_ONE_WITH_BASELINE ) );
        assertTrue( resultFeatures.contains( FULLY_DECLARED_FEATURE_ALL_NAME_TWO_WITH_BASELINE ) );
        assertEquals( 2, resultFeatures.size() );
    }

    /**
     * Test filling out right and left from baseline when all use same
     * dimension. No service required.
     */
    @Test
    public void fillOutLeftAndRightFromDeclaredBaselineWhenSameDimension()
    {
        ProjectConfig.Inputs inputs =
                new ProjectConfig.Inputs( BOILERPLATE_LEFT_DATASOURCE_CUSTOM_DIMENSION,
                                          BOILERPLATE_RIGHT_DATASOURCE_CUSTOM_DIMENSION,
                                          BOILERPLATE_BASELINE_DATASOURCE_CUSTOM_DIMENSION );

        // Pass in sparsely declared features
        List<Feature> features = List.of( BASELINE_NAME_ONE_DECLARED_FEATURE,
                                          BASELINE_NAME_TWO_DECLARED_FEATURE );
        PairConfig pairConfig = FeatureFinderTest.getBoilerplatePairConfigWith( features,
                                                                                null );
        ProjectConfig projectConfig = new ProjectConfig( inputs,
                                                         pairConfig,
                                                         null,
                                                         null,
                                                         null,
                                                         null );

        ProjectConfig result = FeatureFinder.fillFeatures( projectConfig );

        List<Feature> resultFeatures = result.getPair()
                                             .getFeature();

        // Expect features fully declared.
        assertTrue( resultFeatures.contains( FULLY_DECLARED_FEATURE_ALL_NAME_ONE_WITH_BASELINE ) );
        assertTrue( resultFeatures.contains( FULLY_DECLARED_FEATURE_ALL_NAME_TWO_WITH_BASELINE ) );
        assertEquals( 2, resultFeatures.size() );
    }

    /**
     * Test filling out right and baseline from left, left and baseline from
     * right (second feature), and right and left from baseline (third feature)
     * when all use same dimension. No service required.
     */

    @Test
    public void fillOutLeftFromRightAndRightFromLeftWhenSameDimension()
    {
        ProjectConfig.Inputs inputs =
                new ProjectConfig.Inputs( BOILERPLATE_LEFT_DATASOURCE_CUSTOM_DIMENSION,
                                          BOILERPLATE_RIGHT_DATASOURCE_CUSTOM_DIMENSION,
                                          BOILERPLATE_BASELINE_DATASOURCE_CUSTOM_DIMENSION );

        // Pass in sparsely declared features
        List<Feature> features = List.of( LEFT_NAME_ONE_DECLARED_FEATURE,
                                          RIGHT_NAME_TWO_DECLARED_FEATURE,
                                          BASELINE_NAME_THREE_DECLARED_FEATURE );
        PairConfig pairConfig = FeatureFinderTest.getBoilerplatePairConfigWith( features,
                                                                                null );
        ProjectConfig projectConfig = new ProjectConfig( inputs,
                                                         pairConfig,
                                                         null,
                                                         null,
                                                         null,
                                                         null );

        ProjectConfig result = FeatureFinder.fillFeatures( projectConfig );

        List<Feature> resultFeatures = result.getPair()
                                             .getFeature();

        // Expect three features, each with all names based on the one given.
        assertTrue( resultFeatures.contains( FULLY_DECLARED_FEATURE_ALL_NAME_ONE_WITH_BASELINE ) );
        assertTrue( resultFeatures.contains( FULLY_DECLARED_FEATURE_ALL_NAME_TWO_WITH_BASELINE ) );
        assertTrue( resultFeatures.contains( FULLY_DECLARED_FEATURE_ALL_NAME_THREE_WITH_BASELINE ) );
        assertEquals( 3, resultFeatures.size() );
    }
}
