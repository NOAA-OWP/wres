package wres.io.geography;

import java.util.Collections;
import java.util.List;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

import wres.config.generated.DataSourceBaselineConfig;
import wres.config.generated.DataSourceConfig;
import wres.config.generated.DatasourceType;
import wres.config.generated.Feature;
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

    // TODO: add these tests:
    // Test filling out left from declared right when both use same dimension.
    // No service required.

    // Test filling out right from left when both use same dimension. No service
    // required.

    // Test filling out baseline from left when both use same dimension. No
    // service required.

    // Test filling out baseline from right when both use same dimension. No
    // service required.

    // Test filling out right from baseline when both use same dimension. No
    // service required.

    // Test filling out left from baseline when both use same dimension. No
    // service required.

    // Test filling out one of each of the six directions when all use same
    // dimension. No service required.
}
