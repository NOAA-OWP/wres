package wres.config.xml;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.xml.sax.Locator;

import wres.config.xml.generated.DataSourceConfig;
import wres.config.xml.generated.DataSourceConfig.Variable;
import wres.config.xml.generated.DatasourceType;
import wres.config.xml.generated.DesiredTimeScaleConfig;
import wres.config.xml.generated.DestinationConfig;
import wres.config.xml.generated.DestinationType;
import wres.config.xml.generated.DurationUnit;
import wres.config.xml.generated.InterfaceShortHand;
import wres.config.xml.generated.NamedFeature;
import wres.config.xml.generated.LenienceType;
import wres.config.xml.generated.PairConfig;
import wres.config.xml.generated.ProjectConfig;
import wres.config.xml.generated.ProjectConfig.Inputs;
import wres.config.xml.generated.ProjectConfig.Outputs;

@Deprecated( since = "6.14", forRemoval = true )
class ValidationTest
{
    private static final String FOO_TEST_PROJECT = "foo test project";
    private Locator fakeSourceLocator;
    private DataSourceConfig emptyDataSourceConfig;

    @BeforeEach
    public void setup()
    {
        this.fakeSourceLocator =
                new Locator()
                {
                    @Override
                    public String getPublicId()
                    {
                        return "fakePublicId";
                    }

                    @Override
                    public String getSystemId()
                    {
                        return "fakeSystemId";
                    }

                    @Override
                    public int getLineNumber()
                    {
                        return 1;
                    }

                    @Override
                    public int getColumnNumber()
                    {
                        return 1;
                    }
                };

        this.emptyDataSourceConfig = new DataSourceConfig( DatasourceType.OBSERVATIONS,
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


    @Test
    void fullFeaturePassesValidation()
    {
        NamedFeature fullySpecifiedFeature = new NamedFeature( "Chicken", "Cheese", "Tuna" );
        List<NamedFeature> features = List.of( fullySpecifiedFeature );
        boolean isValid = Validation.areFeaturesValidInSingletonContext( features );
        assertTrue( isValid );
    }

    @Test
    void featureWithLeftValuePassesValidation()
    {
        NamedFeature featureWithLeftValue = new NamedFeature( "Cheese", null, null );
        List<NamedFeature> features = List.of( featureWithLeftValue );
        boolean isValid = Validation.areFeaturesValidInSingletonContext( features );
        assertTrue( isValid );
    }

    @Test
    void allNullFeatureFailsValidation()
    {
        NamedFeature featureWithNothing = new NamedFeature( null, null, null );
        List<NamedFeature> features = List.of( featureWithNothing );
        boolean isValid = Validation.areFeaturesValidInSingletonContext( features );
        assertFalse( isValid );
    }

    @Test
    void moreThanOneDestinationPerDestinationTypeFailsValidation()
    {
        List<DestinationConfig> destinations = new ArrayList<>();

        // Add two destinations of one type, which is not allowed
        DestinationConfig one = new DestinationConfig( null, null, null, DestinationType.PNG, null );
        DestinationConfig two = new DestinationConfig( null, null, null, DestinationType.GRAPHIC, null );

        destinations.add( one );
        destinations.add( two );

        Outputs outputs = new Outputs( destinations, DurationUnit.HOURS );
        outputs.setSourceLocation( this.fakeSourceLocator );

        ProjectConfig mockProjectConfig = new ProjectConfig( null, null, null, outputs, null, null );

        ProjectConfigPlus mockProjectConfigPlus = Mockito.mock( ProjectConfigPlus.class );
        Mockito.when( mockProjectConfigPlus.getProjectConfig() )
               .thenReturn( mockProjectConfig );

        boolean isValid = Validation.hasUpToOneDestinationPerDestinationType( mockProjectConfigPlus );

        assertFalse( isValid );
    }

    @Test
    void testDesiredTimeScaleWithPeriodAndBothMonthDaysFailsValidation()
    {
        // Define a minimal declaration that passes validation and a desiredTimeScale that should not
        DesiredTimeScaleConfig desiredTimeScale = new DesiredTimeScaleConfig( null,
                                                                              1,
                                                                              DurationUnit.HOURS,
                                                                              null,
                                                                              null,
                                                                              (short) 4,
                                                                              (short) 1,
                                                                              (short) 7,
                                                                              (short) 31,
                                                                              LenienceType.FALSE );
        desiredTimeScale.setSourceLocation( this.fakeSourceLocator );

        PairConfig pairConfig = new PairConfig( null,
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
                                                desiredTimeScale,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null );

        Inputs inputsConfig = new Inputs( this.emptyDataSourceConfig, this.emptyDataSourceConfig, null );
        Outputs outputs = new Outputs( null, DurationUnit.HOURS );
        outputs.setSourceLocation( this.fakeSourceLocator );

        ProjectConfig mockProjectConfig = new ProjectConfig( inputsConfig, pairConfig, null, outputs, null, null );

        ProjectConfigPlus mockProjectConfigPlus = Mockito.mock( ProjectConfigPlus.class );
        Mockito.when( mockProjectConfigPlus.getProjectConfig() )
               .thenReturn( mockProjectConfig );
        Mockito.when( mockProjectConfigPlus.getOrigin() )
               .thenReturn( FOO_TEST_PROJECT );

        Path fakePath = Path.of( "fakePath.fake" );

        // Not interested in XML validation events, just extra validation
        Mockito.when( mockProjectConfigPlus.getValidationEvents() )
               .thenReturn( List.of() );

        assertFalse( Validation.isProjectValid( fakePath, mockProjectConfigPlus ) );
    }
    
    @Test
    void testDesiredTimeScaleWithEarliestDayMissingFailsValidation()
    {
        // Define a minimal declaration that passes validation and a desiredTimeScale that should not
        DesiredTimeScaleConfig desiredTimeScale = new DesiredTimeScaleConfig( null,
                                                                              null,
                                                                              null,
                                                                              null,
                                                                              null,
                                                                              (short) 4,
                                                                              (short) 1,
                                                                              null,
                                                                              (short) 31,
                                                                              LenienceType.FALSE );
        desiredTimeScale.setSourceLocation( this.fakeSourceLocator );

        PairConfig pairConfig = new PairConfig( null,
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
                                                desiredTimeScale,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null );

        Inputs inputsConfig = new Inputs( this.emptyDataSourceConfig, this.emptyDataSourceConfig, null );
        Outputs outputs = new Outputs( null, DurationUnit.HOURS );
        outputs.setSourceLocation( this.fakeSourceLocator );

        ProjectConfig mockProjectConfig = new ProjectConfig( inputsConfig, pairConfig, null, outputs, null, null );

        ProjectConfigPlus mockProjectConfigPlus = Mockito.mock( ProjectConfigPlus.class );
        Mockito.when( mockProjectConfigPlus.getProjectConfig() )
               .thenReturn( mockProjectConfig );
        Mockito.when( mockProjectConfigPlus.getOrigin() )
               .thenReturn( FOO_TEST_PROJECT );

        Path fakePath = Path.of( "fakePath.fake" );

        // Not interested in XML validation events, just extra validation
        Mockito.when( mockProjectConfigPlus.getValidationEvents() )
               .thenReturn( List.of() );

        assertFalse( Validation.isProjectValid( fakePath, mockProjectConfigPlus ) );
    }
    
    @Test
    void testDesiredTimeScaleWithLatestMonthMissingFailsValidation()
    {
        // Define a minimal declaration that passes validation and a desiredTimeScale that should not
        DesiredTimeScaleConfig desiredTimeScale = new DesiredTimeScaleConfig( null,
                                                                              null,
                                                                              null,
                                                                              null,
                                                                              null,
                                                                              (short) 4,
                                                                              null,
                                                                              (short) 7,
                                                                              (short) 31,
                                                                              LenienceType.FALSE );
        desiredTimeScale.setSourceLocation( this.fakeSourceLocator );

        PairConfig pairConfig = new PairConfig( null,
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
                                                desiredTimeScale,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null );

        Inputs inputsConfig = new Inputs( this.emptyDataSourceConfig, this.emptyDataSourceConfig, null );
        Outputs outputs = new Outputs( null, DurationUnit.HOURS );
        outputs.setSourceLocation( this.fakeSourceLocator );

        ProjectConfig mockProjectConfig = new ProjectConfig( inputsConfig, pairConfig, null, outputs, null, null );

        ProjectConfigPlus mockProjectConfigPlus = Mockito.mock( ProjectConfigPlus.class );
        Mockito.when( mockProjectConfigPlus.getProjectConfig() )
               .thenReturn( mockProjectConfig );
        Mockito.when( mockProjectConfigPlus.getOrigin() )
               .thenReturn( FOO_TEST_PROJECT );

        Path fakePath = Path.of( "fakePath.fake" );

        // Not interested in XML validation events, just extra validation
        Mockito.when( mockProjectConfigPlus.getValidationEvents() )
               .thenReturn( List.of() );

        assertFalse( Validation.isProjectValid( fakePath, mockProjectConfigPlus ) );
    }   

    @Test
    void testDesiredTimeScaleWithInvalidEarliestMonthDayFailsValidation()
    {
        // Define a minimal declaration that passes validation and a desiredTimeScale that should not
        DesiredTimeScaleConfig desiredTimeScale = new DesiredTimeScaleConfig( null,
                                                                              null,
                                                                              null,
                                                                              null,
                                                                              null,
                                                                              (short) 0,
                                                                              (short) 4,
                                                                              (short) 7,
                                                                              (short) 31,
                                                                              LenienceType.FALSE );
        desiredTimeScale.setSourceLocation( this.fakeSourceLocator );

        PairConfig pairConfig = new PairConfig( null,
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
                                                desiredTimeScale,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null );

        Inputs inputsConfig = new Inputs( this.emptyDataSourceConfig, this.emptyDataSourceConfig, null );
        Outputs outputs = new Outputs( null, DurationUnit.HOURS );
        outputs.setSourceLocation( this.fakeSourceLocator );

        ProjectConfig mockProjectConfig = new ProjectConfig( inputsConfig, pairConfig, null, outputs, null, null );

        ProjectConfigPlus mockProjectConfigPlus = Mockito.mock( ProjectConfigPlus.class );
        Mockito.when( mockProjectConfigPlus.getProjectConfig() )
               .thenReturn( mockProjectConfig );
        Mockito.when( mockProjectConfigPlus.getOrigin() )
               .thenReturn( FOO_TEST_PROJECT );

        Path fakePath = Path.of( "fakePath.fake" );

        // Not interested in XML validation events, just extra validation
        Mockito.when( mockProjectConfigPlus.getValidationEvents() )
               .thenReturn( List.of() );

        assertFalse( Validation.isProjectValid( fakePath, mockProjectConfigPlus ) );
    }
    

    @Test
    void testUsgsNwisSourceWithoutVariableFailsValidation() throws URISyntaxException
    {
        DataSourceConfig.Source nwisSource;
        nwisSource = new DataSourceConfig.Source( new URI( "https://nwis.waterservices.usgs.gov/nwis/iv" ),
                                                  InterfaceShortHand.USGS_NWIS,
                                                  null,
                                                  null,
                                                  null );
        ArrayList<DataSourceConfig.Source> sourceList = new ArrayList<>();
        sourceList.add( nwisSource );
        DataSourceConfig left = new DataSourceConfig( DatasourceType.OBSERVATIONS,
                                                      sourceList,
                                                      null,
                                                      null,
                                                      null,
                                                      null,
                                                      null,
                                                      null,
                                                      null,
                                                      "testUsgsNwisSourceWithoutVariableFailsValidation",
                                                      null );
        left.setSourceLocation( fakeSourceLocator );

        Inputs inputs = new Inputs(left, null, null);
        ProjectConfig mockProjectConfig = new ProjectConfig( inputs, null, null, null, null, null );
        ProjectConfigPlus mockProjectConfigPlus = Mockito.mock( ProjectConfigPlus.class );
        Mockito.when( mockProjectConfigPlus.getProjectConfig() )
               .thenReturn( mockProjectConfig );

        boolean isValid = Validation.isVariableSpecifiedIfRequired( mockProjectConfigPlus, left );
        assertFalse( isValid );
    }

    @Test
    void testWrdsNwmSourceWithVariablePassesValidation() throws URISyntaxException
    {
        DataSourceConfig.Source wrdsNwmSource;
       
        wrdsNwmSource = new DataSourceConfig.Source( new URI( "https://mockServer/api/nwm2.1/v2.0/ops/medium_range" ),
                                                     InterfaceShortHand.WRDS_NWM,
                                                     null,
                                                     null,
                                                     null );
        ArrayList<DataSourceConfig.Source> sourceList = new ArrayList<>();
        sourceList.add( wrdsNwmSource );
        DataSourceConfig right = new DataSourceConfig( DatasourceType.ENSEMBLE_FORECASTS,
                                                      sourceList,
                                                      new Variable( "streamflow", "label" ),
                                                      null,
                                                      null,
                                                      null,
                                                      null,
                                                      null,
                                                      null,
                                                      "testWrdsNwmSourceWithVariablePassesValidation",
                                                      null );
        right.setSourceLocation( fakeSourceLocator );

        Inputs inputs = new Inputs(null, right, null);
        ProjectConfig mockProjectConfig = new ProjectConfig( inputs, null, null, null, null, null );
        ProjectConfigPlus mockProjectConfigPlus = Mockito.mock( ProjectConfigPlus.class );
        Mockito.when( mockProjectConfigPlus.getProjectConfig() )
               .thenReturn( mockProjectConfig );

        boolean isValid = Validation.isVariableSpecifiedIfRequired( mockProjectConfigPlus, right );
        assertTrue( isValid );
    }

}

