package wres.config;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.xml.sax.Locator;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.DatasourceType;
import wres.config.generated.DesiredTimeScaleConfig;
import wres.config.generated.DestinationConfig;
import wres.config.generated.DestinationType;
import wres.config.generated.DurationUnit;
import wres.config.generated.NamedFeature;
import wres.config.generated.LenienceType;
import wres.config.generated.PairConfig;
import wres.config.generated.ProjectConfig;
import wres.config.generated.ProjectConfig.Inputs;
import wres.config.generated.ProjectConfig.Outputs;
import wres.system.SystemSettings;

public class ValidationTest
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
    public void fullFeaturePassesValidation()
    {
        NamedFeature fullySpecifiedFeature = new NamedFeature( "Chicken", "Cheese", "Tuna" );
        List<NamedFeature> features = List.of( fullySpecifiedFeature );
        boolean isValid = Validation.areFeaturesValidInSingletonContext( features );
        assertTrue( isValid );
    }

    @Test
    public void featureWithLeftValuePassesValidation()
    {
        NamedFeature featureWithLeftValue = new NamedFeature( "Cheese", null, null );
        List<NamedFeature> features = List.of( featureWithLeftValue );
        boolean isValid = Validation.areFeaturesValidInSingletonContext( features );
        assertTrue( isValid );
    }

    @Test
    public void allNullFeatureFailsValidation()
    {
        NamedFeature featureWithNothing = new NamedFeature( null, null, null );
        List<NamedFeature> features = List.of( featureWithNothing );
        boolean isValid = Validation.areFeaturesValidInSingletonContext( features );
        assertFalse( isValid );
    }

    @Test
    public void moreThanOneDestinationPerDestinationTypeFailsValidation()
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
    public void testDesiredTimeScaleWithPeriodAndBothMonthDaysFailsValidation()
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

        SystemSettings mockSystemSettings = Mockito.mock( SystemSettings.class );

        // Not interested in XML validation events, just extra validation
        Mockito.when( mockProjectConfigPlus.getValidationEvents() )
               .thenReturn( List.of() );

        assertFalse( Validation.isProjectValid( mockSystemSettings, mockProjectConfigPlus ) );
    }
    
    @Test
    public void testDesiredTimeScaleWithEarliestDayMissingFailsValidation()
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

        SystemSettings mockSystemSettings = Mockito.mock( SystemSettings.class );

        // Not interested in XML validation events, just extra validation
        Mockito.when( mockProjectConfigPlus.getValidationEvents() )
               .thenReturn( List.of() );

        assertFalse( Validation.isProjectValid( mockSystemSettings, mockProjectConfigPlus ) );
    }
    
    @Test
    public void testDesiredTimeScaleWithLatestMonthMissingFailsValidation()
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

        SystemSettings mockSystemSettings = Mockito.mock( SystemSettings.class );

        // Not interested in XML validation events, just extra validation
        Mockito.when( mockProjectConfigPlus.getValidationEvents() )
               .thenReturn( List.of() );

        assertFalse( Validation.isProjectValid( mockSystemSettings, mockProjectConfigPlus ) );
    }   

    @Test
    public void testDesiredTimeScaleWithInvalidEarliestMonthDayFailsValidation()
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

        SystemSettings mockSystemSettings = Mockito.mock( SystemSettings.class );

        // Not interested in XML validation events, just extra validation
        Mockito.when( mockProjectConfigPlus.getValidationEvents() )
               .thenReturn( List.of() );

        assertFalse( Validation.isProjectValid( mockSystemSettings, mockProjectConfigPlus ) );
    }
    
}
