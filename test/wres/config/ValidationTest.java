package wres.config;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.xml.sax.Locator;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.DestinationConfig;
import wres.config.generated.DestinationType;
import wres.config.generated.DurationUnit;
import wres.config.generated.Feature;
import wres.config.generated.Format;
import wres.config.generated.ProjectConfig;
import wres.config.generated.ProjectConfig.Outputs;
public class ValidationTest
{
    private Locator fakeSourceLocator;

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
    }


    @Test
    public void specifyingOffsetForPIXMLFailsValidation()
    {
        ProjectConfigPlus mockProjectConfigPlus = mock( ProjectConfigPlus.class );
        String mockPath = "fake.xml";
        when( mockProjectConfigPlus.getOrigin() ).thenReturn( mockPath );


        DataSourceConfig.Source source =
                new DataSourceConfig.Source(
                        null,
                        null,
                        Format.PI_XML,
                        "-0500",
                        null,
                        null
                );

        source.setSourceLocation( this.fakeSourceLocator );

        boolean result = Validation.isDateConfigValid( mockProjectConfigPlus,
                                                       source );
        assertFalse( result, "Expected validation failure when offset "
                     + " for PI-XML data is specified." );
    }

    @Test
    public void fullFeaturePassesValidation()
    {
        Feature fullySpecifiedFeature = new Feature( "Chicken", "Cheese", "Tuna" );
        List<Feature> features = List.of( fullySpecifiedFeature );
        boolean isValid = Validation.areFeaturesValid( features );
        assertTrue( isValid );
    }

    @Test
    public void featureWithLeftValuePassesValidation()
    {
        Feature featureWithLeftValue = new Feature( "Cheese", null, null );
        List<Feature> features = List.of( featureWithLeftValue );
        boolean isValid = Validation.areFeaturesValid( features );
        assertTrue( isValid );
    }

    @Test
    public void allNullFeatureFailsValidation()
    {
        Feature featureWithNothing = new Feature( null, null, null );
        List<Feature> features = List.of( featureWithNothing );
        boolean isValid = Validation.areFeaturesValid( features );
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
    
}
