package wres.config;

import java.util.List;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.xml.sax.Locator;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.Feature;
import wres.config.generated.Format;

public class ValidationTest
{
    private Locator fakeSourceLocator;

    @Before
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
        assertFalse( "Expected validation failure when offset for "
                     + "PI-XML data is specified.", result );
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
}
