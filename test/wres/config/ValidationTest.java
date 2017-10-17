package wres.config;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.Test;
import org.xml.sax.Locator;

import static junit.framework.TestCase.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import wres.config.generated.DataSourceConfig;
import wres.io.config.ProjectConfigPlus;

public class ValidationTest
{

    @Test
    public void specifyingTimeZoneAndOffsetFailsValidation()
            throws IOException
    {
        ProjectConfigPlus mockProjectConfigPlus = mock( ProjectConfigPlus.class );
        Path mockPath = Paths.get("fake.xml");
        when(mockProjectConfigPlus.getPath()).thenReturn( mockPath );

        Locator mockSourceLocator =
                new Locator() {
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

        DataSourceConfig.Source source =
                new DataSourceConfig.Source(
                        null,
                        null,
                        null,
                        null,
                        "-0500",
                        "America/Chicago",
                        null,
                        null,
                        null,
                        null,
                        null
                );

        source.setSourceLocation( mockSourceLocator );

        boolean result = Validation.isDateConfigValid( mockProjectConfigPlus,
                                                       source );
        assertFalse( "Expected validation failure when offset and zone"
                     + " are both specified.", result );
    }
}
