package wres.config.xml;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import javax.xml.XMLConstants;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.ValidationEvent;
import javax.xml.bind.ValidationEventHandler;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import wres.config.xml.generated.ObjectFactory;
import wres.config.xml.generated.ProjectConfig;

/**
 * <p>Associates a project configuration object with its graphgen xml string.
 *
 * <p>Also useful for jaxb-discovered validation issues during parsing.
 *
 * <p>At the highest level, when reading the project config from a path, we also
 * gather the xml string needed by wres-vis to generate graphics.
 *
 * <p>The reason for this extra step is to facilitate re-use of existing code that
 * expects an unmarshaled xml string, and this works OK for now.
 *
 * <p>It is anticipated that most parts of WRES will use ProjectConfig.
 *
 * <p>When performing initial read of the config and validation, it will be handy
 * to keep a copy of the (stateful) information that was found during read, so
 * as of 2017-07-24 ProjectConfigPlus is anticipated to be used primarily by
 * the control module.
 *
 * <p>Intended to be Thread-safe, and the object is. Performs one read then uses
 * and keeps the resulting file in a string.
 * @deprecated
 */
@Deprecated( since = "6.14", forRemoval = true )
public class ProjectConfigPlus
{
    private static final Logger LOGGER = LoggerFactory.getLogger( ProjectConfigPlus.class );

    private final String origin;
    private final String rawConfig;
    private final ProjectConfig projectConfig;
    private final List<ValidationEvent> validationEvents;

    private static final String XSD_NAME = "ProjectConfig.xsd";

    private ProjectConfigPlus( String origin,
                               String rawConfig,
                               ProjectConfig projectConfig,
                               List<ValidationEvent> validationEvents )
    {
        this.origin = origin;
        this.rawConfig = rawConfig;
        this.projectConfig = projectConfig;
        List<ValidationEvent> copiedList = new ArrayList<>( validationEvents );
        this.validationEvents = Collections.unmodifiableList( copiedList );
    }


    /**
     * Return a description of where this project config came from.
     * In the case of a file, the path to the file.
     * @return the origin of the project config
     */

    public String getOrigin()
    {
        return this.origin;
    }

    /**
     * @return the raw project declaration
     */
    public String getRawConfig()
    {
        return this.rawConfig;
    }

    /**
     * @return the project declaration
     */
    public ProjectConfig getProjectConfig()
    {
        // safety ops performed at construction
        return this.projectConfig;
    }

    /**
     * @return the validation events
     */
    public List<ValidationEvent> getValidationEvents()
    {
        // safety ops performed at construction
        return this.validationEvents;
    }

    /**
     * An event handler that collects validation events during xml parsing
     * and can return the list of events.
     */

    private static class ProjValidationEventHandler
            implements ValidationEventHandler, Callable<List<ValidationEvent>>
    {
        private final List<ValidationEvent> events = new ArrayList<>();

        @Override
        public boolean handleEvent( final ValidationEvent validationEvent )
        {
            events.add( validationEvent );
            return true;
        }

        @Override
        public List<ValidationEvent> call()
        {
            return Collections.unmodifiableList( events );
        }
    }

    /**
     * Parse a projectconfig from a string, store validation events, get
     * the vis config strings.
     * @param rawConfig the config to unmarshal
     * @param origin a description of where this projectConfig came from
     * @return a handy bundle including the projectconfig, validation events
     * @throws IOException when the string cannot be successfully parsed
     */

    public static ProjectConfigPlus from( String rawConfig, String origin )
            throws IOException
    {
        Objects.requireNonNull( rawConfig );

        // Trim the string
        rawConfig = rawConfig.trim();

        ProjectConfig projectConfig;

        ProjValidationEventHandler validationEventCollector = new ProjValidationEventHandler();

        try ( Reader reader = new StringReader( rawConfig );
              InputStream schemaStream = ClassLoader.getSystemResourceAsStream( XSD_NAME ) )
        {
            Source xmlSource = new StreamSource( reader );
            JAXBContext jaxbContext = JAXBContext.newInstance( ObjectFactory.class );

            // Validate against schema during unmarshaling
            Source schemaSource = new StreamSource( schemaStream );
            SchemaFactory schemaFactory =
                    SchemaFactory.newInstance( XMLConstants.W3C_XML_SCHEMA_NS_URI );
            Schema schema = schemaFactory.newSchema( schemaSource );

            Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
            jaxbUnmarshaller.setSchema( schema );
            jaxbUnmarshaller.setEventHandler( validationEventCollector );
            final JAXBElement<ProjectConfig> wrappedConfig =
                    jaxbUnmarshaller.unmarshal( xmlSource, ProjectConfig.class );
            projectConfig = wrappedConfig.getValue();

            LOGGER.info( "Unmarshalled project configuration from {}", origin );

            if ( projectConfig == null
                 || projectConfig.getInputs() == null
                 || projectConfig.getOutputs() == null
                 || projectConfig.getPair() == null )
            {
                throw new IOException( "Please add required sections in project config from "
                                       + origin + " : <inputs>, <outputs>, <pair>" );
            }
        }
        catch ( final JAXBException je )
        {
            String message = "Could not parse project from " + origin;
            // communicate failure back up the stack
            throw new IOException( message, je );
        }
        catch ( final NumberFormatException nfe )
        {
            String message = "A value in the project from " + origin
                             + " was unable to be converted to a number.";
            // communicate failure back up the stack
            throw new IOException( message, nfe );
        }
        catch ( SAXException e )
        {
            String message = "Expected a valid XML schema on classpath at "
                             + XSD_NAME + ".";
            throw new IOException( message, e );
        }

        List<ValidationEvent> validationEvents = validationEventCollector.call();

        return new ProjectConfigPlus( origin,
                                      rawConfig,
                                      projectConfig,
                                      validationEvents );
    }

    /**
     * Parse a WRES project from a file, store validation events,
     * get the vis config strings.
     *
     * @param path The path to xml file to unmarshal
     * @return a handy bundle including the projectconfig and path to it
     * @throws IOException when the file cannot be successfully parsed
     */
    public static ProjectConfigPlus from( Path path )
            throws IOException
    {
        List<String> xmlLines;

        // Default to the path passed in
        String projectConfigOrigin = path.toString();

        // Try to get the full path for more detailed information in logs
        File configFileForInformation = path.toFile();

        try
        {
            projectConfigOrigin = configFileForInformation.getCanonicalPath();
        }
        catch ( IOException ioe )
        {
            // Leave it at the default of what was passed in, warn.
            LOGGER.warn( "Unable to get the full path of file {}",
                         configFileForInformation );
        }

        try
        {
            xmlLines = Files.readAllLines( path );
        }
        catch ( IOException ioe )
        {
            String message = "Could not read file " + projectConfigOrigin;
            // communicate failure back up the stack
            throw new IOException( message, ioe );
        }

        String rawConfig = String.join( System.lineSeparator(), xmlLines );

        return ProjectConfigPlus.from( rawConfig, projectConfigOrigin );
    }

    @Override
    public String toString()
    {
        return this.getOrigin();
    }
}
