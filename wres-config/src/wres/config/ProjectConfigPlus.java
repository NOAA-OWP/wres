package wres.config;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

import wres.config.generated.DestinationConfig;
import wres.config.generated.GraphicalType;
import wres.config.generated.ObjectFactory;
import wres.config.generated.ProjectConfig;

/**
 * Associates a project configuration object with its graphgen xml string.
 *
 * Also useful for jaxb-discovered validation issues during parsing.
 *
 * At the highest level, when reading the project config from a path, we also
 * gather the xml string needed by wres-vis to generate graphics.
 *
 * The reason for this extra step is to facilitate re-use of existing code that
 * expects an unmarshaled xml string, and this works OK for now.
 *
 * It is anticipated that most parts of WRES will use ProjectConfig.
 *
 * When performing initial read of the config and validation, it will be handy
 * to keep a copy of the (stateful) information that was found during read, so
 * as of 2017-07-24 ProjectConfigPlus is anticipated to be used primarily by
 * the control module.
 *
 * Intended to be Thread-safe, and the object is. Performs one read then uses
 * and keeps the resulting file in a string.
 */
public class ProjectConfigPlus
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ProjectConfigPlus.class);

    private final Path path;
    private final String rawConfig;
    private final ProjectConfig projectConfig;
    private final Map<DestinationConfig, String> graphicsStrings;
    private final List<ValidationEvent> validationEvents;

    /** Used to find the end of the graphics custom configuration */
    private static final String GFX_END_TAG = "</config>";

    private static final String XSD_NAME = "ProjectConfig.xsd";

    private ProjectConfigPlus( Path path,
                               String rawConfig,
                               ProjectConfig projectConfig,
                               Map<DestinationConfig, String> graphicsStrings,
                               List<ValidationEvent> validationEvents )
    {
        this.path = path;
        this.rawConfig = rawConfig;
        this.projectConfig = projectConfig;
        this.graphicsStrings = Collections.unmodifiableMap(graphicsStrings);
        List<ValidationEvent> copiedList = new ArrayList<>(validationEvents);
        this.validationEvents = Collections.unmodifiableList(copiedList);
    }

    public Path getPath()
    {
        return this.path;
    }

    /**
     * Get the full path and filename for this ProjectConfig
     * @return the full path and filename
     * @throws IOException when the canonical path cannot be determined
     */
    public String getCanonicalPath() throws IOException
    {
        File file = this.getPath()
                        .toFile();
        return file.getCanonicalPath();
    }

    public String getRawConfig()
    {
        return this.rawConfig;
    }

    public ProjectConfig getProjectConfig()
    {
        // safety ops performed at construction
        return this.projectConfig;
    }

    public Map<DestinationConfig, String> getGraphicsStrings()
    {
        // safety ops performed at construction
        return this.graphicsStrings;
    }

    public List<ValidationEvent> getValidationEvents()
    {
        // safety ops performed at construction
        return this.validationEvents;
    }

    /**
     * Parse a config file, store validation events, get the vis config strings.
     *
     * @param path The path to xml file to unmarshal
     * @return a handy bundle including the projectconfig and path to it
     * @throws IOException when the file cannot be successfully parsed
     */
    public static ProjectConfigPlus from(Path path) throws IOException
    {
        List<ValidationEvent> events = new ArrayList<>();
        class ProjValidationEventHandler implements ValidationEventHandler
        {
            @Override
            public boolean handleEvent(final ValidationEvent validationEvent)
            {
                events.add(validationEvent);
                return true;
            }
        }

        List<String> xmlLines;

        /** To get the full path to display information about it */
        File configFileForInformation = path.toFile();

        try
        {
            xmlLines = Files.readAllLines( path );
        }
        catch ( IOException ioe )
        {
            String message = "Could not read file "
                             + configFileForInformation.getCanonicalPath();
            // communicate failure back up the stack
            throw new IOException( message, ioe );
        }

        String rawConfig = String.join(System.lineSeparator(), xmlLines);

        ProjectConfig projectConfig;
        try
        {
            Reader reader = new StringReader( rawConfig );
            Source xmlSource = new StreamSource( reader );
            JAXBContext jaxbContext = JAXBContext.newInstance(ObjectFactory.class);

            // Validate against schema during unmarshaling
            InputStream schemaStream = ClassLoader.getSystemResourceAsStream( XSD_NAME );
            Source schemaSource = new StreamSource( schemaStream );
            SchemaFactory schemaFactory =
                    SchemaFactory.newInstance( XMLConstants.W3C_XML_SCHEMA_NS_URI );
            Schema schema = schemaFactory.newSchema( schemaSource );

            Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
            jaxbUnmarshaller.setSchema( schema );
            jaxbUnmarshaller.setEventHandler(new ProjValidationEventHandler());
            final JAXBElement<ProjectConfig> wrappedConfig = jaxbUnmarshaller.unmarshal(xmlSource, ProjectConfig.class);
            projectConfig = wrappedConfig.getValue();

            if ( LOGGER.isInfoEnabled() )
            {
                LOGGER.info( "Unmarshalled project configuration file "
                             + configFileForInformation.getCanonicalPath() );
            }

            if (projectConfig == null
                || projectConfig.getInputs() == null
                || projectConfig.getOutputs() == null
                || projectConfig.getPair() == null)
            {
                throw new IOException("Please add required sections in project config file "
                                      + path + " : <inputs>, <outputs>, <pair>");
            }

            for (final DestinationConfig d : projectConfig.getOutputs().getDestination())
            {
                if (d.getGraphical() != null && d.getGraphical().getConfig() != null)
                {
                    final GraphicalType.Config conf = d.getGraphical().getConfig();
                    LOGGER.debug("Location of config for {} is line {} col {}",
                            d,
                            conf.sourceLocation().getLineNumber(),
                            conf.sourceLocation().getColumnNumber());
                }
            }
        }
        catch (final JAXBException je)
        {
            String message = "Could not parse file " + path;
            // communicate failure back up the stack
            throw new IOException(message, je);
        }
        catch (final NumberFormatException nfe)
        {
            String message = "A value in the file " + path + " was unable to be converted to a number.";
            // communicate failure back up the stack
            throw new IOException(message, nfe);
        }
        catch ( SAXException e )
        {
            String message = "Expected a valid XML schema on classpath at "
                             + XSD_NAME + ".";
            throw new IOException( message, e );
        }

        Map<DestinationConfig, String> visConfigs =
                ProjectConfigPlus.getVisConfigs( projectConfig,
                                                 xmlLines );

        return new ProjectConfigPlus( path,
                                      rawConfig,
                                      projectConfig,
                                      visConfigs,
                                      events );
    }


    /**
     * Get wres-vis graphical configuration xml strings mapped to Destinations
     * @param projectConfig the config containing the destinations
     * @param xmlLines the raw xml lines that were parsed to produce projectConfig
     * @return a Map of DestinationConfig to graphical configuration xml
     */

    private static Map<DestinationConfig, String> getVisConfigs( ProjectConfig projectConfig,
                                                                 List<String> xmlLines )
    {
        // To read the xml configuration for vis into a string, we go find the
        // start of each, then find the first occurance of </config> ?

        Map<DestinationConfig, String> visConfigs = new HashMap<>();

        for (DestinationConfig d : projectConfig.getOutputs().getDestination())
        {
            if (d.getGraphical() != null && d.getGraphical().getConfig() != null)
            {
                GraphicalType.Config conf = d.getGraphical().getConfig();
                int lineNum = conf.sourceLocation().getLineNumber();
                int colNum = conf.sourceLocation().getColumnNumber();

                LOGGER.debug("Location of config for {} is line {} col {}", d,
                        lineNum, colNum);

                // lines seem to be 1-based in sourceLocation.
                StringBuilder config = new StringBuilder();
                String result = xmlLines.get(lineNum - 1).substring(colNum - 1);

                for (int i = lineNum; !result.contains(GFX_END_TAG); i++)
                {
                    config.append( result );
                    config.append( System.lineSeparator() );
                    result = xmlLines.get(i);
                }

                result = result.substring(0, result.indexOf(GFX_END_TAG));
                config.append(result);

                String configToAdd = config.toString();
                visConfigs.put(d, configToAdd);

                LOGGER.debug("Added following to visConfigs: {}", configToAdd);
            }
        }

        return visConfigs;
    }


    @Override
    public String toString()
    {
        try
        {
            return this.getCanonicalPath();
        }
        catch ( IOException ioe )
        {
            return this.path.toString();
        }
    }
}
