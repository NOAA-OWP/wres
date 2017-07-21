package wres.io.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wres.config.generated.DestinationConfig;
import wres.config.generated.GraphicalType;
import wres.config.generated.ObjectFactory;
import wres.config.generated.ProjectConfig;

import javax.xml.bind.*;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import java.util.*;

public class ProjectConfigPlus
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ProjectConfigPlus.class);

    private final ProjectConfig projectConfig;
    private final Map<DestinationConfig, String> graphicsStrings;
    private final List<ValidationEvent> validationEvents;

    /** Used to find the end of the graphics custom configuration */
    private static final String GFX_END_TAG = "</config>";

    private ProjectConfigPlus(ProjectConfig projectConfig,
                              Map<DestinationConfig, String> graphicsStrings,
                              List<ValidationEvent> validationEvents)
    {
        this.projectConfig = projectConfig;
        this.graphicsStrings = Collections.unmodifiableMap(graphicsStrings);
        List<ValidationEvent> copiedList = new ArrayList<>(validationEvents);
        this.validationEvents = Collections.unmodifiableList(copiedList);
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
     * @return
     * @throws IOException when the file cannot be successfully parsed
     */
    public static ProjectConfigPlus from(Path path) throws IOException
    {
        List<ValidationEvent> events = new ArrayList<>();
        class ValidationEventHandler implements javax.xml.bind.ValidationEventHandler
        {
            @Override
            public boolean handleEvent(final ValidationEvent validationEvent)
            {
                events.add(validationEvent);
                return true;
            }
        }

        ProjectConfig projectConfig;
        try
        {
            File xmlFile = path.toFile();
            Source xmlSource = new StreamSource(xmlFile);
            JAXBContext jaxbContext = JAXBContext.newInstance(ObjectFactory.class);
            Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
            jaxbUnmarshaller.setEventHandler(new ValidationEventHandler());
            final JAXBElement<ProjectConfig> wrappedConfig = jaxbUnmarshaller.unmarshal(xmlSource, ProjectConfig.class);
            projectConfig = wrappedConfig.getValue();

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

        List<String> xmlLines;
        try
        {
            xmlLines = Files.readAllLines(path);
        }
        catch (IOException ioe)
        {
            String message = "Could not read file " + path;
            // communicate failure back up the stack
            throw new IOException(message, ioe);
        }
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
                    config.append(result + System.lineSeparator());
                    result = xmlLines.get(i);
                }

                result = result.substring(0, result.indexOf(GFX_END_TAG));
                config.append(result);

                String configToAdd = config.toString();
                visConfigs.put(d, configToAdd);

                LOGGER.debug("Added following to visConfigs: {}", configToAdd);
            }
        }

        return new ProjectConfigPlus(projectConfig, visConfigs, events);
    }
}
