package wres.io.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wres.config.generated.DestinationConfig;
import wres.config.generated.ObjectFactory;
import wres.config.generated.ProjectConfig;
import wres.io.config.specification.ProjectSpecification;
import wres.io.reading.XMLReader;
import wres.util.Collections;
import wres.util.Strings;
import wres.util.XML;

import javax.xml.bind.*;
import javax.xml.stream.XMLStreamReader;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Provides access to projects determining the configuration of metric execution
 * @author Christopher Tubbs
 */
public final class ProjectSettings extends XMLReader {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProjectSettings.class);
    private static final List<ProjectConfig> CONFIGS = new ArrayList<>();

    /**
     *  The underlying storage structure for the project configurations
     */
	private static final ProjectSettings configuration = new ProjectSettings();
	
	/**
	 * Private Constructor
	 * <br/><br/>
	 * There should be no other instances of the project configuration. The Projects
	 * is merely a cache for configurations
	 */
	private ProjectSettings() {
		super(SystemSettings.getProjectDirectory());
		try
		{
			loadProjects();
			loadConfigs();
		}
		catch (IOException ioe)
		{
			LOGGER.error("Could not load project files.", ioe);
		}
        catch (JAXBException e) {
            LOGGER.error("The JAXB config was not loaded correctly.");
            LOGGER.error(Strings.getStackTrace(e));
        }
    }
	
	/**
	 * Loads all project configurations defined within a directory
	 */
	private void loadProjects() throws IOException
	{
		File directory = new File(this.getFilename());

		if (directory == null)
		{
			return;
		}
		
		FilenameFilter filter = (dir, name) -> {
            File possibleFile = new File(Paths.get(dir.getAbsolutePath(), name).toAbsolutePath().toString());
            return possibleFile.isFile() && possibleFile.getName().endsWith(".xml");
        };
		
		for (File file : directory.listFiles(filter))
		{
			set_filename(file.getAbsolutePath());
			parse();
		}
	}

	private void loadConfigs() throws JAXBException {
        final String configLocation = "nonsrc/config_possibility.xml";
        ProjectConfig projectConfig;

        try
        {
            File xmlFile = new File(configLocation);
            Source xmlSource = new StreamSource(xmlFile);
            JAXBContext jaxbContext = JAXBContext.newInstance(ObjectFactory.class);
            Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();

            jaxbUnmarshaller.setEventHandler(validationEvent -> {
                if (LOGGER.isDebugEnabled())
                {
                    LOGGER.debug("Severity: {}", validationEvent.getSeverity());
                    LOGGER.debug("Location: {}", validationEvent.getLocator());
                    LOGGER.debug("Message: {}", validationEvent.getMessage());
                }
                return true;
            });

            JAXBElement<ProjectConfig> wrappedConfig = jaxbUnmarshaller.unmarshal(xmlSource, ProjectConfig.class);

            projectConfig = wrappedConfig.getValue();
            ProjectSettings.CONFIGS.add(projectConfig);

            LOGGER.debug("ProjectConfig: {}", projectConfig);
            if (LOGGER.isDebugEnabled())
            {
                LOGGER.debug("JAXBContext: {}", jaxbContext);
                LOGGER.debug("Unmarshaller: {}", jaxbUnmarshaller);
                LOGGER.debug("ProjectConfig.getName(): {}", projectConfig.getLabel());
                LOGGER.debug("ProjectConfig metric 0: {}", projectConfig.getOutputs().getMetric().get(0).getValue());
                LOGGER.debug("ProjectConfig forecast variable: {}", projectConfig.getInputs());

                for (DestinationConfig d : projectConfig.getOutputs().getDestination())
                {
                    LOGGER.debug("Location of destination {} is line {} col {}",
                                 d,
                                 d.sourceLocation().getLineNumber(),
                                 d.sourceLocation().getColumnNumber());

                    if (d.getConfig() != null)
                    {
                        DestinationConfig.Config conf = d.getConfig();
                        LOGGER.debug("Location of config for {} is line {} col {}",
                                     d,
                                     conf.sourceLocation().getLineNumber(),
                                     conf.sourceLocation().getColumnNumber());
                    }
                }
            }
        }
        catch (JAXBException je)
        {
            LOGGER.error("Could not parse file {}:", configLocation, je);
            throw je;
        }
        catch (NumberFormatException nfe)
        {
            LOGGER.error("A value in the file {} was unable to be converted to a number.", configLocation, nfe);
            throw nfe;
        }
    }
	
	/**
	 * @return The collection of all configured projects
	 */
	public static List<ProjectSpecification> getProjects() {
		return configuration.projects;
	}
	
    @Override
	protected void parseElement(XMLStreamReader reader)
    {
        if (XML.tagIs(reader, "project"))
        {
            addProject(new ProjectSpecification(reader));
        }
    }

	/**
	 * Finds a project based on its name
	 * @param projectName The name of the desired project
	 * @return The project
	 */
	public static ProjectSpecification getProject(String projectName) {
	    return Collections.find(getProjects(), (ProjectSpecification project) -> {
	        return project.getName().equalsIgnoreCase(projectName);
	    });
	}
	
	/**
	 * Adds a project configuration to the collection of project specifications
	 * @param project The configuration to add to the list
	 */
	private void addProject(ProjectSpecification project)
	{
		if (project == null)
		{
			return;
		}
		
		if (this.projects == null)
		{
			this.projects = new ArrayList<>();
		}
		
		this.projects.add(project);
	}

    public static boolean isModernProject(final String projectName)
    {
        return Collections.exists(ProjectSettings.CONFIGS, projectConfig -> {
            return projectConfig.getLabel().equalsIgnoreCase(projectName);
        });
    }

    public static boolean isLegacyProject(final String projectName)
    {
        return Collections.exists(getProjects(), projectSpecification -> {
            return projectSpecification.getName().equalsIgnoreCase(projectName);
        });
    }

    public static ProjectConfig getModernProject(final String projectName)
    {
        return Collections.find(ProjectSettings.CONFIGS, projectConfig -> {
            return projectConfig.getLabel().equalsIgnoreCase(projectName);
        });
    }

	/**
	 * The collection of all loaded projects
	 */
	private List<ProjectSpecification> projects;
}
