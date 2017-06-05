package wres.io.config;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wres.io.config.specification.ProjectSpecification;
import wres.io.reading.XMLReader;
import wres.util.Collections;
import wres.util.XML;

/**
 * Provides access to projects determining the configuration of metric execution
 * @author Christopher Tubbs
 */
public final class Projects extends XMLReader {

    private static final Logger LOGGER = LoggerFactory.getLogger(Projects.class);

    /**
     *  The underlying storage structure for the project configurations
     */
	private static final Projects configuration = new Projects();
	
	/**
	 * Private Constructor
	 * <br/><br/>
	 * There should be no other instances of the project configuration. The Projects
	 * is merely a cache for configurations
	 */
	private Projects() {
		super(SystemSettings.getProjectDirectory());
		try
		{
			loadProjects();
		}
		catch (IOException ioe)
		{
			LOGGER.error("Could not load project files.", ioe);
		}
	}
	
	/**
	 * Loads all project configurations defined within a directory
	 */
	private void loadProjects() throws IOException
	{
		File directory = new File(this.getFilename());
		
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
            try
            {
                addProject(new ProjectSpecification(reader));
            }
            catch (IOException|XMLStreamException e)
            {
                System.err.println();
                System.err.println();

                System.err.println("A project could not be parsed correctly.");

                System.err.println();
                System.err.println();
                e.printStackTrace();
            }
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

	/**
	 * The collection of all loaded projects
	 */
	private List<ProjectSpecification> projects;
}
