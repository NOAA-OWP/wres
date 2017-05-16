/**
 * 
 */
package config;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import config.specification.ProjectSpecification;
import reading.XMLReader;
import util.Utilities;

/**
 * Provides access to projects determining the configuration of metric execution
 * @author Christopher Tubbs
 */
public final class ProjectConfig extends XMLReader {

    /**
     *  The underlying storage structure for the project configurations
     */
	private static final ProjectConfig configuration = new ProjectConfig();
	
	/**
	 * Private Constructor
	 * <br/><br/>
	 * There should be no other instances of the project configuration. The ProjectConfig
	 * is merely a cache for configurations
	 */
	private ProjectConfig() {
		super(SystemConfig.getProjectDirectory());
		loadProjects();
	}
	
	/**
	 * Loads all project configurations defined within a directory
	 */
	private void loadProjects()
	{
		File directory = new File(this.getFilename());
		
		FilenameFilter filter = new FilenameFilter() {			
			@Override
			public boolean accept(File dir, String name) {
				File possibleFile = new File(Paths.get(dir.getAbsolutePath(), name).toAbsolutePath().toString());
				return possibleFile.isFile() && possibleFile.getName().endsWith(".xml");
			}
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
    /**
     * If the found tag is a project, parses the contents and saves it as a new project
     */
    protected void parseElement(XMLStreamReader reader)
    {
        if (Utilities.tagIs(reader, "project"))
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
	    return Utilities.find(getProjects(), (ProjectSpecification project) -> {
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
			this.projects = new ArrayList<ProjectSpecification>();
		}
		
		this.projects.add(project);
	}

	/**
	 * The collection of all loaded projects
	 */
	private List<ProjectSpecification> projects;
}
