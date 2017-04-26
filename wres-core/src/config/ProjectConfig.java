/**
 * 
 */
package config;

import java.io.File;
import java.io.FilenameFilter;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import javax.xml.stream.XMLStreamReader;

import config.data.Project;
import reading.XMLReader;

/**
 * @author ctubbs
 *
 */
public class ProjectConfig extends XMLReader {

	private static final ProjectConfig configuration = new ProjectConfig();
	/**
	 * 
	 */
	public ProjectConfig() {
		super(SystemConfig.instance().get_project_directory());
		load_projects();
	}
	
	private void load_projects()
	{
		File directory = new File(this.get_filename());
		
		FilenameFilter filter = new FilenameFilter() {			
			@Override
			public boolean accept(File dir, String name) {
				File possible_file = new File(Paths.get(dir.getAbsolutePath(), name).toAbsolutePath().toString());
				return possible_file.isFile() && possible_file.getName().endsWith(".xml");
			}
		};
		
		for (File file : directory.listFiles(filter))
		{
			set_filename(file.getAbsolutePath());
			parse();
		}
	}
	
	public static List<Project> get_projects()
	{
		return configuration.projects;
	}
	
	@Override
	protected void parseElement(XMLStreamReader reader)
	{
		if (tag_is(reader, "project"))
		{
			try {
				addProject(new Project(reader));
			} catch (Exception e) {
				System.err.println();
				System.err.println();
				
				System.err.println("A project could not be parsed correctly.");
				
				System.err.println();
				System.err.println();
				e.printStackTrace();
			}
		}
	}
	
	private void addProject(Project project)
	{
		if (project == null)
		{
			return;
		}
		
		if (this.projects == null)
		{
			this.projects = new ArrayList<Project>();
		}
		
		this.projects.add(project);
	}

	private List<Project> projects;
}
