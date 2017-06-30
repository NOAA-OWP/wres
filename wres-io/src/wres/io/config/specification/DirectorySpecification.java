/**
 * 
 */
package wres.io.config.specification;

import wres.util.XML;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Configuration information detailing files of interest inside of a directory
 *
 * @author Christopher Tubbs
 */
public class DirectorySpecification extends SpecificationElement
{
    /**
     * The Constructor
     * @param reader The XML Reader describing the directory
     */
	public DirectorySpecification(XMLStreamReader reader)
	{
		super(reader);
	}

	@Override
	protected void interpret(XMLStreamReader reader) throws XMLStreamException 
	{
		String tag_name = reader.getLocalName();
		
		if (tag_name.equalsIgnoreCase("path"))
		{
			this.path = XML.getXMLText(reader);
		}
		else if (tag_name.equalsIgnoreCase("file"))
		{
			String file_type = XML.getAttributeValue(reader, "file_type");
			String file_path = XML.getXMLText(reader);
			
			if (!(file_type == null || file_path == null || file_type.isEmpty() || file_path.isEmpty()))
			{
				add_file(new FileSpecification(file_type, file_path));
			}
		}
	}

	@Override
	protected void getAttributes(XMLStreamReader reader)
	{
		for (int attribute_index = 0; attribute_index < reader.getAttributeCount(); ++attribute_index)
		{
			if (reader.getAttributeLocalName(attribute_index).equalsIgnoreCase("load_all"))
			{
				this.loadAll = Boolean.valueOf(reader.getAttributeValue(attribute_index));
			}
		}
	}

	@Override
	protected List<String> tagNames() {
		return Collections.singletonList("Directory");
	}
	
	public boolean shouldLoadAllFiles() {
	    return this.loadAll;
	}

	public Path getPath() throws IOException
	{
	    Path directoryPath = Paths.get(this.path);

	    if (Files.exists(directoryPath)) {
	        return directoryPath;
	    }
	    else
		{
			throw new FileNotFoundException(this.path + " was not found");
		}
	}

	public String getPathString()
	{
		return this.path;
	}

	/**
	 * Returns a list of files in interest inside of the directory
	 * @return A list of file specifications
	 */
	public List<FileSpecification> getFiles ()
	{
		return this.files;
	}
	
	/**
	 * Adds a file specification to the list of contained files
	 * @param file
	 */
	private void add_file(FileSpecification file)
	{
		if (this.files == null)
		{
			this.files = new ArrayList<>();
		}
		
		files.add(file);
	}
	
	@Override
	public String toString() {
		StringBuilder description = new StringBuilder("Directory:");
		description.append(System.lineSeparator());
		
		description.append("\tPath: ");
		description.append(path);
		description.append(System.lineSeparator());
		
		description.append("\tAll files should be loaded: ");
		description.append(String.valueOf(loadAll));
		description.append(System.lineSeparator());
		
		if (files.size() > 0)
		{
			description.append(System.lineSeparator());
			
			for (FileSpecification file : files)
			{
				description.append(file.toString());
			}
		}

		description.append(System.lineSeparator());
		return description.toString();
	}
	
	private List<FileSpecification> files;
	
	/**
	 * Indicates whether or not to load all files within the current level of the directory
	 */
	private boolean loadAll;
	private String path;

    @Override
    public String toXML()
    {
        // TODO Auto-generated method stub
        return null;
    }
}
