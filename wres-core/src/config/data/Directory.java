/**
 * 
 */
package config.data;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import util.Utilities;

/**
 * @author ctubbs
 *
 */
public class Directory extends ConfigElement
{
	public Directory(XMLStreamReader reader)
	{
		super(reader);
	}

	@Override
	protected void interpret(XMLStreamReader reader) throws XMLStreamException 
	{
		String tag_name = reader.getLocalName();
		
		if (tag_name.equalsIgnoreCase("path"))
		{
			this.path = tagValue(reader);
		}
		else if (tag_name.equalsIgnoreCase("file"))
		{
			String file_type = Utilities.get_attribute_value(reader, "file_type");
			String file_path = tagValue(reader);
			
			if (!(file_type == null || file_path == null || file_type.isEmpty() || file_path.isEmpty()))
			{
				add_file(new File(file_type, file_path));
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
				this.load_all = Boolean.valueOf(reader.getAttributeValue(attribute_index));
			}
		}
	}

	@Override
	protected List<String> tagNames() {
		return Arrays.asList("Directory");
	}
	
	public List<File> get_files()
	{
		return files;
	}
	
	private void add_file(File file)
	{
		if (this.files == null)
		{
			this.files = new ArrayList<File>();
		}
		
		files.add(file);
	}
	
	@Override
	public String toString() {
		String description = "Directory:";
		description += System.lineSeparator();
		
		description += "\tPath: ";
		description += path;
		description += System.lineSeparator();
		
		description += "\tAll files should be loaded: ";
		description += String.valueOf(load_all);
		description += System.lineSeparator();
		
		if (files.size() > 0)
		{
			description += System.lineSeparator();
			
			for (File file : files)
			{
				description += file.toString();
			}
		}

		description += System.lineSeparator();
		return description;
	}
	
	private List<File> files;
	private boolean load_all;
	private String path;
	
	public final class File extends collections.TwoTuple<String, String>
	{
		public File(String file_type, String path) {
			super(file_type, path);
		}

		public String get_file_type()
		{
			return itemOne();
		}
		
		public String get_path()
		{
			return itemTwo();
		}
		
		@Override
		public String toString() {
			String description = "\tFile: ";
			description += get_path();
			description += ", Type: ";
			description += get_file_type();
			description += System.lineSeparator();
			
			return description;
		}
	}
}
