/**
 * 
 */
package config.data;

import java.util.ArrayList;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

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
				
	}

	@Override
	protected String tag_name() {
		return "Directory";
	}
	
	public ArrayList<File> get_files()
	{
		return files;
	}
	
	private final ArrayList<File> files = new ArrayList<File>();
	private boolean load_all = false;
	private String path = null;
	
	public final class File extends collections.TwoTuple<String, String>
	{
		public File(String file_type, String path) {
			super(file_type, path);
		}

		public String get_file_type()
		{
			return get_item_one();
		}
		
		public String get_path()
		{
			return get_item_two();
		}
	}
}
