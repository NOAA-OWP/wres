/**
 * 
 */
package reading;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamReader;

import util.Utilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamConstants;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.InputStream;

/**
 * @author Tubbs
 *
 */
public class XMLReader 
{
    private String filename;
    private final boolean find_on_classpath;
    private XMLInputFactory factory = null;

    private static final Logger LOGGER = LoggerFactory.getLogger(XMLReader.class);

	/**
	 * 
	 */
	public XMLReader(String filename)
	{
		this(filename, false);
	}

	public XMLReader(String filename, boolean find_on_classpath)
	{
	    this.filename = filename;
	    this.find_on_classpath = find_on_classpath;
	}
	
	protected String get_filename()
	{
		return filename;
	}

	protected InputStream get_file()
	{
	    return ClassLoader.getSystemResourceAsStream(filename);
	}

	public void parse()
	{
	    XMLStreamReader reader = null;
		try
		{
			reader = create_reader();
			
			while (reader.hasNext())
			{
				parse_element(reader);
				if (reader.hasNext())
				{
					reader.next();
				}
			}
			
			reader.close();
			
		}
		catch (XMLStreamException | FileNotFoundException error)
		{
			LOGGER.error(error.getMessage());
		}
		finally
		{
		    if (reader != null)
		    {
		        try
		        {
		            reader.close();
		        }
		        catch (XMLStreamException xse)
		        {
		            // not much we can do at this point
		            LOGGER.warn("Exception while closing file {}: {}",
		                        this.filename, xse);
		        }
		    }
		}
	}
	
	protected void set_filename(String filename)
	{
		this.filename = filename;
	}
	
	protected String tag_value(XMLStreamReader reader) throws XMLStreamException
	{
		return Utilities.getXMLText(reader);
	}
	
	protected XMLStreamReader create_reader() throws FileNotFoundException, XMLStreamException
	{
	    if (factory == null)
	    {
	        factory = XMLInputFactory.newFactory();
	    }

		if (find_on_classpath)
		{
		    return factory.createXMLStreamReader(get_file());
		}
		return factory.createXMLStreamReader(new FileReader(get_filename()));
	}
	
	protected boolean tag_is(XMLStreamReader reader, String tag_name)
	{
		return Utilities.tagIs(reader, tag_name);
	}
	
	protected void parse_element(XMLStreamReader reader)
	{
		switch (reader.getEventType())
		{
		case XMLStreamConstants.START_DOCUMENT:
			System.out.println("Start of the document");
			break;
		case XMLStreamConstants.START_ELEMENT:
			System.out.println("Start element = '" + reader.getLocalName() + "'");
			break;
		case XMLStreamConstants.CHARACTERS:
			int begin_index = reader.getTextStart();
			int end_index = reader.getTextLength();
			String value = new String(reader.getTextCharacters(), begin_index, end_index).trim();
			
			if (!value.equalsIgnoreCase(""))
			{
				System.out.println("Value = '" + value + "'");
			}
			
			break;
		case XMLStreamConstants.END_ELEMENT:
			System.out.println("End element = '" + reader.getLocalName() + "'");
			break;
		case XMLStreamConstants.COMMENT:
			if (reader.hasText())
			{
				System.out.println(reader.getText());
			}
			break;
		}
	}
}
