package wres.io.reading;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
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

    private final Logger LOGGER = LoggerFactory.getLogger(XMLReader.class);

	/**
	 * 
	 */
	public XMLReader(String filename)
	{
	    this(filename, false);
		//this.filename = filename;
	}
	
	public XMLReader(String filename, boolean find_on_classpath)
	{
	    this.filename = filename;
	    this.find_on_classpath = find_on_classpath;

	    LOGGER.trace("Created XMLReader for file: {} find_on_classpath={}", filename, find_on_classpath);
	}
	
	protected String getFilename()
	{
		return filename;
	}

	protected InputStream get_file()
	{
	    return ClassLoader.getSystemResourceAsStream(filename);
	}

	public void parse() throws IOException
	{
	    XMLStreamReader reader = null;
		try
		{
			reader = create_reader();
			
			while (reader.hasNext())
			{
				parseElement(reader);
				if (reader.hasNext())
				{
					reader.next();
				}
			}
			
			reader.close();
			completeParsing();
		}
		catch (XMLStreamException | FileNotFoundException error)
		{
			throw new IOException("Could not parse file", error);
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
		            LOGGER.warn("Exception while closing file {}: {}", this.filename, xse);
		        }
		    }
		}
	}
	
	protected void completeParsing(){}
	
	protected void set_filename(String filename)
	{
		this.filename = filename;
	}
	
	protected XMLStreamReader create_reader() throws FileNotFoundException, XMLStreamException
	{
		XMLStreamReader reader = null;

	    if (factory == null)
	    {
	        factory = XMLInputFactory.newFactory();
	    }

	    try
		{
			if (find_on_classpath)
			{
				return factory.createXMLStreamReader(get_file());
			}
		}
		catch (XMLStreamException error)
		{
			reader = factory.createXMLStreamReader(new FileReader(getFilename()));
		}

		if (reader == null)
		{
			reader = factory.createXMLStreamReader(new FileReader(getFilename()));
		}

	    return reader;
	}
	
	@SuppressWarnings("static-method")
    protected void parseElement(XMLStreamReader reader)
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
