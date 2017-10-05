package wres.io.reading;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stax.StAXSource;
import javax.xml.transform.stream.StreamResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.util.Internal;

/**
 * @author Tubbs
 *
 */
@Internal(exclusivePackage = "wres.io")
public class XMLReader 
{
    private String filename;
    private final boolean findOnClasspath;
    private final InputStream inputStream;
    private XMLInputFactory factory = null;

    private final Logger LOGGER = LoggerFactory.getLogger(XMLReader.class);

	/**
	 * 
	 * @param filename the file name
	 */
	@Internal(exclusivePackage = "wres.io")
	protected XMLReader( String filename )
	{
	    this(filename, false);
	}

	@Internal(exclusivePackage = "wres.io")
	protected XMLReader( String fileName, InputStream inputStream )
	{
		this.findOnClasspath = false;
		this.filename = fileName;
		this.inputStream = inputStream;
	}

	@Internal(exclusivePackage = "wres.io")
    protected XMLReader( String filename, boolean findOnClasspath )
	{
	    this.filename = filename;
	    this.findOnClasspath = findOnClasspath;
	    this.inputStream = null;

	    LOGGER.trace( "Created XMLReader for file: {} findOnClasspath={}", filename,
					  findOnClasspath );
	}
	
	protected String getFilename()
	{
		return filename;
	}

	private InputStream getFile()
	{
	    return ClassLoader.getSystemResourceAsStream(filename);
	}

	public void parse() throws IOException
	{
	    XMLStreamReader reader = null;
		try
		{
			reader = createReader();
			
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
	
	private XMLStreamReader createReader() throws FileNotFoundException, XMLStreamException
	{
		XMLStreamReader reader = null;

	    if (factory == null)
	    {
	        factory = XMLInputFactory.newFactory();
	    }

	    try
		{
			if ( findOnClasspath )
			{
				return factory.createXMLStreamReader( getFile());
			}
		}
		catch (XMLStreamException error)
		{
			reader = factory.createXMLStreamReader(new FileReader(getFilename()));
		}

        if (reader == null && this.inputStream != null)
        {
            reader = factory.createXMLStreamReader(inputStream);
        }
		else if (reader == null && this.filename != null)
        {
            reader = factory.createXMLStreamReader(new FileReader(getFilename()));
        }

        return reader;
	}

	public String getRawXML() throws FileNotFoundException, XMLStreamException, TransformerException {
		XMLStreamReader reader = createReader();
		Transformer transformer = TransformerFactory.newInstance().newTransformer();
		StringWriter stringWriter = new StringWriter();
		transformer.transform(new StAXSource(reader), new StreamResult(stringWriter));
		return stringWriter.toString();
	}
	
	@SuppressWarnings("static-method")
    protected void parseElement(XMLStreamReader reader)
            throws IOException
	{
		switch (reader.getEventType())
		{
		case XMLStreamConstants.START_DOCUMENT:
			LOGGER.trace("Start of the document");
			break;
		case XMLStreamConstants.START_ELEMENT:
			LOGGER.trace("Start element = '" + reader.getLocalName() + "'");
			break;
		case XMLStreamConstants.CHARACTERS:
			int begin_index = reader.getTextStart();
			int end_index = reader.getTextLength();
			String value = new String(reader.getTextCharacters(), begin_index, end_index).trim();
			
			if (!value.equalsIgnoreCase(""))
			{
				LOGGER.trace("Value = '" + value + "'");
			}
			
			break;
		case XMLStreamConstants.END_ELEMENT:
			LOGGER.trace("End element = '" + reader.getLocalName() + "'");
			break;
		case XMLStreamConstants.COMMENT:
			if (reader.hasText())
			{
				LOGGER.trace(reader.getText());
			}
			break;
		}
	}
}
