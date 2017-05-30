/**
 * 
 */
package wres.io.config.specification;

import java.io.IOException;
import java.util.List;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.util.XML;

/**
 * An element within a configuration file
 * @author Christopher Tubbs
 */
public abstract class SpecificationElement
{

    protected static final String newline = System.lineSeparator();

    private static final Logger LOGGER = LoggerFactory.getLogger(SpecificationElement.class);

    /**
     * The Constructor
     * @param reader The XML Reader containing the configuration elements
     */
	public SpecificationElement(XMLStreamReader reader)
	{
	    if (reader == null)
	    {
	        LOGGER.trace("constructor - reader was null");
	        return;
	    }

	    LOGGER.trace("constructor - reader passed : {}", reader);

		try
		{
			getAttributes(reader);
			
			while (reader.hasNext())
			{
				if (hasEnded(reader))
				{
					break;
				}
				else if (reader.isStartElement())
				{
					interpret(reader);
				}
				next(reader);
			}
		}
		catch (XMLStreamException|IOException e)
		{
			e.printStackTrace();
		}
	}

	/**
	 * Assigns parameters from the XML node to the element
     * @param reader The reader to pull attributes from 
     */
	protected void getAttributes(XMLStreamReader reader){}

	/**
	 * Indicates whether or not the end of the element has been reached
	 * @param reader The XML Reader containing the XML information
	 * @return True if the reader is currently on its closing node
	 */
	protected boolean hasEnded(XMLStreamReader reader)
	{
        if (reader == null)
        {
            LOGGER.trace("hasEnded - reader was null");
        }
        else
        {
            LOGGER.trace("hasEnded - reader passed : {}", reader);
        }
		return XML.xmlTagClosed(reader, this.tagNames());
	}
	
	/**
	 * Moves the reader to the next node if possible (skips whitespace)
	 * @param reader The reader to move
	 * @throws XMLStreamException An exception is thrown if the reader cannot be moved
	 */
	protected static void next(XMLStreamReader reader) throws XMLStreamException
	{
        if (reader == null)
        {
            LOGGER.trace("next - reader was null");
            return;
        }

        LOGGER.trace("next - reader passed : {}", reader);


        if (reader.hasNext()) {
			reader.next();
			if (reader.isWhiteSpace() && reader.hasNext()) {
				reader.next();
			}
		}
	}
	
	/**
	 * Interprets a single start tag
	 * @param reader The reader representing the current XML node
	 * @throws XMLStreamException
	 * @throws IOException 
	 */
	protected abstract void interpret(XMLStreamReader reader) throws XMLStreamException, IOException;
	
	/**
	 * Details a list of possible tag names describing this type of element
	 * @return
	 */
	protected abstract List<String> tagNames();
	
	@Override
	public abstract String toString();
	
	public abstract String toXML();
}
