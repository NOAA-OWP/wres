/**
 * 
 */
package config.data;

import java.util.List;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import util.Utilities;

/**
 * An element within a configuration file
 * @author Christopher Tubbs
 */
public abstract class ConfigElement {
    
    protected static final String newline = System.lineSeparator();
    
    /**
     * The Constructor
     * @param reader The XML Reader containing the configuration elements
     */
	public ConfigElement(XMLStreamReader reader)
	{
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
		catch (Exception e) 
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
		return Utilities.xmlTagClosed(reader, tagNames());
	}
	
	/**
	 * Moves the reader to the next node if possible (skips whitespace)
	 * @param reader The reader to move
	 * @throws XMLStreamException An exception is thrown if the reader cannot be moved
	 */
	protected static void next(XMLStreamReader reader) throws XMLStreamException {
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
	 * @throws Exception 
	 */
	protected abstract void interpret(XMLStreamReader reader) throws XMLStreamException, Exception;
	
	/**
	 * Details a list of possible tag names describing this type of element
	 * @return
	 */
	protected abstract List<String> tagNames();
	
	@Override
	public abstract String toString();
}
