/**
 * 
 */
package config.data;

import java.util.List;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import util.Utilities;

/**
 * @author ctubbs
 *
 */
public abstract class ConfigElement {
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
	
	protected String tagValue(XMLStreamReader reader) throws XMLStreamException
	{
		return Utilities.getXMLText(reader);
	}

	protected void getAttributes(XMLStreamReader reader){}

	protected boolean hasEnded(XMLStreamReader reader)
	{
		return Utilities.xmlTagClosed(reader, tagNames());
	}
	
	protected boolean tagIs(XMLStreamReader reader, String tag_name)
	{
		return Utilities.tagIs(reader, tag_name);
	}
	
	protected void next(XMLStreamReader reader) throws XMLStreamException
	{
		if (reader.hasNext())
		{
			reader.next();
			if (reader.isWhiteSpace() && reader.hasNext())
			{
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
	protected abstract List<String> tagNames();
	
	@Override
	public abstract String toString();
}
