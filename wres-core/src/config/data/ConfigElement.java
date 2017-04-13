/**
 * 
 */
package config.data;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

/**
 * @author ctubbs
 *
 */
public abstract class ConfigElement {
	public ConfigElement(XMLStreamReader reader)
	{
		try 
		{
			while (reader.hasNext())
			{
				if (reader.isEndElement() && reader.getLocalName().equalsIgnoreCase(tag_name()))
				{
					break;
				}
				else if (reader.isStartElement())
				{
					interpret(reader);
				}
				reader.next();
			}
		} 
		catch (XMLStreamException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * Interprets a single start tag
	 * @param reader The reader representing the current XML node
	 * @throws XMLStreamException
	 */
	protected abstract void interpret(XMLStreamReader reader) throws XMLStreamException;
	protected abstract String tag_name();
}
