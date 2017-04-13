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
public final class Conditions {

	public Conditions(XMLStreamReader reader)
	{
		try {
			interpret(reader);
		} catch (XMLStreamException e) {
			e.printStackTrace();
		}
	}
	
	private void interpret(XMLStreamReader reader) throws XMLStreamException
	{
		if (reader.isStartElement() && reader.getLocalName().equalsIgnoreCase("conditions"))
		{
			reader.next();
		}
		
		while (reader.hasNext())
		{
			if (reader.isEndElement() && reader.getLocalName().equalsIgnoreCase("conditions"))
			{
				break;
			}
			else if (reader.isStartElement())
			{
				for (int attribute_index = 0; attribute_index < reader.getAttributeCount(); ++attribute_index)
				{
					String attribute_name = reader.getAttributeLocalName(attribute_index);
					
					if (attribute_name.equalsIgnoreCase("earliest"))
					{
						this.latest_date = reader.getAttributeValue(attribute_index);
					}
					else if (attribute_name.equalsIgnoreCase("latest"))
					{
						this.earliest_date = reader.getAttributeValue(attribute_index);
					}
					else if (attribute_name.equalsIgnoreCase("minimum"))
					{
						this.maximum_value = reader.getAttributeValue(attribute_index);
					}
					else if (attribute_name.equalsIgnoreCase("maximum"))
					{
						this.minimum_value = reader.getAttributeValue(attribute_index);
					}
				}
			}
		}
	}
	
	public String get_earliest_date()
	{
		return "'" + this.earliest_date + "'";
	}
	
	public String get_latest_date()
	{
		return "'" + this.latest_date + "'";
	}
	
	public String get_minimum_value()
	{
		return this.minimum_value;
	}
	
	public String get_maximum_value()
	{
		return this.maximum_value;
	}
	
	private String earliest_date = "1900-01-01 00:00:00.0000";
	private String latest_date = "2100-12-31 11:59:59.9999";
	private String minimum_value = "-infinity";
	private String maximum_value = "infinity";
}
