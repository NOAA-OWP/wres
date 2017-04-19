/**
 * 
 */
package config.data;

import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import util.Utilities;

/**
 * @author ctubbs
 *
 */
public final class Conditions extends ClauseConfig {

	public Conditions(XMLStreamReader reader)
	{
		super(reader);
		
		if (earliest_date == null)
		{
			earliest_date = "1900-01-01 00:00:00.0000";
		}
		
		if (latest_date == null) {
			latest_date = "2100-12-31 11:59:59.9999";
		}
		
		if (minimum_value == null) {
			minimum_value = "-infinity";
		}
		
		if (maximum_value == null)
		{
			maximum_value = "infinity";
		}
	}
	
	@Override
	protected void interpret(XMLStreamReader reader) throws XMLStreamException
	{
		// Loop through all attributes on the element. Since all attributes have unique names,
		// the name of the element is not important
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
				this.minimum_value = reader.getAttributeValue(attribute_index);
			}
			else if (attribute_name.equalsIgnoreCase("maximum"))
			{
				this.maximum_value = reader.getAttributeValue(attribute_index);
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
	
	private void setMinimumValue(String minimum)
	{
		if (minimum == null || Utilities.contains(new String[]{"NA",  "na", "none", "Na", "N\\A", "n\\a", "None", "NONE"}, minimum))
		{
			minimum = "-infinity";
		}
		
		this.minimum_value = minimum;
	}
	
	private String earliest_date;
	private String latest_date;
	private String minimum_value;
	private String maximum_value;
	
	@Override
	public String getCondition(TreeMap<String, String> aliases) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected List<String> tagNames() {
		return Arrays.asList("conditions");
	}
	
	@Override
	public String toString() {
		String description = "Relevant data is from '";
		description += earliest_date;
		description += "' to '";
		description += latest_date;
		description += "', with a minimum value of ";
		description += minimum_value;
		description += " up to ";
		description += maximum_value;
		description += System.lineSeparator();
				
		return description;
	}
}
