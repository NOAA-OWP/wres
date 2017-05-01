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
 * Details conditions to place on selected rows from the database based upon date and value
 * @author Christopher Tubbs
 */
public final class Conditions extends ClauseConfig {

    /**
     * Creates and parses the condition
     * @param reader The XML node(s) containing the definitions for the conditions
     */
	public Conditions(XMLStreamReader reader)
	{
		super(reader);
		
		// Ensure that values are set for the constraints
		setEarliestDate(earliestDate);
		setLatestDate(latestDate);
		setMinimumValue(minimumValue);
		setMaximumValue(maximumValue);
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
				this.latestDate = reader.getAttributeValue(attribute_index);
			}
			else if (attribute_name.equalsIgnoreCase("latest"))
			{
				this.earliestDate = reader.getAttributeValue(attribute_index);
			}
			else if (attribute_name.equalsIgnoreCase("minimum"))
			{
				this.minimumValue = reader.getAttributeValue(attribute_index);
			}
			else if (attribute_name.equalsIgnoreCase("maximum"))
			{
				this.maximumValue = reader.getAttributeValue(attribute_index);
			}
		}
	}
	
	/**
	 * @return Representation of the earliest date that may be selected, formatted in such a way that it may inserted into
	 * a SQL statement
	 */
	public String getEarliestDate()
	{
		return "'" + this.earliestDate + "'";
	}
	
	/**
	 * @return Representation of the latest date that may be selected, formatted in such a way that it may be inserted into
	 * a SQL statement
	 */
	public String getLatestDate()
	{
		return "'" + this.latestDate + "'";
	}
	
	/**
	 * @return Representation of the minimum value for a measurement that may be selected
	 */
	public String getMinimumValue()
	{
		return this.minimumValue;
	}
	
	/**
	 * @return Representation of the maximum value for a measurement that may be selected
	 */
	public String getMaximumValue()
	{
		return this.maximumValue;
	}
	
	/**
	 * Sets the minimum possible value for the condition
	 * @param minimum The minimum possible value for the condition. If the value isn't valid, it is defaulted to -infinity
	 */
	private void setMinimumValue(String minimum)
	{
		if (!Utilities.isNumeric(minimum))
		{
			minimum = "-infinity";
		}
		
		this.minimumValue = minimum;
	}
	
	/**
	 * Sets the maximum possible value for the condition
	 * @param maximum The maximum possible value for the condition. If the value isn't valid, it is defaulted to infinity
	 */
	private void setMaximumValue(String maximum)
	{
		if (!Utilities.isNumeric(maximum))
		{
			maximum = "infinity";
		}
		this.maximumValue = maximum;
	}
	
	/**
	 * Sets the earliest possible date for the condition
	 * @param earliest The earliest possible date. If the date isn't valid, it is set to -infinity
	 */
	private void setEarliestDate(String earliest) {
		if (!Utilities.isTimestamp(earliest)) {
			earliest = "-infinity";
		}
		this.earliestDate = earliest;
	}
	
	/**
	 * Sets the latest possible date for the condition
	 * @param latest The latest possible date. If the date isn't valid, it is set to infinity
	 */
	private void setLatestDate(String latest)
	{
		if (!Utilities.isTimestamp(latest)) {
			latest = "infinity";
		}
		this.latestDate = latest;
	}
	
	private String earliestDate;
	private String latestDate;
	private String minimumValue;
	private String maximumValue;
	
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
		description += earliestDate;
		description += "' to '";
		description += latestDate;
		description += "', with a minimum value of ";
		description += minimumValue;
		description += " up to ";
		description += maximumValue;
		description += System.lineSeparator();
				
		return description;
	}
}
