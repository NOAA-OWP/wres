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
public final class ConditionSpecification extends SpecificationElement {

    /**
     * Creates and parses the condition
     * @param reader The XML node(s) containing the definitions for the conditions
     */
	public ConditionSpecification(XMLStreamReader reader)
	{
		super(reader);
		
		// Ensure that values are set for the constraints
		setEarliestDate(earliestDate);
		setLatestDate(latestDate);
		setMinimumValue(minimumValue);
		setMaximumValue(maximumValue);
		setOffset(this.offset);
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
			if (attribute_name.equalsIgnoreCase("offset"))
			{
			    this.offset = reader.getAttributeValue(attribute_index);
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
	 * @return Returns the hourly offset to use for pairing
	 */
	public String getOffset()
	{
	    return this.offset;
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
	
	/**
	 * Sets the hourly offset for the condition
	 * @param offset The updated offset. If the offset isn't a valid number, it defaults to 0
	 */
	private void setOffset(String offset)
	{
	    if (!Utilities.isNumeric(offset))
	    {
	        offset = "0";
	    }
	    this.offset = offset;
	}
	
	public boolean hasEarliestDate()
	{
	    return this.earliestDate != null && 
	           !this.earliestDate.isEmpty() &&
	           !this.earliestDate.equalsIgnoreCase("-infinity");
	}
	
	public boolean hasLatestDate()
	{
	    return this.latestDate != null && 
	           !this.latestDate.isEmpty() && 
	           !this.latestDate.equalsIgnoreCase("infinity");
	}
	
	public boolean hasMinimumValue()
	{
	    return this.minimumValue != null && 
	           !this.minimumValue.isEmpty() && 
	           !this.minimumValue.equalsIgnoreCase("-infinity");
	}
	
	public boolean hasMaximumValue()
	{
	    return this.maximumValue != null &&
	           !this.maximumValue.isEmpty() &&         
	           !this.maximumValue.equalsIgnoreCase("infinity");
	}
	
	public boolean hasOffset()
	{
	    return this.offset != null && 
	           !this.offset.equalsIgnoreCase("0") &&
	           !this.offset.isEmpty();
	}
	
	/**
	 * The earliest date and time to consider
	 */
	private String earliestDate;
	
	/**
	 * The latest date and time to consider
	 */
	private String latestDate;
	
	/**
	 * The minimum value for consideration
	 */
	private String minimumValue;
	
	/**
	 * The maximum value for consideration
	 */
	private String maximumValue;
	
	/**
	 * The number of hours to offset the time of an entry
	 */
	private String offset;

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

    @Override
    public String toXML()
    {
        // TODO Auto-generated method stub
        return null;
    }
}
