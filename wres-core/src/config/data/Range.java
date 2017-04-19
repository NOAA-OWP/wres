/**
 * 
 */
package config.data;

import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;

import javax.xml.stream.XMLStreamReader;

/**
 * @author ctubbs
 *
 */
public class Range extends ClauseConfig {
	
	public Range(XMLStreamReader reader) {
		super(reader);
	}

	@Override
	protected void interpret(XMLStreamReader reader) {
		for (int attributeIndex = 0; attributeIndex < reader.getAttributeCount(); ++attributeIndex)
		{
			String attribute_name = reader.getAttributeLocalName(attributeIndex);
			if (attribute_name.equalsIgnoreCase("x_minimum"))
			{
				this.xMinimum = reader.getAttributeValue(attributeIndex);
			}
			else if (attribute_name.equalsIgnoreCase("x_maximum"))
			{
				this.xMaximum = reader.getAttributeValue(attributeIndex);
			}
			else if (attribute_name.equalsIgnoreCase("y_minimum"))
			{
				this.yMinimum = reader.getAttributeValue(attributeIndex);
			}
			else if (attribute_name.equalsIgnoreCase("y_maximum"))
			{
				this.yMaximum = reader.getAttributeValue(attributeIndex);
			}
		}
	}

	@Override
	protected List<String> tagNames() {
		return Arrays.asList("range");
	}
	
	/* (non-Javadoc)
	 * @see config.data.Feature#get_condition()
	 */
	@Override
	public String getCondition(TreeMap<String, String> aliases) {
		boolean appendAnd = false;
		String condition = "";
		
		if (xMinimum != null)
		{
			condition += aliases.get("variableposition_alias") + ".x_position >= '" + xMinimum;
			appendAnd = true;
		}
		
		if (xMaximum != null)
		{
			if (appendAnd)
			{
				condition += " AND ";
			}
			condition += aliases.get("variableposition_alias") + ".x_position <= " + xMaximum;
			appendAnd = true;
		}
		
		if (yMinimum != null)
		{
			if (appendAnd)
			{
				condition += " AND ";
			}
			condition += aliases.get("variableposition_alias") + ".y_position >= " + yMinimum;
			appendAnd = true;
		}
		
		if (yMaximum != null)
		{
			if (appendAnd)
			{
				condition += " AND ";
			}
			condition += aliases.get("variableposition_alias") + ".y_position <= " + yMaximum;
		}
		
		if (!condition.isEmpty()){
			condition = "(" + condition + ")";
		}

		return condition;
	}
	
	public String xMinimum()
	{
		return xMinimum;
	}
	
	public String xMaximum()
	{
		return xMaximum;
	}
	
	public String yMinimum()
	{
		return yMinimum;
	}
	
	public String yMaximum()
	{
		return yMaximum;
	}
	
	@Override
	public String toString() 
	{
		String description = "Range:";
		description += System.lineSeparator();
		
		description += "\tAll values with x indices starting at ";
		
		if (xMinimum == null)
		{
			description += "-1";
		}
		else
		{
			description += xMinimum;
		}
		
		description += " and ending at ";
				
		if (xMaximum == null)
		{
			description += "infinity";
		}
		else
		{
			description += xMaximum;
		}
		
		description += ", and y indices starting at ";
		
		if (yMinimum == null)
		{
			description += "-1";
		}
		else
		{
			description += yMinimum;
		}
		
		description += " and ending at ";
		
		if (yMaximum == null)
		{
			description += "infinity";
		}
		else
		{
			description += yMaximum;
		}
		
		return description;
	}

	private String xMinimum = null;
	private String xMaximum = null;
	private String yMinimum = null;
	private String yMaximum = null;
}
