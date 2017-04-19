/**
 * 
 */
package config.data;

import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

/**
 * @author ctubbs
 *
 */
public class Point extends ClauseConfig {

	/**
	 * @param reader The XML Stream containing the point data
	 */
	public Point(XMLStreamReader reader) {
		super(reader);
		
		if (x == null)
		{
			x = "0";
		}
		
		if (y == null)
		{
			y = "0";
		}
	}
	
	@Override
	public String getCondition(TreeMap<String, String> aliases) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected void interpret(XMLStreamReader reader) throws XMLStreamException {
		for (int attributeIndex = 0; attributeIndex < reader.getAttributeCount(); ++attributeIndex)
		{
			if (reader.getLocalName().equalsIgnoreCase("x"))
			{
				this.x = reader.getAttributeValue(attributeIndex);
			}
			else if (reader.getLocalName().equalsIgnoreCase("y"))
			{
				this.y = reader.getAttributeValue(attributeIndex);
			}
		}
	}

	@Override
	protected List<String> tagNames() {
		return Arrays.asList("point");
	}
	
	public String x()
	{
		return x;
	}
	
	public String y()
	{
		return y;
	}
	
	@Override
	public String toString() {
		String description = "Point:";
		description += System.lineSeparator();
		
		description += "\tX: ";
		description += x();
		description += System.lineSeparator();
		
		description += "\tY: ";
		description += y();
		description += System.lineSeparator();
		
		return description;
	}

	private String x;
	private String y;
}
