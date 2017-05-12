/**
 * 
 */
package config.specification;

import java.util.Arrays;
import java.util.List;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

/**
 * Specification for a point in gridded data that should be selected
 * @author Christopher Tubbs
 */
public class PointSpecification extends FeatureSpecification {

	/**
	 * Constructor
	 * @param reader The XML Stream containing the point data
	 */
	public PointSpecification(XMLStreamReader reader) {
		super(reader);
		
		if (x == null)
		{
			x = "0";
		}
		
		if (y == null)
		{
			y = "null";
		}
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
	
	/**
	 * @return String representation of the x index of the point to select
	 */
	public String x()
	{
		return x;
	}
	
	/**
	 * @return String representation of the y index of the point to select
	 */
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

    @Override
    public List<Integer> getVariablePositionIDs(Integer variableID)
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String toXML()
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int getFeatureType()
    {
        return FeatureSpecification.POINT;
    }
}
