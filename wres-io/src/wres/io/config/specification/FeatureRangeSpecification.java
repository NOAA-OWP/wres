/**
 * 
 */
package wres.io.config.specification;

import java.util.Arrays;
import java.util.List;
import javax.xml.stream.XMLStreamReader;

/**
 * Specification for a range of indices for a vector or gridded variable to load from the
 * database. Provides support for bounding box selection
 * <br><br>
 * All possible values for any variable in the database for any given location is stored in the
 * database as infinite gridded information indicated by x and y coordinates. One dimensional 
 * Vector data is stored in a conceptual 2 dimensional array where all y values are stored as
 * <b>null</b>. When using ranges for this vector data, only the x values will be considered in
 * the actual select statements since the y coordinates will forever be <b>null</b>. For gridded
 * data, bounding boxes may be drawn by indicating the inclusive box vertices selecting data
 * between (<b>xMinimum</b>, <b>yMinimum</b>) and (<b>xMaximum</b>, <b>yMaximum</b>). 
 * 
 * @author Christopher Tubbs
 * 
 */
public class FeatureRangeSpecification extends FeatureSpecification {
	
    /**
     * Constructor
     * @param reader The XML Node containing the specification for the range of values
     * to select.
     */
	public FeatureRangeSpecification(XMLStreamReader reader) {
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
	
	/**
	 * @return The first index to use for the x dimension for the range
	 */
	public String xMinimum()
	{
		return xMinimum;
	}
	
	/**
	 * @return The last index to use for the x dimension for the range
	 */
	public String xMaximum()
	{
		return xMaximum;
	}
	
	/**
	 * @return The first index to use for the y dimension for the range
	 */
	public String yMinimum()
	{
		return yMinimum;
	}
	
	/**
	 * @return The last index to use for the y dimension for the range
	 */
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

	private String xMinimum;
	private String xMaximum;
	private String yMinimum;
	private String yMaximum;

    @Override
    public List<Integer> getVariablePositionIDs(Integer variableID) throws Exception
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
        return FeatureSpecification.RANGE;
    }
}
