/**
 * 
 */
package config.specification;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.xml.stream.XMLStreamReader;

/**
 * Specifications for selecting features based on a polygon drawn around gridded indices
 *
 * @author Christopher Tubbs
 */
public final class PolygonSpecification extends FeatureSpecification {
    /**
     * Constructor
     * @param reader The XML Reader containing details about the polygon to draw
     */
	public PolygonSpecification(XMLStreamReader reader)
	{
		super(reader);
	}

	@Override
	protected void interpret(XMLStreamReader reader) 
	{
		if (reader.getLocalName().equalsIgnoreCase("point"))
		{
			addPoint(new PointSpecification(reader));
		}
	}

	@Override
	protected List<String> tagNames() {
		return Arrays.asList("polygon");
	}
	
	/**
	 * Adds a point to the polygon
	 * @param point Point that will act as another vertex for the polygon
	 */
	private void addPoint(PointSpecification point)
	{
		if (point == null)
		{
			return;
		}
		
		if (this.points == null)
		{
			this.points = new ArrayList<PointSpecification>();
		}
		
		this.points.add(point);
	}
	
	@Override
	public String toString() 
	{
		String description = "Polygon:";
		description += System.lineSeparator();
		
		for (PointSpecification point : points)
		{
			description += "\tVertex: (";
			description += point.x();
			description += ", ";
			description += point.y();
			description += ")";
			description += System.lineSeparator();
		}
		
		return description;
	}
	
	private ArrayList<PointSpecification> points;

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
        return FeatureSpecification.POLYGON;
    }
}
