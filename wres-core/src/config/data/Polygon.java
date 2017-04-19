/**
 * 
 */
package config.data;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;

import javax.xml.stream.XMLStreamReader;

/**
 * @author ctubbs
 *
 */
public final class Polygon extends ClauseConfig {
	public Polygon(XMLStreamReader reader)
	{
		super(reader);
	}

	@Override
	public String getCondition(TreeMap<String, String> aliases) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected void interpret(XMLStreamReader reader) 
	{
		if (reader.getLocalName().equalsIgnoreCase("point"))
		{
			addPoint(new Point(reader));
		}
	}

	@Override
	protected List<String> tagNames() {
		return Arrays.asList("polygon");
	}
	
	private void addPoint(Point point)
	{
		if (point == null)
		{
			return;
		}
		
		if (this.points == null)
		{
			this.points = new ArrayList<Point>();
		}
		
		this.points.add(point);
	}
	
	@Override
	public String toString() 
	{
		String description = "Polygon:";
		description += System.lineSeparator();
		
		for (Point point : points)
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
	
	private ArrayList<Point> points;
}
