/**
 * 
 */
package config.data;

import java.util.ArrayList;
import java.util.TreeMap;

import javax.xml.stream.XMLStreamReader;
import collections.Pair;

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
	public String get_condition(TreeMap<String, String> aliases) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected void interpret(XMLStreamReader reader) {
		// TODO Auto-generated method stub
		
	}
	
	public ArrayList<Pair<String, String>> get_points()
	{
		return this.points;
	}
	
	public Pair<String, String> get_point(int index)
	{
		Pair<String, String> point = null;
		
		if (points != null && points.size() > index)
		{
			point = points.get(index);
		}
		
		return point;
	}

	@Override
	protected String tag_name() {
		// TODO Auto-generated method stub
		return null;
	}
	
	private ArrayList<Pair<String, String>> points = null;
}
