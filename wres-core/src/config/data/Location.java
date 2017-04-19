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
public final class Location extends ClauseConfig {
	public Location(XMLStreamReader reader)
	{
		super(reader);
	}
	
	@Override
	protected void interpret(XMLStreamReader reader) throws XMLStreamException {
		String tag_name = reader.getLocalName();
		
		if (tag_name.equalsIgnoreCase("lid"))
		{
			this.lid = tagValue(reader);
		}
		else if (tag_name.equalsIgnoreCase("comid"))
		{
			this.comid = tagValue(reader);
		}
		else if (tag_name.equalsIgnoreCase("gage_id"))
		{
			this.gage_id = tagValue(reader);
		}
		else if (tag_name.equalsIgnoreCase("huc"))
		{
			this.huc = tagValue(reader);
		}
		else if (tag_name.equalsIgnoreCase("name"))
		{
			this.name = tagValue(reader);
		}
	}

	@Override
	protected List<String> tagNames() {
		return Arrays.asList("feature");
	}

	@Override
	public String getCondition(TreeMap<String, String> aliases) {
		// TODO Auto-generated method stub
		return null;
	}
	
	public String lid()
	{
		return lid;
	}
	
	public String comid()
	{
		return comid;
	}
	
	public String gage_id()
	{
		return gage_id;
	}
	
	public String huc()
	{
		return huc;
	}
	
	public String name()
	{
		return name;
	}
	
	@Override
	public String toString() {
		String description = "Location: ";
		description += System.lineSeparator();
		
		if (name != null)
		{
			description += "\tname: ";
			description += name;
			description += System.lineSeparator();
		}
		
		if (lid != null) {
			description += "\tlid: ";
			description += lid;
			description += System.lineSeparator();
		}
		
		if (comid != null) {
			description += "\tcomid: ";
			description += String.valueOf(comid);
			description += System.lineSeparator();
		}
		
		if (gage_id != null) {
			description += "\tgage id: ";
			description += String.valueOf(gage_id);
			description += System.lineSeparator();
		}
		
		if (huc != null)
		{
			description += "\thuc: ";
			description += huc;
		}
		
		return description;
	}
	
	private String lid;
	private String comid;
	private String gage_id;
	private String huc;
	private String name;
}
