/**
 * 
 */
package config.data;

import java.util.TreeMap;

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
	protected void interpret(XMLStreamReader reader) {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected String tag_name() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String get_condition(TreeMap<String, String> aliases) {
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
	
	private String lid = null;
	private String comid = null;
	private String gage_id = null;
	private String huc = null;
	private String name = null;
}
