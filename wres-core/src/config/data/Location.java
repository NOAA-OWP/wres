/**
 * 
 */
package config.data;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import data.FeatureCache;
import util.Utilities;

/**
 * Details a location to query based on information in the configuration
 * @author Christopher Tubbs
 */
public final class Location extends FeatureSelector {
    /**
     * Constructor
     * @param reader The XML Reader containing the specification for the Location
     */
	public Location(XMLStreamReader reader)
	{
		super(reader);
	}
	
	@Override
	protected void interpret(XMLStreamReader reader) throws XMLStreamException {
		String tag_name = reader.getLocalName();
		
		if (tag_name.equalsIgnoreCase("lid"))
		{
			this.lid = Utilities.getXMLText(reader);
		}
		else if (tag_name.equalsIgnoreCase("comid"))
		{
			this.comid = Utilities.getXMLText(reader);
		}
		else if (tag_name.equalsIgnoreCase("gage_id"))
		{
			this.gage_id = Utilities.getXMLText(reader);
		}
		else if (tag_name.equalsIgnoreCase("huc"))
		{
			this.huc = Utilities.getXMLText(reader);
		}
		else if (tag_name.equalsIgnoreCase("name"))
		{
			this.name = Utilities.getXMLText(reader);
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
	
	/**
	 * @return The location id of the indicated feature
	 */
	public String lid()
	{
		return lid;
	}
	
	/**
	 * @return The comid of the indicated feature
	 */
	public String comid()
	{
		return comid;
	}
	
	/**
	 * @return The id of the gage for the indicated feature
	 */
	public String gageID()
	{
		return gage_id;
	}
	
	/**
	 * @return The HUC code for the indicated feature
	 */
	public String huc()
	{
		return huc;
	}
	
	/**
	 * @return The human friendly name for the feature
	 */
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
	
    @Override
    public List<Integer> getVariablePositionIDs(Integer variableID) throws Exception
    {
        List<Integer> id = new ArrayList<Integer>(1);
        id.add(FeatureCache.getVariablePositionID(lid, name, variableID));
        return id;
    }
}
