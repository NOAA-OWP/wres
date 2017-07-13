/**
 * 
 */
package wres.io.config.specification;

import wres.io.data.caching.Features;
import wres.util.XML;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Details a location to query based on information in the configuration
 * @author Christopher Tubbs
 */
public final class LocationSpecification extends FeatureSpecification {
    /**
     * Constructor
     * @param reader The XML Reader containing the specification for the Location
     */
	public LocationSpecification(XMLStreamReader reader)
	{
		super(reader);
	}
	
	@Override
	protected void interpret(XMLStreamReader reader) throws XMLStreamException {
		String tag_name = reader.getLocalName();
		
		if (tag_name.equalsIgnoreCase("lid"))
		{
			this.lid = XML.getXMLText(reader);
		}
		else if (tag_name.equalsIgnoreCase("comid"))
		{
			this.comid = XML.getXMLText(reader);
		}
		else if (tag_name.equalsIgnoreCase("gage_id"))
		{
			this.gage_id = XML.getXMLText(reader);
		}
		else if (tag_name.equalsIgnoreCase("huc"))
		{
			this.huc = XML.getXMLText(reader);
		}
		else if (tag_name.equalsIgnoreCase("name"))
		{
			this.name = XML.getXMLText(reader);
		}
	}

	@Override
	protected List<String> tagNames() {
		return Collections.singletonList("feature");
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
        return Arrays.asList(Features.getVariablePositionID(lid, name, variableID));
    }

	@Override
	public Integer getFirstVariablePositionID (final Integer variableID) throws Exception {
		return Features.getVariablePositionID(lid, name, variableID);
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
        return FeatureSpecification.LOCATION;
    }
}
