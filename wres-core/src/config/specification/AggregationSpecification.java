/**
 * 
 */
package config.specification;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import util.Utilities;

/**
 * Specification for how to aggregate values in a metric
 * @author Christopher Tubbs
 */
public class AggregationSpecification extends SpecificationElement {
	
	public AggregationSpecification(XMLStreamReader reader)
	{
	    super(reader);
	}

	/**
	 * @return Whether or not to perform an aggregation
	 */
	public Boolean performAggregation()
	{
		return this.performAggregation;
	}
	
	/**
	 * @return The range of aggregation
	 */
	public Byte aggregationRange()
	{
		return this.aggregationRange;
	}
	
	/**
	 * @return The unit of the range for the aggregation
	 */
	public String aggregationUnit()
	{
		return this.aggregationUnit;
	}
	
	/**
	 * @return The method to use for aggregation
	 */
	public String aggregationMode()
	{
		return this.aggregationMode;
	}
	
	public Short getLastLead()
	{
	    if (this.lastLead == null)
	    {
	        this.lastLead = Short.MAX_VALUE;
	    }
	    return this.lastLead;
	}
	
	public Short getFirstLead()
	{
	    if (this.firstLead == null)
	    {
	        this.firstLead = 0;
	    }
	    return this.firstLead;
	}
	
	public String getAggregationUnit()
	{
	    if (this.aggregationUnit.isEmpty())
	    {
	        this.aggregationUnit = null;
	    }
	    
	    return this.aggregationUnit;
	}
	
	public boolean leadIsValid(int step, int lastLead)
	{
	    Double currentLead = getLead(step);
	    return currentLead != null && 
	           currentLead <= lastLead && 
	           currentLead <= this.getLastLead() &&
	           currentLead >= this.getFirstLead();
	}
	
	private Double getLead(int step) {
	    Double lead = null;
        
        if (getAggregationUnit() != null)
        {
            lead = Utilities.HOUR_CONVERSION.get(this.aggregationUnit) * step;
        }
	    
	    return lead;
	}
	
	public String getLeadQualifier(int step)
	{
	    String lead = null;
	    
	    if (getAggregationUnit() != null && aggregationRange() > 0)
	    {
	        if (this.aggregationRange == 1)
	        {
	            lead = "lead = " + getLead(step).intValue();
	        }
	        else
	        {
	            Double range = aggregationRange() * Utilities.HOUR_CONVERSION.get(this.aggregationUnit);
	            lead = String.valueOf((int)(step * range)) + " > lead && lead >= " + String.valueOf((int)(step * (range - 1)));
	        }
	    }
	    
	    return lead;
	}
	
	@Override
	public String toString() {
		String description = "Aggregation: ";
		description += System.lineSeparator();
		
		description += "\tAggregate Data: ";
		description += String.valueOf(performAggregation());
		description += System.lineSeparator();
		
		description += "\tRange: ";
		description += String.valueOf(aggregationMode());
		description += " ";
		description += String.valueOf(aggregationUnit());
		description += System.lineSeparator();
		
		description += "\tAggregation Mode: ";
		description += String.valueOf(aggregationMode());
		description += System.lineSeparator();
		
		
		return description;
	}
	
	private void setPerformAggregation(Boolean performAggregation)
	{
	    this.performAggregation = performAggregation;
	}
	
	private void setPerformAggregation(String performAggregation)
	{
	    setPerformAggregation(Utilities.isTrue(performAggregation));
	}
	
	private void setAggregationRange(Byte aggregationRange)
	{
	    this.aggregationRange = aggregationRange;
	}
	
	private void setAggregationRange(String aggregationRange)
	{
	    byte range = 1;
	    
	    if (Utilities.isNumeric(aggregationRange))
	    {
	        range = Byte.parseByte(aggregationRange);
	    }
	    
	    setAggregationRange(range);
	}
	
	private void setAggregationUnit(String aggregationUnit)
	{
	    aggregationUnit = aggregationUnit.toLowerCase();
	    if (!Arrays.asList("hour", "minute", "hour", "day", "week", "year").contains(aggregationUnit))
	    {
	        aggregationUnit = "hour";
	    }
	    this.aggregationUnit = aggregationUnit;
	}
	
	private void setFirstLead(String firstLead)
	{
	    short lead = 0;
	    
	    if (Utilities.isNumeric(firstLead))
	    {
	        lead = Short.parseShort(firstLead);
	    }
	        
	    setFirstLead(lead);   
	}
	
	private void setFirstLead(short firstLead)
	{
	    this.firstLead = firstLead;
	}
	
	private void setLastLead(String lastLead)
	{
	    Short lead = null;
	    
	    if (Utilities.isNumeric(lastLead))
	    {
	        lead = Short.parseShort(lastLead);
	    }
	    
	    setLastLead(lead);
	}
	
	private void setLastLead(Short lastLead)
	{
	    this.lastLead = lastLead;
	}
	
	private void setAggregationMode(String mode)
	{
	    this.aggregationMode = mode;
	}
	
	private Boolean performAggregation;
	private Byte aggregationRange;
	private String aggregationUnit;
	private String aggregationMode;
	private Short firstLead;
	private Short lastLead;
	
    @Override
    protected void interpret(XMLStreamReader reader) throws XMLStreamException, IOException {}

    @Override
    protected List<String> tagNames()
    {
        return Arrays.asList("aggregation");
    }
    
    @Override
    protected void getAttributes(XMLStreamReader reader)
    {
        for (int attribute_index = 0; attribute_index < reader.getAttributeCount(); ++attribute_index)
        {
            String attribute_name = reader.getAttributeLocalName(attribute_index);
            
            if (attribute_name.equalsIgnoreCase("range"))
            {
                this.setAggregationRange(reader.getAttributeValue(attribute_index));
            }
            else if (attribute_name.equalsIgnoreCase("aggregate"))
            {
                this.setPerformAggregation(reader.getAttributeValue(attribute_index));
            }
            else if (attribute_name.equalsIgnoreCase("unit"))
            {
                this.setAggregationUnit(reader.getAttributeValue(attribute_index));
            }
            else if (attribute_name.equalsIgnoreCase("mode"))
            {
                this.setAggregationMode(reader.getAttributeValue(attribute_index));
            }
            else if (attribute_name.equalsIgnoreCase("first_lead"))
            {
                this.setFirstLead(reader.getAttributeValue(attribute_index));
            }
            else if (attribute_name.equalsIgnoreCase("last_lead"))
            {
                this.setLastLead(reader.getAttributeValue(attribute_index));
            }
        }
    }

    @Override
    public String toXML()
    {
        // TODO Auto-generated method stub
        return null;
    }
}
