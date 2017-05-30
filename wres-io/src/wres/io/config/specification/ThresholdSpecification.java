/**
 * 
 */
package wres.io.config.specification;

import javax.xml.stream.XMLStreamReader;

import wres.util.Strings;

/**
 * @author Christopher Tubbs
 *
 */
public class ThresholdSpecification
{
    public ThresholdSpecification() {}

    public ThresholdSpecification(String mode, String value)
    {
        this.setMode(mode);
        this.setValue(value);
    }
    
    public ThresholdSpecification(XMLStreamReader reader)
    {
        this.value = 0.0F;
        
        for (int attributeIndex = 0; attributeIndex < reader.getAttributeCount(); ++attributeIndex)
        {
            String attributeName = reader.getAttributeLocalName(attributeIndex);
            if (attributeName.equalsIgnoreCase("mode"))
            {
                this.setMode(reader.getAttributeValue(attributeIndex));
            }
            else if (attributeName.equalsIgnoreCase("value"))
            {
                this.setValue(reader.getAttributeValue(attributeIndex));
            }
        }
    }
    
    private void setMode(String mode)
    {
        this.mode = ThresholdMode.from(mode);
    }
    
    private void setValue(String value)
    {
        if (!Strings.isNumeric(value))
        {
            value = "0.0";
        }
        this.value = Float.parseFloat(value);
    }
    
    @Override
    public String toString() {
        String description = "\tThreshold\t\tValue:";
        description += this.value;
        description += ", Mode: ";
        description += this.mode;
        description += System.lineSeparator();
        
        return description;
    }
    
    public ThresholdMode getMode()
    {
        return this.mode;
    }
    
    public float getValue()
    {
        return this.value;
    }
    
    private ThresholdMode mode;
    private float value;

    public enum ThresholdMode
    {
        ABSOLUTE,
        DIFFERENCE,
        PERCENT_DIFFERENCE;
        
        public static ThresholdMode from(String modeDescription)
        {
            ThresholdMode mode = ABSOLUTE;
            
            if (modeDescription.equalsIgnoreCase("difference") || modeDescription.equalsIgnoreCase("diff"))
            {
                mode = DIFFERENCE;
            }
            else if (modeDescription.equalsIgnoreCase("%") || 
                     modeDescription.equalsIgnoreCase("percent") || 
                     modeDescription.equalsIgnoreCase("%diff") ||
                     modeDescription.equalsIgnoreCase("diff%") ||
                     modeDescription.equalsIgnoreCase("percent difference") ||
                     modeDescription.equalsIgnoreCase("difference percent") ||
                     modeDescription.equalsIgnoreCase("diff percent") ||
                     modeDescription.equalsIgnoreCase("percent diff") ||
                     modeDescription.equalsIgnoreCase("percent_difference") ||
                     modeDescription.equalsIgnoreCase("difference_percent") ||
                     modeDescription.equalsIgnoreCase("percentage"))
            {
                mode = PERCENT_DIFFERENCE;
            }
            
            return mode;
        }
    }
}
