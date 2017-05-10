/**
 * 
 */
package config.data;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import util.Utilities;

/**
 * Specification for the type of output required for a metric
 * @author Christopher Tubbs
 */
public final class Output extends ConfigElement {
    /**
     * Constructor
     * @param reader The XML Node detailing the specifications for the output
     */
	public Output(XMLStreamReader reader)
	{
		super(reader);
	}

	@Override
	protected void interpret(XMLStreamReader reader) throws XMLStreamException 
	{
		OutputType specs = generateOutputType(reader);
		
		if (Utilities.tagIs(reader, "graphic"))
		{
			addGraphicOutput(specs);
		}
		else if (Utilities.tagIs(reader, "numeric"))
		{
			addNumericOutput(specs);
		}
	}	
	
	/**
	 * Strips basic information off the current XML node to construct details about specific types of outputs
	 * @param reader The XML Node detailing a requested type of output
	 * @return Specifications about the type of output
	 */
	private static OutputType generateOutputType(XMLStreamReader reader)
	{
		String output_attr = Utilities.getAttributeValue(reader, "output");
		String path = Utilities.getAttributeValue(reader, "path");
		String format = Utilities.getAttributeValue(reader, "format");
		
		boolean shouldSave = false;
		if (output_attr != null)
		{
			shouldSave = Utilities.POSSIBLE_TRUE_VALUES.contains(output_attr);
		}
		
		return new OutputType(shouldSave, path, format);
	}

	@Override
	protected List<String> tagNames() {
		return Arrays.asList("output");
	}
	
	@Override
	public String toString() {
		String description = "Output:";
		description += System.lineSeparator();
		description += System.lineSeparator();
		
		description += "Graphical Output:";
		description += System.lineSeparator();
		if (graphics.size() > 0)
		{
			for (OutputType graphic : graphics)
			{
				description += graphic.toString();
			}
		}
		else
		{
			description += "\tNONE";
		}
		description += System.lineSeparator();
		description += System.lineSeparator();
		
		description += "Numerical Output:";
		description += System.lineSeparator();
		if (numerics.size() > 0)
		{
			for (OutputType numeric : numerics)
			{
				description += numeric.toString();
			}
		}
		else
		{
			description += "\tNONE";
		}
		description += System.lineSeparator();
		description += System.lineSeparator();
		
		return description;
	}
	
	/**
	 * Adds output specifications for output that should be saved as graphics
	 * @param graphicalType The specifications for the graphical output
	 */
	private void addGraphicOutput(OutputType graphicalType)
	{
		if (graphics == null)
		{
			graphics = new ArrayList<OutputType>();
		}
		
		graphics.add(graphicalType);
	}
	
	/**
	 * Adds output specifications for output that should be save as numerical logs
	 * @param numericalType The specifications for the numerical output
	 */
	private void addNumericOutput(OutputType numericalType)
	{
		if (numerics == null)
		{
			numerics = new ArrayList<OutputType>();
		}
		numerics.add(numericalType);
	}
	
	private ArrayList<OutputType> graphics;
	private ArrayList<OutputType> numerics;
    @Override
    public String toXML()
    {
        // TODO Auto-generated method stub
        return null;
    }
}
