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
 * @author ctubbs
 *
 */
public final class Output extends ConfigElement {
	public Output(XMLStreamReader reader)
	{
		super(reader);
	}

	@Override
	protected void interpret(XMLStreamReader reader) throws XMLStreamException 
	{
		OutputType specs = generateOutputType(reader);
		
		if (tagIs(reader, "graphic"))
		{
			addGraphicOutput(specs);
		}
		else if (tagIs(reader, "numeric"))
		{
			addNumericOutput(specs);
		}
	}	
	
	private OutputType generateOutputType(XMLStreamReader reader)
	{
		String output_attr = Utilities.get_attribute_value(reader, "output");
		String path = Utilities.get_attribute_value(reader, "path");
		String format = Utilities.get_attribute_value(reader, "format");
		
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
	
	private void addGraphicOutput(OutputType graphicalType)
	{
		if (graphics == null)
		{
			graphics = new ArrayList<OutputType>();
		}
		
		graphics.add(graphicalType);
	}
	
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
}
