/**
 * 
 */
package wres.io.config.specification;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import wres.util.Strings;
import wres.util.XML;

/**
 * Specification for the type of output required for a metric
 * @author Christopher Tubbs
 */
final class OutputSpecification extends SpecificationElement {
    /**
     * Constructor
     * @param reader The XML Node detailing the specifications for the output
     */
	public OutputSpecification(XMLStreamReader reader)
	{
		super(reader);
	}

	@Override
	protected void interpret(XMLStreamReader reader) throws XMLStreamException 
	{
		OutputTypeSpecification specs = generateOutputType(reader);
		
		if (XML.tagIs(reader, "graphic"))
		{
			addGraphicOutput(specs);
		}
		else if (XML.tagIs(reader, "numeric"))
		{
			addNumericOutput(specs);
		}
	}	
	
	/**
	 * Strips basic information off the current XML node to construct details about specific types of outputs
	 * @param reader The XML Node detailing a requested type of output
	 * @return Specifications about the type of output
	 */
	private static OutputTypeSpecification generateOutputType(XMLStreamReader reader)
	{
		String output_attr = XML.getAttributeValue(reader, "output");
		String path = XML.getAttributeValue(reader, "path");
		String format = XML.getAttributeValue(reader, "format");
		
		boolean shouldSave = false;
		if (output_attr != null)
		{
			shouldSave = Strings.POSSIBLE_TRUE_VALUES.contains(output_attr);
		}
		
		return new OutputTypeSpecification(shouldSave, path, format);
	}

	@Override
	protected List<String> tagNames() {
		return Collections.singletonList("output");
	}
	
	@Override
	public String toString() {
		StringBuilder description = new StringBuilder("Output:");
		description.append(System.lineSeparator());
		description.append(System.lineSeparator());
		
		description.append("Graphical Output:");
		description.append(System.lineSeparator());
		if (graphics.size() > 0)
		{
			for (OutputTypeSpecification graphic : graphics)
			{
				description.append(graphic.toString());
			}
		}
		else
		{
			description.append("\tNONE");
		}
		description.append(System.lineSeparator());
		description.append(System.lineSeparator());
		
		description.append("Numerical Output:");
		description.append(System.lineSeparator());
		if (numerics.size() > 0)
		{
			for (OutputTypeSpecification numeric : numerics)
			{
				description.append(numeric.toString());
			}
		}
		else
		{
			description.append("\tNONE");
		}
		description.append(System.lineSeparator());
		description.append(System.lineSeparator());
		
		return description.toString();
	}
	
	/**
	 * Adds output specifications for output that should be saved as graphics
	 * @param graphicalType The specifications for the graphical output
	 */
	private void addGraphicOutput(OutputTypeSpecification graphicalType)
	{
		if (graphics == null)
		{
			graphics = new ArrayList<>();
		}
		
		graphics.add(graphicalType);
	}
	
	/**
	 * Adds output specifications for output that should be save as numerical logs
	 * @param numericalType The specifications for the numerical output
	 */
	private void addNumericOutput(OutputTypeSpecification numericalType)
	{
		if (numerics == null)
		{
			numerics = new ArrayList<>();
		}
		numerics.add(numericalType);
	}
	
	private ArrayList<OutputTypeSpecification> graphics;
	private ArrayList<OutputTypeSpecification> numerics;
    @Override
    public String toXML()
    {
        // TODO Auto-generated method stub
        return null;
    }
}
