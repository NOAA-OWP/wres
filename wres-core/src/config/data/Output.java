/**
 * 
 */
package config.data;

import java.util.ArrayList;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

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
		
	}	

	@Override
	protected String tag_name() {
		// TODO Auto-generated method stub
		return null;
	}
	
	private final ArrayList<OutputType> graphics = new ArrayList<OutputType>();
	private final ArrayList<OutputType> numerics = new ArrayList<OutputType>();
}
