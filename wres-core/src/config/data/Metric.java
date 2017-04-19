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
public class Metric extends ClauseConfig {

	/**
	 * @param reader
	 * @throws Exception 
	 */
	public Metric(XMLStreamReader reader) throws Exception {
		super(reader);
		//validate();
	}

	/* (non-Javadoc)
	 * @see config.data.ConfigElement#interpret(javax.xml.stream.XMLStreamReader)
	 */
	@Override
	protected void interpret(XMLStreamReader reader) throws XMLStreamException 
	{
		String tag_name = reader.getLocalName();
		
		if (tag_name.equalsIgnoreCase("name"))
		{
			this.name = tagValue(reader);
		}
		else if (tag_name.equalsIgnoreCase("output"))
		{
			this.metric_output = new Output(reader);
		}
		else if (tag_name.equalsIgnoreCase("observations"))
		{
			this.observations = new ProjectDataSource(reader);
		}
		else if (tag_name.equalsIgnoreCase("forecasts"))
		{
			this.forecasts = new ProjectDataSource(reader);
		}
	}

	@Override
	public String getCondition(TreeMap<String, String> aliases) 
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected List<String> tagNames() {
		return Arrays.asList("metric");
	}
	
	private void validate() throws Exception
	{
		if (metric_output == null || forecasts == null || observations == null)
		{
			String message = "The project with the name '";
			if (name == null)
			{
				message += "[unnamed project]";
			}
			else
			{
				message += name;
			}
			
			message += "' has not been properly configured and cannot be completed. Please reconfigure.";
			throw new Exception(message);
		}
	}
	
	@Override
	public String toString()
	{
		String description = "Metric: ";
		description += name;
		description += System.lineSeparator();
		description += System.lineSeparator();
		
		description += "-  -  -  -  -  -  -  -  -  -";
		description += System.lineSeparator();
		description += System.lineSeparator();
		
		description += metric_output.toString();
		
		description += "-  -  -  -  -  -  -  -  -  -";
		description += System.lineSeparator();
		description += System.lineSeparator();
		
		description += "Observations: ";
		description += System.lineSeparator();
		description += System.lineSeparator();
		description += observations.toString();
		description += System.lineSeparator();
		
		description += "-  -  -  -  -  -  -  -  -  -";
		description += System.lineSeparator();
		description += System.lineSeparator();
		
		description += "Forecasts: ";
		description += System.lineSeparator();
		description += System.lineSeparator();
		description += forecasts.toString();
		description += System.lineSeparator();
		
		return description;
	}

	private String name;
	private Output metric_output;
	private ProjectDataSource observations;
	private ProjectDataSource forecasts;
}
