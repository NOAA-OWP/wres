/**
 * 
 */
package config.data;

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
		validate();
	}

	/* (non-Javadoc)
	 * @see config.data.ConfigElement#interpret(javax.xml.stream.XMLStreamReader)
	 */
	@Override
	protected void interpret(XMLStreamReader reader) throws XMLStreamException {
		// TODO Auto-generated method stub

	}

	@Override
	public String get_condition(TreeMap<String, String> aliases) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected String tag_name() {
		// TODO Auto-generated method stub
		return null;
	}
	
	private void validate() throws Exception
	{
		if (metric_output == null || forecast_clause == null || observation_clause == null)
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

	private String name = null;
	private Output metric_output = null;
	private MetricData observation_clause = null;
	private MetricData forecast_clause = null;
}
