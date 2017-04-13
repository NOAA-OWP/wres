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
public class Project extends ConfigElement {

	/**
	 * @param reader
	 * @throws Exception 
	 */
	public Project(XMLStreamReader reader) throws Exception 
	{
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
	protected String tag_name() {
		// TODO Auto-generated method stub
		return null;
	}
	
	private void validate() throws Exception
	{
		if (metric_count() == 0)
		{
			String message = "The project with the name '";
			
			if (this.name == null)
			{
				this.name = "[unnamed project]";
			}
			
			message = "The project with the name '" + this.name;
			message += "' is invalid because there are not metrics configured to execute.";
			message += " Metrics must be added for this project to be valid.";
			throw new Exception(message);
		}
	}

	public Metric get_metrics(int index)
	{
		return metrics.get(index);
	}
	
	public int metric_count()
	{
		return metrics.size();
	}

	private ArrayList<Metric> metrics = new ArrayList<Metric>();
	private ProjectDataSource observations = null;
	private ProjectDataSource forecasts = null;
	private String name;
}
