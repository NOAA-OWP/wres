/**
 * 
 */
package config.data;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.xml.stream.XMLStreamReader;

import util.Utilities;

/**
 * Specification for what metrics to run on what data
 * 
 * @author Christopher Tubbs
 */
public class Project extends ConfigElement {

	/**
	 * Constructor
	 * @param reader The XML reader containing the details about the project specification
	 * @throws Exception An error is thrown if there is trouble reading the detabase
	 */
	public Project(XMLStreamReader reader) throws Exception 
	{
		super(reader);
	}

	/* (non-Javadoc)
	 * @see config.data.ConfigElement#interpret(javax.xml.stream.XMLStreamReader)
	 */
	@Override
	protected void interpret(XMLStreamReader reader) throws Exception {
		if (Utilities.tagIs(reader, "name"))
		{
			this.name = Utilities.getXMLText(reader);
		}
		else if (Utilities.tagIs(reader, "observations"))
		{
			this.observations = new ProjectDataSource(reader);
		}
		else if (Utilities.tagIs(reader, "forecasts"))
		{
			this.forecasts = new ProjectDataSource(reader);
		}
		else if (Utilities.tagIs(reader, "metrics"))
		{
			parseMetrics(reader);
		}
	}

	@Override
	protected List<String> tagNames() {
		return Arrays.asList("project");
	}
	
	/**
	 * Pieces together information about metrics that are found within the specification
	 * @param reader The XML Node containing information about metrics
	 * @throws Exception An error is thrown if the XML could not be read correctly
	 */
	private void parseMetrics(XMLStreamReader reader) throws Exception
	{
		while (reader.hasNext())
		{
			reader.next();
			
			if (Utilities.xmlTagClosed(reader, tagNames()))
			{
				break;
			}

			if (Utilities.tagIs(reader, "metric"))
			{
				addMetric(new Metric(reader));
			}
		}
	}
	
	/**
	 * Adds a created metric to the metric collection
	 * @param metric The metric specification to add
	 */
	public void addMetric(Metric metric)
	{
		if (metric == null)
		{
			return;
		}
		
		if (metrics == null)
		{
			metrics = new ArrayList<Metric>();
		}
		
		metrics.add(metric);
	}

	/**
	 * Retrieves a specific metric
	 * @param index The index of the metric to retrieve
	 * @return A metric specification. Null is returned if the index was not valid
	 */
	public Metric getMetric(int index)
	{
		Metric metric = null;
		
		if (index < metricCount())
		{
			metric = metrics.get(index);
		}
		
		return metric;
	}
	
	/**
	 * @return The number of metrics to be run on the project
	 */
	public int metricCount()
	{
		if (metrics == null)
		{
			metrics = new ArrayList<Metric>();
		}
		
		return metrics.size();
	}
	
	/**
	 * Details about the observation data that needs to be present to execute a project
	 * @return Information about the data required for the project's observations
	 */
	public ProjectDataSource getObservations()
	{
		return observations;
	}
	
	/**
	 * Details about the forecast data that needs to be present to execute a project
	 * @return Information about the data required for the project's forecasts
	 */
	public ProjectDataSource getForecasts()
	{
		return forecasts;
	}
	
	@Override
	public String toString() {
		String description = "-----------------------------------";
		description += System.lineSeparator();
		description += System.lineSeparator();
		description += "Project: ";
		
		if (name == null)
		{
			description += "[Unnamed Project]";
		}
		else
		{
			description += name;
		}
		description += System.lineSeparator();
		description += System.lineSeparator();
		
		description += "-----------------------------------";
		description += System.lineSeparator();
		description += System.lineSeparator();
		
		description += "Observations:";
		description += System.lineSeparator();
		description += System.lineSeparator();
		description += observations.toString();
		description += System.lineSeparator();
		
		description += "-----------------------------------";
		description += System.lineSeparator();
		description += System.lineSeparator();
		
		description += "Forecasts:";
		description += System.lineSeparator();
		description += System.lineSeparator();
		description += forecasts.toString();
		description += System.lineSeparator();
		
		description += "-----------------------------------";
		description += System.lineSeparator();
		description += System.lineSeparator();
		
		description += "Metrics:";
		description += System.lineSeparator();
		description += System.lineSeparator();
		description += "\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/";
		description += System.lineSeparator();
		description += System.lineSeparator();
		if (metricCount() > 0)
		{
			for (Metric metric : metrics)
			{
				description += metric.toString();
				description += System.lineSeparator();
				description += "*  *  *  *  *  *  *  *  *  *  *  *  *";
				description += System.lineSeparator();
			}
		}
		else {
			description += "[NONE]";
			description += System.lineSeparator();
		}
		description += "\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/";
		
		return description;
	}

	private ArrayList<Metric> metrics;
	private ProjectDataSource observations;
	private ProjectDataSource forecasts;
	private String name;
}
