/**
 * 
 */
package config.data;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import util.Utilities;

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
	}

	/* (non-Javadoc)
	 * @see config.data.ConfigElement#interpret(javax.xml.stream.XMLStreamReader)
	 */
	@Override
	protected void interpret(XMLStreamReader reader) throws Exception {
		if (tagIs(reader, "name"))
		{
			this.name = tagValue(reader);
		}
		else if (tagIs(reader, "observations"))
		{
			this.observations = new ProjectDataSource(reader);
		}
		else if (tagIs(reader, "forecasts"))
		{
			this.forecasts = new ProjectDataSource(reader);
		}
		else if (tagIs(reader, "metrics"))
		{
			parseMetrics(reader);
		}
	}

	@Override
	protected List<String> tagNames() {
		return Arrays.asList("project");
	}
	
	private void parseMetrics(XMLStreamReader reader) throws Exception
	{
		while (reader.hasNext())
		{
			reader.next();
			
			if (Utilities.xmlTagClosed(reader, tagNames()))
			{
				break;
			}

			if (tagIs(reader, "metric"))
			{
				addMetric(new Metric(reader));
			}
		}
	}
	
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

	public Metric getMetrics(int index)
	{
		Metric metric = null;
		
		if (index < metricCount())
		{
			metric = metrics.get(index);
		}
		
		return metric;
	}
	
	public int metricCount()
	{
		if (metrics == null)
		{
			metrics = new ArrayList<Metric>();
		}
		
		return metrics.size();
	}
	
	public ProjectDataSource getObservations()
	{
		return observations;
	}
	
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
