/**
 * 
 */
package config.specification;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import util.Utilities;

/**
 * Specification for what metrics to run on what data
 * 
 * @author Christopher Tubbs
 */
public class ProjectSpecification extends SpecificationElement
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ProjectSpecification.class);

    /**
     * Constructor
     * @param reader The XML reader containing the details about the project specification
     * @throws IOException
     * @throws XMLStreamException An error is thrown if there is trouble reading the file
     */
    public ProjectSpecification(XMLStreamReader reader) throws IOException, XMLStreamException
    {
        super(reader);
    }

    /* (non-Javadoc)
     * @see config.data.ConfigElement#interpret(javax.xml.stream.XMLStreamReader)
     */
    @Override
    protected void interpret(XMLStreamReader reader) throws IOException, XMLStreamException
    {
        if (reader == null)
        {
            LOGGER.trace("interpret - reader was null");
        }
        else
        {
            LOGGER.trace("interpret - reader passed: {}", reader);
        }
        
		if (Utilities.tagIs(reader, "name"))
		{
			this.name = Utilities.getXMLText(reader);
		}
		else if (Utilities.tagIs(reader, "observations") || 
		         Utilities.tagIs(reader, "forecasts") || 
		         Utilities.tagIs(reader, "dataSource"))
		{
		    addDatasource(reader);
		}
		else if (Utilities.tagIs(reader, "metrics"))
		{
			parseMetrics(reader);
		}
	}
	
	private void addDatasource(XMLStreamReader reader)
	{
        if (this.dataSources == null) {
            this.dataSources = new ArrayList<>();
        }
        this.dataSources.add(new ProjectDataSpecification(reader));
	}

    @Override
    protected List<String> tagNames()
    {
        return Arrays.asList("project");
    }

    /**
     * Pieces together information about metrics that are found within the specification
     * @param reader The XML Node containing information about metrics
     * @throws Exception An error is thrown if the XML could not be read correctly
     */
    private void parseMetrics(XMLStreamReader reader) throws IOException, XMLStreamException
    {
        if (reader == null)
        {
            LOGGER.trace("parseMetrics - reader was null");
        }
        else
        {
            LOGGER.trace("parseMetrics - reader passed: {}", reader);
        }

        while (reader.hasNext())
        {
            reader.next();

            if (Utilities.xmlTagClosed(reader, tagNames()))
            {
                break;
            }

            if (Utilities.tagIs(reader, "metric"))
            {
                addMetric(new MetricSpecification(reader));
            }
        }
    }

    /**
     * Adds a created metric to the metric collection
     * @param metric The metric specification to add
     */
    public void addMetric(MetricSpecification metric)
    {
        if (metric == null)
        {
            LOGGER.trace("addMetric - metric was null");
            return;
        }

        LOGGER.trace("addMetric - metric passed: {}", metric);

        if (metrics == null)
        {
            metrics = new ArrayList<MetricSpecification>();
        }

        metrics.add(metric);
    }

    /**
     * Retrieves a specific metric
     * @param index The index of the metric to retrieve
     * @return A metric specification. Null is returned if the index was not valid
     */
    public MetricSpecification getMetric(int index)
    {
        MetricSpecification metric = null;

        if (index < metricCount())
        {
            metric = metrics.get(index);
        }

        return metric;
    }

    public MetricSpecification getMetric(String metricName)
    {
        MetricSpecification metric = null;

        if (metricCount() > 0)
        {
            metric = Utilities.find(metrics, (MetricSpecification met) -> {
               return met.getName().equalsIgnoreCase(metricName);
            });
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
            metrics = new ArrayList<MetricSpecification>();
        }

        return metrics.size();
    }
    
    public List<ProjectDataSpecification> getDatasources() {
        return this.dataSources;
    }

    public String getName()
    {
        return this.name;
    }

    @Override
    public String toString()
    {
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

        description += "Datasources:";
        description += System.lineSeparator();
        description += " +   +   +   +   +   +   +   +   +";
        description += System.lineSeparator();
        
        for (ProjectDataSpecification datasource : this.dataSources)
        {
            description += System.lineSeparator();
            description += datasource.toString();
            description += System.lineSeparator();
        }

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
            for (MetricSpecification metric : metrics)
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

    private ArrayList<MetricSpecification> metrics;
    private List<ProjectDataSpecification> dataSources;
    
    private String name;
    @Override
    public String toXML()
    {
        // TODO Auto-generated method stub
        return null;
    }
}
