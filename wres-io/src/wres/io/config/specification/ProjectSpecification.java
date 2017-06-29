/**
 * 
 */
package wres.io.config.specification;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.util.Collections;
import wres.util.XML;

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
     */
    public ProjectSpecification(XMLStreamReader reader) {
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
        
		if (XML.tagIs(reader, "name"))
		{
			this.name = XML.getXMLText(reader);
		}
		else if (XML.tagIs(reader, "observations", "forecasts", "dataSource"))
		{
		    addDatasource(reader);
		}
		else if (XML.tagIs(reader, "metrics"))
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
        return java.util.Collections.singletonList("project");
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
            return;
        }
        
        LOGGER.trace("parseMetrics - reader passed: {}", reader);

        while (reader.hasNext())
        {
            reader.next();

            if (XML.xmlTagClosed(reader, tagNames()))
            {
                break;
            }

            if (XML.tagIs(reader, "metric"))
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
            metrics = new ArrayList<>();
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
            metric = Collections.find(metrics, (MetricSpecification met) -> {
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
            metrics = new ArrayList<>();
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
        StringBuilder description = new StringBuilder("-----------------------------------");
        description.append(System.lineSeparator());
        description.append(System.lineSeparator());
        description.append("Project: ");

        if (name == null)
        {
            description.append("[Unnamed Project]");
        }
        else
        {
            description.append(name);
        }
        description.append(System.lineSeparator());
        description.append(System.lineSeparator());

        description.append("-----------------------------------");
        description.append(System.lineSeparator());
        description.append(System.lineSeparator());

        description.append("Datasources:");
        description.append(System.lineSeparator());
        description.append(" +   +   +   +   +   +   +   +   +");
        description.append(System.lineSeparator());
        
        for (ProjectDataSpecification datasource : this.dataSources)
        {
            description.append(System.lineSeparator());
            description.append(datasource.toString());
            description.append(System.lineSeparator());
        }

        description.append("-----------------------------------");
        description.append(System.lineSeparator());
        description.append(System.lineSeparator());

        description.append("Metrics:");
        description.append(System.lineSeparator());
        description.append(System.lineSeparator());
        description.append("\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/");
        description.append(System.lineSeparator());
        description.append(System.lineSeparator());
        if (metricCount() > 0)
        {
            for (MetricSpecification metric : metrics)
            {
                description.append(metric.toString());
                description.append(System.lineSeparator());
                description.append("*  *  *  *  *  *  *  *  *  *  *  *  *");
                description.append(System.lineSeparator());
            }
        }
        else {
            description.append("[NONE]");
            description.append(System.lineSeparator());
        }
        description.append("\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/\\/");

        return description.toString();
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
