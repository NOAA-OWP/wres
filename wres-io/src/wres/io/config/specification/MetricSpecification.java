/**
 * 
 */
package wres.io.config.specification;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.Future;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.io.concurrency.Executor;
import wres.io.concurrency.PairFetcher;
import wres.io.grouping.LabeledScript;
import wres.io.utilities.Database;
import wres.io.utilities.Debug;
import wres.util.NullPrintStream;
import wres.util.Strings;
import wres.util.XML;
import wres.datamodel.PairOfDoubleAndVectorOfDoubles;

/**
 * The specification for a metric and the information necessary to retrieve details
 * for it from the database
 * @author Christopher Tubbs
 */
public class MetricSpecification extends SpecificationElement {

    private static final Logger LOGGER = LoggerFactory.getLogger(MetricSpecification.class);
    /**
     * Constructor
     * @param reader The XML Node containing data about the metric
     * @throws IOException
     * @throws XMLStreamException
     */
    public MetricSpecification(XMLStreamReader reader) throws IOException, XMLStreamException
    {
        super(reader);
    }
    
    @Override
    protected void getAttributes(XMLStreamReader reader)
    {
        for (int attribute_index = 0; attribute_index < reader.getAttributeCount(); ++attribute_index)
        {
            String attribute_name = reader.getAttributeLocalName(attribute_index);
            
            if (attribute_name.equalsIgnoreCase("type"))
            {
                this.metricType = reader.getAttributeValue(attribute_index).trim();
            }
            else if(attribute_name.equalsIgnoreCase("measurement"))
            {
                this.desiredMeasurement = reader.getAttributeValue(attribute_index).trim();
            }
            else if(attribute_name.equalsIgnoreCase("directProcess"))
            {
                this.directProcess = Strings.isTrue(reader.getAttributeValue(attribute_index).trim());
            }
        }
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
			this.name = XML.getXMLText(reader);
		}
		else if (tag_name.equalsIgnoreCase("output"))
		{
			this.metric_output = new OutputSpecification(reader);
		}
		else if(XML.tagIs(reader, "datasource")) {
		    ProjectDataSpecification newDataSource = new ProjectDataSpecification(reader);
		    
		    if (newDataSource.getDataSourceIdentifier().equalsIgnoreCase("one")) {
		        this.sourceOne = newDataSource;
		    } else if (newDataSource.getDataSourceIdentifier().equalsIgnoreCase("two")) {
		        this.sourceTwo = newDataSource;
		    } else if (newDataSource.getDataSourceIdentifier().equalsIgnoreCase("baseline")){
		        this.baseline = newDataSource;
		    }
		}
		else if (tag_name.equalsIgnoreCase("source_one"))
		{
			this.sourceOne = new ProjectDataSpecification(reader);
		}
		else if (tag_name.equalsIgnoreCase("source_two"))
		{
			this.sourceTwo = new ProjectDataSpecification(reader);
		}
		else if (tag_name.equalsIgnoreCase("baseline"))
		{
		    this.baseline = new ProjectDataSpecification(reader);
		}
		else if (XML.tagIs(reader, "aggregation"))
		{
		    this.metricAggregate = new AggregationSpecification(reader);
		}
		else if (XML.tagIs(reader, "threshold"))
		{
		    this.threshold = new ThresholdSpecification(reader);
		}
	}
	
	public Map<Integer, List<PairOfDoubleAndVectorOfDoubles>> getPairs() throws Exception
	{
	    Map<Integer, List<PairOfDoubleAndVectorOfDoubles>> results = new TreeMap<Integer, List<PairOfDoubleAndVectorOfDoubles>>();
	    Map<Integer, Future<List<PairOfDoubleAndVectorOfDoubles>>> threadResults = new TreeMap<Integer, Future<List<PairOfDoubleAndVectorOfDoubles>>>();
	    
	    float threadsComplete = 0;
	    float threadsAdded = 0;
	    
	    LabeledScript lastLeadScript = ScriptFactory.generateFindLastLead(sourceTwo.getVariable().getVariableID());
	    
	    Integer finalLead = Database.getResult(lastLeadScript.getScript(), lastLeadScript.getLabel());
        
	    int step = 1;
	    
	    while (metricAggregate.leadIsValid(step, finalLead)) {
            threadResults.put(step, Executor.submit(new PairFetcher(this, step)));
            threadsAdded++;
	        step++;
	    }
        
        System.err.println(threadsAdded + " operations were added to collect pairs. Waiting for results...");

        for (Entry<Integer, Future<List<PairOfDoubleAndVectorOfDoubles>>> result : threadResults.entrySet())
        {
            results.put(result.getKey(), result.getValue().get());
            threadsComplete++;
            System.err.print("\r" +threadsComplete + "/" + threadsAdded + " operations complete. (" + (threadsComplete/threadsAdded) * 100 + "%)------------");
        }
        
        System.out.println();
        
	    return results;
	}
	
	public Integer getFirstVariableID() throws Exception
	{
	    Integer variableID = null;
	    if (sourceOne != null && sourceOne.getVariable() != null && sourceOne.getVariable().getVariableID() != null)
	    {
	        variableID = sourceOne.getVariable().getVariableID();
	    }
	    else
	    {
	        Debug.warn(LOGGER, "One of these was null: sourceOne.getVariable().getVariableID()", NullPrintStream.get());
	    }
	    return variableID;
	}
	
	public Integer getSecondVariableID() throws Exception
	{
	    Integer variableID = null;
	    if (sourceTwo != null && sourceTwo.getVariable() != null && sourceTwo.getVariable().getVariableID() != null)
	    {
	        variableID =  sourceTwo.getVariable().getVariableID();
	    }
	    else
	    {
	        Debug.warn(LOGGER, "One of these was null: sourceTwo.getVariable().getVariableID()", NullPrintStream.get());
	    }
	    return variableID;
	}
	
	public String getDesiredMeasurementUnit()
	{
	    String desiredUnit = null;
	    
	    if (!this.desiredMeasurement.isEmpty())
	    {
	        desiredUnit = this.desiredMeasurement;
	    }
	    
	    return desiredUnit;
	}

	@Override
	protected List<String> tagNames() {
		return Arrays.asList("metric");
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
		
		description += "Datasource One: ";
		description += System.lineSeparator();
		description += System.lineSeparator();
		if (sourceOne != null)
		{
		    description += sourceOne.toString();
		}
		else
		{
		    description += "[none]";
		}
		description += System.lineSeparator();
		
		description += "-  -  -  -  -  -  -  -  -  -";
		description += System.lineSeparator();
		description += System.lineSeparator();
		
		description += "Datasource Two: ";
		description += System.lineSeparator();
		description += System.lineSeparator();
	    if (sourceTwo != null)
		{
		    description += sourceTwo.toString();
		}
		else
		{
		    description += "[none]";
		}
	    description += System.lineSeparator();
		if (baseline != null) {
	        description += "Baseline: ";
	        description += System.lineSeparator();
	        description += System.lineSeparator();
	        description += baseline.toString();
	        description += System.lineSeparator();
		}
		
		return description;
	}
	
	/**
	 * @return The name of the metric
	 */
	public String getName()
	{
	    return this.name;
	}
	
	public ProjectDataSpecification getFirstSource()
	{
	    return this.sourceOne;
	}
	
	public ProjectDataSpecification getSecondSource() {
	    return this.sourceTwo;
	}

	public ProjectDataSpecification getBaselineSource() {
        if (this.baseline == null)
        {
            Debug.warn(LOGGER, "getBaselineSource - returning null", NullPrintStream.get());
        }
	    return this.baseline;
	}

	public AggregationSpecification getAggregationSpecification()
	{
	    if (this.metricAggregate == null)
	    {
	        Debug.warn(LOGGER, "getAggregationSpecification - returning null", NullPrintStream.get());
	    }
        return this.metricAggregate;
	}
	
	public String getMetricType()
	{
	    return this.metricType;
	}
	
	public ThresholdSpecification.ThresholdMode getThresholdMode()
	{
	    if (this.threshold == null)
	    {
	        this.threshold = new ThresholdSpecification();
	    }
	    
	    return this.threshold.getMode();
	}
	
	public float getThresholdValue()
	{
	    if (this.threshold == null)
	    {
	        this.threshold = new ThresholdSpecification();
	    }
	    
	    return this.threshold.getValue();
	}
	
	public boolean shouldProcessDirectly()
	{
	    return this.directProcess;
	}

	private String name;
	private OutputSpecification metric_output;
	private ProjectDataSpecification sourceOne;
	private ProjectDataSpecification sourceTwo;
	private ProjectDataSpecification baseline;
	private AggregationSpecification metricAggregate;
	private String metricType;
	private ThresholdSpecification threshold;
	private boolean directProcess;
	private String desiredMeasurement;
	
    @Override
    public String toXML()
    {
        // TODO Auto-generated method stub
        return null;
    }
}
