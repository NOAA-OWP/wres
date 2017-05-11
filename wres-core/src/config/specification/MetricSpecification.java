/**
 * 
 */
package config.specification;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.Future;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import collections.TwoTuple;
import concurrency.Executor;
import concurrency.PairFetcher;
import util.Database;
import util.Utilities;
import wres.datamodel.PairOfDoubleAndVectorOfDoubles;

/**
 * The specification for a metric and the information necessary to retrieve details
 * for it from the database
 * @author Christopher Tubbs
 */
public class MetricSpecification extends SpecificationElement {

	/**
	 * Constructor
	 * @param reader The XML Node containing data about the metric
	 * @throws Exception 
	 */
	public MetricSpecification(XMLStreamReader reader) throws Exception {
		super(reader);
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
			this.name = Utilities.getXMLText(reader);
		}
		else if (tag_name.equalsIgnoreCase("output"))
		{
			this.metric_output = new OutputSpecification(reader);
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
		else if (Utilities.tagIs(reader, "aggregation"))
		{
		    this.metricAggregate = new AggregationSpecification(reader);
		}
	}
	
	public Map<Integer, List<PairOfDoubleAndVectorOfDoubles>> getPairs() throws Exception
	{
	    Map<Integer, List<PairOfDoubleAndVectorOfDoubles>> results = new TreeMap<Integer, List<PairOfDoubleAndVectorOfDoubles>>();
	    Map<Integer, Future<List<PairOfDoubleAndVectorOfDoubles>>> threadResults = new TreeMap<Integer, Future<List<PairOfDoubleAndVectorOfDoubles>>>();
	    
	    float threadsComplete = 0;
	    float threadsAdded = 0;
	    
	    TwoTuple<String, String> lastLeadScript = ScriptBuilder.generateFindLastLead(sourceTwo.getVariable().getVariableID());
	    
	    Integer finalLead = Database.getResult(lastLeadScript.getItemOne(), lastLeadScript.getItemTwo());
        
	    int step = 1;
	    
	    while (metricAggregate.leadIsValid(step, finalLead)) {
            //PairFetcher fetcher = new PairFetcher(sourceOne, sourceTwo, metricAggregate.getLeadQualifier(step));
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
		description += sourceOne.toString();
		description += System.lineSeparator();
		
		description += "-  -  -  -  -  -  -  -  -  -";
		description += System.lineSeparator();
		description += System.lineSeparator();
		
		description += "Datasource Two: ";
		description += System.lineSeparator();
		description += System.lineSeparator();
		description += sourceTwo.toString();
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
	    return this.baseline;
	}
	
	public AggregationSpecification getAggregationSpecification() {
	    return this.metricAggregate;
	}

	private String name;
	private OutputSpecification metric_output;
	private ProjectDataSpecification sourceOne;
	private ProjectDataSpecification sourceTwo;
	private ProjectDataSpecification baseline;
	private AggregationSpecification metricAggregate;
    @Override
    public String toXML()
    {
        // TODO Auto-generated method stub
        return null;
    }
}
