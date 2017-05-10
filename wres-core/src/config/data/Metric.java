/**
 * 
 */
package config.data;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.Future;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import concurrency.Executor;
import concurrency.PairFetcher;
import util.Database;
import util.Utilities;
import wres.datamodel.EnsemblePair;

/**
 * The specification for a metric and the information necessary to retrieve details
 * for it from the database
 * @author Christopher Tubbs
 */
public class Metric extends ConfigElement {

	/**
	 * Constructor
	 * @param reader The XML Node containing data about the metric
	 * @throws Exception 
	 */
	public Metric(XMLStreamReader reader) throws Exception {
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
			this.metric_output = new Output(reader);
		}
		else if (tag_name.equalsIgnoreCase("source_one"))
		{
			this.source_one = new ProjectDataSource(reader);
		}
		else if (tag_name.equalsIgnoreCase("source_two"))
		{
			this.source_two = new ProjectDataSource(reader);
		}
		else if (tag_name.equalsIgnoreCase("baseline"))
		{
		    this.baseline = new ProjectDataSource(reader);
		}
		else if (Utilities.tagIs(reader, "aggregation"))
		{
		    this.metricAggregate = new Aggregation(reader);
		}
	}
	
	public Map<Integer, List<EnsemblePair>> getPairs() throws Exception
	{
	    Map<Integer, List<EnsemblePair>> results = new TreeMap<Integer, List<EnsemblePair>>();
	    Map<Integer, Future<List<EnsemblePair>>> threadResults = new TreeMap<Integer, Future<List<EnsemblePair>>>();
	    
	    float threadsComplete = 0;
	    float threadsAdded = 0;
	    
	    Integer finalLead = getLastLead(source_two.getVariable().getVariableID());
        
	    int step = 1;
	    
	    while (metricAggregate.leadIsValid(step, finalLead)) {
            PairFetcher fetcher = new PairFetcher(source_one, source_two, metricAggregate.getLeadQualifier(step));
            threadResults.put(step, Executor.submit(fetcher));
            threadsAdded++;
	        step++;
	    }
        
        System.err.println(threadsAdded + " operations were added to collect pairs. Waiting for results...");

        for (Entry<Integer, Future<List<EnsemblePair>>> result : threadResults.entrySet())
        {
            results.put(result.getKey(), result.getValue().get());
            threadsComplete++;
            System.err.print("\r" +threadsComplete + "/" + threadsAdded + " operations complete. (" + (threadsComplete/threadsAdded) * 100 + "%)------------");
        }
        
        System.out.println();
        
	    return results;
	}
	
	// TODO: Handle case where neither source uses forecast data
	private static Integer getLastLead(int variableID) throws SQLException
	{
	    Integer finalLead = -1;
	    
	    String script = "";
	    script += "SELECT FV.lead AS last_lead" + newline;
	    script += "FROM wres.VariablePosition VP" + newline;
	    script += "INNER JOIN wres.ForecastEnsemble FE" + newline;
	    script += "    ON FE.variableposition_id = VP.variableposition_id" + newline;
	    script += "INNER JOIN wres.ForecastValue FV" + newline;
	    script += "    ON FV.forecastensemble_id = FE.forecastensemble_id" + newline;
	    script += "WHERE VP.variable_id = " + variableID + newline;
	    script += "ORDER BY FV.lead DESC" + newline;
	    script += "LIMIT 1;";
	    
	    finalLead = Database.getResult(script, "last_lead");
	    
	    return finalLead;
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
		description += source_one.toString();
		description += System.lineSeparator();
		
		description += "-  -  -  -  -  -  -  -  -  -";
		description += System.lineSeparator();
		description += System.lineSeparator();
		
		description += "Datasource Two: ";
		description += System.lineSeparator();
		description += System.lineSeparator();
		description += source_two.toString();
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

	private String name;
	private Output metric_output;
	private ProjectDataSource source_one;
	private ProjectDataSource source_two;
	private ProjectDataSource baseline;
	private Aggregation metricAggregate;
    @Override
    public String toXML()
    {
        // TODO Auto-generated method stub
        return null;
    }
}
