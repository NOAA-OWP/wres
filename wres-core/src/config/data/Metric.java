/**
 * 
 */
package config.data;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.Future;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import collections.AssociatedPairs;
import collections.Pair;
import collections.RealCollection;
import concurrency.Executor;
import concurrency.PairFetcher;
import data.FeatureCache;
import data.MeasurementCache;
import data.ValuePairs;
import data.VariableCache;
import thredds.client.catalog.Dataset;
import util.Database;
import util.Utilities;
import wres.datamodel.EnsemblePair;

/**
 * The specification for a metric and the information necessary to retrieve details
 * for it from the database
 * @author Christopher Tubbs
 */
public class Metric extends ClauseConfig {

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
	
	public Map<Integer, List<EnsemblePair>> getPairs() throws Exception
	{
	    Map<Integer, List<EnsemblePair>> results = new TreeMap<Integer, List<EnsemblePair>>();
	    Map<Integer, Future<List<EnsemblePair>>> threadResults = new TreeMap<Integer, Future<List<EnsemblePair>>>();
	    
	    // TODO: Change this to consume the aggregation specification
	    List<Integer> steps = getSteps(forecasts.getVariable().getVariableID());
	    float threadsComplete = 0;
	    float threadsAdded = 0;
        
        for (Integer step : steps)
        {
            // TODO: Convert observations and forecasts to 'Source One' and 'Source Two' (or some derivative)
            PairFetcher fetcher = new PairFetcher(observations, forecasts, "lead = " + step);
            threadResults.put(step, Executor.submit(fetcher));
            threadsAdded++;
        }
        
        System.err.println(threadsAdded + " operations were added to collect pairs. Waiting for results...");
        
        //for (Integer lead : threadResults.keySet())
        //{
            //results.put(lead, threadResults.get(lead).get());
            //threadResults.put(lead, null);
        for (Entry<Integer, Future<List<EnsemblePair>>> result : threadResults.entrySet())
        {
            results.put(result.getKey(), result.getValue().get());
            threadsComplete++;
            System.err.print("\r" +threadsComplete + "/" + threadsAdded + " operations complete. (" + (threadsComplete/threadsAdded) * 100 + "%)\t");
        }
        
        System.out.println();
        
	    return results;
	}
	
	private static List<Integer> getSteps(int variableID) throws SQLException
	{
	    List<Integer> steps = new ArrayList<Integer>();
	    String script = "";
	    script += "SELECT FV.lead" + newline;
	    script += "FROM wres.Forecast F" + newline;
	    script += "INNER JOIN wres.ForecastEnsemble FE" + newline;
	    script += "    ON FE.forecast_id = F.forecast_id" + newline;
	    script += "INNER JOIN wres.ForecastValue FV" + newline;
	    script += "    ON FV.forecastensemble_id = FE.forecastensemble_id" + newline;
	    script += "INNER JOIN wres.VariablePosition VP" + newline;
	    script += "    ON VP.variableposition_id = FE.variableposition_id" + newline;
	    script += "WHERE VP.variable_id = " + variableID + newline;
	    script += "GROUP BY FV.lead;";
	    
	    Connection connection = null;
	    try
	    {
	        connection = Database.getConnection();
	        ResultSet stepResults = Database.getResults(connection, script);
	        
	        while (stepResults.next())
	        {
	            steps.add(stepResults.getInt("lead"));
	        }
	    }
	    finally {
	        Database.returnConnection(connection);
	    }
	    
	    return steps;
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
	
	/**
	 * @return The name of the metric
	 */
	public String getName()
	{
	    return this.name;
	}

	private String name;
	private Output metric_output;
	private ProjectDataSource observations;
	private ProjectDataSource forecasts;
	
}
