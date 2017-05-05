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
	
	public Map<Integer, ValuePairs> getPairs() throws Exception
	{
	    Map<Integer, ValuePairs> results = new TreeMap<Integer, ValuePairs>();
	    Map<Integer, Future<ValuePairs>> threadResults = new TreeMap<Integer, Future<ValuePairs>>();

	    int forecastVariableID = VariableCache.getVariableID(forecasts.getVariables().get(0).name(), 
	                                                         forecasts.getVariables().get(0).getUnit());
	    
	    int observationVariableID = VariableCache.getVariableID(observations.getVariables().get(0).name(), 
	                                                            observations.getVariables().get(0).getUnit());
	    
	    /*int minimumIDs = Math.min(observationVariablePositionIDs.size(), forecastVariablePositionIDs.size());
	    
	    List<Integer> steps = getSteps(forecastVariableID);
	    
	    for (int positionIndex = 0; positionIndex < minimumIDs; ++minimumIDs)
	    {
	        Map<Integer, Future<ValuePairs>> futureLeadPairs = new TreeMap<Integer, Future<ValuePairs>>();
	        
	        for (Integer step : steps)
	        {
	            // TODO: Convert observations and forecasts to 'Source One' and 'Source Two' (or some derivative)
	            PairFetcher fetcher = new PairFetcher(observations, forecasts, "lead = " + step);
	            futureLeadPairs.put(step, Executor.submit(fetcher));
	        }
	        
	        threadResults.put(observationVariablePositionID, forecastVariablePositionID, futureLeadPairs);
	    }
	    
	    for (Pair<Integer, Integer> threadKey : threadResults.keySet())
	    {
	        Map<Integer, ValuePairs> resultingPairs = new TreeMap<Integer, ValuePairs>();
	        for (Integer lead : threadResults.get(threadKey).keySet())
	        {
	            resultingPairs.put(lead, threadResults.get(threadKey).get(lead).get());
	        }
	        results.put(threadKey, resultingPairs);
	    }*/
        
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
