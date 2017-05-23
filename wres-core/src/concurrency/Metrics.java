/**
 * 
 */
package concurrency;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.TreeMap;

import config.specification.ProjectDataSpecification;
import config.specification.MetricSpecification;
import config.specification.ScriptFactory;
import data.caching.MeasurementCache;
import util.Database;
import util.Utilities;
import wres.datamodel.DataFactory;
import wres.datamodel.PairOfDoubleAndVectorOfDoubles;

/**
 * A collection of Metrics that may be performed on selected data
 * 
 * @author Christopher Tubbs
 */
public abstract class Metrics {

    private static final String NEWLINE = System.lineSeparator();
    private static final Logger LOGGER = LoggerFactory.getLogger(Metrics.class);
    
    private static final Map<String, Function<List<PairOfDoubleAndVectorOfDoubles>, Double>> FUNCTIONS = createFunctionMap();
    private static final Map<String, BiFunction<MetricSpecification, Integer, Double>> DIRECT_FUNCTIONS = createDirectFunctionMapping();
    
    private static Map<String, BiFunction<MetricSpecification, Integer, Double>> createDirectFunctionMapping()
    {
        Map<String, BiFunction<MetricSpecification, Integer, Double>> functions = new TreeMap<>();
        
        functions.put("correlation_coefficient", createDirectCorrelationCoefficient());
        functions.put("mean_error", createDirectMeanError());
        
        return functions;
    }
    
    private static Map<String, Function<List<PairOfDoubleAndVectorOfDoubles>, Double>> createFunctionMap()
    {
        Map<String, Function<List<PairOfDoubleAndVectorOfDoubles>, Double>> functions = new TreeMap<>();
        
        functions.put("mean_error", getMeanError());
        functions.put("correlation_coefficient", getCorrelationCoefficient());
        
        return functions;
    }
    
    public static boolean hasFunction(String functionName)
    {
        return FUNCTIONS.containsKey(functionName);
    }
    
    public static boolean hasDirectFunction(String functionName)
    {
        return DIRECT_FUNCTIONS.containsKey(functionName);
    }
    
    public static Double call(MetricSpecification specification, Integer step)
    {
        Double result = null;
        
        if (hasDirectFunction(specification.getMetricType()))
        {
            result = DIRECT_FUNCTIONS.get(specification.getMetricType()).apply(specification, step);
        }
        else
        {
            System.err.println("The function named: '" + specification.getMetricType() + "' is not an available function. Returning null...");
        }
        
        return result;
    }
    
    public static Double call(String functionName, List<PairOfDoubleAndVectorOfDoubles> pairs)
    {
        Double result = null;
        
        if (hasFunction(functionName))
        {
            result = FUNCTIONS.get(functionName).apply(pairs);
        }
        else
        {
            LOGGER.debug("The function named: '" + functionName + "' is not an available function. Returning null...");
        }
        
        return result;
    }
    
    public static String[] getFunctionNames()
    {
        String[] functionNames = new String[FUNCTIONS.size()];
        
        int nameIndex = 0;
        
        for (String name : FUNCTIONS.keySet())
        {
            functionNames[nameIndex] = name;
            nameIndex++;
        }
        
        return functionNames;
    }
	
	@Deprecated
	/**
	 * Creates a function that will transform and evaluate the mean error of all observed and forecasted values
	 * from the database
	 * 
	 * The result set MUST contain a column containing an array of floats named "measurements" and a column containing a float
	 * named "measurement"
	 * 
	 * @return The mean error of all values in the result set
	 * @Deprecated Relies on the old schema which expects forecasted values to be contained within an array
	 */
	public static BiFunction<ResultSet, Function<Double, Double>, Double> calculateMeanError()
	{
		return (ResultSet dataset, Function<Double, Double> scale_func) -> {
			Double sum = 0.0;
			int value_count = 0;
			Float[] ensemble_values;
			Double observation;
			
			try {
				while (dataset.next())
				{
					ensemble_values = (Float[])dataset.getArray("measurements").getArray();
					observation = dataset.getFloat("measurement")*1.0;
					
					for (int ensemble_index = 0; ensemble_index < ensemble_values.length; ++ensemble_index)
					{
						value_count++;		
						sum += scale_func.apply(ensemble_values[ensemble_index]*1.0) - scale_func.apply(observation);
					}
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
			
			return sum/value_count;
		};
	}

	/**
	 * Creates the "meanError" function to run on selected data
	 * @return The function to perform mean error calculations on a set of data containing the columns
	 * "forecast" and "observation".
	 */
	public static BiFunction<ResultSet, Function<Double, Double>, Double> meanError() {
	    return (ResultSet dataset, Function<Double, Double> scaleFunc) -> {
	        Double sum = 0.0;
	        Double intermediateSum = 0.0;
	        int valueCount = 0;
	        
	        try {
	            while (dataset.next()) {
	                intermediateSum = (dataset.getFloat("forecast") * 1.0) - (dataset.getFloat("observation") * 1.0);
	                sum += scaleFunc.apply(intermediateSum);
	                valueCount++;
	            }
	        } catch (SQLException error) {
	            error.printStackTrace();
	        }
	        
	        
	        
	        return sum / valueCount;
	    };
	}
	
	public static List<PairOfDoubleAndVectorOfDoubles> getPairs(MetricSpecification metricSpecification, int progress) throws Exception {
        List<PairOfDoubleAndVectorOfDoubles> pairs = new ArrayList<>();
        Connection connection = null;

        try
        {
            connection = Database.getConnection();
            String script = ScriptFactory.generateGetPairData(metricSpecification, progress);            
            
            ResultSet resultingPairs = Database.getResults(connection, script);
            
            while (resultingPairs.next())
            {
                Double observedValue = resultingPairs.getDouble("sourceOneValue");
                Double[] forecasts = (Double[]) resultingPairs.getArray("measurements").getArray();
                pairs.add(DataFactory.pairOf(observedValue, forecasts));
            }
        }
        catch (Exception error)
        {
            System.err.println("Pairs could not be retrieved for the following parameters:");
            System.err.println("Progress: " + progress);
            System.err.println();
            System.err.println("The metric was:");
            System.err.println(metricSpecification.toString());
            System.err.println();
            error.printStackTrace();
            System.err.println();
            throw error;
        }
        finally
        {
            if (connection != null)
            {
                Database.returnConnection(connection);
            }
        }
        return pairs;
	}
	
	private static BiFunction<MetricSpecification, Integer, Double> createDirectCorrelationCoefficient()
	{
	    return (MetricSpecification specification, Integer progress) -> {
	        Double correlationCoefficient = null;
	        String script = "";        
	        try
	        {
	            String leadQualifier = specification.getAggregationSpecification().getLeadQualifier(progress);
	            ProjectDataSpecification sourceOne = specification.getFirstSource();
	            ProjectDataSpecification sourceTwo = specification.getSecondSource();
                
                script += "SELECT CORR(O.observed_value * OUC.factor, FV.forecasted_value * UC.factor) AS correlation" + NEWLINE; 
                script += "FROM wres.Forecast F" + NEWLINE;
                script += "INNER JOIN wres.ForecastEnsemble FE" + NEWLINE;
                script += "     ON FE.forecast_id = F.forecast_id" + NEWLINE;
                script += "INNER JOIN wres.ForecastValue FV" + NEWLINE;
                script += "     ON FE.forecastensemble_id = FV.forecastensemble_id" + NEWLINE;
                script += "INNER JOIN wres.Observation O" + NEWLINE;
                script += "     ON O.observation_time = F.forecast_date + INTERVAL '1 HOUR' * lead" + NEWLINE;  // What about the offset?
                script += "INNER JOIN wres.UnitConversion UC" + NEWLINE;
                script += "     ON UC.from_unit = FE.measurementunit_id" + NEWLINE;
                script += "INNER JOIN wres.UnitConversion OUC" + NEWLINE;
                script += "     ON OUC.from_unit = O.measurementunit_id" + NEWLINE;
                script += "WHERE FE.variableposition_id = " + sourceTwo.getFirstVariablePositionID() + NEWLINE;
                script += "     AND O.variableposition_id = " + sourceOne.getFirstVariablePositionID() + NEWLINE;
                script += "     AND FV." + leadQualifier + NEWLINE;
                script += "     AND UC.to_unit = " + MeasurementCache.getMeasurementUnitID(sourceTwo.getMeasurementUnit()) + NEWLINE;
                script += "     AND OUC.to_unit = " + MeasurementCache.getMeasurementUnitID(sourceOne.getMeasurementUnit()) + ";" + NEWLINE;
                
                correlationCoefficient = Database.getResult(script, "correlation");
	        }
	        catch (Exception error)
	        {
	            System.err.println();
	            System.err.println("An error was encountered");
	            System.err.println();
	            System.err.println("The script was:");
	            System.err.println();
	            System.err.println(String.valueOf(script));
	            System.err.println();
	            
	            error.printStackTrace();
	        }
	        
	        return correlationCoefficient;
	    };
	}
	
	private static BiFunction<MetricSpecification, Integer, Double> createDirectMeanError()
	{
	    return (MetricSpecification specification, Integer progress) -> {
	        Double meanError = null;
	        
	        String script = "";
	        try
            {
	            
	            script += "SELECT AVG(FV.forecasted_value * UC.factor - O.observed_value * OUC.factor) AS mean_error" + NEWLINE;
	            script += "FROM wres.Forecast F" + NEWLINE;
	            script += "INNER JOIN wres.ForecastEnsemble FE" + NEWLINE;
	            script += "    ON FE.forecast_id = F.forecast_id" + NEWLINE;
	            script += "INNER JOIN wres.ForecastValue FV" + NEWLINE;
	            script += "    ON FE.forecastensemble_id = FV.forecastensemble_id" + NEWLINE;
	            script += "INNER JOIN wres.Observation O" + NEWLINE;
	            script += "    ON O.observation_time = F.forecast_date + INTERVAL '1 hour' * lead" + NEWLINE;
	            script += "INNER JOIN wres.UnitConversion UC" + NEWLINE;
	            script += "    ON UC.from_unit = FE.measurementunit_id" + NEWLINE;
	            script += "INNER JOIN wres.UnitConversion OUC" + NEWLINE;
	            script += "    ON O.measurementunit_id = OUC.from_unit" + NEWLINE;
                script += "WHERE FE.variableposition_id = " + specification.getSecondSource().getFirstVariablePositionID() + NEWLINE;
                script += "    AND O.variableposition_id = " + specification.getFirstSource().getFirstVariablePositionID() + NEWLINE;
                script += "    AND FV." + specification.getAggregationSpecification().getLeadQualifier(progress) + NEWLINE;
                script += "    AND UC.to_unit = " + MeasurementCache.getMeasurementUnitID(specification.getSecondSource().getMeasurementUnit()) + NEWLINE;
                script += "    AND OUC.to_unit = " + MeasurementCache.getMeasurementUnitID(specification.getFirstSource().getMeasurementUnit()) + NEWLINE;
                
                meanError = Database.getResult(script, "mean_error");
            }
            catch(Exception e)
            {
                System.err.println();
                System.err.println("An error was encountered");
                System.err.println();
                System.err.println("The script was:");
                System.err.println();
                System.err.println(String.valueOf(script));
                e.printStackTrace();
            }
	        
	        return meanError;
	    };
	}
	
	private static Function<List<PairOfDoubleAndVectorOfDoubles>, Double> getMeanError()
	{
	    return (List<PairOfDoubleAndVectorOfDoubles> pairs) -> {
	        double total = 0.0;
	        int ensembleCount = 0;
	        Double mean = null;
	        
	        for (PairOfDoubleAndVectorOfDoubles pair : pairs)
	        {
	            for (int pairedValue = 0; pairedValue < pair.getItemTwo().length; ++pairedValue)
	            {
	                double error = (pair.getItemTwo()[pairedValue] - pair.getItemOne());
	                total += error;
	                ensembleCount++;
	            }
	        }
	        
	        if (pairs.size() > 0)
	        {
	            mean = total / ensembleCount;
	        }
	        
	        return mean;
	    };
	}
	
	private static Function<List<PairOfDoubleAndVectorOfDoubles>, Double> getCorrelationCoefficient()
	{
	    return (List<PairOfDoubleAndVectorOfDoubles> pairs) -> {
	        double CC = 0.0;
	        
	        double leftSTD = Utilities.getPairedDoubleStandardDeviation(pairs);
	        double rightSTD = Utilities.getPairedDoubleVectorStandardDeviation(pairs);
	        double covariance = Utilities.getCovariance(pairs);
	        
	        if (leftSTD != 0 && rightSTD != 0)
	        {
	            CC = covariance / (leftSTD * rightSTD);
	        }
	        
	        return CC;
	    };
	}
}
