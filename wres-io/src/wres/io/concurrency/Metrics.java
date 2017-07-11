package wres.io.concurrency;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wres.datamodel.DataFactory;
import wres.datamodel.PairOfDoubleAndVectorOfDoubles;
import wres.datamodel.metric.DefaultMetricInputFactory;
import wres.datamodel.metric.MetricInputFactory;
import wres.io.config.specification.MetricSpecification;
import wres.io.config.specification.ProjectDataSpecification;
import wres.io.config.specification.ScriptFactory;
import wres.io.data.caching.MeasurementUnits;
import wres.io.utilities.Database;
import wres.util.DataModel;
import wres.util.Strings;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.BiFunction;
import java.util.function.Function;

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
        final Map<String, BiFunction<MetricSpecification, Integer, Double>> functions = new TreeMap<>();
        
        functions.put("correlation_coefficient", createDirectCorrelationCoefficient());
        functions.put("mean_error", createDirectMeanError());
        
        return functions;
    }
    
    private static Map<String, Function<List<PairOfDoubleAndVectorOfDoubles>, Double>> createFunctionMap()
    {
        final Map<String, Function<List<PairOfDoubleAndVectorOfDoubles>, Double>> functions = new TreeMap<>();
        
        functions.put("mean_error", new MeanError());
        functions.put("correlation_coefficient", new CorrelationCoefficient());
        
        return functions;
    }
    
    public static boolean hasFunction(final String functionName)
    {
        return FUNCTIONS.containsKey(functionName);
    }
    
    public static boolean hasDirectFunction(final String functionName)
    {
        return DIRECT_FUNCTIONS.containsKey(functionName);
    }
    
    public static Double call(final MetricSpecification specification, final Integer step)
    {
        Double result = null;
        
        if (hasDirectFunction(specification.getMetricType()))
        {
            result = DIRECT_FUNCTIONS.get(specification.getMetricType()).apply(specification, step);
            if (LOGGER.isDebugEnabled())
            {
                LOGGER.debug("Result came from " + specification.getMetricType()
                             + " and step " + step);
            }
        }
        else
        {
            LOGGER.debug("The function named: '" + specification.getMetricType()
                         + "' is not an available function. Returning null...");
        }

        return result;
    }
    
    public static Double call(final String functionName, final List<PairOfDoubleAndVectorOfDoubles> pairs)
    {
        Double result = null;

        if (hasFunction(functionName))
        {
            result = FUNCTIONS.get(functionName).apply(pairs);
        }
        else
        {
            LOGGER.debug("The function named: {} is not an available function. Returning null...", functionName);
        }

        return result;
    }

    @Deprecated
	public static BiFunction<ResultSet, Function<Double, Double>, Double> calculateMeanError()
	{
		return (final ResultSet dataset, final Function<Double, Double> scale_func) -> {
			Double sum = 0.0;
			int value_count = 0;
			Float[] ensemble_values;
			Double observation;
			
			try {
				while (dataset.next())
				{
					ensemble_values = (Float[])dataset.getArray("measurements").getArray();
					observation = dataset.getFloat("measurement")*1.0;

					for (final Float ensemble_value : ensemble_values) {
						value_count++;
						sum += scale_func.apply(ensemble_value * 1.0) - scale_func.apply(observation);
					}
				}
			} catch (final SQLException e) {
			    LOGGER.debug(Strings.getStackTrace(e));
			}
			
			return sum/value_count;
		};
	}

	public static List<PairOfDoubleAndVectorOfDoubles> getPairs(final MetricSpecification metricSpecification, final int progress) throws Exception {
        final List<PairOfDoubleAndVectorOfDoubles> pairs = new ArrayList<>();
        
        Connection connection = null;
        ResultSet resultingPairs = null;
        //JBr: will need to inject this factory to eliminate dependence on wres-datamodel
        // CT: Switched reference back to wres-datamodel because it didn't compile
        final MetricInputFactory dataFactory = DefaultMetricInputFactory.getInstance();
        try
        {
            connection = Database.getConnection();
            final String script = ScriptFactory.generateGetPairData(metricSpecification, progress);            

            resultingPairs = Database.getResults(connection, script);

            while (resultingPairs.next())
            {
                final Double observedValue = resultingPairs.getDouble("sourceOneValue");
                final Double[] forecasts = (Double[]) resultingPairs.getArray("measurements").getArray();
                pairs.add(DataFactory.pairOf(observedValue, forecasts));
            }
        }
        catch (final Exception error)
        {
            LOGGER.error("Pairs could not be retrieved for the following parameters:");
            LOGGER.error("Progress: " + progress);
            LOGGER.error("The metric was:");
            LOGGER.error(String.valueOf(metricSpecification));
            LOGGER.error(Strings.getStackTrace(error));
            throw error;
        }
        finally
        {
            if (resultingPairs != null)
            {
                resultingPairs.close();
            }
            if (connection != null)
            {
                Database.returnConnection(connection);
            }
        }
        return pairs;
	}
	
	private static BiFunction<MetricSpecification, Integer, Double> createDirectCorrelationCoefficient()
	{
	    return (final MetricSpecification specification, final Integer progress) -> {
	        Double correlationCoefficient = null;
	        String script = "";        
	        try
	        {
	            final String leadQualifier = specification.getAggregationSpecification().getLeadQualifier(progress);
	            final ProjectDataSpecification sourceOne = specification.getFirstSource();
	            final ProjectDataSpecification sourceTwo = specification.getSecondSource();
                
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
                script += "     AND UC.to_unit = " + MeasurementUnits.getMeasurementUnitID(sourceTwo.getMeasurementUnit()) + NEWLINE;
                script += "     AND OUC.to_unit = " + MeasurementUnits.getMeasurementUnitID(sourceOne.getMeasurementUnit()) + ";" + NEWLINE;
                
                correlationCoefficient = Database.getResult(script, "correlation");
	        }
	        catch (final Exception error)
	        {
	            LOGGER.error("An error was encountered");
	            LOGGER.error("The script was:");
	            LOGGER.error(String.valueOf(script));
	            LOGGER.error(Strings.getStackTrace(error));
	        }
	        
	        return correlationCoefficient;
	    };
	}
	
	private static BiFunction<MetricSpecification, Integer, Double> createDirectMeanError()
	{
	    return (final MetricSpecification specification, final Integer progress) -> {
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
                script += "    AND UC.to_unit = " + MeasurementUnits.getMeasurementUnitID(specification.getSecondSource().getMeasurementUnit()) + NEWLINE;
                script += "    AND OUC.to_unit = " + MeasurementUnits.getMeasurementUnitID(specification.getFirstSource().getMeasurementUnit()) + ";";

                meanError = Database.getResult(script, "mean_error");
            }
            catch(final Exception e)
            {
                LOGGER.error("An error was encountered");
                LOGGER.error("The script was:");
                LOGGER.error(String.valueOf(script));
                LOGGER.error(Strings.getStackTrace(e));
            }
	        
	        return meanError;
	    };
	}
	
	private static class MeanError implements Function<List<PairOfDoubleAndVectorOfDoubles>, Double>
	{
	    @Override
	    public Double apply(final List<PairOfDoubleAndVectorOfDoubles> pairs)
	    {
	        double total = 0.0;
	        int ensembleCount = 0;
	        Double mean = null;
	        
	        for (final PairOfDoubleAndVectorOfDoubles pair : pairs)
	        {
	            for (int pairedValue = 0; pairedValue < pair.getItemTwo().length; ++pairedValue)
	            {
	                final double error = (pair.getItemTwo()[pairedValue] - pair.getItemOne());
	                total += error;
	                ensembleCount++;
	            }
	        }
	        
	        if (pairs.size() > 0)
	        {
	            mean = total / ensembleCount;
	        }
	        
	        return mean;
	    }
	}

	private static class CorrelationCoefficient implements Function<List<PairOfDoubleAndVectorOfDoubles>, Double>
	{
	    @Override
	    public Double apply(final List<PairOfDoubleAndVectorOfDoubles> pairs)
	    {
	        double CC = 0.0;
	        
	        final double leftSTD = DataModel.getPairedDoubleStandardDeviation(pairs);
	        final double rightSTD = DataModel.getPairedDoubleVectorStandardDeviation(pairs);
	        final double covariance = DataModel.getCovariance(pairs);
	        
	        if (leftSTD != 0 && rightSTD != 0)
	        {
	            CC = covariance / (leftSTD * rightSTD);
	        }
	        
	        return CC;
	    }
	}
}
