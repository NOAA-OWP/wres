/**
 * 
 */
package concurrency;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.concurrent.Callable;

import collections.RealCollection;
import config.data.Conditions;
import config.data.ProjectDataSource;
import data.FeatureCache;
import data.MeasurementCache;
import data.ValuePairs;
import util.Database;

/**
 * @author Christopher Tubbs
 *
 */
public final class PairFetcher implements Callable<ValuePairs>
{
    private static final boolean USE_DOUBLE_PAIR = false;
    private static final String newline = System.lineSeparator();
    /**
     * 
     */
    public PairFetcher(ProjectDataSource sourceOne, ProjectDataSource sourceTwo, String leads)
    {
        this.sourceOne = sourceOne;
        this.sourceTwo = sourceTwo;
        this.leads = leads;
    }

    @Override
    public ValuePairs call() throws Exception
    {
        ValuePairs pairs = new ValuePairs();
        Connection connection = null;
        try
        {
            connection = Database.getConnection();
            String script = "";
            if (USE_DOUBLE_PAIR)
            {
                script = createSelectScript();
            }
            else
            {
                script = createArrayScript();
            }

            System.err.println(script);
            /*
            ResultSet resultingPairs = Database.getResults(connection, script);
            
            while (resultingPairs.next())
            {
                Float observedValue = resultingPairs.getFloat("observation");
                RealCollection forecasts = new RealCollection((Double[])resultingPairs.getArray("forecasts").getArray());
                pairs.add(observedValue, forecasts);
            }*/
        }
        catch (Exception error)
        {
            System.err.println("Pairs could not be retrieved for the following parameters:");
            System.err.println("Lead Time(s): " + leads);
            System.err.println();
            System.err.println("First set of data comes from:");
            System.err.println(sourceOne.toString());
            System.err.println();
            System.err.println("The Second set of data comes from:");
            System.err.println(sourceTwo.toString());
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
    
    private String createArrayScript() throws Exception
    {
        String script = "";
        script +=   "WITH sourceTwo AS      -- The CTE that produces the array for the second source" + newline;
        script +=   "(" + newline;
        script +=   "   SELECT ";
        
        if (sourceTwo.isForecast())
        {
            script += "F.forecast_date + INTERVAL '1 hour' * lead";
            
            if (sourceTwo.conditions().hasOffset())
            {
                script += " + (INTERVAL '1 hour' * " + sourceTwo.conditions().getOffset() + ")";
            }
            
            script += " AS sourceTwoDate,       -- The date to match the first source's" + newline;            
            script += "         array_agg(FV.forecasted_value * UC.factor) AS measurements      ";
            script += "-- Array consisting of each ensemble member corresponding to a lead time from a forecast" + newline;
            script += "     FROM wres.Forecast F        -- Start by selecting from the available forecasts" + newline;
            script += "     INNER JOIN wres.ForecastEnsemble FE     -- Retrieve all of the ensembles for the forecasts from above" + newline;
            script += "         ON F.forecast_id = FE.forecast_id       -- Match on the generated identifier for the forecast" + newline;
            script += "     INNER JOIN wres.ForecastValue FV        -- Retrieve the values for all of the retrieved ensembles" + newline;
            script += "         ON FV.forecastensemble_id = FE.forecastensemble_id      -- Match on the generated identifier for the ensemble matched with the forecast" + newline;
            script += "     INNER JOIN wres.UnitConversion UC       -- Retrieve the conversion factor to convert the value with the ensemble's unit to the desired unit" + newline;
            script += "         ON UC.from_unit = FE.measurementunit_id     ";
            script += "-- The conversion factor will be obtained by matching the unit from the ensemble with the factor's unit to convert from" + newline;
            script += "     WHERE " + this.leads + "        -- Select the values attached to the lead time specification passed into the thread" + newline;
            script += "         AND FE.variableposition_id = ";
            script += String.valueOf(sourceTwo.getFeature().getVariablePositionIDs(sourceTwo.getVariable().getVariableID()).get(0));
            script += "     -- Select the ensembles for variable values at a specific location" + newline;
            script += "         AND UC.to_unit = " + MeasurementCache.getMeasurementUnitID(sourceTwo.getVariable().getUnit());
            script += "     -- Determine to unit conversion based on the specification's indication of the desired unit to convert to"+ newline;
            
            if (sourceTwo.conditions().hasEarliestDate())
            {
                script += "         AND F.forecast_date + INTERVAL '1 hour' * lead";
                
                if (sourceTwo.conditions().hasOffset())
                {
                    script += " + INTERVAL '1 hour' * " + sourceTwo.conditions().getOffset();
                }
                
                script += " >= " + sourceTwo.conditions().getEarliestDate() + "     -- Only select the values whose date is on or after the minimum date" + newline;
            }
            
            if (sourceTwo.conditions().hasLatestDate())
            {
                script += "         AND " + sourceTwo.conditions().getLatestDate() + " >= ";
                script += " F.forecast_date + INTERVAL '1 hour' * lead";
                
                if (sourceTwo.conditions().hasOffset())
                {
                    script += " + INTERVAL '1 hour' * " + sourceTwo.conditions().getOffset();
                }
                
                script += " <= " + sourceTwo.conditions().getLatestDate() + "       -- Only select the values whose date is on or before the maximum date" + newline;
            }
            
            if (sourceTwo.conditions().hasMinimumValue())
            {
                script += "         AND FV.forecasted_value * UC.factor >= " + sourceTwo.conditions().getMinimumValue();
                script += "     -- Only select the values that or greater than or equal to the minimum value" + newline;
            }
            
            if (sourceTwo.conditions().hasMaximumValue())
            {
                script += "         AND FV.forecasted_value * UC.factor <= " + sourceTwo.conditions().getMaximumValue();
                script += "     -- Only select the values that are less than or equal to the maximum value" + newline;
            }
            
            if (!sourceTwo.loadAllEnsembles() && sourceTwo.ensembleCount() > 0)
            {
                script += "         AND FE.ensemble_id = " + sourceTwo.getEnsembleCondition() + "       -- Only select values attached to specific ensembles" + newline;
            }
            
            script += "     GROUP BY F.forecast_date, FV.lead       -- Combine results based on the date of their initial forecast, then on their lead time" + newline;
        }
        else
        {
            script += "O.observation_time";
            
            if (sourceTwo.conditions().hasOffset())
            {
                script += " + INTERVAL '1 hour' * " + sourceTwo.conditions().getOffset();
            }
            
            script += " AS sourceTwoDate,       -- Retrieve the date of the observed value, modified by a suggested offset" + newline;
            script += "         ARRAY[O.observed_value * UC.factor]     -- place the observed value into an array" + newline;
            script += "     FROM wres.Observation O     -- Start by looking into observed values" + newline;
            script += "     INNER JOIN wres.UnitConversion UC       -- Find the unit conversion that will convert from the observed unit" + newline;
            script += "         ON UC.from_unit = O.measurementunit_id      -- Match the observation to the unit conversion based on the unit on the observation" + newline;
            script += "     WHERE UC.to_unit = " + MeasurementCache.getMeasurementUnitID(sourceTwo.getVariable().getUnit());
            script += "     -- Find the conversion factor based on the unit of measurement that we want to convert to" + newline;
            script += "         AND O.variableposition_id = ";
            script += String.valueOf(sourceTwo.getFeature().getVariablePositionIDs(sourceTwo.getVariable().getVariableID()).get(0));
            script += "     -- Select only the observations that correspond to the value of a variable at a specific location" + newline;
            
            if (sourceTwo.conditions().hasEarliestDate())
            {
                script += "         AND O.observation_time + INTERVAL '1 hour' * lead";
                
                if (sourceTwo.conditions().hasOffset())
                {
                    script += " + INTERVAL '1 hour' * " + sourceTwo.conditions().getOffset();
                }
                
                script += " >= " + sourceTwo.conditions().getEarliestDate() + "     -- Only select observations on or after a specific date" + newline;
            }
            
            if (sourceTwo.conditions().hasLatestDate())
            {
                script += "         AND " + sourceTwo.conditions().getLatestDate() + " >= ";
                script += " O.observation_time + INTERVAL '1 hour' * lead";
                
                if (sourceTwo.conditions().hasOffset())
                {
                    script += " + INTERVAL '1 hour' * " + sourceTwo.conditions().getOffset();
                }
                
                script += " <= " + sourceTwo.conditions().getLatestDate() + "       -- Only select observations on or before a specific date" + newline;
            }
            
            if (sourceTwo.conditions().hasMinimumValue())
            {
                script += "         AND O.observed_value * UC.factor >= " + sourceTwo.conditions().getMinimumValue();
                script += "     -- Only select observations that are greater than or equal to a specific minimum value" + newline;
            }
            
            if (sourceTwo.conditions().hasMaximumValue())
            {
                script += "         AND O.observed_value * UC.factor <= " + sourceTwo.conditions().getMaximumValue();
                script += "     -- Only select observations that are less than or equal to a specific maximum value" + newline;
            }
        }
        
        script += ")" + newline;
        script += "SELECT ";
        
        if (sourceOne.isForecast())
        {
            script += "AVG(FV.forecasted_value * UC.factor) AS sourceOneValue,      ";
            script += "-- Aggregate all the values for forecasts at a specific and lead time to match that of the (possibly) multiple values to pair with" + newline;
            script += "     ST.measurements     -- The array of values to pair with" + newline;
            script += "FROM wres.Forecast F     -- Start by identifying forecasts to search through" + newline;
            script += "INNER JOIN wres.ForecastEnsemble FE      -- Match the found forecasts with their ensembles" + newline;
            script += "     ON FE.forecast_id = F.forecast_id       -- Matched based on the generated ID for the forecasts" + newline;
            script += "INNER JOIN wres.ForecastValue FV     -- match the found ensembles for the forecasts with their values" + newline;
            script += "     ON FV.forecastensemble_id = FE.forecastensemble_id      -- Match on the generated IDs for the ensembles" + newline;
            script += "INNER JOIN sourceTwo ST      -- Join with the values returned from the previous query" + newline;
            script += "     ON F.forecast_date + INTERVAL '1 hour' * lead";
            
            if (sourceOne.conditions().hasOffset())
            {
                script += " + INTERVAL '1 hour' * " + sourceOne.conditions().getOffset();
            }
            
            script += " = ST.sourceTwoDate      -- Match the found forecasts with the values from the previous query based on their shared dates" + newline;
            script += "INNER JOIN wres.UnitConversion UC    -- Identify the factor to convert the values for the forecasts to the desired unit of measurement" + newline;
            script += "     ON UC.from_unit = FE.measurementunit_id     -- Match the conversion factor to the measurements by the id of the unit to convert from" + newline;
            script += "WHERE FE.variableposition_id = ";
            script += String.valueOf(sourceOne.getFeature().getVariablePositionIDs(sourceOne.getVariable().getVariableID()).get(0));
            script += "     -- Limit the forecast values to those attached to variable values at specific locations" + newline;
            script += "     AND UC.to_unit = " + MeasurementCache.getMeasurementUnitID(sourceOne.getVariable().getUnit()); 
            script += "     -- Determine the conversion factor based on the unit of measurement we want to convert to" + newline;
            script += "     AND " + this.leads + "        -- The range of Lead Times to select forecasted values from" + newline;
            
            if (sourceOne.conditions().hasEarliestDate())
            {
                script += "         AND F.forecast_date + INTERVAL '1 hour' * lead";
                
                if (sourceOne.conditions().hasOffset())
                {
                    script += " + INTERVAL '1 hour' * " + sourceOne.conditions().getOffset();
                }
                
                script += " >= " + sourceOne.conditions().getEarliestDate() + "     -- Limit the forecasted values to those on or after a minimum date" + newline;
            }
            
            if (sourceOne.conditions().hasLatestDate())
            {
                script += "         AND " + sourceOne.conditions().getLatestDate() + " >= ";
                script += " F.forecast_date + INTERVAL '1 hour' * lead";
                
                if (sourceOne.conditions().hasOffset())
                {
                    script += " + INTERVAL '1 hour' * " + sourceOne.conditions().getOffset();
                }
                
                script += " <= " + sourceOne.conditions().getLatestDate() + "       -- Limit the forecasted values to those on or before a latest date" + newline;
            }
            
            if (sourceOne.conditions().hasMinimumValue())
            {
                script += "     AND FV.forecasted_value * UC.factor >= " + sourceOne.conditions().getMinimumValue();
                script += "     -- Limit the forecasted values to those greater than or equal to a minimum value"+ newline;
            }
            
            if (sourceOne.conditions().hasMaximumValue())
            {
                script += "     AND FV.forecasted_value * UC.factor <= " + sourceOne.conditions().getMaximumValue();
                script += "     -- Limit the forecasted values to those less than or equal to a maximum value" + newline;
            }
            
            if (!sourceOne.loadAllEnsembles() && sourceOne.ensembleCount() > 0)
            {
                script += "     AND FE.ensemble_id = " + sourceOne.getEnsembleCondition() + "       -- Only select values belonging to specific ensembles" + newline;
            }
            
            script += "ORDER BY F.forecast_date, FV.lead    --  Return the results based on the date of their values" + newline;
            
            script += "GROUP BY F.forecast_date + INTERVAL '1 hour' * FV.lead";
            
            if (sourceOne.conditions().hasOffset())
            {
                script += " + INTERVAL '1 hour' * " + sourceOne.conditions().getOffset();
            }
            
            script += "     -- Average the forecasted values by grouping them based on their date" + newline;
        }
        else
        {
            script += "O.observed_value * UC.factor AS sourceOneValue,       ";
            script += "-- Select a single observed value converted to the requested unit of measurement" + newline;
            script += "     ST.measurements     -- Select the values returned from the previous query" + newline;
            script += "FROM wres.Observation O      -- Start by searching through the observed values" + newline;
            script += "INNER JOIN sourceTwo ST      -- Match the observed values with the previous query" + newline;
            script += "     ON O.observation_time";
            
            if (sourceOne.conditions().hasOffset())
            {
                script += " + INTERVAL '1 hour' * " + sourceOne.conditions().getOffset();
            }
            
            script += " = ST.sourceTwoDate      -- match the values based on their dates" + newline;
            script += "INNER JOIN wres.UnitConversion UC        -- Determine the conversion factor" + newline;
            script += "     ON UC.from_unit = O.measurementunit_id      -- Match on the unit to convert from" + newline;
            script += "WHERE UC.to_unit = " + MeasurementCache.getMeasurementUnitID(sourceOne.getVariable().getUnit());
            script += "     -- Find the correct conversion factor based on the unit of measurement to convert to" + newline;
            script += "     AND O.variableposition_id = ";
            script += String.valueOf(sourceOne.getFeature().getVariablePositionIDs(sourceOne.getVariable().getVariableID()).get(0));
            script += "     -- Select only observations from a variable at a specific locations" + newline;
            
            if (sourceOne.conditions().hasEarliestDate())
            {
                script += "     AND O.observation_time + INTERVAL '1 hour' * lead";
                
                if (sourceOne.conditions().hasOffset())
                {
                    script += " + INTERVAL '1 hour' * " + sourceOne.conditions().getOffset();
                }
                
                script += " >= " + sourceOne.conditions().getEarliestDate() + "     -- Limit observations to those on or after a specific date" + newline;
            }
            
            if (sourceOne.conditions().hasLatestDate())
            {
                script += "     AND " + sourceOne.conditions().getLatestDate() + " >= ";
                script += " O.observation_time + INTERVAL '1 hour' * lead";
                
                if (sourceOne.conditions().hasOffset())
                {
                    script += " + INTERVAL '1 hour' * " + sourceOne.conditions().getOffset();
                }
                
                script += " <= " + sourceOne.conditions().getLatestDate() + "       -- Limit observations to those on or before a specific date" + newline;
            }
            
            if (sourceOne.conditions().hasMinimumValue())
            {
                script += "     AND O.observed_value * UC.factor >= " + sourceOne.conditions().getMinimumValue();
                script += "     -- Limit observed values to those greater than or equal to a minimum value" + newline;
            }
            
            if (sourceOne.conditions().hasMaximumValue())
            {
                script += "     AND O.observed_value * UC.factor <= " + sourceOne.conditions().getMaximumValue();
                script += "     -- Limit observed values to those less than or equal to a maximum value" + newline;
            }
            
            script += "ORDER BY O.observation_time      -- Order results based on date (earliest to latest)" + newline;
        }
        
        script += ";        -- Conclude the query";
        
        return script;
    }
    
    private String createSelectScript() throws Exception
    {
        String script = "";
        
        script += "SELECT ";
        
        if (sourceOne.isForecast())
        {
            script += "SO.forecasted_value";
        }
        else
        {
            script += "SO.observed_value";
        }
        
        script += " * SOUC.factor, ";
        
        if (sourceTwo.isForecast())
        {
            script += "ST.forecasted_value";
        }
        else
        {
            script += "ST.observed_value";
        }
        
        script += " * STUC.factor" + newline;
        
        if (sourceOne.isForecast())
        {
            script += "FROM wres.Forecast SOF" + newline;
            script += "INNER JOIN wres.ForecastEnsemble SOFE" + newline;
            script += "     ON SOFE.forecast_id = SOF.forecast_id" + newline;
            script += "INNER JOIN wres.ForecastValue SOFV" + newline;
            script += "     ON SOFV.forecastensemble_id = SOFE.forecastensemble_id" + newline;
            script += "INNER JOIN wres.UnitConversion SOUC" + newline;
            script += "     ON SOUC.from_unit = SOFE.measurementunit_id" + newline;
        }
        else
        {
            script += "FROM wres.Observation SOO" + newline;
            script += "INNER JOIN wres.UnitConversion SOUC" + newline;
            script += "     ON SOUC.from_unit = SOO.measurementunit_id" + newline;
        }
        
        if (sourceOne.isForecast() && sourceTwo.isForecast())
        {
            script += "INNER JOIN wres.ForecastValue STFV" + newline;
            script += "     ON STFV.lead = SOFV.lead" + newline;
            script += "INNER JOIN wres.ForecastEnsemble STFE" + newline;
            script += "     ON STFE.forecastensemble_id = STFV.forecastensemble_id" + newline;
            script += "INNER JOIN wres.Forecast STF" + newline;
            script += "     ON STF.forecast_id = STFE.forecast_id" + newline;
            script += "         AND SOF.forecast_date";
                        
            if (!sourceOne.conditions().getOffset().equalsIgnoreCase("0"))
            {
                script += " + (INTERVAL '1 hour' * " + sourceOne.conditions().getOffset() + ")";
            }
            
            script += " = STF.forecast_date";
            
            if (!sourceTwo.conditions().getOffset().equalsIgnoreCase("0"))
            {
                script += " + (INTERVAL '1 hour' * " + sourceTwo.conditions().getOffset() + ")";
            }
        }
        else if (!sourceOne.isForecast() && sourceTwo.isForecast())
        {
            // TODO: Consult Matt Winther about a better way to join a forecast to an observation,
            // not the other way around
            script += "CROSS JOIN wres.Forecast STF" + newline;
            script += "INNER JOIN wres.ForecastEnsemble STFE" + newline;
            script += "     ON STFE.forecast_id = STF.forecast_id" + newline;
            script += "INNER JOIN wres.ForecastValue STFV" + newline;
            script += "     ON STFV.forecastensemble_id = STFE.forecastensemble_id" + newline;
            script += "         AND STF.forecast_date + (INTERVAL '1 hour' * STFV.lead)";
            
            if (!sourceTwo.conditions().getOffset().equalsIgnoreCase("0"))
            {
                script += " + (INTERVAL '1 hour' * " + sourceTwo.conditions().getOffset() + ")";
            }
            
            script += " = SOO.observation_time";
            
            if (!sourceOne.conditions().getOffset().equalsIgnoreCase("0"))
            {
                script += " + (INTERVAL '1 hour' * " + sourceOne.conditions().getOffset() + ")";
            }
        }
        else
        {
            script += "INNER JOIN wres.Observation STO" + newline;
            script += "     STO.observation_time";
            
            if (!sourceTwo.conditions().getOffset().equalsIgnoreCase("0"))
            {
                script += " + INTERVAL '1 hour' * " + sourceTwo.conditions().getOffset();
            }
            
            script += " = SOF.forecast_date + (INTERVAL '1 hour' * SOFV.lead)";
            
            if (!sourceOne.conditions().getOffset().equalsIgnoreCase("0"))
            {
                script += " + (INTERVAL '1 hour' * " + sourceOne.conditions().getOffset() + ")";
            }
        }
        
        script += newline + "WHERE ";
        
        if (sourceOne.isForecast())
        {
            script += "SOFE.variableposition_id = " + String.valueOf(sourceOne.getFeature()
                                                                              .getVariablePositionIDs(sourceOne.getVariable()
                                                                                                               .getVariableID())
                                                                              .get(0));
            script += newline;
            
            if (!sourceOne.loadAllEnsembles())
            {
                script += "     SOFE.ensemble_id = " + sourceOne.getEnsembleCondition() + newline;;
            }
            
            if (sourceOne.conditions().hasMinimumValue())
            {
                script += "     SOFV.forecasted_value * SOUC.factor >= " + sourceOne.conditions().getMinimumValue() + newline;
            }
            
            if (sourceOne.conditions().hasMaximumValue())
            {
                script += "     SOFV.forecasted_value * SOUC.factor <= " + sourceOne.conditions().getMaximumValue() + newline;
            }
            
            if (sourceOne.conditions().hasEarliestDate())
            {
                script += "     (SOF.forecast_date + (INTERVAL '1 hour' * SOFV.lead)) >= " + sourceOne.conditions().getEarliestDate() + newline;
            }
            
            if (sourceOne.conditions().hasLatestDate())
            {
                script += "     (SOF.forecast_date + (INTERVAL '1 hour' * SOFV.lead)) <= " + sourceOne.conditions().getLatestDate() + newline;
            }
        }
        else
        {
            script += "SOO.variableposition_id";
        }
        
        return script;
    }

    private final ProjectDataSource sourceOne;
    private final ProjectDataSource sourceTwo;
    private final String leads;
}
