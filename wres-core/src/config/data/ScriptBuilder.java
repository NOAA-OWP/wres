/**
 * 
 */
package config.data;

import collections.TwoTuple;
import data.caching.MeasurementCache;

/**
 * @author Christopher Tubbs
 *
 */
public final class ScriptBuilder
{
    private static String newline = System.lineSeparator();

    /**
     * Private constructor; This should not be initialized since it is merely
     * a store for functions
     */
    private ScriptBuilder() {}

    public static TwoTuple<String, String> generateFindLastLead(int variableID) {
        final String label = "last_lead";
        String script = "";

        script += "SELECT FV.lead AS " + label + newline;
        script += "FROM wres.VariablePosition VP" + newline;
        script += "INNER JOIN wres.ForecastEnsemble FE" + newline;
        script += "    ON FE.variableposition_id = VP.variableposition_id" + newline;
        script += "INNER JOIN wres.ForecastValue FV" + newline;
        script += "    ON FV.forecastensemble_id = FE.forecastensemble_id" + newline;
        script += "WHERE VP.variable_id = " + variableID + newline;
        script += "ORDER BY FV.lead DESC" + newline;
        script += "LIMIT 1;";
        
        return new TwoTuple<String, String>(script, label);
    }
    
    public static String generateGetPairData(Metric metricSpecification, int progress) throws Exception {
        // TODO: Break into multiple functions
        
        // Expose members of the specification to reduce the depth of data accessors
        ProjectDataSource firstSourceSpec = metricSpecification.getFirstSource();
        ProjectDataSource secondSourceSpec = metricSpecification.getSecondSource();
        
        String leadSpecification = metricSpecification.getAggregationSpecification().getLeadQualifier(progress);
        
        String script = "";
        script +=   "WITH sourceTwo AS      -- The CTE that produces the array for the second source" + newline;
        script +=   "(" + newline;
        script +=   "   SELECT ";
        
        if (secondSourceSpec.isForecast())
        {
            script += "F.forecast_date + INTERVAL '1 hour' * lead";
            
            if (secondSourceSpec.getTimeOffset() != null)
            {
                script += " + (INTERVAL '1 hour' * " + secondSourceSpec.conditions().getOffset() + ")";
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
            script += "     WHERE " + leadSpecification + "        -- Select the values attached to the lead time specification passed into the thread" + newline;
            script += "         AND FE.variableposition_id = ";
            script += String.valueOf(secondSourceSpec.getFirstVariablePositionID());
            script += "     -- Select the ensembles for variable values at a specific location" + newline;
            script += "         AND UC.to_unit = " + MeasurementCache.getMeasurementUnitID(secondSourceSpec.getMeasurementUnit());
            script += "     -- Determine to unit conversion based on the specification's indication of the desired unit to convert to"+ newline;
            
            if (secondSourceSpec.getEarliestDate() != null)
            {
                script += "         AND F.forecast_date + INTERVAL '1 hour' * lead";
                
                if (secondSourceSpec.getTimeOffset() != null)
                {
                    script += " + INTERVAL '1 hour' * " + secondSourceSpec.getTimeOffset();
                }
                
                script += " >= " + secondSourceSpec.getEarliestDate() + "     -- Only select the values whose date is on or after the minimum date" + newline;
            }
            
            if (secondSourceSpec.getLatestDate() != null)
            {
                script += "         AND " + secondSourceSpec.getLatestDate() + " >= ";
                script += " F.forecast_date + INTERVAL '1 hour' * lead";
                
                if (secondSourceSpec.getTimeOffset() != null)
                {
                    script += " + INTERVAL '1 hour' * " + secondSourceSpec.getTimeOffset();
                }
                
                script += " <= " + secondSourceSpec.getLatestDate() + "       -- Only select the values whose date is on or before the maximum date" + newline;
            }
            
            if (secondSourceSpec.getMinimumValue() != null)
            {
                script += "         AND FV.forecasted_value * UC.factor >= " + secondSourceSpec.getMinimumValue();
                script += "     -- Only select the values that or greater than or equal to the minimum value" + newline;
            }
            
            if (secondSourceSpec.getMaximumValue() != null)
            {
                script += "         AND FV.forecasted_value * UC.factor <= " + secondSourceSpec.getMaximumValue();
                script += "     -- Only select the values that are less than or equal to the maximum value" + newline;
            }
            
            if (!secondSourceSpec.loadAllEnsembles() && secondSourceSpec.ensembleCount() > 0)
            {
                script += "         AND FE.ensemble_id = " + secondSourceSpec.getEnsembleCondition() + "       -- Only select values attached to specific ensembles" + newline;
            }
            
            script += "     GROUP BY F.forecast_date, FV.lead       -- Combine results based on the date of their initial forecast, then on their lead time" + newline;
        }
        else
        {
            script += "O.observation_time";
            
            if (secondSourceSpec.getTimeOffset() != null)
            {
                script += " + INTERVAL '1 hour' * " + secondSourceSpec.getTimeOffset();
            }
            
            script += " AS sourceTwoDate,       -- Retrieve the date of the observed value, modified by a suggested offset" + newline;
            script += "         ARRAY[O.observed_value * UC.factor]     -- place the observed value into an array" + newline;
            script += "     FROM wres.Observation O     -- Start by looking into observed values" + newline;
            script += "     INNER JOIN wres.UnitConversion UC       -- Find the unit conversion that will convert from the observed unit" + newline;
            script += "         ON UC.from_unit = O.measurementunit_id      -- Match the observation to the unit conversion based on the unit on the observation" + newline;
            script += "     WHERE UC.to_unit = " + MeasurementCache.getMeasurementUnitID(secondSourceSpec.getMeasurementUnit());
            script += "     -- Find the conversion factor based on the unit of measurement that we want to convert to" + newline;
            script += "         AND O.variableposition_id = ";
            script += String.valueOf(secondSourceSpec.getFirstVariablePositionID());
            script += "     -- Select only the observations that correspond to the value of a variable at a specific location" + newline;
            
            if (secondSourceSpec.getEarliestDate() != null)
            {
                script += "         AND O.observation_time + INTERVAL '1 hour' * lead";
                
                if (secondSourceSpec.getTimeOffset() != null)
                {
                    script += " + INTERVAL '1 hour' * " + secondSourceSpec.getTimeOffset();
                }
                
                script += " >= " + secondSourceSpec.getEarliestDate() + "     -- Only select observations on or after a specific date" + newline;
            }
            
            if (secondSourceSpec.getLatestDate() != null)
            {
                script += "         AND " + secondSourceSpec.getLatestDate() + " >= ";
                script += " O.observation_time + INTERVAL '1 hour' * lead";
                
                if (secondSourceSpec.getTimeOffset() != null)
                {
                    script += " + INTERVAL '1 hour' * " + secondSourceSpec.getTimeOffset();
                }
                
                script += " <= " + secondSourceSpec.getLatestDate() + "       -- Only select observations on or before a specific date" + newline;
            }
            
            if (secondSourceSpec.getMinimumValue() != null)
            {
                script += "         AND O.observed_value * UC.factor >= " + secondSourceSpec.getMinimumValue();
                script += "     -- Only select observations that are greater than or equal to a specific minimum value" + newline;
            }
            
            if (secondSourceSpec.getMaximumValue() != null)
            {
                script += "         AND O.observed_value * UC.factor <= " + secondSourceSpec.getMaximumValue();
                script += "     -- Only select observations that are less than or equal to a specific maximum value" + newline;
            }
        }
        
        script += ")" + newline;
        script += "SELECT ";
        
        if (firstSourceSpec.isForecast())
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
            
            if (firstSourceSpec.getTimeOffset() != null)
            {
                script += " + INTERVAL '1 hour' * " + firstSourceSpec.getTimeOffset();
            }
            
            script += " = ST.sourceTwoDate      -- Match the found forecasts with the values from the previous query based on their shared dates" + newline;
            script += "INNER JOIN wres.UnitConversion UC    -- Identify the factor to convert the values for the forecasts to the desired unit of measurement" + newline;
            script += "     ON UC.from_unit = FE.measurementunit_id     -- Match the conversion factor to the measurements by the id of the unit to convert from" + newline;
            script += "WHERE FE.variableposition_id = ";
            script += String.valueOf(firstSourceSpec.getFirstVariablePositionID());
            script += "     -- Limit the forecast values to those attached to variable values at specific locations" + newline;
            script += "     AND UC.to_unit = " + MeasurementCache.getMeasurementUnitID(firstSourceSpec.getMeasurementUnit()); 
            script += "     -- Determine the conversion factor based on the unit of measurement we want to convert to" + newline;
            script += "     AND " + leadSpecification + "        -- The range of Lead Times to select forecasted values from" + newline;
            
            if (firstSourceSpec.getEarliestDate() != null)
            {
                script += "         AND F.forecast_date + INTERVAL '1 hour' * lead";
                
                if (firstSourceSpec.getTimeOffset() != null)
                {
                    script += " + INTERVAL '1 hour' * " + firstSourceSpec.getTimeOffset();
                }
                
                script += " >= " + firstSourceSpec.getEarliestDate() + "     -- Limit the forecasted values to those on or after a minimum date" + newline;
            }
            
            if (firstSourceSpec.getLatestDate() != null)
            {
                script += "         AND " + firstSourceSpec.getLatestDate() + " >= ";
                script += " F.forecast_date + INTERVAL '1 hour' * lead";
                
                if (firstSourceSpec.getTimeOffset() != null)
                {
                    script += " + INTERVAL '1 hour' * " + firstSourceSpec.getTimeOffset();
                }
                
                script += " <= " + firstSourceSpec.getLatestDate() + "       -- Limit the forecasted values to those on or before a latest date" + newline;
            }
            
            if (firstSourceSpec.getMinimumValue() != null)
            {
                script += "     AND FV.forecasted_value * UC.factor >= " + firstSourceSpec.conditions().getMinimumValue();
                script += "     -- Limit the forecasted values to those greater than or equal to a minimum value"+ newline;
            }
            
            if (firstSourceSpec.getMaximumValue() != null)
            {
                script += "     AND FV.forecasted_value * UC.factor <= " + firstSourceSpec.getMaximumValue();
                script += "     -- Limit the forecasted values to those less than or equal to a maximum value" + newline;
            }
            
            if (!firstSourceSpec.loadAllEnsembles() && firstSourceSpec.ensembleCount() > 0)
            {
                script += "     AND FE.ensemble_id = " + firstSourceSpec.getEnsembleCondition() + "       -- Only select values belonging to specific ensembles" + newline;
            }
            
            script += "ORDER BY F.forecast_date, FV.lead    --  Return the results based on the date of their values" + newline;
            
            script += "GROUP BY F.forecast_date + INTERVAL '1 hour' * FV.lead";
            
            if (firstSourceSpec.getTimeOffset() != null)
            {
                script += " + INTERVAL '1 hour' * " + firstSourceSpec.getTimeOffset();
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
            
            if (firstSourceSpec.getTimeOffset() != null)
            {
                script += " + INTERVAL '1 hour' * " + firstSourceSpec.getTimeOffset();
            }
            
            script += " = ST.sourceTwoDate      -- match the values based on their dates" + newline;
            script += "INNER JOIN wres.UnitConversion UC        -- Determine the conversion factor" + newline;
            script += "     ON UC.from_unit = O.measurementunit_id      -- Match on the unit to convert from" + newline;
            script += "WHERE UC.to_unit = " + MeasurementCache.getMeasurementUnitID(firstSourceSpec.getMeasurementUnit());
            script += "     -- Find the correct conversion factor based on the unit of measurement to convert to" + newline;
            script += "     AND O.variableposition_id = ";
            script += String.valueOf(firstSourceSpec.getFirstVariablePositionID());
            script += "     -- Select only observations from a variable at a specific locations" + newline;
            
            if (firstSourceSpec.getEarliestDate() != null)
            {
                script += "     AND O.observation_time + INTERVAL '1 hour' * lead";
                
                if (firstSourceSpec.getTimeOffset() != null)
                {
                    script += " + INTERVAL '1 hour' * " + firstSourceSpec.getTimeOffset();
                }
                
                script += " >= " + firstSourceSpec.getEarliestDate() + "     -- Limit observations to those on or after a specific date" + newline;
            }
            
            if (firstSourceSpec.getLatestDate() != null)
            {
                script += "     AND " + firstSourceSpec.getLatestDate() + " >= ";
                script += " O.observation_time + INTERVAL '1 hour' * lead";
                
                if (firstSourceSpec.getTimeOffset() != null)
                {
                    script += " + INTERVAL '1 hour' * " + firstSourceSpec.getTimeOffset();
                }
                
                script += " <= " + firstSourceSpec.getLatestDate() + "       -- Limit observations to those on or before a specific date" + newline;
            }
            
            if (firstSourceSpec.getMinimumValue() != null)
            {
                script += "     AND O.observed_value * UC.factor >= " + firstSourceSpec.getMinimumValue();
                script += "     -- Limit observed values to those greater than or equal to a minimum value" + newline;
            }
            
            if (firstSourceSpec.getMaximumValue() != null)
            {
                script += "     AND O.observed_value * UC.factor <= " + firstSourceSpec.getMaximumValue();
                script += "     -- Limit observed values to those less than or equal to a maximum value" + newline;
            }
            
            script += "ORDER BY O.observation_time      -- Order results based on date (earliest to latest)" + newline;
        }
        
        return "SELECT * FROM (" + script + ") AS pairs;";
    }
}
