/**
 * 
 */
package wres.io.config.specification;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.EnsembleCondition;
import wres.config.generated.ProjectConfig;
import wres.io.config.ConfigHelper;
import wres.io.data.caching.Ensembles;
import wres.io.data.caching.MeasurementUnits;
import wres.io.data.caching.Variables;
import wres.io.grouping.LabeledScript;
import wres.util.NotImplementedException;

import java.sql.SQLException;
import java.util.InvalidPropertiesFormatException;
import java.util.StringJoiner;

/**
 * @author Christopher Tubbs
 *
 */
public final class ScriptFactory
{
    private ScriptFactory(){}
    
    private static final String NEWLINE = System.lineSeparator();

    public static LabeledScript generateFindLastLead(int variableID) {
        final String label = "last_lead";
        String script = "";

        script += "SELECT FV.lead AS " + label + NEWLINE;
        script += "FROM wres.VariablePosition VP" + NEWLINE;
        script += "INNER JOIN wres.ForecastEnsemble FE" + NEWLINE;
        script += "    ON FE.variableposition_id = VP.variableposition_id" + NEWLINE;
        script += "INNER JOIN wres.ForecastValue FV" + NEWLINE;
        script += "    ON FV.forecastensemble_id = FE.forecastensemble_id" + NEWLINE;
        script += "WHERE VP.variable_id = " + variableID + NEWLINE;
        script += "ORDER BY FV.lead DESC" + NEWLINE;
        script += "LIMIT 1;";
        
        return new LabeledScript(label, script);
    }
    
    public static String generateGetPairData(MetricSpecification metricSpecification, int progress) throws Exception {
        // TODO: Break into multiple functions
        
        // Expose members of the specification to reduce the depth of data accessors
        ProjectDataSpecification firstSourceSpec = metricSpecification.getFirstSource();
        ProjectDataSpecification secondSourceSpec = metricSpecification.getSecondSource();
        
        Integer firstDesiredMeasurementUnitID;
        Integer secondDesiredMeasurementUnitID;
        
        if (metricSpecification.getDesiredMeasurementUnit() == null)
        {
            firstDesiredMeasurementUnitID = MeasurementUnits.getMeasurementUnitID(firstSourceSpec.getMeasurementUnit());
            secondDesiredMeasurementUnitID = MeasurementUnits.getMeasurementUnitID(secondSourceSpec.getMeasurementUnit());
        }
        else
        {
            firstDesiredMeasurementUnitID = MeasurementUnits.getMeasurementUnitID(metricSpecification.getDesiredMeasurementUnit());
            secondDesiredMeasurementUnitID = firstDesiredMeasurementUnitID;
        }
        
        String leadSpecification = metricSpecification.getAggregationSpecification().getLeadQualifier(progress);
        
        String script = "";
        script +=   "WITH sourceTwo AS      -- The CTE that produces the array for the second source" + NEWLINE;
        script +=   "(" + NEWLINE;
        script +=   "   SELECT ";
        
        if (secondSourceSpec.isForecast())
        {
            script += "F.forecast_date + INTERVAL '1 hour' * lead";
            
            if (secondSourceSpec.getTimeOffset() != null)
            {
                script += " + (INTERVAL '1 hour' * " + secondSourceSpec.conditions().getOffset() + ")";
            }
            
            script += " AS sourceTwoDate,       -- The date to match the first source's" + NEWLINE;
            script += "         array_agg(FV.forecasted_value * UC.factor) AS measurements      ";
            script += "-- Array consisting of each ensemble member corresponding to a lead time from a forecast" + NEWLINE;
            script += "     FROM wres.Forecast F        -- Start by selecting from the available forecasts" + NEWLINE;
            script += "     INNER JOIN wres.ForecastEnsemble FE     -- Retrieve all of the ensembles for the forecasts from above" + NEWLINE;
            script += "         ON F.forecast_id = FE.forecast_id       -- Match on the generated identifier for the forecast" + NEWLINE;
            script += "     INNER JOIN wres.ForecastValue FV        -- Retrieve the values for all of the retrieved ensembles" + NEWLINE;
            script += "         ON FV.forecastensemble_id = FE.forecastensemble_id      -- Match on the generated identifier for the ensemble matched with the forecast" + NEWLINE;
            script += "     INNER JOIN wres.UnitConversion UC       -- Retrieve the conversion factor to convert the value with the ensemble's unit to the desired unit" + NEWLINE;
            script += "         ON UC.from_unit = FE.measurementunit_id     ";
            script += "-- The conversion factor will be obtained by matching the unit from the ensemble with the factor's unit to convert from" + NEWLINE;
            script += "     WHERE " + leadSpecification + "        -- Select the values attached to the lead time specification passed into the thread" + NEWLINE;
            script += "         AND FE.variableposition_id = ";
            script += String.valueOf(secondSourceSpec.getFirstVariablePositionID());
            script += "     -- Select the ensembles for variable values at a specific location" + NEWLINE;
            script += "         AND UC.to_unit = " + secondDesiredMeasurementUnitID;
            script += "     -- Determine to unit conversion based on the specification's indication of the desired unit to convert to"+ NEWLINE;
            
            if (secondSourceSpec.getEarliestDate() != null)
            {
                script += "         AND F.forecast_date + INTERVAL '1 hour' * lead";
                
                if (secondSourceSpec.getTimeOffset() != null)
                {
                    script += " + INTERVAL '1 hour' * " + secondSourceSpec.getTimeOffset();
                }
                
                script += " >= " + secondSourceSpec.getEarliestDate() + "     -- Only select the values whose date is on or after the minimum date" + NEWLINE;
            }
            
            if (secondSourceSpec.getLatestDate() != null)
            {
                script += "         AND " + secondSourceSpec.getLatestDate() + " >= ";
                script += " F.forecast_date + INTERVAL '1 hour' * lead";
                
                if (secondSourceSpec.getTimeOffset() != null)
                {
                    script += " + INTERVAL '1 hour' * " + secondSourceSpec.getTimeOffset();
                }
                
                script += " <= " + secondSourceSpec.getLatestDate() + "       -- Only select the values whose date is on or before the maximum date" + NEWLINE;
            }
            
            if (secondSourceSpec.getMinimumValue() != null)
            {
                script += "         AND FV.forecasted_value * UC.factor >= " + secondSourceSpec.getMinimumValue();
                script += "     -- Only select the values that or greater than or equal to the minimum value" + NEWLINE;
            }
            
            if (secondSourceSpec.getMaximumValue() != null)
            {
                script += "         AND FV.forecasted_value * UC.factor <= " + secondSourceSpec.getMaximumValue();
                script += "     -- Only select the values that are less than or equal to the maximum value" + NEWLINE;
            }
            
            if (!secondSourceSpec.loadAllEnsembles() && secondSourceSpec.ensembleCount() > 0)
            {
                script += "         AND FE.ensemble_id = " + secondSourceSpec.getEnsembleCondition() + "       -- Only select values attached to specific ensembles" + NEWLINE;
            }
            
            script += "     GROUP BY F.forecast_date, FV.lead       -- Combine results based on the date of their initial forecast, then on their lead time" + NEWLINE;
        }
        else
        {
            script += "O.observation_time";
            
            if (secondSourceSpec.getTimeOffset() != null)
            {
                script += " + INTERVAL '1 hour' * " + secondSourceSpec.getTimeOffset();
            }
            
            script += " AS sourceTwoDate,       -- Retrieve the date of the observed value, modified by a suggested offset" + NEWLINE;
            script += "         ARRAY[O.observed_value * UC.factor]     -- place the observed value into an array" + NEWLINE;
            script += "     FROM wres.Observation O     -- Start by looking into observed values" + NEWLINE;
            script += "     INNER JOIN wres.UnitConversion UC       -- Find the unit conversion that will convert from the observed unit" + NEWLINE;
            script += "         ON UC.from_unit = O.measurementunit_id      -- Match the observation to the unit conversion based on the unit on the observation" + NEWLINE;
            script += "     WHERE UC.to_unit = " + secondDesiredMeasurementUnitID;
            script += "     -- Find the conversion factor based on the unit of measurement that we want to convert to" + NEWLINE;
            script += "         AND O.variableposition_id = ";
            script += String.valueOf(secondSourceSpec.getFirstVariablePositionID());
            script += "     -- Select only the observations that correspond to the value of a variable at a specific location" + NEWLINE;
            
            if (secondSourceSpec.getEarliestDate() != null)
            {
                script += "         AND O.observation_time + INTERVAL '1 hour' * lead";
                
                if (secondSourceSpec.getTimeOffset() != null)
                {
                    script += " + INTERVAL '1 hour' * " + secondSourceSpec.getTimeOffset();
                }
                
                script += " >= " + secondSourceSpec.getEarliestDate() + "     -- Only select observations on or after a specific date" + NEWLINE;
            }
            
            if (secondSourceSpec.getLatestDate() != null)
            {
                script += "         AND " + secondSourceSpec.getLatestDate() + " >= ";
                script += " O.observation_time + INTERVAL '1 hour' * lead";
                
                if (secondSourceSpec.getTimeOffset() != null)
                {
                    script += " + INTERVAL '1 hour' * " + secondSourceSpec.getTimeOffset();
                }
                
                script += " <= " + secondSourceSpec.getLatestDate() + "       -- Only select observations on or before a specific date" + NEWLINE;
            }
            
            if (secondSourceSpec.getMinimumValue() != null)
            {
                script += "         AND O.observed_value * UC.factor >= " + secondSourceSpec.getMinimumValue();
                script += "     -- Only select observations that are greater than or equal to a specific minimum value" + NEWLINE;
            }
            
            if (secondSourceSpec.getMaximumValue() != null)
            {
                script += "         AND O.observed_value * UC.factor <= " + secondSourceSpec.getMaximumValue();
                script += "     -- Only select observations that are less than or equal to a specific maximum value" + NEWLINE;
            }
        }
        
        script += ")" + NEWLINE;
        script += "SELECT ";
        
        if (firstSourceSpec.isForecast())
        {
            script += "AVG(FV.forecasted_value * UC.factor) AS sourceOneValue,      ";
            script += "-- Aggregate all the values for forecasts at a specific and lead time to match that of the (possibly) multiple values to pair with" + NEWLINE;
            script += "     ST.measurements     -- The array of values to pair with" + NEWLINE;
            script += "FROM wres.Forecast F     -- Start by identifying forecasts to search through" + NEWLINE;
            script += "INNER JOIN wres.ForecastEnsemble FE      -- Match the found forecasts with their ensembles" + NEWLINE;
            script += "     ON FE.forecast_id = F.forecast_id       -- Matched based on the generated ID for the forecasts" + NEWLINE;
            script += "INNER JOIN wres.ForecastValue FV     -- match the found ensembles for the forecasts with their values" + NEWLINE;
            script += "     ON FV.forecastensemble_id = FE.forecastensemble_id      -- Match on the generated IDs for the ensembles" + NEWLINE;
            script += "INNER JOIN sourceTwo ST      -- Join with the values returned from the previous query" + NEWLINE;
            script += "     ON F.forecast_date + INTERVAL '1 hour' * lead";
            
            if (firstSourceSpec.getTimeOffset() != null)
            {
                script += " + INTERVAL '1 hour' * " + firstSourceSpec.getTimeOffset();
            }
            
            script += " = ST.sourceTwoDate      -- Match the found forecasts with the values from the previous query based on their shared dates" + NEWLINE;
            script += "INNER JOIN wres.UnitConversion UC    -- Identify the factor to convert the values for the forecasts to the desired unit of measurement" + NEWLINE;
            script += "     ON UC.from_unit = FE.measurementunit_id     -- Match the conversion factor to the measurements by the id of the unit to convert from" + NEWLINE;
            script += "WHERE FE.variableposition_id = ";
            script += String.valueOf(firstSourceSpec.getFirstVariablePositionID());
            script += "     -- Limit the forecast values to those attached to variable values at specific locations" + NEWLINE;
            script += "     AND UC.to_unit = " + firstDesiredMeasurementUnitID; 
            script += "     -- Determine the conversion factor based on the unit of measurement we want to convert to" + NEWLINE;
            script += "     AND " + leadSpecification + "        -- The range of Lead Times to select forecasted values from" + NEWLINE;
            
            if (firstSourceSpec.getEarliestDate() != null)
            {
                script += "         AND F.forecast_date + INTERVAL '1 hour' * lead";
                
                if (firstSourceSpec.getTimeOffset() != null)
                {
                    script += " + INTERVAL '1 hour' * " + firstSourceSpec.getTimeOffset();
                }
                
                script += " >= " + firstSourceSpec.getEarliestDate() + "     -- Limit the forecasted values to those on or after a minimum date" + NEWLINE;
            }
            
            if (firstSourceSpec.getLatestDate() != null)
            {
                script += "         AND " + firstSourceSpec.getLatestDate() + " >= ";
                script += " F.forecast_date + INTERVAL '1 hour' * lead";
                
                if (firstSourceSpec.getTimeOffset() != null)
                {
                    script += " + INTERVAL '1 hour' * " + firstSourceSpec.getTimeOffset();
                }
                
                script += " <= " + firstSourceSpec.getLatestDate() + "       -- Limit the forecasted values to those on or before a latest date" + NEWLINE;
            }
            
            if (firstSourceSpec.getMinimumValue() != null)
            {
                script += "     AND FV.forecasted_value * UC.factor >= " + firstSourceSpec.conditions().getMinimumValue();
                script += "     -- Limit the forecasted values to those greater than or equal to a minimum value"+ NEWLINE;
            }
            
            if (firstSourceSpec.getMaximumValue() != null)
            {
                script += "     AND FV.forecasted_value * UC.factor <= " + firstSourceSpec.getMaximumValue();
                script += "     -- Limit the forecasted values to those less than or equal to a maximum value" + NEWLINE;
            }
            
            if (!firstSourceSpec.loadAllEnsembles() && firstSourceSpec.ensembleCount() > 0)
            {
                script += "     AND FE.ensemble_id = " + firstSourceSpec.getEnsembleCondition() + "       -- Only select values belonging to specific ensembles" + NEWLINE;
            }
            
            script += "GROUP BY F.forecast_date + INTERVAL '1 hour' * FV.lead";
            
            if (firstSourceSpec.getTimeOffset() != null)
            {
                script += " + INTERVAL '1 hour' * " + firstSourceSpec.getTimeOffset();
            }
            
            script += "     -- Average the forecasted values by grouping them based on their date" + NEWLINE;
        }
        else
        {
            script += "O.observed_value * UC.factor AS sourceOneValue,       ";
            script += "-- Select a single observed value converted to the requested unit of measurement" + NEWLINE;
            script += "     ST.measurements     -- Select the values returned from the previous query" + NEWLINE;
            script += "FROM wres.Observation O      -- Start by searching through the observed values" + NEWLINE;
            script += "INNER JOIN sourceTwo ST      -- Match the observed values with the previous query" + NEWLINE;
            script += "     ON O.observation_time";
            
            if (firstSourceSpec.getTimeOffset() != null)
            {
                script += " + INTERVAL '1 hour' * " + firstSourceSpec.getTimeOffset();
            }
            
            script += " = ST.sourceTwoDate      -- match the values based on their dates" + NEWLINE;
            script += "INNER JOIN wres.UnitConversion UC        -- Determine the conversion factor" + NEWLINE;
            script += "     ON UC.from_unit = O.measurementunit_id      -- Match on the unit to convert from" + NEWLINE;
            script += "WHERE UC.to_unit = " + firstDesiredMeasurementUnitID;
            script += "     -- Find the correct conversion factor based on the unit of measurement to convert to" + NEWLINE;
            script += "     AND O.variableposition_id = ";
            script += String.valueOf(firstSourceSpec.getFirstVariablePositionID());
            script += "     -- Select only observations from a variable at a specific locations" + NEWLINE;
            
            if (firstSourceSpec.getEarliestDate() != null)
            {
                script += "     AND O.observation_time";
                
                if (firstSourceSpec.getTimeOffset() != null)
                {
                    script += " + INTERVAL '1 hour' * " + firstSourceSpec.getTimeOffset();
                }
                
                script += " >= " + firstSourceSpec.getEarliestDate() + "     -- Limit observations to those on or after a specific date" + NEWLINE;
            }
            
            if (firstSourceSpec.getLatestDate() != null)
            {
                script += "     AND " + firstSourceSpec.getLatestDate() + " >= ";
                script += " O.observation_time";
                
                if (firstSourceSpec.getTimeOffset() != null)
                {
                    script += " + INTERVAL '1 hour' * " + firstSourceSpec.getTimeOffset();
                }
                
                script += " <= " + firstSourceSpec.getLatestDate() + "       -- Limit observations to those on or before a specific date" + NEWLINE;
            }
            
            if (firstSourceSpec.getMinimumValue() != null)
            {
                script += "     AND O.observed_value * UC.factor >= " + firstSourceSpec.getMinimumValue();
                script += "     -- Limit observed values to those greater than or equal to a minimum value" + NEWLINE;
            }
            
            if (firstSourceSpec.getMaximumValue() != null)
            {
                script += "     AND O.observed_value * UC.factor <= " + firstSourceSpec.getMaximumValue();
                script += "     -- Limit observed values to those less than or equal to a maximum value" + NEWLINE;
            }
        }
        
        return "SELECT * FROM (" + script + ") AS pairs;";
    }

    public static String generateGetPairData(final ProjectConfig projectConfig, final int progress) throws NotImplementedException {
        StringBuilder script = new StringBuilder("SELECT * FROM (");

        DataSourceConfig leftSource = projectConfig.getInputs().getLeft();
        DataSourceConfig rightSource = projectConfig.getInputs().getRight();


        try {
            Integer leftVariableId = Variables.getVariableID(leftSource.getVariable().getValue(), leftSource.getVariable().getUnit());
            Integer rightVariableId = Variables.getVariableID(rightSource.getVariable().getValue(), rightSource.getVariable().getUnit());

            Integer desiredMeasurementID = MeasurementUnits.getMeasurementUnitID(projectConfig.getPair().getUnit());
            Double minimumValue = null;
            Double maximumValue = null;
            String earliestDate = null;
            String latestDate = null;
            String earliestIssueDate = null;
            String latestIssueDate = null;
            String rightDate = null;

            String leftVariablePositionClause = ConfigHelper.getVariablePositionClause(projectConfig
                                                                                               .getConditions()
                                                                                               .getFeature()
                                                                                               .get(0),
                                                                                       leftVariableId);
            String rightVariablePositionClause = ConfigHelper.getVariablePositionClause(projectConfig
                                                                                                .getConditions()
                                                                                                .getFeature()
                                                                                                .get(0),
                                                                                        rightVariableId);;
            Integer leftTimeShift = null;
            Integer rightTimeShift = null;

            if (projectConfig.getConditions().getValues() != null)
            {
                if (projectConfig.getConditions().getValues().getMinimum() != null)
                {
                    minimumValue = projectConfig.getConditions().getValues().getMinimum();
                }

                if (projectConfig.getConditions().getValues().getMaximum() != null)
                {
                    maximumValue = projectConfig.getConditions().getValues().getMaximum();
                }
            }

            if (projectConfig.getConditions().getDates() != null)
            {
                if (projectConfig.getConditions().getDates().getEarliest() != null)
                {
                    earliestDate = "'" + projectConfig.getConditions().getDates().getEarliest() + "'";
                }

                if (projectConfig.getConditions().getDates().getLatest() != null)
                {
                    latestDate = "'" + projectConfig.getConditions().getDates().getLatest() + "'";
                }
            }

            if (projectConfig.getConditions().getIssuedDates() != null)
            {
                if (projectConfig.getConditions().getIssuedDates().getEarliest() != null)
                {
                    earliestIssueDate = "'" + projectConfig.getConditions().getIssuedDates().getEarliest() + "'";
                }

                if (projectConfig.getConditions().getIssuedDates().getLatest() != null)
                {
                    latestIssueDate = "'" + projectConfig.getConditions().getIssuedDates().getLatest() + "'";
                }
            }

            if (leftSource.getTimeShift() != null && leftSource.getTimeShift().getWidth() != 0)
            {
                leftTimeShift = leftSource.getTimeShift().getWidth();
            }

            if (rightSource.getTimeShift() != null && rightSource.getTimeShift().getWidth() != 0)
            {
                rightTimeShift = rightSource.getTimeShift().getWidth();
            }

            String leadSpecification = ConfigHelper.getLeadQualifier(projectConfig, progress);

            script.append("WITH sourceTwo AS        -- The CTE that produces the array for the second source").append(NEWLINE);
            script.append("(").append(NEWLINE);
            script.append("     SELECT ");

            if (ConfigHelper.isForecast(rightSource))
            {
                rightDate = "F.forecast_date + INTERVAL '1 hour' * lead";

                if (rightTimeShift != null)
                {
                    rightDate += " + (INTERVAL '1 hour' * " + rightTimeShift + ")";
                }

                script.append(rightDate)
                      .append(" AS sourceTwoDate,       -- The date to match the first source's").append(NEWLINE);
                script.append("         array_agg(FV.forecasted_value * UC.factor) AS measurements  ")
                      .append(" -- Array consisting of each ensemble member corresponding to a lead from a forecast")
                      .append(NEWLINE);
                script.append("     FROM wres.Forecast F    ")
                      .append("-- Start by selecting from the available forecasts")
                      .append(NEWLINE);
                script.append("     INNER JOIN wres.ForecastEnsemble FE     ")
                      .append("-- Retrieve all ensembles for the forecasts from the above")
                      .append(NEWLINE);
                script.append("         ON F.forecast_id = FE.forecast_id").append(NEWLINE);
                script.append("     INNER JOIN wres.ForecastValue FV    ")
                      .append("-- Retrieve the values for all of the retrieved ensembles")
                      .append(NEWLINE);
                script.append("         ON FV.forecastensemble_id = FE.forecastensemble_id")
                      .append(" -- Match on the generated identifierfor the ensemble matched with the forecast")
                      .append(NEWLINE);
                script.append("     INNER JOIN wres.unitConversion UC       ")
                      .append("-- Retrieve the conversion factor to convert the value with the ensemble's unit to the desired unit")
                      .append(NEWLINE);
                script.append("         ON UC.from_unit = FE.measurementunit_id     ")
                      .append("-- The conversion factor will be obtained by matching the unit from the ensemble ")
                      .append("with the factor's unit to convert from")
                      .append(NEWLINE);
                script.append("     WHERE ").append(leadSpecification)
                      .append("         ")
                      .append("-- Select that values attached to the lead time specification")
                      .append(NEWLINE);
                script.append("            AND ").append(rightVariablePositionClause)
                      .append("         ")
                      .append("-- Select only the values that pertain to the specific variable and location")
                      .append(NEWLINE);
                script.append("             AND UC.to_unit = ")
                      .append(desiredMeasurementID)
                      .append("         ")
                      .append("-- Determine the unit conversion by specifying the id of the unit to convert to")
                      .append(NEWLINE);

                if (rightSource.getEnsemble() != null && rightSource.getEnsemble().size() > 0)
                {
                    StringJoiner include =  new StringJoiner(",", "(", ")");;
                    StringJoiner exclude = new StringJoiner(",", "(", ")");

                    int includeCount = 0;
                    int excludeCount = 0;

                    for (EnsembleCondition condition : rightSource.getEnsemble())
                    {
                        if (condition.isExclude())
                        {
                            excludeCount++;
                            exclude.add(String.valueOf(Ensembles.getEnsembleID(condition.getName(),
                                                                               condition.getMemberId(),
                                                                               condition.getQualifier())));
                        }
                        else
                        {
                            includeCount++;
                            include.add(String.valueOf(Ensembles.getEnsembleID(condition.getName(),
                                                                               condition.getMemberId(),
                                                                               condition.getQualifier())));
                        }
                    }

                    if (includeCount > 0)
                    {
                        script.append("         AND FE.ensemble_id IN ")
                              .append(include.toString())
                              .append("             ")
                              .append("-- Only get values from these ensembles")
                              .append(NEWLINE);
                    }

                    if (excludeCount > 0)
                    {
                        script.append("         AND FE.ensemble NOT IN ")
                              .append(exclude.toString())
                              .append("             ")
                              .append("-- Only get values not pertaining to these ensembles")
                              .append(NEWLINE);
                    }
                }

                if (earliestIssueDate != null)
                {
                    script.append("         AND F.forecastDate >= ")
                          .append(earliestIssueDate)
                          .append("        ")
                          .append("-- Only get values that were forecasted on or after this date")
                          .append(NEWLINE);
                }

                if (latestIssueDate != null)
                {
                    script.append("         AND F.forecast_date <= ")
                          .append(latestIssueDate)
                          .append("        ")
                          .append("-- Only retrieve values forecasted on or before this date")
                          .append(NEWLINE);
                }

                if (earliestDate != null)
                {
                    script.append("         AND ").append(rightDate).append(" >= ").append(earliestDate)
                          .append("            ")
                          .append("-- Only retrieve values on or after this date")
                          .append(NEWLINE);
                }

                if (latestDate != null)
                {
                    script.append("         AND ").append(rightDate).append(" <= ").append(latestDate)
                          .append("        ")
                          .append("-- Only retrieve values on or before this date")
                          .append(NEWLINE);
                }

                if (minimumValue != null)
                {
                    script.append("         AND FV.forecasted_value >= ")
                          .append(minimumValue)
                          .append("          ")
                          .append("-- Only get values greater than or equal to this value")
                          .append(NEWLINE);
                }

                if (maximumValue != null)
                {
                    script.append("         AND FV.forecasted_value <= ")
                          .append(maximumValue)
                          .append("         ")
                          .append("-- Only get values less than or equal to this value")
                          .append(NEWLINE);
                }

                script.append("     GROUP BY F.forecast_date, FV.lead")
                      .append("                     ")
                      .append("-- Combine results based on the date and lead time")
                      .append(NEWLINE);
            }
            else
            {
                rightDate = "O.observation_time";

                if (rightTimeShift != null)
                {
                    rightDate += " + INTERVAL '1 hour' * " + rightTimeShift;
                }

                script.append(rightDate).append(" AS sourceTwoDate,")
                      .append("         ")
                      .append("-- Retrieve the date of the observed value, modified by a suggested offset")
                      .append(NEWLINE);
                script.append("         ARRAY[O.observed_value * UC.factor]")
                      .append("         ")
                      .append("-- Place the observed value into an array and convert it to the desired measurement unit")
                      .append(NEWLINE);

                if (earliestDate != null)
                {
                    script.append("         AND ").append(rightDate).append(" >= ").append(earliestDate)
                          .append("            ")
                          .append("-- Only retrieve values on or after this date")
                          .append(NEWLINE);
                }

                if (latestDate != null)
                {
                    script.append("         AND ").append(rightDate).append(" <= ").append(latestDate)
                          .append("        ")
                          .append("-- Only retrieve values on or before this date")
                          .append(NEWLINE);
                }

                if (minimumValue != null)
                {
                    script.append("         AND O.observed_value >= ")
                          .append(minimumValue)
                          .append("         ")
                          .append("-- Only retrieve values greater than or equal to this value")
                          .append(NEWLINE);
                }

                if (maximumValue != null)
                {
                    script.append("         AND O.observed_value <= ")
                          .append(maximumValue)
                          .append("         ")
                          .append("-- Only retrieve values less than or equal to this value")
                          .append(NEWLINE);
                }
            }

            script.append(")").append(NEWLINE);
            script.append("SELECT ");

            if (ConfigHelper.isForecast(leftSource))
            {
                script.append(leftSource.getRollingTimeAggregation().getFunction())
                      .append("(FV.forecasted_value * UC.factor) AS sourceOneValue,")
                      .append("             ")
                      .append("-- Aggregate the results since it is possible that there will be many modeled values")
                      .append(NEWLINE);
                script.append("     ST.measurements")
                      .append("             ")
                      .append("-- The array of values to pair with")
                      .append(NEWLINE);
                script.append("FROM wres.Forecast F")
                      .append("         ")
                      .append("-- Start by identifying forecasts to search through")
                      .append(NEWLINE);
                script.append("INNER JOIN wres.ForecastEnsemble FE")
                      .append("         ")
                      .append("-- Match the found forecasts with their ensembles")
                      .append(NEWLINE);
                script.append("     ON FE.forecast_id = F.forecast_id")
                      .append("             ")
                      .append("-- Match based on the generated ID for the forecast")
                      .append(NEWLINE);
                script.append("INNER JOIN wres.ForecastValue FV")
                      .append("             ")
                      .append("-- Match the found ensembles for the forecasts with their values")
                      .append(NEWLINE);
                script.append("     ON FV.forecastensemble_id = FE.forecastensemble_id")
                      .append("             ")
                      .append("-- Match the found ensembles with the found values on the generated ID for the link between the forecast and ensemble")
                      .append(NEWLINE);
                script.append("INNER JOIN sourceTwo ST")
                      .append("         ")
                      .append("-- Join with the values returned from the previous query")
                      .append(NEWLINE);
                script.append("     ON F.forecast_date + INTERVAL '1 hour' * lead");

                if (leftTimeShift != null)
                {
                    script.append(" + INTERVAL '1 hour' * ").append(leftTimeShift);
                }

                script.append(" = ST.sourceTwoDate          ")
                      .append("-- Match the found forecasts with the values from the previous queries based on their shared dates")
                      .append(NEWLINE);
                script.append("INNER JOIN wres.UnitConversion UC")
                      .append("            ")
                      .append("-- Identify the factor to convert the values for the forecasts to the desired unit of measurement")
                      .append(NEWLINE);
                script.append("     ON UC.from_unit = FE.measurementunit_id")
                      .append("             ")
                      .append("-- Match the conversion factor to the measurements by the id of the unit to convert")
                      .append(NEWLINE);
                script.append("WHERE ").append(leftVariablePositionClause)
                      .append("             ")
                      .append("-- Limit the forecast values to those attached to variable values at specific locations")
                      .append(NEWLINE);
                script.append("     AND ").append(leadSpecification)
                      .append("             ")
                      .append("-- The range of lead times to select forecasted values from")
                      .append(NEWLINE);
                script.append("     AND UC.to_unit = ").append(desiredMeasurementID)
                      .append("             ")
                      .append("-- Determine the conversion factor based on the unit of measurement we want to convert to")
                      .append(NEWLINE);

                if (leftSource.getEnsemble() != null)
                {
                    StringJoiner include = new StringJoiner(",", "(", ")");
                    StringJoiner exclude = new StringJoiner(",", "(", ")");

                    for (EnsembleCondition condition : leftSource.getEnsemble())
                    {
                        if (condition.isExclude())
                        {
                            exclude.add(String.valueOf(Ensembles.getEnsembleID(condition.getName(),
                                                                               condition.getMemberId(),
                                                                               condition.getQualifier())));
                        }
                        else
                        {
                            include.add(String.valueOf(Ensembles.getEnsembleID(condition.getName(),
                                                                               condition.getMemberId(),
                                                                               condition.getQualifier())));
                        }
                    }

                    if (include.length() > 0)
                    {
                        script.append("     AND FE.ensemble_id IN ")
                              .append(include.toString())
                              .append("         ")
                              .append("-- Only get the values from these ensembles")
                              .append(NEWLINE);
                    }

                    if (exclude.length() > 0)
                    {
                        script.append("     AND FE.ensemble NOT IN ")
                              .append(exclude.toString())
                              .append("         ")
                              .append("-- Only get values not pertaining to these ensembles")
                              .append(NEWLINE);
                    }
                }

                if (earliestIssueDate != null)
                {
                    script.append("     AND F.forecast_date >= ")
                          .append(earliestIssueDate)
                          .append("            ")
                          .append("-- Limit results to values that were forecasted on or after the given date")
                          .append(NEWLINE);
                }

                if (latestIssueDate != null)
                {
                    script.append("     AND F.forecast_date <= ")
                          .append(latestIssueDate)
                          .append("            ")
                          .append("-- Limit results to values that were forecasted on or before the given date")
                          .append(NEWLINE);
                }

                if (earliestDate != null)
                {
                    script.append("     AND F.forecast_date + INTERVAL '1 hour' * lead");

                    if (leftTimeShift != null)
                    {
                        script.append(" + INTERVAL '1 hour' * ").append(leftTimeShift);
                    }

                    script.append(" >= ").append(earliestDate)
                          .append("         ")
                          .append("-- Limit the forecasts to values on or after this date")
                          .append(NEWLINE);
                }

                if (latestDate != null)
                {
                    script.append("     AND F.forecast_date + INTERVAL '1 hour' * lead");

                    if (leftTimeShift != null)
                    {
                        script.append(" + INTERVAL '1 hour' *").append(leftTimeShift);
                    }

                    script.append(" <= ").append(latestDate)
                          .append("         ")
                          .append("-- Limit the forecasts to values on or before this date")
                          .append(NEWLINE);
                }

                if (minimumValue != null)
                {
                    script.append("     AND FV.forecasted_value * UC.factor >= ")
                          .append(minimumValue)
                          .append("         ")
                          .append("-- Limit the forecasted values to those greater than or equal to the given value")
                          .append(NEWLINE);
                }

                if (maximumValue != null)
                {
                    script.append("     AND FV.forecasted_value * UC.factor <= ")
                          .append(maximumValue)
                          .append("         ")
                          .append("-- Limit the forecasted values to those greater than or equal to the given value")
                          .append(NEWLINE);
                }

                script.append("GROUP BY F.forecast_date")
                      .append("         ")
                      .append("-- Aggregate the forecasted values by grouping them based on their date")
                      .append(NEWLINE);
            }
            else
            {
                script.append("O.observed_value * UC.factor AS sourceOneValue,")
                      .append("         ")
                      .append("-- Select a single observed value converted to the requested unit of measurement")
                      .append(NEWLINE);
                script.append("     ST.measurements")
                      .append("         ")
                      .append("-- Select the values from the previous query")
                      .append(NEWLINE);
                script.append("FROM wres.Observation O")
                      .append("         ")
                      .append("-- Start by searching through the observed values")
                      .append(NEWLINE);
                script.append("INNER JOIN sourceTwo ST")
                      .append("         ")
                      .append("-- Match the observed values with the values from the previous queries")
                      .append(NEWLINE);
                script.append("     ON O.observation_time");

                if (leftTimeShift != null)
                {
                    script.append(" + INTERVAL '1 hour' * ").append(leftTimeShift);
                }

                script.append(" = ST.sourceTwoDate")
                      .append("         ")
                      .append("-- Match the values based on their dates")
                      .append(NEWLINE);
                script.append("INNER JOIN wres.UnitConversion UC")
                      .append("         ")
                      .append("-- Determine the conversion factor")
                      .append(NEWLINE);
                script.append("     ON UC.from_unit = O.measurementunit_id")
                      .append("         ")
                      .append("-- Match on the unit to convert from")
                      .append(NEWLINE);
                script.append("WHERE ").append(leftVariablePositionClause)
                      .append("         ")
                      .append("-- Select only observations from a variable at a specific location")
                      .append(NEWLINE);
                script.append("     AND UC.to_unit = ").append(desiredMeasurementID)
                      .append("         ")
                      .append("-- Find the correct conversion factor based on the unit of measurement to convert to")
                      .append(NEWLINE);

                if (earliestDate != null)
                {
                    script.append("     AND O.observation_time");

                    if (leftTimeShift != null)
                    {
                        script.append(" + INTERVAL '1 hour' * ").append(leftTimeShift);
                    }

                    script.append(" >= ").append(earliestDate)
                          .append("            ")
                          .append("-- Only retrieve observations on or after this date")
                          .append(NEWLINE);
                }

                if (latestDate != null)
                {
                    script.append("     AND O.observation_time");

                    if (leftTimeShift != null) {
                        script.append(" + INTERVAL '1 hour' * ").append(leftTimeShift);
                    }

                    script.append(" <= ").append(latestDate)
                          .append("            ")
                          .append("-- Only retrieve observations on or before this date")
                          .append(NEWLINE);
                }

                if (minimumValue != null)
                {
                    script.append("     AND O.observed_value * UC.factor >= ").append(minimumValue)
                          .append("         ")
                          .append("-- Limit observed values to those greater than or equal to the indicated value")
                          .append(NEWLINE);
                }

                if (maximumValue != null)
                {
                    script.append("     AND O.observed_value * UC.factor <= ")
                          .append(maximumValue)
                          .append("         ")
                          .append("-- Limit the observed values to those less than or equal to the indicated value")
                          .append(NEWLINE);
                }
            }

            script.append(") AS pairs;");
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
        catch (InvalidPropertiesFormatException e) {
            e.printStackTrace();
        }

        return script.toString();
    }
}
