/**
 * 
 */
package wres.io.utilities;

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
public final class ScriptGenerator
{
    private ScriptGenerator (){}
    
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

    public static String generateGetPairData(final ProjectConfig projectConfig, final int progress) throws NotImplementedException
    {
        StringBuilder script = new StringBuilder("SELECT * FROM (");

        try {

            script.append(constructSourceTwoClause(projectConfig, progress));
            script.append(constructSourceOneClause(projectConfig, progress));
        }
        catch (SQLException | InvalidPropertiesFormatException e) {
            e.printStackTrace();
        }

        script.append(") AS pairs;");

        return script.toString();
    }

    private static String constructSourceOneClause(ProjectConfig projectConfig, int progress) throws SQLException, NotImplementedException, InvalidPropertiesFormatException {
        StringBuilder script = new StringBuilder("SELECT ");

        if (ConfigHelper.isForecast(projectConfig.getInputs().getLeft()))
        {
            script.append(constructForecastSourceOneClause(projectConfig, progress));
        }
        else
        {
            script.append(constructObservationSourceOneClause(projectConfig));
        }

        return script.toString();
    }

    private static String constructObservationSourceOneClause(ProjectConfig projectConfig) throws SQLException, NotImplementedException {
        StringBuilder script = new StringBuilder();

        DataSourceConfig leftSource = projectConfig.getInputs().getLeft();
        Integer leftVariableId = Variables.getVariableID(leftSource.getVariable().getValue(), leftSource.getVariable().getUnit());

        Integer desiredMeasurementID = MeasurementUnits.getMeasurementUnitID(projectConfig.getPair().getUnit());
        Double minimumValue = null;
        Double maximumValue = null;
        String earliestDate = null;
        String latestDate = null;

        String leftVariablePositionClause = ConfigHelper.getVariablePositionClause(projectConfig
                                                                                           .getConditions()
                                                                                           .getFeature()
                                                                                           .get(0),
                                                                                   leftVariableId);

        Integer leftTimeShift = null;

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

        if (leftSource.getTimeShift() != null && leftSource.getTimeShift().getWidth() != 0)
        {
            leftTimeShift = leftSource.getTimeShift().getWidth();
        }

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

        return script.toString();
    }

    private static String constructForecastSourceOneClause(ProjectConfig projectConfig, int progress) throws SQLException,
                                                                                                             NotImplementedException,
                                                                                                             InvalidPropertiesFormatException
    {
        StringBuilder script = new StringBuilder();

        DataSourceConfig source = projectConfig.getInputs().getLeft();
        Integer leftVariableId = Variables.getVariableID(source.getVariable().getValue(), source.getVariable().getUnit());

        Integer desiredMeasurementID = MeasurementUnits.getMeasurementUnitID(projectConfig.getPair().getUnit());
        Double minimumValue = null;
        Double maximumValue = null;
        String earliestDate = null;
        String latestDate = null;
        String earliestIssueDate = null;
        String latestIssueDate = null;

        String leftVariablePositionClause = ConfigHelper.getVariablePositionClause(projectConfig
                                                                                           .getConditions()
                                                                                           .getFeature()
                                                                                           .get(0),
                                                                                   leftVariableId);

        Integer leftTimeShift = null;

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

        if (source.getTimeShift() != null && source.getTimeShift().getWidth() != 0)
        {
            leftTimeShift = source.getTimeShift().getWidth();
        }

        String leadSpecification = ConfigHelper.getLeadQualifier(projectConfig, progress);

        script.append(source.getRollingTimeAggregation().getFunction())
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

        String ensembleClause = constructEnsembleClause(source);

        if (!ensembleClause.isEmpty())
        {
            script.append(ensembleClause);
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

        return script.toString();
    }

    private static String constructSourceTwoClause(ProjectConfig projectConfig, int progress) throws SQLException,
                                                                                                     NotImplementedException,
                                                                                                     InvalidPropertiesFormatException {
        StringBuilder script = new StringBuilder();

        script.append("WITH sourceTwo AS        -- The CTE that produces the array for the second source")
              .append(NEWLINE);
        script.append("(")
              .append(NEWLINE);
        script.append("     SELECT ");

        if (ConfigHelper.isForecast(projectConfig.getInputs().getRight()))
        {
            script.append(constructForecastSourceTwoClause(projectConfig, progress));
        }
        else
        {
            script.append(constructObservationSourceTwoClause(projectConfig));
        }


        script.append(")")
              .append(NEWLINE);
        return script.toString();
    }

    private static String constructObservationSourceTwoClause(ProjectConfig projectConfig) throws SQLException,
                                                                                                  NotImplementedException,
                                                                                                  InvalidPropertiesFormatException {
        StringBuilder script = new StringBuilder();
        DataSourceConfig rightSource = projectConfig.getInputs().getRight();
        Integer rightVariableId = Variables.getVariableID(rightSource.getVariable().getValue(), rightSource.getVariable().getUnit());
        Integer desiredMeasurementID = MeasurementUnits.getMeasurementUnitID(projectConfig.getPair().getUnit());
        Double minimumValue = null;
        Double maximumValue = null;
        String earliestDate = null;
        String latestDate = null;

        Integer rightTimeShift = null;

        String rightVariablePositionClause = ConfigHelper.getVariablePositionClause(projectConfig
                                                                                            .getConditions()
                                                                                            .getFeature()
                                                                                            .get(0),
                                                                                    rightVariableId);

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

        if (rightSource.getTimeShift() != null && rightSource.getTimeShift().getWidth() != 0)
        {
            rightTimeShift = rightSource.getTimeShift().getWidth();
        }

        String rightDate = "O.observation_time";

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

        script.append("     FROM wres.Observation O")
              .append("         ")
              .append("-- Start by selecting observations")
              .append(NEWLINE);

        script.append("     INNER JOIN wres.UnitConversion UC")
              .append("         ")
              .append("-- Retrieve the conversion factor to convert the observed value to the desired measurement unit")
              .append(NEWLINE);

        script.append("         ON UC.from_unit = O.measurementunit_id")
              .append("         ")
              .append("-- The conversion factor will be obtained by matching the unit from the ensemble")
              .append(NEWLINE);
        script.append("     WHERE ").append(rightVariablePositionClause)
              .append("         ")
              .append("-- Select only the values that pertain to the specific variable and location")
              .append(NEWLINE);

        script.append("         AND UC.to_unit = ").append(desiredMeasurementID)
              .append("         ")
              .append("-- Determine the unit conversion by specifying the id of the unit to convert to")
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

        return script.toString();
    }

    private static String constructForecastSourceTwoClause(ProjectConfig projectConfig, int progress) throws SQLException, NotImplementedException, InvalidPropertiesFormatException {
        StringBuilder script = new StringBuilder();
        DataSourceConfig rightSource = projectConfig.getInputs().getRight();
        Integer rightVariableId = Variables.getVariableID(rightSource.getVariable().getValue(), rightSource.getVariable().getUnit());
        Integer desiredMeasurementID = MeasurementUnits.getMeasurementUnitID(projectConfig.getPair().getUnit());
        Double minimumValue = null;
        Double maximumValue = null;
        String earliestDate = null;
        String latestDate = null;
        String earliestIssueDate = null;
        String latestIssueDate = null;
        String rightDate = null;
        Integer rightTimeShift = null;

        String rightVariablePositionClause = ConfigHelper.getVariablePositionClause(projectConfig
                                                                                            .getConditions()
                                                                                            .getFeature()
                                                                                            .get(0),
                                                                                    rightVariableId);

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

        if (rightSource.getTimeShift() != null && rightSource.getTimeShift().getWidth() != 0)
        {
            rightTimeShift = rightSource.getTimeShift().getWidth();
        }

        String leadSpecification = ConfigHelper.getLeadQualifier(projectConfig, progress);

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

        String ensembleClause = constructEnsembleClause(rightSource);

        if (!ensembleClause.isEmpty())
        {
            script.append(ensembleClause);
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

        return script.toString();
    }

    private static String constructEnsembleClause(DataSourceConfig source) throws SQLException {

        StringBuilder script = new StringBuilder();

        if (source.getEnsemble() != null && source.getEnsemble().size() > 0)
        {
            StringJoiner include = new StringJoiner(",", "(", ")");
            StringJoiner exclude = new StringJoiner(",", "(", ")");

            for (EnsembleCondition condition : source.getEnsemble())
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

        return script.toString();
    }
}
