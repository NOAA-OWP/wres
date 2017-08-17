/**
 * 
 */
package wres.io.utilities;

import wres.config.generated.Conditions;
import wres.config.generated.DataSourceConfig;
import wres.config.generated.EnsembleCondition;
import wres.config.generated.ProjectConfig;
import wres.io.config.ConfigHelper;
import wres.io.data.caching.Ensembles;
import wres.io.grouping.LabeledScript;
import wres.util.Internal;

import java.sql.SQLException;
import java.util.InvalidPropertiesFormatException;
import java.util.StringJoiner;

/**
 * @author Christopher Tubbs
 *
 */
@Internal(exclusivePackage = "wres.io")
public final class ScriptGenerator
{
    private ScriptGenerator (){}
    
    private static final String NEWLINE = System.lineSeparator();

    public static LabeledScript generateFindLastLead(int variableID)
    {
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

    public static String generateLoadDatasourceScript(final ProjectConfig projectConfig,
                                                      final DataSourceConfig dataSourceConfig,
                                                      final Conditions.Feature feature,
                                                      final int progress) throws SQLException, InvalidPropertiesFormatException {
        StringBuilder script = new StringBuilder();
        Integer variableID = ConfigHelper.getVariableID(dataSourceConfig);

        Double minimumValue = null;
        Double maximumValue = null;
        String earliestDate = null;
        String latestDate = null;
        String earliestIssueDate = null;
        String latestIssueDate = null;

        String variablePositionClause = ConfigHelper.getVariablePositionClause(feature, variableID);

        Integer timeShift = null;

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

        if (dataSourceConfig.getTimeShift() != null && dataSourceConfig.getTimeShift().getWidth() != 0)
        {
            timeShift = dataSourceConfig.getTimeShift().getWidth();
        }

        if (ConfigHelper.isForecast(dataSourceConfig))
        {
            script.append("SELECT (F.forecast_date + INTERVAL '1 HOUR' * lead");

            if (timeShift != null)
            {
                script.append(" + INTERVAL '1 HOUR' * ").append(timeShift);
            }

            script.append(")::text AS value_date,").append(NEWLINE);
            script.append("     ARRAY_AGG(FV.forecasted_value) AS measurements,").append(NEWLINE);
            script.append("     FE.measurementunit_id").append(NEWLINE);
            script.append("FROM wres.Forecast F").append(NEWLINE);
            script.append("INNER JOIN wres.ForecastEnsemble FE").append(NEWLINE);
            script.append("     ON FE.forecast_id = F.forecast_id").append(NEWLINE);
            script.append("INNER JOIN wres.ForecastValue FV").append(NEWLINE);
            script.append("     ON FV.forecastensemble_id = FE.forecastensemble_id").append(NEWLINE);
            script.append("WHERE ").append(ConfigHelper.getLeadQualifier(projectConfig, progress)).append(NEWLINE);
            script.append("     AND ").append(variablePositionClause).append(NEWLINE);

            String ensembleClause = constructEnsembleClause(dataSourceConfig);

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

                if (timeShift != null)
                {
                    script.append(" + INTERVAL '1 hour' * ").append(timeShift);
                }

                script.append(" >= ").append(earliestDate)
                      .append("         ")
                      .append("-- Limit the forecasts to values on or after this date")
                      .append(NEWLINE);
            }

            if (latestDate != null)
            {
                script.append("     AND F.forecast_date + INTERVAL '1 hour' * lead");

                if (timeShift != null)
                {
                    script.append(" + INTERVAL '1 hour' *").append(timeShift);
                }

                script.append(" <= ").append(latestDate)
                      .append("         ")
                      .append("-- Limit the forecasts to values on or before this date")
                      .append(NEWLINE);
            }

            if (minimumValue != null)
            {
                script.append("     AND FV.forecasted_value >= ")
                      .append(minimumValue)
                      .append("         ")
                      .append("-- Limit the forecasted values to those greater than or equal to the given value")
                      .append(NEWLINE);
            }

            if (maximumValue != null)
            {
                script.append("     AND FV.forecasted_value <= ")
                      .append(maximumValue)
                      .append("         ")
                      .append("-- Limit the forecasted values to those greater than or equal to the given value")
                      .append(NEWLINE);
            }

            script.append("GROUP BY F.forecast_date, FV.lead, FE.measurementunit_id")
                  .append("         ")
                  .append("-- Aggregate the forecasted values by grouping them based on their date")
                  .append(NEWLINE);
        }
        else
        {
            script.append("SELECT ARRAY[O.observed_value] AS measurements,").append(NEWLINE);
            script.append("     (O.observation_time");

            if (timeShift != null)
            {
                script.append("+ INTERVAL '1 HOUR' * ").append(timeShift);
            }

            script.append(")::text AS value_date,").append(NEWLINE);
            script.append("     O.measurementunit_id").append(NEWLINE);
            script.append("FROM wres.Observation O").append(NEWLINE);
            script.append("WHERE ").append(variablePositionClause).append(NEWLINE);

            if (earliestDate != null)
            {
                script.append("     AND O.observation_time");

                if (timeShift != null)
                {
                    script.append(" + INTERVAL '1 hour' * ").append(timeShift);
                }

                script.append(" >= ").append(earliestDate)
                      .append("            ")
                      .append("-- Only retrieve observations on or after this date")
                      .append(NEWLINE);
            }

            if (latestDate != null)
            {
                script.append("     AND O.observation_time");

                if (timeShift != null) {
                    script.append(" + INTERVAL '1 hour' * ").append(timeShift);
                }

                script.append(" <= ").append(latestDate)
                      .append("            ")
                      .append("-- Only retrieve observations on or before this date")
                      .append(NEWLINE);
            }

            if (minimumValue != null)
            {
                script.append("     AND O.observed_value >= ").append(minimumValue)
                      .append("         ")
                      .append("-- Limit observed values to those greater than or equal to the indicated value")
                      .append(NEWLINE);
            }

            if (maximumValue != null)
            {
                script.append("     AND O.observed_value <= ")
                      .append(maximumValue)
                      .append("         ")
                      .append("-- Limit the observed values to those less than or equal to the indicated value")
                      .append(NEWLINE);
            }
        }
        script.append(";");

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
