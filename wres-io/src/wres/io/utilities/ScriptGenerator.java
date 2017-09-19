/**
 * 
 */
package wres.io.utilities;

import java.sql.SQLException;
import java.util.InvalidPropertiesFormatException;
import java.util.List;
import java.util.StringJoiner;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.EnsembleCondition;
import wres.config.generated.Feature;
import wres.config.generated.ProjectConfig;
import wres.io.config.ConfigHelper;
import wres.io.data.caching.Ensembles;
import wres.io.data.caching.Projects;
import wres.io.data.details.ProjectDetails;
import wres.io.grouping.LabeledScript;
import wres.util.Collections;
import wres.util.Internal;

/**
 * @author Christopher Tubbs
 *
 */
@Internal(exclusivePackage = "wres.io")
public final class ScriptGenerator
{
    private ScriptGenerator (){}
    
    private static final String NEWLINE = System.lineSeparator();

    // TODO: This does not consider the ramifacations of project configurations
    public static LabeledScript generateFindLastLead(int variableID,
                                                     Feature feature,
                                                     boolean isForecast)
    {
        final String label = "last_lead";
        String script = "";

        if (isForecast)
        {

            script += "SELECT FV.lead AS " + label + NEWLINE;
            script += "FROM wres.ForecastEnsemble FE" + NEWLINE;
            script += "INNER JOIN wres.ForecastValue FV" + NEWLINE;
            script += "    ON FV.forecastensemble_id = FE.forecastensemble_id"
                      + NEWLINE;
            script += "INNER JOIN wres.Forecast F" + NEWLINE;
            script += "     ON FE.forecast_id = F.forecast_id" + NEWLINE;
            script += "WHERE " +
                      ConfigHelper.getVariablePositionClause( feature, variableID, "FE" ) +
                      NEWLINE;
            script += "ORDER BY FV.lead DESC" + NEWLINE;
            script += "LIMIT 1;";
        }
        else
        {
            script += "SELECT COUNT(*)::int AS " + label + NEWLINE;
            script += "FROM wres.Observation O" + NEWLINE;
            script += "WHERE " + ConfigHelper.getVariablePositionClause( feature, variableID, "O" ) + NEWLINE;
        }
        
        return new LabeledScript(label, script);
    }

    // TODO: Convert function to its own class
    public static String generateLoadDatasourceScript(final ProjectConfig projectConfig,
                                                      final DataSourceConfig dataSourceConfig,
                                                      final Feature feature,
                                                      final int progress,
                                                      final String zeroDate,
                                                      final int leadOffset)
            throws SQLException, InvalidPropertiesFormatException
    {
        StringBuilder script = new StringBuilder();
        Integer variableID = ConfigHelper.getVariableID(dataSourceConfig);

        String earliestDate = null;
        String latestDate = null;
        String earliestIssueDate = null;
        String latestIssueDate = null;

        String variablePositionClause = ConfigHelper.getVariablePositionClause(feature, variableID, "");
        ProjectDetails projectDetails = Projects.getProject( projectConfig.getName() );

        Integer timeShift = null;

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

            List<Integer> forecastIds;

            if ( ConfigHelper.isLeft( dataSourceConfig, projectConfig ))
            {
                forecastIds = projectDetails.getLeftForecastIDs();
            }
            else if (ConfigHelper.isRight( dataSourceConfig, projectConfig ))
            {
                forecastIds = projectDetails.getRightForecastIDs();
            }
            else
            {
                forecastIds = projectDetails.getBaselineForecastIDs();
            }

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
            script.append("WHERE F.forecast_id = ")
                  .append( Collections.formAnyStatement( forecastIds, "int" ))
                  .append(NEWLINE);
            script.append("     AND ").append(ConfigHelper.getLeadQualifier(projectConfig, progress, leadOffset)).append(NEWLINE);
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

            script.append("GROUP BY F.forecast_date, FV.lead, FE.measurementunit_id")
                  .append("         ")
                  .append("-- Aggregate the forecasted values by grouping them based on their date")
                  .append(NEWLINE);
        }
        else
        {
            List<Integer> sourceIds;

            if (ConfigHelper.isLeft( dataSourceConfig, projectConfig ))
            {
                sourceIds = projectDetails.getLeftSources();
            }
            else if ( ConfigHelper.isRight( dataSourceConfig, projectConfig ))
            {
                sourceIds = projectDetails.getRightSources();
            }
            else
            {
                sourceIds = projectDetails.getBaselineSources();
            }

            script.append("SELECT ARRAY[O.observed_value] AS measurements,").append(NEWLINE);
            script.append("     (O.observation_time");

            if (timeShift != null)
            {
                script.append(" + INTERVAL '1 HOUR' * ").append(timeShift);
            }

            script.append(")::text AS value_date,").append(NEWLINE);
            script.append("     O.measurementunit_id").append(NEWLINE);
            script.append("FROM wres.Observation O").append(NEWLINE);
            script.append("WHERE ").append(variablePositionClause).append(NEWLINE);
            script.append("     AND O.source_id = ")
                  .append(Collections.formAnyStatement( sourceIds, "int" ))
                  .append(NEWLINE);
            script.append("     AND '")
                  .append(zeroDate)
                  .append("' <= ")
                  .append("O.observation_time");

            if (timeShift != null)
            {
                script.append(" + INTERVAL + '1 hour' * ").append(timeShift);
            }

            script.append(NEWLINE);

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

    public static String generateZeroDateScript(ProjectConfig projectConfig,
                                                DataSourceConfig simulation)
            throws SQLException
    {

        if (simulation == null)
        {
            return null;
        }

        // TODO: This will need to evolve once we get multiple locations involved
        Feature feature = simulation.getFeatures().get( 0 );

        String earliestDate = null;
        String latestDate = null;

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

        StringBuilder script = new StringBuilder(  );

        script.append( "SELECT MIN(O.observation_time)::text AS zero_date" ).append(NEWLINE);
        script.append("FROM wres.Observation O").append(NEWLINE);
        script.append("WHERE ")
              .append(ConfigHelper.getVariablePositionClause( feature,
                                                              ConfigHelper.getVariableID( simulation ),
                                                              "O"))
              .append(NEWLINE);

        if (earliestDate != null)
        {
            script.append("     AND O.observation_time >= ")
                  .append(earliestDate)
                  .append(NEWLINE);
        }

        if (latestDate != null)
        {
            script.append("     AND O.observation_time <= ")
                  .append(latestDate)
                  .append(NEWLINE);
        }

        script.append( ";" );

        return script.toString();
    }
}
