package wres.io.retrieval;

import java.sql.SQLException;
import java.util.InvalidPropertiesFormatException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.Feature;
import wres.config.generated.ProjectConfig;
import wres.io.config.ConfigHelper;
import wres.io.data.details.ProjectDetails;
import wres.io.utilities.Database;
import wres.io.utilities.NoDataException;

public class RollingMetricInputIterator extends MetricInputIterator
{
    private static final Logger LOGGER =
            LoggerFactory.getLogger(RollingMetricInputIterator.class);

    RollingMetricInputIterator( ProjectConfig projectConfig,
                                Feature feature,
                                ProjectDetails projectDetails )
            throws SQLException, InvalidPropertiesFormatException,
            NoDataException
    {
        super( projectConfig, feature, projectDetails );
    }

    @Override
    int calculateWindowCount()
            throws SQLException, InvalidPropertiesFormatException,
            NoDataException
    {
        return 0;
    }

    /**
     * Calculates the first viable date for a match in the database
     *
     * All we need to know is a date after which matches exist.
     * @return The string representation of the first date
     * @throws SQLException
     */
    private String getFirstMatchDate() throws SQLException
    {
        if (this.firstMatchDate == null)
        {
            StringBuilder script = new StringBuilder();

            script.append("WITH forecasts AS" ).append(NEWLINE);
            script.append("(").append(NEWLINE);
            script.append("    SELECT ")
                  .append("TS.initialization_date + INTERVAL '1 HOUR' * FV.lead AS first_date")
                  .append(NEWLINE);
            script.append("    FROM wres.TimeSeries TS").append(NEWLINE);
            script.append("    INNER JOIN wres.ForecastValue FV").append(NEWLINE);
            script.append("        ON FV.timeseries_id = TS.timeseries_id")
                  .append(NEWLINE);
            script.append("    WHERE FV.lead < 10").append(NEWLINE);
            script.append("        AND ")
                  .append(
                          ConfigHelper.getVariablePositionClause(
                                  this.getFeature(),
                                  this.getProjectDetails().getRightVariableID(),
                                  "TS" )
                  ).append(NEWLINE);
            script.append("        AND EXISTS (").append(NEWLINE);
            script.append("            SELECT 1").append(NEWLINE);
            script.append("            FROM wres.ForecastSource FS").append(NEWLINE);
            script.append("            INNER JOIN wres.ProjectSource PS").append(NEWLINE);
            script.append("                ON PS.source_id = FS.source_id").append(NEWLINE);
            script.append("            WHERE project_id = ")
                  .append(this.getProjectDetails().getId())
                  .append(NEWLINE);
            script.append("                AND member = 'right'").append(NEWLINE);
            script.append("                AND FS.forecast_id = TS.timeseries_id").append(NEWLINE);
            script.append("        )").append(NEWLINE);
            script.append(")").append(NEWLINE);
            script.append("SELECT first_date").append(NEWLINE);
            script.append("FROM wres.Observation O").append(NEWLINE);
            script.append("INNER JOIN forecasts F").append(NEWLINE);
            script.append("    ON F.first_date = O.observation_time").append(NEWLINE);
            script.append("WHERE ");

            script.append(
                    ConfigHelper.getVariablePositionClause(
                            this.getFeature(),
                            this.getProjectDetails().getLeftVariableID(),
                            "O")
            );
            script.append("    AND EXISTS (").append(NEWLINE);
            script.append("        SELECT 1").append(NEWLINE);
            script.append("        FROM wres.ProjectSource PS").append(NEWLINE);
            script.append("        WHERE PS.source_id = O.source_id").append(NEWLINE);
            script.append("            AND member = 'left'").append(NEWLINE);
            script.append("    )").append(NEWLINE);

            this.firstMatchDate = Database.getResult(script.toString(), "first_date");
        }

        return this.firstMatchDate;
    }

    private String getLastMatchDate() throws SQLException
    {
        if (this.lastMatchDate == null)
        {
            StringBuilder script = new StringBuilder(  );

            script.append("SELECT TS.initialization_date + INTERVAL '1 HOUR' * FV.lead AS max")
                  .append(NEWLINE);
            script.append("FROM wres.TimeSeries TS").append(NEWLINE);
            script.append("INNER JOIN wres.ForecastValue FV").append(NEWLINE);
            script.append("    ON FV.timeseries_id = TS.timeseries_id").append(NEWLINE);
            script.append("WHERE ")
                  .append(
                          ConfigHelper.getVariablePositionClause(
                                  this.getFeature(),
                                  this.getProjectDetails().getRightVariableID(),
                                  "TS" ))
                  .append(NEWLINE);
            script.append("    AND EXISTS (").append(NEWLINE);
            script.append("        SELECT 1").append(NEWLINE);
            script.append("        FROM wres.ForecastSource FS").append(NEWLINE);
            script.append("        INNER JOIN wres.ProjectSource PS").append(NEWLINE);
            script.append("            ON PS.source_id = FS.source_id").append(NEWLINE);
            script.append("        WHERE PS.project_id = ")
                  .append( this.getProjectDetails().getId() )
                  .append(NEWLINE);
            script.append("            AND FS.forecast_id = TS.timeseries_id")
                  .append(NEWLINE);
            script.append("            AND PS.member = 'right'").append(NEWLINE);
            script.append(")").append(NEWLINE);
            script.append("ORDER BY max DESC").append(NEWLINE);
            script.append("LIMIT 1;");

            this.lastMatchDate = Database.getResult( script.toString(), "max" );
        }

        return this.lastMatchDate;
    }

    @Override
    protected Integer getWindowCount() throws NoDataException, SQLException,
            InvalidPropertiesFormatException
    {
        return super.getWindowCount();
    }

    @Override
    Logger getLogger()
    {
        return LOGGER;
    }

    private String firstMatchDate;
    private String lastMatchDate;
}