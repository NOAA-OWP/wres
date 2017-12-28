/**
 * 
 */
package wres.io.utilities;

import java.sql.SQLException;
import java.util.InvalidPropertiesFormatException;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.Feature;
import wres.config.generated.TimeAnchor;
import wres.io.config.ConfigHelper;
import wres.io.data.details.ProjectDetails;
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

    public static String formInitialRollingDataScript(
            ProjectDetails projectDetails,
            Feature feature)
            throws SQLException, InvalidPropertiesFormatException
    {
        StringBuilder script = new StringBuilder();

        String timeSeriesVariablePosition =
                ConfigHelper.getVariablePositionClause(
                        feature,
                        ConfigHelper.getVariableID( projectDetails.getRight() ),
                        "TS"
                );

        script.append("WITH earliest_latest AS").append(NEWLINE);
        script.append("(").append(NEWLINE);
        script.append("    SELECT GREATEST(O.min, F.min) AS earliest,").append(NEWLINE);
        script.append("        LEAST(O.max, F.max) AS latest").append(NEWLINE);
        script.append("    FROM (").append(NEWLINE);
        script.append("        SELECT MIN(TS.initialization_date),").append(NEWLINE);
        script.append("            MAX(TS.initialization_date)").append(NEWLINE);
        script.append("        FROM wres.TimeSeries TS").append(NEWLINE);
        script.append("        WHERE ").append(timeSeriesVariablePosition).append(NEWLINE);

        if (projectDetails.getEarliestIssueDate() != null)
        {
            script.append("            AND TS.initialization_date >= '")
                  .append(projectDetails.getEarliestIssueDate())
                  .append("'")
                  .append(NEWLINE);
        }

        if (projectDetails.getLatestIssueDate() != null)
        {
            script.append("            AND TS.initialization_date <= '")
                  .append(projectDetails.getLatestIssueDate())
                  .append("'")
                  .append(NEWLINE);
        }

        script.append("            AND EXISTS (").append(NEWLINE);
        script.append("                SELECT 1").append(NEWLINE);
        script.append("                FROM wres.ForecastSource FS").append(NEWLINE);
        script.append("                INNER JOIN wres.ProjectSource PS").append(NEWLINE);
        script.append("                    ON PS.source_id = FS.source_id").append(NEWLINE);
        script.append("                WHERE PS.project_id = ").append(projectDetails.getId()).append(NEWLINE);
        script.append("                    AND PS.member = 'right'").append(NEWLINE);
        script.append("                    AND FS.forecast_id = TS.timeseries_id").append(NEWLINE);
        script.append("            )");
        script.append("        ) AS F").append(NEWLINE);
        script.append("    CROSS JOIN (").append(NEWLINE);
        script.append("        SELECT MIN(O.observation_time), MAX(O.observation_time)").append(NEWLINE);
        script.append("        FROM wres.Observation O").append(NEWLINE);
        script.append("        WHERE ");
        script.append(
                ConfigHelper.getVariablePositionClause(
                        feature,
                        ConfigHelper.getVariableID( projectDetails.getLeft() ),
                        "O"
                )
        ).append(NEWLINE);
        script.append("            AND EXISTS (").append(NEWLINE);
        script.append("                SELECT 1").append(NEWLINE);
        script.append("                FROM wres.ProjectSource PS").append(NEWLINE);
        script.append("                WHERE PS.project_id = ").append(projectDetails.getId()).append(NEWLINE);
        script.append("                    AND PS.member = 'left'").append(NEWLINE);
        script.append("                    AND PS.source_id = O.source_id").append(NEWLINE);
        script.append("        )").append(NEWLINE);
        script.append("    ) AS O").append(NEWLINE);
        script.append(")").append(NEWLINE);
        script.append("SELECT MIN(TS.initialization_date)::text,").append(NEWLINE);
        script.append( "    ( EXTRACT( epoch FROM ( MAX( TS.initialization_date ) " )
              .append( NEWLINE );
        script.append( "                           - MIN( TS.initialization_date )" )
              .append( NEWLINE );
        script.append( "                           - INTERVAL '" );
        script.append( projectDetails.getPoolingWindow().getPeriod() );
        script.append( " " );
        script.append( projectDetails.getPoolingWindow().getUnit().value() );
        script.append( "' ) )").append( NEWLINE );
        script.append( "     /").append( NEWLINE );
        script.append( "     EXTRACT( epoch FROM ( INTERVAL '" );
        script.append( projectDetails.getPoolingWindow().getFrequency() );
        script.append( " " );
        script.append( projectDetails.getPoolingWindow().getUnit().value() );
        script.append( "' ) )" ).append( NEWLINE );
        script.append( "    )::int + 1 AS window_count" ).append( NEWLINE );
        script.append("FROM wres.TimeSeries TS").append(NEWLINE);
        script.append("CROSS JOIN earliest_latest EL").append(NEWLINE);
        script.append("WHERE ").append(timeSeriesVariablePosition).append(NEWLINE);

        if ( projectDetails.getPoolingWindow().getAnchor() == TimeAnchor.CENTER)
        {
            script.append( "    AND TS.initialization_date >= EL.earliest" )
                  .append( NEWLINE );
            script.append( "    AND TS.initialization_date <= EL.latest" )
                  .append( NEWLINE );
        }
        else if (projectDetails.getPoolingWindow().getAnchor() == TimeAnchor.LEFT)
        {
            script.append("    AND TS.initialization_date >= EL.earliest").append(NEWLINE);
            script.append("    AND TS.initialization_date + INTERVAL '");
            script.append(projectDetails.getPoolingWindow().getPeriod());
            script.append(" ");
            script.append(projectDetails.getPoolingWindowUnit());
            script.append("' <= EL.latest");
            script.append(NEWLINE);
        }
        else
        {
            script.append("    AND TS.initialization_date - INTERVAL '");
            script.append(projectDetails.getPoolingWindow().getPeriod());
            script.append(" ");
            script.append(projectDetails.getPoolingWindowUnit());
            script.append("' >= EL.earliest");
            script.append(NEWLINE);
            script.append("    AND TS.initialization_date <= EL.latest");
            script.append(NEWLINE);
        }

        script.append("    AND EXISTS (").append(NEWLINE);
        script.append("        SELECT 1").append(NEWLINE);
        script.append("        FROM wres.ForecastSource FS").append(NEWLINE);
        script.append("        INNER JOIN wres.ProjectSource PS").append(NEWLINE);
        script.append("            ON PS.source_id = FS.source_id").append(NEWLINE);
        script.append("        WHERE PS.project_id = ").append(projectDetails.getId()).append(NEWLINE);
        script.append("            AND PS.member = 'right'").append(NEWLINE);
        script.append("            AND FS.forecast_id = TS.timeseries_id").append(NEWLINE);
        script.append("    );");


        return script.toString();
    }

    public static String generateZeroDateScript(ProjectDetails projectDetails,
                                                DataSourceConfig simulation,
                                                Feature feature)
            throws SQLException
    {

        if (simulation == null)
        {
            return null;
        }

        StringBuilder script = new StringBuilder(  );

        script.append( "SELECT MIN(O.observation_time)::text AS zero_date" ).append(NEWLINE);
        script.append("FROM wres.Observation O").append(NEWLINE);
        script.append("WHERE ")
              .append(ConfigHelper.getVariablePositionClause( feature,
                                                              ConfigHelper.getVariableID( simulation ),
                                                              "O"))
              .append(NEWLINE);
        script.append("    AND EXISTS (").append(NEWLINE);
        script.append("        SELECT 1").append(NEWLINE);
        script.append("        FROM wres.ProjectSource PS").append(NEWLINE);
        script.append("        WHERE PS.project_id = ").append(projectDetails.getId()).append(NEWLINE);
        script.append("            AND PS.member = ");

        if (projectDetails.getRight().equals( simulation ))
        {
            script.append(ProjectDetails.RIGHT_MEMBER);
        }
        else if (projectDetails.getLeft().equals( simulation ))
        {
            script.append(ProjectDetails.LEFT_MEMBER);
        }
        else
        {
            script.append(ProjectDetails.BASELINE_MEMBER);
        }

        script.append(NEWLINE);

        script.append("            AND PS.source_id = O.source_id").append(NEWLINE);
        script.append("        )").append(NEWLINE);

        if (projectDetails.getEarliestDate() != null)
        {
            script.append("     AND O.observation_time >= '")
                  .append(projectDetails.getEarliestDate())
                  .append( "'" )
                  .append(NEWLINE);
        }

        if (projectDetails.getLatestDate() != null)
        {
            script.append("     AND O.observation_time <= '")
                  .append(projectDetails.getLatestDate())
                  .append( "'" )
                  .append(NEWLINE);
        }

        script.append( ";" );

        return script.toString();
    }
}
