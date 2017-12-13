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
import wres.io.data.details.ProjectDetails;
import wres.util.Collections;
import wres.util.Internal;
import wres.util.Time;

/**
 * @author Christopher Tubbs
 *
 */
@Internal(exclusivePackage = "wres.io")
public final class ScriptGenerator
{
    private ScriptGenerator (){}
    
    private static final String NEWLINE = System.lineSeparator();

    public static String generateZeroDateScript(ProjectConfig projectConfig,
                                                DataSourceConfig simulation,
                                                Feature feature)
            throws SQLException
    {

        if (simulation == null)
        {
            return null;
        }

        String earliestDate = null;
        String latestDate = null;

        if ( projectConfig.getPair()
                          .getDates() != null )
        {
            if ( projectConfig.getPair()
                              .getDates()
                              .getEarliest() != null )
            {
                earliestDate = "'" + projectConfig.getPair()
                                                  .getDates()
                                                  .getEarliest() + "'";
            }

            if ( projectConfig.getPair()
                              .getDates()
                              .getLatest() != null )
            {
                latestDate = "'" + projectConfig.getPair()
                                                .getDates()
                                                .getLatest() + "'";
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
