package wres.io.data.caching;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.ProjectConfig;
import wres.io.data.details.ProjectDetails;
import wres.io.utilities.Database;
import wres.util.Internal;

/**
 * Cache of available types of forecast
 */
@Internal(exclusivePackage = "wres.io")
public class Projects extends Cache<ProjectDetails, Integer> {

    private static final Logger LOGGER = LoggerFactory.getLogger(Projects.class);
    private static Projects INTERNAL_CACHE = null;
    private static final Object CACHE_LOCK = new Object();

    private static Projects getCache ()
    {
        synchronized (CACHE_LOCK)
        {
            if (INTERNAL_CACHE == null)
            {
                INTERNAL_CACHE = new Projects();
                INTERNAL_CACHE.init();
            }
            return INTERNAL_CACHE;
        }
    }

    public static ProjectDetails getProject( ProjectConfig projectConfig)
            throws SQLException
    {
        ProjectDetails details = null;
        Integer inputCode = ProjectDetails.hash( projectConfig );

        if (Projects.getCache().hasID( inputCode ))
        {
            details = Projects.getCache().get( Projects.getCache().getID( inputCode )  );
        }

        if (details == null)
        {
            details = new ProjectDetails(projectConfig);
            Projects.getCache().addElement( details );
        }


        return details;
    }

    @Override
    protected int getMaxDetails() {
        return 10;
    }

    @Override
    protected void init() {
        this.getDetails();

        Connection connection = null;
        ResultSet projects = null;

        try
        {
            connection = Database.getHighPriorityConnection();
            String loadScript = "SELECT *" + NEWLINE;
            loadScript += "FROM wres.project;";

            projects = Database.getResults(connection, loadScript);

            while (projects.next())
            {
                Integer projectId = projects.getInt( "project_id" );
                Integer inputCode = projects.getInt("input_code");
                this.getKeyIndex().put(
                        inputCode,
                        projectId
                );
            }

        }
        catch (SQLException error)
        {
            LOGGER.error("An error was encountered when trying to populate the Project cache.");
        }
        finally
        {
            if (projects != null)
            {
                try
                {
                    projects.close();
                }
                catch (SQLException error)
                {
                    LOGGER.error("The result set containing projects could not be closed.");
                }
            }

            if (connection != null)
            {
                Database.returnHighPriorityConnection(connection);
            }
        }
    }
}
