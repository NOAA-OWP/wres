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
import wres.util.Strings;

/**
 * Cache of available types of forecast
 */
@Internal(exclusivePackage = "wres.io")
public class Projects extends Cache<ProjectDetails, String> {

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

    public static Integer getProjectID( String projectName) throws SQLException
    {
        Integer projectID;

        try
        {
            projectID = getCache().getID( projectName );
        }
        catch (SQLException e)
        {
            LOGGER.error("An error was encountered while trying to get the id for the project named '{}'",
                         projectName);
            LOGGER.error("{} is not a valid project.", projectName);
            LOGGER.error(Strings.getStackTrace( e ));

            throw e;
        }

        if (projectID == null)
        {
            ProjectDetails details = new ProjectDetails();
            details.setProjectName( projectName );

            projectID = getCache().getID( details );
        }

        return projectID;
    }

    public static ProjectDetails getProject( ProjectConfig projectConfig)
            throws SQLException
    {
        ProjectDetails projectDetails = Projects.getProject( projectConfig.getName() );
        projectDetails.load( projectConfig );
        return projectDetails;
    }

    public static ProjectDetails getProject(String projectName)
            throws SQLException
    {
        ProjectDetails details = null;
        if (Projects.getCache().hasID( projectName ))
        {
            details = Projects.getCache().get( Projects.getProjectID( projectName ) );
        }

        if (details == null)
        {
            details = new ProjectDetails();
            details.setProjectName( projectName );
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
                String projectName = projects.getString("project_name");
                this.getKeyIndex().put(
                        projectName,
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
