package wres.io.data.caching;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.io.data.details.ScenarioDetails;
import wres.io.grouping.DualString;
import wres.io.utilities.Database;
import wres.util.Internal;
import wres.util.Strings;

/**
 * Cache of available types of forecast
 */
@Internal(exclusivePackage = "wres.io")
public class Scenarios extends Cache<ScenarioDetails, DualString> {

    private static final Logger LOGGER = LoggerFactory.getLogger(Scenarios.class);
    private static Scenarios INTERNAL_CACHE = null;
    private static final Object CACHE_LOCK = new Object();

    private static Scenarios getCache ()
    {
        synchronized (CACHE_LOCK)
        {
            if (INTERNAL_CACHE == null)
            {
                INTERNAL_CACHE = new Scenarios();
                INTERNAL_CACHE.init();
            }
            return INTERNAL_CACHE;
        }
    }

    public static Integer getScenarioID(String scenarioName, String scenarioType) throws SQLException
    {
        Integer scenarioID;

        try
        {
            scenarioID = getCache().getID( new DualString( scenarioName, scenarioType ) );
        }
        catch (SQLException e)
        {
            LOGGER.error("An error was encountered while trying to get the id for the {} scenario named '{}'",
                         scenarioName,
                         scenarioType);
            LOGGER.error("{} is not a valid scenario.", scenarioName);
            LOGGER.error(Strings.getStackTrace( e ));

            throw e;
        }

        if (scenarioID == null)
        {
            ScenarioDetails details = new ScenarioDetails();
            details.setScenarioName( scenarioName );
            details.setScenarioType( scenarioType );

            scenarioID = getCache().getID( details );
        }

        return scenarioID;
    }

    @Override
    protected int getMaxDetails() {
        return 10;
    }

    @Override
    protected void init() {
        Connection connection = null;
        ResultSet scenarios = null;

        try
        {
            connection = Database.getHighPriorityConnection();
            String loadScript = "SELECT *" + NEWLINE;
            loadScript += "FROM wres.Scenario;";

            scenarios = Database.getResults(connection, loadScript);

            while (scenarios.next())
            {
                this.getKeyIndex().put(
                        new DualString( scenarios.getString( "scenario_name"),
                                        scenarios.getString( "scenario_type" ) ),
                        scenarios.getInt( "scenario_id" )
                );
            }

        }
        catch (SQLException error)
        {
            LOGGER.error("An error was encountered when trying to populate the Scenario cache.");
        }
        finally
        {
            if (scenarios != null)
            {
                try
                {
                    scenarios.close();
                }
                catch (SQLException error)
                {
                    LOGGER.error("The result set containing scenarios could not be closed.");
                }
            }

            if (connection != null)
            {
                Database.returnHighPriorityConnection(connection);
            }
        }
    }
}
