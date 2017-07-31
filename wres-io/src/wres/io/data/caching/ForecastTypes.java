package wres.io.data.caching;

import wres.io.data.details.ForecastTypeDetails;
import wres.io.utilities.Database;
import wres.util.Internal;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Cache of available types of forecast
 */
@Internal(exclusivePackage = "wres.io")
public class ForecastTypes extends Cache<ForecastTypeDetails, String> {

    private static ForecastTypes INTERNAL_CACHE = null;
    private static final Object CACHE_LOCK = new Object();

    private static ForecastTypes getCache ()
    {
        synchronized (CACHE_LOCK)
        {
            if (INTERNAL_CACHE == null)
            {
                INTERNAL_CACHE = new ForecastTypes();
                INTERNAL_CACHE.init();
            }
            return INTERNAL_CACHE;
        }
    }

    public static Integer getForecastTypeId(String description) throws SQLException {
        Integer forecastTypeID;

        try {
            forecastTypeID = getCache().getID(description);
        } catch (SQLException e) {
            System.err.println("An error was encountered while trying to get the id for the forecast type named: '" + description + "'.");
            System.err.println(description + " is not a valid forecast type.");
            e.printStackTrace();

            throw e;
        }

        return forecastTypeID;
    }

    @Override
    protected int getMaxDetails() {
        return 10;
    }

    @Override
    protected void init() {
        Connection connection = null;
        ResultSet types = null;

        try
        {
            connection = Database.getHighPriorityConnection();
            String loadScript = "SELECT forecasttype_id, type_name, timestep" + NEWLINE;
            loadScript += "FROM wres.ForecastType;";

            types = Database.getResults(connection, loadScript);

            while (types.next())
            {
                this.getKeyIndex().put(types.getString("type_name"), types.getInt("forecasttype_id"));
            }

        }
        catch (SQLException error)
        {
            System.err.println("An error was encountered when trying to populate the ForecastType cache.");
        }
        finally
        {
            if (types != null)
            {
                try
                {
                    types.close();
                }
                catch (SQLException error)
                {
                    System.err.println("The result set containing forecast types could not be closed.");
                }
            }

            if (connection != null)
            {
                Database.returnHighPriorityConnection(connection);
            }
        }
    }
}
