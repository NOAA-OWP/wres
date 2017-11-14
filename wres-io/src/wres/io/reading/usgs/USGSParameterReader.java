package wres.io.reading.usgs;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.io.data.caching.USGSParameters;
import wres.io.utilities.Database;
import wres.util.Strings;

public class USGSParameterReader
{
    private static final Logger
            LOGGER = LoggerFactory.getLogger( USGSParameterReader.class );

    private static final String INSERT =
            "INSERT INTO wres.USGSParameter (name, description, parameter_code, measurement_unit, aggregation)" + System.lineSeparator() +
            "SELECT ?, ?, ?, ?, ?" + System.lineSeparator() +
            "WHERE NOT EXISTS (" + System.lineSeparator() +
            "    SELECT 1" + System.lineSeparator() +
            "    FROM wres.USGSParameter" + System.lineSeparator() +
            "    WHERE name = ?" + System.lineSeparator() +
            "        AND description = ?" + System.lineSeparator() +
            "        AND parameter_code = ?" + System.lineSeparator() +
            "        AND measurement_unit = ?" + System.lineSeparator() +
            "        AND aggregation = ?" + System.lineSeparator() +
            ");";

    private List<USGSParameters.USGSParameter> parameters;
    private final String fileName;

    private List<USGSParameters.USGSParameter> getParameters()
    {
        if (this.parameters == null)
        {
            this.parameters = new ArrayList<>(  );
        }
        return this.parameters;
    }

    public USGSParameterReader( String fileName)
    {
        this.fileName = fileName;
    }

    public void read() throws IOException
    {
        try ( BufferedReader reader = new BufferedReader( new FileReader( this.fileName ) ))
        {
            reader.readLine();
            String line = "";
            while ((line = reader.readLine()) != null)
            {
                this.getParameters().add( new USGSParameters.USGSParameter( line ) );
            }
        }

        Connection connection = null;
        PreparedStatement statement = null;

        try
        {
            connection = Database.getConnection();
            statement = connection.prepareStatement( USGSParameterReader.INSERT );

            for (USGSParameters.USGSParameter parameter : this.getParameters())
            {
                statement.setString( 1, parameter.getName() );
                statement.setString( 2, parameter.getDescription());
                statement.setString( 3, parameter.getParameterCode());
                statement.setString( 4, parameter.getMeasurementUnit());
                statement.setString( 5, parameter.getAggregation());
                statement.setString( 6, parameter.getName() );
                statement.setString( 7, parameter.getDescription());
                statement.setString( 8, parameter.getParameterCode());
                statement.setString( 9, parameter.getMeasurementUnit());
                statement.setString( 10, parameter.getAggregation());

                statement.addBatch();
                //LOGGER.info( parameter.toString() );
            }

            statement.executeBatch();

            this.setMeasuremenUnitIDS( );
        }
        catch (SQLException e)
        {
            LOGGER.error(Strings.getStackTrace( e ));
        }
        finally
        {
            if (connection != null)
            {
                Database.returnConnection( connection );
            }
        }
    }

    private void setMeasuremenUnitIDS() throws SQLException
    {
        String script = "UPDATE wres.USGSParameter UP" + System.lineSeparator() +
                        "SET measurementunit_id = MU.measurementunit_id" + System.lineSeparator() +
                        "FROM wres.MeasurementUnit MU" + System.lineSeparator() +
                        "WHERE UP.measurement_unit = ?" + System.lineSeparator() +
                        "    AND MU.unit_name = ?;" + System.lineSeparator();

        Connection connection = null;

        PreparedStatement statement = null;

        try
        {
            connection = Database.getConnection();
            statement = connection.prepareStatement( script );

            statement.setString( 1, "m3/sec" );
            statement.setString( 2, "CMS");

            statement.addBatch();

            statement.setString( 1, "ft3/s" );
            statement.setString( 2, "CFS");

            statement.addBatch();

            statement.setString( 1, "deg F" );
            statement.setString( 2, "F");

            statement.addBatch();

            statement.setString( 1, "deg C" );
            statement.setString( 2, "C");

            statement.addBatch();

            statement.setString( 1, "in" );
            statement.setString( 2, "IN");

            statement.addBatch();

            statement.setString( 1, "m" );
            statement.setString( 2, "M");

            statement.addBatch();

            statement.setString( 1, "mm" );
            statement.setString( 2, "MM");

            statement.addBatch();

            statement.setString( 1, "ft" );
            statement.setString( 2, "FT");

            statement.addBatch();

            statement.setString( 1, "minutes" );
            statement.setString( 2, "M");

            statement.addBatch();

            statement.setString( 1, "%" );
            statement.setString( 2, "%");

            statement.addBatch();

            statement.setString( 1, "ft/sec" );
            statement.setString( 2, "ft/sec");

            statement.addBatch();

            statement.setString( 1, "gal/min" );
            statement.setString( 2, "gal/min");

            statement.addBatch();

            statement.setString( 1, "mgd" );
            statement.setString( 2, "mgd");

            statement.addBatch();

            statement.setString( 1, "m/sec" );
            statement.setString( 2, "m/sec");

            statement.addBatch();

            statement.setString( 1, "ac-ft" );
            statement.setString( 2, "ac-ft");

            statement.addBatch();

            statement.setString( 1, "mph" );
            statement.setString( 2, "mph");

            statement.addBatch();

            statement.setString( 1, "l/sec" );
            statement.setString( 2, "l/sec");

            statement.addBatch();

            statement.setString( 1, "ft3/day" );
            statement.setString( 2, "ft3/day");

            statement.addBatch();

            statement.executeBatch();
            statement.close();
        }
        catch (SQLException e)
        {
            LOGGER.error("USGS Parameters could not be linked to their units of measurement.");
            LOGGER.error("Please try again.");
            throw e;
        }
        finally
        {
            if (connection != null)
            {
                Database.returnConnection( connection );
            }
        }
    }
}
