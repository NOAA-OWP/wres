package wres.io.data.caching;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static wres.io.data.caching.Cache.NEWLINE;

import wres.io.utilities.Database;
import wres.util.Collections;
import wres.util.NotImplementedException;
import wres.util.Strings;

public final class UnitConversions
{
    private static final Logger LOGGER = LoggerFactory.getLogger(UnitConversions.class);

    private static UnitConversions INTERNAL_CACHE = null;
    private static final Object CACHE_LOCK = new Object();
    private final List<Conversion> conversionList;


    private static UnitConversions getCache () {
        synchronized (CACHE_LOCK)
        {
            if (INTERNAL_CACHE == null)
            {
                INTERNAL_CACHE = new UnitConversions();
            }
            return INTERNAL_CACHE;
        }
    }

    private UnitConversions () {
        List<Conversion> conversions = new ArrayList<>();

        Connection connection = null;
        ResultSet conversionRows = null;

        try
        {
            String script = "SELECT UC.*, M.unit_name" + NEWLINE;
            script += "FROM wres.UnitConversion UC" + NEWLINE;
            script += "INNER JOIN wres.MeasurementUnit M" + NEWLINE;
            script += "     ON M.measurementunit_id = UC.to_unit;";

            connection = Database.getHighPriorityConnection();
            conversionRows = Database.getResults(connection, script);//"SELECT * FROM wres.UnitConversion;");

            while (conversionRows.next())
            {
                conversions.add(new Conversion(conversionRows));
            }
        }
        catch (SQLException e) {
            LOGGER.error(Strings.getStackTrace(e));
        }
        finally
        {
            if (conversionRows != null)
            {
                try {
                    conversionRows.close();
                }
                catch (SQLException e) {
                    LOGGER.error(Strings.getStackTrace(e));
                }
            }

            if (connection != null)
            {
                Database.returnHighPriorityConnection(connection);
            }
        }
        this.conversionList = conversions;
    }

    public static double convert(double value, int fromMeasurementUnitID, String toMeasurementUnit) throws SQLException
    {
        //int toMeasurementUnitID = MeasurementUnits.getMeasurementUnitID(toMeasurementUnit);

        Conversion conversion = getConversion( fromMeasurementUnitID, toMeasurementUnit ); //getConversion(fromMeasurementUnitID, toMeasurementUnitID);

        if (conversion == null) {
            throw new NotImplementedException("There is not currently a conversion from the measurement unit " +
                                                      String.valueOf(fromMeasurementUnitID) +
                                                      " to the measurement unit " +
                                                        toMeasurementUnit);
                                                      //String.valueOf(toMeasurementUnitID));
        }
        return conversion.convert(value);
    }

    public static Conversion getConversion(final int fromID, final String desiredName)
    {
        return Collections.find(getCache().conversionList, (Conversion conversion) -> {
           return conversion.getFromID() == fromID && conversion.getDesiredName().equalsIgnoreCase( desiredName );
        });
    }

    private static Conversion getConversion(final int fromID, final int toID)
    {
        return Collections.find(getCache().conversionList, (Conversion conversion) -> {
            return conversion.getFromID() == fromID && conversion.getToID() == toID;
        });
    }

    public static class Conversion
    {
        private final int fromID;
        private final int toID;
        private final String toName;
        private final double factor;
        private final double initial_offset;
        private final double final_offset;

        public Conversion (ResultSet row) throws SQLException
        {
            this.fromID = row.getInt("from_unit");
            this.toID = row.getInt("to_unit");
            this.factor = row.getDouble("factor");
            this.initial_offset = row.getDouble("initial_offset");
            this.final_offset = row.getDouble("final_offset");
            this.toName = row.getString("unit_name");
        }

        public double convert(double value)
        {
            return ((value + this.initial_offset) * this.factor) + this.final_offset;
        }

        public int getFromID()
        {
            return this.fromID;
        }

        public int getToID()
        {
            return this.toID;
        }

        public String getDesiredName()
        {
            return this.toName;
        }
    }
}
