package wres.io.data.caching;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wres.io.utilities.Database;
import wres.util.Collections;
import wres.util.NotImplementedException;
import wres.util.Strings;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

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
            connection = Database.getHighPriorityConnection();
            conversionRows = Database.getResults(connection, "SELECT * FROM wres.UnitConversion;");

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
        int toMeasurementUnitID = MeasurementUnits.getMeasurementUnitID(toMeasurementUnit);

        Conversion conversion = getConversion(fromMeasurementUnitID, toMeasurementUnitID);

        if (conversion == null) {
            throw new NotImplementedException("There is not currently a conversion from the measurement unit " +
                                                      String.valueOf(fromMeasurementUnitID) +
                                                      " to the measurement unit " +
                                                      String.valueOf(toMeasurementUnitID));
        }
        return conversion.convert(value);
    }

    private static Conversion getConversion(final int fromID, final int toID)
    {
        return Collections.find(getCache().conversionList, (Conversion conversion) -> {
            return conversion.getFromID() == fromID && conversion.getToID() == toID;
        });
    }

    private static class Conversion
    {
        private final int fromID;
        private final int toID;
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
    }
}
