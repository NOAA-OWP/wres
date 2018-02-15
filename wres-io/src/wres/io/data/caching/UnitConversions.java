package wres.io.data.caching;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static wres.io.data.caching.Cache.NEWLINE;

import wres.io.utilities.Database;
import wres.util.NotImplementedException;
import wres.util.Strings;

public final class UnitConversions
{
    private static final Logger LOGGER = LoggerFactory.getLogger(UnitConversions.class);

    private static class ConversionKey
    {
        @Override
        public int hashCode()
        {
            return Objects.hash( this.toUnitName, this.fromUnitID );
        }

        @Override
        public boolean equals( Object obj )
        {
            boolean equal = false;

            if (obj instanceof ConversionKey)
            {
                ConversionKey other = (ConversionKey)obj;

                equal = this.toUnitName.equalsIgnoreCase( other.toUnitName );

                equal = equal && this.fromUnitID == other.fromUnitID;
            }

            return equal;
        }

        private final String toUnitName;
        private final int fromUnitID;

        private ConversionKey( String toUnitName, int fromUnitID )
        {
            this.toUnitName = toUnitName;
            this.fromUnitID = fromUnitID;
        }

        private ConversionKey (ResultSet row) throws SQLException
        {
            this.toUnitName = Database.getValue( row, "unit_name" );
            this.fromUnitID = Database.getValue( row, "from_unit" );
        }
    }

    private static UnitConversions instance = null;
    private static final Object CACHE_LOCK = new Object();
    private final Map<ConversionKey, Conversion> conversionMap;

    private static UnitConversions getCache ()
    {
        synchronized (CACHE_LOCK)
        {
            if ( instance == null)
            {
                instance = new UnitConversions();
            }
            return instance;
        }
    }

    private UnitConversions ()
    {
        this.conversionMap = new ConcurrentHashMap<>(  );
        Connection connection = null;
        ResultSet conversionRows = null;

        try
        {
            String script = "SELECT UC.*, M.unit_name" + NEWLINE;
            script += "FROM wres.UnitConversion UC" + NEWLINE;
            script += "INNER JOIN wres.MeasurementUnit M" + NEWLINE;
            script += "     ON M.measurementunit_id = UC.to_unit;";

            connection = Database.getHighPriorityConnection();
            conversionRows = Database.getResults(connection, script);

            while (conversionRows.next())
            {
                this.conversionMap.put(new ConversionKey( conversionRows ),
                                       new Conversion( conversionRows ));
            }
        }
        catch (SQLException e)
        {
            LOGGER.error(Strings.getStackTrace(e));
        }
        finally
        {
            if (conversionRows != null)
            {
                try
                {
                    conversionRows.close();
                }
                catch (SQLException e)
                {
                    LOGGER.error(Strings.getStackTrace(e));
                }
            }

            if (connection != null)
            {
                Database.returnHighPriorityConnection(connection);
            }
        }
    }

    public static double convert(double value, int fromMeasurementUnitID, String toMeasurementUnit)
    {
        Conversion conversion = getConversion( fromMeasurementUnitID, toMeasurementUnit );

        if (conversion == null)
        {
            throw new NotImplementedException("There is not currently a conversion from the measurement unit " +
                                                      fromMeasurementUnitID +
                                                      " to the measurement unit " +
                                                        toMeasurementUnit);
        }
        return conversion.convert(value);
    }

    public static Conversion getConversion(final int fromID, final String desiredName)
    {
        return getCache().conversionMap.get(new ConversionKey( desiredName, fromID ));
    }

    /**
     * Provides the operation that will convert a value to another unit
     */
    public static class Conversion
    {
        private final double factor;
        private final double initialOffset;
        private final double finalOffset;

        public Conversion (ResultSet row) throws SQLException
        {
            this.factor = row.getDouble("factor");
            this.initialOffset = row.getDouble("initial_offset");
            this.finalOffset = row.getDouble("final_offset");
        }

        /**
         * Uses loaded conversion factors to convert a value to a desired unit
         * @param value The value to convert
         * @return The value in the new unit of measurement
         */
        public double convert(double value)
        {
            return ((value + this.initialOffset) * this.factor) + this.finalOffset;
        }

        @Override
        public String toString()
        {
            return "(({value} + " + this.initialOffset + ") * " +
                   this.factor + ") + " + this.finalOffset;
        }
    }
}
