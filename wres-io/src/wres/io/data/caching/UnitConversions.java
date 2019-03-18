package wres.io.data.caching;

import java.sql.SQLException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.io.utilities.DataProvider;
import wres.io.utilities.DataScripter;

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

                equal = this.toUnitName.toLowerCase().equalsIgnoreCase( other.toUnitName.toLowerCase() );

                equal = equal && this.fromUnitID == other.fromUnitID;
            }

            return equal;
        }

        @Override
        public String toString()
        {
            return this.fromUnitName + " -> " + this.toUnitName;
        }

        private final String toUnitName;
        private final int fromUnitID;
        private final String fromUnitName;

        private ConversionKey( String toUnitName, int fromUnitID )
        {
            this.toUnitName = toUnitName.toLowerCase();
            this.fromUnitID = fromUnitID;
            this.fromUnitName = MeasurementUnits.getNameByID( this.fromUnitID );
        }

        private ConversionKey (DataProvider data)
        {
            String unitName = data.getValue( "unit_name" );
            this.toUnitName = unitName.toLowerCase();
            this.fromUnitID = data.getValue( "from_unit" );
            this.fromUnitName = MeasurementUnits.getNameByID( this.fromUnitID );
        }
    }

    private static final Object CACHE_LOCK = new Object();
    private final Map<ConversionKey, Conversion> conversionMap;

    private static final UnitConversions INSTANCE = new UnitConversions();
    
    /**
     * <p>Invalidates the global cache of the singleton associated with this class, {@link #INSTANCE}.
     * 
     * <p>See #61206.
     */
    
    public static void invalidateGlobalCache()
    {
        synchronized ( CACHE_LOCK )
        {
            if( Objects.nonNull( INSTANCE.conversionMap ) )
            {
                UnitConversions.INSTANCE.conversionMap.clear();
            }
        }
    }

    private static UnitConversions getCache ()
    {
        synchronized (CACHE_LOCK)
        {
            if ( INSTANCE.conversionMap.isEmpty())
            {
                UnitConversions.initialize();
            }
            return INSTANCE;
        }
    }

    private UnitConversions()
    {
        this.conversionMap = new ConcurrentHashMap<>(  );
    }

    private void populate(DataProvider data)
    {
        data.consume(
                row -> this.conversionMap.put(new ConversionKey( row ),
                                              new Conversion( row ))
        );
    }

    /**
     * Loads all unit conversions for later use
     */
    public static void initialize()
    {
        DataScripter script = new DataScripter(  );
        script.setHighPriority( true );

        script.addLine("SELECT UC.*, M.unit_name");
        script.addLine("FROM wres.UnitConversion UC");
        script.addLine("INNER JOIN wres.MeasurementUnit M");
        script.addTab().add("ON M.measurementunit_id = UC.to_unit;");

        try(DataProvider data = script.getData())
        {
            INSTANCE.populate( data );
        }
        catch ( SQLException e )
        {
            // Failure to pre-populate cache should not affect primary outputs.
            LOGGER.warn( "Failed to pre-populate unit conversions cache.", e );
        }
        
        LOGGER.debug( "Finished populating the UnitConversion details." );
    }

    public static double convert(final double value, final String fromMeasurementUnit, final String toMeasurementUnit)
            throws SQLException
    {
        if (Double.isNaN(value) || fromMeasurementUnit.equalsIgnoreCase( toMeasurementUnit ))
        {
            return value;
        }

        return UnitConversions.convert(
                value,
                MeasurementUnits.getMeasurementUnitID( fromMeasurementUnit ),
                toMeasurementUnit
        );
    }

    public static double convert(double value, int fromMeasurementUnitID, String toMeasurementUnit)
    {
        Conversion conversion = getConversion( fromMeasurementUnitID, toMeasurementUnit );
        return conversion.convert(value);
    }

    public static Conversion getConversion(final int fromID, final String desiredName)
    {
        Conversion conversion = getCache().conversionMap.get(new ConversionKey( desiredName, fromID ));
        Objects.requireNonNull( conversion,
                                "No valid conversion could be found between " +
                                MeasurementUnits.getNameByID( fromID ) + " and " + desiredName);
        return conversion;
    }

    /**
     * Provides the operation that will convert a value to another unit
     */
    public static class Conversion
    {
        private final double factor;
        private final double initialOffset;
        private final double finalOffset;

        Conversion (DataProvider data)
        {
            this.factor = data.getDouble("factor");
            this.initialOffset = data.getDouble("initial_offset");
            this.finalOffset = data.getDouble("final_offset");
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
