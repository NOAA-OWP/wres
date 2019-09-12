package wres.io.retrieval.datashop;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.DoubleUnaryOperator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.MetricConstants.MissingValues;
import wres.io.utilities.DataProvider;
import wres.io.utilities.DataScripter;
import wres.io.utilities.ScriptBuilder;

/**
 * A collection of functions for mapping between measurement units, as recognized by the WRES database. Supplies a
 * {@link DoubleUnaryOperator} to map between an existing unit, which is known at conversion time, and a desired unit, 
 * which is known on construction. In other words, this class contains a cache of all possible unit conversions for
 * the desired unit supplied on construction. 
 * 
 * double values in a required unit, supplied on construction, from an existing
 * <code>measurementunit_id</code>, which is known at runtime.
 * 
 * @author james.brown@hydrosolved.com
 */

class UnitMapper
{
    /**
     * Logger.
     */

    private static final Logger LOGGER = LoggerFactory.getLogger( UnitMapper.class );

    /**
     * A cache of unit conversions as {@link DoubleUnaryOperator}. Each conversion is stored by the 
     * <code>measurementunit_id</code> of the existing unit, which is used to convert to the desired unit,
     * supplied on construction.
     */

    private final Map<Integer, DoubleUnaryOperator> conversions = new HashMap<>();

    /**
     * Desired measurement units.
     */

    private final String desiredMeasurementUnit;

    /**
     * Returns an instance.
     * 
     * @param desiredMeasurementUnit the desired units
     * @throws NullPointerException if the input is null
     * @throws DataAccessException if the data could not be accessed 
     */
    
    public static UnitMapper of( String desiredMeasurementUnit )
    {
        return new UnitMapper( desiredMeasurementUnit );
    }
    
    /**
     * Returns a unit mapper for the existing <code>measurementunit_id</code> provided.
     * 
     * @param measurementUnitId the existing <code>measurementunit_id</code>
     * @throws NoSuchUnitConversionException if there is no conversion for the supplied <code>measurementunit_id</code>
     */

    DoubleUnaryOperator getUnitMapper( Integer measurementUnitId )
    {
        Objects.requireNonNull( measurementUnitId, "Specify a non-null measurement unit for conversion." );

        if ( !conversions.containsKey( measurementUnitId ) )
        {
            throw new NoSuchUnitConversionException( "There is no such unit conversion function to "
                                                     + "'"
                                                     + desiredMeasurementUnit
                                                     + "' for the measurementunit_id '"
                                                     + measurementUnitId
                                                     + "'.");
        }

        return conversions.get( measurementUnitId );
    }

    /**
     * Hidden constructor.
     * 
     * @param desiredMeasurementUnit the desired units
     * @throws NullPointerException if the input is null
     * @throws DataAccessException if the data could not be accessed
     */

    private UnitMapper( String desiredMeasurementUnit )
    {
        Objects.requireNonNull( desiredMeasurementUnit,
                                "Specify a desired measurement unit to create unit "
                                                        + "conversions." );

        this.desiredMeasurementUnit = desiredMeasurementUnit;

        // Create the retrieval script      
        ScriptBuilder scripter = new ScriptBuilder();

        scripter.addLine( "SELECT UC.*, M.unit_name" );
        scripter.addLine( "FROM wres.UnitConversion UC" );
        scripter.addLine( "INNER JOIN wres.MeasurementUnit M" );
        scripter.addTab().addLine( "ON M.measurementunit_id = UC.to_unit" );
        scripter.addLine( "WHERE M.unit_name = '", desiredMeasurementUnit, "'" );

        String script = scripter.toString();

        if ( LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( "Created the following script to retrieve measurement unit conversions from the database:"
                          + "{}{}",
                          System.lineSeparator(),
                          script );
        }

        DataScripter dataScripter = new DataScripter( script );

        // Retrieve the conversions
        try ( DataProvider provider = dataScripter.buffer() )
        {
            while ( provider.next() )
            {
                Integer measurementUnitId = provider.getInt( "from_unit" );
                double initialOffset = provider.getDouble( "initial_offset" );
                double finalOffset = provider.getDouble( "final_offset" );
                double factor = provider.getDouble( "factor" );

                // Converted value or missing value when the input is not finite
                DoubleUnaryOperator mapper =
                        input -> Double.isFinite( input ) ? ( input + initialOffset ) * factor + finalOffset
                                                          : MissingValues.MISSING_DOUBLE;

                this.conversions.put( measurementUnitId, mapper );
            }

            LOGGER.debug( "Added {} unit conversions to the cache for the desired units of '{}'.",
                          this.conversions.size(),
                          this.desiredMeasurementUnit );
        }
        catch ( SQLException e )
        {
            throw new DataAccessException( "Failed to retrieve measurement unit conversions from the database.", e );
        }

    }

}
