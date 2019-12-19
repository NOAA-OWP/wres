package wres.io.retrieval;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.DoubleUnaryOperator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.MissingValues;
import wres.io.utilities.DataProvider;
import wres.io.utilities.DataScripter;
import wres.io.utilities.ScriptBuilder;

/**
 * A collection of functions for mapping between measurement units, as recognized by the WRES database. Construct 
 * with a desired measurement unit then call {@link #getUnitMapper(Integer)} with the existing unit, identified with 
 * its <code>measurementunit_id</code>, which is known at conversion time. In other words, this class contains a cache 
 * of all possible unit conversions for the desired unit supplied on construction. Create one instance of this class
 * per evaluation and inject wherever a unit conversion is needed.
 * 
 * @author james.brown@hydrosolved.com
 */

public class UnitMapper
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
     * A mapping between the names of existing measurement units and their corresponding 
     * <code>measurementunit_id</code>
     */

    private final Map<String, Integer> namesToIdentifiers = new HashMap<>();

    /**
     * Desired measurement units.
     */

    private final String desiredMeasurementUnit;

    /**
     * The <code>measurementunit_id</code> for the {@link #desiredMeasurementUnit},
     */

    private final Integer desiredMeasurementUnitId;

    /**
     * Returns an instance.
     * 
     * @param desiredMeasurementUnit the desired units
     * @return an instance
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

        // Identity
        if ( measurementUnitId.equals( this.desiredMeasurementUnitId ) )
        {
            return in -> in;
        }

        if ( !this.conversions.containsKey( measurementUnitId ) )
        {
            throw new NoSuchUnitConversionException( "There is no such unit conversion function to "
                                                     + "'"
                                                     + this.desiredMeasurementUnit
                                                     + "' for the measurementunit_id '"
                                                     + measurementUnitId
                                                     + "'." );
        }

        return this.conversions.get( measurementUnitId );
    }

    /**
     * Returns a unit mapper for the name of an existing measurement unit.
     * 
     * @param unitName the name of an existing measurement unit
     * @throws NoSuchUnitConversionException if there is no conversion for the supplied unitName
     */

    DoubleUnaryOperator getUnitMapper( String unitName )
    {
        Objects.requireNonNull( unitName, "Specify a non-null measurement unit name for conversion." );

        String upperCaseUnits = unitName.toUpperCase();

        // Identity
        if ( upperCaseUnits.equalsIgnoreCase( this.desiredMeasurementUnit ) )
        {
            return in -> in;
        }

        if ( !this.namesToIdentifiers.containsKey( upperCaseUnits ) )
        {
            throw new NoSuchUnitConversionException( "There is no such unit conversion function to "
                                                     + "'"
                                                     + this.desiredMeasurementUnit
                                                     + "' for the unit name '"
                                                     + upperCaseUnits
                                                     + "'." );
        }

        Integer identifier = this.namesToIdentifiers.get( upperCaseUnits );
        return this.conversions.get( identifier );
    }

    /**
     * Hidden constructor.
     * 
     * @param desiredMeasurementUnit the desired units
     * @throws NullPointerException if the input is null
     * @throws DataAccessException if the data could not be accessed
     * @throws NoSuchUnitConversionException if no unit conversions were available
     */

    private UnitMapper( String desiredMeasurementUnit )
    {
        Objects.requireNonNull( desiredMeasurementUnit,
                                "Specify a desired measurement unit to create unit "
                                                        + "conversions." );

        // #65972
        if ( desiredMeasurementUnit.isBlank() )
        {
            throw new NoSuchUnitConversionException( "The desired measurement unit is blank. There are "
                                                     + "no appropriate unit conversions for the blank unit." );
        }

        this.desiredMeasurementUnit = desiredMeasurementUnit.toUpperCase();

        // Create the retrieval script      
        ScriptBuilder scripter = new ScriptBuilder();

        scripter.addLine( "WITH units AS (" );
        scripter.addTab().addLine( "SELECT UC.*" );
        scripter.addTab().addLine( "FROM wres.UnitConversion UC" );
        scripter.addTab().addLine( "INNER JOIN wres.MeasurementUnit M" );
        scripter.addTab( 2 ).addLine( "ON M.measurementunit_id = UC.to_unit" );
        // Check for both upper and lower case unit names
        scripter.addTab()
                .addLine( "WHERE M.unit_name IN ('",
                          this.desiredMeasurementUnit,
                          "', '",
                          this.desiredMeasurementUnit.toLowerCase(),
                          "')" );
        scripter.addLine( ")" );
        scripter.addLine( "SELECT M.unit_name AS from_unit_name, units.*" );
        scripter.addLine( "FROM wres.MeasurementUnit M" );
        scripter.addLine( "INNER JOIN units" );
        scripter.addTab().addLine( "ON units.from_unit = M.measurementunit_id" );

        String script = scripter.toString();

        if ( LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( "Created the following script to retrieve measurement unit conversions from the database:"
                          + "{}{}",
                          System.lineSeparator(),
                          script );
        }

        DataScripter dataScripter = new DataScripter( script );

        // Set high priority
        dataScripter.setHighPriority( true );

        // Retrieve the conversions
        try ( DataProvider provider = dataScripter.buffer() )
        {
            Integer desiredUnitId = null;
            while ( provider.next() )
            {
                Integer fromUnitId = provider.getInt( "from_unit" );
                desiredUnitId = provider.getInt( "to_unit" );
                String fromUnitName = provider.getString( "from_unit_name" );
                double initialOffset = provider.getDouble( "initial_offset" );
                double finalOffset = provider.getDouble( "final_offset" );
                double factor = provider.getDouble( "factor" );

                // Converted value or missing value when the input is not finite
                DoubleUnaryOperator mapper =
                        input -> Double.isFinite( input ) ? ( input + initialOffset ) * factor + finalOffset
                                                          : MissingValues.DOUBLE;

                this.conversions.put( fromUnitId, mapper );
                this.namesToIdentifiers.put( fromUnitName.toUpperCase(), fromUnitId );
            }

            this.desiredMeasurementUnitId = desiredUnitId;

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
