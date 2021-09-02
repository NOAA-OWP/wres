package wres.io.retrieval;

import java.sql.Connection;
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
import wres.io.utilities.Database;

/**
 * A collection of functions for mapping between measurement units, as recognized by the WRES database. Construct 
 * with a desired measurement unit then call {@link #getUnitMapper(Long)} with the existing unit, identified with
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

    private final Map<Long, DoubleUnaryOperator> conversions = new HashMap<>();

    /**
     * A mapping between the names of existing measurement units and their corresponding 
     * <code>measurementunit_id</code>
     */

    private final Map<String, Long> namesToIdentifiers = new HashMap<>();

    /**
     * Desired measurement units.
     */

    private final String desiredMeasurementUnit;

    /**
     * The <code>measurementunit_id</code> for the {@link #desiredMeasurementUnit},
     */

    private final Long desiredMeasurementUnitId;

    /**
     * Returns an instance.
     *
     * @param database The database to use.
     * @param desiredMeasurementUnit the desired units
     * @return an instance
     * @throws NullPointerException if the input is null
     * @throws DataAccessException if the data could not be accessed 
     */

    public static UnitMapper of( Database database,
                                 String desiredMeasurementUnit )
    {
        return new UnitMapper( database, desiredMeasurementUnit );
    }

    /**
     * Returns a unit mapper for the name of an existing measurement unit.
     * 
     * @param unitName the name of an existing measurement unit
     * @return a unit mapper for the prescribed, existing, units
     * @throws NoSuchUnitConversionException if there is no conversion for the supplied unitName
     */

    public DoubleUnaryOperator getUnitMapper( String unitName )
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

        Long identifier = this.namesToIdentifiers.get( upperCaseUnits );
        return this.conversions.get( identifier );
    }

    /**
     * Returns the name of the desired measurement unit for which the mapper was constructed.
     * 
     * @return the name of the desired measurement unit
     */

    public String getDesiredMeasurementUnitName()
    {
        return this.desiredMeasurementUnit;
    }

    /**
     * Returns a unit mapper for the existing <code>measurementunit_id</code> provided.
     * 
     * @param measurementUnitId the existing <code>measurementunit_id</code>
     * @return a unit mapper for the prescribed, existing, units
     * @throws NoSuchUnitConversionException if there is no conversion for the supplied <code>measurementunit_id</code>
     */

    DoubleUnaryOperator getUnitMapper( Long measurementUnitId )
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
     * Hidden constructor.
     * 
     * @param desiredMeasurementUnit the desired units
     * @throws NullPointerException if the input is null
     * @throws DataAccessException if the data could not be accessed
     * @throws NoSuchUnitConversionException if no unit conversions were available
     */

    private UnitMapper( Database database, String desiredMeasurementUnit )
    {
        Objects.requireNonNull( database );
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
        DataScripter dataScripter = new DataScripter( database );

        dataScripter.addLine( "WITH units AS (" );
        dataScripter.addTab().addLine( "SELECT UC.*" );
        dataScripter.addTab().addLine( "FROM wres.UnitConversion UC" );
        dataScripter.addTab().addLine( "INNER JOIN wres.MeasurementUnit M" );
        dataScripter.addTab( 2 ).addLine( "ON M.measurementunit_id = UC.to_unit" );
        // Check for both upper and lower case unit names
        dataScripter.addTab()
                    .addLine( "WHERE M.unit_name IN (?,?)" );
        dataScripter.addLine( ")" );
        dataScripter.addLine( "SELECT M.unit_name AS from_unit_name, units.*" );
        dataScripter.addLine( "FROM wres.MeasurementUnit M" );
        dataScripter.addLine( "INNER JOIN units" );
        dataScripter.addTab().addLine( "ON units.from_unit = M.measurementunit_id" );

        // Add the arguments
        dataScripter.addArgument( this.desiredMeasurementUnit )
                    .addArgument( this.desiredMeasurementUnit.toLowerCase() );

        if ( LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( "Created the following script to retrieve measurement unit conversions from the database: {}"
                          + "{}The script was built as a prepared statement with the following list of parameters: {}.",
                          System.lineSeparator(),
                          dataScripter,
                          dataScripter.getParameterStrings() );
        }

        // Set high priority
        dataScripter.setHighPriority( true );

        // Retrieve the conversions
        try ( Connection connection = database.getConnection();
              DataProvider provider = dataScripter.buffer( connection ) )
        {
            Long desiredUnitId = null;
            while ( provider.next() )
            {
                Long fromUnitId = provider.getLong( "from_unit" );
                desiredUnitId = provider.getLong( "to_unit" );
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
