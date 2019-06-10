package wres.io.data.caching;

import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.io.utilities.DataProvider;
import wres.io.utilities.DataScripter;

/**
 * A cache for USGS parameter metadata
 * <br><br>
 * Since interaction with this cache is so simple, there is no need to inherit capabilities from  {@link Cache}
 */
public class USGSParameters
{

    private static final Logger LOGGER = LoggerFactory.getLogger( USGSParameters.class );

    private USGSParameters() {}

    /**
     * A three-tuple key used to index USGS Parameter details
     * <br><br>
     * The three tuple is used because parameters are unique along those lines;
     * there will be several that have the same name and measurement unit, but different aggregations
     */
    private static class ParameterKey implements Comparable<ParameterKey>
    {
        /**
         * Constructor
         * @param name USGS' name for the parameter
         * @param measurementUnit The WRES measurement unit ID for the unit that USGS measures the parameter in
         * @param aggregation An optional aggregation identifier for the parameter ('mean', 'sum', etc)
         */
        ParameterKey(String name, String measurementUnit, String aggregation)
        {
            this.name = name;
            this.measurementUnit = measurementUnit;
            this.aggregation = aggregation;
        }

        /**
         * USGS' name for the parameter
         */
        private final String name;

        /**
         * The WRES measurement unit ID for the unit that USGS measures the parameter in
         */
        private final String measurementUnit;

        /**
         * An optional aggregation identifier for the parameter ('mean', 'sum', etc)
         */
        private final String aggregation;

        @Override
        public boolean equals( Object obj )
        {
            if (obj instanceof ParameterKey)
            {
                ParameterKey otherKey = ( ParameterKey ) obj;

                // TODO: This isn't valid - this will give us '00060' if we indicate '00061' because they both have the name streamflow
                boolean equal = StringUtils.equalsIgnoreCase( this.name, otherKey.name );
                equal = equal || StringUtils.equalsIgnoreCase( this.measurementUnit, otherKey.measurementUnit );
                equal = equal || StringUtils.equalsIgnoreCase( this.aggregation, otherKey.aggregation );

                return equal;
            }

            return false;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(name.toLowerCase(),
                                measurementUnit.toLowerCase(),
                                aggregation.toLowerCase());
        }

        @Override
        public int compareTo( ParameterKey parameterKey )
        {
            if (parameterKey == null)
            {
                return 1;
            }

            return Integer.compare( this.hashCode(), parameterKey.hashCode() );
        }
    }

    /**
     * The parameter that gets cached
     * <br><br>
     * The parameter doesn't need all of the logic of a {@link wres.io.data.details.CachedDetail},
     * so it is defined here instead
     */
    public static class USGSParameter
    {
        /**
         * Creates a parameter from a line in a '|' delimited CSV file
         * @param line a line from a '|' delimited CSV file
         */
        public USGSParameter( String line)
        {
            String[] lineParts = line.split("\\|");

            this.description = lineParts[0].replaceAll( "\"", "" );
            this.parameterCode = lineParts[1].replaceAll( "\"", "" );
            this.name = lineParts[2].replaceAll( "\"", "" );
            this.measurementUnit = lineParts[3].replaceAll("\"", "");
            this.aggregation = lineParts[4].replaceAll("\"", "");
            this.measurementUnitID = Integer.parseInt( lineParts[5].replaceAll("\"", "") );
        }

        /**
         * Creates a parameter from an entry in a DataProvider
         * @param data A DataProvider that supposedly holds information about USGS Parameters
         */
        public USGSParameter (DataProvider data)
        {
            this.name = data.getString("name");
            this.description = data.getString( "description" );
            this.parameterCode = data.getString("parameter_code");
            this.measurementUnit = data.getString("measurement_unit");
            this.aggregation = data.getString("aggregation");
            this.measurementUnitID = data.getInt( "measurementunit_id" );
        }

        private USGSParameter( String name,
                               String description,
                               String parameterCode,
                               String measurementUnit,
                               String aggregation,
                               int measurementUnitID )
        {
            this.name = name;
            this.description = description;
            this.parameterCode = parameterCode;
            this.measurementUnit = measurementUnit;
            this.aggregation = aggregation;
            this.measurementUnitID = measurementUnitID;
        }

        @Override
        public String toString()
        {
            return "Name: '" + this.name + "', " +
                   "Description: '" + this.description + "', " +
                   "Code: " + this.parameterCode + ", " +
                   "Aggregated as: " + this.aggregation + ", " +
                   "Measurement Unit: " + this.measurementUnit;
        }

        /**
         * Gets the human and configuration friendly name specified for and by the WRES
         * 
         * @return a friendly name
         */
        public String getName()
        {
            return name;
        }

        /**
         * Gets the USGS description for the parameter
         * 
         * @return a parameter description
         */
        public String getDescription()
        {
            return description;
        }

        /**
         * Gets the five digit parameter code
         * 
         * @return a parameter code
         */
        public String getParameterCode()
        {
            return parameterCode;
        }

        /**
         * Gets some description for how the data was accumulated
         * 
         * @return the aggregation
         */
        public String getAggregation()
        {
            return aggregation;
        }

        /**
         * Gets the name of the unit of measurement that USGS says the data is measured in
         * <br><br>
         * There's a chance that the unit that USGS uses isn't mapped to a unit utilized by the WRES
         * 
         * @return the measurement unit string
         */
        public String getMeasurementUnit()
        {
            return measurementUnit;
        }

        /**
         * Gets the WRES ID for the unit of measurement that USGS measures the data in
         * 
         * @return the measurement unit identifier
         */
        public Integer getMeasurementUnitID()
        {
            return measurementUnitID;
        }

        /**
         * @return the parameter key
         */
        
        public ParameterKey getKey()
        {
            return new ParameterKey( this.getName(),
                                     this.getMeasurementUnit(),
                                     this.getAggregation() );
        }

        /**
         * The human and configuration friendly name specified for and by the WRES
         */
        private final String name;

        /**
         * The USGS description for the parameter
         */
        private final String description;

        /**
         * The five digit parameter code
         */
        private final String parameterCode;

        /**
         * Some description for how the data was accumulated
         */
        private final String aggregation;

        /**
         * The name of the unit of measurement that USGS says the data is measured in
         * <br><br>
         * There's a chance that the unit that USGS uses isn't mapped to a unit utilized by the WRES
         */
        private final String measurementUnit;

        /**
         * The WRES ID for the unit of measurement that USGS measures the data in
         */
        private final int measurementUnitID;
    }

    /**
     * Locks modification access to the cache
     */
    private static final Object PARAMETER_LOCK = new Object();

    /**
     * The cache used to access USGS parameter data
     */
    private static final ConcurrentMap<ParameterKey, USGSParameter> PARAMETER_STORE = new ConcurrentSkipListMap<>(  );
    /**
     * <p>Invalidates the global cache of the singleton associated with this class, {@link #PARAMETER_STORE}.
     *
     * <p>See #61206.
     */

    public static void invalidateGlobalCache()
    {
        synchronized ( PARAMETER_LOCK )
        {
            if( Objects.nonNull( PARAMETER_STORE ) )
            {
                USGSParameters.PARAMETER_STORE.clear();
            }
        }
    }

    /**
     * Accessor and lazy initializer for the USGS Parameter cache
     * @return A map containing USGS Parameter data
     * @throws SQLException Thrown if the cache could not be populated
     */
    private static ConcurrentMap<ParameterKey, USGSParameter> getParameterStore()
            throws SQLException
    {
        synchronized ( PARAMETER_LOCK )
        {
            if (USGSParameters.PARAMETER_STORE.isEmpty())
            {
                USGSParameters.populate();
            }

            return USGSParameters.PARAMETER_STORE;
        }
    }

    /**
     * Loads USGS Parameter to WRES Mapping data into the cache
     * @throws SQLException Thrown if an exception was encountered while communicating with the database
     */
    private static void populate() throws SQLException
    {
        DataScripter script = new DataScripter( "SELECT * FROM wres.USGSParameter;" );

        // Use a high priority connection so that this isn't blocked by another query
        script.setHighPriority( true );

        try (DataProvider data = script.getData())
        {
            while ( data.next() )
            {
                USGSParameter parameter = new USGSParameter( data );
                USGSParameters.PARAMETER_STORE.putIfAbsent( parameter.getKey(), parameter );
            }
        }

        LOGGER.debug( "Finished populating the USGSParameters details." );
    }

    /**
     * Gets USGS parameter metadata based on the USGS parameter code
     * @param code The five digit USGS parameter code to get the metadata for (such as '00060')
     * @return USGS Parameter metadata
     * @throws SQLException Thrown if the needed data could not be loaded and read from the cache
     */
    public static USGSParameter getParameterByCode(final String code)
            throws SQLException
    {
        USGSParameter foundParameter = null;

        for (USGSParameter parameter : USGSParameters.getParameterStore().values())
        {
            if (parameter.getParameterCode().equals(code))
            {
                foundParameter = parameter;
                break;
            }
        }

        return foundParameter;
    }

    /**
     * Gets USGS parameter metadata based on its name and what it was measured in. Assumes that there
     * shouldn't be a defined data accumulation method (i.e. nothing like 'mean' or 'sum')
     * @param parameterName The name of the parameter to use (like 'streamflow')
     * @param measurementUnit The unit that USGS measures the parameter in
     * @return USGS Parameter metadata
     * @throws SQLException Thrown if the needed data could not be loaded and read from the cache
     */
    public static USGSParameter getParameter(String parameterName, String measurementUnit)
            throws SQLException
    {
        return USGSParameters.getParameter( parameterName, measurementUnit, "None" );
    }

    public static USGSParameter getParameter(String parameterName, String measurementUnit, String aggregationMethod)
            throws SQLException
    {
        USGSParameter parameter;

        ParameterKey key = new ParameterKey( parameterName, measurementUnit, aggregationMethod );

        if (USGSParameters.getParameterStore().containsKey( key ))
        {
            parameter = USGSParameters.getParameterStore().get( key );
        }
        else
        {
            String message = "There is not a known USGS parameter with the name '" +
                             parameterName +
                             "' and a measurement unit of " +
                             measurementUnit;

            if (aggregationMethod.equalsIgnoreCase( "none" ))
            {
                message += " that is not aggregated.";
            }
            else
            {
                message += " that is aggregated by " + aggregationMethod;
            }

            throw new IllegalArgumentException( message );

        }

        return parameter;
    }

    public static USGSParameter getParameterByDescription(String description)
            throws SQLException
    {
        USGSParameter matchingParameter = null;

        for (USGSParameter parameter : USGSParameters.getParameterStore().values())
        {
            if (parameter.getDescription().equalsIgnoreCase( description ))
            {
                matchingParameter = parameter;
                break;
            }
        }

        return matchingParameter;
    }

    /**
     * Adds USGS parameter metadata received via ingest to the database
     * <br><br>
     * If a user states that they want '74072' and we don't have that entry, this is what stores
     * the metadata that will be used
     * @param name The USGS name for the parameter
     * @param code The USGS parameter code for the parameter
     * @param description USGS' description of the parameter
     * @param measurementUnit The unit of measurement that USGS uses for the parameter
     * @return USGS Parameter metadata for the new parameter
     * @throws SQLException if the parameter cannot be retrieved from the database
     */
    public static USGSParameter addRequestedParameter(
            final String name,
            final String code,
            final String description,
            final String measurementUnit)
            throws SQLException
    {
        final String aggregation = "None";

        synchronized ( USGSParameters.PARAMETER_LOCK )
        {
            // USGS often gives complex names to their parameters, often of the name: "Simple name, some description"
            // This takes that name and shortens it down to a reasonable parameter to reference. In this case,
            // it will be "Simple name"
            String usgsName = name.split( "," )[0].strip();
            int measurementUnitId =
                    MeasurementUnits.getMeasurementUnitID( measurementUnit );

            DataScripter script = new DataScripter();
            script.retryOnSqlState( "40001" );
            script.retryOnSqlState( "23505" );
            script.setUseTransaction( true );
            script.addLine( "INSERT INTO wres.USGSParameter(" );
            script.addTab().addLine( "measurementunit_id," );
            script.addTab().addLine( "aggregation," );
            script.addTab().addLine( "name," );
            script.addTab().addLine( "description," );
            script.addTab().addLine( "parameter_code," );
            script.addTab().addLine( "measurement_unit" );
            script.addLine( ")" );
            script.addLine( "SELECT ?, ?, ?, ?, ?, ?" );
            // Do measurementUnitId first because Query's attempt to get
            // inserted ids will fail if it's not a long.
            script.addArgument( measurementUnitId );
            // It might be possible to use the litany at usgs instead:
            // https://help.waterdata.usgs.gov/code/stat_cd_nm_query?stat_nm_cd=%250%25&fmt=html
            script.addArgument( aggregation );
            script.addArgument( usgsName );
            script.addArgument( description );
            script.addArgument( code );
            script.addArgument( measurementUnit );
            script.addLine( "WHERE NOT EXISTS (" );
            script.addTab().addLine( "SELECT 1");
            script.addTab().addLine( "FROM wres.USGSParameter" );
            script.addTab().addLine( "WHERE name = ?" );
            script.addArgument( usgsName );
            script.addTab( 2 ).addLine( "AND description = ?" );
            script.addArgument( description );
            script.addTab( 2 ).addLine( "AND parameter_code = ?" );
            script.addArgument( code );
            script.addTab( 2 ).addLine( "AND measurement_unit = ?" );
            script.addArgument( measurementUnit );
            script.addTab( 2 ).addLine( "AND measurementunit_id = ?" );
            script.addArgument( measurementUnitId );
            script.addTab( 2 ).addLine( "AND aggregation = ?" );
            script.addArgument( aggregation );
            script.addLine( ");" );

            script.execute();

            // There is no surrogate key, the table has only the values above.
            USGSParameter parameter = new USGSParameter( name,
                                                         description,
                                                         code,
                                                         measurementUnit,
                                                         aggregation,
                                                         measurementUnitId );

            USGSParameters.getParameterStore()
                          .putIfAbsent( parameter.getKey(), parameter );

            return parameter;
        }
    }
}
