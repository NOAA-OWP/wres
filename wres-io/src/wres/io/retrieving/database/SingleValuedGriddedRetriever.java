package wres.io.retrieving.database;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.yaml.components.Variable;
import wres.datamodel.space.Feature;
import wres.datamodel.time.TimeSeries;
import wres.reading.netcdf.grid.GridRequest;
import wres.reading.netcdf.grid.GridReader;
import wres.datamodel.DataProvider;
import wres.io.database.DataScripter;
import wres.io.database.Database;
import wres.io.database.ScriptBuilder;
import wres.io.retrieving.DataAccessException;

/**
 * Retrieves {@link TimeSeries} of single-valued observations or forecasts from gridded sources.
 *
 * @author James Brown
 */

class SingleValuedGriddedRetriever extends TimeSeriesRetriever<Double>
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( SingleValuedGriddedRetriever.class );

    /** Exception message used several times on construction. */
    private static final String CANNOT_BUILD_A_TIME_SERIES_RETRIEVER_WITHOUT_A =
            "Cannot build a time-series retriever without a ";

    /** Error message when attempting to retrieve by identifier. See #68334 and #56214-56. */
    private static final String NO_IDENTIFIER_ERROR = "Retrieval of gridded time-series by identifier is not "
                                                      + "currently possible.";

    /** Start of script. */
    private static final String GET_START_OF_SCRIPT =
            SingleValuedGriddedRetriever.getStartOfScriptForGetAllTimeSeries();

    /** Complete script. */
    private final DataScripter script;

    /** Is <code>true</code> to retrieve a forecast type, <code>false</code> for a non-forecast type. */
    private final Boolean isForecast;

    @Override
    boolean isForecast()
    {
        return isForecast;
    }

    /**
     * Builder.
     */

    static class Builder extends TimeSeriesRetriever.Builder<Double>
    {

        /**
         * Is <code>true</code> to retrieve a forecast type, <code>false</code> for a non-forecast type. 
         */

        private Boolean isForecast;

        /**
         * Is <code>true</code> to retrieve a forecast type, <code>false</code> for a non-forecast type.
         *
         * @param isForecast is true to retrieve forecast data, otherwise false
         * @return the builder
         */

        Builder setIsForecast( Boolean isForecast )
        {
            this.isForecast = isForecast;
            return this;
        }

        /**
         * Builds an instance.
         *
         * @return the instance
         */
        SingleValuedGriddedRetriever build()
        {
            return new SingleValuedGriddedRetriever( this );
        }
    }

    /**
     * Reads a time-series by <code>wres.TimeSeries.timeseries_id</code>.
     *
     * @param identifier the <code>wres.TimeSeries.timeseries_id</code>
     * @return a possible time-series for the given identifier
     */

    @Override
    public Optional<TimeSeries<Double>> get( long identifier )
    {
        throw new UnsupportedOperationException( NO_IDENTIFIER_ERROR );
    }

    /**
     * Returns all of the <code>wres.TimeSeries.timeseries_id</code> associated with this instance.
     *
     * @return a stream of<code>wres.TimeSeries.timeseries_id</code>
     */

    @Override
    public LongStream getAllIdentifiers()
    {
        throw new UnsupportedOperationException( NO_IDENTIFIER_ERROR );
    }

    /**
     * Overrides the default implementation to get all specified time-series in one pull, rather than one pull for 
     * each series.
     *
     * @param identifiers the stream of identifiers
     * @return a stream over the identified objects
     * @throws NullPointerException if the input is null
     */
    @Override
    public Stream<TimeSeries<Double>> get( LongStream identifiers )
    {
        throw new UnsupportedOperationException( NO_IDENTIFIER_ERROR );
    }

    /**
     * Overrides the default implementation to get all time-series in one pull, rather than one pull for each series.
     *
     * @return the possible object
     * @throws DataAccessException if the data could not be accessed for whatever reason
     */

    @Override
    public Stream<TimeSeries<Double>> get()
    {
        try
        {
            List<String> paths = this.getPaths();

            if ( paths.isEmpty() )
            {
                LOGGER.debug( "Skipping request for gridded time-series as no paths were discovered for the "
                              + "variable {}, time window {} and features {}.",
                              this.getVariable(),
                              this.getTimeWindow(),
                              this.getFeatures() );

                return Stream.of();
            }

            // Create a grid request for each required variable name
            Variable variable = this.getVariable();
            GridRequest request = this.getRequest( paths, variable.name() );

            Stream<TimeSeries<Double>> series = this.getResponse( request );

            for ( String alias : variable.aliases() )
            {
                GridRequest nextRequest = this.getRequest( paths, alias );
                Stream<TimeSeries<Double>> nextResponse = this.getResponse( nextRequest );
                series = Stream.concat( series, nextResponse );
            }

            // Obtain the response
            return series;
        }
        catch ( IOException | SQLException e )
        {
            throw new DataAccessException( "Failed to access the time-series data.", e );
        }
    }

    /**
     * Returns the path strings.
     *
     * @return the path strings
     * @throws SQLException if the request could not be formed
     */

    List<String> getPaths() throws SQLException
    {
        // Log the script
        super.logScript( this.script );

        try ( Connection connection = this.getDatabase()
                                          .getConnection();
              DataProvider provider = this.script.buffer( connection ) )
        {
            List<String> paths = new ArrayList<>();

            while ( provider.next() )
            {
                paths.add( provider.getString( "path" ) );
            }

            return Collections.unmodifiableList( paths );
        }
    }

    /**
     * Returns the request.
     *
     * @param paths the paths
     * @param variableName the variable name
     * @return the request
     */

    private GridRequest getRequest( List<String> paths,
                                    String variableName )
    {
        return new GridRequest( paths,
                                this.getFeatures(),
                                variableName,
                                this.getTimeWindow(),
                                this.isForecast(),
                                this.getDeclaredExistingTimeScale() );
    }

    /**
     * Returns the time-series response for a given request.
     *
     * @param request the request
     * @return the time-series
     * @throws IOException if the response could not be obtained
     */

    private Stream<TimeSeries<Double>> getResponse( GridRequest request ) throws IOException
    {
        Objects.requireNonNull( request );

        // Obtain the response
        Map<Feature, Stream<TimeSeries<Double>>> response = GridReader.getSingleValuedTimeSeries( request );

        // Pooling all features, since this retriever does not provide a per-feature API
        Stream<TimeSeries<Double>> concatenated = Stream.of();
        for ( Stream<TimeSeries<Double>> next : response.values() )
        {
            concatenated = Stream.concat( concatenated, next );
        }

        return concatenated;
    }

    /**
     * Returns the start of a script to acquire a time-series from the WRES database for all time-series.
     *
     * @return the start of a script for the time-series
     */

    private static String getStartOfScriptForGetAllTimeSeries()
    {
        ScriptBuilder scripter = new ScriptBuilder();

        scripter.addLine( "SELECT path" );
        scripter.addLine( "FROM wres.Source S" );
        scripter.addLine( "INNER JOIN wres.ProjectSource PS" );
        scripter.addTab( 1 ).addLine( "ON PS.source_id = S.source_id" );
        scripter.addLine( "LEFT JOIN wres.TimeSeriesReferenceTime TSRT" );
        scripter.addTab( 1 ).addLine( "ON TSRT.source_id = S.source_id" );
        scripter.addLine( "WHERE PS.project_id = ?" );
        scripter.addTab( 1 ).addLine( "AND PS.member = ?" );
        scripter.addTab( 1 ).addLine( "AND S.is_point_data = FALSE" );

        return scripter.toString();
    }

    /**
     * Construct.
     *
     * @throws NullPointerException if any required input is null
     */

    private SingleValuedGriddedRetriever( Builder builder )
    {
        super( builder, "TSRT.reference_time", "S.lead" );

        this.isForecast = builder.isForecast;

        // Validate
        Objects.requireNonNull( this.getDatasetOrientation(),
                                CANNOT_BUILD_A_TIME_SERIES_RETRIEVER_WITHOUT_A
                                + "dataset orientation." );

        Objects.requireNonNull( this.isForecast,
                                CANNOT_BUILD_A_TIME_SERIES_RETRIEVER_WITHOUT_A + "forecast status." );

        // Build the script
        this.script = this.getScript();
    }

    /**
     * Returns the script for acquiring source paths.
     *
     * @return the script for acquiring source paths
     */

    private DataScripter getScript()
    {
        Database database = super.getDatabase();
        DataScripter dataScripter = new DataScripter( database, GET_START_OF_SCRIPT );

        // Parameters
        dataScripter.addArgument( this.getProjectId() )
                    .addArgument( this.getDatasetOrientation()
                                      .name()
                                      .toLowerCase() );

        // Time window
        this.addTimeWindowClause( dataScripter );

        return dataScripter;
    }

}
