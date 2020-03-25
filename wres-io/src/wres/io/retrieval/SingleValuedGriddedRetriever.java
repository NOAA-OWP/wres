package wres.io.retrieval;

import java.io.IOException;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.Feature;
import wres.datamodel.time.TimeSeries;
import wres.grid.client.Fetcher;
import wres.grid.client.Request;
import wres.grid.client.SingleValuedTimeSeriesResponse;
import wres.datamodel.time.TimeSeriesSlicer;
import wres.io.utilities.DataProvider;
import wres.io.utilities.DataScripter;
import wres.io.utilities.ScriptBuilder;

/**
 * Retrieves {@link TimeSeries} of single-valued observations or forecasts from gridded sources.
 * 
 * @author james.brown@hydrosolved.com
 */

class SingleValuedGriddedRetriever extends TimeSeriesRetriever<Double>
{

    /**
     * Exception message used several times on construction.
     */

    private static final String CANNOT_BUILD_A_TIME_SERIES_RETRIEVER_WITHOUT_A =
            "Cannot build a time-series retriever without a ";

    /**
     * Log message.
     */

    private static final String LOG_SCRIPT =
            "Built retriever {} for the retrieval of single-valued gridded forecasts using this script to obtain "
                                             + "the paths to the gridded data files:{}{}";

    /**
     * Logger.
     */

    private static final Logger LOGGER = LoggerFactory.getLogger( SingleValuedGriddedRetriever.class );

    /**
     * Error message when attempting to retrieve by identifier. See #68334 and #56214-56.
     */

    private static final String NO_IDENTIFIER_ERROR = "Retrieval of gridded time-series by identifier is not "
                                                      + "currently possible.";

    /**
     * Start of script for {@link #getAll()}.
     */

    private static final String GET_START_OF_SCRIPT =
            SingleValuedGriddedRetriever.getStartOfScriptForGetAllTimeSeries();

    /**
     * End of script for {@link #getAll()}.
     */

    private static final String GET_END_OF_SCRIPT =
            SingleValuedGriddedRetriever.getEndOfScriptForGetAllTimeSeries();

    /**
     * Complete script for {@link #getAll()}.
     */

    private final String script;

    /**
     * The variable name.
     */

    private final String variableName;

    /**
     * The features.
     */

    private final List<Feature> features;

    /**
     * Is <code>true</code> to retrieve a forecast type, <code>false</code> for a non-forecast type. 
     */

    private final Boolean isForecast;

    /**
     * The request.
     */

    private Request request = null;

    @Override
    boolean isForecast()
    {
        return isForecast;
    }

    /**
     * Builder.
     */

    static class Builder extends TimeSeriesRetrieverBuilder<Double>
    {

        /**
         * The features.
         */

        private List<Feature> features;

        /**
         * The variable name.
         */

        private String variableName;

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
         * Sets the features.
         * 
         * @param features the features
         * @return the builder
         */

        Builder setFeatures( List<Feature> features )
        {
            this.features = features;
            return this;
        }

        /**
         * Sets the variable name.
         * 
         * @param variableName the variable name
         * @return the builder
         */

        Builder setVariableName( String variableName )
        {
            this.variableName = variableName;
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
            // Build the request object
            if ( Objects.isNull( this.request ) )
            {
                this.request = this.getRequest();
            }

            // Obtain the response
            return this.getResponse( this.request );
        }
        catch ( IOException | SQLException e )
        {
            throw new DataAccessException( "Failed to access the time-series data.", e );
        }
    }

    /**
     * Returns the request.
     * 
     * @return the request
     * @throws SQLException if the request could not be formed
     */

    private Request getRequest() throws SQLException
    {
        DataScripter scripter = new DataScripter( this.getDatabase(),
                                                  this.script );

        if ( LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( LOG_SCRIPT,
                          this,
                          System.lineSeparator(),
                          this.script );
        }

        try ( DataProvider provider = scripter.buffer() )
        {
            List<String> paths = new ArrayList<>();

            while ( provider.next() )
            {
                paths.add( provider.getString( "path" ) );
            }

            // Build the request object
            return Fetcher.prepareRequest( paths,
                                           this.getFeatures(),
                                           this.getVariableName(),
                                           this.getTimeWindow(),
                                           this.isForecast(),
                                           this.getDeclaredExistingTimeScale() );

        }
    }

    /**
     * Returns the time-series response for a given request.
     * 
     * @param request the request
     * @return the time-series
     * @throws IOException if the response could not be obtained
     */

    private Stream<TimeSeries<Double>> getResponse( Request request ) throws IOException
    {
        Objects.requireNonNull( request );

        // Obtain the response
        SingleValuedTimeSeriesResponse response = Fetcher.getSingleValuedTimeSeries( request );

        // Get the unit mapper
        UnitMapper mapper = this.getMeasurementUnitMapper();
        String responseUnits = response.getMeasuremenUnits();

        // Map the units, pooling all features, since this retriever does not provide a per-feature API
        List<TimeSeries<Double>> toStream = new ArrayList<>();
        for ( Stream<TimeSeries<Double>> next : response.getTimeSeries().values() )
        {
            // Acquire the unit mapper only when encountering a time-series with one or more events, as all series 
            // may be empty and the units unknown
            List<TimeSeries<Double>> mapped =
                    next.filter( nextSeries -> !nextSeries.getEvents().isEmpty() )
                        .map( in -> TimeSeriesSlicer.transform( in,
                                                                mapper.getUnitMapper( responseUnits )::applyAsDouble ) )
                        .collect( Collectors.toList() );

            toStream.addAll( mapped );
        }

        return toStream.stream();
    }

    /**
     * Returns the variable name.
     * 
     * @return the variable name
     */

    private String getVariableName()
    {
        return this.variableName;
    }

    /**
     * Returns the features.
     * 
     * @return the features
     */

    private List<Feature> getFeatures()
    {
        return this.features;
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
        scripter.addLine( "WHERE S.is_point_data = FALSE" );

        return scripter.toString();
    }

    /**
     * Returns the end of a script to acquire a time-series from the WRES database for all time-series.
     * 
     * @return the end of a script for the time-series
     */

    private static String getEndOfScriptForGetAllTimeSeries()
    {
        ScriptBuilder scripter = new ScriptBuilder();

        scripter.addTab().addLine( "AND EXISTS (" );
        scripter.addTab( 2 ).addLine( "SELECT 1" );
        scripter.addTab( 2 ).addLine( "FROM wres.ProjectSource PS" );
        scripter.addTab( 3 ).addLine( "WHERE PS.source_id = S.source_id" );
        scripter.addTab( 2 ).addLine( "AND PS.project_id = ''{0}''" );
        scripter.addTab( 3 ).addLine( "AND PS.member = ''{1}''" );
        scripter.addTab().addLine( ");" );

        return scripter.toString();
    }

    /**
     * Construct.
     * 
     * @param timeWindow the time window
     * @throws NullPointerException if any required input is null
     */

    private SingleValuedGriddedRetriever( Builder builder )
    {
        super( builder, "S.output_time", "S.lead" );

        this.features = builder.features;
        this.variableName = builder.variableName;
        this.isForecast = builder.isForecast;

        // Validate
        Objects.requireNonNull( this.getProjectId(),
                                CANNOT_BUILD_A_TIME_SERIES_RETRIEVER_WITHOUT_A + "project identifier." );

        if ( Objects.isNull( this.getFeatures() ) || this.getFeatures().isEmpty() )
        {
            throw new NullPointerException( CANNOT_BUILD_A_TIME_SERIES_RETRIEVER_WITHOUT_A + "list of features." );
        }

        Objects.requireNonNull( this.getVariableName(),
                                CANNOT_BUILD_A_TIME_SERIES_RETRIEVER_WITHOUT_A + "variable name." );
        Objects.requireNonNull( this.getLeftOrRightOrBaseline(),
                                CANNOT_BUILD_A_TIME_SERIES_RETRIEVER_WITHOUT_A
                                                                 + "member type (left or right or baseline)." );

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

    private String getScript()
    {
        // Start of the script
        ScriptBuilder scripter = new ScriptBuilder( GET_START_OF_SCRIPT );

        // Time window
        this.addTimeWindowClause( scripter, 0 );

        // End of the script
        String end = MessageFormat.format( GET_END_OF_SCRIPT,
                                           this.getProjectId(),
                                           this.getLeftOrRightOrBaseline()
                                               .value() );
        scripter.addLine( end );

        return scripter.toString();
    }

}
