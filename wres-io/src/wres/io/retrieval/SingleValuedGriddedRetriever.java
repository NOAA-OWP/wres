package wres.io.retrieval;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.FeatureTuple;
import wres.datamodel.FeatureKey;
import wres.datamodel.time.TimeSeries;
import wres.grid.client.Fetcher;
import wres.grid.client.Request;
import wres.grid.client.SingleValuedTimeSeriesResponse;
import wres.datamodel.time.TimeSeriesSlicer;
import wres.io.utilities.DataProvider;
import wres.io.utilities.DataScripter;
import wres.io.utilities.Database;
import wres.io.utilities.ScriptBuilder;

/**
 * Retrieves {@link TimeSeries} of single-valued observations or forecasts from gridded sources.
 * 
 * @author james.brown@hydrosolved.com
 */

class SingleValuedGriddedRetriever extends TimeSeriesRetriever<Double>
{

    /** 
     * Logger. 
     * */

    private static final Logger LOGGER = LoggerFactory.getLogger( SingleValuedGriddedRetriever.class );

    /**
     * Exception message used several times on construction.
     */

    private static final String CANNOT_BUILD_A_TIME_SERIES_RETRIEVER_WITHOUT_A =
            "Cannot build a time-series retriever without a ";

    /**
     * Error message when attempting to retrieve by identifier. See #68334 and #56214-56.
     */

    private static final String NO_IDENTIFIER_ERROR = "Retrieval of gridded time-series by identifier is not "
                                                      + "currently possible.";

    /**
     * Start of script.
     */

    private static final String GET_START_OF_SCRIPT =
            SingleValuedGriddedRetriever.getStartOfScriptForGetAllTimeSeries();

    /**
     * Complete script.
     */

    private final DataScripter script;

    /**
     * The features.
     */

    private final List<FeatureTuple> features;

    /**
     * Is <code>true</code> to retrieve a forecast type, <code>false</code> for a non-forecast type. 
     */

    private final Boolean isForecast;

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

        private List<FeatureTuple> features;

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

        Builder setFeatures( List<FeatureTuple> features )
        {
            this.features = features;
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
                              this.getVariableName(),
                              this.getFeatures(),
                              this.getTimeWindow() );

                return Stream.of();
            }

            Request request = this.getRequest( paths );

            // Obtain the response
            return this.getResponse( request );
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
     * @return the request
     */

    private Request getRequest( List<String> paths )
    {
        List<FeatureKey> featureKeys = this.getFeatures()
                                           .stream()
                                           .map( FeatureTuple::getRight )
                                           .filter( Objects::nonNull )
                                           .collect( Collectors.toList() );

        // Build the request object
        return Fetcher.prepareRequest( paths,
                                       featureKeys,
                                       this.getVariableName(),
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
     * Returns the features.
     *
     * @return the features
     */

    private List<FeatureTuple> getFeatures()
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
        scripter.addLine( "INNER JOIN wres.ProjectSource PS" );
        scripter.addTab( 1 ).addLine( "ON PS.source_id = S.source_id" );
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
        super( builder, "S.output_time", "S.lead" );

        this.features = builder.features;
        this.isForecast = builder.isForecast;

        // Validate
        Objects.requireNonNull( this.getProjectId(),
                                CANNOT_BUILD_A_TIME_SERIES_RETRIEVER_WITHOUT_A + "project identifier." );

        if ( Objects.isNull( this.getFeatures() ) || this.getFeatures().isEmpty() )
        {
            throw new NullPointerException( CANNOT_BUILD_A_TIME_SERIES_RETRIEVER_WITHOUT_A + "list of features." );
        }

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

    private DataScripter getScript()
    {
        Database database = super.getDatabase();
        DataScripter dataScripter = new DataScripter( database, GET_START_OF_SCRIPT );

        // Parameters
        dataScripter.addArgument( this.getProjectId() )
                    .addArgument( this.getLeftOrRightOrBaseline()
                                      .value() );

        // Time window
        this.addTimeWindowClause( dataScripter, 0 );

        return dataScripter;
    }

}
