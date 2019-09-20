package wres.io.retrieval.datashop;

import java.util.Arrays;
import java.util.Optional;
import java.util.function.DoubleUnaryOperator;
import java.util.function.Function;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.Ensemble;
import wres.datamodel.time.TimeSeries;
import wres.io.utilities.DataProvider;
import wres.io.utilities.ScriptBuilder;

/**
 * Retrieves {@link TimeSeries} of ensemble forecasts from the WRES database.
 * 
 * @author james.brown@hydrosolved.com
 */

class EnsembleForecastRetriever extends TimeSeriesRetriever<Ensemble>
{

    /**
     * Script string re-used several times. 
     */

    private static final String FROM_WRES_TIME_SERIES_TS = "FROM wres.TimeSeries TS";

    /**
     * Error message when attempting to retrieve by identifier. See #68334.
     */

    private static final String NO_IDENTIFIER_ERROR = "Retrieval of ensemble time-series by identifier is not "
                                                      + "currently possible because there is no identifier for "
                                                      + "ensemble time-series in the WRES database.";

    /**
     * Log message.
     */

    private static final String LOG_SCRIPT =
            "Built {} for the retrieval of ensemble forecasts with the following script:{}{}";

    /**
     * Start of script for {@link #getAll()}.
     */

    private static final String GET_ALL_TIME_SERIES_SCRIPT_START =
            EnsembleForecastRetriever.getStartOfScriptForGetAllTimeSeries();

    /**
     * End of script for {@link #getAll()}.
     */

    private static final String GET_ALL_TIME_SERIES_SCRIPT_END =
            EnsembleForecastRetriever.getEndOfScriptForGetAllTimeSeries();

    /**
     * Logger.
     */

    private static final Logger LOGGER = LoggerFactory.getLogger( EnsembleForecastRetriever.class );

    /**
     * Builder.
     */

    public static class Builder extends TimeSeriesDataShopBuilder<Ensemble>
    {

        @Override
        EnsembleForecastRetriever build()
        {
            return new EnsembleForecastRetriever( this );
        }

    }

    /**
     * Reads a time-series by <code>wres.TimeSeries.timeseries_id</code>.
     * 
     * TODO: implement this method when there is a composition identifier for an ensemble. See #68334.
     * 
     * @param identifier the <code>wres.TimeSeries.timeseries_id</code>
     * @return a possible time-series for the given identifier
     * @throws UnsupportedOperationException in all cases - see #68334.
     */

    @Override
    public Optional<TimeSeries<Ensemble>> get( long identifier )
    {
        throw new UnsupportedOperationException( NO_IDENTIFIER_ERROR );
    }

    /**
     * Returns all of the <code>wres.TimeSeries.timeseries_id</code> associated with this instance. 
     * 
     * TODO: implement this method when there is a composition identifier for an ensemble. See #68334.
     * 
     * @return a stream of<code>wres.TimeSeries.timeseries_id</code>
     * @throws UnsupportedOperationException in all cases - see #68334.
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
     * TODO: implement this method when there is a composition identifier for an ensemble. See #68334.
     * 
     * @param identifiers the stream of identifiers
     * @return a stream over the identified objects
     * @throws UnsupportedOperationException in all cases - see #68334.
     * @throws NullPointerException if the input is null
    
     */
    @Override
    public Stream<TimeSeries<Ensemble>> get( LongStream identifiers )
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
    public Stream<TimeSeries<Ensemble>> getAll()
    {
        this.validateForMultiSeriesRetrieval();

        ScriptBuilder scripter = new ScriptBuilder( GET_ALL_TIME_SERIES_SCRIPT_START );

        // Add basic constraints
        this.addProjectVariableAndMemberConstraints( scripter, 1 );

        // Add ORDER BY clause
        scripter.addTab().addLine( "ORDER BY TS.timeseries_id -- Can lead to faster joins on TSV" );

        scripter.add( GET_ALL_TIME_SERIES_SCRIPT_END );

        // Add time window constraint
        this.addTimeWindowClause( scripter, 0 );

        // Add GROUP BY clause
        scripter.addLine( "GROUP BY TS.initialization_date, TSV.lead" );
        scripter.addLine( "ORDER BY TS.initialization_date, TSV.lead;" );

        String script = scripter.toString();

        if ( LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( LOG_SCRIPT,
                          this,
                          System.lineSeparator(),
                          script );
        }

        return this.getTimeSeriesFromScript( script, this.getDataSupplier() );
    }

    @Override
    boolean isForecastRetriever()
    {
        return true;
    }

    /**
     * Returns a function that obtains the measured value in the desired units.
     * 
     * TODO: include the labels too, once they are needed. See #56214-37 for the amended script. When processing these, 
     * obtain the labels from a local cache, because they will be repeated across many ensembles, typically, and 
     * String[] are comparatively expensive.
     * 
     * @return a function to obtain the measured value in the correct units
     */

    private Function<DataProvider, Ensemble> getDataSupplier()
    {
        return provider -> {

            // Existing units
            int measurementUnitId = provider.getInt( "measurementunit_id" );

            // Units mapper
            DoubleUnaryOperator mapper = this.getMeasurementUnitMapper().getUnitMapper( measurementUnitId );

            // Map the units
            double[] mapped = Arrays.stream( provider.getDoubleArray( "ensemble_members" ) )
                                    .mapToDouble( Double::doubleValue )
                                    .map( mapper )
                                    .toArray();

            return Ensemble.of( mapped );
        };
    }

    /**
     * Returns the start of a script to acquire a time-series from the WRES database for all time-series.
     * 
     * @return the start of a script for the time-series
     */

    private static String getStartOfScriptForGetAllTimeSeries()
    {
        ScriptBuilder scripter = new ScriptBuilder();

        scripter.addLine( "SELECT " );
        scripter.addTab().addLine( "MIN(TS.timeseries_id) AS series_id," );
        scripter.addTab().addLine( "TS.initialization_date + INTERVAL '1' MINUTE * TSV.lead AS valid_time," );
        scripter.addTab().addLine( "TS.initialization_date AS reference_time," );
        scripter.addTab().addLine( "ARRAY_AGG(" );
        scripter.addTab( 2 ).addLine( "TSV.series_value" );
        scripter.addTab( 2 ).addLine( "ORDER BY TS.ensemble_id" );
        scripter.addTab().addLine( ") AS ensemble_members," );
        scripter.addTab().addLine( "TS.measurementunit_id" );
        scripter.addLine( "FROM (" );
        scripter.addTab().addLine( "SELECT " );
        scripter.addTab( 2 ).addLine( "TS.timeseries_id," );
        scripter.addTab( 2 ).addLine( "TS.ensemble_id," );
        scripter.addTab( 2 ).addLine( "TS.initialization_date," );
        scripter.addTab( 2 ).addLine( "TS.measurementunit_id," );
        scripter.addTab( 2 ).addLine( "TSS.lead" );
        scripter.addTab().addLine( FROM_WRES_TIME_SERIES_TS );
        scripter.addTab().addLine( "INNER JOIN wres.TimeSeriesSource TSS" );
        scripter.addTab( 2 ).addLine( "ON TSS.timeseries_id = TS.timeseries_id" );
        scripter.addTab().addLine( "INNER JOIN wres.ProjectSource PS" );
        scripter.addTab( 2 ).addLine( "ON PS.source_id = TSS.source_id" );

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

        scripter.addLine( ") AS TS" );
        scripter.addLine( "INNER JOIN wres.TimeSeriesValue TSV" );
        scripter.addTab().addLine( "ON TSV.timeseries_id = TS.timeseries_id" );
        scripter.addTab( 2 ).addLine( "AND (TS.lead IS NULL OR TS.lead = TSV.lead)" );

        return scripter.toString();
    }

    /**
     * Construct.
     * 
     * @param timeWindow the time window
     * @throws NullPointerException if the filter is null
     */

    private EnsembleForecastRetriever( Builder builder )
    {
        super( builder );
    }


}
