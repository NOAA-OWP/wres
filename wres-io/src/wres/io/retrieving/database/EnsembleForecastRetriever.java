package wres.io.retrieving.database;

import java.sql.SQLException;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import wres.datamodel.types.Ensemble;
import wres.datamodel.types.Ensemble.Labels;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.DataProvider;
import wres.io.database.caching.Ensembles;
import wres.io.database.DataScripter;
import wres.io.database.Database;
import wres.io.database.ScriptBuilder;
import wres.io.retrieving.DataAccessException;

/**
 * Retrieves {@link TimeSeries} of ensemble forecasts from the WRES database.
 *
 * @author James Brown
 */

class EnsembleForecastRetriever extends TimeSeriesRetriever<Ensemble>
{
    /**
     * Error message when attempting to retrieve by identifier. See #68334.
     */

    private static final String NO_IDENTIFIER_ERROR = "Retrieval of ensemble time-series by identifier is not "
                                                      + "currently possible because there is no identifier for "
                                                      + "ensemble time-series in the WRES database.";

    /**
     * The ensemble cache.
     */

    private final Ensembles ensemblesCache;

    /**
     * Builder.
     */

    static class Builder extends TimeSeriesRetriever.Builder<Ensemble>
    {
        /**
         * The ensemble cache.
         */

        private Ensembles ensemblesCache;

        /**
         * Sets the ensemble orm/cache.
         *
         * @param ensemblesCache the ensembles cache
         * @return the builder
         */

        Builder setEnsemblesCache( Ensembles ensemblesCache )
        {
            this.ensemblesCache = ensemblesCache;

            return this;
        }

        @Override
        EnsembleForecastRetriever build()
        {
            return new EnsembleForecastRetriever( this );
        }

    }

    /**
     * <p>Reads a time-series by <code>wres.TimeSeries.timeseries_id</code>.
     *
     * <p>TODO: implement this method when there is a composition identifier for an ensemble. See #68334.
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
     * <p>Returns all of the <code>wres.TimeSeries.timeseries_id</code> associated with this instance.
     *
     * <p>TODO: implement this method when there is a composition identifier for an ensemble. See #68334.
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
     * <p>Overrides the default implementation to get all specified time-series in one pull, rather than one pull for
     * each series.
     *
     * <p>TODO: implement this method when there is a composition identifier for an ensemble. See #68334.
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
    public Stream<TimeSeries<Ensemble>> get()
    {
        this.validateForMultiSeriesRetrieval();

        Database database = super.getDatabase();
        String start = this.getStartOfScriptForGetAllTimeSeries();
        DataScripter dataScripter = new DataScripter( database, start );

        // Add basic constraints at zero tabs
        this.addProjectFeatureVariableAndMemberConstraints( dataScripter, 0 );

        dataScripter.addTab().addLine( "GROUP BY S.source_id," );
        dataScripter.addTab( 2 ).addLine( "S.measurementunit_id," );
        dataScripter.addTab( 2 ).addLine( "TimeScale.duration_ms," );
        dataScripter.addTab( 2 ).addLine( "TimeScale.function_name," );
        dataScripter.addTab( 2 ).addLine( "TSRT.reference_time_type" );
        dataScripter.addLine( ") AS metadata " );
        dataScripter.addLine( "INNER JOIN wres.TimeSeries TS" );
        dataScripter.addTab().addLine( "ON TS.source_id = metadata.series_id" );
        dataScripter.addLine( "INNER JOIN wres.TimeSeriesValue TSV" );
        dataScripter.addTab().addLine( "ON TSV.timeseries_id = TS.timeseries_id" );

        // Add time window constraint at zero tabs
        this.addTimeWindowClause( dataScripter );

        // Add season constraint at one tab
        this.addSeasonClause( dataScripter, 1 );

        // Add GROUP BY clause
        String groupBy = "GROUP BY metadata.series_id,"
                         + "metadata.reference_time, "
                         + "metadata.reference_time_type, "
                         + "metadata.feature_id, "
                         + "TSV.lead, "
                         + "metadata.scale_period, "
                         + "metadata.scale_function, "
                         + "metadata.measurementunit_id,"
                         + "metadata.occurrences";

        if(!this.getVariable()
                               .aliases()
                               .isEmpty() )
        {
            groupBy += ",metadata.variable_name";
        }

        dataScripter.addLine( groupBy );

        // Log the script
        super.logScript( dataScripter );

        // Retrieve the time-series        
        return this.getTimeSeriesFromScript( dataScripter, this.getDataSupplier() );
    }

    @Override
    boolean isForecast()
    {
        return true;
    }

    /**
     * <p>Returns a function that obtains the measured value.
     *
     * <p>TODO: include the labels too, once they are needed. See #56214-37 for the amended script. When processing these,
     * obtain the labels from a local cache, because they will be repeated across many ensembles, typically, and 
     * String[] are comparatively expensive.
     *
     * @return a function to obtain the measured value
     */

    private Function<DataProvider, Event<Ensemble>> getDataSupplier()
    {
        return provider -> {

            Double[] members = provider.getDoubleArray( "ensemble_members" );
            Integer[] ids = provider.getIntegerArray( "ensemble_ids" );
            Instant validTime = provider.getInstant( "valid_time" );

            // Re-duplication is handled in the superclass, so do not consider here, instead map by label
            Map<String, Double> ensemble = new TreeMap<>();

            // Iterate the members, map the units and discover the names and add to the map
            for ( int i = 0; i < members.length; i++ )
            {
                // Get the name from the cache
                String name;
                try
                {
                    name = this.getEnsemblesCache()
                               .getEnsembleName( ids[i] );
                }
                catch ( SQLException e )
                {
                    throw new DataAccessException( "While attempting to map an ensemble identifier to a name.", e );
                }

                ensemble.put( name, members[i] );
            }

            // Labels are cached centrally
            String[] names = ensemble.keySet()
                                     .toArray( new String[0] );
            Labels labels = Labels.of( names );
            double[] unboxed = ensemble.values()
                                       .stream()
                                       .mapToDouble( Double::doubleValue )
                                       .toArray();

            return Event.of( validTime, Ensemble.of( unboxed, labels ) );
        };
    }

    /**
     * @return the ensemble cache
     */

    private Ensembles getEnsemblesCache()
    {
        return this.ensemblesCache;
    }

    /**
     * Returns the start of a script to acquire a time-series from the WRES database for all time-series.
     *
     * @return the start of a script for the time-series
     */

    private String getStartOfScriptForGetAllTimeSeries()
    {
        ScriptBuilder scripter = new ScriptBuilder();

        boolean aliases = !this.getVariable()
                               .aliases()
                               .isEmpty();

        scripter.addLine( "SELECT " );
        scripter.addTab().addLine( "metadata.series_id AS series_id," );
        scripter.addTab().addLine( "metadata.reference_time + INTERVAL '1' MINUTE * TSV.lead AS valid_time," );
        scripter.addTab().addLine( "metadata.reference_time," );
        scripter.addTab().addLine( "metadata.reference_time_type," );
        scripter.addTab().addLine( "ARRAY_AGG(TSV.series_value ORDER BY TS.ensemble_id) AS ensemble_members," );
        scripter.addTab().addLine( "ARRAY_AGG(TS.ensemble_id ORDER BY TS.ensemble_id) AS ensemble_ids," );
        scripter.addTab().addLine( "metadata.measurementunit_id," );
        scripter.addTab().addLine( "metadata.scale_period," );
        scripter.addTab().addLine( "metadata.scale_function," );
        scripter.addTab().addLine( "metadata.feature_id," );

        if ( aliases )
        {
            scripter.addTab().addLine( "metadata.variable_name," );
        }

        // See #56214-272. Add the count to allow re-duplication of duplicate series
        scripter.addTab().addLine( "metadata.occurrences" );
        scripter.addLine( "FROM" );
        scripter.addLine( "(" );
        scripter.addTab().addLine( "SELECT " );
        scripter.addTab( 2 ).addLine( "S.source_id AS series_id," );
        scripter.addTab( 2 ).addLine( "MAX( reference_time ) AS reference_time," );
        scripter.addTab( 2 ).addLine( "TSRT.reference_time_type," );
        scripter.addTab( 2 ).addLine( "S.feature_id," );
        scripter.addTab( 2 ).addLine( "S.measurementunit_id," );
        scripter.addTab( 2 ).addLine( "TimeScale.duration_ms AS scale_period," );
        scripter.addTab( 2 ).addLine( "TimeScale.function_name AS scale_function," );

        if ( aliases )
        {
            scripter.addTab( 2 ).addLine( "S.variable_name," );
        }

        scripter.addTab( 2 ).addLine( "COUNT(*) AS occurrences " );
        scripter.addTab().addLine( "FROM wres.Source S" );
        scripter.addTab().addLine( "INNER JOIN wres.ProjectSource PS" );
        scripter.addTab( 2 ).addLine( "ON PS.source_id = S.source_id" );
        scripter.addTab().addLine( "INNER JOIN wres.TimeSeriesReferenceTime TSRT" );
        scripter.addTab( 2 ).addLine( "ON TSRT.source_id = S.source_id" );
        // TODO: use the timescale_id and TimeScales cache instead
        scripter.addTab().addLine( "LEFT JOIN wres.TimeScale TimeScale" );
        scripter.addTab( 2 ).addLine( "ON TimeScale.timescale_id = S.timescale_id" );

        return scripter.toString();
    }

    /**
     * Construct.
     ** @throws NullPointerException if any required input is null
     */

    private EnsembleForecastRetriever( Builder builder )
    {
        super( builder, "metadata.reference_time", "TSV.lead" );
        this.ensemblesCache = builder.ensemblesCache;

        Objects.requireNonNull( this.ensemblesCache,
                                "The ensemble cache is required when building an ensemble "
                                + "retriever." );
    }


}