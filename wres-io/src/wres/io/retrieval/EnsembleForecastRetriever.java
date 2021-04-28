package wres.io.retrieval;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.DoubleUnaryOperator;
import java.util.function.Function;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import wres.datamodel.Ensemble;
import wres.datamodel.Ensemble.Labels;
import wres.datamodel.time.TimeSeries;
import wres.io.data.caching.Ensembles;
import wres.io.utilities.DataProvider;
import wres.io.utilities.DataScripter;
import wres.io.utilities.Database;
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
     * Start of script for {@link #getAllIdentifiers()}.
     */

    private static final String GET_ALL_TIME_SERIES_SCRIPT =
            EnsembleForecastRetriever.getStartOfScriptForGetAllTimeSeries();

    /**
     * A set of <code>ensemble_id</code> to include in the selection.
     */

    private final Set<Long> ensembleIdsToInclude;

    /**
     * A set of <code>ensemble_id</code> to exclude from the selection.
     */

    private final Set<Long> ensembleIdsToExclude;

    /**
     * The ensemble cache.
     */

    private final Ensembles ensemblesCache;

    /**
     * Builder.
     */

    static class Builder extends TimeSeriesRetrieverBuilder<Ensemble>
    {
        /**
         * A set of <code>ensemble_id</code> to include in the selection.
         */

        private Set<Long> ensembleIdsToInclude = new HashSet<>();

        /**
         * A set of <code>ensemble_id</code> to exclude from the selection.
         */

        private Set<Long> ensembleIdsToExclude = new HashSet<>();

        /**
         * The ensemble cache.
         */

        private Ensembles ensemblesCache;

        /**
         * Adds a set of <code>ensemble_id</code> to include in the selection.
         * 
         * @param ensembleIdsToInclude the ensemble identifiers to include
         * @return the builder
         */

        Builder setEnsembleIdsToInclude( Set<Long> ensembleIdsToInclude )
        {
            this.ensembleIdsToInclude.addAll( ensembleIdsToInclude );

            return this;
        }

        /**
         * Adds a set of <code>ensemble_id</code> to exclude from the selection.
         * 
         * @param ensembleIdsToExclude the ensemble identifiers to exclude
         * @return the builder
         */

        Builder setEnsembleIdsToExclude( Set<Long> ensembleIdsToExclude )
        {
            this.ensembleIdsToExclude.addAll( ensembleIdsToExclude );

            return this;
        }

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
    public Stream<TimeSeries<Ensemble>> get()
    {
        this.validateForMultiSeriesRetrieval();

        Database database = super.getDatabase();
        DataScripter dataScripter = new DataScripter( database, GET_ALL_TIME_SERIES_SCRIPT );

        // Add basic constraints at zero tabs
        this.addProjectFeatureVariableAndMemberConstraints( dataScripter, 0 );

        // Add time window constraint at zero tabs
        this.addTimeWindowClause( dataScripter, 0 );

        // Add season constraint at one tab
        this.addSeasonClause( dataScripter, 1 );

        // Add GROUP BY clause
        dataScripter.addLine( "GROUP BY reference_time, "
                              + "series_id, "
                              + "TSV.lead, "
                              + "TS.scale_period, "
                              + "TS.scale_function, "
                              + "TS.measurementunit_id;" );

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
     * Returns a function that obtains the measured value in the desired units from a {@link DataProvider}.
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
            long measurementUnitId = provider.getLong( "measurementunit_id" );

            // Units mapper
            DoubleUnaryOperator mapper = this.getMeasurementUnitMapper().getUnitMapper( measurementUnitId );

            Double[] members = provider.getDoubleArray( "ensemble_members" );
            Integer[] ids = provider.getIntegerArray( "ensemble_ids" );

            // Re-duplication is handled in the superclass, so do not consider here, instead map by label
            Map<String, Double> ensemble = new TreeMap<>();

            // Iterate the members, map the units and discover the names and add to the map
            for ( int i = 0; i < members.length; i++ )
            {
                // Use this member?
                if ( this.getUseThisEnsembleId( ids[i] ) )
                {
                    // Get the name from the cache
                    String name = null;
                    try
                    {
                        name = this.getEnsemblesCache()
                                   .getEnsembleName( ids[i] );
                    }
                    catch ( SQLException e )
                    {
                        throw new DataAccessException( "While attempting to map an ensemble identifier to a name.", e );
                    }

                    // Map the units
                    double mapped = mapper.applyAsDouble( members[i] );
                    ensemble.put( name, mapped );
                }
            }

            // Labels are cached centrally
            String[] names = ensemble.keySet()
                                     .toArray( new String[ensemble.size()] );
            Labels labels = Labels.of( names );
            double[] unboxed = ensemble.values()
                                       .stream()
                                       .mapToDouble( Double::doubleValue )
                                       .toArray();

            return Ensemble.of( unboxed, labels );
        };
    }

    /**
     * @param ensembleId the ensemble identifier to consider
     * @return {@code true} if the ensemble identifier should be considered, otherwise false
     */

    private boolean getUseThisEnsembleId( long ensembleId )
    {
        // Empty means unconstrained
        if ( !this.hasEnsembleConstraint() )
        {
            return true;
        }

        boolean include = true;
        if ( !this.getEnsembleIdsToInclude().isEmpty() )
        {
            include = this.getEnsembleIdsToInclude().contains( ensembleId );
        }

        if ( !this.getEnsembleIdsToExclude().isEmpty() )
        {
            include = include && !this.getEnsembleIdsToExclude().contains( ensembleId );
        }

        return include;
    }

    /**
     * @return {@code true} if one or more ensemble constraints are present, otherwise {@code false}.
     */

    private boolean hasEnsembleConstraint()
    {
        return !this.ensembleIdsToInclude.isEmpty() || !this.ensembleIdsToExclude.isEmpty();
    }

    /**
     * @return ensemble identifiers to include
     */

    private Set<Long> getEnsembleIdsToInclude()
    {
        return this.ensembleIdsToInclude;
    }

    /**
     * @return ensemble identifiers to exclude
     */

    private Set<Long> getEnsembleIdsToExclude()
    {
        return this.ensembleIdsToExclude;
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

    private static String getStartOfScriptForGetAllTimeSeries()
    {
        ScriptBuilder scripter = new ScriptBuilder();

        scripter.addLine( "SELECT " );
        scripter.addTab().addLine( "TS.source_id AS series_id," );
        scripter.addTab().addLine( "TS.initialization_date AS reference_time," );
        scripter.addTab().addLine( "TS.initialization_date + INTERVAL '1' MINUTE * TSV.lead AS valid_time," );
        scripter.addTab().addLine( "ARRAY_AGG(TSV.series_value ORDER BY TS.ensemble_id) AS ensemble_members," );
        scripter.addTab().addLine( "ARRAY_AGG(TS.ensemble_id ORDER BY TS.ensemble_id) AS ensemble_ids," );
        scripter.addTab().addLine( "TS.scale_period," );
        scripter.addTab().addLine( "TS.scale_function," );
        scripter.addTab().addLine( "TS.measurementunit_id," );
        // To discover duplicates
        scripter.addTab().addLine( "COUNT(TS.ensemble_id) / COUNT(DISTINCT TS.ensemble_id) AS occurrences" );
        scripter.addLine( FROM_WRES_TIME_SERIES_TS );
        scripter.addTab().addLine( "INNER JOIN wres.TimeSeriesValue TSV" );
        scripter.addTab( 2 ).addLine( "ON TSV.timeseries_id = TS.timeseries_id" );
        scripter.addTab().addLine( "INNER JOIN wres.ProjectSource PS" );
        scripter.addTab( 2 ).addLine( "ON PS.source_id = TS.source_id" );

        return scripter.toString();
    }

    /**
     * Construct.
     ** @throws NullPointerException if any required input is null
     */

    private EnsembleForecastRetriever( Builder builder )
    {
        super( builder, "TS.initialization_date", "TSV.lead" );
        this.ensembleIdsToInclude = builder.ensembleIdsToInclude;
        this.ensembleIdsToExclude = builder.ensembleIdsToExclude;
        this.ensemblesCache = builder.ensemblesCache;

        Objects.requireNonNull( this.ensemblesCache,
                                "The ensemble cache is required when building an ensemble "
                                                     + "retriever." );
    }


}