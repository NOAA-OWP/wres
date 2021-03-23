package wres.io.retrieval;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
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
            "Built retriever {} for the retrieval of ensemble forecasts using script:{}{}";

    /**
     * Start of script for {@link #getAllIdentifiers()}.
     */

    private static final String GET_ALL_TIME_SERIES_SCRIPT =
            EnsembleForecastRetriever.getStartOfScriptForGetAllTimeSeries();

    /**
     * Logger.
     */

    private static final Logger LOGGER = LoggerFactory.getLogger( EnsembleForecastRetriever.class );

    /**
     * A set of <code>ensemble_id</code> to include in the selection.
     */

    private final Set<Long> ensembleIdsToInclude;

    /**
     * A set of <code>ensemble_id</code> to exclude from the selection.
     */

    private final Set<Long> ensembleIdsToExclude;

    /**
     * Builder.
     */

    static class Builder extends TimeSeriesRetrieverBuilder<Ensemble>
    {
        /**
         * A set of <code>ensemble_id</code> to include in the selection.
         */

        private Set<Long> ensembleIdsToInclude;

        /**
         * A set of <code>ensemble_id</code> to exclude from the selection.
         */

        private Set<Long> ensembleIdsToExclude;

        /**
         * Adds a set of <code>ensemble_id</code> to include in the selection.
         * 
         * @param ensembleIdsToInclude the ensemble identifiers to include
         * @return the builder
         */

        Builder setEnsembleIdsToInclude( Set<Long> ensembleIdsToInclude )
        {
            this.ensembleIdsToInclude = ensembleIdsToInclude;

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
            this.ensembleIdsToExclude = ensembleIdsToExclude;

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

        ScriptBuilder scripter = new ScriptBuilder( GET_ALL_TIME_SERIES_SCRIPT );

        // Add basic constraints at zero tabs
        this.addProjectVariableAndMemberConstraints( scripter, 0 );

        // Add time window constraint at zero tabs
        this.addTimeWindowClause( scripter, 0 );

        // Add season constraint at one tab
        this.addSeasonClause( scripter, 1 );

        // Add ensemble member constraints at one tab
        this.addEnsembleMemberClauses( scripter, 1 );

        String groupBySource = ", TS.source_id";

        // Add GROUP BY clause
        scripter.addLine( "GROUP BY TS.initialization_date, "
                          + "TSV.lead, "
                          + "TS.scale_period, "
                          + "TS.scale_function, "
                          + "TS.measurementunit_id"
                          + groupBySource );

        // Add ORDER BY clause
        scripter.addLine( "ORDER BY TS.initialization_date, valid_time, series_id;" );

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
    boolean isForecast()
    {
        return true;
    }

    /**
     * Adds clause for each ensemble member constraint discovered.
     * 
     * @param script the script to augment
     * @param tabsIn the number of tabs in for the outermost clause
     * @throws NullPointerException if the input is null
     */

    private void addEnsembleMemberClauses( ScriptBuilder script, int tabsIn )
    {
        Objects.requireNonNull( script );

        // Does the filter exist?
        if ( this.hasEnsembleConstraint() )
        {
            // Include these
            if ( Objects.nonNull( this.ensembleIdsToInclude )
                 && !this.ensembleIdsToInclude.isEmpty() )
            {
                StringJoiner include = new StringJoiner( ",", "IN(", ")" );
                this.ensembleIdsToInclude.forEach( next -> include.add( "'" + next.toString() + "'" ) );
                script.addTab( tabsIn ).addLine( "AND E.ensemble_id ", include.toString() );
            }

            // Ignore these
            if ( Objects.nonNull( this.ensembleIdsToExclude )
                 && !this.ensembleIdsToExclude.isEmpty() )
            {
                StringJoiner exclude = new StringJoiner( ",", "IN(", ")" );
                this.ensembleIdsToExclude.forEach( next -> exclude.add( "'" + next.toString() + "'" ) );
                script.addTab( tabsIn ).addLine( "AND NOT E.ensemble_id ", exclude.toString() );
            }
        }
    }

    /**
     * Returns <code>true</code> if one or more ensemble constraints are present, otherwise <code>false</code>.
     * 
     * @return true if one or more ensemble constraints are defined, otherwise false
     */

    private boolean hasEnsembleConstraint()
    {
        return Objects.nonNull( this.ensembleIdsToInclude ) || Objects.nonNull( this.ensembleIdsToExclude );
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
     * TODO: support reduplication of time-series for ensemble forecasts. See #56214-272. Also see 
     * {@link SingleValuedForecastRetriever} and {@link TimeSeriesRetriever} for the hint required in the form of a
     * series count for each identified series. Note that the <code>timeseries_id</code> is common across duplicated
     * series.
     * 
     * @return the start of a script for the time-series
     */

    private static String getStartOfScriptForGetAllTimeSeries()
    {
        ScriptBuilder scripter = new ScriptBuilder();

        scripter.addLine( "SELECT " );
        scripter.addTab().addLine( "MIN(TS.timeseries_id) AS series_id," );
        scripter.addTab().addLine( "TS.initialization_date AS reference_time," );
        scripter.addTab().addLine( "TS.initialization_date + INTERVAL '1' MINUTE * TSV.lead AS valid_time," );
        scripter.addTab().addLine( "ARRAY_AGG(" );
        scripter.addTab( 2 ).addLine( "TSV.series_value" );
        scripter.addTab( 2 ).addLine( "ORDER BY E.ensemble_name" );
        scripter.addTab().addLine( ") AS ensemble_members," );
        scripter.addTab().addLine( "TS.scale_period," );
        scripter.addTab().addLine( "TS.scale_function," );
        scripter.addTab().addLine( "TS.measurementunit_id" );
        scripter.addLine( FROM_WRES_TIME_SERIES_TS );
        scripter.addTab().addLine( "INNER JOIN wres.Ensemble E" );
        scripter.addTab( 2 ).addLine( "ON E.ensemble_id = TS.ensemble_id" );
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
    }


}
