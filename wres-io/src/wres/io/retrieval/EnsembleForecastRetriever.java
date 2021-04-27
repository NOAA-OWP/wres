package wres.io.retrieval;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.DoubleUnaryOperator;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import wres.datamodel.Ensemble;
import wres.datamodel.Ensemble.Labels;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.time.Event;
import wres.datamodel.time.ReferenceTimeType;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.datamodel.time.TimeSeries.TimeSeriesBuilder;
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

        Database database = super.getDatabase();
        DataScripter dataScripter = new DataScripter( database, GET_ALL_TIME_SERIES_SCRIPT );

        // Add basic constraints at zero tabs
        this.addProjectFeatureVariableAndMemberConstraints( dataScripter, 0 );

        // Add time window constraint at zero tabs
        this.addTimeWindowClause( dataScripter, 0 );

        // Add season constraint at one tab
        this.addSeasonClause( dataScripter, 1 );

        // Add ensemble member constraints at one tab
        this.addEnsembleMemberClauses( dataScripter, 1 );

        // Add ORDER BY clause
        dataScripter.addLine( "ORDER BY series_id, reference_time, valid_time, ensemble_name;" );

        // Log the script
        super.logScript( dataScripter );

        // Return the composed time-series
        return this.getTimeSeriesFromScript( dataScripter );
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

    private void addEnsembleMemberClauses( DataScripter script, int tabsIn )
    {
        Objects.requireNonNull( script );

        // Does the filter exist?
        if ( this.hasEnsembleConstraint() )
        {
            // Include these
            if ( Objects.nonNull( this.ensembleIdsToInclude )
                 && !this.ensembleIdsToInclude.isEmpty() )
            {
                script.addTab( tabsIn ).addLine( "AND E.ensemble_id = ANY(?)" );
                script.addArgument( this.ensembleIdsToInclude.toArray( new Long[this.ensembleIdsToInclude.size()] ) );
            }

            // Ignore these
            if ( Objects.nonNull( this.ensembleIdsToExclude )
                 && !this.ensembleIdsToExclude.isEmpty() )
            {
                script.addTab( tabsIn ).addLine( "AND NOT E.ensemble_id = ANY(?)" );
                script.addArgument( this.ensembleIdsToExclude.toArray( new Long[this.ensembleIdsToExclude.size()] ) );
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
        scripter.addTab().addLine( "TS.source_id AS series_id," );
        scripter.addTab().addLine( "TS.initialization_date AS reference_time," );
        scripter.addTab().addLine( "TS.initialization_date + INTERVAL '1' MINUTE * TSV.lead AS valid_time," );
        scripter.addTab().addLine( "TSV.series_value AS ensemble_member," );
        scripter.addTab().addLine( "E.ensemble_name AS ensemble_name," );
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
     * Creates a stream of ensemble time-series from a retrieval script.
     * 
     * @param script the retrieval script
     * @return the ensemble time-series in a stream
     */

    private Stream<TimeSeries<Ensemble>> getTimeSeriesFromScript( DataScripter script )
    {
        Database database = super.getDatabase();
        try ( Connection connection = database.getConnection();
              DataProvider provider = script.buffer( connection ) )
        {
            List<TimeSeries<Ensemble>> ensembles = new ArrayList<>();

            TimeScaleOuter lastScale = null; // Last time scale
            Long lastSeriesId = null; // Last identity for the ensemble time-series
            Instant lastReferenceTime = null; // Last reference time
            Map<Instant, Map<String, Double>> events = new TreeMap<>(); // [valid_time,(label, member)]

            // Events for last duplicate
            double duplicateCount = 1;
            Set<String> labels = new HashSet<>(); // The set of labels to help determine duplicates

            // Iterate through the ordered events
            while ( provider.next() )
            {
                // The reference time
                Instant referenceTime = provider.getInstant( TimeSeriesRetriever.REFERENCE_TIME );

                // The series identity
                Long seriesId = provider.getLong( "series_id" );

                // Ordered data, so create a time-series when the reference time flips
                if ( Objects.nonNull( lastSeriesId ) && !seriesId.equals( lastSeriesId ) )
                {
                    // Calculate the number of replications and then replicate the series
                    int replications = (int) Math.ceil( duplicateCount / labels.size() );
                    List<TimeSeries<Ensemble>> ensemble = this.getTimeSeriesFromEvents( lastReferenceTime,
                                                                                        events,
                                                                                        lastScale,
                                                                                        replications );
                    ensembles.addAll( ensemble );
                    events.clear();
                    labels.clear();
                    duplicateCount = 1;
                }

                // Get the valid time
                Instant validTime = provider.getInstant( "valid_time" );

                // Get or add the event container
                Map<String, Double> nextEvent = events.get( validTime );
                if ( Objects.isNull( nextEvent ) )
                {
                    nextEvent = new TreeMap<>();
                    events.put( validTime, nextEvent );
                }

                // Get the label
                String label = provider.getString( "ensemble_name" );
                labels.add( label );

                // Duplicate?
                if ( nextEvent.containsKey( label ) )
                {
                    duplicateCount++;
                }

                // Existing units
                long measurementUnitId = provider.getLong( "measurementunit_id" );

                // Units mapper
                DoubleUnaryOperator mapper = this.getMeasurementUnitMapper()
                                                 .getUnitMapper( measurementUnitId );

                // Get the value and map the units                
                Double value = provider.getDouble( "ensemble_member" );
                double mapped = mapper.applyAsDouble( value );
                nextEvent.put( label, mapped );

                // Add the time-scale info
                String functionString = provider.getString( "scale_function" );
                Duration period = provider.getDuration( "scale_period" );

                TimeScaleOuter latestScale = super.checkAndGetLatestScale( lastScale,
                                                                           period,
                                                                           functionString,
                                                                           validTime );

                lastScale = latestScale;
                lastReferenceTime = referenceTime;
                lastSeriesId = seriesId;
            }

            // Calculate the number of replications and then replicate the series
            int replications = (int) Math.ceil( duplicateCount / labels.size() );
            List<TimeSeries<Ensemble>> lastSeries = this.getTimeSeriesFromEvents( lastReferenceTime,
                                                                                  events,
                                                                                  lastScale,
                                                                                  replications );
            ensembles.addAll( lastSeries );

            return ensembles.stream();
        }
        catch ( SQLException e )
        {
            throw new DataAccessException( "Failed to access the time-series data.", e );
        }
    }

    /**
     * Creates an ensemble time-series from a collection of events.
     * 
     * @param referenceTime the reference time
     * @param events the events
     * @param timeScale the time scale
     * @param duplicateCount the number of series to add
     * @return the ensemble time-series
     */

    private List<TimeSeries<Ensemble>> getTimeSeriesFromEvents( Instant referenceTime,
                                                                Map<Instant, Map<String, Double>> events,
                                                                TimeScaleOuter timeScale,
                                                                int duplicateCount )
    {

        TimeSeriesBuilder<Ensemble> builder = new TimeSeriesBuilder<>();

        Map<ReferenceTimeType, Instant> referenceTimes = Map.of( this.getReferenceTimeType(), referenceTime );

        TimeSeriesMetadata metadata =
                TimeSeriesMetadata.of( referenceTimes,
                                       timeScale,
                                       this.getVariableName(),
                                       this.getFeature(),
                                       this.getMeasurementUnitMapper()
                                           .getDesiredMeasurementUnitName() );
        builder.setMetadata( metadata );

        // Create the events
        for ( Map.Entry<Instant, Map<String, Double>> nextEvent : events.entrySet() )
        {
            Map<String, Double> members = nextEvent.getValue();
            double[] doubles = members.values()
                                      .stream()
                                      .mapToDouble( Double::doubleValue )
                                      .toArray();
            String[] labels = members.keySet()
                                     .toArray( new String[doubles.length] );


            Ensemble next = Ensemble.of( doubles, Labels.of( labels ) );
            Instant validTime = nextEvent.getKey();
            Event<Ensemble> event = Event.of( validTime, next );

            builder.addEvent( event );
        }

        List<TimeSeries<Ensemble>> ensembles = new ArrayList<>();
        TimeSeries<Ensemble> ensemble = builder.build();

        // Re-duplicate
        for ( int i = 0; i < duplicateCount; i++ )
        {
            ensembles.add( ensemble );
        }

        return Collections.unmodifiableList( ensembles );
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
