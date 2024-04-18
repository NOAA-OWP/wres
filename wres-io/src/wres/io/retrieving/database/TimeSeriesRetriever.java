package wres.io.retrieving.database;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.MonthDay;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.yaml.components.DatasetOrientation;
import wres.datamodel.scale.TimeScaleOuter;

import wres.datamodel.space.Feature;
import wres.datamodel.time.Event;

import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.datamodel.time.TimeWindowOuter;
import wres.datamodel.DataProvider;
import wres.io.database.caching.Features;
import wres.io.database.caching.MeasurementUnits;
import wres.io.database.DataScripter;
import wres.io.database.Database;
import wres.io.retrieving.Retriever;
import wres.io.retrieving.DataAccessException;
import wres.statistics.generated.TimeScale.TimeScaleFunction;
import wres.statistics.generated.ReferenceTime.ReferenceTimeType;

/**
 * Abstract base class for retrieving {@link TimeSeries} from a database.
 *
 * @author James Brown
 */

abstract class TimeSeriesRetriever<T> implements Retriever<TimeSeries<T>>
{
    // Re-used strings
    private static final String REFERENCE_TIME = "reference_time";
    private static final String REFERENCE_TIME_TYPE = "reference_time_type";
    private static final String OCCURRENCES = "occurrences";
    private static final String AND = " = ? AND ";
    private static final String WHILE_BUILDING_THE_RETRIEVER = "While building the retriever for project_id '{}' "
                                                               + "and data type {}, ";
    private static final String INTERVAL_1_MINUTE = " + INTERVAL '1' MINUTE * ";
    private static final String LESS_EQUAL = " <= ?";

    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( TimeSeriesRetriever.class );

    /** Log message for the retriever script. */
    private static final String LOG_SCRIPT = "Built retriever {}:{}{}The script was built as a prepared statement with "
                                             + "the following list of parameters: {}.";

    /** Database instance. */
    private final Database database;

    /** Features cache/orm to allow "get db id from a FeatureKey." */
    private final Features featuresCache;

    /** Measurement units cache/orm. */
    private final MeasurementUnits measurementUnits;

    /** Time window filter. */
    private final TimeWindowOuter timeWindow;

    /** The desired timescale, which is used to adjust retrieval when a forecast lead duration ends within
     * the {@link #timeWindow} but starts outside it. */
    private final TimeScaleOuter desiredTimeScale;

    /** The <code>wres.Project.project_id</code>. */
    private final long projectId;

    /** The features. */
    private final Set<Feature> features;

    /** The variable name. */
    private final String variableName;

    /** The data type. */
    private final DatasetOrientation orientation;

    /** A declared existing timescale, which can be used to augment a source, but not override it. */
    private final TimeScaleOuter declaredExistingTimeScale;

    /** The start monthday of a season constraint. */
    private final MonthDay seasonStart;

    /** The end monthday of a season constraint. */
    private final MonthDay seasonEnd;

    /** The time column name, including the table alias (e.g., O.observation_time). This may be a reference time or a
     * valid time, depending on context. See {@link #timeColumnIsReferenceTime()}. */
    private final String timeColumn;

    /** The lead duration column name, including the table alias (e.g., TSV.lead). */
    private final String leadDurationColumn;

    /**
     * Abstract builder.
     *
     * @author James Brown
     * @param <S> the type of time-series to build
     */

    abstract static class Builder<S>
    {
        /**
         * The database used to retrieve data.
         */

        private Database database;

        /**
         * The cache/ORM to get feature data from.
         */
        private Features featuresCache;

        /**
         * The cache/ORM for measurement units.
         */

        private MeasurementUnits measurementUnits;

        /**
         * Time window filter.
         */

        private TimeWindowOuter timeWindow;

        /**
         * The <code>wres.Project.project_id</code>.
         */

        private long projectId;

        /**
         * Variable name.
         */

        private String variableName;

        /**
         * Features.
         */

        private final Set<Feature> features = new HashSet<>();

        /**
         * The data type.
         */

        private DatasetOrientation orientation;

        /**
         * Desired timescale.
         */

        private TimeScaleOuter desiredTimeScale;

        /**
         * Declared existing timescale;
         */

        private TimeScaleOuter declaredExistingTimeScale;

        /**
         * The start monthday of a season constraint.
         */

        private MonthDay seasonStart;

        /**
         * The end monthday of a season constraint.
         */

        private MonthDay seasonEnd;

        /**
         * Sets the database.
         * @param database the database
         * @return the builder
         */

        Builder<S> setDatabase( Database database )
        {
            this.database = database;
            return this;
        }

        /**
         * Sets the features cache/ORM.
         * @param featuresCache the features cache
         * @return the builder
         */

        Builder<S> setFeaturesCache( Features featuresCache )
        {
            this.featuresCache = featuresCache;
            return this;
        }

        /**
         * Sets the measurement units cache/ORM.
         * @param measurementUnits the measurement units cache
         * @return the builder
         */

        Builder<S> setMeasurementUnitsCache( MeasurementUnits measurementUnits )
        {
            this.measurementUnits = measurementUnits;
            return this;
        }

        /**
         * Sets the <code>wres.Project.project_id</code>.
         *
         * @param projectId the <code>wres.Project.project_id</code>
         * @return the builder
         */

        Builder<S> setProjectId( long projectId )
        {
            this.projectId = projectId;
            return this;
        }

        /**
         * Sets the variable name.
         *
         * @param variableName the variable name
         * @return the builder
         */

        Builder<S> setVariableName( String variableName )
        {
            this.variableName = variableName;
            return this;
        }

        /**
         * Sets the features.
         *
         * @param features the features
         * @return the builder
         */

        Builder<S> setFeatures( Set<Feature> features )
        {
            if ( Objects.nonNull( features ) )
            {
                this.features.addAll( features );
            }

            return this;
        }

        /**
         * Sets the data type.
         *
         * @param orientation the data type
         * @return the builder
         */

        Builder<S> setDatasetOrientation( DatasetOrientation orientation )
        {
            this.orientation = orientation;
            return this;
        }

        /**
         * Sets the time window filter.
         *
         * @param timeWindow the time window filter
         * @return the builder
         */

        Builder<S> setTimeWindow( TimeWindowOuter timeWindow )
        {
            this.timeWindow = timeWindow;
            return this;
        }

        /**
         * Sets the desired timescale, which is used to adjust retrieval when a forecast lead duration ends within
         * the {@link #timeWindow} but starts outside it.
         *
         * @param desiredTimeScale the desired timescale
         * @return the builder
         */

        Builder<S> setDesiredTimeScale( TimeScaleOuter desiredTimeScale )
        {
            this.desiredTimeScale = desiredTimeScale;
            return this;
        }

        /**
         * Sets the existing timescale from the project declaration, which can be used to augment a source, but not
         * override it.
         *
         * @param declaredExistingTimeScale the declared existing timescale
         * @return the builder
         */

        Builder<S> setDeclaredExistingTimeScale( TimeScaleOuter declaredExistingTimeScale )
        {
            this.declaredExistingTimeScale = declaredExistingTimeScale;
            return this;
        }

        /**
         * Sets the start of a season in which values will be selected.
         *
         * @param seasonStart the start of the season
         * @return the builder
         */

        Builder<S> setSeasonStart( MonthDay seasonStart )
        {
            this.seasonStart = seasonStart;
            return this;
        }

        /**
         * Sets the end of a season in which values will be selected.
         *
         * @param seasonEnd the end of the season
         * @return the builder
         */

        Builder<S> setSeasonEnd( MonthDay seasonEnd )
        {
            this.seasonEnd = seasonEnd;
            return this;
        }

        abstract TimeSeriesRetriever<S> build();
    }

    /**
     * Returns true if the retriever supplies forecast data.
     *
     * @return true if this instance supplies forecast data
     */

    abstract boolean isForecast();

    /**
     * @return the database.
     */

    Database getDatabase()
    {
        return this.database;
    }

    /**
     * @return the features cache.
     */

    Features getFeaturesCache()
    {
        return this.featuresCache;
    }

    /**
     * @return the feature.
     */

    Set<Feature> getFeatures()
    {
        return this.features;
    }

    /**
     * @return the variable name.
     */

    String getVariableName()
    {
        return this.variableName;
    }

    /**
     * Creates one or more {@link TimeSeries} from a script that retrieves time-series data. Assumes that the script
     * returns time-series events that are ordered by time-series id.
     *
     * @param <S> the time-series data type
     * @param scripter the scripter
     * @param mapper a function that retrieves a time-series value from a prescribed column in a {@link DataProvider}
     * @return the time-series
     * @throws NullPointerException if either input is null
     * @throws DataAccessException if the data could not be accessed for any reason
     */

    <S> Stream<TimeSeries<S>> getTimeSeriesFromScript( DataScripter scripter, Function<DataProvider, Event<S>> mapper )
    {
        Objects.requireNonNull( scripter );
        Objects.requireNonNull( mapper );

        LOGGER.debug( "Submitting a script to obtain time-series data from an underlying data store." );

        // JFR monitoring
        RetrievalEvent retrievalEvent = RetrievalEvent.of( this.getDatasetOrientation(),
                                                           this.getTimeWindow(),
                                                           this.getFeatures(),
                                                           this.getVariableName() );

        Instant startTime = Instant.now();

        retrievalEvent.begin();

        // This connection remains open for the duration that the time-series data stream remains open and is closed on 
        // exception or when the retrieval completes.
        Connection connection = null;

        try
        {
            // To remain open until all series have been read
            connection = this.getDatabase()
                             .getConnection();

            DataProvider provider = scripter.buffer( connection );

            retrievalEvent.commit();

            if ( LOGGER.isDebugEnabled() )
            {
                Duration duration = Duration.between( startTime, Instant.now() );
                LOGGER.debug( "Finished executing a script to obtain time-series data. The time-series data is now "
                              + "available for streaming. The script completed in {}.",
                              duration );
            }

            Supplier<TimeSeries<S>> supplier = this.getTimeSeriesSupplier( mapper,
                                                                           connection,
                                                                           provider );

            // Generate a stream of time-series
            Connection finalConnection = connection;
            return Stream.generate( supplier )
                         // Finite stream, proceeds while a time-series is returned
                         .takeWhile( Objects::nonNull )
                         // Close the connection when the stream is closed
                         .onClose( () -> {
                             try
                             {
                                 LOGGER.debug( "Detected a stream close event, closing an underlying database "
                                               + "connection." );
                                 finalConnection.close();
                             }
                             catch ( SQLException e )
                             {
                                 LOGGER.warn( "Failed to close a database connection." );
                             }
                         } );
        }
        // Close early for known exceptions in this context
        catch ( DataAccessException | SQLException e )
        {
            try
            {
                if ( Objects.nonNull( connection ) )
                {
                    connection.close();
                }
            }
            catch ( SQLException f )
            {
                LOGGER.warn( "Failed to close a database connection.", f );
            }

            throw new DataAccessException( "Failed to access the time-series data.", e );
        }
    }

    /**
     * Adds a {@link TimeWindowOuter} constraint to the retrieval script, if available. All intervals are treated as
     * right-closed.
     *
     * @param script the script to augment
     * @throws NullPointerException if the input is null
     */

    void addTimeWindowClause( DataScripter script )
    {
        Objects.requireNonNull( script );

        // Does the filter exist?
        if ( this.hasTimeWindow() )
        {
            TimeWindowOuter filter = this.getTimeWindow();

            // Forecasts?
            if ( this.isForecast() )
            {
                this.addLeadBoundsToScript( script, filter, 0 );
                this.addReferenceTimeBoundsToScript( script, filter, 0 );
            }

            // Is the time column a reference time?
            // This is different from forecast vs. observation, because some nominally "observed"
            // datasets, such as analyses, may have reference times and lead durations
            if ( this.timeColumnIsReferenceTime() )
            {
                this.addValidTimeBoundsToScriptUsingReferenceTimeAndLeadDuration( script, filter, 0 );
            }
            else
            {
                this.addValidTimeBoundsToScript( script, filter, 0 );
            }
        }
    }

    /**
     * <p>Adds a seasonal constraint to the retrieval script, if available.
     *
     * <p>TODO: reconsider how seasons are applied. Currently, they are applied to forecast reference times,
     * which means they would need to be adjusted for observation valid times. Either way, this complexity 
     * should probably not be delegated to the caller without a much more explicit API. See #40405. 
     *
     * @param script the script to augment
     * @param tabsIn the number of tabs in for the outermost clause
     * @throws NullPointerException if the input is null
     */

    void addSeasonClause( DataScripter script, int tabsIn )
    {
        Objects.requireNonNull( script );

        // Does the filter exist?
        if ( this.hasSeason() )
        {
            String columnName = this.getTimeColumnName();

            String monthOfYearTemplate = "EXTRACT( MONTH FROM " + columnName + " )";
            String dayOfMonthTemplate = "EXTRACT( DAY FROM " + columnName + " )";

            // Seasons can wrap, so order the start and end correctly
            MonthDay earliestDay = this.seasonStart;
            MonthDay latestDay = this.seasonEnd;
            boolean daysFlipped = false;

            if ( this.seasonStart.isAfter( this.seasonEnd ) )
            {
                earliestDay = this.seasonEnd;
                latestDay = this.seasonStart;
                daysFlipped = true;
            }

            if ( daysFlipped )
            {
                script.addTab( tabsIn )
                      .addLine( "AND ( -- The dates should wrap around the end of the year, ",
                                "so we're going to check for values before the latest ",
                                "date and after the earliest" );
                script.addTab( tabsIn + 1 )
                      .addLine( "( " + monthOfYearTemplate + " < ? OR ( ",
                                monthOfYearTemplate + AND,
                                dayOfMonthTemplate + " <= ? ) )",
                                " -- In the set [1/1, ",
                                earliestDay.getMonthValue(),
                                "/",
                                earliestDay.getDayOfMonth(),
                                "]" );
                script.addTab( tabsIn + 1 )
                      .addLine( "OR ( " + monthOfYearTemplate + " > ? OR ( ",
                                monthOfYearTemplate + AND,
                                dayOfMonthTemplate + " >= ? ) )",
                                " -- Or in the set [",
                                latestDay.getMonthValue(),
                                "/",
                                latestDay.getDayOfMonth(),
                                ", 12/31]" );
                script.addTab( tabsIn ).addLine( ")" );
            }
            else
            {
                script.addTab()
                      .addLine( "AND ( " + monthOfYearTemplate + " > ? OR ( ",
                                monthOfYearTemplate + AND,
                                dayOfMonthTemplate + " >= ? ) )" );
                script.addTab()
                      .addLine( "AND ( " + monthOfYearTemplate + " < ? OR ( ",
                                monthOfYearTemplate + " = ? ",
                                "AND " + dayOfMonthTemplate + " <= ? ) )" );
            }

            // Add the parameters in order
            script.addArgument( earliestDay.getMonthValue() )
                  .addArgument( earliestDay.getMonthValue() )
                  .addArgument( earliestDay.getDayOfMonth() )
                  .addArgument( latestDay.getMonthValue() )
                  .addArgument( latestDay.getMonthValue() )
                  .addArgument( latestDay.getDayOfMonth() );
        }
    }

    /**
     * Where available adds the clauses to the input script associated with {@link #getProjectId()}, the 
     * {@link #getVariableName()}, {@link #getFeatureIds()} and {@link #getDatasetOrientation()}.
     *
     * @param script the script to augment
     * @param tabsIn the number of tabs in for the outermost clause
     * @throws DataAccessException if the feature identifier could not be found
     */

    void addProjectFeatureVariableAndMemberConstraints( DataScripter script, int tabsIn )
    {
        // Project identifier
        this.addWhereOrAndClause( script, tabsIn, "PS.project_id = ?", this.getProjectId() );

        // Variable name
        if ( Objects.nonNull( this.getVariableName() ) )
        {
            this.addWhereOrAndClause( script,
                                      tabsIn,
                                      "S.variable_name = ?",
                                      this.getVariableName() );
        }

        // Feature identifier, can be null with no baseline.
        if ( !this.getFeatures()
                  .isEmpty() )
        {
            Long[] featureIds = this.getFeatureIds();
            Object parameter = featureIds;
            String clause = "S.feature_id = ANY(?)";

            // Simplify script if there is only one
            if ( featureIds.length == 1 )
            {
                parameter = featureIds[0];
                clause = "S.feature_id = ?";
            }

            this.addWhereOrAndClause( script,
                                      tabsIn,
                                      clause,
                                      parameter );
        }

        // Member
        if ( Objects.nonNull( this.getDatasetOrientation() ) )
        {
            this.addWhereOrAndClause( script,
                                      tabsIn,
                                      "PS.member = ?",
                                      this.getDatasetOrientation()
                                          .name()
                                          .toLowerCase() );
        }
    }

    /**
     * Returns the time window constraint.
     *
     * @return the time window filter
     */

    TimeWindowOuter getTimeWindow()
    {
        return this.timeWindow;
    }

    /**
     * Returns the desired timescale.
     *
     * @return the desired timescale
     */

    TimeScaleOuter getDesiredTimeScale()
    {
        return this.desiredTimeScale;
    }

    /**
     * Returns the declared existing timescale, which may be null.
     *
     * @return the declared existing timescale or null
     */

    TimeScaleOuter getDeclaredExistingTimeScale()
    {
        return this.declaredExistingTimeScale;
    }

    /**
     * Returns the <code>wres.Project.project_id</code>.
     *
     * @return the <code>wres.Project.project_id</code>
     */

    long getProjectId()
    {
        return this.projectId;
    }

    /**
     * Returns the data type.
     *
     * @return the data type
     */

    DatasetOrientation getDatasetOrientation()
    {
        return this.orientation;
    }

    /**
     * @return the measurement units cache
     */

    MeasurementUnits getMeasurementUnitsCache()
    {
        return this.measurementUnits;
    }

    /**
     * Returns <code>true</code> if a seasonal constraint is defined, otherwise <code>false</code>.
     *
     * @return true if a seasonal constraint is defined, otherwise false
     */

    boolean hasSeason()
    {
        return Objects.nonNull( this.seasonStart );
    }

    /**
     * Returns <code>true</code> if a time window is defined, otherwise <code>false</code>.
     *
     * @return true if a time window is defined, otherwise false
     */

    boolean hasTimeWindow()
    {
        return Objects.nonNull( this.getTimeWindow() );
    }

    /**
     * Adds a clause to a script according to the start of the last available clause. When the last available clause
     * starts with <code>WHERE</code>, then the clause added starts with <code>AND</code>, otherwise <code>WHERE</code>. 
     *
     * @param script the script
     * @param tabsIn the number of tabs in for the outermost clause
     * @param clause the clause
     * @param parameter the parameter
     */

    void addWhereOrAndClause( DataScripter script, int tabsIn, String clause, Object parameter )
    {
        Objects.requireNonNull( script );

        Objects.requireNonNull( clause );

        String existing = script.toString();
        String[] lines = existing.split( "\\r?\\n" );

        if ( lines.length == 0 )
        {
            throw new IllegalStateException( "Cannot add the clause '" + clause
                                             + "' to the input script, because the script is improperly formed." );
        }

        String lastLine = lines[lines.length - 1];

        StringJoiner joiner = new StringJoiner( "" );
        String tab = "    ";
        for ( int i = 0; i < tabsIn; i++ )
        {
            joiner.add( tab );
        }

        // Last lines starts with a WHERE or an AND at the same tabs
        String tabs = joiner.toString();
        if ( lastLine.startsWith( tabs + "WHERE" ) || lastLine.startsWith( tabs + tab + "AND" ) )
        {
            script.addTab( tabsIn + 1 ).addLine( "AND ", clause );
        }
        else
        {
            script.addTab( tabsIn ).addLine( "WHERE ", clause );
        }

        // Add the parameter
        if ( Objects.nonNull( parameter ) )
        {
            script.addArgument( parameter );
        }
    }

    /**
     * Validates the instance for multi-series retrieval and throws an exception if one or more expected constraints
     * are not set.
     *
     * @throws DataAccessException if the instance is not properly configured for multi-series retrieval
     */

    void validateForMultiSeriesRetrieval()
    {
        // Check for constraints
        if ( this.getProjectId() <= 0 )
        {
            throw new DataAccessException( "There is no projectId associated with this Data Access Object: "
                                           + "cannot determine the time-series identifiers without a projectID." );
        }

        if ( Objects.isNull( this.getDatasetOrientation() ) )
        {
            throw new DataAccessException( "There is no leftOrRightOrBaseline identifier associated with this Data "
                                           + "Access Object: cannot determine the time-series identifiers without a "
                                           + "leftOrRightOrBaseline." );
        }
    }

    /**
     * Logs a script.
     * @param dataScripter the script to log
     */

    void logScript( DataScripter dataScripter )
    {
        // Log the prepared statement actually used
        if ( LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( LOG_SCRIPT,
                          this,
                          System.lineSeparator(),
                          dataScripter,
                          dataScripter.getParameterStrings() );
        }

        // Log the runnable form of the prepared statement to assist in debugging
        if ( LOGGER.isTraceEnabled() )
        {
            LOGGER.trace( "The following runnable script was obtained from the prepared statement in retriever {}. "
                          + "As such, this script differs from the original script and is designed to assist in "
                          + "debugging only. See the DEBUG logging for the original, prepared, statement:{}{}",
                          this,
                          System.lineSeparator(),
                          dataScripter.toStringRunnableForDebugPurposes() );
        }
    }

    /**
     * Checks that the timescale information is consistent with the last timescale. If not, throws an exception. If
     * so, returns the valid timescale, which is obtained from the input period and function, possibly augmented by
     * any declared timescale information attached to this instance on construction. In using an existing timescale 
     * from the project declaration, the principle is to augment, but not override, because the source is canonical
     * on its own timescale. The only exception is the function {@link TimeScaleFunction#UNKNOWN}, which can be
     * overridden.
     *
     * @param lastScale the last scale information retrieved
     * @param period the period of ther current timescale to be retrieved
     * @param functionString the function string for the current timescale to be retrieved
     * @param validTime the valid time of the event whose timescale is to be determined, which helps with messaging
     * @return the current timescale
     * @throws DataAccessException if the current timescale is inconsistent with the last timescale
     */

    TimeScaleOuter checkAndGetLatestTimeScale( TimeScaleOuter lastScale,
                                               Duration period,
                                               String functionString,
                                               Instant validTime )
    {
        // As of v6.5, the db schema represents a timescale with a period and a function only and does not admit
        // month-days
        Duration periodToUse = null;
        TimeScaleFunction functionToUse = null;

        // Period available?
        if ( Objects.nonNull( period ) )
        {
            periodToUse = period;
        }

        // Function available?
        if ( Objects.nonNull( functionString ) )
        {
            functionToUse = TimeScaleFunction.valueOf( functionString.toUpperCase() );
        }

        // Otherwise, existing scale to help augment?
        if ( Objects.nonNull( this.getDeclaredExistingTimeScale() ) )
        {
            TimeScaleOuter declared = this.getDeclaredExistingTimeScale();

            if ( Objects.isNull( periodToUse ) )
            {
                periodToUse = declared.getPeriod();
            }

            // Can override null or TimeScaleFunction.UNKNOWN
            if ( Objects.isNull( functionToUse ) || functionToUse == TimeScaleFunction.UNKNOWN )
            {
                functionToUse = declared.getFunction();
            }
        }

        TimeScaleOuter returnMe = null;

        if ( Objects.nonNull( periodToUse ) && Objects.nonNull( functionToUse ) )
        {
            returnMe = TimeScaleOuter.of( periodToUse, functionToUse );
        }

        // Consistent with any declaration? If not, this is exceptional: #92404
        if ( Objects.nonNull( this.getDeclaredExistingTimeScale() )
             && !this.getDeclaredExistingTimeScale()
                     .equalsOrInstantaneous( returnMe ) )
        {
            throw new DataAccessException( "The timescale information associated with a "
                                           + this.getDatasetOrientation()
                                           + " event at '"
                                           + validTime
                                           + "' was declared as '"
                                           + this.getDeclaredExistingTimeScale()
                                           + "' but the timescale recorded in the time-series data is '"
                                           + returnMe
                                           + "', which is inconsistent. If the declaration is incorrect, it should be "
                                           + "fixed. Otherwise, the time-series data was not ingested accurately and "
                                           + "you should contact the WRES developers for support." );
        }

        if ( Objects.nonNull( lastScale ) && !lastScale.equals( returnMe ) )
        {
            throw new DataAccessException( "The timescale information associated with an event at'" + validTime
                                           + "' is '"
                                           + returnMe
                                           + "' but other events in the same series have a different time "
                                           + "scale of '"
                                           + lastScale
                                           + "', which is not allowed." );
        }

        return returnMe;
    }

    /**
     * Returns <code>true</code> if the time column represents a reference time, <code>false</code> if it represents a 
     * valid time.
     *
     * @return true if the time column is a reference time, false for a valid time
     */

    private boolean timeColumnIsReferenceTime()
    {
        return Objects.nonNull( this.leadDurationColumn );
    }

    /**
     * Returns a time-series supplier from the inputs.
     *
     * @param <S> the time-series event value type
     * @param mapper a function that retrieves a time-series value from a prescribed column in a {@link DataProvider}
     * @param connection the connection
     * @param provider the data provider
     * @return a time-series supplier
     * @throws DataAccessException if the data could not be accessed for any reason
     */

    private <S> Supplier<TimeSeries<S>> getTimeSeriesSupplier( Function<DataProvider, Event<S>> mapper,
                                                               Connection connection,
                                                               DataProvider provider )
    {
        // Last series, builder, scale and id
        TimeSeries.Builder<S> lastBuilder = new TimeSeries.Builder<>();
        AtomicReference<TimeScaleOuter> lastScale = new AtomicReference<>(); // Record of last scale
        AtomicLong lastSeriesId = new AtomicLong( -1 ); // Last series id

        // Number of replicates of the last time series
        AtomicInteger replicateCount = new AtomicInteger();

        // Was the final time-series returned already?
        AtomicBoolean returnedFinal = new AtomicBoolean();

        // Was the retrieval completely empty?
        AtomicBoolean emptyRetrieval = new AtomicBoolean( true );

        // Store of duplicates that remain to be returned, at most one fewer than the number of replicates
        List<TimeSeries<S>> duplicates = new ArrayList<>();

        // Create a supplier that returns a time-series once complete
        return () -> {

            // Clean up before sending the null sentinel, which terminates the stream
            try
            {
                // Remaining duplicates? Then return them before incrementing any more rows
                if ( !duplicates.isEmpty() )
                {
                    return duplicates.remove( 0 );
                }

                // New rows to increment
                while ( provider.next() )
                {
                    // Some data was retrieved
                    emptyRetrieval.set( false );

                    // Increment the current series or return a completed one
                    // The series may contain up to N replicates
                    List<TimeSeries<S>> series = this.incrementOrCompleteSeries( mapper,
                                                                                 provider,
                                                                                 lastBuilder,
                                                                                 lastSeriesId,
                                                                                 replicateCount,
                                                                                 lastScale );

                    // Complete? If so, return it
                    if ( !series.isEmpty() )
                    {
                        // Take one and add the rest to the list of duplicates, to be returned on future iterations
                        duplicates.addAll( series );
                        return duplicates.remove( 0 );
                    }
                }

                // If there was a final series (of which there may be replicates), build and return it 
                if ( !emptyRetrieval.get() && !returnedFinal.getAndSet( true ) )
                {
                    TimeSeries<S> finalSeries = lastBuilder.build();
                    List<TimeSeries<S>> replicates = this.getReplicates( finalSeries, replicateCount.get() );
                    duplicates.addAll( replicates );
                    return duplicates.remove( 0 );
                }

                // No more data, so clean up: do not close before this point because the supplier is called up to N
                // times, one for each time-series retrieved, and uses a single result set across all N calls
                provider.close();
                connection.close();
            }
            catch ( SQLException | DataAccessException e )
            {
                throw new DataAccessException( "Encountered an exception while retrieving time-series data.", e );
            }

            return null;
        };
    }

    /**
     * Increments an existing series by reading the current row or completes a series and returns it.
     *
     * @param <S> the time-series event value type
     * @param mapper a function that retrieves a time-series value from a prescribed column in a {@link DataProvider}
     * @param provider the data provider
     * @param lastBuilder the builder for the last time-series
     * @param lastSeriesId the last time-series id
     * @param replicateCount the number of replicates of the last series
     * @param lastScale the last timescale to use in validation
     * @return a completed series or null when incrementing a series
     * @throws DataAccessException if the data could not be accessed for any reason
     */

    private <S> List<TimeSeries<S>> incrementOrCompleteSeries( Function<DataProvider, Event<S>> mapper,
                                                               DataProvider provider,
                                                               TimeSeries.Builder<S> lastBuilder,
                                                               AtomicLong lastSeriesId,
                                                               AtomicInteger replicateCount,
                                                               AtomicReference<TimeScaleOuter> lastScale )
    {
        long seriesId = provider.getLong( "series_id" ); // Current series
        long lastSeriesIdInner = lastSeriesId.get(); // Last series

        List<TimeSeries<S>> returnMe = new ArrayList<>();

        // New series id encountered: reset and return the time-series
        if ( lastSeriesIdInner != -1 && lastSeriesIdInner != seriesId )
        {
            if ( LOGGER.isTraceEnabled() )
            {
                LOGGER.trace( "Completing time-series {}.", seriesId );
            }

            // Reset the scale
            lastScale.set( null );
            lastSeriesId.set( -1 );

            TimeSeries<S> replicate = lastBuilder.build();

            List<TimeSeries<S>> replicates = this.getReplicates( replicate, replicateCount.get() );
            returnMe.addAll( replicates );

            // Clean the builder for re-use
            lastBuilder.clear();
        }

        // New series continues
        if ( LOGGER.isTraceEnabled() )
        {
            LOGGER.trace( "Continuing to build a new time-series, {}.", seriesId );
        }

        // Records occurrences?
        if ( provider.hasColumn( OCCURRENCES ) )
        {
            int seriesCount = provider.getInt( OCCURRENCES );
            replicateCount.getAndSet( seriesCount ); // Record the number of replicates
        }
        else
        {
            throw new DataAccessException( "Could not find the \"occurrences\" column in the time-series results." );
        }

        // Get the valid time
        Instant validTime = provider.getInstant( "valid_time" );

        Map<ReferenceTimeType, Instant> referenceTimes = Collections.emptyMap();

        // Add the explicit reference time
        if ( provider.hasColumn( REFERENCE_TIME )
             && !provider.isNull( REFERENCE_TIME ) )
        {
            ReferenceTimeType type = ReferenceTimeType.UNKNOWN;

            if ( provider.hasColumn( REFERENCE_TIME_TYPE ) )
            {
                String typeString = provider.getString( REFERENCE_TIME_TYPE );
                type = ReferenceTimeType.valueOf( typeString.toUpperCase() );
            }

            Instant referenceTime = provider.getInstant( REFERENCE_TIME );
            referenceTimes = new EnumMap<>( ReferenceTimeType.class );
            referenceTimes.put( type, referenceTime );
            referenceTimes = Collections.unmodifiableMap( referenceTimes );
        }

        // Add the event     
        Event<S> event = mapper.apply( provider );
        this.addEventToTimeSeries( event, lastBuilder );

        // Add the timescale info
        String functionString = provider.getString( "scale_function" );
        long periodInMs = provider.getLong( "scale_period" );
        Duration period = null;

        // In getLong() above, the underlying getLong is primitive, not
        // boxed, so a null value in the db will be 0 rather than null.
        // Because the function name must be present (non-null) for a
        // scale row to be present, test the function name nullity
        // before creating a scale duration/period.
        if ( functionString != null )
        {
            period = Duration.ofMillis( periodInMs );
        }

        TimeScaleOuter latestScale = this.checkAndGetLatestTimeScale( lastScale.get(),
                                                                      period,
                                                                      functionString,
                                                                      validTime );

        long featureId = provider.getLong( "feature_id" );

        Feature featureKey;
        try
        {
            featureKey = this.getFeaturesCache()
                             .getFeatureKey( featureId );
        }
        catch ( SQLException e )
        {
            throw new DataAccessException( "While reading a time-series, failed to acquire a feature from "
                                           + "the cache.",
                                           e );
        }

        // Existing units
        long measurementUnitId = provider.getLong( "measurementunit_id" );
        String measurementUnitName = this.getMeasurementUnitsCache()
                                         .getUnit( measurementUnitId );

        TimeSeriesMetadata metadata =
                TimeSeriesMetadata.of( referenceTimes,
                                       latestScale,
                                       this.getVariableName(),
                                       featureKey,
                                       measurementUnitName );
        lastBuilder.setMetadata( metadata );

        lastScale.set( latestScale );
        lastSeriesId.set( seriesId );

        return Collections.unmodifiableList( returnMe );
    }

    /**
     * Returns as many replicates of the time-series as required. 
     * @param <S> the time-series event value type
     * @param replicate the time-series to replicate
     * @param replicateCount the number of replicates
     * @return a list of replicates
     */

    private <S> List<TimeSeries<S>> getReplicates( TimeSeries<S> replicate, int replicateCount )
    {
        if ( replicateCount < 1 )
        {
            throw new IllegalArgumentException( "Cannot replicate a time-series fewer than 1 times: "
                                                + replicateCount
                                                + "." );
        }

        List<TimeSeries<S>> replicates = new ArrayList<>();

        for ( int i = 0; i < replicateCount; i++ )
        {
            replicates.add( replicate );
        }

        return Collections.unmodifiableList( replicates );
    }

    /**
     * Adds an event to a time-series and annotates any exception raised.
     *
     * @param event the event
     * @param builder the builder
     * @throws DataAccessException if the event could not be added
     */

    private <S> void addEventToTimeSeries( Event<S> event, TimeSeries.Builder<S> builder )
    {
        try
        {
            builder.addEvent( event );
        }
        catch ( IllegalArgumentException e )
        {
            throw new DataAccessException( "While processing a time-series for project_id '"
                                           + this.getProjectId()
                                           + "' and data type '"
                                           + this.getDatasetOrientation()
                                           + "', encountered an error: ",
                                           e );
        }
    }

    /**
     * Adds the lead duration bounds (if any) to the script. The interval is left-closed.
     *
     * @param script the script to augment
     * @param tabsIn the number of tabs in for the outermost clause
     * @param filter the time window filter
     * @throws NullPointerException if any input is null
     */

    private void addLeadBoundsToScript( DataScripter script, TimeWindowOuter filter, int tabsIn )
    {
        Objects.requireNonNull( script );

        Objects.requireNonNull( filter );

        Long lowerLead = null;
        Long upperLead = null;

        // Lower bound
        if ( !filter.getEarliestLeadDuration()
                    .equals( TimeWindowOuter.DURATION_MIN ) )
        {
            Duration period = Duration.ZERO;

            // Adjust the lower bound of the lead duration window by the non-instantaneous desired timescale
            if ( Objects.nonNull( this.desiredTimeScale ) && !this.desiredTimeScale.isInstantaneous() )
            {
                period = TimeScaleOuter.getOrInferPeriodFromTimeScale( this.desiredTimeScale );
            }

            Duration lowered = filter.getEarliestLeadDuration()
                                     .minus( period );

            if ( Objects.nonNull( this.desiredTimeScale ) && LOGGER.isDebugEnabled() )
            {
                LOGGER.debug( "Adjusting the lower lead duration of time window {} from {} to {} "
                              + "in order to acquire data at the desired timescale of {}.",
                              filter,
                              filter.getEarliestLeadDuration(),
                              lowered,
                              this.desiredTimeScale );
            }

            lowerLead = lowered.toMinutes();
        }
        // Upper bound
        if ( !filter.getLatestLeadDuration()
                    .equals( TimeWindowOuter.DURATION_MAX ) )
        {
            upperLead = filter.getLatestLeadDuration()
                              .toMinutes();
        }

        this.addLeadBoundsClauseToScript( script, lowerLead, upperLead, tabsIn );
    }

    /**
     * Adds the lead time constraints to a script.
     *
     * @param script the script
     * @param lowerLead the lower lead time
     * @param upperLead the upper lead time
     * @param tabsIn the number of tabs in
     */

    private void addLeadBoundsClauseToScript( DataScripter script, Long lowerLead, Long upperLead, int tabsIn )
    {
        // Add the clause
        if ( Objects.nonNull( lowerLead ) && Objects.nonNull( upperLead )
             && ( long ) lowerLead == upperLead )
        {
            this.addWhereOrAndClause( script, tabsIn, this.getLeadDurationColumnName() + " = ?", upperLead );
        }
        else
        {
            if ( Objects.nonNull( lowerLead ) )
            {
                this.addWhereOrAndClause( script, tabsIn, this.getLeadDurationColumnName() + " > ?", lowerLead );
            }
            if ( Objects.nonNull( upperLead ) )
            {
                this.addWhereOrAndClause( script,
                                          tabsIn,
                                          this.getLeadDurationColumnName() + LESS_EQUAL,
                                          upperLead );
            }
        }
    }

    /**
     * Adds the valid time bounds (if any) to the script by inspecting a reference time column and a lead duration 
     * column. The interval is right-closed.
     *
     * @param script the script to augment
     * @param filter the time window filter
     * @param tabsIn the number of tabs in for the outermost clause
     * @throws NullPointerException if any input is null
     */

    private void addValidTimeBoundsToScriptUsingReferenceTimeAndLeadDuration( DataScripter script,
                                                                              TimeWindowOuter filter,
                                                                              int tabsIn )
    {
        Objects.requireNonNull( script );

        Objects.requireNonNull( filter );

        // Lower and upper bounds are equal
        if ( filter.getEarliestValidTime().equals( filter.getLatestValidTime() ) )
        {
            OffsetDateTime validTime = OffsetDateTime.ofInstant( filter.getEarliestValidTime(),
                                                                 ZoneId.of( "UTC" ) );

            String clause = this.getTimeColumnName()
                            + INTERVAL_1_MINUTE
                            + this.getLeadDurationColumnName()
                            + " = ?";

            this.addWhereOrAndClause( script,
                                      tabsIn,
                                      clause,
                                      validTime );
        }
        // Lower bound
        else
        {

            if ( !filter.getEarliestValidTime().equals( Instant.MIN ) )
            {
                OffsetDateTime lowerValidTime = OffsetDateTime.ofInstant( filter.getEarliestValidTime(),
                                                                          ZoneId.of( "UTC" ) );

                String clause = this.getTimeColumnName()
                                + INTERVAL_1_MINUTE
                                + this.getLeadDurationColumnName()
                                + " > ?";

                this.addWhereOrAndClause( script,
                                          tabsIn,
                                          clause,
                                          lowerValidTime );
            }

            // Upper bound
            if ( !filter.getLatestValidTime().equals( Instant.MAX ) )
            {
                OffsetDateTime upperValidTime = OffsetDateTime.ofInstant( filter.getLatestValidTime(),
                                                                          ZoneId.of( "UTC" ) );

                String clause = this.getTimeColumnName()
                                + INTERVAL_1_MINUTE
                                + this.getLeadDurationColumnName()
                                + LESS_EQUAL;

                this.addWhereOrAndClause( script,
                                          tabsIn,
                                          clause,
                                          upperValidTime );
            }
        }
    }

    /**
     * Adds the valid time bounds (if any) to the script. The interval is right-closed.
     *
     * @param script the script to augment
     * @param filter the time window filter
     * @param tabsIn the number of tabs in for the outermost clause
     * @throws NullPointerException if any input is null
     */

    private void addValidTimeBoundsToScript( DataScripter script, TimeWindowOuter filter, int tabsIn )
    {
        Objects.requireNonNull( script );

        Objects.requireNonNull( filter );

        Instant lowerValidTime = this.getOrInferLowerValidTime( filter );
        Instant upperValidTime = this.getOrInferUpperValidTime( filter );

        // Add the clauses
        if ( !lowerValidTime.equals( Instant.MIN ) )
        {
            this.addWhereOrAndClause( script,
                                      tabsIn,
                                      this.getTimeColumnName() + " > ?",
                                      OffsetDateTime.ofInstant( lowerValidTime, ZoneId.of( "UTC" ) ) );
        }

        if ( !upperValidTime.equals( Instant.MAX ) )
        {
            this.addWhereOrAndClause( script,
                                      tabsIn,
                                      this.getTimeColumnName() + LESS_EQUAL,
                                      OffsetDateTime.ofInstant( upperValidTime, ZoneId.of( "UTC" ) ) );
        }

        // Log the bounds in case they were inferred
        if ( LOGGER.isDebugEnabled() && !lowerValidTime.equals( Instant.MIN )
             || !upperValidTime.equals( Instant.MAX ) )
        {
            String message = WHILE_BUILDING_THE_RETRIEVER
                             + "used an earliest valid time of {} "
                             + "and a latest valid time of {}.";

            LOGGER.debug( message,
                          this.getProjectId(),
                          this.getDatasetOrientation(),
                          lowerValidTime,
                          upperValidTime );
        }
    }

    /**
     * Helper that returns the lower valid time from the {@link TimeWindowOuter}, preferentially, but otherwise infers 
     * the lower valid time from the forecast information present.
     *
     * @param timeWindow the time window
     * @return the lower valid time
     */

    private Instant getOrInferLowerValidTime( TimeWindowOuter timeWindow )
    {
        Instant lowerValidTime = Instant.MIN;

        // Lower bound present
        if ( !timeWindow.getEarliestValidTime().equals( Instant.MIN ) )
        {
            lowerValidTime = timeWindow.getEarliestValidTime();
        }
        // Make a best effort to infer the valid times from any forecast information
        else
        {
            // Lower reference time available?
            if ( !timeWindow.getEarliestReferenceTime().equals( Instant.MIN ) )
            {
                // Use the lower reference time
                lowerValidTime = timeWindow.getEarliestReferenceTime();

                // Adjust for the earliest lead duration
                if ( !timeWindow.getEarliestLeadDuration().equals( TimeWindowOuter.DURATION_MIN ) )
                {
                    lowerValidTime = lowerValidTime.plus( timeWindow.getEarliestLeadDuration() );

                    //Adjust for the desired timescale, if available
                    Duration period = Duration.ZERO;

                    if ( Objects.nonNull( this.desiredTimeScale ) )
                    {
                        period = TimeScaleOuter.getOrInferPeriodFromTimeScale( this.desiredTimeScale );
                    }

                    lowerValidTime = lowerValidTime.minus( period );
                }
            }
        }

        return lowerValidTime;
    }

    /**
     * Helper that returns the upper valid time from the {@link TimeWindowOuter}, preferentially, but otherwise infers the
     * upper valid time from the forecast information present.
     *
     * @param timeWindow the time window
     * @return the upper valid time
     */

    private Instant getOrInferUpperValidTime( TimeWindowOuter timeWindow )
    {
        Instant upperValidTime = Instant.MAX;

        // Upper bound present
        if ( !timeWindow.getLatestValidTime().equals( Instant.MAX ) )
        {
            upperValidTime = timeWindow.getLatestValidTime();
        }
        // Make a best effort to infer the valid times from any forecast information
        else
        {
            // Both the latest reference time and the latest lead duration available?
            if ( !timeWindow.getLatestReferenceTime().equals( Instant.MAX )
                 && !timeWindow.getLatestLeadDuration().equals( TimeWindowOuter.DURATION_MAX ) )
            {
                // Use the upper reference time plus upper lead duration
                upperValidTime = timeWindow.getLatestReferenceTime().plus( timeWindow.getLatestLeadDuration() );
            }
        }

        return upperValidTime;
    }

    /**
     * Adds the reference time bounds (if any) to the script.
     *
     * @param script the script to augment
     * @param filter the time window filter
     * @param tabsIn the number of tabs in for the outermost clause
     * @throws NullPointerException if any input is null
     */

    private void addReferenceTimeBoundsToScript( DataScripter script, TimeWindowOuter filter, int tabsIn )
    {
        Objects.requireNonNull( script );

        Objects.requireNonNull( filter );

        // Lower and upper bounds are equal
        if ( filter.getEarliestReferenceTime().equals( filter.getLatestReferenceTime() ) )
        {
            OffsetDateTime referenceTime = OffsetDateTime.ofInstant( filter.getEarliestReferenceTime(),
                                                                     ZoneId.of( "UTC" ) );

            this.addWhereOrAndClause( script, tabsIn, this.getTimeColumnName() + " = ?", referenceTime );
        }
        else
        {
            // Lower bound
            if ( !filter.getEarliestReferenceTime().equals( Instant.MIN ) )
            {
                OffsetDateTime lowerReferenceTime = OffsetDateTime.ofInstant( filter.getEarliestReferenceTime(),
                                                                              ZoneId.of( "UTC" ) );

                this.addWhereOrAndClause( script,
                                          tabsIn,
                                          this.getTimeColumnName() + " > ?",
                                          lowerReferenceTime );
            }

            // Upper bound
            if ( !filter.getLatestReferenceTime().equals( Instant.MAX ) )
            {
                OffsetDateTime upperReferenceTime = OffsetDateTime.ofInstant( filter.getLatestReferenceTime(),
                                                                              ZoneId.of( "UTC" ) );

                this.addWhereOrAndClause( script,
                                          tabsIn,
                                          this.getTimeColumnName() + LESS_EQUAL,
                                          upperReferenceTime );
            }
        }
    }

    /**
     * Returns the time column name.
     *
     * @return the time column name
     */

    private String getTimeColumnName()
    {
        return this.timeColumn;
    }

    /**
     * Returns the lead duration column name.
     *
     * @return the lead duration column name
     */

    private String getLeadDurationColumnName()
    {
        return this.leadDurationColumn;
    }

    /**
     * Construct.
     *
     * @param builder the builder
     * @param timeColumn the name of the time column, which is an implementation detail
     * @param leadDurationColumn the name of the lead duration column, which is an implementation detail
     * @throws NullPointerException if any required input is null
     */

    TimeSeriesRetriever( Builder<T> builder, String timeColumn, String leadDurationColumn )
    {
        Objects.requireNonNull( builder );

        this.database = builder.database;
        this.featuresCache = builder.featuresCache;
        this.projectId = builder.projectId;
        this.variableName = builder.variableName;
        this.features = Set.copyOf( builder.features );
        this.orientation = builder.orientation;
        this.timeWindow = builder.timeWindow;
        this.desiredTimeScale = builder.desiredTimeScale;
        this.declaredExistingTimeScale = builder.declaredExistingTimeScale;
        this.seasonStart = builder.seasonStart;
        this.seasonEnd = builder.seasonEnd;
        this.timeColumn = timeColumn;
        this.leadDurationColumn = leadDurationColumn;
        this.measurementUnits = builder.measurementUnits;

        // Validate
        String validationStart = "Cannot build a time-series retriever without a ";
        Objects.requireNonNull( this.database, "database instance." );
        Objects.requireNonNull( this.getTimeColumnName(), validationStart + "time column name." );
        Objects.requireNonNull( this.variableName, validationStart + "variable name." );
        Objects.requireNonNull( this.getMeasurementUnitsCache(), validationStart + "measurement units cache." );
        Objects.requireNonNull( this.getFeaturesCache(), validationStart + "features cache." );

        if ( Objects.isNull( this.seasonStart ) != Objects.isNull( this.seasonEnd ) )
        {
            throw new IllegalArgumentException( validationStart + "without a fully defined season. Season start: "
                                                + this.seasonStart
                                                + "Season end: "
                                                + this.seasonEnd );
        }

        if ( this.features.isEmpty() )
        {
            throw new IllegalArgumentException( validationStart + " set of one or more features." );
        }

        // Log missing information
        if ( LOGGER.isDebugEnabled() )
        {
            String start = WHILE_BUILDING_THE_RETRIEVER
                           + "{}";

            if ( Objects.isNull( this.timeWindow ) )
            {
                LOGGER.debug( start,
                              this.projectId,
                              this.orientation,
                              "the time window was null: the retrieval will be unconditional in time." );
            }

            if ( Objects.isNull( this.desiredTimeScale ) )
            {
                LOGGER.debug( start,
                              this.projectId,
                              this.orientation,
                              "the desired timescale was null: the retrieval will not be adjusted to account "
                              + "for the desired timescale." );
            }

            if ( Objects.nonNull( this.timeWindow ) || Objects.isNull( this.desiredTimeScale ) )
            {
                String message = WHILE_BUILDING_THE_RETRIEVER
                                 + "discovered a time window of {} and a desired timescale of {}.";

                LOGGER.debug( message,
                              this.projectId,
                              this.orientation,
                              this.timeWindow,
                              this.desiredTimeScale );
            }

            if ( Objects.isNull( this.leadDurationColumn ) )
            {
                LOGGER.debug( start,
                              this.projectId,
                              this.orientation,
                              "the supplied lead duration column was null." );
            }
        }
    }

    /**
     * Use the features cache to get a db id for each feature. Return boxed types as the H2 driver seems to prefer
     * this as a parameter and the postgres driver is also fine with it.
     * @return the db row id for each feature.
     */

    Long[] getFeatureIds()
    {
        return this.getFeatures()
                   .stream()
                   .mapToLong( nextFeature -> {
                       try
                       {
                           return this.featuresCache.getFeatureId( nextFeature );
                       }
                       catch ( SQLException e )
                       {
                           throw new DataAccessException( "Unable to find a feature id for "
                                                          + nextFeature
                                                          + "." );
                       }
                   } )
                   .boxed()
                   .toArray( Long[]::new );
    }
}
