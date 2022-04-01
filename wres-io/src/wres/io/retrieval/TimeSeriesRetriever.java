package wres.io.retrieval;

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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.LeftOrRightOrBaseline;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.scale.TimeScaleOuter.TimeScaleFunction;
import wres.datamodel.space.FeatureKey;
import wres.datamodel.time.Event;
import wres.datamodel.time.ReferenceTimeType;

import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.datamodel.time.TimeWindowOuter;
import wres.io.data.caching.Features;
import wres.io.utilities.DataProvider;
import wres.io.utilities.DataScripter;
import wres.io.utilities.Database;

/**
 * Abstract base class for retrieving {@link TimeSeries} from the WRES database.
 * 
 * @author james.brown@hydrosolved.com
 */

abstract class TimeSeriesRetriever<T> implements Retriever<TimeSeries<T>>
{
    private static final Logger LOGGER = LoggerFactory.getLogger( TimeSeriesRetriever.class );

    private static final String REFERENCE_TIME_COLUMN = "metadata.reference_time";
    private static final String LEAD_DURATION_COLUMN = "TSTV.valid_datetime - metadata.reference_time";
    private static final String VALID_DATETIME_COLUMN = "TSTV.valid_datetime";

    /**
     * String used repeatedly to denote a reference_time.
     */

    static final String REFERENCE_TIME = "reference_time";

    /**
     * Message used several times.
     */

    private static final String WHILE_BUILDING_THE_RETRIEVER = "While building the retriever for project_id '{}' "
                                                               + "and data type {}, ";

    /**
     * Log message for the retriever script.
     */

    private static final String LOG_SCRIPT = "Built retriever {}:{}{}The script was built as a prepared statement with "
                                             + "the following list of parameters: {}.";

    /**
     * Database instance.
     */

    private final Database database;

    /**
     * Features cache/orm to allow "get db id from a FeatureKey."
     */

    private final Features featuresCache;

    /**
     * Time window filter.
     */

    private final TimeWindowOuter timeWindow;

    /**
     * The desired time scale, which is used to adjust retrieval when a forecast lead duration ends within
     * the {@link #timeWindow} but starts outside it.
     */

    private final TimeScaleOuter desiredTimeScale;

    /**
     * The <code>wres.Project.project_id</code>.
     */

    private final long projectId;

    /**
     * The features.
     */

    private final Set<FeatureKey> features;

    /**
     * The variable name.
     */

    private final String variableName;

    /**
     * The data type.
     */

    private final LeftOrRightOrBaseline lrb;

    /**
     * Mapper for changing measurement units.
     */

    private final UnitMapper unitMapper;

    /**
     * A declared existing time-scale, which can be used to augment a source, but not override it.
     */

    private final TimeScaleOuter declaredExistingTimeScale;

    /**
     * The start monthday of a season constraint.
     */

    private final MonthDay seasonStart;

    /**
     * The end monthday of a season constraint.
     */

    private final MonthDay seasonEnd;

    /**
     * Reference time type. If there are multiple instances per time-series in future, then the shape of retrieval will 
     * substantively differ and the reference time type would necessarily become inline to the time-series, not 
     * declared upfront.  
     */

    private ReferenceTimeType referenceTimeType = ReferenceTimeType.UNKNOWN;

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

    Set<FeatureKey> getFeatures()
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
     * Creates one or more {@link TimeSeries} from a script that retrieves time-series data.
     * 
     * @param <S> the time-series data type
     * @param scripter the scripter
     * @param mapper a function that retrieves a time-series value from a prescribed column in a {@link DataProvider}
     * @return the time-series
     * @throws NullPointerException if either input is null
     * @throws DataAccessException if data could not be accessed for whatever reason
     */

    <S> Stream<TimeSeries<S>> getTimeSeriesFromScript( DataScripter scripter, Function<DataProvider, S> mapper )
    {
        Objects.requireNonNull( scripter );
        Objects.requireNonNull( mapper );

        try ( Connection connection = this.getDatabase().getConnection();
              DataProvider provider = scripter.buffer( connection ) )
        {
            Map<Long, TimeSeries.Builder<S>> builders = new TreeMap<>();

            // Time-series are duplicated per common source in wres.ProjectSource, 
            // so re-duplicate here. See #56214-272
            Map<Long, Integer> seriesCounts = new HashMap<>();

            TimeScaleOuter lastScale = null; // Record of last scale
            long lastSeriesId = -1;
            
            while ( provider.next() )
            {
                long seriesId = provider.getLong( "series_id" );
                
                // Reset the last time scale
                if( seriesId != lastSeriesId )
                {
                    lastScale = null;
                }
                
                int seriesCount = 1;

                // Records occurrences?
                if ( provider.hasColumn( "occurrences" ) )
                {
                    seriesCount = provider.getInt( "occurrences" );
                }
                seriesCounts.put( seriesId, seriesCount );

                TimeSeries.Builder<S> builder = builders.get( seriesId );

                // Start a new series when required                 
                if ( Objects.isNull( builder ) )
                {
                    builder = new TimeSeries.Builder<>();
                    builders.put( seriesId, builder );
                }

                // Get the valid time
                Instant validTime = provider.getInstant( "valid_time" );

                Map<ReferenceTimeType, Instant> referenceTimes = Collections.emptyMap();

                // Add the explicit reference time
                if ( provider.hasColumn( REFERENCE_TIME ) && !provider.isNull( REFERENCE_TIME ) )
                {
                    Instant referenceTime = provider.getInstant( REFERENCE_TIME );
                    referenceTimes = new EnumMap<>( ReferenceTimeType.class );
                    referenceTimes.put( this.getReferenceTimeType(), referenceTime );
                    referenceTimes = Collections.unmodifiableMap( referenceTimes );
                }

                // Add the event     
                S value = mapper.apply( provider );
                Event<S> event = Event.of( validTime, value );
                this.addEventToTimeSeries( event, builder );

                // Add the time-scale info
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

                TimeScaleOuter latestScale = this.checkAndGetLatestScale( lastScale,
                                                                          period,
                                                                          functionString,
                                                                          validTime );

                long featureId = provider.getLong( "feature_id" );
                FeatureKey featureKey = this.featuresCache.getFeatureKey( featureId );

                TimeSeriesMetadata metadata =
                        TimeSeriesMetadata.of( referenceTimes,
                                               latestScale,
                                               this.getVariableName(),
                                               featureKey,
                                               this.unitMapper.getDesiredMeasurementUnitName() );
                builder.setMetadata( metadata );
                lastScale = latestScale;
                lastSeriesId = seriesId;
            }

            return this.composeWithDuplicates( Collections.unmodifiableMap( builders ),
                                               Collections.unmodifiableMap( seriesCounts ) );
        }
        catch ( SQLException e )
        {
            throw new DataAccessException( "Failed to access the time-series data.", e );
        }
    }

    /**
     * Adds a {@link TimeWindowOuter} constraint to the retrieval script, if available. All intervals are treated as
     * right-closed.
     * 
     * @param script the script to augment
     * @param tabsIn the number of tabs in for the outermost clause
     * @throws NullPointerException if the input is null
     */

    void addTimeWindowClause( DataScripter script, int tabsIn )
    {
        Objects.requireNonNull( script );

        // Does the filter exist?
        if ( this.hasTimeWindow() )
        {
            TimeWindowOuter filter = this.getTimeWindow();

            // Forecasts?
            if ( this.isForecast() )
            {
                this.addLeadBoundsToScript( script, filter, tabsIn );
                this.addReferenceTimeBoundsToScript( script, filter, tabsIn );
            }

            this.addValidTimeBoundsToScript( script, filter, tabsIn );

        }
    }

    /**
     * Adds a seasonal constraint to the retrieval script, if available.
     * 
     * TODO: reconsider how seasons are applied. Currently, they are applied to forecast reference times, 
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
            String columnName = this.getReferenceTimeColumn();

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
                                monthOfYearTemplate + " = ? AND ",
                                dayOfMonthTemplate + " <= ? ) )",
                                " -- In the set [1/1, ",
                                earliestDay.getMonthValue(),
                                "/",
                                earliestDay.getDayOfMonth(),
                                "]" );
                script.addTab( tabsIn + 1 )
                      .addLine( "OR ( " + monthOfYearTemplate + " > ? OR ( ",
                                monthOfYearTemplate + " = ? AND ",
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
                script.addTab().addLine( "AND ( " + monthOfYearTemplate + " > ? OR ( ",
                                         monthOfYearTemplate + " = ? AND ",
                                         dayOfMonthTemplate + " >= ? ) )" );
                script.addTab().addLine( "AND ( " + monthOfYearTemplate + " < ? OR ( ",
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
     * {@link #getVariableName()}, {@link #getFeatureIds()} and {@link #getLeftOrRightOrBaseline()}.
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
        if ( !this.getFeatures().isEmpty() )
        {
            Long[] featureIds = this.getFeatureIds();
            Object parameter = featureIds;
            String clause = "S.feature_id = ANY(?)";
            
            // Simplify script if there is only one
            if(featureIds.length==1 )
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
        if ( Objects.nonNull( this.getLeftOrRightOrBaseline() ) )
        {
            this.addWhereOrAndClause( script,
                                      tabsIn,
                                      "PS.member = ?",
                                      this.getLeftOrRightOrBaseline().toString().toLowerCase() );
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
     * Returns the desired time scale.
     * 
     * @return the desired time scale
     */

    TimeScaleOuter getDesiredTimeScale()
    {
        return this.desiredTimeScale;
    }

    /**
     * Returns the declared existing time scale, which may be null.
     * 
     * @return the declared existing time scale or null
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

    LeftOrRightOrBaseline getLeftOrRightOrBaseline()
    {
        return this.lrb;
    }

    /**
     * Returns the measurement unit mapper.
     * 
     * @return the measurement unit mapper.
     */

    UnitMapper getMeasurementUnitMapper()
    {
        return this.unitMapper;
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
     * Returns the {@link ReferenceTimeType} of the retriever instance.
     * 
     * @return the reference time type
     */

    ReferenceTimeType getReferenceTimeType()
    {
        return this.referenceTimeType;
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

        if ( Objects.isNull( this.getLeftOrRightOrBaseline() ) )
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
     * Checks that the time-scale information is consistent with the last time scale. If not, throws an exception. If
     * so, returns the valid time scale, which is obtained from the input period and function, possibly augmented by
     * any declared time scale information attached to this instance on construction. In using an existing time scale 
     * from the project declaration, the principle is to augment, but not override, because the source is canonical
     * on its own time scale. The only exception is the function {@link TimeScaleFunction.UNKNOWN}, which can be
     * overridden.
     * 
     * @param lastScale the last scale information retrieved
     * @param period the period of ther current time scale to be retrieved
     * @param functionString the function string for the current time scale to be retrieved
     * @param validTime the valid time of the event whose time scale is to be determined, which helps with messaging
     * @return the current time scale
     * @throws DataAccessException if the current time scale is inconsistent with the last time scale
     */

    TimeScaleOuter checkAndGetLatestScale( TimeScaleOuter lastScale,
                                           Duration period,
                                           String functionString,
                                           Instant validTime )
    {
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
            functionToUse = TimeScaleOuter.TimeScaleFunction.valueOf( functionString.toUpperCase() );
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
            if ( Objects.nonNull( declared.getFunction() )
                 && ( Objects.isNull( functionToUse ) || functionToUse == TimeScaleFunction.UNKNOWN ) )
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
             && !this.getDeclaredExistingTimeScale().equalsOrInstantaneous( returnMe ) )
        {
            throw new DataAccessException( "The time scale information associated with a "
                                           + this.getLeftOrRightOrBaseline()
                                           + " event at '"
                                           + validTime
                                           + "' was declared as '"
                                           + this.getDeclaredExistingTimeScale()
                                           + "' but the time scale recorded in the time-series data is '"
                                           + returnMe
                                           + "', which is inconsistent. If the declaration is incorrect, it should be "
                                           + "fixed. Otherwise, the time-series data was not ingested accurately and "
                                           + "you should contact the WRES developers for support." );
        }

        if ( Objects.nonNull( lastScale ) && !lastScale.equals( returnMe ) )
        {
            throw new DataAccessException( "The time scale information associated with an event at'" + validTime
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
                                           + this.getLeftOrRightOrBaseline()
                                           + "', encountered an error: ",
                                           e );
        }
    }

    /**
     * Returns a stream of time-series from the inputs. For each builder in the map of builders, create as many series 
     * as indicated in the map of series counts.
     * 
     * @param <S> the event value type
     * @param builders the builders
     * @param seriesCounts the sreies counts
     * @return a stream of time-series
     */

    private <S> Stream<TimeSeries<S>> composeWithDuplicates( Map<Long, TimeSeries.Builder<S>> builders,
                                                             Map<Long, Integer> seriesCounts )
    {
        List<TimeSeries<S>> streamMe = new ArrayList<>();

        for ( Map.Entry<Long, TimeSeries.Builder<S>> nextSeries : builders.entrySet() )
        {
            int count = seriesCounts.get( nextSeries.getKey() );
            for ( int i = 0; i < count; i++ )
            {
                streamMe.add( nextSeries.getValue().build() );
            }
        }

        return streamMe.stream();
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
        if ( !filter.getEarliestLeadDuration().equals( TimeWindowOuter.DURATION_MIN ) )
        {
            lowerLead = filter.getEarliestLeadDuration().toSeconds();

            // Adjust by the desired time scale if the desired time scale is not instantaneous
            if ( Objects.nonNull( this.desiredTimeScale ) && !this.desiredTimeScale.isInstantaneous() )
            {

                Duration lowered = filter.getEarliestLeadDuration()
                                         .minus( this.desiredTimeScale.getPeriod() );

                LOGGER.debug( "Adjusting the lower lead duration of time window {} from {} to {} "
                              + "in order to acquire data at the desired time scale of {}.",
                              filter,
                              filter.getEarliestLeadDuration(),
                              lowered,
                              this.desiredTimeScale );

                lowerLead = lowered.toSeconds();
            }
        }
        // Upper bound
        if ( !filter.getLatestLeadDuration().equals( TimeWindowOuter.DURATION_MAX ) )
        {
            upperLead = filter.getLatestLeadDuration().toSeconds();
        }

        this.addLeadBoundsClauseToScript( script, lowerLead, upperLead, tabsIn );
    }

    /**
     * Adds the lead time constraints to a script.
     *
     * @param script the script
     * @param lowerLead the lower lead time in seconds
     * @param upperLead the upper lead time in seconds
     * @param tabsIn the number of tabs in
     */

    private void addLeadBoundsClauseToScript( DataScripter script, Long lowerLead, Long upperLead, int tabsIn )
    {
        // Add the clause
        if ( Objects.nonNull( lowerLead ) && Objects.nonNull( upperLead )
             && Long.compare( lowerLead, upperLead ) == 0 )
        {
            this.addWhereOrAndClause( script, tabsIn, this.getLeadDurationColumn() + " = INTERVAL '1' SECOND * CAST( ? AS BIGINT )", upperLead );
        }
        else
        {
            if ( Objects.nonNull( lowerLead ) )
            {
                this.addWhereOrAndClause( script, tabsIn, this.getLeadDurationColumn() + " > INTERVAL '1' SECOND * CAST( ? AS BIGINT )", lowerLead );
            }
            if ( Objects.nonNull( upperLead ) )
            {
                this.addWhereOrAndClause( script,
                                          tabsIn,
                                          this.getLeadDurationColumn() + " <= INTERVAL '1' SECOND * CAST( ? AS BIGINT )",
                                          upperLead );
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
                                      this.getValidDatetimeColumn() + " > ?",
                                      OffsetDateTime.ofInstant( lowerValidTime, ZoneId.of( "UTC" ) ) );
        }

        if ( !upperValidTime.equals( Instant.MAX ) )
        {
            this.addWhereOrAndClause( script,
                                      tabsIn,
                                      this.getValidDatetimeColumn() + " <= ?",
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
                          this.getLeftOrRightOrBaseline(),
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

                    //Adjust for the desired time scale
                    if ( Objects.nonNull( this.getDesiredTimeScale() )
                         && !this.getDesiredTimeScale().isInstantaneous() )
                    {
                        lowerValidTime = lowerValidTime.minus( this.getDesiredTimeScale().getPeriod() );
                    }
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

            this.addWhereOrAndClause( script,
                                      tabsIn,
                                      this.getReferenceTimeColumn() + " = ?",
                                      referenceTime );
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
                                          this.getReferenceTimeColumn() + " > ?",
                                          lowerReferenceTime );
            }

            // Upper bound
            if ( !filter.getLatestReferenceTime().equals( Instant.MAX ) )
            {
                OffsetDateTime upperReferenceTime = OffsetDateTime.ofInstant( filter.getLatestReferenceTime(),
                                                                              ZoneId.of( "UTC" ) );

                this.addWhereOrAndClause( script,
                                          tabsIn,
                                          this.getReferenceTimeColumn() + " <= ?",
                                          upperReferenceTime );
            }
        }
    }

    /**
     * Returns the reference time (aka basis datetime) column name or expression.
     * 
     * @return the reference time (aka basis datetime) column name or expression.
     */

    protected String getReferenceTimeColumn()
    {
        return TimeSeriesRetriever.REFERENCE_TIME_COLUMN;
    }

    /**
     * Returns the valid (aka natural) datetime column name or expression.
     *
     * @return the valid (aka natural) datetime column name or expression.
     */

    protected String getValidDatetimeColumn()
    {
        return TimeSeriesRetriever.VALID_DATETIME_COLUMN;
    }

    /**
     * Returns the lead duration column name or expression.
     * 
     * @return the lead duration column name or expression.
     */

    protected String getLeadDurationColumn()
    {
        return TimeSeriesRetriever.LEAD_DURATION_COLUMN;
    }

    /**
     * Abstract builder.
     * 
     * @author james.brown@hydrosolved.com
     * @param <S> the type of time-series to build
     */

    abstract static class TimeSeriesRetrieverBuilder<S>
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

        private Set<FeatureKey> features = new HashSet<>();

        /**
         * The data type.
         */

        private LeftOrRightOrBaseline lrb;

        /**
         * The measurement unit mapper.
         */

        private UnitMapper unitMapper;

        /**
         * Desired time scale.
         */

        private TimeScaleOuter desiredTimeScale;

        /**
         * Declared existing time scale;
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
         * The reference time type.
         */

        private ReferenceTimeType referenceTimeType = ReferenceTimeType.UNKNOWN;

        TimeSeriesRetrieverBuilder<S> setDatabase( Database database )
        {
            this.database = database;
            return this;
        }

        TimeSeriesRetrieverBuilder<S> setFeaturesCache( Features featuresCache )
        {
            this.featuresCache = featuresCache;
            return this;
        }

        /**
         * Sets the <code>wres.Project.project_id</code>.
         * 
         * @param projectId the <code>wres.Project.project_id</code>
         * @return the builder
         */

        TimeSeriesRetrieverBuilder<S> setProjectId( long projectId )
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

        TimeSeriesRetrieverBuilder<S> setVariableName( String variableName )
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

        TimeSeriesRetrieverBuilder<S> setFeatures( Set<FeatureKey> features )
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
         * @param lrb the data type
         * @return the builder
         */

        TimeSeriesRetrieverBuilder<S> setLeftOrRightOrBaseline( LeftOrRightOrBaseline lrb )
        {
            this.lrb = lrb;
            return this;
        }

        /**
         * Sets the time window filter.
         * 
         * @param timeWindow the time window filter
         * @return the builder
         */

        TimeSeriesRetrieverBuilder<S> setTimeWindow( TimeWindowOuter timeWindow )
        {
            this.timeWindow = timeWindow;
            return this;
        }

        /**
         * Sets the desired time scale, which is used to adjust retrieval when a forecast lead duration ends within
         * the {@link #timeWindow} but starts outside it.
         * 
         * @param desiredTimeScale the desired time scale
         * @return the builder
         */

        TimeSeriesRetrieverBuilder<S> setDesiredTimeScale( TimeScaleOuter desiredTimeScale )
        {
            this.desiredTimeScale = desiredTimeScale;
            return this;
        }

        /**
         * Sets the existing time scale from the project declaration, which can be used to augment a source, but not
         * override it.
         * 
         * @param declaredExistingTimeScale the declared existing time scale
         * @return the builder
         */

        TimeSeriesRetrieverBuilder<S> setDeclaredExistingTimeScale( TimeScaleOuter declaredExistingTimeScale )
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

        TimeSeriesRetrieverBuilder<S> setSeasonStart( MonthDay seasonStart )
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

        TimeSeriesRetrieverBuilder<S> setSeasonEnd( MonthDay seasonEnd )
        {
            this.seasonEnd = seasonEnd;
            return this;
        }

        /**
         * Sets the measurement unit mapper.
         * 
         * @param unitMapper the measurement unit mapper
         * @return the builder
         */

        TimeSeriesRetrieverBuilder<S> setUnitMapper( UnitMapper unitMapper )
        {
            this.unitMapper = unitMapper;
            return this;
        }

        /**
         * Sets the {@link ReferenceTimeType}.
         * 
         * @param referenceTimeType the reference time type
         * @return the builder
         */

        TimeSeriesRetrieverBuilder<S> setReferenceTimeType( ReferenceTimeType referenceTimeType )
        {
            this.referenceTimeType = referenceTimeType;
            return this;
        }

        abstract TimeSeriesRetriever<S> build();
    }

    /**
     * Construct.
     * 
     * @param builder the builder
     * @throws NullPointerException if any required input is null
     */

    TimeSeriesRetriever( TimeSeriesRetrieverBuilder<T> builder )
    {
        Objects.requireNonNull( builder );

        this.database = builder.database;
        this.featuresCache = builder.featuresCache;
        this.projectId = builder.projectId;
        this.variableName = builder.variableName;
        this.features = Collections.unmodifiableSet( new HashSet<>( builder.features ) );
        this.lrb = builder.lrb;
        this.timeWindow = builder.timeWindow;
        this.desiredTimeScale = builder.desiredTimeScale;
        this.declaredExistingTimeScale = builder.declaredExistingTimeScale;
        this.unitMapper = builder.unitMapper;
        this.seasonStart = builder.seasonStart;
        this.seasonEnd = builder.seasonEnd;
        this.referenceTimeType = builder.referenceTimeType;

        // Validate
        String validationStart = "Cannot build a time-series retriever without a ";
        Objects.requireNonNull( this.database, "database instance." );
        Objects.requireNonNull( this.variableName, validationStart + "variable name." );

        Objects.requireNonNull( this.getMeasurementUnitMapper(), validationStart + "measurement unit mapper." );

        if ( Objects.isNull( this.seasonStart ) != Objects.isNull( this.seasonEnd ) )
        {
            throw new IllegalArgumentException( validationStart + "without a fully defined season. Season start: "
                                                + this.seasonStart
                                                + "Season end: "
                                                + this.seasonEnd );
        }

        if ( Objects.isNull( this.referenceTimeType ) )
        {
            throw new IllegalArgumentException( "Cannot build a time-series retriever with a null reference time "
                                                + "type." );
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
                              this.lrb,
                              "the time window was null: the retrieval will be unconditional in time." );
            }

            if ( Objects.isNull( this.desiredTimeScale ) )
            {
                LOGGER.debug( start,
                              this.projectId,
                              this.lrb,
                              "the desired time scale was null: the retrieval will not be adjusted to account "
                                        + "for the desired time scale." );
            }

            if ( Objects.nonNull( this.timeWindow ) || Objects.isNull( this.desiredTimeScale ) )
            {
                String message = WHILE_BUILDING_THE_RETRIEVER
                                 + "discovered a time window of {} and a desired time scale of {}.";

                LOGGER.debug( message,
                              this.projectId,
                              this.lrb,
                              this.timeWindow,
                              this.desiredTimeScale );
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
