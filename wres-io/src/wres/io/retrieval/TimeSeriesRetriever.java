package wres.io.retrieval;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.MonthDay;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.scale.TimeScale;
import wres.datamodel.scale.TimeScale.TimeScaleFunction;
import wres.datamodel.time.Event;
import wres.datamodel.time.ReferenceTimeType;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeWindow;
import wres.datamodel.time.TimeSeries.TimeSeriesBuilder;
import wres.io.config.LeftOrRightOrBaseline;
import wres.io.utilities.DataProvider;
import wres.io.utilities.DataScripter;
import wres.io.utilities.ScriptBuilder;

/**
 * Abstract base class for retrieving {@link TimeSeries} from the WRES database.
 * 
 * @author james.brown@hydrosolved.com
 */

abstract class TimeSeriesRetriever<T> implements Retriever<TimeSeries<T>>
{

    /**
     * Message used several times.
     */

    private static final String WHILE_BUILDING_THE_RETRIEVER = "While building the retriever for project_id '{}' "
                                                               + "with variablefeature_id '{}' "
                                                               + "and data type {}, ";

    /**
     * Script string used several times.
     */

    private static final String INTERVAL_1_MINUTE = " + INTERVAL '1' MINUTE * ";

    /**
     * Operator used several times in scripts.
     */

    private static final String LESS_EQUAL = " <= '";

    /**
     * Logger.
     */

    private static final Logger LOGGER = LoggerFactory.getLogger( TimeSeriesRetriever.class );

    /**
     * Time window filter.
     */

    private final TimeWindow timeWindow;

    /**
     * The desired time scale, which is used to adjust retrieval when a forecast lead duration ends within
     * the {@link #timeWindow} but starts outside it.
     */

    private final TimeScale desiredTimeScale;

    /**
     * The <code>wres.Project.project_id</code>.
     */

    private final Integer projectId;

    /**
     * TODO: replace this with a variable name and a list of features. Retrieval should support
     * multiple features and separate them from variables.
     * 
     * The <code>wres.VariableFeature.variablefeature_id</code>.
     */

    private final Integer variableFeatureId;

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

    private final TimeScale declaredExistingTimeScale;

    /**
     * The start monthday of a season constraint.
     */

    private final MonthDay seasonStart;

    /**
     * The end monthday of a season constraint.
     */

    private final MonthDay seasonEnd;

    /**
     * The time column name, including the table alias (e.g., O.observation_time). This may be a reference time or a
     * valid time, depending on context. See {@link #timeColumnIsAReferenceTime()}.
     */

    protected String timeColumn;

    /**
     * The lead duration column name, including the table alias (e.g., TSV.lead).
     */

    private final String leadDurationColumn;

    /**
     * Is <code>true</code> to retrieve values from time-series that were distributed across multiple sources. The need
     * for this distinction persists while ingest is not time-series-shaped, rather event-shaped. For example, one 
     * consequence is the need to group by <code>source_id</code>, in general, but not when individual time-series were 
     * distributed across multiple different sources. See #65216.
     * 
     * TODO: please remove me when ingest is time-series-shaped.
     */

    private final boolean hasMultipleSourcesPerSeries;

    /**
     * Returns true if the retriever supplies forecast data.
     * 
     * @return true if this instance supplies forecast data
     */

    abstract boolean isForecast();

    /**
     * Creates one or more {@link TimeSeries} from a script that retrieves time-series data.
     * 
     * @param <S> the time-series data type
     * @param script the script
     * @param mapper a function that retrieves a time-series value from a prescribed column in a {@link DataProvider}
     * @return the time-series
     * @throws DataAccessException if data could not be accessed for whatever reason
     */

    <S> Stream<TimeSeries<S>> getTimeSeriesFromScript( String script, Function<DataProvider, S> mapper )
    {
        // Acquire the raw time-series data from the db for the input time-series identifier
        DataScripter scripter = new DataScripter( script );

        try ( DataProvider provider = scripter.buffer() )
        {
            Map<Integer, TimeSeriesBuilder<S>> builders = new TreeMap<>();

            // Time-series are duplicated per common source in wres.ProjectSource, 
            // so re-duplicate here. See #56214-272
            Map<Integer, Integer> seriesCounts = new HashMap<>();

            TimeScale lastScale = null; // Record of last scale

            while ( provider.next() )
            {
                int seriesId = provider.getInt( "series_id" );
                int seriesCount = 1;

                // Records occurrences?
                if ( provider.hasColumn( "occurrences" ) )
                {
                    seriesCount = provider.getInt( "occurrences" );
                }
                seriesCounts.put( seriesId, seriesCount );

                TimeSeriesBuilder<S> builder = builders.get( seriesId );

                // Start a new series when required                 
                if ( Objects.isNull( builder ) )
                {
                    builder = new TimeSeriesBuilder<>();
                    builders.put( seriesId, builder );
                }

                // Get the valid time
                Instant validTime = provider.getInstant( "valid_time" );

                // Add the explicit reference time
                if ( provider.hasColumn( "reference_time" ) )
                {
                    Instant referenceTime = provider.getInstant( "reference_time" );
                    builder.addReferenceTime( referenceTime, ReferenceTimeType.UNKNOWN );
                }

                // Add the event     
                S value = mapper.apply( provider );
                Event<S> event = Event.of( validTime, value );
                this.addEventToTimeSeries( event, builder );

                // Add the time-scale info
                String functionString = provider.getString( "scale_function" );
                Duration period = provider.getDuration( "scale_period" );

                TimeScale latestScale = this.checkAndGetLatestScale( lastScale, period, functionString, event );
                builder.setTimeScale( latestScale );
                lastScale = latestScale;
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
     * Adds a {@link TimeWindow} constraint to the retrieval script, if available. All intervals are treated as 
     * right-closed.
     * 
     * @param script the script to augment
     * @param tabsIn the number of tabs in for the outermost clause
     * @throws NullPointerException if the input is null
     */

    void addTimeWindowClause( ScriptBuilder script, int tabsIn )
    {
        Objects.requireNonNull( script );

        // Does the filter exist?
        if ( this.hasTimeWindow() )
        {
            TimeWindow filter = this.getTimeWindow();

            // Forecasts?
            if ( this.isForecast() )
            {
                this.addLeadBoundsToScript( script, filter, tabsIn );
                this.addReferenceTimeBoundsToScript( script, filter, tabsIn );
            }

            // Is the time column a reference time?
            // This is different from forecast vs. observation, because some nominally "observed"
            // datasets, such as analyses, may have reference times and lead durations
            if ( this.timeColumnIsAReferenceTime() )
            {
                this.addValidTimeBoundsToScriptUsingReferenceTimeAndLeadDuration( script, filter, tabsIn );
            }
            else
            {
                this.addValidTimeBoundsToScript( script, filter, tabsIn );
            }
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

    void addSeasonClause( ScriptBuilder script, int tabsIn )
    {
        Objects.requireNonNull( script );

        // Does the filter exist?
        if ( this.hasSeason() )
        {
            String columnName = this.getTimeColumnName();

            String dateTemplate = "MAKE_DATE( EXTRACT( YEAR FROM " + columnName + " )::INTEGER, %d, %d)";

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

            String earliestConstraint =
                    String.format( dateTemplate, earliestDay.getMonthValue(), earliestDay.getDayOfMonth() );
            String latestConstraint =
                    String.format( dateTemplate, latestDay.getMonthValue(), latestDay.getDayOfMonth() );

            if ( daysFlipped )
            {
                script.addTab( tabsIn )
                      .addLine( "AND ( -- The dates should wrap around the end of the year, ",
                                "so we're going to check for values before the latest ",
                                "date and after the earliest" );
                script.addTab( tabsIn + 1 )
                      .addLine( "("
                                + columnName
                                + ")::DATE <= ",
                                earliestConstraint,
                                " -- In the set [1/1, ",
                                earliestDay.getMonthValue(),
                                "/",
                                earliestDay.getDayOfMonth(),
                                "]" );
                script.addTab( tabsIn + 1 )
                      .addLine( "OR (",
                                columnName,
                                ")::DATE >= ",
                                latestConstraint,
                                " -- Or in the set [",
                                latestDay.getMonthValue(),
                                "/",
                                latestDay.getDayOfMonth(),
                                ", 12/31]" );
                script.addTab( tabsIn ).addLine( ")" );
            }
            else
            {
                script.addTab().addLine( "AND (", columnName + ")::DATE >= ", earliestConstraint );
                script.addTab().addLine( "AND (", columnName, ")::DATE <= ", latestConstraint );
            }
        }
    }

    /**
     * Where available adds the clauses to the input script associated with {@link #getProjectId()}, 
     * {@link #getVariableFeatureId()} and {@ #getLeftOrRightOrBaseline()}.
     * 
     * @param script the script to augment
     * @param tsTable True if using the wres.TimeSeries table, false if wres.Observations
     * @param tabsIn the number of tabs in for the outermost clause
     */

    void addProjectVariableAndMemberConstraints( ScriptBuilder script, int tabsIn, boolean tsTable )
    {
        // project_id
        if ( Objects.nonNull( this.getProjectId() ) )
        {
            this.addWhereOrAndClause( script, tabsIn, "PS.project_id = '", this.getProjectId(), "'" );
        }
        // variablefeature_id
        if ( Objects.nonNull( this.getVariableFeatureId() ) )
        {
            if ( tsTable )
            {
                this.addWhereOrAndClause( script,
                                          tabsIn,
                                          "TS.variablefeature_id = '",
                                          this.getVariableFeatureId(),
                                          "'" );
            }
            else
            {
                this.addWhereOrAndClause( script,
                                          tabsIn,
                                          "O.variablefeature_id = '",
                                          this.getVariableFeatureId(),
                                          "'" );
            }
        }
        // member
        if ( Objects.nonNull( this.getLeftOrRightOrBaseline() ) )
        {
            this.addWhereOrAndClause( script,
                                      tabsIn,
                                      "PS.member = '",
                                      this.getLeftOrRightOrBaseline().toString().toLowerCase(),
                                      "'" );
        }
    }

    /**
     * Returns the time window constraint.
     * 
     * @return the time window filter
     */

    TimeWindow getTimeWindow()
    {
        return this.timeWindow;
    }

    /**
     * Returns the desired time scale.
     * 
     * @return the desired time scale
     */

    TimeScale getDesiredTimeScale()
    {
        return this.desiredTimeScale;
    }

    /**
     * Returns the declared existing time scale, which may be null.
     * 
     * @return the declared existing time scale or null
     */

    TimeScale getDeclaredExistingTimeScale()
    {
        return this.declaredExistingTimeScale;
    }

    /**
     * Returns the <code>wres.Project.project_id</code>.
     * 
     * @return the <code>wres.Project.project_id</code>
     */

    Integer getProjectId()
    {
        return this.projectId;
    }

    /**
     * Returns the <code>wres.VariableFeature.variablefeature_id</code>.
     * 
     * @return the <code>wres.VariableFeature.variablefeature_id</code>
     */

    Integer getVariableFeatureId()
    {
        return this.variableFeatureId;
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
     * Gets the status of individual time-series as originating from several sources.
     * 
     * TODO: please remove me when ingest is time-series-shaped.
     * 
     * @return true if each series originates from multiple sources, otherwise false
     */

    boolean hasMultipleSourcesPerSeries()
    {
        return this.hasMultipleSourcesPerSeries;
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
     * @param clauseElements the clause elements
     */

    void addWhereOrAndClause( ScriptBuilder script, int tabsIn, Object... clauseElements )
    {
        Objects.requireNonNull( script );

        Objects.requireNonNull( clauseElements );

        String existing = script.toString();
        String[] lines = existing.split( "\\r?\\n" );

        if ( lines.length == 0 )
        {
            throw new IllegalStateException( "Cannot add the clause '" + Arrays.toString( clauseElements )
                                             + "' to the input script, because the script is improperly formed." );
        }

        String lastLine = lines[lines.length - 1];

        // Compose the clause elements into a clause
        String clause = Arrays.stream( clauseElements )
                              .map( Object::toString )
                              .collect( Collectors.joining() );

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
        if ( Objects.isNull( this.getProjectId() ) )
        {
            throw new DataAccessException( "There is no projectId associated with this Data Access Object: "
                                           + "cannot determine the time-series identifiers without a projectID." );
        }

        if ( Objects.isNull( this.getVariableFeatureId() ) )
        {
            throw new DataAccessException( "There is no variableFeatureId associated with this Data Access "
                                           + "Object: cannot determine the time-series identifiers without a "
                                           + "variableFeatureId." );
        }

        if ( Objects.isNull( this.getLeftOrRightOrBaseline() ) )
        {
            throw new DataAccessException( "There is no leftOrRightOrBaseline identifier associated with this Data "
                                           + "Access Object: cannot determine the time-series identifiers without a "
                                           + "leftOrRightOrBaseline." );
        }
    }

    /**
     * Returns <code>true</code> if the time column represents a reference time, <code>false</code> if it represents a 
     * valid time.
     * 
     * @return true if the time column is a reference time, false for a valid time
     */

    private boolean timeColumnIsAReferenceTime()
    {
        return Objects.nonNull( this.leadDurationColumn );
    }

    /**
     * Checks that the time-scale information is consistent with the last time scale. If not, throws an exception. If
     * so, returns the valid time scale, which is obtained from the input period and function, possibly augmented by
     * any declared time scale information attached to this instance on construction. In using an existing time scale 
     * from the project declaration, the principle is to augment, but not override, because the source is canonical 
     * on its own time scale. The only exception is the function {@link TimeScaleFunction.UNKNOWN}, which can be 
     * overridden.
     * 
     * @param <S> the event value type
     * @param lastScale the last scale information retrieved
     * @param period the period of ther current time scale to be retrieved
     * @param functionString the function string for the current time scale to be retrieved
     * @param the event whose time scale is to be determined, which helps with messaging
     * @return the current time scale
     * @throws DataAccessException if the current time scale is inconsistent with the last time scale
     */

    private <S> TimeScale checkAndGetLatestScale( TimeScale lastScale,
                                                  Duration period,
                                                  String functionString,
                                                  Event<S> event )
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
            functionToUse = TimeScale.TimeScaleFunction.valueOf( functionString.toUpperCase() );
        }

        // Otherwise, existing scale to help augment?
        if ( Objects.nonNull( this.getDeclaredExistingTimeScale() ) )
        {
            TimeScale declared = this.getDeclaredExistingTimeScale();

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

        TimeScale returnMe = null;

        if ( Objects.nonNull( periodToUse ) && Objects.nonNull( functionToUse ) )
        {
            returnMe = TimeScale.of( periodToUse, functionToUse );
        }

        if ( Objects.nonNull( lastScale ) && !lastScale.equals( returnMe ) )
        {
            throw new DataAccessException( "The time scale information associated with event '" + event
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

    private <S> void addEventToTimeSeries( Event<S> event, TimeSeriesBuilder<S> builder )
    {
        try
        {
            builder.addEvent( event );
        }
        catch ( IllegalArgumentException e )
        {
            throw new DataAccessException( "While processing a time-series for project_id '"
                                           + this.getProjectId()
                                           + "' with variablefeature_id '"
                                           + this.getVariableFeatureId()
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

    private <S> Stream<TimeSeries<S>> composeWithDuplicates( Map<Integer, TimeSeriesBuilder<S>> builders,
                                                             Map<Integer, Integer> seriesCounts )
    {
        List<TimeSeries<S>> streamMe = new ArrayList<>();

        for ( Map.Entry<Integer, TimeSeriesBuilder<S>> nextSeries : builders.entrySet() )
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

    private void addLeadBoundsToScript( ScriptBuilder script, TimeWindow filter, int tabsIn )
    {
        Objects.requireNonNull( script );

        Objects.requireNonNull( filter );

        Long lowerLead = null;
        Long upperLead = null;

        // Lower bound
        if ( !filter.getEarliestLeadDuration().equals( TimeWindow.DURATION_MIN ) )
        {
            lowerLead = filter.getEarliestLeadDuration().toMinutes();

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

                lowerLead = lowered.toMinutes();
            }
        }
        // Upper bound
        if ( !filter.getLatestLeadDuration().equals( TimeWindow.DURATION_MAX ) )
        {
            upperLead = filter.getLatestLeadDuration().toMinutes();
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

    private void addLeadBoundsClauseToScript( ScriptBuilder script, Long lowerLead, Long upperLead, int tabsIn )
    {
        // Add the clause
        if ( Objects.nonNull( lowerLead ) && Objects.nonNull( upperLead )
             && Long.compare( lowerLead, upperLead ) == 0 )
        {
            this.addWhereOrAndClause( script, tabsIn, this.getLeadDurationColumnName() + " = '", upperLead, "'" );
        }
        else
        {
            if ( Objects.nonNull( lowerLead ) )
            {
                this.addWhereOrAndClause( script, tabsIn, this.getLeadDurationColumnName() + " > '", lowerLead, "'" );
            }
            if ( Objects.nonNull( upperLead ) )
            {
                this.addWhereOrAndClause( script,
                                          tabsIn,
                                          this.getLeadDurationColumnName() + LESS_EQUAL,
                                          upperLead,
                                          "'" );
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

    private void addValidTimeBoundsToScriptUsingReferenceTimeAndLeadDuration( ScriptBuilder script,
                                                                              TimeWindow filter,
                                                                              int tabsIn )
    {
        Objects.requireNonNull( script );

        Objects.requireNonNull( filter );

        // Lower and upper bounds are equal
        if ( filter.getEarliestValidTime().equals( filter.getLatestValidTime() ) )
        {
            String validTime = filter.getEarliestValidTime().toString();

            String clause = this.getTimeColumnName()
                            + INTERVAL_1_MINUTE
                            + this.getLeadDurationColumnName()
                            + " = '";

            this.addWhereOrAndClause( script,
                                      tabsIn,
                                      clause,
                                      validTime,
                                      "'" );
        }
        // Lower bound
        else
        {

            if ( !filter.getEarliestValidTime().equals( Instant.MIN ) )
            {
                String lowerValidTime = filter.getEarliestValidTime().toString();

                String clause = this.getTimeColumnName()
                                + INTERVAL_1_MINUTE
                                + this.getLeadDurationColumnName()
                                + " > '";

                this.addWhereOrAndClause( script,
                                          tabsIn,
                                          clause,
                                          lowerValidTime,
                                          "'" );
            }

            // Upper bound
            if ( !filter.getLatestValidTime().equals( Instant.MAX ) )
            {
                String upperValidTime = filter.getLatestValidTime().toString();

                String clause = this.getTimeColumnName()
                                + INTERVAL_1_MINUTE
                                + this.getLeadDurationColumnName()
                                + LESS_EQUAL;

                this.addWhereOrAndClause( script,
                                          tabsIn,
                                          clause,
                                          upperValidTime,
                                          "'" );
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

    private void addValidTimeBoundsToScript( ScriptBuilder script, TimeWindow filter, int tabsIn )
    {
        Objects.requireNonNull( script );

        Objects.requireNonNull( filter );

        Instant lowerValidTime = this.getOrInferLowerValidTime( filter );
        Instant upperValidTime = this.getOrInferUpperValidTime( filter );

        // Add the clauses
        if ( !lowerValidTime.equals( Instant.MIN ) )
        {
            this.addWhereOrAndClause( script, tabsIn, this.getTimeColumnName() + " > '", lowerValidTime, "'" );
        }

        if ( !upperValidTime.equals( Instant.MAX ) )
        {
            this.addWhereOrAndClause( script, tabsIn, this.getTimeColumnName() + LESS_EQUAL, upperValidTime, "'" );
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
                          this.getVariableFeatureId(),
                          this.getLeftOrRightOrBaseline(),
                          lowerValidTime,
                          upperValidTime );
        }
    }

    /**
     * Helper that returns the lower valid time from the {@link TimeWindow}, preferentially, but otherwise infers the 
     * lower valid time from the forecast information present.
     * 
     * @param timeWindow the time window
     * @return the lower valid time
     */

    private Instant getOrInferLowerValidTime( TimeWindow timeWindow )
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
                if ( !timeWindow.getEarliestLeadDuration().equals( TimeWindow.DURATION_MIN ) )
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
     * Helper that returns the upper valid time from the {@link TimeWindow}, preferentially, but otherwise infers the 
     * upper valid time from the forecast information present.
     * 
     * @param timeWindow the time window
     * @return the upper valid time
     */

    private Instant getOrInferUpperValidTime( TimeWindow timeWindow )
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
                 && !timeWindow.getLatestLeadDuration().equals( TimeWindow.DURATION_MAX ) )
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

    private void addReferenceTimeBoundsToScript( ScriptBuilder script, TimeWindow filter, int tabsIn )
    {
        Objects.requireNonNull( script );

        Objects.requireNonNull( filter );

        // Lower and upper bounds are equal
        if ( filter.getEarliestReferenceTime().equals( filter.getLatestReferenceTime() ) )
        {
            String referenceTime = filter.getEarliestReferenceTime().toString();

            this.addWhereOrAndClause( script, tabsIn, this.getTimeColumnName() + " = '", referenceTime, "'" );
        }
        else
        {
            // Lower bound
            if ( !filter.getEarliestReferenceTime().equals( Instant.MIN ) )
            {
                String lowerReferenceTime = filter.getEarliestReferenceTime().toString();
                this.addWhereOrAndClause( script, tabsIn, this.getTimeColumnName() + " > '", lowerReferenceTime, "'" );
            }

            // Upper bound
            if ( !filter.getLatestReferenceTime().equals( Instant.MAX ) )
            {
                String upperReferenceTime = filter.getLatestReferenceTime().toString();
                this.addWhereOrAndClause( script,
                                          tabsIn,
                                          this.getTimeColumnName() + LESS_EQUAL,
                                          upperReferenceTime,
                                          "'" );
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
     * Abstract builder.
     * 
     * @author james.brown@hydrosolved.com
     * @param <S> the type of time-series to build
     */

    abstract static class TimeSeriesRetrieverBuilder<S>
    {
        /**
         * Time window filter.
         */

        private TimeWindow timeWindow;

        /**
         * The <code>wres.Project.project_id</code>.
         */

        private Integer projectId;

        /**
         * The <code>wres.VariableFeature.variablefeature_id</code>.
         */

        private Integer variableFeatureId;

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

        private TimeScale desiredTimeScale;

        /**
         * Declared existing time scale;
         */

        private TimeScale declaredExistingTimeScale;

        /**
         * The start monthday of a season constraint.
         */

        private MonthDay seasonStart;

        /**
         * The end monthday of a season constraint.
         */

        private MonthDay seasonEnd;

        /**
         * Is <code>true</code> to retrieve values from time-series that were distributed across multiple sources. 
         * See #65216.
         * 
         * TODO: please remove me when ingest is time-series-shaped.
         */

        private boolean hasMultipleSourcesPerSeries;

        /**
         * Sets the <code>wres.Project.project_id</code>.
         * 
         * @param projectId the <code>wres.Project.project_id</code>
         * @return the builder
         */

        TimeSeriesRetrieverBuilder<S> setProjectId( int projectId )
        {
            this.projectId = projectId;
            return this;
        }

        /**
         * Sets the <code>wres.VariableFeature.variablefeature_id</code>.
         * 
         * TODO: replace this with a variable name and a list of features. Retrieval should support
         * multiple features and separate them from variables.
         * 
         * @param variableFeatureId the <code>wres.VariableFeature.variablefeature_id</code>
         * @return the builder
         */

        TimeSeriesRetrieverBuilder<S> setVariableFeatureId( int variableFeatureId )
        {
            this.variableFeatureId = variableFeatureId;
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

        TimeSeriesRetrieverBuilder<S> setTimeWindow( TimeWindow timeWindow )
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

        TimeSeriesRetrieverBuilder<S> setDesiredTimeScale( TimeScale desiredTimeScale )
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

        TimeSeriesRetrieverBuilder<S> setDeclaredExistingTimeScale( TimeScale declaredExistingTimeScale )
        {
            this.declaredExistingTimeScale = declaredExistingTimeScale;
            return this;
        }

        /**
         * Sets the status of individual time-series as originating from several sources.
         * 
         * TODO: please remove me when ingest is time-series-shaped.
         * 
         * @param hasMultipleSourcesPerSeries is true if each series originates from multiple sources
         * @return the builder
         */

        TimeSeriesRetrieverBuilder<S> setHasMultipleSourcesPerSeries( boolean hasMultipleSourcesPerSeries )
        {
            this.hasMultipleSourcesPerSeries = hasMultipleSourcesPerSeries;
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

        abstract TimeSeriesRetriever<S> build();
    }

    /**
     * Construct.
     * 
     * @param builder the builder
     * @param timeColumn the name of the time column, which is an implementation detail
     * @param leadDurationColumn the name of the lead duration column, which is an implementation detail
     * @throws NullPointerException if any required input is null
     */

    TimeSeriesRetriever( TimeSeriesRetrieverBuilder<T> builder, String timeColumn, String leadDurationColumn )
    {
        Objects.requireNonNull( builder );

        this.projectId = builder.projectId;
        this.variableFeatureId = builder.variableFeatureId;
        this.lrb = builder.lrb;
        this.timeWindow = builder.timeWindow;
        this.desiredTimeScale = builder.desiredTimeScale;
        this.declaredExistingTimeScale = builder.declaredExistingTimeScale;
        this.unitMapper = builder.unitMapper;
        this.hasMultipleSourcesPerSeries = builder.hasMultipleSourcesPerSeries;
        this.seasonStart = builder.seasonStart;
        this.seasonEnd = builder.seasonEnd;
        this.timeColumn = timeColumn;
        this.leadDurationColumn = leadDurationColumn;

        // Validate
        String validationStart = "Cannot build a time-series retriever without a ";
        Objects.requireNonNull( this.getTimeColumnName(), validationStart + "time column name." );

        Objects.requireNonNull( this.getMeasurementUnitMapper(), validationStart + "measurement unit mapper." );

        if ( Objects.isNull( this.seasonStart ) != Objects.isNull( this.seasonEnd ) )
        {
            throw new IllegalArgumentException( validationStart + "without a fully defined season. Season start: "
                                                + this.seasonStart
                                                + "Season end: "
                                                + this.seasonEnd );
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
                              this.variableFeatureId,
                              this.lrb,
                              "the time window was null: the retrieval will be unconditional in time." );
            }

            if ( Objects.isNull( this.desiredTimeScale ) )
            {
                LOGGER.debug( start,
                              this.projectId,
                              this.variableFeatureId,
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
                              this.variableFeatureId,
                              this.lrb,
                              this.timeWindow,
                              this.desiredTimeScale );
            }

            if ( Objects.isNull( this.leadDurationColumn ) )
            {
                LOGGER.debug( start,
                              this.projectId,
                              this.variableFeatureId,
                              this.lrb,
                              "the supplied lead duration column was null." );
            }
        }

    }

}
