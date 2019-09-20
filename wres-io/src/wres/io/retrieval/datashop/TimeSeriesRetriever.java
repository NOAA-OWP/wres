package wres.io.retrieval.datashop;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.scale.TimeScale;
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
     * Logger.
     */

    private static final Logger LOGGER = LoggerFactory.getLogger( TimeSeriesRetriever.class );

    /**
     * Time window filter.
     */

    private final TimeWindow timeWindow;

    /**
     * The <code>wres.Project.project_id</code>.
     */

    private final Integer projectId;

    /**
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
     * Returns true if the retriever supplies forecast data.
     * 
     * @return true if this instance supplies forecast data
     */

    abstract boolean isForecastRetriever();

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

        LOGGER.debug( "Preparing to execute script with hash {}...", script.hashCode() );

        try ( DataProvider provider = scripter.buffer() )
        {
            Map<Integer, TimeSeriesBuilder<S>> builders = new TreeMap<>();

            Set<TimeSeries<S>> returnMe = new HashSet<>();

            TimeScale timeScale = null;

            while ( provider.next() )
            {
                int seriesId = provider.getInt( "series_id" );

                TimeSeriesBuilder<S> builder = builders.get( seriesId );

                // Start a new series when required                 
                if ( Objects.isNull( builder ) )
                {
                    builder = new TimeSeriesBuilder<>();
                    builders.put( seriesId, builder );
                }

                // Get the valid time
                Instant validTime = provider.getInstant( "valid_time" );

                // Add the reference time
                if ( provider.hasColumn( "reference_time" ) )
                {
                    Instant referenceTime = provider.getInstant( "reference_time" );
                    builder.addReferenceTime( referenceTime, ReferenceTimeType.DEFAULT );
                }

                // Add the event     
                S value = mapper.apply( provider );
                Event<S> event = Event.of( validTime, value );
                builder.addEvent( event );

                // Add the time-scale info
                String functionString = provider.getString( "scale_function" );
                Duration period = provider.getDuration( "scale_period" );

                TimeScale.TimeScaleFunction function =
                        TimeScale.TimeScaleFunction.valueOf( functionString.toUpperCase() );

                TimeScale latestScale = TimeScale.of( period, function );

                if ( Objects.nonNull( timeScale ) && !latestScale.equals( timeScale ) )
                {
                    throw new DataAccessException( "The time scale information associated with event '" + event
                                                   + "' is '"
                                                   + latestScale
                                                   + "' but other events in the same series have a different time "
                                                   + "scale of '"
                                                   + timeScale
                                                   + "', which is not allowed." );
                }

                timeScale = latestScale;
                builder.addTimeScale( latestScale );
            }

            LOGGER.debug( "Finished execute script with hash {}, which retrieved {} time-series.",
                          script.hashCode(),
                          returnMe.size() );

            return builders.values().stream().map( TimeSeriesBuilder::build );
        }
        catch ( SQLException e )
        {
            throw new DataAccessException( "Failed to access the time-series data.", e );
        }
    }

    /**
     * Adds an empty {@link TimeWindow} constraint to the retrieval script. All intervals are treated as left-closed.
     * 
     * @param script the script to augment
     * @param tabsIn the number of tabs in for the outermost clause
     * @throws NullPointerException if the input is null
     */

    void addTimeWindowClause( ScriptBuilder script, int tabsIn )
    {
        Objects.requireNonNull( script );

        // Does the filter exist?
        TimeWindow filter = this.getTimeWindow();
        if ( Objects.nonNull( filter ) )
        {
            // Forecasts?
            if ( this.isForecastRetriever() )
            {
                this.addLeadBoundsToScript( script, filter, tabsIn );
                this.addReferenceTimeBoundsToScript( script, filter, tabsIn );
                this.addValidTimeBoundsToForecastScript( script, filter, tabsIn );
            }
            else
            {
                this.addValidTimeBoundsToObservedScript( script, filter, tabsIn );
            }
        }
    }

    /**
     * Adds the lead duration bounds (if any) to the script. The interval is left-closed.
     * 
     * @param script the script to augment
     * @param tabsIn the number of tabs in for the outermost clause
     * @param filter the time window filter
     * @throws NullPointerException if the script is null
     */

    void addLeadBoundsToScript( ScriptBuilder script, TimeWindow filter, int tabsIn )
    {
        Objects.requireNonNull( script );

        if ( Objects.nonNull( filter ) )
        {
            // Lower bound
            if ( !filter.getEarliestLeadDuration().equals( TimeWindow.DURATION_MIN ) )
            {
                long lowerLead = filter.getEarliestLeadDuration().toMinutes();
                this.addWhereOrAndClause( script, tabsIn, "TSV.lead >= '", lowerLead, "'" );
            }

            // Upper bound
            if ( !filter.getLatestLeadDuration().equals( TimeWindow.DURATION_MAX ) )
            {
                long upperLead = filter.getLatestLeadDuration().toMinutes();
                this.addWhereOrAndClause( script, tabsIn, "TSV.lead < '", upperLead, "'" );
            }
        }
    }

    /**
     * Adds the valid time bounds (if any) to the script. The interval is left-closed.
     * 
     * @param script the script to augment
     * @param filter the time window filter
     * @param tabsIn the number of tabs in for the outermost clause
     * @throws NullPointerException if the script is null
     */

    void addValidTimeBoundsToForecastScript( ScriptBuilder script, TimeWindow filter, int tabsIn )
    {
        Objects.requireNonNull( script );

        if ( Objects.nonNull( filter ) )
        {
            // Lower bound
            if ( !filter.getEarliestValidTime().equals( Instant.MIN ) )
            {
                String lowerValidTime = filter.getEarliestValidTime().toString();

                String clause = "TS.initialization_date + INTERVAL '1' MINUTE * TSV.lead >= '";

                // Observation?
                if ( !this.isForecastRetriever() )
                {
                    clause = "O.observation_time >= '";
                }

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

                String clause = "TS.initialization_date + INTERVAL '1' MINUTE * TSV.lead < '";

                // Observation?
                if ( !this.isForecastRetriever() )
                {
                    clause = "O.observation_time < '";
                }

                this.addWhereOrAndClause( script,
                                          tabsIn,
                                          clause,
                                          upperValidTime,
                                          "'" );
            }
        }
    }

    /**
     * Adds the valid time bounds (if any) to the script. The interval is left-closed.
     * 
     * @param script the script to augment
     * @param filter the time window filter
     * @param tabsIn the number of tabs in for the outermost clause
     * @throws NullPointerException if the script is null
     */

    void addValidTimeBoundsToObservedScript( ScriptBuilder script, TimeWindow filter, int tabsIn )
    {
        Objects.requireNonNull( script );

        if ( Objects.nonNull( filter ) )
        {
            // Lower bound
            if ( !filter.getEarliestValidTime().equals( Instant.MIN ) )
            {
                String lowerValidTime = filter.getEarliestValidTime().toString();
                this.addWhereOrAndClause( script, tabsIn, "O.observation_time >= '", lowerValidTime, "'" );
            }

            // Upper bound
            if ( !filter.getLatestValidTime().equals( Instant.MAX ) )
            {
                String upperValidTime = filter.getLatestValidTime().toString();
                this.addWhereOrAndClause( script, tabsIn, "O.observation_time < '", upperValidTime, "'" );
            }
        }
    }

    /**
     * Adds the reference time bounds (if any) to the script.
     * 
     * @param script the script to augment
     * @param filter the time window filter
     * @param tabsIn the number of tabs in for the outermost clause
     * @throws NullPointerException if the script is null
     */

    void addReferenceTimeBoundsToScript( ScriptBuilder script, TimeWindow filter, int tabsIn )
    {
        Objects.requireNonNull( script );

        if ( Objects.nonNull( filter ) )
        {
            // Lower bound
            if ( !filter.getEarliestReferenceTime().equals( Instant.MIN ) )
            {
                String lowerReferenceTime = filter.getEarliestReferenceTime().toString();
                this.addWhereOrAndClause( script, tabsIn, "TS.initialization_date >= '", lowerReferenceTime, "'" );
            }

            // Upper bound
            if ( !filter.getLatestReferenceTime().equals( Instant.MAX ) )
            {
                String upperReferenceTime = filter.getLatestReferenceTime().toString();
                this.addWhereOrAndClause( script, tabsIn, "TS.initialization_date < '", upperReferenceTime, "'" );
            }
        }
    }

    /**
     * Where available adds the clauses to the input script associated with {@link #getProjectId()}, 
     * {@link #getVariableFeatureId()} and {@ #getLeftOrRightOrBaseline()}.
     * 
     * @param script the script to augment
     * @param tabsIn the number of tabs in for the outermost clause
     */

    void addProjectVariableAndMemberConstraints( ScriptBuilder script, int tabsIn )
    {
        // project_id
        if ( Objects.nonNull( this.getProjectId() ) )
        {
            this.addWhereOrAndClause( script, tabsIn, "PS.project_id = '", this.getProjectId(), "'" );
        }
        // variablefeature_id
        if ( Objects.nonNull( this.getVariableFeatureId() ) )
        {
            if ( this.isForecastRetriever() )
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
     * Abstract builder.
     * 
     * @author james.brown@hydrosolved.com
     * @param <S> the type of time-series to build
     */

    abstract static class TimeSeriesDataShopBuilder<S>
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
         * Sets the <code>wres.Project.project_id</code>.
         * 
         * @param projectId the <code>wres.Project.project_id</code>
         * @return the builder
         */

        TimeSeriesDataShopBuilder<S> setProjectId( int projectId )
        {
            this.projectId = projectId;
            return this;
        }

        /**
         * Sets the <code>wres.VariableFeature.variablefeature_id</code>.
         * 
         * @param variableFeatureId the <code>wres.VariableFeature.variablefeature_id</code>
         * @return the builder
         */

        TimeSeriesDataShopBuilder<S> setVariableFeatureId( int variableFeatureId )
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

        TimeSeriesDataShopBuilder<S> setLeftOrRightOrBaseline( LeftOrRightOrBaseline lrb )
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

        TimeSeriesDataShopBuilder<S> setTimeWindow( TimeWindow timeWindow )
        {
            this.timeWindow = timeWindow;
            return this;
        }

        /**
         * Sets the measurement unit mapper.
         * 
         * @param unitMapper the measurement unit mapper
         * @return the builder
         */

        TimeSeriesDataShopBuilder<S> setUnitMapper( UnitMapper unitMapper )
        {
            this.unitMapper = unitMapper;
            return this;
        }

        abstract TimeSeriesRetriever<S> build();
    }

    /**
     * Construct.
     */

    TimeSeriesRetriever( TimeSeriesDataShopBuilder<T> builder )
    {
        Objects.requireNonNull( builder );

        this.projectId = builder.projectId;
        this.variableFeatureId = builder.variableFeatureId;
        this.lrb = builder.lrb;
        this.timeWindow = builder.timeWindow;
        this.unitMapper = builder.unitMapper;

        // Validate
        Objects.requireNonNull( this.getMeasurementUnitMapper(),
                                "Cannot build a time-series retriever without a "
                                                                 + "measurement unit mapper." );

    }

}
