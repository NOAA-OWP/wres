package wres.io.retrieval.dao;

import java.sql.SQLException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * An abstract DAO for the retrieval of {@link TimeSeries} from the WRES database.
 * 
 * @author james.brown@hydrosolved.com
 */

abstract class TimeSeriesDataShop<T> implements WresDataShop<TimeSeries<T>>
{

    /**
     * Logger.
     */

    private static final Logger LOGGER = LoggerFactory.getLogger( TimeSeriesDataShop.class );

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

    <S> Set<TimeSeries<S>> getTimeSeriesFromScript( String script, Function<DataProvider, S> mapper )
    {
        // Acquire the raw time-series data from the db for the input time-series identifier
        DataScripter scripter = new DataScripter( script );

        LOGGER.debug( "Preparing to execute script with hash {}...", script.hashCode() );

        try ( DataProvider provider = scripter.buffer() )
        {

            // Identifier for the last series read
            int lastSeriesId = Integer.MIN_VALUE;

            TimeSeriesBuilder<S> builder = new TimeSeriesBuilder<>();

            Set<TimeSeries<S>> returnMe = new HashSet<>();

            while ( provider.next() )
            {
                int seriesId = provider.getInt( "series_id" );

                // Start a new series when required                 
                if ( seriesId != lastSeriesId )
                {
                    lastSeriesId = seriesId;
                    returnMe.add( builder.build() );
                    builder = new TimeSeriesBuilder<>();
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
                builder.addEvent( Event.of( validTime, value ) );
            }

            LOGGER.debug( "Finished execute script with hash {}, which retrieved {} time-series.",
                          script.hashCode(),
                          returnMe.size() );

            return Collections.unmodifiableSet( returnMe );
        }
        catch ( SQLException e )
        {
            throw new DataAccessException( "Failed to access the time-series data.", e );
        }
    }

    /**
     * Adds an empty {@link TimeWindow} constraint to the retrieval script.
     * 
     * @param script the script to augment
     * @throws NullPointerException if the input is null
     */

    void addTimeWindowClause( ScriptBuilder script )
    {
        Objects.requireNonNull( script );

        // Does the filter exist?
        TimeWindow filter = this.getTimeWindow();
        if ( Objects.nonNull( filter ) )
        {
            // Forecasts?
            if ( this.isForecastRetriever() )
            {
                this.addLeadBoundsToScript( script, filter );
                this.addReferenceTimeBoundsToScript( script, filter );
                this.addValidTimeBoundsToForecastScript( script, filter );
            }
            else
            {
                this.addValidTimeBoundsToObservedScript( script, filter );
            }
        }
    }

    /**
     * Adds the lead duration bounds (if any) to the script.
     * 
     * @param script the script to augment
     * @param filter the time window filter
     * @throws NullPointerException if the script is null
     */

    void addLeadBoundsToScript( ScriptBuilder script, TimeWindow filter )
    {
        Objects.requireNonNull( script );

        if ( Objects.nonNull( filter ) )
        {
            // Lower bound
            if ( !filter.getEarliestLeadDuration().equals( TimeWindow.DURATION_MIN ) )
            {
                long lowerLead = filter.getEarliestLeadDuration().toMinutes();
                this.addWhereOrAndClause( script, "TSV.lead >= '", lowerLead, "'" );
            }

            // Upper bound
            if ( !filter.getLatestLeadDuration().equals( TimeWindow.DURATION_MAX ) )
            {
                long upperLead = filter.getLatestLeadDuration().toMinutes();
                this.addWhereOrAndClause( script, "TSV.lead <= '", upperLead, "'" );
            }
        }
    }

    /**
     * Adds the valid time bounds (if any) to the script.
     * 
     * @param script the script to augment
     * @param filter the time window filter
     * @throws NullPointerException if the script is null
     */

    void addValidTimeBoundsToForecastScript( ScriptBuilder script, TimeWindow filter )
    {
        Objects.requireNonNull( script );

        if ( Objects.nonNull( filter ) )
        {
            // Lower bound
            if ( !filter.getEarliestValidTime().equals( Instant.MIN ) )
            {
                String lowerValidTime = filter.getEarliestValidTime().toString();
                this.addWhereOrAndClause( script,
                                          "TS.initialization_date + INTERVAL '1 MINUTE' * TSV.lead >= '",
                                          lowerValidTime,
                                          "'" );
            }

            // Upper bound
            if ( !filter.getLatestValidTime().equals( Instant.MAX ) )
            {
                String upperValidTime = filter.getLatestValidTime().toString();
                this.addWhereOrAndClause( script,
                                          "TS.initialization_date + INTERVAL '1 MINUTE' * TSV.lead <= '",
                                          upperValidTime,
                                          "'" );
            }
        }
    }

    /**
     * Adds the valid time bounds (if any) to the script.
     * 
     * @param script the script to augment
     * @param filter the time window filter
     * @throws NullPointerException if the script is null
     */

    void addValidTimeBoundsToObservedScript( ScriptBuilder script, TimeWindow filter )
    {
        Objects.requireNonNull( script );

        if ( Objects.nonNull( filter ) )
        {
            // Lower bound
            if ( !filter.getEarliestValidTime().equals( Instant.MIN ) )
            {
                String lowerValidTime = filter.getEarliestValidTime().toString();
                this.addWhereOrAndClause( script, "O.observation_time >= '", lowerValidTime, "'" );
            }

            // Upper bound
            if ( !filter.getLatestValidTime().equals( Instant.MAX ) )
            {
                String upperValidTime = filter.getLatestValidTime().toString();
                this.addWhereOrAndClause( script, "O.observation_time <= '", upperValidTime, "'" );
            }
        }
    }

    /**
     * Adds the reference time bounds (if any) to the script.
     * 
     * @param script the script to augment
     * @param filter the time window filter
     * @throws NullPointerException if the script is null
     */

    void addReferenceTimeBoundsToScript( ScriptBuilder script, TimeWindow filter )
    {
        Objects.requireNonNull( script );

        if ( Objects.nonNull( filter ) )
        {
            // Lower bound
            if ( !filter.getEarliestReferenceTime().equals( Instant.MIN ) )
            {
                String lowerReferenceTime = filter.getEarliestReferenceTime().toString();
                this.addWhereOrAndClause( script, "TS.initialization_date >= '", lowerReferenceTime, "'" );
            }

            // Upper bound
            if ( !filter.getLatestReferenceTime().equals( Instant.MAX ) )
            {
                String upperReferenceTime = filter.getLatestReferenceTime().toString();
                this.addWhereOrAndClause( script, "TS.initialization_date <= '", upperReferenceTime, "'" );
            }
        }
    }

    /**
     * Where available adds the clauses to the input script associated with {@link #getProjectId()}, 
     * {@link #getVariableFeatureId()} and {@ #getLeftOrRightOrBaseline()}.
     * 
     * @param script the script to augment
     */

    void addProjectVariableAndMemberConstraints( ScriptBuilder script )
    {
        // project_id
        if ( Objects.nonNull( this.getProjectId() ) )
        {
            this.addWhereOrAndClause( script, "P.project_id = '", this.getProjectId(), "'" );
        }
        // variablefeature_id
        if ( Objects.nonNull( this.getVariableFeatureId() ) )
        {
            this.addWhereOrAndClause( script, "TS.variablefeature_id = '", this.getVariableFeatureId(), "'" );
        }
        // member
        if ( Objects.nonNull( this.getLeftOrRightOrBaseline() ) )
        {
            this.addWhereOrAndClause( script,
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
     * Adds a clause to a script according to the start of the last available clause. When the last available clause
     * starts with <code>WHERE</code>, then the clause added starts with <code>AND</code>, otherwise <code>WHERE</code>. 
     * 
     * @param script the script
     * @param clauseElements the clause elements
     */

    void addWhereOrAndClause( ScriptBuilder script, Object... clauseElements )
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

        if ( lastLine.contains( "WHERE" ) || lastLine.contains( "AND" ) )
        {
            script.addTab().addLine( "AND ", clause );
        }
        else
        {
            script.addLine( "WHERE ", clause );
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

    abstract static class TimeSeriesDAOBuilder<S>
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
         * Sets the <code>wres.Project.project_id</code>.
         * 
         * @param projectId the <code>wres.Project.project_id</code>
         * @return the builder
         */

        TimeSeriesDAOBuilder<S> setProjectId( int projectId )
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

        TimeSeriesDAOBuilder<S> setVariableFeatureId( int variableFeatureId )
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

        TimeSeriesDAOBuilder<S> setLeftOrRightOrBaseline( LeftOrRightOrBaseline lrb )
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

        TimeSeriesDAOBuilder<S> setTimeWindow( TimeWindow timeWindow )
        {
            this.timeWindow = timeWindow;
            return this;
        }

        abstract TimeSeriesDataShop<S> build();
    }

    /**
     * Construct.
     */

    TimeSeriesDataShop( TimeSeriesDAOBuilder<T> builder )
    {
        Objects.requireNonNull( builder );

        this.projectId = builder.projectId;
        this.variableFeatureId = builder.variableFeatureId;
        this.lrb = builder.lrb;
        this.timeWindow = builder.timeWindow;
    }

}
