package wres.io.retrieval.dao;

import java.text.MessageFormat;
import java.time.Instant;
import java.util.Objects;

import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeWindow;
import wres.io.config.LeftOrRightOrBaseline;
import wres.io.utilities.ScriptBuilder;

/**
 * An abstract DAO for the retrieval of {@link TimeSeries} from the WRES database.
 * 
 * @author james.brown@hydrosolved.com
 */

abstract class TimeSeriesDAO<T> implements WresDAO<TimeSeries<T>>
{

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
     * Creates a {@link TimeSeries} from a script that retrieves time-series data.
     * 
     * @param script the script
     * @return the time-series
     * @throws DataAccessException if data could not be accessed for whatever reason
     */

    abstract TimeSeries<T> getTimeSeriesFromScript( String script ); 
    
    /**
     * Adds a {@link TimeWindow} constraint to the retrieval script.
     * 
     * @param script the script to augment
     * @throws NullPointerException if the input is null
     */
    
    String addTimeWindowToForecastScript( String script )
    {
        Objects.requireNonNull( script );
     
        TimeWindow filter = this.getTimeWindow();

        String returnMe = script;

        // Does the filter exist?
        if ( Objects.nonNull( filter ) )
        {

            // Get the time constraints       
            long lowerLead = Integer.MIN_VALUE;
            long upperLead = Integer.MAX_VALUE;
            String lowerReferenceTime = "-Infinity";
            String upperReferenceTime = "Infinity";
            String lowerValidTime = "-Infinity";
            String upperValidTime = "Infinity";

            // Lead durations
            if ( filter.getEarliestLeadDuration().toMinutes() > lowerLead )
            {
                lowerLead = filter.getEarliestLeadDuration().toMinutes();
            }
            if ( filter.getLatestLeadDuration().toMinutes() < upperLead )
            {
                upperLead = filter.getLatestLeadDuration().toMinutes();
            }

            // Reference times
            if ( filter.getEarliestReferenceTime() != Instant.MIN )
            {
                lowerReferenceTime = filter.getEarliestReferenceTime().toString();
            }
            if ( filter.getLatestReferenceTime() != Instant.MAX )
            {
                upperReferenceTime = filter.getLatestReferenceTime().toString();
            }

            // Valid times
            if ( filter.getEarliestValidTime() != Instant.MIN )
            {
                lowerValidTime = filter.getEarliestValidTime().toString();
            }

            if ( filter.getLatestValidTime() != Instant.MAX )
            {
                upperValidTime = filter.getLatestValidTime().toString();
            }

            // Double quote any existing quoted elements as the formatter will replace these otherwise
            String formatted = script.replace( "'", "''" );

            ScriptBuilder scripter = new ScriptBuilder( formatted );

            scripter.addLine( "WHERE TSV.lead >= ''{0,number,#}''" );
            scripter.addTab().add( "AND " );
            scripter.addLine( "TSV.lead <= ''{1,number,#}''" );
            scripter.addTab().add( "AND " );
            scripter.addLine( "TS.initialization_date >= ''{2}''" );
            scripter.addTab().add( "AND " );
            scripter.addLine( "TS.initialization_date <= ''{3}''" );
            scripter.addTab().add( "AND " );
            scripter.addLine( "TS.initialization_date + INTERVAL ''1 MINUTE'' * TSV.lead >= ''{4}''" );
            scripter.addTab().add( "AND " );
            scripter.addLine( "TS.initialization_date + INTERVAL ''1 MINUTE'' * TSV.lead <= ''{5}''" );

            returnMe = MessageFormat.format( scripter.toString(),
                                             lowerLead,
                                             upperLead,
                                             lowerReferenceTime,
                                             upperReferenceTime,
                                             lowerValidTime,
                                             upperValidTime );
        }

        return returnMe;
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
        
        abstract TimeSeriesDAO<S> build();
    }
    
    /**
     * Construct.
     */

    TimeSeriesDAO( TimeSeriesDAOBuilder<T> builder )
    {
        Objects.requireNonNull( builder );
        
        this.projectId = builder.projectId;
        this.variableFeatureId = builder.variableFeatureId;
        this.lrb = builder.lrb;
        this.timeWindow = builder.timeWindow;
    }

}
