package wres.io.retrieval.dao;

import java.sql.SQLException;
import java.text.MessageFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.LongStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.time.Event;
import wres.datamodel.time.ReferenceTimeType;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeries.TimeSeriesBuilder;
import wres.datamodel.time.TimeWindow;
import wres.io.utilities.DataProvider;
import wres.io.utilities.DataScripter;
import wres.io.utilities.ScriptBuilder;

/**
 * A DAO for the retrieval of forecasts of double values as {@link TimeSeries}.
 * 
 * <p>TODO: A lot of the handling in this class is boilerplate for different types of forecast event, not only
 * single-valued forecasts, which are represented by {@link Double} event. Consider abstracting away some of this 
 * boilerplate when adding DAOs for other types, such as ensemble forecasts. The differences in handling between 
 * different event types amount to the script that queries the database (e.g., for a double array vs. a double) and
 * a mapper between the database values and the required/composed type. 
 * 
 * @author james.brown@hydrosolved.com
 */

public class SingleValuedForecastDAO extends TimeSeriesDAO<Double>
{

    /**
     * Template script for the {@link #get(long)}.
     */

    private static final String GET_SCRIPT = SingleValuedForecastDAO.getScriptForGet();

    /**
     * Template script for the {@link SingleValuedForecastDAO#getAllIdentifiers()}.
     */

    private static final String GET_ALL_IDENTIFIERS_SCRIPT =
            SingleValuedForecastDAO.getScriptForGetAllIdentifiers();

    /**
     * Logger.
     */

    private static final Logger LOGGER = LoggerFactory.getLogger( SingleValuedForecastDAO.class );

    /**
     * Builder.
     */

    public static class Builder extends TimeSeriesDAOBuilder<Double>
    {

        @Override
        SingleValuedForecastDAO build()
        {
            return new SingleValuedForecastDAO( this );
        }

    }

    /**
     * Returns an instance.
     * 
     * @return the DAO instance
     */

    public static SingleValuedForecastDAO of()
    {
        return new Builder().build();
    }

    /**
     * Returns an instance.
     * 
     * @param timeWindow the time window
     * @return the DAO instance
     * @throws NullPointerException if the input is null
     */

    public static SingleValuedForecastDAO of( TimeWindow timeWindow )
    {
        return (SingleValuedForecastDAO) new Builder().setTimeWindow( timeWindow ).build();
    }

    @Override
    public Optional<TimeSeries<Double>> get( long identifier )
    {
        String script = MessageFormat.format( GET_SCRIPT, identifier );

        // Time window constraint for individual series?
        script = this.addTimeWindowToForecastScript( script );

        ScriptBuilder scripter = new ScriptBuilder( script );

        scripter.add( "ORDER BY TSV.lead;" );

        if ( LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( "Built the following script with hash {} for time-series retrieval:{}{}",
                          script.hashCode(),
                          System.lineSeparator(),
                          script );
        }

        return Optional.of( this.getTimeSeriesFromScript( script ) );
    }

    @Override
    public LongStream getAllIdentifiers()
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

        String script = MessageFormat.format( GET_ALL_IDENTIFIERS_SCRIPT,
                                              this.getProjectId(),
                                              this.getVariableFeatureId(),
                                              this.getLeftOrRightOrBaseline().toString().toLowerCase() );

        // Acquire the time-series identifiers
        DataScripter scripter = new DataScripter( script );

        LOGGER.debug( "Preparing to execute script with hash {}...", script.hashCode() );

        try ( DataProvider provider = scripter.buffer() )
        {
            LongStream.Builder b = LongStream.builder();

            while ( provider.next() )
            {
                b.add( provider.getLong( "timeseries_id" ) );
            }

            return b.build();
        }
        catch ( SQLException e )
        {
            throw new DataAccessException( "Failed to access the time-series identifiers.", e );
        }
    }

    /**
     * Creates a {@link TimeSeries} from a script that retrieves time-series data.
     * 
     * @param script the script
     * @return the time-series
     * @throws DataAccessException if data could not be accessed for whatever reason
     */
    @Override
    TimeSeries<Double> getTimeSeriesFromScript( String script )
    {
        // Acquire the raw time-series data from the db for the input time-series identifier
        DataScripter scripter = new DataScripter( script );

        LOGGER.debug( "Preparing to execute script with hash {}...", script.hashCode() );

        try ( DataProvider provider = scripter.buffer() )
        {
            TimeSeriesBuilder<Double> builder = new TimeSeriesBuilder<>();

            while ( provider.next() )
            {
                // Get the event data
                Instant referenceTime = provider.getInstant( "reference_time" );
                Duration leadDuration = provider.getDuration( "lead_duration" );
                Double value = provider.getDouble( "measurement" );
                Instant validTime = referenceTime.plus( leadDuration );

                // Add the event
                builder.addReferenceTime( referenceTime, ReferenceTimeType.DEFAULT );
                builder.addEvent( Event.of( validTime, value ) );
            }

            TimeSeries<Double> returnMe = builder.build();

            LOGGER.debug( "Finished execute script with hash {}, which retrieved one time-series with {} events.",
                          script.hashCode(),
                          returnMe.getEvents().size() );

            return returnMe;
        }
        catch ( SQLException e )
        {
            throw new DataAccessException( "Failed to access the time-series data.", e );
        }
    }

    /**
     * Returns an unpopulated script to acquire a time-series from the WRES database. The placeholders are in the
     * {@link MessageFormat} format. This is akin to a prepared statement string.
     * 
     * @return an unpopulated script for the time-series
     */

    private static String getScriptForGet()
    {
        ScriptBuilder scripter = new ScriptBuilder();

        scripter.add( "SELECT " );
        scripter.addLine()
                .addTab()
                .addLine( "EXTRACT( epoch FROM TS.initialization_date + INTERVAL ''1 MINUTE'' * TSV.lead )::bigint AS valid_time," );
        scripter.addTab().addLine( "TS.initialization_date AS reference_time," );
        scripter.addTab().addLine( "TSV.lead AS lead_duration," );
        scripter.addTab().addLine( "TSV.series_value AS measurement" );
        scripter.addLine( "FROM (" );
        scripter.addTab()
                .addLine( "SELECT TS.initialization_date, TS.timeseries_id" );
        scripter.addTab().addLine( "FROM wres.TimeSeries TS" );
        scripter.addTab().addLine( "WHERE TS.timeseries_id = ''{0}''" );
        scripter.addLine( ") AS TS" );
        scripter.addLine( "INNER JOIN wres.TimeSeriesValue TSV" );
        scripter.addTab().addLine( "ON TS.timeseries_id = TSV.timeseries_id" );

        return scripter.toString();
    }

    /**
     * Returns an unpopulated script to acquire the time-series identifiers.  The placeholders are in the
     * {@link MessageFormat} format. This is akin to a prepared statement string.
     * 
     * @return an unpopulated script for the time-series identifiers
     */

    private static String getScriptForGetAllIdentifiers()
    {
        ScriptBuilder scripter = new ScriptBuilder();

        scripter.add( "SELECT TS.timeseries_id" );
        scripter.addLine().add( "FROM wres.TimeSeries TS" );
        scripter.addLine().add( "INNER JOIN wres.TimeSeriesSource TSS ON TS.timeseries_id = TSS.timeseries_id" );
        scripter.addLine().add( "INNER JOIN wres.ProjectSource PS ON TSS.source_id = PS.source_id" );
        scripter.addLine().add( "INNER JOIN wres.Project P ON PS.project_id = P.project_id" );
        scripter.addLine().add( "WHERE PS.project_id = ''{0}''" );
        scripter.addLine().addTab().add( "AND TS.variablefeature_id = ''{1}''" );
        scripter.addLine().addTab().add( "AND PS.member = ''{2}''" );

        return scripter.toString();
    }

    /**
     * Construct.
     * 
     * @param timeWindow the time window
     * @throws NullPointerException if the filter is null
     */

    private SingleValuedForecastDAO( Builder builder )
    {
        super( builder );
    }

}
