package wres.io.retrieval.dao;

import java.sql.SQLException;
import java.text.MessageFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

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

public class SingleValuedForecastDAO implements WresDAO<TimeSeries<Double>>
{

    /**
     * Template script.
     */

    private static final String SCRIPT = SingleValuedForecastDAO.getScript();

    /**
     * Logger.
     */

    private static final Logger LOGGER = LoggerFactory.getLogger( SingleValuedForecastDAO.class );

    /**
     * Time window to restrict retrieval.
     */

    private final TimeWindow timeFilter;

    /**
     * Returns an instance.
     * 
     * @return the DAO instance
     */

    public static SingleValuedForecastDAO of()
    {
        // Unbounded time window
        return new SingleValuedForecastDAO( TimeWindow.of() );
    }

    /**
     * Returns an instance.
     * 
     * @param timeFilter the time filter
     * @return the DAO instance
     * @throws NullPointerException if the input is null
     */

    public static SingleValuedForecastDAO of( TimeWindow timeFilter )
    {
        return new SingleValuedForecastDAO( timeFilter );
    }

    @Override
    public Optional<TimeSeries<Double>> get( long identifier )
    {
        // TODO: abstract away this boilerplate for other forecast implementations,
        // like ensemble forecasts

        // Get the time constraints       
        long lowerLead = Integer.MIN_VALUE;
        long upperLead = Integer.MAX_VALUE;
        String lowerReferenceTime = "-Infinity";
        String upperReferenceTime = "Infinity";
        String lowerValidTime = "-Infinity";
        String upperValidTime = "Infinity";

        // Map the filter to valid database constraints
        TimeWindow filter = this.getTimeFilter();

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

        String script = MessageFormat.format( SCRIPT,
                                              identifier,
                                              lowerLead,
                                              upperLead,
                                              lowerReferenceTime,
                                              upperReferenceTime,
                                              lowerValidTime,
                                              upperValidTime );

        LOGGER.debug( "Built the following script with hash {} for time-series retrieval:{}{}",
                      script.hashCode(),
                      System.lineSeparator(),
                      script );

        return Optional.of( SingleValuedForecastDAO.getTimeSeriesFromScript( script ) );
    }

    /**
     * Returns an unpopulated script to acquire a time-series from the WRES database. The placeholders are in the
     * {@link MessageFormat} format. This is akin to a prepared statement string.
     * 
     * @return an unpopulated script
     */

    private static String getScript()
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
        scripter.addLine( "WHERE TSV.lead >= ''{1,number,#}''" );
        scripter.addTab().add( "AND " );
        scripter.addLine( "TSV.lead <= ''{2,number,#}''" );
        scripter.addTab().add( "AND " );
        scripter.addLine( "TS.initialization_date >= ''{3}''" );
        scripter.addTab().add( "AND " );
        scripter.addLine( "TS.initialization_date <= ''{4}''" );
        scripter.addTab().add( "AND " );
        scripter.addLine( "TS.initialization_date + INTERVAL ''1 MINUTE'' * TSV.lead >= ''{5}''" );
        scripter.addTab().add( "AND " );
        scripter.addLine( "TS.initialization_date + INTERVAL ''1 MINUTE'' * TSV.lead <= ''{6}''" );
        scripter.add( "ORDER BY TSV.lead;" );

        return scripter.toString();
    }

    /**
     * Creates a {@link TimeSeries} from a script that retrieves time-series data.
     * 
     * @param script the script
     * @return the time-series
     * @throws DataAccessException if data could not be accessed for whatever reason
     */

    private static TimeSeries<Double> getTimeSeriesFromScript( String script )
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
     * Returns the time filter.
     * 
     * @return the time filter
     */

    private TimeWindow getTimeFilter()
    {
        return this.timeFilter;
    }

    /**
     * @param timeFilter the time filter
     * @throws NullPointerException if the filter is null
     */

    private SingleValuedForecastDAO( TimeWindow timeFilter )
    {
        Objects.requireNonNull( timeFilter );

        this.timeFilter = timeFilter;
    }


}
