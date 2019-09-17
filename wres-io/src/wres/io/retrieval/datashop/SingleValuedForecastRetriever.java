package wres.io.retrieval.datashop;

import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.Objects;
import java.util.Optional;

import java.util.StringJoiner;
import java.util.function.DoubleUnaryOperator;
import java.util.function.Function;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.time.TimeSeries;
import wres.io.utilities.DataProvider;
import wres.io.utilities.DataScripter;
import wres.io.utilities.ScriptBuilder;

/**
 * Retrieves {@link TimeSeries} of single-valued forecasts from the WRES database.
 * 
 * @author james.brown@hydrosolved.com
 */

class SingleValuedForecastRetriever extends TimeSeriesRetriever<Double>
{

    /**
     * Script string re-used several times. 
     */

    private static final String FROM_WRES_TIME_SERIES_TS = "FROM wres.TimeSeries TS";

    /**
     * Measurement string re-used several times.
     */

    private static final String MEASUREMENT = "measurement";

    /**
     * Log message.
     */

    private static final String LOG_SCRIPT = "Built the following script with hash {} for time-series retrieval:{}{}";

    /**
     * Template script for the {@link #get(long)}.
     */

    private static final String GET_ONE_TIME_SERIES_SCRIPT =
            SingleValuedForecastRetriever.getScriptForGetOneTimeSeries();

    /**
     * Template script for the {@link #getAllIdentifiers()}.
     */

    private static final String GET_ALL_IDENTIFIERS_SCRIPT =
            SingleValuedForecastRetriever.getScriptForGetAllIdentifiers();

    /**
     * Template script for the {@link #getAll()}.
     */

    private static final String GET_ALL_TIME_SERIES_SCRIPT =
            SingleValuedForecastRetriever.getScriptForGetAllTimeSeries();

    /**
     * Logger.
     */

    private static final Logger LOGGER = LoggerFactory.getLogger( SingleValuedForecastRetriever.class );

    /**
     * Builder.
     */

    public static class Builder extends TimeSeriesDataShopBuilder<Double>
    {

        @Override
        SingleValuedForecastRetriever build()
        {
            return new SingleValuedForecastRetriever( this );
        }

    }

    /**
     * Reads a time-series by <code>wres.TimeSeries.timeseries_id</code>.
     * 
     * @param identifier the <code>wres.TimeSeries.timeseries_id</code>
     * @return a possible time-series for the given identifier
     */

    @Override
    public Optional<TimeSeries<Double>> get( long identifier )
    {
        String script = MessageFormat.format( GET_ONE_TIME_SERIES_SCRIPT, identifier );

        ScriptBuilder scripter = new ScriptBuilder( script );

        // Time window constraint for individual series?
        this.addTimeWindowClause( scripter );

        // Add ORDER BY clause
        scripter.addLine( "ORDER BY TSV.lead;" );

        script = scripter.toString();

        if ( LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( LOG_SCRIPT,
                          script.hashCode(),
                          System.lineSeparator(),
                          script );
        }

        return this.getTimeSeriesFromScript( script, this.getDataSupplier() )
                   .findFirst();
    }

    /**
     * Returns all of the <code>wres.TimeSeries.timeseries_id</code> associated with this instance.
     * 
     * @return a stream of<code>wres.TimeSeries.timeseries_id</code>
     */

    @Override
    public LongStream getAllIdentifiers()
    {
        this.validateForMultiSeriesRetrieval();

        ScriptBuilder scripter = new ScriptBuilder( GET_ALL_IDENTIFIERS_SCRIPT );

        // Add basic constraints
        this.addProjectVariableAndMemberConstraints( scripter );

        String script = scripter.toString();

        // Acquire the time-series identifiers
        DataScripter dataScripter = new DataScripter( script );

        if ( LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( LOG_SCRIPT,
                          script.hashCode(),
                          System.lineSeparator(),
                          script );
        }

        try ( DataProvider provider = dataScripter.buffer() )
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
     * Overrides the default implementation to get all specified time-series in one pull, rather than one pull for 
     * each series.
     * 
     * @param identifiers the stream of identifiers
     * @return a stream over the identified objects
     * @throws NullPointerException if the input is null
     */
    @Override
    public Stream<TimeSeries<Double>> get( LongStream identifiers )
    {
        Objects.requireNonNull( identifiers );

        this.validateForMultiSeriesRetrieval();

        ScriptBuilder scripter = new ScriptBuilder( GET_ALL_TIME_SERIES_SCRIPT );

        // Add basic constraints
        this.addProjectVariableAndMemberConstraints( scripter );

        // Time window constraint
        this.addTimeWindowClause( scripter );

        // Add constraint on the timeseries_ids provided
        StringJoiner joiner = new StringJoiner( ",", "{", "}" );
        identifiers.forEach( next -> joiner.add( Long.toString( next ) ) );
        scripter.addTab().addLine( "AND TS.timeseries_id = ANY( '", joiner.toString(), "' )::integer[]" );

        // Add ORDER BY clause
        scripter.addLine( "ORDER BY TS.initialization_date, TSV.lead;" );

        String script = scripter.toString();

        if ( LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( LOG_SCRIPT,
                          script.hashCode(),
                          System.lineSeparator(),
                          script );
        }

        return this.getTimeSeriesFromScript( script, this.getDataSupplier() );
    }
    
    /**
     * Overrides the default implementation to get all time-series in one pull, rather than one pull for each series.
     * 
     * @return the possible object
     * @throws DataAccessException if the data could not be accessed for whatever reason
     */

    @Override
    public Stream<TimeSeries<Double>> getAll()
    {
        this.validateForMultiSeriesRetrieval();

        ScriptBuilder scripter = new ScriptBuilder( GET_ALL_TIME_SERIES_SCRIPT );

        // Add basic constraints
        this.addProjectVariableAndMemberConstraints( scripter );

        // Add time window constraint
        this.addTimeWindowClause( scripter );

        // Add ORDER BY clause
        scripter.addLine( "ORDER BY TS.initialization_date, TSV.lead;" );

        String script = scripter.toString();

        if ( LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( LOG_SCRIPT,
                          script.hashCode(),
                          System.lineSeparator(),
                          script );
        }
        
        return this.getTimeSeriesFromScript( script, this.getDataSupplier() );
    }

    @Override
    boolean isForecastRetriever()
    {
        return true;
    }
    
    /**
     * Returns a function that obtains the measured value in the desired units.
     * 
     * @return a function to obtain the measured value in the correct units
     */

    private Function<DataProvider, Double> getDataSupplier()
    {
        return provider -> {           
            // Raw value
            double unmapped = provider.getDouble( MEASUREMENT );
            
            // Existing units
            int measurementUnitId = provider.getInt( "measurementunit_id" );
            
            // Units mapper
            DoubleUnaryOperator mapper = this.getMeasurementUnitMapper().getUnitMapper( measurementUnitId );
            
            // Convert
            return mapper.applyAsDouble( unmapped );
        };
    }
    
    /**
     * Returns an unpopulated script to acquire a time-series from the WRES database. The placeholders are in the
     * {@link MessageFormat} format. This is akin to a prepared statement string.
     * 
     * @return an unpopulated script for the time-series
     */

    private static String getScriptForGetOneTimeSeries()
    {
        ScriptBuilder scripter = new ScriptBuilder();

        scripter.addLine( "SELECT " );
        scripter.addTab().addLine( "TS.initialization_date + INTERVAL ''1 MINUTE'' * TSV.lead AS valid_time," );
        scripter.addTab().addLine( "TS.initialization_date AS reference_time," );
        scripter.addTab().addLine( "TSV.series_value AS measurement," );
        scripter.addTab().addLine( "TS.measurementunit_id" ); 
        scripter.addLine( "FROM (" );
        scripter.addTab().addLine( "SELECT TS.initialization_date, TS.timeseries_id" );
        scripter.addTab().addLine( FROM_WRES_TIME_SERIES_TS );
        scripter.addTab().addLine( "WHERE TS.timeseries_id = ''{0}''" );
        scripter.addLine( ") AS TS" );
        scripter.addLine( "INNER JOIN wres.TimeSeriesValue TSV" );
        scripter.addTab().addLine( "ON TS.timeseries_id = TSV.timeseries_id" );

        return scripter.toString();
    }

    /**
     * Returns the start of a script to acquire a time-series from the WRES database for all time-series.
     * 
     * @return the start of a script for the time-series
     */

    private static String getScriptForGetAllTimeSeries()
    {
        ScriptBuilder scripter = new ScriptBuilder();

        scripter.addLine( "SELECT " );
        scripter.addTab().addLine( "TS.timeseries_id AS series_id," );
        scripter.addTab().addLine( "TS.initialization_date + INTERVAL '1' MINUTE * TSV.lead AS valid_time," );
        scripter.addTab().addLine( "TS.initialization_date AS reference_time," );
        scripter.addTab().addLine( "TSV.series_value AS measurement," );
        scripter.addTab().addLine( "TS.measurementunit_id" );       
        scripter.addLine( FROM_WRES_TIME_SERIES_TS );
        scripter.addLine( "INNER JOIN wres.TimeSeriesValue TSV" );
        scripter.addTab().addLine( "ON TSV.timeseries_id = TS.timeseries_id" );
        scripter.addLine( "INNER JOIN wres.TimeSeriesSource TSS" );
        scripter.addTab().addLine( "ON TSS.timeseries_id = TS.timeseries_id" );
        scripter.addTab( 2 ).addLine( "AND (TSS.lead IS NULL OR TSS.lead = TSV.lead)" );
        scripter.addLine( "INNER JOIN wres.ProjectSource PS" );
        scripter.addTab().addLine( "ON PS.source_id = TSS.source_id" );
        scripter.addLine( "INNER JOIN wres.Project P" );
        scripter.addTab().addLine( "ON P.project_id = PS.project_id" );

        return scripter.toString();
    }

    /**
     * Returns the start of a script to acquire the time-series identifiers.
     * 
     * @return the start of a script for the time-series identifiers
     */

    private static String getScriptForGetAllIdentifiers()
    {
        ScriptBuilder scripter = new ScriptBuilder();

        scripter.addLine( "SELECT TS.timeseries_id" );
        scripter.addLine( FROM_WRES_TIME_SERIES_TS );
        scripter.addLine( "INNER JOIN wres.TimeSeriesSource TSS" );
        scripter.addTab().addLine( "ON TS.timeseries_id = TSS.timeseries_id" );
        scripter.addLine( "INNER JOIN wres.ProjectSource PS" );
        scripter.addTab().addLine( "ON TSS.source_id = PS.source_id" );
        scripter.addLine( "INNER JOIN wres.Project P" );
        scripter.addTab().addLine( "ON PS.project_id = P.project_id" );

        return scripter.toString();
    }

    /**
     * Construct.
     * 
     * @param timeWindow the time window
     * @throws NullPointerException if the filter is null
     */

    private SingleValuedForecastRetriever( Builder builder )
    {
        super( builder );
    }


}
