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

import wres.datamodel.MissingValues;
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
     * <code>ORDER BY</code> clause, which is repeated several times.
     */
    
    private static final String ORDER_BY_OCCURRENCES_SERIES_ID_TS_INITIALIZATION_DATE_VALID_TIME = 
            "ORDER BY occurrences, series_id, TS.initialization_date, valid_time;";

    /**
     * <code>GROUP BY</code> clause, which is repeated several times.
     */
    
    private static final String GROUP_BY_SERIES_ID_TSV_LEAD_TSV_SERIES_VALUE = 
            "GROUP BY series_id, TSV.lead, TSV.series_value";

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

    private static final String LOG_SCRIPT =
            "Built retriever {} for the retrieval of single-valued forecasts using script:{}{}";

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
     * Start of script for {@link #getAll()}.
     */

    private static final String GET_ALL_TIME_SERIES_SCRIPT =
            SingleValuedForecastRetriever.getStartOfScriptForGetAllTimeSeries();
    
    /**
     * Logger.
     */

    private static final Logger LOGGER = LoggerFactory.getLogger( SingleValuedForecastRetriever.class );

    /**
     * Builder.
     */

    static class Builder extends TimeSeriesRetrieverBuilder<Double>
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
        this.addTimeWindowClause( scripter, 0 );

        // Add GROUP BY and ORDER BY clauses
        scripter.addLine( GROUP_BY_SERIES_ID_TSV_LEAD_TSV_SERIES_VALUE ); // #56214-272
        scripter.addLine( ORDER_BY_OCCURRENCES_SERIES_ID_TS_INITIALIZATION_DATE_VALID_TIME );

        script = scripter.toString();

        if ( LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( LOG_SCRIPT,
                          this,
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
        this.addProjectVariableAndMemberConstraints( scripter, 0 );

        String script = scripter.toString();

        // Acquire the time-series identifiers
        DataScripter dataScripter = new DataScripter( script );

        if ( LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( LOG_SCRIPT,
                          this,
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
        this.addProjectVariableAndMemberConstraints( scripter, 0 );

        // Time window constraint
        this.addTimeWindowClause( scripter, 0 );
        
        // Add constraint on the timeseries_ids provided
        StringJoiner joiner = new StringJoiner( ",", "{", "}" );
        identifiers.forEach( next -> joiner.add( Long.toString( next ) ) );
        scripter.addTab( 1 ).addLine( "AND TS.timeseries_id = ANY( '", joiner.toString(), "' )::integer[]" );

        // Add GROUP BY and ORDER BY clauses
        scripter.addLine( GROUP_BY_SERIES_ID_TSV_LEAD_TSV_SERIES_VALUE ); // #56214-272
        scripter.addLine( ORDER_BY_OCCURRENCES_SERIES_ID_TS_INITIALIZATION_DATE_VALID_TIME );

        String script = scripter.toString();

        if ( LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( LOG_SCRIPT,
                          this,
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
    public Stream<TimeSeries<Double>> get()
    {
        this.validateForMultiSeriesRetrieval();

        ScriptBuilder scripter = new ScriptBuilder( GET_ALL_TIME_SERIES_SCRIPT );

        // Add basic constraints
        this.addProjectVariableAndMemberConstraints( scripter, 0 );
        
        // Add time window constraint
        this.addTimeWindowClause( scripter, 0 );
        
        // Add GROUP BY and ORDER BY clauses
        scripter.addLine( GROUP_BY_SERIES_ID_TSV_LEAD_TSV_SERIES_VALUE ); // #56214-272
        scripter.addLine( ORDER_BY_OCCURRENCES_SERIES_ID_TS_INITIALIZATION_DATE_VALID_TIME );

        String script = scripter.toString();

        if ( LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( LOG_SCRIPT,
                          this,
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
            
            if( !Double.isFinite( unmapped ) )
            {
                return MissingValues.DOUBLE;
            }
            
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
        scripter.addTab().addLine( "TS.initialization_date AS reference_time," );
        scripter.addTab().addLine( "TS.initialization_date + INTERVAL ''1 MINUTE'' * TSV.lead AS valid_time," );
        scripter.addTab().addLine( "TSV.series_value AS measurement," );
        scripter.addTab().addLine( "TS.measurementunit_id" ); 
        scripter.addTab().addLine( FROM_WRES_TIME_SERIES_TS );
        scripter.addLine( "INNER JOIN wres.TimeSeriesValue TSV" );
        scripter.addTab( 2 ).addLine( "ON TS.timeseries_id = TSV.timeseries_id" );        
        scripter.addTab().addLine( "WHERE TS.timeseries_id = ''{0}''" );

        return scripter.toString();
    }

    /**
     * Returns the start of a script to acquire a time-series from the WRES database for all time-series.
     * 
     * @return the start of a script for the time-series
     */

    private static String getStartOfScriptForGetAllTimeSeries()
    {
        ScriptBuilder scripter = new ScriptBuilder();

        scripter.addLine( "SELECT " );
        scripter.addTab().addLine( "TS.timeseries_id AS series_id," );
        scripter.addTab().addLine( "TS.initialization_date AS reference_time," );
        scripter.addTab().addLine( "TS.initialization_date + INTERVAL '1' MINUTE * TSV.lead AS valid_time," );
        scripter.addTab().addLine( "TSV.series_value AS measurement," );
        scripter.addTab().addLine( "TS.measurementunit_id," );
        scripter.addTab().addLine( "TS.scale_period," );
        scripter.addTab().addLine( "TS.scale_function," );
        // See #56214-272. Add the count to allow re-duplication of duplicate series
        scripter.addTab().addLine( "COUNT(*) AS occurrences" );
        scripter.addLine( FROM_WRES_TIME_SERIES_TS );
        scripter.addTab().addLine( "INNER JOIN wres.TimeSeriesValue TSV" );
        scripter.addTab( 2 ).addLine( "ON TSV.timeseries_id = TS.timeseries_id" );
        scripter.addTab().addLine( "INNER JOIN wres.TimeSeriesSource TSS" );
        scripter.addTab( 2 ).addLine( "ON TSS.timeseries_id = TS.timeseries_id" );
        scripter.addTab( 3 ).addLine( "AND (TSS.lead IS NULL OR TSS.lead = TSV.lead)" );
        scripter.addTab().addLine( "INNER JOIN wres.ProjectSource PS" );
        scripter.addTab( 2 ).addLine( "ON PS.source_id = TSS.source_id" );

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
