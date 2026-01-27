package wres.io.retrieving.database;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import wres.datamodel.MissingValues;
import wres.datamodel.time.DoubleEvent;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.DataProvider;
import wres.io.database.DataScripter;
import wres.io.database.Database;
import wres.io.database.ScriptBuilder;
import wres.io.retrieving.DataAccessException;
import wres.system.DatabaseSettingsHelper;

/**
 * Retrieves {@link TimeSeries} of single-valued forecasts from the WRES database.
 *
 * @author James Brown
 */

class SingleValuedForecastRetriever extends TimeSeriesRetriever<Double>
{
    /**
     * <code>ORDER BY</code> clause, which is repeated several times.
     */

    private static final String ORDER_BY_METADATA = "ORDER BY metadata.series_id;";

    /**
     * <code>GROUP BY</code> clause, which is repeated several times.
     */

    private static final String GROUP_BY_FEATURE_ID_SERIES_ID_TSV_LEAD_TSV_SERIES_VALUE =
            "GROUP BY metadata.feature_id, metadata.series_id, valid_time";

    /**
     * Template script for the {@link #getAllIdentifiers()}.
     */

    private static final String GET_ALL_IDENTIFIERS_SCRIPT =
            SingleValuedForecastRetriever.getScriptForGetAllIdentifiers();

    /** Re-used string. */
    private static final String S_MEASUREMENTUNIT_ID = "S.measurementunit_id,";
    /** Re-used string. */
    private static final String SELECT = "SELECT ";
    /** Re-used string. */
    private static final String FROM_WRES_SOURCE_S = "FROM wres.Source S";
    /** Re-used string. */
    private static final String INNER_JOIN_WRES_PROJECT_SOURCE_PS = "INNER JOIN wres.ProjectSource PS";

    /**
     * Builder.
     */

    static class Builder extends TimeSeriesRetriever.Builder<Double>
    {

        @Override
        SingleValuedForecastRetriever build()
        {
            return new SingleValuedForecastRetriever( this );
        }

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

        Database database = super.getDatabase();
        String start = this.getStartOfScriptForGetAllTimeSeries();
        DataScripter dataScripter = new DataScripter( database, start );

        // Add basic constraints at zero tabs
        this.addProjectFeatureVariableAndMemberConstraints( dataScripter, 1 );

        dataScripter.addTab().addLine( "GROUP BY S.source_id," );
        dataScripter.addTab( 2 ).addLine( S_MEASUREMENTUNIT_ID );
        dataScripter.addTab( 2 ).addLine( "TimeScale.duration_ms," );
        dataScripter.addTab( 2 ).addLine( "TimeScale.function_name," );
        dataScripter.addTab( 2 ).addLine( "TSRT.reference_time_type" );
        dataScripter.addLine( ") AS metadata " );
        dataScripter.addLine( "INNER JOIN wres.TimeSeries TS" );
        dataScripter.addTab().addLine( "ON TS.source_id = metadata.series_id" );
        dataScripter.addLine( "INNER JOIN wres.TimeSeriesValue TSV" );
        dataScripter.addTab().addLine( "ON TSV.timeseries_id = TS.timeseries_id" );

        // Add time window constraint at zero tabs
        this.addTimeWindowClause( dataScripter );

        // Add season constraint at one tab
        this.addSeasonClause( dataScripter, 1 );

        // Add ORDER BY clause
        dataScripter.addLine( ORDER_BY_METADATA );

        // Log
        super.logScript( dataScripter );

        // Retrieve the time-series
        return this.getTimeSeriesFromScript( dataScripter, this.getDataSupplier() );
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
        // Retrieve the time-series
        return this.get( LongStream.of( identifier ) )
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

        Database database = super.getDatabase();
        DataScripter dataScripter = new DataScripter( database, GET_ALL_IDENTIFIERS_SCRIPT );

        // Add basic constraints
        this.addProjectFeatureVariableAndMemberConstraints( dataScripter, 0 );

        // Log
        super.logScript( dataScripter );

        try ( Connection connection = this.getDatabase()
                                          .getConnection();
              DataProvider provider = dataScripter.buffer( connection ) )
        {
            LongStream.Builder b = LongStream.builder();

            while ( provider.next() )
            {
                b.add( provider.getLong( "series_id" ) );
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

        Database database = super.getDatabase();
        String start = this.getStartOfScriptForGetAllTimeSeries();
        DataScripter dataScripter = new DataScripter( database, start );

        // Add basic constraints at zero tabs
        this.addProjectFeatureVariableAndMemberConstraints( dataScripter, 0 );

        // Time window constraint at zero tabs
        this.addTimeWindowClause( dataScripter );

        // Add season constraint at one tab
        this.addSeasonClause( dataScripter, 1 );

        // Add the time-series identifiers
        dataScripter.addTab( 1 ).addLine( "AND S.source_id = ANY( ? )" );
        dataScripter.addArgument( identifiers.boxed().toArray() );

        dataScripter.addTab().addLine( "GROUP BY S.source_id," );
        dataScripter.addTab( 2 ).addLine( S_MEASUREMENTUNIT_ID );
        dataScripter.addTab( 2 ).addLine( "TimeScale.duration_ms," );
        dataScripter.addTab( 2 ).addLine( "TimeScale.function_name" );
        dataScripter.addLine( ") AS metadata " );
        dataScripter.addLine( "INNER JOIN wres.TimeSeries TS" );
        dataScripter.addTab().addLine( "ON TS.source_id = metadata.series_id" );
        dataScripter.addLine( "INNER JOIN wres.TimeSeriesValue TSV" );
        dataScripter.addTab().addLine( "ON TSV.timeseries_id = TS.timeseries_id" );

        // Add GROUP BY clause
        dataScripter.addLine( GROUP_BY_FEATURE_ID_SERIES_ID_TSV_LEAD_TSV_SERIES_VALUE ); // #56214-272

        // Add ORDER BY clause
        dataScripter.addLine( ORDER_BY_METADATA );

        // Log
        super.logScript( dataScripter );

        // Retrieve the time-series
        return this.getTimeSeriesFromScript( dataScripter, this.getDataSupplier() );
    }

    @Override
    boolean isForecast()
    {
        return true;
    }

    /**
     * Returns a function that obtains the measured value.
     *
     * @return a function to obtain the measured value
     */

    private Function<DataProvider, Event<Double>> getDataSupplier()
    {
        return provider -> {
            // Raw value
            double unmapped = provider.getDouble( "trace_value" );
            Instant validTime = provider.getInstant( "valid_time" );

            if ( MissingValues.isMissingValue( unmapped ) )
            {
                return DoubleEvent.of( validTime, MissingValues.DOUBLE );
            }

            return DoubleEvent.of( validTime, unmapped );
        };
    }

    /**
     * Returns the start of a script to acquire a time-series from the WRES database for all time-series.
     *
     * @return the start of a script for the time-series
     */

    private String getStartOfScriptForGetAllTimeSeries()
    {
        ScriptBuilder scripter = new ScriptBuilder();

        boolean aliases = !this.getVariable()
                               .aliases()
                               .isEmpty();

        scripter.addLine( SELECT );
        scripter.addTab().addLine( "metadata.series_id AS series_id," );
        scripter.addTab().addLine( "metadata.reference_time + INTERVAL '1' "
                                   + DatabaseSettingsHelper.getLeadDurationString()
                                   + " * TSV.lead AS valid_time," );
        scripter.addTab().addLine( "metadata.reference_time," );
        scripter.addTab().addLine( "metadata.reference_time_type," );
        scripter.addTab().addLine( "TSV.series_value AS trace_value," );
        scripter.addTab().addLine( "metadata.measurementunit_id," );
        scripter.addTab().addLine( "metadata.scale_period," );
        scripter.addTab().addLine( "metadata.scale_function," );
        scripter.addTab().addLine( "metadata.feature_id," );

        if ( aliases )
        {
            scripter.addTab().addLine( "metadata.variable_name," );
        }

        // See #56214-272. Add the count to allow re-duplication of duplicate series
        scripter.addTab().addLine( "metadata.occurrences" );
        scripter.addLine( "FROM" );
        scripter.addLine( "(" );
        scripter.addTab().addLine( SELECT );
        scripter.addTab( 2 ).addLine( "S.source_id AS series_id," );
        scripter.addTab( 2 ).addLine( "MAX( reference_time ) AS reference_time," );
        scripter.addTab( 2 ).addLine( "reference_time_type," );
        scripter.addTab( 2 ).addLine( "S.feature_id," );
        scripter.addTab( 2 ).addLine( S_MEASUREMENTUNIT_ID );
        scripter.addTab( 2 ).addLine( "TimeScale.duration_ms AS scale_period," );
        scripter.addTab( 2 ).addLine( "TimeScale.function_name AS scale_function," );

        if ( aliases )
        {
            scripter.addTab( 2 ).addLine( "S.variable_name," );
        }

        scripter.addTab( 2 ).addLine( "COUNT(*) AS occurrences " );
        scripter.addTab().addLine( FROM_WRES_SOURCE_S );
        scripter.addTab().addLine( INNER_JOIN_WRES_PROJECT_SOURCE_PS );
        scripter.addTab( 2 ).addLine( "ON PS.source_id = S.source_id" );
        scripter.addTab().addLine( "INNER JOIN wres.TimeSeriesReferenceTime TSRT" );
        scripter.addTab( 2 ).addLine( "ON TSRT.source_id = S.source_id" );
        // TODO: use the timescale_id and TimeScales cache instead
        scripter.addTab().addLine( "LEFT JOIN wres.TimeScale TimeScale" );
        scripter.addTab( 2 ).addLine( "ON TimeScale.timescale_id = S.timescale_id" );

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

        scripter.addLine( "SELECT S.source_id AS series_id" );
        scripter.addLine( FROM_WRES_SOURCE_S );
        scripter.addLine( INNER_JOIN_WRES_PROJECT_SOURCE_PS );
        scripter.addTab().addLine( "ON S.source_id = PS.source_id" );

        return scripter.toString();
    }

    /**
     * Construct.
     *
     * @throws NullPointerException if any required input is null
     */

    private SingleValuedForecastRetriever( Builder builder )
    {
        super( builder, "metadata.reference_time", "TSV.lead" );
    }


}
