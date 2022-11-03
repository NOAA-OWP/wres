package wres.io.retrieval.database;

import java.util.Optional;

import java.util.function.Function;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import wres.datamodel.MissingValues;
import wres.datamodel.time.TimeSeries;
import wres.io.data.DataProvider;
import wres.io.database.DataScripter;
import wres.io.database.Database;
import wres.io.database.ScriptBuilder;
import wres.io.retrieval.DataAccessException;

/**
 * Retrieves an observation {@link TimeSeries} from the WRES database.
 * 
 * @author James Brown
 */

class ObservationRetriever extends TimeSeriesRetriever<Double>
{

    /**
     * Error message when attempting to retrieve by identifier. See #68334 and #56214-56.
     */

    private static final String NO_IDENTIFIER_ERROR = "Retrieval of observed time-series by identifier is not "
                                                      + "currently possible.";

    /**
     * Builder.
     */

    static class Builder extends TimeSeriesRetriever.Builder<Double>
    {

        @Override
        ObservationRetriever build()
        {
            return new ObservationRetriever( this );
        }

    }

    /**
     * Reads a time-series by <code>wres.TimeSeries.timeseries_id</code>.
     * 
     * TODO: implement this method when there is an identifier for an observed time-series. See #68334 and #56214-56.
     * 
     * @param identifier the <code>wres.TimeSeries.timeseries_id</code>
     * @return a possible time-series for the given identifier
     */

    @Override
    public Optional<TimeSeries<Double>> get( long identifier )
    {
        throw new UnsupportedOperationException( NO_IDENTIFIER_ERROR );
    }

    /**
     * Returns all of the <code>wres.TimeSeries.timeseries_id</code> associated with this instance.
     * 
     * TODO: implement this method when there is an identifier for an observed time-series. See #68334 and #56214-56.
     * 
     * @return a stream of<code>wres.TimeSeries.timeseries_id</code>
     */

    @Override
    public LongStream getAllIdentifiers()
    {
        throw new UnsupportedOperationException( NO_IDENTIFIER_ERROR );
    }

    /**
     * Overrides the default implementation to get all specified time-series in one pull, rather than one pull for 
     * each series.
     * 
     * TODO: implement this method when there is an identifier for an observed time-series. See #68334 and #56214-56.
     * 
     * @param identifiers the stream of identifiers
     * @return a stream over the identified objects
     * @throws NullPointerException if the input is null
     */
    @Override
    public Stream<TimeSeries<Double>> get( LongStream identifiers )
    {
        throw new UnsupportedOperationException( NO_IDENTIFIER_ERROR );
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
        String timeSeriesTableScript = getStartOfScriptForGetAllTimeSeries();

        Database database = super.getDatabase();
        DataScripter dataScripter = new DataScripter( database, timeSeriesTableScript );

        this.addProjectFeatureVariableAndMemberConstraints( dataScripter, 1 );

        dataScripter.addTab().addLine( "GROUP BY S.source_id," );
        dataScripter.addTab( 2 ).addLine( "S.measurementunit_id," );
        dataScripter.addTab( 2 ).addLine( "TimeScale.duration_ms," );
        dataScripter.addTab( 2 ).addLine( "TimeScale.function_name" );
        dataScripter.addLine( ") AS metadata " );
        dataScripter.addLine( "INNER JOIN wres.TimeSeries TS" );
        dataScripter.addTab().addLine( "ON TS.source_id = metadata.series_id" );
        dataScripter.addLine( "INNER JOIN wres.TimeSeriesValue TSV" );
        dataScripter.addTab().addLine( "ON TSV.timeseries_id = TS.timeseries_id" );
        this.addTimeWindowClause( dataScripter, 0 );
        this.addSeasonClause( dataScripter, 1 );

        // Add ORDER BY clause
        dataScripter.addLine( "ORDER BY metadata.series_id, valid_time;" );

        // Log the script
        super.logScript( dataScripter );

        // Retrieve the time-series
        return this.getTimeSeriesFromScript( dataScripter, this.getDataSupplier() );
    }

    @Override
    boolean isForecast()
    {
        return false;
    }

    /**
     * Returns a function that obtains the measured value in the existing units from a {@link DataProvider}.
     * 
     * @return a function to obtain the measured value
     */

    private Function<DataProvider, Double> getDataSupplier()
    {
        return provider -> {
            // Raw value
            double unmapped = provider.getDouble( "trace_value" );

            if ( MissingValues.isMissingValue( unmapped ) )
            {
                return MissingValues.DOUBLE;
            }

            return unmapped;
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

        scripter.addLine( "SELECT " );
        scripter.addTab().addLine( "metadata.series_id AS series_id," );
        scripter.addTab().addLine( "metadata.reference_time + INTERVAL '1' MINUTE * TSV.lead AS valid_time," );

        // Some code will be confused if a reference_time shows up in "obs" data
        //scripter.addTab().addLine( "metadata.reference_time," );
        scripter.addTab().addLine( "TSV.series_value AS trace_value," );
        scripter.addTab().addLine( "metadata.measurementunit_id," );
        scripter.addTab().addLine( "metadata.scale_period," );
        scripter.addTab().addLine( "metadata.scale_function," );
        scripter.addTab().addLine( "metadata.feature_id," );
        // See #56214-272. Add the count to allow re-duplication of duplicate series
        scripter.addTab().addLine( "metadata.occurrences" );
        scripter.addLine( "FROM" );
        scripter.addLine( "(" );
        scripter.addTab().addLine( "SELECT " );
        scripter.addTab( 2 ).addLine( "S.source_id AS series_id," );
        scripter.addTab( 2 ).addLine( "MAX( reference_time ) AS reference_time," );
        scripter.addTab( 2 ).addLine( "S.feature_id," );
        scripter.addTab( 2 ).addLine( "S.measurementunit_id," );
        scripter.addTab( 2 ).addLine( "TimeScale.duration_ms AS scale_period," );
        scripter.addTab( 2 ).addLine( "TimeScale.function_name AS scale_function," );

        scripter.addTab( 2 ).addLine( "COUNT(*) AS occurrences " );
        scripter.addTab().addLine( "FROM wres.Source S" );
        scripter.addTab().addLine( "INNER JOIN wres.ProjectSource PS" );
        scripter.addTab( 2 ).addLine( "ON PS.source_id = S.source_id" );
        scripter.addTab().addLine( "INNER JOIN wres.TimeSeriesReferenceTime TSRT" );
        scripter.addTab( 2 ).addLine( "ON TSRT.source_id = S.source_id" );
        // TODO: use the timescale_id and TimeScales cache instead
        scripter.addTab().addLine( "LEFT JOIN wres.TimeScale TimeScale" );
        scripter.addTab( 2 ).addLine( "ON TimeScale.timescale_id = S.timescale_id" );

        return scripter.toString();
    }


    /**
     * Construct.
     *
     * @throws NullPointerException if any required input is null
     */

    private ObservationRetriever( Builder builder )
    {
        super( builder, "metadata.reference_time + INTERVAL '1' MINUTE * TSV.lead", null );
    }


}
