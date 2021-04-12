package wres.io.retrieval;

import java.util.Optional;

import java.util.function.DoubleUnaryOperator;
import java.util.function.Function;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import wres.datamodel.MissingValues;
import wres.datamodel.time.TimeSeries;
import wres.io.utilities.DataProvider;
import wres.io.utilities.DataScripter;
import wres.io.utilities.Database;
import wres.io.utilities.ScriptBuilder;

/**
 * Retrieves an observation {@link TimeSeries} from the WRES database.
 * 
 * @author james.brown@hydrosolved.com
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

    static class Builder extends TimeSeriesRetrieverBuilder<Double>
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
        
        this.addTimeWindowClause( dataScripter, 0 );
        this.addSeasonClause( dataScripter, 1 );
        this.addProjectFeatureVariableAndMemberConstraints( dataScripter, 0 );

        dataScripter.addLine( "GROUP BY TS.timeseries_id, TSV.lead, TSV.series_value" );

        // Add ORDER BY clause
        dataScripter.addLine( "ORDER BY series_id, valid_time;" );

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
     * Returns a function that obtains the measured value in the desired units from a {@link DataProvider}.
     * 
     * @return a function to obtain the measured value in the correct units
     */

    private Function<DataProvider, Double> getDataSupplier()
    {
        return provider -> {
            // Raw value
            double unmapped = provider.getDouble( "observation" );

            if ( !Double.isFinite( unmapped ) )
            {
                return MissingValues.DOUBLE;
            }

            // Existing units
            long measurementUnitId = provider.getLong( "measurementunit_id" );

            // Units mapper
            DoubleUnaryOperator mapper = this.getMeasurementUnitMapper()
                                             .getUnitMapper( measurementUnitId );

            // Convert
            return mapper.applyAsDouble( unmapped );
        };
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
        scripter.addTab().addLine( "TS.initialization_date + INTERVAL '1' MINUTE * TSV.lead AS valid_time," );
        scripter.addTab().addLine( "TSV.series_value AS observation," );
        scripter.addTab().addLine( "TS.measurementunit_id," );
        scripter.addTab().addLine( "TS.scale_period," );
        scripter.addTab().addLine( "TS.scale_function," );
        // See #56214-272. Add the count to allow re-duplication of duplicate series
        scripter.addTab().addLine( "COUNT(*) AS occurrences" );
        scripter.addLine( "FROM wres.TimeSeries TS" );
        scripter.addTab().addLine( "INNER JOIN wres.TimeSeriesValue TSV" );
        scripter.addTab( 2 ).addLine( "ON TSV.timeseries_id = TS.timeseries_id" );
        scripter.addTab().addLine( "INNER JOIN wres.ProjectSource PS" );
        scripter.addTab( 2 ).addLine( "ON PS.source_id = TS.source_id" );

        return scripter.toString();
    }


    /**
     * Construct.
     *
     * @throws NullPointerException if any required input is null
     */

    private ObservationRetriever( Builder builder )
    {
        super( builder, "TS.initialization_date + INTERVAL '1' MINUTE * TSV.lead", null );
    }


}
