package wres.io.ingesting.memory;

import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.components.DataType;
import wres.config.components.DatasetOrientation;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesSlicer;
import wres.datamodel.time.TimeSeriesStore;
import wres.datamodel.types.Ensemble;
import wres.io.ingesting.IngestResult;
import wres.io.ingesting.TimeSeriesIngester;
import wres.reading.DataSource;
import wres.reading.TimeSeriesTuple;

/**
 * Facade for ingesting time-series into an in-memory {@link TimeSeriesStore}.
 * @author James Brown
 */

public class InMemoryTimeSeriesIngester implements TimeSeriesIngester
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( InMemoryTimeSeriesIngester.class );

    /** The time-series store builder to populate with time-series. */
    private final TimeSeriesStore.Builder timeSeriesStoreBuilder;

    /**
     * Create an instance.
     * @param timeSeriesStoreBuilder the time-series store builder to populate
     * @return an instance
     * @throws NullPointerException if the input is null
     */

    public static InMemoryTimeSeriesIngester of( TimeSeriesStore.Builder timeSeriesStoreBuilder )
    {
        return new InMemoryTimeSeriesIngester( timeSeriesStoreBuilder );
    }

    @Override
    public List<IngestResult> ingest( Stream<TimeSeriesTuple> timeSeriesTuple, DataSource outerSource )
    {
        Objects.requireNonNull( timeSeriesTuple );
        Objects.requireNonNull( outerSource );

        // Close the stream on completion
        try ( timeSeriesTuple )
        {
            List<TimeSeriesTuple> listedTuples = timeSeriesTuple.toList();
            DataType dataType = null;
            for ( TimeSeriesTuple nextTuple : listedTuples )
            {
                // If needed, adjust the tuple to handle the special case where a time-series was read as single-valued,
                // but declared to be treated as an ensemble. It is convenient to perform that adaptation on "ingest"
                // because a retriever will be constructed for the expected type, i.e., ensemble time-series
                nextTuple = this.checkAndAdaptTupleForDataType( nextTuple );

                DataSource innerSource = nextTuple.getDataSource();
                DatasetOrientation innerOrientation =
                        DatasetOrientation.valueOf( innerSource.getDatasetOrientation()
                                                               .name() );

                // Single-valued time-series?
                if ( nextTuple.hasSingleValuedTimeSeries() )
                {
                    TimeSeries<Double> timeSeries = nextTuple.getSingleValuedTimeSeries();

                    dataType = TimeSeriesSlicer.getDataType( timeSeries );
                    this.timeSeriesStoreBuilder.addSingleValuedSeries( timeSeries,
                                                                       innerOrientation );

                    // Add in all other contexts too
                    for ( DatasetOrientation lrb : innerSource.getLinks() )
                    {
                        DatasetOrientation linkOrientation = DatasetOrientation.valueOf( lrb.name() );
                        this.timeSeriesStoreBuilder.addSingleValuedSeries( nextTuple.getSingleValuedTimeSeries(),
                                                                           linkOrientation );
                    }
                }

                // Ensemble time-series?
                if ( nextTuple.hasEnsembleTimeSeries() )
                {
                    TimeSeries<Ensemble> timeSeries = nextTuple.getEnsembleTimeSeries();
                    dataType = TimeSeriesSlicer.getDataType( timeSeries );
                    this.timeSeriesStoreBuilder.addEnsembleSeries( timeSeries,
                                                                   innerOrientation );

                    // Add in all other contexts too
                    for ( DatasetOrientation lrb : innerSource.getLinks() )
                    {
                        DatasetOrientation linkOrientation = DatasetOrientation.valueOf( lrb.name() );
                        this.timeSeriesStoreBuilder.addEnsembleSeries( nextTuple.getEnsembleTimeSeries(),
                                                                       linkOrientation );
                    }
                }
            }

            // Arbitrary surrogate key, since this is an in-memory ingest
            return List.of( new IngestResultInMemory( outerSource, dataType ) );
        }
    }

    /**
     * Handles the special case where a single-valued time-series should be treated as an ensemble time-series.
     *
     * @param tuple the tuple to check and adapt
     * @return the adapted tuple
     */

    private TimeSeriesTuple checkAndAdaptTupleForDataType( TimeSeriesTuple tuple )
    {
        // Special case where time-series were read as single-valued, but declared as ensemble, and should
        // be treated as declared, i.e., a one-member ensemble. See #130267.
        if ( tuple.getDataSource()
                  .getContext()
                  .type() == DataType.ENSEMBLE_FORECASTS
             && tuple.hasSingleValuedTimeSeries() )
        {
            LOGGER.debug( "Discovered a time-series tuple with single-valued time-series declared to be treated as "
                          + "ensemble time-series. The single-valued time-series will be converted to one-member "
                          + "ensembles." );

            TimeSeries<Double> singleValued = tuple.getSingleValuedTimeSeries();

            TimeSeries<Ensemble> ensemble = TimeSeriesSlicer.transform( singleValued, Ensemble::of, m -> m );

            return TimeSeriesTuple.ofEnsemble( ensemble, tuple.getDataSource() );
        }
        else
        {
            return tuple;
        }
    }

    /**
     * Hidden constructor.
     * @param timeSeriesStoreBuilder the time-series store builder
     */
    private InMemoryTimeSeriesIngester( TimeSeriesStore.Builder timeSeriesStoreBuilder )
    {
        Objects.requireNonNull( timeSeriesStoreBuilder );
        this.timeSeriesStoreBuilder = timeSeriesStoreBuilder;
    }
}
