package wres.io.ingesting.memory;

import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import wres.config.yaml.components.DataType;
import wres.config.yaml.components.DatasetOrientation;
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
     * Hidden constructor.
     * @param timeSeriesStoreBuilder the time-series store builder
     */
    private InMemoryTimeSeriesIngester( TimeSeriesStore.Builder timeSeriesStoreBuilder )
    {
        Objects.requireNonNull( timeSeriesStoreBuilder );
        this.timeSeriesStoreBuilder = timeSeriesStoreBuilder;
    }
}
