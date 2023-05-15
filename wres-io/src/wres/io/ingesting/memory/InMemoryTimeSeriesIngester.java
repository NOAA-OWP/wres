package wres.io.ingesting.memory;

import java.util.List;
import java.util.Objects;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import wres.config.yaml.components.DatasetOrientation;
import wres.datamodel.time.TimeSeriesStore;
import wres.io.ingesting.IngestResult;
import wres.io.ingesting.TimeSeriesIngester;
import wres.io.ingesting.TimeSeriesTracker;
import wres.io.reading.DataSource;
import wres.io.reading.TimeSeriesTuple;

/**
 * Facade for ingesting time-series into an in-memory {@link TimeSeriesStore}.
 * @author James Brown
 */

public class InMemoryTimeSeriesIngester implements TimeSeriesIngester
{
    /** The time-series store builder to populate with time-series. */
    private final TimeSeriesStore.Builder timeSeriesStoreBuilder;

    /** A time-series tracker. */
    private final UnaryOperator<TimeSeriesTuple> timeSeriesTracker;

    /**
     * Create an instance.
     * @param timeSeriesStoreBuilder the time-series store builder to populate
     * @param timeSeriesTracker a time-series tracker
     * @return an instance
     * @throws NullPointerException if the input is null
     */

    public static InMemoryTimeSeriesIngester of( TimeSeriesStore.Builder timeSeriesStoreBuilder,
                                                 TimeSeriesTracker timeSeriesTracker )
    {
        return new InMemoryTimeSeriesIngester( timeSeriesStoreBuilder, timeSeriesTracker );
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
            for ( TimeSeriesTuple nextTuple : listedTuples )
            {
                // Track the time-series
                this.getTimeSeriesTracker()
                    .apply( nextTuple );

                DataSource innerSource = nextTuple.getDataSource();
                DatasetOrientation innerOrientation =
                        DatasetOrientation.valueOf( innerSource.getDatasetOrientation()
                                                               .name() );

                // Single-valued time-series?
                if ( nextTuple.hasSingleValuedTimeSeries() )
                {
                    this.timeSeriesStoreBuilder.addSingleValuedSeries( nextTuple.getSingleValuedTimeSeries(),
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
                    this.timeSeriesStoreBuilder.addEnsembleSeries( nextTuple.getEnsembleTimeSeries(),
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
            return List.of( new IngestResultInMemory( outerSource ) );
        }
    }

    /**
     * @return the time-series tracker
     */

    private UnaryOperator<TimeSeriesTuple> getTimeSeriesTracker()
    {
        return this.timeSeriesTracker;
    }

    /**
     * Hidden constructor.
     * @param timeSeriesTracker a time-series tracker
     * @param timeSeriesStoreBuilder the time-series store builder
     */
    private InMemoryTimeSeriesIngester( TimeSeriesStore.Builder timeSeriesStoreBuilder,
                                        TimeSeriesTracker timeSeriesTracker )
    {
        Objects.requireNonNull( timeSeriesStoreBuilder );
        this.timeSeriesStoreBuilder = timeSeriesStoreBuilder;

        // Set the tracker or an identity operator
        if( Objects.isNull( timeSeriesTracker ) )
        {
            this.timeSeriesTracker = in -> in;
        }
        else
        {
            this.timeSeriesTracker = timeSeriesTracker;
        }
    }
}
