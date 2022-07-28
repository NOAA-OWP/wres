package wres.io.ingesting;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import wres.config.generated.LeftOrRightOrBaseline;
import wres.datamodel.Ensemble;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesStore;
import wres.io.reading.DataSource;

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
    public List<IngestResult> ingestSingleValuedTimeSeries( Stream<TimeSeries<Double>> timeSeries,
                                                            DataSource dataSource )
    {
        Objects.requireNonNull( timeSeries );
        Objects.requireNonNull( dataSource );

        List<TimeSeries<Double>> listedSeries = timeSeries.collect( Collectors.toList() );
        for ( TimeSeries<Double> nextSeries : listedSeries )
        {
            this.timeSeriesStoreBuilder.addSingleValuedSeries( nextSeries, dataSource.getLeftOrRightOrBaseline() );

            // Add in all other contexts too
            for ( LeftOrRightOrBaseline lrb : dataSource.getLinks() )
            {
                this.timeSeriesStoreBuilder.addSingleValuedSeries( nextSeries, lrb );
            }
        }

        // Arbitrary surrogate key, since this is an in-memory ingest
        return List.of( new IngestResultInMemory( dataSource ) );
    }

    @Override
    public List<IngestResult> ingestEnsembleTimeSeries( Stream<TimeSeries<Ensemble>> timeSeries,
                                                        DataSource dataSource )
    {
        Objects.requireNonNull( timeSeries );
        Objects.requireNonNull( dataSource );

        List<TimeSeries<Ensemble>> listedSeries = timeSeries.collect( Collectors.toList() );
        for ( TimeSeries<Ensemble> nextSeries : listedSeries )
        {
            this.timeSeriesStoreBuilder.addEnsembleSeries( nextSeries, dataSource.getLeftOrRightOrBaseline() );

            // Add in all other contexts too
            for ( LeftOrRightOrBaseline lrb : dataSource.getLinks() )
            {
                this.timeSeriesStoreBuilder.addEnsembleSeries( nextSeries, lrb );
            }
        }
        // Arbitrary surrogate key, since this is an in-memory ingest
        return List.of( new IngestResultInMemory( dataSource ) );
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
