package wres.io.ingesting;

import java.util.List;
import java.util.Objects;

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
    public List<IngestResult> ingestSingleValuedTimeSeries( TimeSeries<Double> timeSeries, DataSource dataSource )
    {
        Objects.requireNonNull( timeSeries );
        Objects.requireNonNull( dataSource );
        
        this.timeSeriesStoreBuilder.addSingleValuedSeries( timeSeries, dataSource.getLeftOrRightOrBaseline() );

        // Add in all other contexts too
        for ( LeftOrRightOrBaseline lrb : dataSource.getLinks() )
        {
            this.timeSeriesStoreBuilder.addSingleValuedSeries( timeSeries, lrb );
        }

        // Arbitrary surrogate key, since this is an in-memory ingest
        return List.of( new IngestResultInMemory( dataSource ) );
    }

    @Override
    public List<IngestResult> ingestEnsembleTimeSeries( TimeSeries<Ensemble> timeSeries, DataSource dataSource )
    {
        Objects.requireNonNull( timeSeries );
        Objects.requireNonNull( dataSource );

        this.timeSeriesStoreBuilder.addEnsembleSeries( timeSeries, dataSource.getLeftOrRightOrBaseline() );

        // Add in all other contexts too
        for ( LeftOrRightOrBaseline lrb : dataSource.getLinks() )
        {
            this.timeSeriesStoreBuilder.addEnsembleSeries( timeSeries, lrb );
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
