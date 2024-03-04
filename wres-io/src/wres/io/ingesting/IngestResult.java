package wres.io.ingesting;

import java.util.List;

import wres.config.yaml.components.DatasetOrientation;
import wres.reading.DataSource;

/**
 * High-level result for a single fragment of ingest, namely a time-series.
 */

public interface IngestResult
{
    /**
     * @return The surrogate key (the auto-incremented integer) for the source.
     */
    long getSurrogateKey();

    /**
     * @return The DataSource to use when retrying ingest.
     * @throws UnsupportedOperationException when requiresRetry() is false.
     */
    DataSource getDataSource();

    /**
     * @return Whether Left or Right or Baseline when retrying ingest.
     * @throws UnsupportedOperationException when requiresRetry() is false.
     */
    DatasetOrientation getDatasetOrientation();

    /**
     * Whether this data requires another try at ingest.
     * @return true when ingest needs to be retried.
     */

    boolean requiresRetry();

    /**
     * How many times the timeseries is included in the left dataset.
     * @return Count of left dataset associations.
     */
    short getLeftCount();

    /**
     * How many times the timeseries is included in the right dataset.
     * @return Count of right dataset associations.
     */
    short getRightCount();

    /**
     * How many times the timeseries is included in the baseline dataset.
     * @return Count of baseline dataset associations.
     */
    short getBaselineCount();

    /**
     * Creates a singleton list from the inputs.
     * @param dataSource the data source information
     * @param surrogateKey The surrogate key of the source data.
     * @param foundAlready true if found in the backing store, false otherwise
     * @param requiresRetry true if this requires retry, false otherwise
     * @return a list with a single IngestResult in it
     */

    static List<IngestResult> singleItemListFrom( DataSource dataSource,
                                                  long surrogateKey,
                                                  boolean foundAlready,
                                                  boolean requiresRetry )
    {
        IngestResult ingestResult = IngestResult.from( dataSource,
                                                       surrogateKey,
                                                       foundAlready,
                                                       requiresRetry );
        return List.of( ingestResult );
    }

    /**
     * Get an IngestResult using the configuration elements, for convenience
     * @param dataSource the data source information
     * @param surrogateKey The surrogate key of the source data.
     * @param foundAlready true if found in the backing store, false otherwise
     * @param requiresRetry true if this requires retry, false otherwise
     * @return the IngestResult
     */
    private static IngestResult from( DataSource dataSource,
                                      long surrogateKey,
                                      boolean foundAlready,
                                      boolean requiresRetry )
    {
        if ( requiresRetry && !foundAlready )
        {
            throw new IllegalArgumentException( "If requiring retry, it must have been found already!" );
        }

        if ( requiresRetry )
        {
            return new IngestResultNeedingRetry( dataSource,
                                                 surrogateKey );
        }
        else
        {
            return new IngestResultCompact( dataSource,
                                            surrogateKey,
                                            foundAlready );
        }
    }

}
