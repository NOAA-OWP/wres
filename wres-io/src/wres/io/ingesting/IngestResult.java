package wres.io.ingesting;

import java.util.List;
import java.util.Set;

import wres.config.components.DataType;
import wres.config.components.DatasetOrientation;
import wres.reading.DataSource;

/**
 * High-level result for a single fragment of ingest, which corresponds to a single time-series.
 */

public interface IngestResult
{
    /**
     * @return The surrogate key (the auto-incremented integer) for the source.
     */
    long getSurrogateKey();

    /**
     * @return The DataSource to use when retrying ingest.
     */
    DataSource getDataSource();

    /**
     * @return the orientations in which this time-series appears.
     */
    Set<DatasetOrientation> getDatasetOrientations();

    /**
     * @return The type of data in the time-series or null when undefined
     */
    DataType getDataType();

    /**
     * Whether this data requires another try at ingest.
     * @return true when ingest needs to be retried.
     */

    boolean requiresRetry();

    /**
     * @return whether valid time-series data was ingested, i.e., some non-missing values.
     */

    boolean hasNonMissingData();

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
     * How many times the timeseries is included in the covariate dataset.
     * @return Count of covariate dataset associations.
     */
    short getCovariateCount();

    /**
     * Creates a singleton list from the inputs.
     * @param dataSource the data source information
     * @param dataType the optional data type
     * @param surrogateKey The surrogate key of the source data.
     * @param foundAlready true if found in the backing store, false otherwise
     * @param requiresRetry true if this requires retry, false otherwise
     * @param hasNonMissingData true if non-missing data was found, false otherwise
     * @return a list with a single IngestResult in it
     */

    static List<IngestResult> singleItemListFrom( DataSource dataSource,
                                                  DataType dataType,
                                                  long surrogateKey,
                                                  boolean foundAlready,
                                                  boolean requiresRetry,
                                                  boolean hasNonMissingData )
    {
        IngestResult ingestResult = IngestResult.from( dataSource,
                                                       dataType,
                                                       surrogateKey,
                                                       foundAlready,
                                                       requiresRetry,
                                                       hasNonMissingData );
        return List.of( ingestResult );
    }

    /**
     * Get an IngestResult using the configuration elements, for convenience
     * @param dataSource the data source information
     * @param dataType the optional data type
     * @param surrogateKey The surrogate key of the source data.
     * @param foundAlready true if found in the backing store, false otherwise
     * @param requiresRetry true if this requires retry, false otherwise
     * @param hasNonMissingData true if non-missing data was found, false otherwise
     * @return the IngestResult
     */
    private static IngestResult from( DataSource dataSource,
                                      DataType dataType,
                                      long surrogateKey,
                                      boolean foundAlready,
                                      boolean requiresRetry,
                                      boolean hasNonMissingData )
    {
        if ( requiresRetry && !foundAlready )
        {
            throw new IllegalArgumentException( "If requiring retry, it must have been found already!" );
        }

        if ( requiresRetry )
        {
            return new IngestResultNeedingRetry( dataSource,
                                                 dataType,
                                                 surrogateKey,
                                                 hasNonMissingData );
        }
        else
        {
            return new IngestResultCompact( dataSource,
                                            dataType,
                                            surrogateKey,
                                            foundAlready,
                                            hasNonMissingData );
        }
    }

}
