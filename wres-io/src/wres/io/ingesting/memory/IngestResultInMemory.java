package wres.io.ingesting.memory;

import java.util.Objects;
import java.util.Set;

import org.apache.commons.lang3.builder.ToStringBuilder;

import wres.config.components.DataType;
import wres.config.components.DatasetOrientation;
import wres.io.ingesting.IngestResult;
import wres.io.ingesting.IngestResultNeedingRetry;
import wres.reading.DataSource;

/**
 * An {@link IngestResult} that stores the {@link DataSource} and is connected to an in-memory ingest implementation.
 */

public class IngestResultInMemory implements IngestResult
{
    /** Composes a similar implementation of an ingest fragment for convenience, overriding only a subset of methods. */
    private final IngestResultNeedingRetry innerResult;

    /**
     * Creates an instance.
     *
     * @param dataSource the data source
     * @param dataType the optional data type
     * @param hasNonMissingData whether the data source has valid data present
     */
    public IngestResultInMemory( DataSource dataSource, DataType dataType, boolean hasNonMissingData )
    {
        Objects.requireNonNull( dataSource, "Ingester requires datasource information." );

        this.innerResult = new IngestResultNeedingRetry( dataSource, dataType, 1, hasNonMissingData );
    }

    @Override
    public DataSource getDataSource()
    {
        return this.innerResult.getDataSource();
    }

    @Override
    public DataType getDataType()
    {
        return this.innerResult.getDataType();
    }

    @Override
    public Set<DatasetOrientation> getDatasetOrientations()
    {
        return this.innerResult.getDatasetOrientations();
    }

    @Override
    public long getSurrogateKey()
    {
        return -1;
    }

    @Override
    public boolean requiresRetry()
    {
        return false;
    }

    @Override
    public boolean hasNonMissingData()
    {
        return this.innerResult.hasNonMissingData();
    }

    @Override
    public short getLeftCount()
    {
        return this.innerResult.getLeftCount();
    }

    @Override
    public short getRightCount()
    {
        return this.innerResult.getRightCount();
    }

    @Override
    public short getBaselineCount()
    {
        return this.innerResult.getBaselineCount();
    }

    @Override
    public short getCovariateCount()
    {
        return this.innerResult.getCovariateCount();
    }

    @Override
    public String toString()
    {
        return new ToStringBuilder( this ).append( "orientations", this.getDatasetOrientations() )
                                          .append( "dataSource", this.getDataSource() )
                                          .toString();
    }
}
