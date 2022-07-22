package wres.io.ingesting;

import java.util.Objects;

import org.apache.commons.lang3.builder.ToStringBuilder;

import wres.config.generated.LeftOrRightOrBaseline;
import wres.io.reading.DataSource;

/**
 * An {@link IngestResult} that stores the {@link DataSource} and is connected to an in-memory ingest implementation.
 */

public class IngestResultInMemory implements IngestResult
{
    private final IngestResultNeedingRetry innerResult;

    public IngestResultInMemory( DataSource dataSource )
    {
        Objects.requireNonNull( dataSource, "Ingester must include datasource information." );

        this.innerResult = new IngestResultNeedingRetry( dataSource, 1 );
    }


    @Override
    public DataSource getDataSource()
    {
        return this.innerResult.getDataSource();
    }

    @Override
    public LeftOrRightOrBaseline getLeftOrRightOrBaseline()
    {
        return this.innerResult.getLeftOrRightOrBaseline();
    }

    @Override
    public long getSurrogateKey()
    {
        return -1;
    }

    @Override
    public boolean wasFoundAlready()
    {
        return true;
    }

    @Override
    public boolean requiresRetry()
    {
        return false;
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
    public String toString()
    {
        return new ToStringBuilder( this )
                                          .append( "leftOrRightOrBaseline", this.getLeftOrRightOrBaseline() )
                                          .append( "dataSource", this.getDataSource() )
                                          .toString();
    }
}
