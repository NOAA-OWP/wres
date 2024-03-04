package wres.io.ingesting;

import java.util.Objects;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.Short.MAX_VALUE;

import wres.config.yaml.components.DatasetOrientation;
import wres.reading.DataSource;

/**
 * An IngestResult exclusively for data needing retry of ingest.
 */
public class IngestResultNeedingRetry implements IngestResult
{
    private static final Logger LOGGER = LoggerFactory.getLogger( IngestResultNeedingRetry.class );
    private static final String TOO_MANY_RE_USES_OF = "Too many re-uses of ";
    private final DatasetOrientation orientation;
    private final DataSource dataSource;
    private final long surrogateKey;

    /**
     * Creates an instance.
     * @param dataSource the data source
     * @param surrogateKey the surrogate key
     */
    public IngestResultNeedingRetry( DataSource dataSource,
                                     long surrogateKey )
    {
        Objects.requireNonNull( dataSource, "Ingester must include datasource information." );

        if ( surrogateKey == 0 )
        {
            LOGGER.warn( "Suspicious surrogate key id=0 given for dataSource={}",
                         dataSource );
        }

        if ( surrogateKey < 0 )
        {
            throw new IllegalArgumentException( "Auto-generated ids are usually positive, but given surrogateKey was "
                                                + surrogateKey );
        }

        this.orientation = dataSource.getDatasetOrientation();
        this.surrogateKey = surrogateKey;
        this.dataSource = dataSource;
    }

    @Override
    public DataSource getDataSource()
    {
        return this.dataSource;
    }

    @Override
    public DatasetOrientation getDatasetOrientation()
    {
        return this.orientation;
    }

    @Override
    public long getSurrogateKey()
    {
        return this.surrogateKey;
    }

    @Override
    public boolean requiresRetry()
    {
        return true;
    }

    @Override
    public short getLeftCount()
    {
        short leftCount = 0;

        if ( this.getDatasetOrientation()
                 .equals( DatasetOrientation.LEFT ) )
        {
            leftCount++;
        }

        for ( DatasetOrientation lrb : this.getDataSource()
                                           .getLinks() )
        {
            if ( lrb.equals( DatasetOrientation.LEFT ) )
            {
                if ( leftCount == MAX_VALUE )
                {
                    throw new IllegalStateException( TOO_MANY_RE_USES_OF
                                                     + this.getDataSource()
                                                     + " in left, more than "
                                                     + MAX_VALUE );
                }

                leftCount++;
            }
        }

        return leftCount;
    }

    @Override
    public short getRightCount()
    {
        short rightCount = 0;

        if ( this.getDatasetOrientation()
                 .equals( DatasetOrientation.RIGHT ) )
        {
            rightCount++;
        }

        for ( DatasetOrientation lrb : this.getDataSource()
                                           .getLinks() )
        {
            if ( lrb.equals( DatasetOrientation.RIGHT ) )
            {
                if ( rightCount == MAX_VALUE )
                {
                    throw new IllegalStateException( TOO_MANY_RE_USES_OF
                                                     + this.getDataSource()
                                                     + " in right, more than "
                                                     + MAX_VALUE );
                }

                rightCount++;
            }
        }

        return rightCount;
    }

    @Override
    public short getBaselineCount()
    {
        short baselineCount = 0;

        if ( this.getDatasetOrientation()
                 .equals( DatasetOrientation.BASELINE ) )
        {
            baselineCount++;
        }

        for ( DatasetOrientation lrb : this.getDataSource()
                                           .getLinks() )
        {
            if ( lrb.equals( DatasetOrientation.BASELINE ) )
            {
                if ( baselineCount == MAX_VALUE )
                {
                    throw new IllegalStateException( TOO_MANY_RE_USES_OF
                                                     + this.getDataSource()
                                                     + " in baseline, more than "
                                                     + MAX_VALUE );
                }

                baselineCount++;
            }
        }

        return baselineCount;
    }

    @Override
    public String toString()
    {
        return new ToStringBuilder( this ).append( "orientation", this.getDatasetOrientation() )
                                          .append( "dataSource", this.getDataSource() )
                                          .append( "surrogateKey", this.getSurrogateKey() )
                                          .toString();
    }
}
