package wres.io.ingesting;

import java.util.Objects;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.Short.MAX_VALUE;

import wres.config.yaml.components.DatasetOrientation;
import wres.reading.DataSource;

/**
 * A compact IngestResult exclusively for fully ingested data. If the data needs
 * retry, use IngestResultNeedingRetry.
 */

class IngestResultCompact implements IngestResult
{
    private static final Logger LOGGER = LoggerFactory.getLogger( IngestResultCompact.class );
    private static final String TOO_MANY_RE_USES_OF = "Too many re-uses of ";
    private final long surrogateKey;
    private final short leftCount;
    private final short rightCount;
    private final short baselineCount;
    private final boolean foundAlready;

    IngestResultCompact( DataSource dataSource,
                         long surrogateKey,
                         boolean foundAlready )
    {
        Objects.requireNonNull( dataSource, "Ingester must include datasource information." );

        if ( surrogateKey == 0 )
        {
            LOGGER.warn( "Suspicious surrogate key id=0 given for dataSource={} foundAlready={}",
                         dataSource,
                         foundAlready );
        }

        if ( surrogateKey < 0 )
        {
            throw new IllegalArgumentException( "Auto-generated ids are usually positive, but given surrogateKey was "
                                                + surrogateKey );
        }

        this.surrogateKey = surrogateKey;
        DatasetOrientation lrb = dataSource.getDatasetOrientation();

        short leftCountInner = 0;
        short rightCountInner = 0;
        short baselineCountInner = 0;

        if ( lrb == DatasetOrientation.LEFT )
        {
            leftCountInner++;
        }
        else if ( lrb == DatasetOrientation.RIGHT )
        {
            rightCountInner++;
        }
        else if ( lrb == DatasetOrientation.BASELINE )
        {
            baselineCountInner++;
        }

        for ( DatasetOrientation lrbn : dataSource.getLinks() )
        {
            if ( lrbn.equals( DatasetOrientation.LEFT ) )
            {
                if ( leftCountInner == MAX_VALUE )
                {
                    throw new IllegalArgumentException( TOO_MANY_RE_USES_OF
                                                        + dataSource
                                                        + " in left, more than "
                                                        + MAX_VALUE );
                }

                leftCountInner++;
            }
            else if ( lrbn.equals( DatasetOrientation.RIGHT ) )
            {
                if ( rightCountInner == MAX_VALUE )
                {
                    throw new IllegalArgumentException( TOO_MANY_RE_USES_OF
                                                        + dataSource
                                                        + " in right, more than "
                                                        + MAX_VALUE );
                }

                rightCountInner++;
            }
            else if ( lrbn.equals( DatasetOrientation.BASELINE ) )
            {
                if ( baselineCountInner == MAX_VALUE )
                {
                    throw new IllegalArgumentException( TOO_MANY_RE_USES_OF
                                                        + dataSource
                                                        + " in baseline, more than "
                                                        + MAX_VALUE );
                }

                baselineCountInner++;
            }
        }

        this.foundAlready = foundAlready;
        this.leftCount = leftCountInner;
        this.rightCount = rightCountInner;
        this.baselineCount = baselineCountInner;
    }

    /**
     * @throws UnsupportedOperationException Every time.
     */
    @Override
    public DataSource getDataSource()
    {
        throw new UnsupportedOperationException( "DataSource is unavailable in compact form." );
    }

    /**
     * @throws UnsupportedOperationException Every time.
     */
    @Override
    public DatasetOrientation getDatasetOrientation()
    {
        throw new UnsupportedOperationException( "Primary dataset orientation is unavailable in compact form." );
    }

    @Override
    public long getSurrogateKey()
    {
        return this.surrogateKey;
    }

    @Override
    public boolean requiresRetry()
    {
        return false;
    }

    @Override
    public short getLeftCount()
    {
        return this.leftCount;
    }

    @Override
    public short getRightCount()
    {
        return this.rightCount;
    }

    @Override
    public short getBaselineCount()
    {
        return this.baselineCount;
    }

    @Override
    public String toString()
    {
        return new ToStringBuilder( this ).append( "surrogateKey", surrogateKey )
                                          .append( "leftCount", leftCount )
                                          .append( "rightCount", rightCount )
                                          .append( "baselineCount", baselineCount )
                                          .append( "foundAlready", foundAlready )
                                          .toString();
    }
}
