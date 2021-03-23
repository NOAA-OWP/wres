package wres.io.reading;

import java.util.Objects;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.Short.MAX_VALUE;

import wres.config.generated.LeftOrRightOrBaseline;

/**
 * A compact IngestResult exclusively for fully ingested data. If the data needs
 * retry, use IngestResultNeedingRetry.
 */

class IngestResultCompact implements IngestResult
{
    private static final Logger LOGGER = LoggerFactory.getLogger( IngestResultCompact.class );
    private final long surrogateKey;
    private final short leftCount;
    private final short rightCount;
    private final short baselineCount;
    private final boolean foundAlready;

    IngestResultCompact( LeftOrRightOrBaseline leftOrRightOrBaseline,
                         DataSource dataSource,
                         long surrogateKey,
                         boolean foundAlready )
    {
        Objects.requireNonNull( leftOrRightOrBaseline, "Ingester must include left/right/baseline" );
        Objects.requireNonNull( dataSource, "Ingester must include datasource information." );

        if ( surrogateKey == 0 )
        {
            LOGGER.warn( "Suspicious surrogate key id=0 given for dataSource={} with l/r/b={} foundAlready={}",
                         dataSource, leftOrRightOrBaseline, foundAlready );
        }

        if ( surrogateKey < 0 )
        {
            throw new IllegalArgumentException( "Auto-generated ids are usually positive, but given surrogateKey was "
                                                + surrogateKey );
        }

        this.surrogateKey = surrogateKey;

        short leftCount = 0;
        short rightCount = 0;
        short baselineCount = 0;

        if ( leftOrRightOrBaseline.equals( LeftOrRightOrBaseline.LEFT ) )
        {
            leftCount++;
        }
        else if ( leftOrRightOrBaseline.equals( LeftOrRightOrBaseline.RIGHT ) )
        {
            rightCount++;
        }
        else if ( leftOrRightOrBaseline.equals( LeftOrRightOrBaseline.BASELINE ) )
        {
            baselineCount++;
        }

        for ( LeftOrRightOrBaseline lrb : dataSource.getLinks() )
        {
            if ( lrb.equals( LeftOrRightOrBaseline.LEFT ) )
            {
                if ( leftCount == MAX_VALUE )
                {
                    throw new IllegalArgumentException( "Too many re-uses of "
                                                        + dataSource
                                                        + " in left, more than "
                                                        + MAX_VALUE );
                }

                leftCount++;
            }
            else if ( lrb.equals( LeftOrRightOrBaseline.RIGHT ) )
            {
                if ( rightCount == MAX_VALUE )
                {
                    throw new IllegalArgumentException( "Too many re-uses of "
                                                        + dataSource
                                                        + " in right, more than "
                                                        + MAX_VALUE );
                }

                rightCount++;
            }
            else if ( lrb.equals( LeftOrRightOrBaseline.BASELINE ) )
            {
                if ( baselineCount == MAX_VALUE )
                {
                    throw new IllegalArgumentException( "Too many re-uses of "
                                                        + dataSource
                                                        + " in baseline, more than "
                                                        + MAX_VALUE );
                }

                baselineCount++;
            }
        }

        this.foundAlready = foundAlready;
        this.leftCount = leftCount;
        this.rightCount = rightCount;
        this.baselineCount = baselineCount;
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
    public LeftOrRightOrBaseline getLeftOrRightOrBaseline()
    {
        throw new UnsupportedOperationException( "Primary LRB is unavailable in compact form." );
    }

    @Override
    public long getSurrogateKey()
    {
        return this.surrogateKey;
    }

    @Override
    public boolean wasFoundAlready()
    {
        return this.foundAlready;
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
        return new ToStringBuilder( this )
                .append( "surrogateKey", surrogateKey )
                .append( "leftCount", leftCount )
                .append( "rightCount", rightCount )
                .append( "baselineCount", baselineCount )
                .append( "foundAlready", foundAlready )
                .toString();
    }
}
