package wres.io.ingesting;

import java.util.Objects;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.Short.MAX_VALUE;

import wres.config.generated.LeftOrRightOrBaseline;
import wres.io.reading.DataSource;

/**
 * An IngestResult exclusively for data needing retry of ingest.
 */

public class IngestResultNeedingRetry implements IngestResult
{
    private static final Logger LOGGER = LoggerFactory.getLogger( IngestResultNeedingRetry.class );
    private final LeftOrRightOrBaseline leftOrRightOrBaseline;
    private final DataSource dataSource;
    private final long surrogateKey;

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

        this.leftOrRightOrBaseline = dataSource.getLeftOrRightOrBaseline();
        this.surrogateKey = surrogateKey;
        this.dataSource = dataSource;
    }


    @Override
    public DataSource getDataSource()
    {
        return this.dataSource;
    }

    @Override
    public LeftOrRightOrBaseline getLeftOrRightOrBaseline()
    {
        return this.leftOrRightOrBaseline;
    }

    @Override
    public long getSurrogateKey()
    {
        return this.surrogateKey;
    }

    @Override
    public boolean wasFoundAlready()
    {
        return true;
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

        if ( this.getLeftOrRightOrBaseline()
                 .equals( LeftOrRightOrBaseline.LEFT ) )
        {
            leftCount++;
        }

        for ( LeftOrRightOrBaseline lrb : this.getDataSource()
                                              .getLinks() )
        {
            if ( lrb.equals( LeftOrRightOrBaseline.LEFT ) )
            {
                if ( leftCount == MAX_VALUE )
                {
                    throw new IllegalStateException( "Too many re-uses of "
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

        if ( this.getLeftOrRightOrBaseline()
                 .equals( LeftOrRightOrBaseline.RIGHT ) )
        {
            rightCount++;
        }

        for ( LeftOrRightOrBaseline lrb : this.getDataSource()
                                              .getLinks() )
        {
            if ( lrb.equals( LeftOrRightOrBaseline.RIGHT ) )
            {
                if ( rightCount == MAX_VALUE )
                {
                    throw new IllegalStateException( "Too many re-uses of "
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

        if ( this.getLeftOrRightOrBaseline()
                 .equals( LeftOrRightOrBaseline.BASELINE ) )
        {
            baselineCount++;
        }

        for ( LeftOrRightOrBaseline lrb : this.getDataSource()
                                              .getLinks() )
        {
            if ( lrb.equals( LeftOrRightOrBaseline.BASELINE ) )
            {
                if ( baselineCount == MAX_VALUE )
                {
                    throw new IllegalStateException( "Too many re-uses of "
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
        return new ToStringBuilder( this ).append( "leftOrRightOrBaseline", this.getLeftOrRightOrBaseline() )
                                          .append( "dataSource", this.getDataSource() )
                                          .append( "surrogateKey", this.getSurrogateKey() )
                                          .toString();
    }
}
