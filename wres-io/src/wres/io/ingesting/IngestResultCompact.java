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
    private final short covariateCount;
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
        DatasetOrientation orientation = dataSource.getDatasetOrientation();

        short leftCountInner = 0;
        short rightCountInner = 0;
        short baselineCountInner = 0;
        short covariateCountInner = 0;

        switch ( orientation )
        {
            case LEFT -> leftCountInner++;
            case RIGHT -> rightCountInner++;
            case BASELINE -> baselineCountInner++;
            case COVARIATE -> covariateCountInner++;
        }

        for ( DatasetOrientation linkedOrientation : dataSource.getLinks() )
        {
            switch ( linkedOrientation )
            {
                case LEFT -> leftCountInner++;
                case RIGHT -> rightCountInner++;
                case BASELINE -> baselineCountInner++;
                case COVARIATE -> covariateCountInner++;
            }
        }

        // Validate the counts
        this.validateCount( dataSource, DatasetOrientation.LEFT, leftCountInner );
        this.validateCount( dataSource, DatasetOrientation.RIGHT, rightCountInner );
        this.validateCount( dataSource, DatasetOrientation.BASELINE, baselineCountInner );
        this.validateCount( dataSource, DatasetOrientation.COVARIATE, covariateCountInner );

        this.foundAlready = foundAlready;
        this.leftCount = leftCountInner;
        this.rightCount = rightCountInner;
        this.baselineCount = baselineCountInner;
        this.covariateCount = covariateCountInner;
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
    public short getCovariateCount()
    {
        return this.covariateCount;
    }

    @Override
    public String toString()
    {
        return new ToStringBuilder( this ).append( "surrogateKey", this.surrogateKey )
                                          .append( "leftCount", this.leftCount )
                                          .append( "rightCount", this.rightCount )
                                          .append( "baselineCount", this.baselineCount )
                                          .append( "covariateCount", this.covariateCount )
                                          .append( "foundAlready", this.foundAlready )
                                          .toString();
    }

    /**
     * Validates the number of re-uses of a datasource.
     * @param source the data source
     * @param orientation the orientation
     * @param count the count of re-uses
     */
    private void validateCount( DataSource source, DatasetOrientation orientation, long count )
    {
        if ( count == MAX_VALUE )
        {
            throw new IllegalArgumentException( TOO_MANY_RE_USES_OF
                                                + source
                                                + " in "
                                                + orientation
                                                + ", more than "
                                                + MAX_VALUE );
        }
    }
}
