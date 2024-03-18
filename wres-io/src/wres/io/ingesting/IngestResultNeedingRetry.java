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
    private final short leftCount;
    private final short rightCount;
    private final short baselineCount;
    private final short covariateCount;
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

        short leftCountInner = 0;
        short rightCountInner = 0;
        short baselineCountInner = 0;
        short covariateCountInner = 0;

        DatasetOrientation innerOrientation = dataSource.getDatasetOrientation();

        switch ( innerOrientation )
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

        this.orientation = dataSource.getDatasetOrientation();
        this.surrogateKey = surrogateKey;
        this.dataSource = dataSource;
        this.leftCount = leftCountInner;
        this.rightCount = rightCountInner;
        this.baselineCount = baselineCountInner;
        this.covariateCount = covariateCountInner;
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
        return new ToStringBuilder( this ).append( "orientation", this.getDatasetOrientation() )
                                          .append( "dataSource", this.getDataSource() )
                                          .append( "surrogateKey", this.getSurrogateKey() )
                                          .append( "leftCount", this.leftCount )
                                          .append( "rightCount", this.rightCount )
                                          .append( "baselineCount", this.baselineCount )
                                          .append( "covariateCount", this.covariateCount )
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
