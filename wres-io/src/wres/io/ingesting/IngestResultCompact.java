package wres.io.ingesting;

import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.Short.MAX_VALUE;

import wres.config.components.DataType;
import wres.config.components.DatasetOrientation;
import wres.reading.DataSource;

/**
 * A compact {@link IngestResult} exclusively for fully ingested data. If the data needs
 * retry, use {@link IngestResultNeedingRetry}.
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
    private final boolean hasNonMissingData;
    private final DataType dataType;
    private final Set<DatasetOrientation> orientations;

    /**
     * @param dataSource the data source
     * @param dataType the optional data type
     * @param surrogateKey the surrogate key
     * @param foundAlready whether the time-series has been found already
     * @param hasNonMissingData true if non-missing data was found, false otherwise
     */
    IngestResultCompact( DataSource dataSource,
                         DataType dataType,
                         long surrogateKey,
                         boolean foundAlready,
                         boolean hasNonMissingData )
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

        short leftCountInner = 0;
        short rightCountInner = 0;
        short baselineCountInner = 0;
        short covariateCountInner = 0;

        Set<DatasetOrientation> innerOrientations = new HashSet<>();
        DatasetOrientation orientation = dataSource.getDatasetOrientation();
        innerOrientations.add( orientation );

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
            innerOrientations.add( linkedOrientation );
        }

        // Validate the counts
        this.validateCount( dataSource, DatasetOrientation.LEFT, leftCountInner );
        this.validateCount( dataSource, DatasetOrientation.RIGHT, rightCountInner );
        this.validateCount( dataSource, DatasetOrientation.BASELINE, baselineCountInner );
        this.validateCount( dataSource, DatasetOrientation.COVARIATE, covariateCountInner );

        this.orientations = Collections.unmodifiableSet( innerOrientations );
        this.foundAlready = foundAlready;
        this.leftCount = leftCountInner;
        this.rightCount = rightCountInner;
        this.baselineCount = baselineCountInner;
        this.covariateCount = covariateCountInner;
        this.dataType = dataType;
        this.hasNonMissingData = hasNonMissingData;
    }

    /**
     * @return null in compact form
     */
    @Override
    public DataSource getDataSource()
    {
        return null;
    }

    @Override
    public DataType getDataType()
    {
        return this.dataType;
    }

    @Override
    public Set<DatasetOrientation> getDatasetOrientations()
    {
        return this.orientations;
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
    public boolean hasNonMissingData()
    {
        return this.hasNonMissingData;
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
                                          .append( "hasNonMissingData", this.hasNonMissingData )
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
