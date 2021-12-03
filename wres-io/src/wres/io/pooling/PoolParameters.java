package wres.io.pooling;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A small value class that contains parameters for pool generation.
 * 
 * @author James Brown
 */

public class PoolParameters
{
    /** Logger. **/
    private static final Logger LOGGER = LoggerFactory.getLogger( PoolParameters.class );

    /**
     * The threshold number of singleton feature groups at which features are batched for retrieval as an optimization. 
     * For example, if the value of this parameter is 10, then feature-batched retrieval will occur for
     * any evaluation that contains 10 or more singleton feature groups. A singleton feature group is a feature group
     * that contains a single feature tuple.
     * 
     * @see #featureBatchSize for the number of features per batch.
     */

    private final int featureBatchThreshold;

    /**
     * The number of features to include in a batched retrieval when performing feature batching.
     * 
     * @see #featureBatchThreshold for the number of features in an evaluation above which batching occurs
     */

    private final int featureBatchSize;

    @Override
    public String toString()
    {
        return new ToStringBuilder( this, ToStringStyle.SHORT_PREFIX_STYLE ).append( "featureBatchThreshold",
                                                                                     this.featureBatchThreshold )
                                                                            .append( "featureBatchSize",
                                                                                     this.featureBatchSize )
                                                                            .toString();
    }

    /**
     * @return the minimum number of singleton feature groups at which feature-batching should occur.
     */

    public int getFeatureBatchThreshold()
    {
        return this.featureBatchThreshold;
    }

    /**
     * @return the number of features in a feature batch when conducting feature-batched retrieval.
     */

    public int getFeatureBatchSize()
    {
        return this.featureBatchSize;
    }

    /**
     * Build an instance.
     */

    public static class Builder
    {
        /** The threshold number of singleton feature groups above which features are batched for retrieval. */
        private int featureBatchThreshold = 10;

        /** The number of features to include in a batched retrieval when performing feature batching. */
        private int featureBatchSize = 50;

        /**
         * Sets the threshold number of singleton feature groups above which features are batched for retrieval.
         * @param featureBatchAboveThisThreshold the threshold above which feature-batching occurs
         * @return this builder
         */
        public Builder setFeatureBatchThreshold( int featureBatchAboveThisThreshold )
        {
            this.featureBatchThreshold = featureBatchAboveThisThreshold;
            return this;
        }

        /**
         * Sets the number of singleton feature groups to include in a feature batch when conducting feature-batched 
         * retrieval.
         * @param featureBatchSize the number of features in a batch
         * @return this builder
         */
        public Builder setFeatureBatchSize( int featureBatchSize )
        {
            this.featureBatchSize = featureBatchSize;
            return this;
        }

        /**
         * @return an instance.
         */

        public PoolParameters build()
        {
            return new PoolParameters( this );
        }
    }

    /**
     * Builds an instance.
     * @param builder the builder
     */
    private PoolParameters( Builder builder )
    {
        // Set then validate
        this.featureBatchThreshold = builder.featureBatchThreshold;
        this.featureBatchSize = builder.featureBatchSize;

        if ( this.featureBatchThreshold < 0 )
        {
            throw new IllegalArgumentException( "Expected a non-negative number of features above which feature "
                                                + "batching should occur, but actually received: "
                                                + this.featureBatchThreshold
                                                + "." );
        }

        if ( this.featureBatchSize < 1 )
        {
            throw new IllegalArgumentException( "Expected a number of features greater than or equal to 1 for the size "
                                                + "of a feature batch when performing feature-batched retrieval, but "
                                                + "actually received: "
                                                + this.featureBatchSize
                                                + "." );
        }

        LOGGER.debug( "Created pool parameters of {}.", this );
    }

}
