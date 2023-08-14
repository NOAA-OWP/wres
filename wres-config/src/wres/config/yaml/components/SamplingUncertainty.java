package wres.config.yaml.components;

import java.time.Duration;
import java.util.Collections;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.datatype.jsr310.deser.DurationDeserializer;
import io.soabase.recordbuilder.core.RecordBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The parameters to use when estimating the sampling uncertainty of a verification statistic.
 *
 * @param quantiles the desired quantiles of the sampling distribution of a statistic
 * @param sampleSize the number of samples to use when estimating the sampling distribution of a statistic
 * @param blockSize the temporal auto-correlation length to use when estimating the sampling uncertainties
 */
@RecordBuilder
public record SamplingUncertainty( @JsonProperty( "quantiles" ) SortedSet<Double> quantiles,
                                   @JsonProperty( "sample_size" ) int sampleSize,
                                   @JsonDeserialize( using = DurationDeserializer.class ) Duration blockSize )
{
    /** The default sample size. */
    public static final int DEFAULT_SAMPLE_SIZE = 5000;

    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( SamplingUncertainty.class );

    /**
     * Sets the default values.
     * @param quantiles the desired quantiles of the sampling distribution of a statistic
     * @param sampleSize the number of samples to use when estimating the sampling distribution of a statistic
     * @param blockSize the temporal auto-correlation length to use when estimating the sampling uncertainties
     */
    public SamplingUncertainty
    {
        if ( Objects.isNull( quantiles ) )
        {
            // Undefined is the sentinel for "all valid"
            quantiles = Collections.emptySortedSet();
        }
        else
        {
            // Immutable copy
            quantiles = Collections.unmodifiableSortedSet( new TreeSet<>( quantiles ) );
        }

        if ( sampleSize == 0 )
        {
            LOGGER.debug( "Discovered a sample size of zero for sampling uncertainty estimation. Using the default "
                          + "sample size of {} instead.", DEFAULT_SAMPLE_SIZE );

            sampleSize = DEFAULT_SAMPLE_SIZE;
        }
    }
}