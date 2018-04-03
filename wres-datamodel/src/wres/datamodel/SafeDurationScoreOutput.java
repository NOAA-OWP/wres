package wres.datamodel;

import java.time.Duration;
import java.util.Map;

import wres.datamodel.MetricConstants.ScoreOutputGroup;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.outputs.DurationScoreOutput;
import wres.datamodel.outputs.MetricOutputException;

/**
 * An immutable output that contains {@link Duration} values associated with a score.
 * 
 * @author james.brown@hydrosolved.com
 */

class SafeDurationScoreOutput extends SafeScoreOutput<Duration,DurationScoreOutput> implements DurationScoreOutput
{

    /**
     * Construct the output.
     * 
     * @param output the verification output
     * @param meta the metadata
     * @throws MetricOutputException if any of the inputs are invalid
     */

    SafeDurationScoreOutput( final Duration output, final MetricOutputMetadata meta )
    {
        super(output, meta);
    }

    /**
     * Construct the output with a map.
     * 
     * @param output the verification output
     * @param meta the metadata
     * @throws MetricOutputException if any of the inputs are invalid
     */

    SafeDurationScoreOutput( final Map<MetricConstants, Duration> output, final MetricOutputMetadata meta )
    {
        super(output, meta);
    }

    /**
     * Construct the output with a template.
     * 
     * @param output the verification output
     * @param template the score template
     * @param meta the metadata
     * @throws MetricOutputException if any of the inputs are invalid
     */

    SafeDurationScoreOutput( final Duration[] output, final ScoreOutputGroup template, final MetricOutputMetadata meta )
    {
        super( output, template, meta );
    }

    @Override
    DurationScoreOutput getScoreOutput( Duration input, MetricOutputMetadata meta )
    {
        return new SafeDurationScoreOutput( input, meta );
    }

}
