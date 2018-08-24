package wres.datamodel.statistics;

import java.time.Duration;
import java.util.Map;

import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.ScoreOutputGroup;
import wres.datamodel.metadata.MetricOutputMetadata;

/**
 * An immutable metric output that comprises one or more {@link Duration} components.
 * 
 * @author james.brown@hydrosolved.com
 */

public class DurationScoreOutput extends BasicScoreOutput<Duration, DurationScoreOutput>
{

    /**
     * Construct the output.
     * 
     * @param output the verification output
     * @param meta the metadata
     * @throws MetricOutputException if any of the inputs are invalid
     * @return an instance of the output
     */

    public static DurationScoreOutput of( final Duration output, final MetricOutputMetadata meta )
    {
        return new DurationScoreOutput( output, meta );
    }

    /**
     * Construct the output with a map.
     * 
     * @param output the verification output
     * @param meta the metadata
     * @throws MetricOutputException if any of the inputs are invalid
     * @return an instance of the output
     */

    public static DurationScoreOutput of( final Map<MetricConstants, Duration> output, final MetricOutputMetadata meta )
    {
        return new DurationScoreOutput( output, meta );
    }

    /**
     * Construct the output with a template.
     * 
     * @param output the verification output
     * @param template the score template
     * @param meta the metadata
     * @throws MetricOutputException if any of the inputs are invalid
     * @return an instance of the output
     */

    public static DurationScoreOutput
            of( final Duration[] output, final ScoreOutputGroup template, final MetricOutputMetadata meta )
    {
        return new DurationScoreOutput( output, template, meta );
    }

    @Override
    DurationScoreOutput getScoreOutput( Duration input, MetricOutputMetadata meta )
    {
        return new DurationScoreOutput( input, meta );
    }

    /**
     * Construct the output.
     * 
     * @param output the verification output
     * @param meta the metadata
     * @throws MetricOutputException if any of the inputs are invalid
     */

    private DurationScoreOutput( final Duration output, final MetricOutputMetadata meta )
    {
        super( output, meta );
    }

    /**
     * Construct the output with a map.
     * 
     * @param output the verification output
     * @param meta the metadata
     * @throws MetricOutputException if any of the inputs are invalid
     */

    private DurationScoreOutput( final Map<MetricConstants, Duration> output, final MetricOutputMetadata meta )
    {
        super( output, meta );
    }

    /**
     * Construct the output with a template.
     * 
     * @param output the verification output
     * @param template the score template
     * @param meta the metadata
     * @throws MetricOutputException if any of the inputs are invalid
     */

    private DurationScoreOutput( final Duration[] output,
                                 final ScoreOutputGroup template,
                                 final MetricOutputMetadata meta )
    {
        super( output, template, meta );
    }

}
