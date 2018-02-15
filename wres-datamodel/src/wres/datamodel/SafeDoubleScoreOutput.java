package wres.datamodel;

import java.util.Arrays;
import java.util.Map;

import wres.datamodel.MetricConstants.ScoreOutputGroup;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.datamodel.outputs.MetricOutputException;

/**
 * An immutable output that contains <code>double</code> values associated with a score.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.4
 */

class SafeDoubleScoreOutput extends SafeScoreOutput<Double,DoubleScoreOutput> implements DoubleScoreOutput
{

    /**
     * Construct the output.
     * 
     * @param output the verification output
     * @param meta the metadata
     * @throws MetricOutputException if any of the inputs are invalid
     */

    SafeDoubleScoreOutput( final double output, final MetricOutputMetadata meta )
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

    SafeDoubleScoreOutput( final Map<MetricConstants, Double> output, final MetricOutputMetadata meta )
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

    SafeDoubleScoreOutput( final double[] output, final ScoreOutputGroup template, final MetricOutputMetadata meta )
    {
        super( Arrays.stream( output ).boxed().toArray( Double[]::new ), template, meta );
    }

    @Override
    DoubleScoreOutput getScoreOutput( Double input, MetricOutputMetadata meta )
    {
        return new SafeDoubleScoreOutput( input, meta );
    }


}
