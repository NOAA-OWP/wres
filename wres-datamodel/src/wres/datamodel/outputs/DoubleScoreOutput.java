package wres.datamodel.outputs;

import java.util.Arrays;
import java.util.Map;

import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.ScoreOutputGroup;
import wres.datamodel.metadata.MetricOutputMetadata;

/**
 * An immutable metric output that comprises one or more {@link Double} components.
 * 
 * @author james.brown@hydrosolved.com
 */

public class DoubleScoreOutput extends BasicScoreOutput<Double, DoubleScoreOutput>
{

    /**
     * Construct the output.
     * 
     * @param output the verification output
     * @param meta the metadata
     * @throws MetricOutputException if any of the inputs are invalid
     * @return an instance of the output
     */

    public static DoubleScoreOutput of( final double output, final MetricOutputMetadata meta )
    {
        return new DoubleScoreOutput( output, meta );
    }

    /**
     * Construct the output with a map.
     * 
     * @param output the verification output
     * @param meta the metadata
     * @throws MetricOutputException if any of the inputs are invalid
     * @return an instance of the output
     */

    public static DoubleScoreOutput of( final Map<MetricConstants, Double> output, final MetricOutputMetadata meta )
    {
        return new DoubleScoreOutput( output, meta );
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

    public static DoubleScoreOutput
            of( final double[] output, final ScoreOutputGroup template, final MetricOutputMetadata meta )
    {
        return new DoubleScoreOutput( output, template, meta );
    }

    @Override
    DoubleScoreOutput getScoreOutput( Double input, MetricOutputMetadata meta )
    {
        return new DoubleScoreOutput( input, meta );
    }

    /**
     * Construct the output.
     * 
     * @param output the verification output
     * @param meta the metadata
     * @throws MetricOutputException if any of the inputs are invalid
     */

    private DoubleScoreOutput( final double output, final MetricOutputMetadata meta )
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

    private DoubleScoreOutput( final Map<MetricConstants, Double> output, final MetricOutputMetadata meta )
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

    private DoubleScoreOutput( final double[] output, final ScoreOutputGroup template, final MetricOutputMetadata meta )
    {
        super( Arrays.stream( output ).boxed().toArray( Double[]::new ), template, meta );
    }

}
