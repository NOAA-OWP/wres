package wres.datamodel.metric;

import java.util.Arrays;
import java.util.Objects;

import wres.datamodel.MatrixOfDoubles;
import wres.datamodel.metric.MetricOutput;
import wres.datamodel.metric.MetricOutputMetadata;

/**
 * <p>
 * A matrix of outputs associated with a metric. The number of elements and the order in which they are stored, is
 * prescribed by the metric from which the outputs originate.
 * </p>
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public class MatrixOutput implements MetricOutput<MatrixOfDoubles>
{

    /**
     * The output data.
     */

    private final MatrixOfDoubles output;

    /**
     * The metadata associated with the output.
     */

    private final MetricOutputMetadata meta;

    @Override
    public MetricOutputMetadata getMetadata()
    {
        return meta;
    }

    @Override
    public MatrixOfDoubles getData()
    {
        return output;
    }

    @Override
    public boolean equals(final Object o)
    {
        boolean start = o instanceof MatrixOutput;
        if(start)
        {
            final MatrixOutput m = (MatrixOutput)o;
            start = meta.equals(m.getMetadata());
            start = start && m.output.rows() == output.rows() && m.output.columns() == output.columns();
            start = start && Arrays.deepEquals(output.getDoubles(), m.getData().getDoubles());
        }
        return start;
    }

    @Override
    public int hashCode()
    {
        return getMetadata().hashCode() + Arrays.deepHashCode(output.getDoubles());
    }
    
    /**
     * Construct the output.
     * 
     * @param output the verification output.
     * @param meta the metadata.
     */

    protected MatrixOutput(final MatrixOfDoubles output, final MetricOutputMetadata meta)
    {
        Objects.requireNonNull(output,"Specify a non-null output.");
        Objects.requireNonNull(meta,"Specify non-null metadata.");             
        this.output = output;
        this.meta = meta;
    }    

}
