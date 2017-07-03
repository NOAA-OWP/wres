package wres.datamodel.metric;

import java.util.ArrayList;

/**
 * A collection of metric outputs
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public final class UnsafeMetricOutputCollection<T extends MetricOutput<?>> extends ArrayList<T>
implements MetricOutputCollection<T>
{
    private static final long serialVersionUID = -7715026905377885849L;

    /**
     * Default constructor.
     */

    public UnsafeMetricOutputCollection()
    {
        super();
    }

    /**
     * Construct with an initial size for the collection.
     * 
     * @param size the initial size.
     */

    public UnsafeMetricOutputCollection(final int size)
    {
        super(size);
    }

}
