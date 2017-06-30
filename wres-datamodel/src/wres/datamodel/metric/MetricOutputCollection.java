package wres.datamodel.metric;

import java.util.ArrayList;

import wres.datamodel.metric.MetricOutput;

/**
 * A collection of metric outputs
 * 
 * @author james.brown@hydrosolved.com
 */

public final class MetricOutputCollection<T extends MetricOutput<?>> extends ArrayList<T>
{
    private static final long serialVersionUID = -7715026905377885849L;

    /**
     * Default constructor.
     */

    public MetricOutputCollection()
    {
        super();
    }

    /**
     * Construct with an initial size for the collection.
     * 
     * @param size the initial size.
     */

    public MetricOutputCollection(final int size)
    {
        super(size);
    }

}
