package wres.datamodel.statistics;

import java.util.List;
import java.util.Objects;

import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.metadata.StatisticMetadata;

/**
 * Immutable store of several box plot statistics.
 * 
 * @author james.brown@hydrosolved.com
 */
public class BoxPlotStatistics extends ListOfStatistics<BoxPlotStatistic> implements Statistic<List<BoxPlotStatistic>>
{

    /**
     * The dimension associated with the domain axis.
     */

    private final MetricDimension domainAxisDimension;

    /**
     * The dimension associated with the range axis (boxes).
     */

    private final MetricDimension rangeAxisDimension;
    
    /**
     * The statistics metadata.
     */

    private final StatisticMetadata metadata;
    
    /**
     * Returns an instance from the inputs.
     * 
     * @param statistics the box plot data
     * @param domainAxisDimension the domain axis dimension
     * @param rangeAxisDimension the range axis dimension
     * @param metadata the metadata
     * @throws StatisticException if any of the inputs is invalid
     * @throws NullPointerException if any of the inputs is null
     * @return an instance of the output
     */

    public static BoxPlotStatistics of( List<BoxPlotStatistic> statistics,
                                        MetricDimension domainAxisDimension,
                                        MetricDimension rangeAxisDimension,
                                        StatisticMetadata metadata )
    {
        return new BoxPlotStatistics( statistics, domainAxisDimension, rangeAxisDimension, metadata );
    }

    /**
     * Returns the dimension associated with the left side of the pairing, i.e. the value against which each box is
     * plotted on the domain axis. 
     * 
     * @return the domain axis dimension
     */

    public MetricDimension getDomainAxisDimension()
    {
        return this.domainAxisDimension;
    }

    /**
     * Returns the dimension associated with the right side of the pairing, i.e. the values associated with the 
     * whiskers of each box. 
     * 
     * @return the range axis dimension
     */

    public MetricDimension getRangeAxisDimension()
    {
        return this.rangeAxisDimension;
    }


    @Override
    public StatisticMetadata getMetadata()
    {
        return metadata;
    }
    
    @Override
    public boolean equals( Object o )
    {
        if ( ! ( o instanceof BoxPlotStatistics ) )
        {
            return false;
        }

        if ( !super.equals( o ) )
        {
            return false;
        }

        BoxPlotStatistics p = (BoxPlotStatistics) o;
 
        if( !this.getMetadata().equals( p.getMetadata() ) )
        {
            return false;
        }
        
        return this.getDomainAxisDimension() == p.getDomainAxisDimension()
               && this.getRangeAxisDimension() == p.getRangeAxisDimension();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( super.hashCode(), domainAxisDimension, rangeAxisDimension );
    }

    /**
     * Hidden constructor.
     * 
     * @param statistics the box plot data
     * @param domainAxisDimension the domain axis dimension
     * @param rangeAxisDimension the range axis dimension
     * @param metadata the metadata
     * @throws NullPointerException if any input is null
     */

    private BoxPlotStatistics( List<BoxPlotStatistic> statistics,
                               MetricDimension domainAxisDimension,
                               MetricDimension rangeAxisDimension,
                               StatisticMetadata metadata )
    {
        super( statistics );

        Objects.requireNonNull( domainAxisDimension );
        
        Objects.requireNonNull( rangeAxisDimension );
        
        Objects.requireNonNull( metadata );
        
        this.domainAxisDimension = domainAxisDimension;
        this.rangeAxisDimension = rangeAxisDimension;        
        this.metadata = metadata;
    }

}
