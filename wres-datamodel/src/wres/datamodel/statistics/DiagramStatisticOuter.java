package wres.datamodel.statistics;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.commons.lang3.tuple.Pair;

import net.jcip.annotations.Immutable;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.metrics.MetricConstants;
import wres.datamodel.metrics.MetricConstants.MetricDimension;
import wres.statistics.generated.DiagramStatistic;
import wres.statistics.generated.DiagramStatistic.DiagramStatisticComponent;

/**
 * A wrapping for a {@link DiagramStatistic}.
 * 
 * @author james.brown@hydrosolved.com
 */
@Immutable
public class DiagramStatisticOuter implements Statistic<DiagramStatistic>
{
    /**
     * The diagram.
     */

    private final DiagramStatistic diagram;

    /**
     * Local set of component names from the canonical {@link #diagram}.
     */

    private final SortedSet<MetricDimension> componentNames;

    /**
     * Local set of component name qualifiers from the canonical {@link #diagram}.
     */

    private final SortedSet<String> componentNameQualifiers;

    /**
     * The metadata associated with the statistic.
     */

    private final PoolMetadata metadata;

    /**
     * The metric name.
     */

    private final MetricConstants metricName;

    /**
     * An internal map of indexes of pairs of metric names and qualifiers to allow fast lookup.
     */

    private final Map<Pair<MetricDimension, String>, Integer> componentIndexes;

    /**
     * Construct the diagram.
     * 
     * @param diagram the verification diagram
     * @param meta the metadata
     * @throws StatisticException if any of the inputs are invalid
     * @return an instance of the output
     */

    public static DiagramStatisticOuter of( DiagramStatistic diagram,
                                            PoolMetadata meta )
    {
        return new DiagramStatisticOuter( diagram, meta );
    }

    @Override
    public MetricConstants getMetricName()
    {
        return this.metricName;
    }

    /**
     * Returns the diagram component names.
     * 
     * @return the component names
     */

    public SortedSet<MetricDimension> getComponentNames()
    {
        return this.componentNames; // Immutable on construction.
    }

    /**
     * Returns the qualifiers for diagram component names where the same name appears more than once.
     * 
     * @return the component name qualifiers
     */

    public SortedSet<String> getComponentNameQualifiers()
    {
        return this.componentNameQualifiers;
    }

    /**
     * Returns a component with a prescribed name and qualifier.
     * 
     * @param name the component name
     * @param qualifier the component name qualifier
     * @return the metric component
     * @throws IllegalArgumentException if no such component exists
     * @throws NullPointerException if either input is null
     */

    public DiagramStatisticComponent getComponent( MetricDimension name, String qualifier )
    {
        Objects.requireNonNull( name );
        Objects.requireNonNull( qualifier );

        Pair<MetricDimension, String> key = Pair.of( name, qualifier );
        if ( !this.componentIndexes.containsKey( key ) )
        {
            throw new IllegalArgumentException( "There is no such metric component named " + name
                                                + " with qualifier "
                                                + qualifier
                                                + "." );
        }

        int componentIndex = this.componentIndexes.get( key );
        return this.diagram.getStatistics( componentIndex );
    }

    @Override
    public PoolMetadata getMetadata()
    {
        return this.metadata;
    }

    @Override
    public DiagramStatistic getData()
    {
        return this.diagram;
    }

    @Override
    public boolean equals( final Object o )
    {
        if( o == this )
        {
            return true;
        }
        
        if ( ! ( o instanceof DiagramStatisticOuter ) )
        {
            return false;
        }
        final DiagramStatisticOuter v = (DiagramStatisticOuter) o;

        return this.getMetadata().equals( v.getMetadata() ) && this.getData().equals( v.getData() );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( this.getMetadata(), this.getData() );
    }

    @Override
    public String toString()
    {
        ToStringBuilder builder = new ToStringBuilder( this, ToStringStyle.SHORT_PREFIX_STYLE );

        builder.append( "metric name", this.getData().getMetric().getName() );

        this.getData()
            .getStatisticsList()
            .forEach( component -> builder.append( "component name", component.getMetric().getName() )
                                          .append( "component values", component.getValuesList() ) );

        return builder.toString();
    }

    /**
     * Construct the statistic.
     * 
     * @param diagram the verification diagram
     * @param meta the metadata
     * @throws StatisticException if any of the inputs are invalid
     */

    private DiagramStatisticOuter( DiagramStatistic diagram, PoolMetadata meta )
    {
        if ( Objects.isNull( diagram ) )
        {
            throw new StatisticException( "Specify a non-null output." );
        }
        if ( Objects.isNull( meta ) )
        {
            throw new StatisticException( "Specify non-null metadata." );
        }

        this.diagram = diagram;
        this.metadata = meta;

        SortedSet<MetricDimension> componentNames = new TreeSet<>();
        SortedSet<String> componentNameQualifiers = new TreeSet<>();
        Map<Pair<MetricDimension, String>, Integer> componentIndexes = new HashMap<>();

        int index = 0;
        for ( DiagramStatisticComponent next : this.getData().getStatisticsList() )
        {
            MetricDimension nextName = MetricDimension.valueOf( next.getMetric().getName().name() );
            componentNames.add( nextName );
            String nextQualifier = next.getName();
            componentNameQualifiers.add( nextQualifier );
            componentIndexes.put( Pair.of( nextName, nextQualifier ), index );
            index++;
        }

        this.componentNames = Collections.unmodifiableSortedSet( componentNames );
        this.componentNameQualifiers = Collections.unmodifiableSortedSet( componentNameQualifiers );
        this.metricName = MetricConstants.valueOf( diagram.getMetric().getName().name() );
        this.componentIndexes = componentIndexes; // Not exposed to mutation
    }

}
