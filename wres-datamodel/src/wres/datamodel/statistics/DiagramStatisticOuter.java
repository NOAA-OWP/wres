package wres.datamodel.statistics;

import java.util.Collections;
import java.util.EnumMap;
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
import wres.config.MetricConstants;
import wres.config.MetricConstants.MetricDimension;
import wres.statistics.generated.DiagramMetric.DiagramMetricComponent;
import wres.statistics.generated.DiagramMetric.DiagramMetricComponent.DiagramComponentType;
import wres.statistics.generated.DiagramStatistic;
import wres.statistics.generated.DiagramStatistic.DiagramStatisticComponent;

/**
 * A wrapping for a {@link DiagramStatistic}.
 * 
 * @author James Brown
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
     * Local set of metric components by type from the canonical {@link #diagram}.
     */

    private final Map<DiagramComponentType, Pair<DiagramMetricComponent, MetricDimension>> componentsByType;

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
     * Returns the diagram metric component name for the prescribed type of component.
     * 
     * @param type the component type
     * @return the prescribed component name or null if no such component exists
     * @throws NullPointerException if the input is null
     */

    public MetricDimension getComponentName( DiagramComponentType type )
    {
        Objects.requireNonNull( type );
        MetricDimension name = null;
        if ( this.componentsByType.containsKey( type ) )
        {
            Pair<DiagramMetricComponent, MetricDimension> pair = this.componentsByType.get( type );
            name = pair.getRight();
        }

        return name;
    }

    /**
     * Returns the diagram metric component for the prescribed type of component.
     * 
     * @param type the component type
     * @return the prescribed component or null if no such component exists
     * @throws NullPointerException if the input is null
     */

    public DiagramMetricComponent getComponent( DiagramComponentType type )
    {
        Objects.requireNonNull( type );
        DiagramMetricComponent component = null;
        if ( this.componentsByType.containsKey( type ) )
        {
            Pair<DiagramMetricComponent, MetricDimension> pair = this.componentsByType.get( type );
            component = pair.getLeft();
        }

        return component;
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
        if ( o == this )
        {
            return true;
        }

        if ( ! ( o instanceof final DiagramStatisticOuter v ) )
        {
            return false;
        }

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

        SortedSet<MetricDimension> cNames = new TreeSet<>();
        SortedSet<String> cQual = new TreeSet<>();
        Map<Pair<MetricDimension, String>, Integer> cInd = new HashMap<>();
        Map<DiagramComponentType, Pair<DiagramMetricComponent, MetricDimension>> byType
                = new EnumMap<>( DiagramComponentType.class );

        int index = 0;
        for ( DiagramStatisticComponent next : this.getData().getStatisticsList() )
        {
            String nameString = next.getMetric()
                                    .getName()
                                    .name();

            MetricDimension name = MetricDimension.valueOf( nameString );
            DiagramMetricComponent component = next.getMetric();
            DiagramComponentType type = component.getType();
            Pair<DiagramMetricComponent, MetricDimension> pair = Pair.of( component, name );
            byType.put( type, pair );
            cNames.add( name );
            String nextQualifier = next.getName();
            cQual.add( nextQualifier );
            cInd.put( Pair.of( name, nextQualifier ), index );
            index++;
        }

        this.componentNames = Collections.unmodifiableSortedSet( cNames );
        this.componentNameQualifiers = Collections.unmodifiableSortedSet( cQual );
        this.metricName = MetricConstants.valueOf( diagram.getMetric().getName().name() );
        this.componentsByType = byType; // Not exposed to mutation
        this.componentIndexes = cInd; // Not exposed to mutation
    }

}
