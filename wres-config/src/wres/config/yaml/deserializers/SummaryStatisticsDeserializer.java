package wres.config.yaml.deserializers;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.SortedSet;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectReader;

import wres.config.yaml.DeclarationFactory;
import wres.config.yaml.DeclarationUtilities;
import wres.statistics.generated.SummaryStatistic;

/**
 * Custom deserializer for a set of {@link SummaryStatistic}.
 *
 * @author James Brown
 */
public class SummaryStatisticsDeserializer
        extends JsonDeserializer<Set<SummaryStatistic>>
{
    @Override
    public Set<SummaryStatistic> deserialize( JsonParser jp,
                                              DeserializationContext context )
            throws IOException
    {
        Objects.requireNonNull( jp );

        ObjectReader mapper = ( ObjectReader ) jp.getCodec();
        JsonNode node = mapper.readTree( jp );

        // Preserve insertion order
        Set<SummaryStatistic> summaryStatistics = new LinkedHashSet<>();

        // A template builder
        SummaryStatistic.Builder template = SummaryStatistic.newBuilder();

        // Plain array of statistics
        if ( node.isArray() )
        {
            this.readStatisticsFromArrayNode( node, summaryStatistics, mapper, template );
        }
        // Qualified statistics
        else if ( node.isObject()
                  && node.has( "statistics" ) )
        {
            JsonNode metricsNode = node.get( "statistics" );
            this.readStatisticsFromArrayNode( metricsNode, summaryStatistics, mapper, template );

            // Create one summary statistic for each qualified dimension. The logic surrounding how dimensions are
            // combined is deferred until evaluation time, i.e., keep things simple at deserialization
            if ( node.has( "dimensions" ) )
            {
                JsonNode dimensionsNode = node.get( "dimensions" );
                int count = dimensionsNode.size();
                Set<SummaryStatistic> dimensionedStatistics = new LinkedHashSet<>();
                for ( int i = 0; i < count; i++ )
                {
                    String nodeText = dimensionsNode.get( i )
                                                    .asText();

                    String enumName = DeclarationUtilities.toEnumName( nodeText );

                    // Declared in the plural, build in the singular.
                    if ( enumName.equals( "FEATURE_GROUPS" ) )
                    {
                        enumName = "FEATURE_GROUP";
                    }

                    SummaryStatistic.StatisticDimension dimension =
                            SummaryStatistic.StatisticDimension.valueOf( enumName );

                    summaryStatistics.forEach( n -> dimensionedStatistics.add( n.toBuilder()
                                                                                .addDimension( dimension )
                                                                                .build() ) );
                }

                // Add any extras to the overall set of statistics
                summaryStatistics = dimensionedStatistics;
            }
        }

        return Collections.unmodifiableSet( summaryStatistics );
    }

    /**
     * Reads an array of summary statistics into the supplied container.
     *
     * @param arrayNode the array of statistics
     * @param summaryStatistics the container to populate
     * @param mapper the object mapper
     * @param template the template builder to use
     * @throws IOException if the statistic could not be read for any reason
     */

    private void readStatisticsFromArrayNode( JsonNode arrayNode,
                                              Set<SummaryStatistic> summaryStatistics,
                                              ObjectReader mapper,
                                              SummaryStatistic.Builder template ) throws IOException
    {
        int count = arrayNode.size();
        for ( int i = 0; i < count; i++ )
        {
            JsonNode nextNode = arrayNode.get( i );
            SummaryStatistic.Builder nextBuilder = SummaryStatistic.newBuilder( template.build() );
            Set<SummaryStatistic> statistics = this.getSummaryStatistics( nextNode,
                                                                          mapper,
                                                                          nextBuilder );
            summaryStatistics.addAll( statistics );
        }
    }

    /**
     * Reads one or more summary statistics from a node.
     * @param nextNode the node to read
     * @param mapper the object mapper
     * @param startFrom an optional builder to start from
     * @return the summary statistics
     * @throws IOException if the statistics could not be read for any reason
     */
    private Set<SummaryStatistic> getSummaryStatistics( JsonNode nextNode,
                                                        ObjectReader mapper,
                                                        SummaryStatistic.Builder startFrom ) throws IOException
    {
        Set<SummaryStatistic> summaryStatistics = new LinkedHashSet<>();

        // Summary statistic with parameters
        if ( nextNode.isObject()
             && nextNode.has( "name" ) )
        {
            JsonNode nameNode = nextNode.get( "name" );
            String nodeText = nameNode.asText();
            SummaryStatistic.Builder builder = this.getNamedStatisticBuilder( nodeText,
                                                                              startFrom );

            if ( nextNode.has( "bins" ) )
            {
                JsonNode binsNode = nextNode.get( "bins" );
                int bins = binsNode.asInt();
                builder.setHistogramBins( bins );
                summaryStatistics.add( builder.build() );
            }
            else if ( nextNode.has( "probabilities" ) )
            {
                JsonNode probabilitiesNode = nextNode.get( "probabilities" );
                double[] probabilities = mapper.readValue( probabilitiesNode, double[].class );
                Arrays.stream( probabilities )
                      .forEach( n -> summaryStatistics.add( builder.setProbability( n )
                                                                   .build() ) );
            }
            else
            {
                switch ( builder.getStatistic() )
                {
                    // Quantiles declared without probabilities
                    case QUANTILE -> this.addDefaultQuantiles( summaryStatistics, builder );
                    // Histogram without bins
                    case HISTOGRAM ->
                            summaryStatistics.add( builder.setHistogramBins( DeclarationFactory.DEFAULT_HISTOGRAM_BINS )
                                                          .build() );
                    default -> summaryStatistics.add( builder.build() );
                }
            }
        }
        // Summary statistic without parameters
        else
        {
            String nodeText = nextNode.asText();
            SummaryStatistic.Builder builder = this.getNamedStatisticBuilder( nodeText, startFrom );

            switch ( builder.getStatistic() )
            {
                // Quantiles declared without probabilities
                case QUANTILE -> this.addDefaultQuantiles( summaryStatistics, builder );
                // Histogram without bins
                case HISTOGRAM ->
                        summaryStatistics.add( builder.setHistogramBins( DeclarationFactory.DEFAULT_HISTOGRAM_BINS )
                                                      .build() );
                default -> summaryStatistics.add( builder.build() );
            }
        }

        return Collections.unmodifiableSet( summaryStatistics );
    }

    /**
     * Creates a summary statistic builder for the named statistic.
     * @param nodeText the summary statistic name, as reported by the JSON node
     * @param startFrom an optional builder to start from
     * @return the builder
     */
    private SummaryStatistic.Builder getNamedStatisticBuilder( String nodeText,
                                                               SummaryStatistic.Builder startFrom )
    {
        String enumName = DeclarationUtilities.toEnumName( nodeText );

        // Declared in the plural, build in the singular
        if ( "QUANTILES".equals( enumName ) )
        {
            enumName = "QUANTILE";
        }

        SummaryStatistic.StatisticName name = SummaryStatistic.StatisticName.valueOf( enumName );

        return startFrom.setStatistic( name );
    }

    /**
     * Adds default quantiles to the supplied container.
     * @param summaryStatistics the summary statistics to which the default quantiles should be added
     * @param builder the template builder
     */

    private void addDefaultQuantiles( Set<SummaryStatistic> summaryStatistics, SummaryStatistic.Builder builder )
    {
        SortedSet<Double> probabilities = DeclarationFactory.DEFAULT_QUANTILES;
        probabilities.forEach( n -> summaryStatistics.add( SummaryStatistic.newBuilder( builder.build() )
                                                                           .setProbability( n )
                                                                           .build() ) );
    }
}

