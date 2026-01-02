package wres.config.serializers;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import tools.jackson.core.JsonGenerator;
import tools.jackson.databind.ValueSerializer;
import tools.jackson.databind.SerializationContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.DeclarationFactory;
import wres.config.DeclarationUtilities;
import wres.statistics.generated.SummaryStatistic;

/**
 * Serializes a {@link SummaryStatistic}.
 * @author James Brown
 */
public class SummaryStatisticsSerializer extends ValueSerializer<Set<SummaryStatistic>>
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( SummaryStatisticsSerializer.class );

    @Override
    public void serialize( Set<SummaryStatistic> summaryStatistics,
                           JsonGenerator writer,
                           SerializationContext serializers )
    {
        if ( Objects.nonNull( summaryStatistics )
             && !summaryStatistics.isEmpty() )
        {
            LOGGER.debug( "Discovered {} summary statistics.", summaryStatistics.size() );

            Set<SummaryStatistic.StatisticDimension> dimensions =
                    summaryStatistics.stream()
                                     .flatMap( s -> s.getDimensionList()
                                                     .stream() )
                                     .collect( Collectors.toCollection( TreeSet::new ) );

            if ( dimensions.isEmpty() )
            {
                this.writeSummaryStatistics( summaryStatistics, writer );
            }
            else
            {
                // Statistics
                writer.writeStartObject();

                writer.writeName( "statistics" );
                this.writeSummaryStatistics( summaryStatistics, writer );

                // dimensions
                writer.writeName( "dimensions" );

                writer.writeStartArray();
                for ( SummaryStatistic.StatisticDimension dimension : dimensions )
                {
                    String name = DeclarationUtilities.fromEnumName( dimension.name() );
                    writer.writeString( name );
                }
                writer.writeEndArray();

                writer.writeEndObject();
            }
        }
    }

    @Override
    public boolean isEmpty( SerializationContext provider, Set<SummaryStatistic> summaryStatistics )
    {
        return Objects.isNull( summaryStatistics ) || summaryStatistics.isEmpty();
    }

    /**
     * Writes a set of {@link SummaryStatistic}.
     * @param summaryStatistics the summary statistics
     * @param writer the writer
     */
    private void writeSummaryStatistics( Set<SummaryStatistic> summaryStatistics, JsonGenerator writer )
    {
        writer.writeStartArray();

        // Filter out any quantiles, which must be aggregated together
        SortedSet<Double> probabilities = summaryStatistics.stream()
                                                           .filter( s -> s.getStatistic()
                                                                         == SummaryStatistic.StatisticName.QUANTILE )
                                                           .map( SummaryStatistic::getProbability )
                                                           .collect( Collectors.toCollection( TreeSet::new ) );

        // Filter out any statistics without parameters and write these
        Set<SummaryStatistic.StatisticName> statisticsWithoutParameters =
                summaryStatistics.stream()
                                 .map( SummaryStatistic::getStatistic )
                                 .filter( s -> s != SummaryStatistic.StatisticName.QUANTILE
                                               && s != SummaryStatistic.StatisticName.HISTOGRAM )
                                 .collect( Collectors.toCollection( TreeSet::new ) );

        for ( SummaryStatistic.StatisticName name : statisticsWithoutParameters )
        {
            String nameString = DeclarationUtilities.fromEnumName( name.name() );
            writer.writeString( nameString );
        }

        // Write the statistics with parameters
        this.writeQuantiles( probabilities, writer );
        this.writeHistogram( summaryStatistics, writer );

        writer.writeEndArray();
    }

    /**
     * Writes the quantile statistics.
     * @param probabilities the probabilities
     * @param writer the writer
     */

    private void writeQuantiles( Set<Double> probabilities, JsonGenerator writer )
    {
        if ( !probabilities.isEmpty() )
        {
            if ( !probabilities.equals( DeclarationFactory.DEFAULT_QUANTILES ) )
            {
                writer.writeStartObject();
                writer.writeStringProperty( "name", "quantiles" );
                writer.writeName( "probabilities" );

                double[] p = probabilities.stream()
                                          .mapToDouble( d -> d )
                                          .toArray();
                writer.writeArray( p, 0, p.length );

                writer.writeEndObject();
            }
            else
            {
                writer.writeString( "quantiles" );
            }
        }
    }

    /**
     * Writes a histogram, if present.
     * @param summaryStatistics the probabilities
     * @param writer the writer
     */

    private void writeHistogram( Set<SummaryStatistic> summaryStatistics, JsonGenerator writer )
    {
        // Write a histogram, if present
        Optional<SummaryStatistic> possibleHistogram =
                summaryStatistics.stream()
                                 .filter( s -> s.getStatistic() == SummaryStatistic.StatisticName.HISTOGRAM )
                                 .findFirst();
        if ( possibleHistogram.isPresent() )
        {
            SummaryStatistic histogram = possibleHistogram.get();
            if ( histogram.getHistogramBins() > 0
                 && histogram.getHistogramBins() != DeclarationFactory.DEFAULT_HISTOGRAM_BINS )
            {
                writer.writeStartObject();

                writer.writeStringProperty( "name", "histogram" );
                writer.writeNumberProperty( "bins", histogram.getHistogramBins() );

                writer.writeEndObject();
            }
            else
            {
                writer.writeString( "histogram" );
            }
        }
    }

}
