package wres.config.serializers;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.DeclarationFactory;
import wres.config.DeclarationUtilities;
import wres.statistics.generated.SummaryStatistic;

/**
 * Serializes a {@link SummaryStatistic}.
 * @author James Brown
 */
public class SummaryStatisticsSerializer extends JsonSerializer<Set<SummaryStatistic>>
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( SummaryStatisticsSerializer.class );

    @Override
    public void serialize( Set<SummaryStatistic> summaryStatistics,
                           JsonGenerator writer,
                           SerializerProvider serializers ) throws IOException
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

                writer.writeFieldName( "statistics" );
                this.writeSummaryStatistics( summaryStatistics, writer );

                // dimensions
                writer.writeFieldName( "dimensions" );

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
    public boolean isEmpty( SerializerProvider provider, Set<SummaryStatistic> summaryStatistics )
    {
        return Objects.isNull( summaryStatistics ) || summaryStatistics.isEmpty();
    }

    /**
     * Writes a set of {@link SummaryStatistic}.
     * @param summaryStatistics the summary statistics
     * @param writer the writer
     * @throws IOException if the summary statistics could not be written for any reason
     */
    private void writeSummaryStatistics( Set<SummaryStatistic> summaryStatistics, JsonGenerator writer )
            throws IOException
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
     * @throws IOException if the writing failed for any reason
     */

    private void writeQuantiles( Set<Double> probabilities, JsonGenerator writer ) throws IOException
    {
        if ( !probabilities.isEmpty() )
        {
            if ( !probabilities.equals( DeclarationFactory.DEFAULT_QUANTILES ) )
            {
                writer.writeStartObject();
                writer.writeFieldName( "name" );
                writer.writeString( "quantiles" );
                writer.writeFieldName( "probabilities" );

                // Use flow style for the array if possible
                if ( writer instanceof CustomGenerator custom )
                {
                    custom.setFlowStyleOn();
                }

                double[] p = probabilities.stream()
                                          .mapToDouble( d -> d )
                                          .toArray();
                writer.writeArray( p, 0, p.length );

                writer.writeEndObject();

                // Turn off flow style
                if ( writer instanceof CustomGenerator custom )
                {
                    custom.setFlowStyleOff();
                }
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
     * @throws IOException if the writing failed for any reason
     */

    private void writeHistogram( Set<SummaryStatistic> summaryStatistics, JsonGenerator writer ) throws IOException
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

                writer.writeFieldName( "name" );
                writer.writeString( "histogram" );
                writer.writeFieldName( "bins" );
                writer.writeNumber( histogram.getHistogramBins() );

                writer.writeEndObject();
            }
            else
            {
                writer.writeString( "histogram" );
            }
        }
    }

}
