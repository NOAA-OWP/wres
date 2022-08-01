package wres.io.reading;

import java.net.URI;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.Ensemble;
import wres.datamodel.Ensemble.Labels;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.io.ingesting.PreIngestException;

/**
 * Utilities for file reading.
 * 
 * @author James Brown
 */

public class ReaderUtilities
{

    /** Logger. */
    private static Logger LOGGER = LoggerFactory.getLogger( ReaderUtilities.class );

    /**
     * Transform a single trace into a {@link TimeSeries} of {@link Double} values.
     * @param metadata The metadata of the timeseries.
     * @param trace The raw data to build a TimeSeries.
     * @param lineNumber The approximate location in the source.
     * @return The complete TimeSeries
     */

    public static TimeSeries<Double> transform( TimeSeriesMetadata metadata,
                                                SortedMap<Instant, Double> trace,
                                                int lineNumber )
    {
        Objects.requireNonNull( metadata );
        Objects.requireNonNull( trace );
        
        if ( trace.isEmpty() )
        {
            throw new IllegalArgumentException( "Cannot transform fewer than "
                                                + "one values into timeseries "
                                                + "with metadata "
                                                + metadata
                                                + " from line number "
                                                + lineNumber );
        }

        TimeSeries.Builder<Double> builder = new TimeSeries.Builder<>();
        builder.setMetadata( metadata );

        for ( Map.Entry<Instant, Double> events : trace.entrySet() )
        {
            Event<Double> event = Event.of( events.getKey(), events.getValue() );
            builder.addEvent( event );
        }

        return builder.build();
    }

    /**
     * Transform a map of traces into a {@link TimeSeries} of {@link Ensemble} values (flip it) but
     * also validate the density and valid datetimes of the ensemble prior.
     * @param metadata The metadata of the timeseries.
     * @param traces The raw data to build a TimeSeries.
     * @param lineNumber The approximate location in the source.
     * @param uri a uri to help with messaging
     * @return The complete TimeSeries
     * @throws IllegalArgumentException When fewer than two traces given.
     * @throws PreIngestException When ragged (non-dense) data given.
     */

    public static TimeSeries<Ensemble> transformEnsemble( TimeSeriesMetadata metadata,
                                                          SortedMap<String, SortedMap<Instant, Double>> traces,
                                                          int lineNumber,
                                                          URI uri )
    {
        int traceCount = traces.size();

        // See #62993-4: be lenient.
        if ( traceCount < 2 )
        {
            LOGGER.debug( "Found 'ensemble' data with fewer than two traces: {}",
                          traces );
        }

        Map<Instant, double[]> reshapedValues = new HashMap<>();
        Map.Entry<String, SortedMap<Instant, Double>> previousTrace = null;
        int i = 0;

        for ( Map.Entry<String, SortedMap<Instant, Double>> trace : traces.entrySet() )
        {
            SortedSet<Instant> theseInstants = new TreeSet<>( trace.getValue()
                                                                   .keySet() );

            if ( Objects.nonNull( previousTrace ) )
            {
                SortedSet<Instant> previousInstants = new TreeSet<>( previousTrace.getValue()
                                                                                  .keySet() );
                if ( !theseInstants.equals( previousInstants ) )
                {
                    throw new PreIngestException( "Cannot build ensemble from "
                                                  + uri
                                                  + " with data at or before "
                                                  + "line number "
                                                  + lineNumber
                                                  + " because the trace named "
                                                  + trace.getKey()
                                                  + " had these valid datetimes"
                                                  + ": "
                                                  + theseInstants
                                                  + " but previous trace named "
                                                  + previousTrace.getKey()
                                                  + " had different ones: "
                                                  + previousInstants
                                                  + " which is not allowed. All"
                                                  + " traces must be dense and "
                                                  + "match valid datetimes." );
                }
            }

            for ( Map.Entry<Instant, Double> event : trace.getValue()
                                                          .entrySet() )
            {
                Instant validDateTime = event.getKey();

                if ( !reshapedValues.containsKey( validDateTime ) )
                {
                    reshapedValues.put( validDateTime, new double[traceCount] );
                }

                double[] values = reshapedValues.get( validDateTime );
                values[i] = event.getValue();
            }

            previousTrace = trace;
            i++;
        }

        wres.datamodel.time.TimeSeries.Builder<Ensemble> builder =
                new wres.datamodel.time.TimeSeries.Builder<>();

        // Because the iteration is over a sorted map, assuming same order here.
        SortedSet<String> traceNamesSorted = new TreeSet<>( traces.keySet() );
        String[] traceNames = new String[traceNamesSorted.size()];
        traceNamesSorted.toArray( traceNames );
        Labels labels = Labels.of( traceNames );

        builder.setMetadata( metadata );

        for ( Map.Entry<Instant, double[]> events : reshapedValues.entrySet() )
        {
            Ensemble ensembleSlice = Ensemble.of( events.getValue(), labels );
            Event<Ensemble> ensembleEvent = Event.of( events.getKey(), ensembleSlice );
            builder.addEvent( ensembleEvent );
        }

        return builder.build();
    }

    /**
     * Do not construct.
     */

    private ReaderUtilities()
    {
    }
}
