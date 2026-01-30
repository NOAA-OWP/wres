package wres.reading;

import java.time.Instant;
import java.util.SortedSet;
import java.util.function.Supplier;

import org.apache.commons.lang3.tuple.Pair;

/**
 * An interface for generating a sequence of time chunks to use when reading data in a time-chunked pattern.
 *
 * @author James Brown
 */

public interface TimeChunker extends Supplier<SortedSet<Pair<Instant, Instant>>>
{
    /**
     * An enumeration of fixed chunking strategies.
     */

    enum ChunkingStrategy
    {
        /** Calendar years on the UTC timeline. */
        YEAR_RANGES,

        /** A simple range that contains all time. */
        SIMPLE_RANGE
    }
}
