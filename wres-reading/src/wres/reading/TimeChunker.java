package wres.reading;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;
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

        /** Weekly ranges on the UTC timeline from Sunday 0Z to Sunday 0Z. */
        WEEK_RANGES,

        /** A simple range that contains all time. */
        SIMPLE_RANGE
    }

    /**
     * Subtracts precisely one of the prescribed units from the right bookend of every interval, except the last
     * interval.
     *
     * @return the non-overlapping intervals
     * @throws IllegalArgumentException if the unit is null
     */

    default SortedSet<Pair<Instant, Instant>> getNonOverlapping( ChronoUnit unit )
    {
        Objects.requireNonNull( unit );

        SortedSet<Pair<Instant, Instant>> raw = this.get();

        SortedSet<Pair<Instant, Instant>> adjusted = new TreeSet<>();

        int currentIndex = 0;
        int lastIndex = raw.size() - 1;

        for ( Pair<Instant, Instant> current : raw )
        {
            if ( currentIndex < lastIndex )
            {
                Instant adjustedRight = current.getRight()
                                               .minus( 1, unit );
                adjusted.add( Pair.of( current.getLeft(), adjustedRight ) );
            }
            else
            {
                adjusted.add( current );
            }
            currentIndex++;
        }

        return Collections.unmodifiableSortedSet( adjusted );
    }
}
