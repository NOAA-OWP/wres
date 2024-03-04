package wres.events.subscribe;

import java.io.Closeable;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

import wres.statistics.generated.Statistics;

/**
 * A consumer of statistics messages that may open resources to close on completion.
 * @author James Brown
 */
public interface StatisticsConsumer extends Function<Collection<Statistics>, Set<Path>>, Closeable
{
    /**
     * Returns a default implementation with no resources to close.
     * @param consumer the consumer
     * @return the wrapped consumer
     */
    static StatisticsConsumer getResourceFreeConsumer( Function<Collection<Statistics>, Set<Path>> consumer )
    {
        Objects.requireNonNull( consumer );

        return new StatisticsConsumer()
        {
            @Override
            public void close()
            {
                // No op
            }

            @Override
            public Set<Path> apply( Collection<Statistics> statistics )
            {
                return consumer.apply( statistics );
            }
        };
    }

}
