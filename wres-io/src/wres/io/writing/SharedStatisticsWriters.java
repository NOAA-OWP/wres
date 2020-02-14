package wres.io.writing;

import java.io.Closeable;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.commons.lang3.tuple.Pair;

import wres.config.generated.DestinationType;
import wres.datamodel.MetricConstants.StatisticType;
import wres.datamodel.statistics.DoubleScoreStatistic;
import wres.datamodel.statistics.ListOfStatistics;
import wres.io.writing.netcdf.NetcdfOutputWriter;

/**
 * Contains a set of shared writers for managing the shared state of statistical outputs that are written 
 * incrementally.  
 * 
 * @author james.brown@hydrosolved.com
 */
public class SharedStatisticsWriters implements Closeable,
                                      Consumer<ListOfStatistics<DoubleScoreStatistic>>,
                                      Supplier<Set<Path>>
{

    private final NetcdfOutputWriter netcdfOutputWriter;

    /**
     * Set of types for which writers are available.
     */
    
    private final Set<Pair<DestinationType,StatisticType>> storedTypes;
    
    /**
     * Returns <code>true</code> if a writer is available for the specified types, otherwise <code>false</code>.
     * 
     * @param type the data type
     * @param format the output format
     * @return true if a writer is available for the input types, otherwise false
     * @throws NullPointerException if either input is null
     */

    public boolean contains( StatisticType type, DestinationType format  )
    {
        Objects.requireNonNull( type, "Specify a non-null type to test." );
        
        Objects.requireNonNull( format, "Specify a non-null format to test." );
        
        return storedTypes.contains( Pair.of( format, type ) );
    }


    NetcdfOutputWriter getNetcdfOutputWriter()
    {
        return this.netcdfOutputWriter;
    }

    /**
     * Use a builder to add writers.
     */
    
    public static class SharedWritersBuilder 
    {

        /**
         * Instance of a {@link NetcdfOutputWriter}
         */

        private NetcdfOutputWriter netcdfOutputWriter;


        public SharedWritersBuilder setNetcdfOutputWriter( NetcdfOutputWriter netcdfOutputWriter )
        {
            this.netcdfOutputWriter = netcdfOutputWriter;
            return this;
        }

        /**
         * Return an instance of a {@link SharedStatisticsWriters}.
         * 
         * @return a container for sharing writers
         */
        
        public SharedStatisticsWriters build()
        {
            return new SharedStatisticsWriters( this );
        }
    }
    
    
    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     */

    private SharedStatisticsWriters( SharedWritersBuilder builder )
    {
        // Set
        this.netcdfOutputWriter = builder.netcdfOutputWriter;
        
        // Register the stored types
        Set<Pair<DestinationType,StatisticType>> localTypes = new HashSet<>();
        
        if( Objects.nonNull( this.getNetcdfOutputWriter() ) )
        {
            localTypes.add( Pair.of( DestinationType.NETCDF, StatisticType.DOUBLE_SCORE ) );
        }
        
        this.storedTypes = Collections.unmodifiableSet( localTypes );
    }


    /**
     * Pass-through any metrics to underlying writers.
     *
     * @param metricOutput metrics to write
     */

    @Override
    public void accept( ListOfStatistics<DoubleScoreStatistic> metricOutput )
    {
        if ( Objects.nonNull( this.netcdfOutputWriter ) )
        {
            this.netcdfOutputWriter.accept( metricOutput );
        }
    }

    @Override
    public Set<Path> get()
    {
        Set<Path> paths = new HashSet<>( 1 );

        if ( Objects.nonNull( this.getNetcdfOutputWriter() ) )
        {
            Set<Path> outputWriterPaths = this.getNetcdfOutputWriter().get();
            paths.addAll( outputWriterPaths );
        }

        return Collections.unmodifiableSet( paths );
    }

    /**
     * Closes resources managed by SharedWriters
     */
    @Override
    public void close()
    {
        if ( this.getNetcdfOutputWriter() != null )
        {
            this.getNetcdfOutputWriter().close();
        }
    }
}
