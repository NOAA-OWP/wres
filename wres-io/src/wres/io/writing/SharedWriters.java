package wres.io.writing;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.commons.lang3.tuple.Pair;

import wres.config.generated.DestinationType;
import wres.datamodel.MetricConstants.StatisticGroup;
import wres.datamodel.statistics.DoubleScoreStatistic;
import wres.datamodel.statistics.ListOfStatistics;
import wres.io.writing.netcdf.NetcdfDoubleScoreWriter;
import wres.io.writing.netcdf.NetcdfOutputWriter;

/**
 * Contains a set of shared writers. This is useful for managing the shared state of outputs that are written 
 * incrementally.  
 * 
 * @author james.brown@hydrosolved.com
 */
public class SharedWriters implements Closeable,
                                      Consumer<ListOfStatistics<DoubleScoreStatistic>>,
                                      Supplier<Set<Path>>
{

    /**
     * Instance of a {@link NetcdfDoubleScoreWriter}
     */
    
    private final NetcdfDoubleScoreWriter netcdfDoublescoreWriter;

    private final NetcdfOutputWriter netcdfOutputWriter;

    /**
     * Set of types for which writers are available.
     */
    
    private final Set<Pair<DestinationType,StatisticGroup>> storedTypes;
    
    /**
     * Returns <code>true</code> if a writer is available for the specified types, otherwise <code>false</code>.
     * 
     * @param type the data type
     * @param format the output format
     * @return true if a writer is available for the input types, otherwise false
     * @throws NullPointerException if either input is null
     */

    public boolean contains( StatisticGroup type, DestinationType format  )
    {
        Objects.requireNonNull( type, "Specify a non-null type to test." );
        
        Objects.requireNonNull( format, "Specify a non-null format to test." );
        
        return storedTypes.contains( Pair.of( format, type ) );
    }
    
    /**
     * Returns a {@link NetcdfDoubleScoreWriter} or null.
     * 
     * @return a writer or null
     */
    
    public NetcdfDoubleScoreWriter getNetcdfDoubleScoreWriter()
    {
        return netcdfDoublescoreWriter;
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
         * Instance of a {@link NetcdfDoubleScoreWriter}
         */

        private NetcdfDoubleScoreWriter netcdfDoublescoreWriter;

        /**
         * Instance of a {@link NetcdfDoubleScoreWriter}
         */

        private NetcdfOutputWriter netcdfOutputWriter;

        /**
         * Sets a {@link NetcdfDoubleScoreWriter}.
         * 
         * @param netcdfDoublescoreWriter the writer
         * @return the builder
         */
        
        public SharedWritersBuilder setNetcdfDoublescoreWriter( NetcdfDoubleScoreWriter netcdfDoublescoreWriter )
        {
            this.netcdfDoublescoreWriter = netcdfDoublescoreWriter;
            return this;
        }

        public SharedWritersBuilder setNetcdfOutputWriter( NetcdfOutputWriter netcdfOutputWriter )
        {
            this.netcdfOutputWriter = netcdfOutputWriter;
            return this;
        }

        /**
         * Return an instance of a {@link SharedWriters}.
         * 
         * @return a container for sharing writers
         */
        
        public SharedWriters build()
        {
            return new SharedWriters( this );
        }
    }
    
    
    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     */

    private SharedWriters( SharedWritersBuilder builder )
    {
        // Set
        this.netcdfDoublescoreWriter = builder.netcdfDoublescoreWriter;
        this.netcdfOutputWriter = builder.netcdfOutputWriter;
        
        // Register the stored types
        Set<Pair<DestinationType,StatisticGroup>> localTypes = new HashSet<>();
        
        if( Objects.nonNull( this.netcdfDoublescoreWriter )
            || Objects.nonNull( this.getNetcdfOutputWriter() ) )
        {
            localTypes.add( Pair.of( DestinationType.NETCDF, StatisticGroup.DOUBLE_SCORE ) );
        }
        
        this.storedTypes = Collections.unmodifiableSet( localTypes );
    }


    /**
     * Pass-through any metrics to underlying writers.
     *
     * At the moment, only DoubleScoreOutput but in future could be generic.
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
    public void close() throws IOException
    {
        if ( this.getNetcdfDoubleScoreWriter() != null )
        {
            this.getNetcdfDoubleScoreWriter().close();
        }

        if ( this.getNetcdfOutputWriter() != null )
        {
            this.getNetcdfOutputWriter().close();
        }
    }
}
