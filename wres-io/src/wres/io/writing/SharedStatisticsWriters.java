package wres.io.writing;

import java.io.Closeable;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

import org.apache.commons.lang3.tuple.Pair;

import wres.config.generated.DestinationType;
import wres.datamodel.MetricConstants.StatisticType;
import wres.io.writing.netcdf.NetcdfOutputWriter;
import wres.io.writing.protobuf.ProtobufWriter;

/**
 * Contains a set of shared writers for managing the shared state of statistical outputs that are written 
 * incrementally.  
 * 
 * @author james.brown@hydrosolved.com
 */
public class SharedStatisticsWriters implements Closeable, Supplier<Set<Path>>
{

    /**
     * Netcdf writer.
     */

    private final NetcdfOutputWriter netcdfOutputWriter;

    /**
     * Consumer of protobufs.
     */

    private final ProtobufWriter protobufWriter;

    /**
     * Set of types for which writers are available.
     */

    private final Set<Pair<DestinationType, StatisticType>> storedTypes;

    /**
     * Returns <code>true</code> if a writer is available for the specified types, otherwise <code>false</code>.
     * 
     * @param type the data type
     * @param format the output format
     * @return true if a writer is available for the input types, otherwise false
     * @throws NullPointerException if either input is null
     */

    public boolean contains( StatisticType type, DestinationType format )
    {
        Objects.requireNonNull( type, "Specify a non-null type to test." );
        Objects.requireNonNull( format, "Specify a non-null format to test." );

        // Protobufs accept all types
        if ( format == DestinationType.PROTOBUF && Objects.nonNull( this.protobufWriter ) )
        {
            return true;
        }

        return this.storedTypes.contains( Pair.of( format, type ) );
    }

    /**
     * Returns <code>true</code> if a writer is available for the specified format, otherwise <code>false</code>.
     * 
     * @param format the output format
     * @return true if a writer is available for the input types, otherwise false
     * @throws NullPointerException if either input is null
     */

    public boolean contains( DestinationType format )
    {
        Objects.requireNonNull( format, "Specify a non-null format to test." );

        return this.storedTypes.stream().anyMatch( next -> next.getLeft() == format );
    }

    /**
     * @return the netcdf writer
     */

    public NetcdfOutputWriter getNetcdfOutputWriter()
    {
        return this.netcdfOutputWriter;
    }

    /**
     * @return the protobuf writer
     */

    public ProtobufWriter getProtobufWriter()
    {
        return this.protobufWriter;
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

        /**
         * Consumer of protobufs.
         */

        private ProtobufWriter protobufWriter;

        /**
         * Sets the netcdf writer.
         * @param netcdfOutputWriter the netcdf writer
         * @return this builder
         */

        public SharedWritersBuilder setNetcdfOutputWriter( NetcdfOutputWriter netcdfOutputWriter )
        {
            this.netcdfOutputWriter = netcdfOutputWriter;

            return this;
        }

        /**
         * Sets the protobuf writer.
         * @param protobufWriter the protobuf writer
         * @return this builder
         */

        public SharedWritersBuilder setProtobufWriter( ProtobufWriter protobufWriter )
        {
            this.protobufWriter = protobufWriter;

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
        this.protobufWriter = builder.protobufWriter;

        // Register the stored types
        Set<Pair<DestinationType, StatisticType>> localTypes = new HashSet<>();

        if ( Objects.nonNull( this.getNetcdfOutputWriter() ) )
        {
            localTypes.add( Pair.of( DestinationType.NETCDF, StatisticType.DOUBLE_SCORE ) );
        }

        if ( Objects.nonNull( this.getProtobufWriter() ) )
        {
            localTypes.add( Pair.of( DestinationType.PROTOBUF, null ) );
        }

        this.storedTypes = Collections.unmodifiableSet( localTypes );
    }

    @Override
    public Set<Path> get()
    {
        Set<Path> paths = new HashSet<>( 1 );

        if ( this.contains( DestinationType.NETCDF ) )
        {
            Set<Path> outputWriterPaths = this.getNetcdfOutputWriter().get();
            paths.addAll( outputWriterPaths );
        }
        
        if ( this.contains( DestinationType.PROTOBUF ) )
        {
            Set<Path> outputWriterPaths = this.getProtobufWriter().get();
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
