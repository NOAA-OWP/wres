package wres.io.retrieval.datashop;

import java.util.Objects;
import java.util.Optional;
import java.util.function.LongFunction;
import java.util.function.Supplier;
import java.util.stream.LongStream;
import java.util.stream.Stream;

/**
 * <p>An API for performing read operations on data objects of type <code>T</code>. Retrievers that read specific data 
 * types, <code>T</code>, from specific data stores should implement this interface.
 * 
 * @author james.brown@hydrosolved.com
 * @param <T> the type of object to retrieve
 */

public interface Retriever<T> extends Supplier<Stream<T>>
{

    /**
     * Reads an object with a prescribed identifier.
     * 
     * @param identifier the object identifier
     * @return the possible object
     * @throws DataAccessException if the data could not be accessed for whatever reason
     */

    Optional<T> get( long identifier );
    
    /**
     * Returns the identifiers associated with all objects.
     * 
     * @return the identifiers
     * @throws DataAccessException if the identifiers could not be accessed for whatever reason
     */

    LongStream getAllIdentifiers();
    
    /**
     * Reads a collection of objects, by unique identifier, into a stream. This implementation reads each object
     * sequentially. Implementations that benefit from reading multiple objects at once should override this default.
     * 
     * @param identifiers the stream of identifiers
     * @return a stream over the identified objects
     * @throws NullPointerException if the input is null
     */

    default Stream<T> get( LongStream identifiers )
    {
        Objects.requireNonNull( identifiers );

        // Create the supplier of objects from the object identifiers
        LongFunction<Optional<T>> supplier = this::get;

        // Map the object identifiers to objects in a stream view
        Stream<Optional<T>> optionals = identifiers.mapToObj( supplier );

        // Filter for objects that exist
        return optionals.filter( Optional::isPresent ).map( Optional::get );
    }
    
    /**
     * Reads all objects. This implementation reads each object sequentially. Implementations that benefit from reading 
     * multiple objects at once should override this default.
     * 
     * @return the possible object
     * @throws DataAccessException if the data could not be accessed for whatever reason
     */

    default Stream<T> get()
    {
        return this.get( this.getAllIdentifiers() );
    }
    
}
