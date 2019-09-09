package wres.io.retrieval.dao;

import java.util.Objects;
import java.util.Optional;
import java.util.function.LongFunction;
import java.util.stream.LongStream;
import java.util.stream.Stream;

/**
 * <p>An API for performing (C)reate (R)ead (U)update and (D)elete operations on data objects of type <code>T</code>. 
 * In general, the WRES does *not* encourage mutation and, thus, support (U)pdate. Data Access Objects (DAOs) that
 * perform CRUD operations on specific data types, <code>T</code>, in specific data stores should implement this 
 * interface.
 * 
 * @author james.brown@hydrosolved.com
 * @param <T> the type of object
 */

public interface WresDAO<T>
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
     * Reads a collection of objects, by unique identifier, into a stream.
     * 
     * @param identifiers the stream of identifiers
     * @return a stream over the identified objects
     * @throws NullPointerException if the input is null
     */

    default Stream<T> get( LongStream identifiers )
    {
        Objects.nonNull( identifiers );

        // Create the supplier of objects from the object identifiers
        LongFunction<Optional<T>> supplier = this::get;

        // Map the object identifiers to objects in a stream view
        Stream<Optional<T>> optionals = identifiers.mapToObj( supplier );

        // Filter for objects that exist
        return optionals.filter( Optional::isPresent ).map( Optional::get );
    }
    
    /**
     * Reads all objects.
     * @return the possible object
     * @throws DataAccessException if the data could not be accessed for whatever reason
     */

    default Stream<T> getAll()
    {
        return this.get( this.getAllIdentifiers() );
    }
    
}
