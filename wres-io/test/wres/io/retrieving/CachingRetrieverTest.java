package wres.io.retrieving;

import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Tests the {@link CachingRetriever}.
 * 
 * @author James Brown
 */

class CachingRetrieverTest
{
    @Test
    void testGetPerformsRetrievalOnFirstCallAndSuppliesCacheOnSecondCall()
    {
        @SuppressWarnings( "unchecked" )
        Retriever<String> retriever = Mockito.mock( Retriever.class );

        // Mock two successive calls with different returns
        Mockito.when( retriever.get() )
               .thenReturn( Stream.of( "1" ) )
               .thenReturn( Stream.of( "2" ) );

        // Create with caching
        Supplier<Stream<String>> supplier = CachingRetriever.of( retriever );

        // First call returns a stream of "1"
        Assertions.assertEquals( List.of( "1" ), supplier.get().collect( Collectors.toList() ) );

        // Second call returns the cached stream of "1" from the first call
        Assertions.assertEquals( List.of( "1" ), supplier.get().collect( Collectors.toList() ) );
    }

}
