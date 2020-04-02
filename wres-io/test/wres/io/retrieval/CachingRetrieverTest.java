package wres.io.retrieval;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Test;
import org.mockito.Mockito;

/**
 * Tests the {@link CachingRetriever}.
 * 
 * @author james.brown@hydrosolved.com
 */

public class CachingRetrieverTest
{

    @Test
    public void testGetPerformsRetrievalOnFirstCallAndSuppliesCacheOnSecondCall()
    {
        @SuppressWarnings( "unchecked" )
        Retriever<String> retriever = Mockito.mock( Retriever.class );

        // Mock two successive calls with different returns
        Mockito.when( retriever.get() )
               .thenReturn( Stream.of( "1" ) )
               .thenReturn( Stream.of( "2" ) );

        // Create with caching
        CachingRetriever<String> supplier = CachingRetriever.of( retriever );

        // First call returns a stream of "1"
        assertEquals( List.of( "1" ), supplier.get().collect( Collectors.toList() ) );

        // Second call returns the cached stream of "1" from the first call
        assertEquals( List.of( "1" ), supplier.get().collect( Collectors.toList() ) );
    }

}
