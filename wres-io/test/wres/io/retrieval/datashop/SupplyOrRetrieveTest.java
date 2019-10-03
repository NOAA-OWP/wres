package wres.io.retrieval.datashop;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.stream.Stream;

import org.junit.Test;
import org.mockito.Mockito;

/**
 * Tests the {@link SupplyOrRetrieve}.
 * 
 * @author james.brown@hydrosolved.com
 */

public class SupplyOrRetrieveTest
{

    @Test
    public void testGetPerformsRetrievalOnFirstCallAndSuppliesCacheOnSecondCallWhenCacheIsEnabled()
    {
        @SuppressWarnings( "unchecked" )
        Retriever<String> retriever = Mockito.mock( Retriever.class );

        // Mock two successive calls with different returns
        Mockito.when( retriever.getAll() )
               .thenReturn( Stream.of( "1" ) )
               .thenReturn( Stream.of( "2" ) );

        // Create with caching
        SupplyOrRetrieve<String> supplier = SupplyOrRetrieve.of( retriever, true );

        // First call returns a stream of "1"
        assertEquals( List.of( "1" ), supplier.get() );

        // Second call returns the cached stream of "1" from the first call
        assertEquals( List.of( "1" ), supplier.get() );
    }

    @Test
    public void testGetPerformsRetrievalOnFirstCallAndRetrievalOnSecondCallWhenCacheIsNotEnabled()
    {
        @SuppressWarnings( "unchecked" )
        Retriever<String> retriever = Mockito.mock( Retriever.class );

        // Mock two successive calls with different returns
        Mockito.when( retriever.getAll() )
               .thenReturn( Stream.of( "1" ) )
               .thenReturn( Stream.of( "2" ) );

        // Create without caching
        SupplyOrRetrieve<String> supplier = SupplyOrRetrieve.of( retriever, false );

        // First call returns a stream of "1"
        assertEquals( List.of( "1" ), supplier.get() );

        // Second call returns the cached stream of "2"
        assertEquals( List.of( "2" ), supplier.get() );
    }

}
