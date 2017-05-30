package wres.util;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;

/**
 * A print stream designed to eat output; cross platform solution analogous to outputting to /dev/null
 * @author Christopher Tubbs
 *
 */
public class NullPrintStream extends PrintStream
{
    public NullPrintStream()
    {
        super(new OutputStream(){

            @Override
            public void write(int b) throws IOException
            {
                // Do nothing; this stream is designed to eat streamed information
            }
            
        });
    }
    
    public static PrintStream get()
    {
        return new NullPrintStream();
    }
    
}
