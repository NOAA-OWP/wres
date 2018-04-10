package wres.io.utilities;

import java.io.IOException;

@FunctionalInterface
public interface IOExceptionalConsumer<U>
{
    void accept(U value) throws IOException;
}
