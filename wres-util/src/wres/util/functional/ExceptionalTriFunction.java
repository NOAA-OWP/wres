package wres.util.functional;

import java.io.IOException;

@FunctionalInterface
public interface ExceptionalTriFunction<U, V, W, X, Y extends Exception>
{
    X call(U u, V v, W w) throws Y;
}
