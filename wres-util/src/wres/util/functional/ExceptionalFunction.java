package wres.util.functional;

@FunctionalInterface
public interface ExceptionalFunction<U, V, W extends Exception>
{
    V call(U u) throws W;
}
