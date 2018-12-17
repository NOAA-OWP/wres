package wres.util.functional;

@FunctionalInterface
public interface ExceptionalFunction<U, V, W extends Throwable>
{
    V call(U u) throws W;
}
