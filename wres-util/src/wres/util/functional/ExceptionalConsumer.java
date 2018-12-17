package wres.util.functional;

@FunctionalInterface
public interface ExceptionalConsumer<U, V extends Throwable>
{
    void accept(U value) throws V;
}
