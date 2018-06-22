package wres.util.functional;

@FunctionalInterface
public interface ExceptionalConsumer<U, V extends Exception>
{
    void accept(U value) throws V;
}
