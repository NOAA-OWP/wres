package wres.util.functional;

@FunctionalInterface
public interface ExceptionalPredicate<U, V extends Throwable>
{
    boolean test( U value ) throws V;
}
