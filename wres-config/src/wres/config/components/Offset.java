package wres.config.components;

/**
 * The offset value to apply to each dataset orientation. For example, a datum offset.
 * @param left the left-oriented offset
 * @param right the right-oriented offset
 * @param baseline the baseline-oriented offset
 */
public record Offset( double left, double right, double baseline )
{
}
