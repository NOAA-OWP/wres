package wres.config.deserializers;

import java.time.Duration;

/**
 * A duration interval.
 *
 * @author James Brown
 */
public record DurationInterval( Duration left, Duration right, boolean reverse ) {}