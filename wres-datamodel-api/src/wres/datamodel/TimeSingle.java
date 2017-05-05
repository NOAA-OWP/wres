package wres.datamodel;

import java.time.LocalDateTime;

/**
 * An object associated with only a single datetime.
 * @author jesse
 *
 */
public interface TimeSingle
{
    /** @return The single datetime associated with other data, in GMT */
    public LocalDateTime getDateTime();
}
