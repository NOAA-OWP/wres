package wres.datamodel;

import java.time.LocalDateTime;

/**
 * Data in this object varies with time, there are multiple datetimes.
 * @author jesse
 *
 */
public interface TimeMany
{
    // I vary in time
    /** @return All times with some data, in GMT (may be ordered?) */
    public LocalDateTime[] getDateTimes();
}
