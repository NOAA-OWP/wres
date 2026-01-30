package wres.reading.nwis.iv.response.query;

import java.io.Serial;
import java.io.Serializable;

import lombok.Getter;
import lombok.Setter;

/**
 * A query time parameter.
 */

@Setter
@Getter
public class QueryTimeParameter implements Serializable
{
    @Serial
    private static final long serialVersionUID = 8007018441738383263L;

    /** Start time. */
    private String beginDateTime;

    /** End time. */
    private String endDateTime;
}
