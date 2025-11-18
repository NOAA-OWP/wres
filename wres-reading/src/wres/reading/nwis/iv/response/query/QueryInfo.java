package wres.reading.nwis.iv.response.query;

import java.io.Serial;
import java.io.Serializable;

import lombok.Getter;
import lombok.Setter;

/**
 * Query information.
 */
@Setter
@Getter
public class QueryInfo implements Serializable
{
    @Serial
    private static final long serialVersionUID = 6672020602331767905L;

    /** Query URL. */
    private String queryURL;

    /** Criteria. */
    private QueryCriteria criteria;

    /** Query notes. */
    private QueryNote[] note;
}
