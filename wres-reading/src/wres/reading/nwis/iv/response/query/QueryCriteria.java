package wres.reading.nwis.iv.response.query;

import java.io.Serial;
import java.io.Serializable;

import lombok.Getter;
import lombok.Setter;

/**
 * The query criteria.
 */

@Setter
@Getter
public class QueryCriteria implements Serializable
{
    @Serial
    private static final long serialVersionUID = -6329750451321834556L;

    /** Location parameter. */
    private String locationParam;

    /** Variable parameter. */
    private String variableParam;

    /** Time parameter. */
    private QueryTimeParameter timeParam;

    /** Parameter. */
    private String[] parameter;
}
