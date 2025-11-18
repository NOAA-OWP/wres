package wres.reading.nwis.iv.response.query;

import java.io.Serial;
import java.io.Serializable;

import lombok.Getter;
import lombok.Setter;

/**
 * A query note.
 */
@Setter
@Getter
public class QueryNote implements Serializable
{
    @Serial
    private static final long serialVersionUID = -5115638777040755540L;

    /** Value. */
    private String value;

    /** Title. */
    private String title;

}
