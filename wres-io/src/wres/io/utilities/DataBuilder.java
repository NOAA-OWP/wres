package wres.io.utilities;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class DataBuilder
{
    private final Map<String, Integer> columnNames;
    private final List<Object[]> rows;
    private int currentRow;

    public DataBuilder(String... columnNames)
    {
        if (columnNames.length == 0)
        {
            throw new IllegalArgumentException(
                    "Column names must be added to create a new data set."
            );
        }
        this.columnNames = new TreeMap<>(  );

        for (int index = 0; index < columnNames.length; ++index)
        {
            this.columnNames.put(columnNames[index], index);
        }

        this.rows = new ArrayList<>();
        this.currentRow = -1;
    }

    public DataBuilder addRow()
    {
        this.currentRow += 1;
        this.rows.add( new Object[columnNames.size()] );
        return this;
    }

    public DataBuilder set(final String columnName, final Object value)
    {
        if (!this.columnNames.containsKey( columnName ))
        {
            throw new IllegalArgumentException(
                    "'" + columnName + "' is not a valid column for this dataset."
            );
        }

        if (this.currentRow < 0)
        {
            this.addRow();
        }

        int index = this.columnNames.get( columnName );
        this.rows.get(currentRow)[index] = value;
        return this;
    }

    public DataProvider build()
    {
        return DataSetProvider.from(this.columnNames, this.rows);
    }
}
