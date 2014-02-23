package liquibase.ext.vertica.change;

import liquibase.change.ColumnConfig;

/**
 * Created with IntelliJ IDEA.
 * User: vesterma
 * Date: 13/11/13
 * Time: 21:09
 * To change this template use File | Settings | File Templates.
 */
public class ColumnConfigVertica extends ColumnConfig {
    private String encoding = null;
    private Integer accessrank = null;

    public String getEncoding() {
        return encoding;
    }

    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    public Integer getAccessrank() {
        return accessrank;
    }

    public void setAccessrank(Integer accessrank) {
        this.accessrank = accessrank;
    }
}
