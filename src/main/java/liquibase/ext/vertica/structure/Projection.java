package liquibase.ext.vertica.structure;

import liquibase.structure.core.Relation;

/**
 * Created with IntelliJ IDEA.
 * User: vesterma
 * Date: 11/11/13
 * Time: 16:47
 * To change this template use File | Settings | File Templates.
 */
public class Projection extends Relation {

    public String getDefinition() {
        return getAttribute("definition", String.class);
    }

    public void setDefinition(String definition) {
        this.setAttribute("definition", definition);
    }

    public String getSubquery() {
        return getAttribute("subquery", String.class);
    }

    public void setSubquery(String subquery) {
        this.setAttribute("subquery", subquery);
    }




}
