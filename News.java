package mypackage;

import java.util.ArrayList;
import java.util.List;


public class News {

    private String url;
    private String title;
    private List<String> budgets = new ArrayList<String>();
    private String content;
    private Integer ftime;
    
    public void setContent(String content) {
        this.content = content;
    }
    
    public void setFtime(Integer ftime) {
        this.ftime = ftime;
    }
    
    public String getContent() {
        return content;
    }
    
    public Integer getFtime() {
        return ftime;
    }
    
    public String getUrl() {
        return url;
    }
    
    public String getTitle() {
        return title;
    }
    
    public String getBudgets_jin() {
    	String topics = "";
    	for(String s:budgets){
    		topics = topics + s +",";
    	}
        return topics;
    }

    public void setUrl(String url) {
        this.url = url;
    }
    
    public void setTitle(String title) {
        this.title = title;
    }
    

    public List<String> getBudgets() {
        return budgets;
    }

    public void setBudgets(List<String> budgets) {
        this.budgets = budgets;
    }
}