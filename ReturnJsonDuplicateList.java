package mypackage;

import java.util.ArrayList;
import java.util.List;

public class ReturnJsonDuplicateList {
	private String status;
    private List<String> url = new ArrayList<String>();
    
    public void setStatus(String status) {
        this.status = status;
    }
    
    public String getStatus() {
        return status;
    }
    
    public List<String> getUrl() {
        return url;
    }

    public void setUrl(List<String> url) {
        this.url = url;
    }
}
