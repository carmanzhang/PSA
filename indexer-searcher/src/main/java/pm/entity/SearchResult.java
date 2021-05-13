package pm.entity;


public class SearchResult {
    private String pm_id;
    private int intersect;
    private float score;

    public SearchResult(String pm_id, int intersect, float score) {
        this.pm_id = pm_id;
        this.intersect = intersect;
        this.score = score;
    }

    public String getPm_id() {
        return pm_id;
    }

    public void setPm_id(String pm_id) {
        this.pm_id = pm_id;
    }

    public int getIntersect() {
        return intersect;
    }

    public void setIntersect(int intersect) {
        this.intersect = intersect;
    }

    public float getScore() {
        return score;
    }

    public void setScore(float score) {
        this.score = score;
    }
}
