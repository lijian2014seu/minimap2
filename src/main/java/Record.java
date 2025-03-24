import java.io.Serializable; 

@SuppressWarnings("serial")
public class Record implements Serializable {
	private String ctgname;
	private int qlen;
	private int qstart;
	private int qend;
	private String refname;
	private int matchlen;
	private int maplen;
	private int s1score;

	public String getCtgname(){
		return ctgname;
	}
	public void setCtgname(String x){
		this.ctgname = x;
	}
	public int getQlen(){
		return qlen;
	}
	public void setQlen(int x){
		this.qlen = x;
	}
	public int getQstart(){
		return qstart;
	}
	public void setQstart(int x){
		this.qstart = x;
	}
	public int getQend(){
		return qend;
	}
	public void setQend(int x){
		this.qend = x;
	}
	/*public int getRlen(){
		return rlen;
	}
	public void setRlen(int x){
		this.rlen = x;
	}
	public int getRstart(){
		return rstart;
	}
	public void setRstart(int x){
		this.rstart = x;
	}
	public int getRend(){
		return rend;
	}
	public void setRend(int x){
		this.rend = x;
	}*/
	public String getRefname(){
		return refname;
	}
	public void setRefname(String x){
		this.refname = x;
	}
	public int getMatchlen(){
		return matchlen;
	}
	public void setMatchlen(int x){
		this.matchlen = x;
	}
	public int getMaplen(){
		return maplen;
	}
	public void setMaplen(int x){
		this.maplen = x;
	}
	public int getS1score(){
		return s1score;
	}
	public void setS1score(int x){
		this.s1score = x;
	}
	/*public int getS2score(){
		return s2score;
	}
	public void setS2score(int x){
		this.s2score = x;
	}*/
}