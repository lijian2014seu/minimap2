package PreProcess;

public class Seqdata{
    String sequance;
    String seqname;
    String quality;

    public Seqdata(String name, String seq, String qua){
        this.sequance= seq;
        this.seqname=name;
        this.quality=qua;
    }

    public String getQuality() {
        return quality;
    }

    public String getSequance() {
        return sequance;
    }

    public String getSeqname() {
        return seqname;
    }

    public void setQuality(String quality) {
        this.quality = quality;
    }

    public void setSeqname(String seqname) {
        seqname = seqname;
    }

    public void setSequance(String sequance) {
        this.sequance = sequance;
    }
}