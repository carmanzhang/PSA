package pm.index;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import pm.utils.DBUtil;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;


public class PubMedPaperContentIndexer {
    private String indexPath;
    private IndexWriter writer = null;

    public PubMedPaperContentIndexer(String indexPath) {
        this.indexPath = indexPath;
    }

    public void work() throws IOException {
        Directory dir = FSDirectory.open(Paths.get(indexPath));
//        Analyzer analyzer = new StandardAnalyzer();
        IndexWriterConfig iwc = new IndexWriterConfig(new StandardAnalyzer());
        iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        iwc.setRAMBufferSizeMB(25600.0);
        writer = new IndexWriter(dir, iwc);
        indexDocs(writer);
        writer.close();
    }

    private void indexDocs(IndexWriter writer) throws IOException {
        String pm_id = null;
        String clean_content, clean_mesh_headings, clean_keywords = null;
        Connection conn = DBUtil.getConn();

        String sql = "select pm_id, concat(clean_title, ' ', clean_abstract) as clean_content, clean_mesh_headings, clean_keywords\n" +
                "from fp.paper_clean_content;";
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            System.out.println("execute query sql successfully");

            int cnt = 0;
            while (rs.next()) {
                cnt += 1;
                if (cnt % 100000 == 0) {
                    System.out.println("Count: " + cnt / 10000 + "万");
                }

                pm_id = rs.getString(1);
                clean_content = rs.getString(2);
                clean_mesh_headings = rs.getString(3);
                clean_keywords = rs.getString(4);

                indexPaperContent(pm_id, clean_content, clean_mesh_headings, clean_keywords, writer);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private void indexPaperContent(String pm_id, String cleanContent, String cleanMesh, String cleanKeywords, IndexWriter writer) throws IOException {
        Document doc = new Document();
        doc.add(new StringField("pm_id", pm_id, Field.Store.YES));
        doc.add(new TextField("content", cleanContent, Field.Store.YES));
//        doc.add(new TextField("mesh", cleanMesh, Field.Store.YES));
//        doc.add(new TextField("keyword", cleanKeywords, Field.Store.YES));

        writer.addDocument(doc);
    }

    public static void main(String[] args) {
        String indexPath = "/home/zhangli/ssd-1t/lucene-index/pubmed/pubmed-all-paper-index";
        PubMedPaperContentIndexer indexer = new PubMedPaperContentIndexer(indexPath);
        try {
            indexer.work();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
