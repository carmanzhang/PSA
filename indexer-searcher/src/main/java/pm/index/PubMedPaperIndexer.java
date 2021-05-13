package pm.index;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.standard.*;
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

//class MyAnalyzer extends Analyzer{
//
//    public MyAnalyzer() {
//    }
//
//    @Override
//    protected TokenStreamComponents createComponents(String fieldName) {
//        final Tokenizer source=new StandardTokenizer();
//        //添加过滤器TokenFilter
//        //LengthFielter results=new LengthFilter(source,1,3);
//        return new TokenStreamComponents(source);
//    }
//}
public class PubMedPaperIndexer {
    private String indexPath;
    private IndexWriter writer = null;

    public PubMedPaperIndexer(String indexPath) {
        this.indexPath = indexPath;
    }

    public void work() throws IOException {
        Directory dir = FSDirectory.open(Paths.get(indexPath));
        Analyzer analyzer = new StandardAnalyzer();
        IndexWriterConfig iwc = new IndexWriterConfig(new StandardAnalyzer());
        iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        iwc.setRAMBufferSizeMB(25600.0);
        writer = new IndexWriter(dir, iwc);
        indexDocs(writer);
        writer.close();
    }

    private void indexDocs(IndexWriter writer) throws IOException {
        String pm_id = null;
        String original_mesh, original_reference, enhanced_mesh, enhanced_reference, enhanced_mesh_reference
                = null;
        Connection conn = DBUtil.getConn();

        String sql = "select pm_id,\n" +
                "       arrayStringConcat(mesh_ids, ' ')                                                            as original_mesh,\n" +
                "       arrayStringConcat(references, ' ')                                                          as original_reference,\n" +
                "       arrayStringConcat(arrayDistinct(arrayConcat(mesh_ids, bern_entity_ids) as m), ' ')          as enhanced_mesh,\n" +
                "       arrayStringConcat(arrayDistinct(arrayConcat(references, european_pm_references) as n),' ')  as enhanced_reference,\n" +
                "       arrayStringConcat(arrayDistinct(arrayConcat(m, n)), ' ')                                    as enhanced_mesh_reference\n" +
                "from sp.pubmed_paper_mesh_reference_bioentity\n" +
                "where length(mesh_ids) > 0\n" +
                "   or length(references) > 0\n" +
                "   or length(european_pm_references) > 0\n" +
                "   or length(bern_entity_ids) > 0\n" +
                "   or length(matched_mesh_ids) > 0;";
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
                original_mesh = rs.getString(2);
                original_reference = rs.getString(3);
                enhanced_mesh = rs.getString(4);
                enhanced_reference = rs.getString(5);
                enhanced_mesh_reference = rs.getString(6);

                indexPaper(pm_id, original_mesh, original_reference, enhanced_mesh, enhanced_reference, enhanced_mesh_reference, writer);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private void indexPaper(String pm_id, String original_mesh, String original_reference, String enhanced_mesh, String enhanced_reference, String enhanced_mesh_reference, IndexWriter writer) throws IOException {
        Document doc = new Document();
        doc.add(new StringField("pm_id", pm_id, Field.Store.YES));
        doc.add(new TextField("original_mesh", original_mesh, Field.Store.YES));
        doc.add(new TextField("original_reference", original_reference, Field.Store.YES));
        doc.add(new TextField("enhanced_mesh", enhanced_mesh, Field.Store.YES));
        doc.add(new TextField("enhanced_reference", enhanced_reference, Field.Store.YES));
        doc.add(new TextField("enhanced_mesh_reference", enhanced_mesh_reference, Field.Store.YES));

        writer.addDocument(doc);
    }

    public static void main(String[] args) {
        String indexPath = "~/ssd-1t/pubmed-similar-paper-task-index";
        PubMedPaperIndexer indexer = new PubMedPaperIndexer(indexPath);
        try {
            indexer.work();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
