package pm.searcher;

import com.google.common.collect.Lists;
import jdk.nashorn.internal.objects.NativeUint8Array;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MultiCollectorManager;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import pm.entity.SearchResult;
import pm.utils.DBUtil;
import pm.utils.JsonUtil;
import pm.utils.SentenceUtil;

import java.io.*;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;

public class PaperPaperSearcher {
    private static PaperPaperSearcher ourInstance = new PaperPaperSearcher();
    private static StandardAnalyzer analyzer = new StandardAnalyzer();

    private IndexSearcher searcher = null;
    private String indexPath = "/home/zhangli/ssd-1t/pubmed-similar-paper-task-index";
//    private String indexPath = "/home/zhangli/mydisk-2t/repo/pubmed-paper-clustering/data/index";

    private int numTotalHits = 20;

    public static PaperPaperSearcher getInstance() {
        return ourInstance;
    }

    private PaperPaperSearcher() {
        try {
            init();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void init() throws IOException {
        IndexReader reader = DirectoryReader.open(NIOFSDirectory.open(Paths.get(indexPath)));
        this.searcher = new IndexSearcher(reader);
    }

    public List<SearchResult> search(String line, String field) throws ParseException, IOException {
        List<SearchResult> result = new ArrayList<>();
        if (line == null || line.trim().equals("")) {
            return result;
        }
        QueryParser parser = new QueryParser(field, analyzer);
        Query query = parser.parse(line);
        ScoreDoc[] hits = searcher.search(query, numTotalHits).scoreDocs;
//        System.out.println("search key: " + line);
        List<String> searchTokens = Arrays.asList(line.split(" "));

        for (ScoreDoc hit : hits) {
            float score = hit.score;

            Document doc = searcher.doc(hit.doc);
            String searched_pm_id = doc.get("pm_id");
            String matched_field = doc.get(field);
            Collection intersection = CollectionUtils.intersection(searchTokens, Arrays.asList(matched_field.split(" ")));
            int intersect = intersection.size();
//            System.out.println(searched_pm_id + "\t[" + intersection.size() + "]\t" + StringUtils.join(intersection, " "));

            result.add(new SearchResult(searched_pm_id, intersect, score));
        }

        return result;
    }

    public static void main(String[] args) throws Exception {
        String savedFile = "/home/zhangli/mydisk-2t/repo/pubmed-paper-clustering/data/pubmed_paper_found_sample_similar_article.tsv";
        BufferedReader rd = new BufferedReader(new FileReader(savedFile));
        String tmp;
        HashSet<String> eixtingPMIDSet = new HashSet<>();
        while ((tmp = rd.readLine()) != null) {
            String pm_id = tmp.split("\t")[0];
            eixtingPMIDSet.add(pm_id);
        }
        rd.close();
        System.out.println(eixtingPMIDSet.size());
        // TODO file append
        BufferedWriter writer = new BufferedWriter(new FileWriter(new File(savedFile), true));

        PaperPaperSearcher searcher = PaperPaperSearcher.getInstance();
        String pm_id = null;
        String original_mesh, original_reference, enhanced_mesh, enhanced_reference, enhanced_mesh_reference
                = null;
        Connection conn = DBUtil.getConn();
        String sql = "select pm_id,\n" +
                "       arrayStringConcat(mesh_ids, ' ')                                                   as original_mesh,\n" +
                "       arrayStringConcat(references, ' ')                                                 as original_reference,\n" +
                "       arrayStringConcat(arrayDistinct(arrayConcat(mesh_ids, bern_entity_ids) as m), ' ') as enhanced_mesh,\n" +
                "       arrayStringConcat(arrayDistinct(arrayConcat(references, european_pm_references) as n),\n" +
                "                         ' ')                                                             as enhanced_reference,\n" +
                "       arrayStringConcat(arrayDistinct(arrayConcat(m, n)), ' ')                           as enhanced_mesh_reference\n" +
                "from sp.pubmed_randomly_selected_papers;\n";
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            System.out.println("execute query sql successfully");

            int cnt = 0;
            while (rs.next()) {
                cnt += 1;
                if (cnt % 10000 == 0) {
                    System.out.println("Count: " + cnt / 10000 + "万");
                }

                pm_id = rs.getString(1);
                if (eixtingPMIDSet.contains(pm_id)) {
                    System.out.println("contained: " + pm_id);
                    continue;
                }
                original_mesh = rs.getString(2);
                original_reference = rs.getString(3);
                enhanced_mesh = rs.getString(4);
                enhanced_reference = rs.getString(5);
                enhanced_mesh_reference = rs.getString(6);
                try {
                    List<SearchResult> search_doc_original_mesh = searcher.search(original_mesh, "original_mesh");
                    List<SearchResult> search_doc_original_reference = searcher.search(original_reference, "original_reference");
                    List<SearchResult> search_doc_enhanced_mesh = searcher.search(enhanced_mesh, "enhanced_mesh");
                    List<SearchResult> search_doc_enhanced_reference = searcher.search(enhanced_reference, "enhanced_reference");
                    List<SearchResult> search_doc_enhanced_mesh_reference = searcher.search(enhanced_mesh_reference, "enhanced_mesh_reference");
                    writer.write(
                            StringUtils.join(
                                    new String[]{
                                            pm_id,
                                            JsonUtil.Marshal(search_doc_original_mesh),
                                            JsonUtil.Marshal(search_doc_original_reference),
                                            JsonUtil.Marshal(search_doc_enhanced_mesh),
                                            JsonUtil.Marshal(search_doc_enhanced_reference),
                                            JsonUtil.Marshal(search_doc_enhanced_mesh_reference)
                                    },
                                    "\t"
                            )
                    );

                    writer.write("\n");
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        writer.close();
    }
}
