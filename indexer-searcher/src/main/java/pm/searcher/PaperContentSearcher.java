package pm.searcher;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.NIOFSDirectory;
import pm.entity.SearchResult;
import pm.utils.DBUtil;
import pm.utils.JsonUtil;

import java.io.*;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;

public class PaperContentSearcher {
    private static PaperContentSearcher ourInstance = new PaperContentSearcher();
    private static StandardAnalyzer analyzer = new StandardAnalyzer();

    static {
        BooleanQuery.setMaxClauseCount(Integer.MAX_VALUE);
    }

    private IndexSearcher searcher = null;
    private String indexPath = "/home/zhangli/ssd-1t/lucene-index/pubmed/pubmed-all-paper-index";
    private static String searchField = "content";
    private int numTotalHits = 200;

    public static PaperContentSearcher getInstance() {
        return ourInstance;
    }

    private PaperContentSearcher() {
        try {
            init();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void init() throws IOException {
        NIOFSDirectory dir = new NIOFSDirectory(Paths.get(indexPath));
//        MMapDirectory dir = new MMapDirectory(Paths.get(indexPath));
//        dir.setPreload(true);
//        FSDirectory fsDirectory = FSDirectory.open(Paths.get(indexPath));
//        RAMDirectory dir = new RAMDirectory(fsDirectory, IOContext.READ);
//        IndexReader reader = DirectoryReader.open(dir);
        IndexReader reader = DirectoryReader.open(dir);
        IndexSearcher searcher = new IndexSearcher(reader);
        // The default similarity in Lucene and hence also Elasticsearch is however not a pure tf-idf implementation.
        // It does actually have a form of document length normalization too.
        searcher.setSimilarity(new BM25Similarity());
        this.searcher = searcher;

    }


    public List<SearchResult> search(String line, QueryParser parser) throws ParseException, IOException {
        List<SearchResult> result = new ArrayList<>();
        if (line == null || line.trim().equals("")) {
            return result;
        }
        Query query = parser.parse(line);
        ScoreDoc[] hits = searcher.search(query, numTotalHits).scoreDocs;
//        System.out.println("search key: " + line);
        List<String> searchTokens = Arrays.asList(line.split(" "));

        for (ScoreDoc hit : hits) {
            float score = hit.score;

            Document doc = searcher.doc(hit.doc);
            String searched_pm_id = doc.get("pm_id");
            String matched_field = doc.get(searchField);
            Collection intersection = CollectionUtils.intersection(searchTokens, Arrays.asList(matched_field.split(" ")));
            int intersect = intersection.size();
//            System.out.println(searched_pm_id + "\t[" + intersection.size() + "]\t" + StringUtils.join(intersection, " "));

            result.add(new SearchResult(searched_pm_id, intersect, score));
        }

        return result;
    }

    public static void main(String[] args) throws Exception {
        String savedFile = "/home/zhangli/mydisk-2t/repo/pubmed-paper-clustering/code/data/pubmed_paper_found_sample_similar_article_using_bm25.tsv";
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

        PaperContentSearcher searcher = PaperContentSearcher.getInstance();
        String pm_id = null;
        String content = null;
        Connection conn = DBUtil.getConn();
        String sql = "select pm_id, concat(clean_title, ' ', clean_abstract) as content\n" +
                "from (select toString(pm_id) as pm_id, clean_title, clean_abstract from fp.paper_clean_content) any\n" +
                "         inner join sp.pubmed_randomly_selected_papers using pm_id;\n";

        QueryParser parser = new QueryParser(searchField, analyzer);

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
                content = rs.getString(2);
                try {
                    List<SearchResult> search_doc_original_mesh = searcher.search(content, parser);
                    writer.write(
                            StringUtils.join(
                                    new String[]{
                                            pm_id,
                                            JsonUtil.Marshal(search_doc_original_mesh)
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


