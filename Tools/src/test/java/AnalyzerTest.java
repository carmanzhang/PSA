import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class AnalyzerTest {
    public static List<String> tokenizeString(Analyzer analyzer, String string) {
        List<String> result = new ArrayList<String>();
        try {
            TokenStream stream  = analyzer.tokenStream(null, new StringReader(string));
            stream.reset();
            while (stream.incrementToken()) {
                result.add(stream.getAttribute(CharTermAttribute.class).toString());
            }
        } catch (IOException e) {
            // not thrown b/c we're using a string reader...
            throw new RuntimeException(e);
        }
        return result;
    }

    public static void main(String[] args) {
        StandardAnalyzer analyzer = new StandardAnalyzer();
        List<String> strings = tokenizeString(analyzer, "321 543 5 657 5687 89");
        for (String s : strings) {
            System.out.println(s);
        }

    }
}
