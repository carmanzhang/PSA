package indexing.utils;

import com.google.gson.*;

import java.util.Arrays;
import java.util.List;

public class JsonUtil {

    private static final Gson gson = new Gson();

    public static String Marshal(Object obj) {
        return gson.toJson(obj);
    }

    public static <T> T Unmarshal(String str, Class<T> classOfT) {
        return gson.fromJson(str, classOfT);
    }

    public static <T> List<T> UnmarshalList(String str, Class<T[]> clazz) {
        T[] arr = gson.fromJson(str, clazz);
        if (arr == null) {
            return null;
        }
        return Arrays.asList(arr);
    }
}
