package Common;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class Job {

    public static Map<String, String> getInputData(String[] args) {
        Map<String, String> options = new HashMap<>();
        Pattern optionPattern = Pattern.compile("(?<=--)(.*?)(?==)");
        Pattern valuePattern = Pattern.compile("(?<==)(.*)");

        for(String arg: args) {
            Matcher optionMatcher = optionPattern.matcher(arg);
            Matcher valueMatcher = valuePattern.matcher(arg);

            if (optionMatcher.find() && valueMatcher.find()) {
                options.put(optionMatcher.group(), valueMatcher.group());
            }
        }

        return options;
    }

    public static String glueDirWithFile(String dir, String file) {
        String glue = dir.matches(".*/$") ? "" : "/";
        return dir + glue + file;
    }

    public static List<String> missingOptions(Map<String, String> options, List<String> required) {
        return required.stream().filter(a ->  !options.containsKey(a)).collect(Collectors.toList());
    }

    public static String missingOptionsString(List<String> missingOptions) {
        StringBuilder sb = new StringBuilder();

        for (String option: missingOptions) {
            sb.append("--").append(option).append(" ");
        }

        return sb.toString();
    }
}
