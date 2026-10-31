package net.woaf.jono.envoy.xds.spec;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

/**
 * Replay mode, for the spec's differential fuzzer: reads vectors from stdin, one per line, and prints what each Envoy
 * got, one line per vector. See docs/REPLAY.md in the spec repo. {@code ./gradlew :envoy-xds-spec-tests:replayScript}
 * writes a script that runs it.
 */
public final class Replay {

    private Replay() {
    }

    public static void main(String[] args) throws IOException {
        // Stdout is for results: anything else, such as logging, goes to stderr.
        PrintStream out = System.out;
        System.setOut(System.err);
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
        for (String line = in.readLine(); line != null; line = in.readLine()) {
            if (!line.isBlank()) {
                out.println(replay(JsonParser.parseString(line).getAsJsonObject()));
                out.flush();
            }
        }
    }

    static JsonObject replay(JsonObject vector) {
        JsonObject runs = new JsonObject();
        for (JsonElement protocol : vector.getAsJsonArray("protocols")) {
            runs.add(protocol.getAsString(), new VectorRun(vector, protocol.getAsString(), true).replay());
        }
        JsonObject result = new JsonObject();
        result.add("name", vector.get("name"));
        result.add("runs", runs);
        return result;
    }
}
