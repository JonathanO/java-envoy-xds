package net.woaf.jono.envoy.xds.spec;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import static org.assertj.core.api.Assertions.assertThat;

/** Runs each trace vector in the spec submodule (spec/vectors) through the real ADS service, once per protocol. */
public class VectorTest {

    @TestFactory
    Stream<DynamicTest> vectors() throws IOException {
        Path dir = Path.of(System.getProperty("vectors"));
        List<Path> files;
        try (Stream<Path> listed = Files.isDirectory(dir) ? Files.list(dir) : Stream.empty()) {
            files = listed.filter(f -> f.toString().endsWith(".json")).sorted().toList();
        }
        assertThat(files).as("vectors in %s: is the spec submodule checked out? git submodule update --init", dir).isNotEmpty();
        return files.stream().flatMap(VectorTest::tests);
    }

    private static Stream<DynamicTest> tests(Path file) {
        JsonObject vector = read(file);
        String name = vector.get("name").getAsString();
        assertThat(file.getFileName()).as("the file of vector %s", name).hasToString(name + ".json");
        return StreamSupport.stream(vector.getAsJsonArray("protocols").spliterator(), false)
            .map(JsonElement::getAsString)
            .map(protocol -> DynamicTest.dynamicTest(name + " [" + protocol + "]", file.toUri(), () -> {
                skipIfJavaDiffers(vector);
                new VectorRun(vector, protocol).run();
            }));
    }

    private static void skipIfJavaDiffers(JsonObject vector) {
        if (vector.has("differs")) {
            for (JsonElement difference : vector.getAsJsonArray("differs")) {
                JsonObject d = difference.getAsJsonObject();
                Assumptions.assumeFalse(d.get("port").getAsString().equals("java"), () -> "Java differs: " + d.get("reason").getAsString());
            }
        }
    }

    private static JsonObject read(Path file) {
        try {
            return JsonParser.parseString(Files.readString(file, StandardCharsets.UTF_8)).getAsJsonObject();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
