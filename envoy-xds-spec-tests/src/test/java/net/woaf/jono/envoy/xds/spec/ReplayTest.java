package net.woaf.jono.envoy.xds.spec;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.gson.JsonParser;
import org.junit.jupiter.api.Test;

class ReplayTest {

    @Test
    void printsWhatEachEnvoyGotLeavingOutAnAckOfNoResponse() {
        String vector = """
            {"name": "v", "protocols": ["sotw", "delta"], "serves": ["CDS"], "envoys": [{"name": "e", "view": "v"}], "steps": [
              {"update": [{"setView": "v", "groups": ["g"]}, {"editGroup": "g", "put": {"CDS": [{"name": "a"}]}}]},
              {"request": "CDS", "ack": "last", "expect": [{"type": "CDS", "resources": [{"name": "wrong"}]}]},
              {"request": "CDS", "ack": "last"},
              {"request": "CDS", "nack": "last-1"}
            ]}""";

        assertThat(Replay.replay(JsonParser.parseString(vector).getAsJsonObject())).isEqualTo(JsonParser.parseString("""
            {"name": "v", "runs": {
              "sotw": {"steps": [[], [{"envoy": "e", "type": "CDS", "resources": [{"name": "a"}]}], [], []]},
              "delta": {"steps": [[], [{"envoy": "e", "type": "CDS", "resources": [{"name": "a"}], "removed": []}], [], []]}
            }}"""));
    }

    @Test
    void carriesOnAfterAStreamTheServerEnds() {
        String vector = """
            {"name": "v", "protocols": ["delta"], "serves": ["CDS"], "envoys": [{"name": "e", "view": "v"}], "steps": [
              {"request": "CDS"},
              {"resync": "e"},
              {"request": "CDS"}
            ]}""";

        assertThat(Replay.replay(JsonParser.parseString(vector).getAsJsonObject())).isEqualTo(JsonParser.parseString("""
            {"name": "v", "runs": {"delta": {"steps": [
              [{"envoy": "e", "type": "CDS", "resources": [], "removed": []}],
              [{"envoy": "e", "ended": "OK"}],
              [{"envoy": "e", "type": "CDS", "resources": [], "removed": []}]
            ]}}}"""));
    }

    @Test
    void reportsAVectorInError() {
        String vector = """
            {"name": "v", "protocols": ["sotw"], "serves": ["CDS"], "envoys": [{"name": "e", "view": "v"}], "steps": [
              {"disconnect": "e"}
            ]}""";

        assertThat(Replay.replay(JsonParser.parseString(vector).getAsJsonObject()).getAsJsonObject("runs").getAsJsonObject("sotw").has("error"))
            .isTrue();
    }
}
