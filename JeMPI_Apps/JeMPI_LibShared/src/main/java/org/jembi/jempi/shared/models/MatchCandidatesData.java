package org.jembi.jempi.shared.models;

import java.util.List;

public record MatchCandidatesData(
        Interaction interaction,
        String topCandidateGoldenId,
        List<GoldenRecord> candidates) {
}
