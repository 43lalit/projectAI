package com.projectai.projectai.recent;

public final class RecentModels {

    private RecentModels() {
    }

    public record RecentItemRequest(String itemType, String itemKey, String label, String subtitle, String url) {
    }

    public record RecentItemResponse(
            long recentId,
            String itemType,
            String itemKey,
            String label,
            String subtitle,
            String url,
            long lastAccessed
    ) {
    }
}
