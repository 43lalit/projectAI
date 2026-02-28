package com.projectai.projectai.bookmarks;

public final class BookmarkModels {

    private BookmarkModels() {
    }

    public record CreateGroupRequest(String name) {
    }

    public record RenameGroupRequest(String name) {
    }

    public record BookmarkGroupResponse(long groupId, String name, int itemCount) {
    }

    public record AddBookmarkRequest(Long groupId, String itemType, String itemKey, String title, String subtitle, String url) {
    }

    public record MoveBookmarkRequest(Long groupId) {
    }

    public record BookmarkItemResponse(
            long bookmarkId,
            Long groupId,
            String groupName,
            String itemType,
            String itemKey,
            String title,
            String subtitle,
            String url,
            long createdAt
    ) {
    }
}
