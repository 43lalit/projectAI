package com.projectai.projectai.bookmarks;

import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;

@Service
public class BookmarkService {

    private final JdbcTemplate jdbcTemplate;

    public BookmarkService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        ensureTables();
    }

    public List<BookmarkModels.BookmarkGroupResponse> listGroups(long userId, Long runId) {
        Long normalizedRunId = normalizeRunContext(runId);
        return jdbcTemplate.query(
                """
                SELECT g.group_id, g.name, COUNT(b.bookmark_id) AS item_count
                FROM favorite_group g
                LEFT JOIN bookmark_item b
                    ON b.group_id = g.group_id
                   AND b.run_context_id IS NOT DISTINCT FROM CAST(? AS BIGINT)
                WHERE g.user_id = ?
                GROUP BY g.group_id, g.name
                ORDER BY LOWER(g.name)
                """,
                (rs, rowNum) -> new BookmarkModels.BookmarkGroupResponse(
                        rs.getLong("group_id"),
                        rs.getString("name"),
                        rs.getInt("item_count")
                ),
                normalizedRunId,
                userId
        );
    }

    public BookmarkModels.BookmarkGroupResponse createGroup(long userId, BookmarkModels.CreateGroupRequest request) {
        String name = request.name() == null ? "" : request.name().trim();
        if (name.isBlank()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Group name is required");
        }

        Integer existing = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM favorite_group WHERE user_id = ? AND LOWER(name) = LOWER(?)",
                Integer.class,
                userId,
                name
        );
        if (existing != null && existing > 0) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "Group already exists");
        }

        long now = System.currentTimeMillis();
        jdbcTemplate.update(
                "INSERT INTO favorite_group (user_id, name, created_at) VALUES (?, ?, ?)",
                userId,
                name,
                now
        );

        Long groupId = jdbcTemplate.queryForObject(
                "SELECT group_id FROM favorite_group WHERE user_id = ? AND LOWER(name) = LOWER(?)",
                Long.class,
                userId,
                name
        );

        return new BookmarkModels.BookmarkGroupResponse(groupId == null ? 0L : groupId, name, 0);
    }

    public BookmarkModels.BookmarkGroupResponse renameGroup(
            long userId,
            long groupId,
            BookmarkModels.RenameGroupRequest request
    ) {
        String name = request.name() == null ? "" : request.name().trim();
        if (name.isBlank()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Group name is required");
        }

        ensureGroupBelongsToUser(groupId, userId);

        Integer existing = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM favorite_group WHERE user_id = ? AND LOWER(name) = LOWER(?) AND group_id <> ?",
                Integer.class,
                userId,
                name,
                groupId
        );
        if (existing != null && existing > 0) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "Group name already exists");
        }

        jdbcTemplate.update(
                "UPDATE favorite_group SET name = ? WHERE group_id = ? AND user_id = ?",
                name,
                groupId,
                userId
        );

        Integer itemCount = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM bookmark_item WHERE group_id = ? AND user_id = ?",
                Integer.class,
                groupId,
                userId
        );
        return new BookmarkModels.BookmarkGroupResponse(groupId, name, itemCount == null ? 0 : itemCount);
    }

    public void deleteGroup(long userId, long groupId) {
        ensureGroupBelongsToUser(groupId, userId);
        jdbcTemplate.update(
                "UPDATE bookmark_item SET group_id = NULL WHERE user_id = ? AND group_id = ?",
                userId,
                groupId
        );
        jdbcTemplate.update(
                "DELETE FROM favorite_group WHERE group_id = ? AND user_id = ?",
                groupId,
                userId
        );
    }

    public List<BookmarkModels.BookmarkItemResponse> listItems(long userId, Long groupId, Long runId) {
        Long normalizedRunId = normalizeRunContext(runId);
        if (groupId != null) {
            ensureGroupBelongsToUser(groupId, userId);
        }

        if (groupId == null) {
            return jdbcTemplate.query(
                    """
                    SELECT
                        b.bookmark_id,
                        b.group_id,
                        g.name AS group_name,
                        b.item_type,
                        b.item_key,
                        b.title,
                        b.subtitle,
                        b.url,
                        b.created_at
                    FROM bookmark_item b
                    LEFT JOIN favorite_group g ON g.group_id = b.group_id
                    WHERE b.user_id = ?
                      AND b.run_context_id IS NOT DISTINCT FROM CAST(? AS BIGINT)
                    ORDER BY b.created_at DESC
                    """,
                    (rs, rowNum) -> new BookmarkModels.BookmarkItemResponse(
                            rs.getLong("bookmark_id"),
                            rs.getObject("group_id") == null ? null : rs.getLong("group_id"),
                            rs.getString("group_name"),
                            rs.getString("item_type"),
                            rs.getString("item_key"),
                            rs.getString("title"),
                            rs.getString("subtitle"),
                            rs.getString("url"),
                            rs.getLong("created_at")
                    ),
                    userId,
                    normalizedRunId
            );
        }

        return jdbcTemplate.query(
                """
                SELECT
                    b.bookmark_id,
                    b.group_id,
                    g.name AS group_name,
                    b.item_type,
                    b.item_key,
                    b.title,
                    b.subtitle,
                    b.url,
                    b.created_at
                FROM bookmark_item b
                LEFT JOIN favorite_group g ON g.group_id = b.group_id
                WHERE b.user_id = ?
                  AND b.group_id = ?
                  AND b.run_context_id IS NOT DISTINCT FROM CAST(? AS BIGINT)
                ORDER BY b.created_at DESC
                """,
                (rs, rowNum) -> new BookmarkModels.BookmarkItemResponse(
                        rs.getLong("bookmark_id"),
                        rs.getObject("group_id") == null ? null : rs.getLong("group_id"),
                        rs.getString("group_name"),
                        rs.getString("item_type"),
                        rs.getString("item_key"),
                        rs.getString("title"),
                        rs.getString("subtitle"),
                        rs.getString("url"),
                        rs.getLong("created_at")
                ),
                userId,
                groupId,
                normalizedRunId
        );
    }

    public BookmarkModels.BookmarkItemResponse addItem(long userId, BookmarkModels.AddBookmarkRequest request, Long runId) {
        Long normalizedRunId = normalizeRunContext(runId);
        String itemType = normalizeRequired(request.itemType(), "itemType");
        String itemKey = normalizeRequired(request.itemKey(), "itemKey");
        String title = normalizeRequired(request.title(), "title");
        Long groupId = request.groupId();

        if (groupId != null) {
            Integer ownsGroup = jdbcTemplate.queryForObject(
                    "SELECT COUNT(*) FROM favorite_group WHERE group_id = ? AND user_id = ?",
                    Integer.class,
                    groupId,
                    userId
            );
            if (ownsGroup == null || ownsGroup == 0) {
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid groupId");
            }
        }

        Integer existing = jdbcTemplate.queryForObject(
                """
                SELECT COUNT(*)
                FROM bookmark_item
                WHERE user_id = ?
                  AND item_type = ?
                  AND item_key = ?
                  AND group_id IS NOT DISTINCT FROM CAST(? AS BIGINT)
                  AND run_context_id IS NOT DISTINCT FROM CAST(? AS BIGINT)
                """,
                Integer.class,
                userId,
                itemType,
                itemKey,
                groupId,
                normalizedRunId
        );
        if (existing != null && existing > 0) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "Item already bookmarked in this group");
        }

        long now = System.currentTimeMillis();
        jdbcTemplate.update(
                """
                INSERT INTO bookmark_item (user_id, group_id, item_type, item_key, title, subtitle, url, run_context_id, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                userId,
                groupId,
                itemType,
                itemKey,
                title,
                safeNullable(request.subtitle()),
                safeNullable(request.url()),
                normalizedRunId,
                now
        );

        Long bookmarkId = jdbcTemplate.queryForObject(
                "SELECT MAX(bookmark_id) FROM bookmark_item WHERE user_id = ?",
                Long.class,
                userId
        );

        String groupName = null;
        if (groupId != null) {
            groupName = jdbcTemplate.queryForObject(
                    "SELECT name FROM favorite_group WHERE group_id = ?",
                    String.class,
                    groupId
            );
        }

        return new BookmarkModels.BookmarkItemResponse(
                bookmarkId == null ? 0L : bookmarkId,
                groupId,
                groupName,
                itemType,
                itemKey,
                title,
                safeNullable(request.subtitle()),
                safeNullable(request.url()),
                now
        );
    }

    public void deleteItem(long userId, long bookmarkId, Long runId) {
        Long normalizedRunId = normalizeRunContext(runId);
        int updated = jdbcTemplate.update(
                "DELETE FROM bookmark_item WHERE bookmark_id = ? AND user_id = ?"
                        + " AND run_context_id IS NOT DISTINCT FROM CAST(? AS BIGINT)",
                bookmarkId,
                userId,
                normalizedRunId
        );
        if (updated == 0) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Bookmark not found");
        }
    }

    public BookmarkModels.BookmarkItemResponse moveItemToGroup(long userId, long bookmarkId, Long groupId, Long runId) {
        Long normalizedRunId = normalizeRunContext(runId);
        if (groupId != null) {
            ensureGroupBelongsToUser(groupId, userId);
        }

        int updated = jdbcTemplate.update(
                "UPDATE bookmark_item SET group_id = ? WHERE bookmark_id = ? AND user_id = ?"
                        + " AND run_context_id IS NOT DISTINCT FROM CAST(? AS BIGINT)",
                groupId,
                bookmarkId,
                userId,
                normalizedRunId
        );
        if (updated == 0) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Bookmark not found");
        }

        return jdbcTemplate.queryForObject(
                """
                SELECT
                    b.bookmark_id,
                    b.group_id,
                    g.name AS group_name,
                    b.item_type,
                    b.item_key,
                    b.title,
                    b.subtitle,
                    b.url,
                    b.created_at
                FROM bookmark_item b
                LEFT JOIN favorite_group g ON g.group_id = b.group_id
                WHERE b.bookmark_id = ? AND b.user_id = ?
                  AND b.run_context_id IS NOT DISTINCT FROM CAST(? AS BIGINT)
                """,
                (rs, rowNum) -> new BookmarkModels.BookmarkItemResponse(
                        rs.getLong("bookmark_id"),
                        rs.getObject("group_id") == null ? null : rs.getLong("group_id"),
                        rs.getString("group_name"),
                        rs.getString("item_type"),
                        rs.getString("item_key"),
                        rs.getString("title"),
                        rs.getString("subtitle"),
                        rs.getString("url"),
                        rs.getLong("created_at")
                ),
                bookmarkId,
                userId,
                normalizedRunId
        );
    }

    private String normalizeRequired(String value, String field) {
        String normalized = value == null ? "" : value.trim();
        if (normalized.isBlank()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, field + " is required");
        }
        return normalized;
    }

    private String safeNullable(String value) {
        if (value == null || value.trim().isBlank()) {
            return null;
        }
        return value.trim();
    }

    private void ensureGroupBelongsToUser(long groupId, long userId) {
        Integer ownsGroup = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM favorite_group WHERE group_id = ? AND user_id = ?",
                Integer.class,
                groupId,
                userId
        );
        if (ownsGroup == null || ownsGroup == 0) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid groupId");
        }
    }

    private void ensureTables() {
        jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS app_user (
                    user_id BIGSERIAL PRIMARY KEY,
                    username TEXT NOT NULL UNIQUE,
                    display_name TEXT NOT NULL,
                    password_hash TEXT NOT NULL,
                    created_at BIGINT NOT NULL
                )
                """);

        jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS favorite_group (
                    group_id BIGSERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL REFERENCES app_user(user_id) ON DELETE CASCADE,
                    name TEXT NOT NULL,
                    created_at BIGINT NOT NULL,
                    UNIQUE (user_id, name)
                )
                """);

        jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS bookmark_item (
                    bookmark_id BIGSERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL REFERENCES app_user(user_id) ON DELETE CASCADE,
                    group_id BIGINT NULL REFERENCES favorite_group(group_id) ON DELETE SET NULL,
                    item_type TEXT NOT NULL,
                    item_key TEXT NOT NULL,
                    title TEXT NOT NULL,
                    subtitle TEXT NULL,
                    url TEXT NULL,
                    run_context_id BIGINT NULL,
                    created_at BIGINT NOT NULL
                )
                """);
        jdbcTemplate.execute("ALTER TABLE bookmark_item ADD COLUMN IF NOT EXISTS run_context_id BIGINT NULL");

        jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS idx_favorite_group_user ON favorite_group(user_id)");
        jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS idx_bookmark_item_user ON bookmark_item(user_id)");
        jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS idx_bookmark_item_group ON bookmark_item(group_id)");
        jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS idx_bookmark_item_user_context ON bookmark_item(user_id, run_context_id)");
    }

    private Long normalizeRunContext(Long runId) {
        if (runId == null) {
            return null;
        }
        return runId > 0 ? runId : null;
    }
}
