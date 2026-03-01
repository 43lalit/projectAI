package com.projectai.projectai.recent;

import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;
import java.util.Locale;

@Service
public class RecentService {

    private final JdbcTemplate jdbcTemplate;

    public RecentService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        ensureTable();
    }

    public List<RecentModels.RecentItemResponse> listItems(long userId, int limit) {
        int safeLimit = Math.max(1, Math.min(limit, 50));
        return jdbcTemplate.query(
                """
                SELECT recent_id, item_type, item_key, label, subtitle, url, last_accessed
                FROM recent_item
                WHERE user_id = ?
                ORDER BY last_accessed DESC
                LIMIT ?
                """,
                (rs, rowNum) -> new RecentModels.RecentItemResponse(
                        rs.getLong("recent_id"),
                        rs.getString("item_type"),
                        rs.getString("item_key"),
                        rs.getString("label"),
                        rs.getString("subtitle"),
                        rs.getString("url"),
                        rs.getLong("last_accessed")
                ),
                userId,
                safeLimit
        );
    }

    public List<RecentModels.RecentItemResponse> upsertItem(long userId, RecentModels.RecentItemRequest request, int limit) {
        String itemType = normalizeRequired(request.itemType(), "itemType").toLowerCase(Locale.ROOT);
        String itemKey = normalizeRequired(request.itemKey(), "itemKey");
        String label = normalizeRequired(request.label(), "label");
        String subtitle = safeNullable(request.subtitle());
        String url = safeNullable(request.url());
        long now = System.currentTimeMillis();

        int updated = jdbcTemplate.update(
                """
                UPDATE recent_item
                SET label = ?, subtitle = ?, url = ?, last_accessed = ?
                WHERE user_id = ? AND item_type = ? AND item_key = ?
                """,
                label, subtitle, url, now, userId, itemType, itemKey
        );
        if (updated == 0) {
            jdbcTemplate.update(
                    """
                    INSERT INTO recent_item (user_id, item_type, item_key, label, subtitle, url, created_at, last_accessed)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    userId, itemType, itemKey, label, subtitle, url, now, now
            );
        }
        return listItems(userId, limit);
    }

    public void clearItems(long userId) {
        jdbcTemplate.update("DELETE FROM recent_item WHERE user_id = ?", userId);
    }

    private void ensureTable() {
        jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS recent_item (
                    recent_id BIGSERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    item_type TEXT NOT NULL,
                    item_key TEXT NOT NULL,
                    label TEXT NOT NULL,
                    subtitle TEXT,
                    url TEXT,
                    created_at BIGINT NOT NULL,
                    last_accessed BIGINT NOT NULL
                )
                """);
        jdbcTemplate.execute("CREATE UNIQUE INDEX IF NOT EXISTS uk_recent_item_user_key ON recent_item(user_id, item_type, item_key)");
        jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS idx_recent_item_user_last_accessed ON recent_item(user_id, last_accessed DESC)");
    }

    private String normalizeRequired(String value, String fieldName) {
        String normalized = value == null ? "" : value.trim();
        if (normalized.isBlank()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, fieldName + " is required");
        }
        return normalized;
    }

    private String safeNullable(String value) {
        if (value == null) {
            return null;
        }
        String normalized = value.trim();
        return normalized.isBlank() ? null : normalized;
    }
}
