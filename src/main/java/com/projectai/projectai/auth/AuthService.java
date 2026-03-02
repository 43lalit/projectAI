package com.projectai.projectai.auth;

import jakarta.servlet.http.HttpSession;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

@Service
public class AuthService {

    public static final String ROLE_ADMIN = "ADMIN";
    public static final String ROLE_USER = "USER";
    private static final String PRIMARY_ADMIN_EMAIL = "43.lalit@gmail.com";

    private final JdbcTemplate jdbcTemplate;
    private final PasswordEncoder passwordEncoder;

    public AuthService(JdbcTemplate jdbcTemplate, PasswordEncoder passwordEncoder) {
        this.jdbcTemplate = jdbcTemplate;
        this.passwordEncoder = passwordEncoder;
        ensureUserTable();
    }

    public AuthModels.AuthUserResponse register(AuthModels.RegisterRequest request, HttpSession session) {
        String email = normalizeEmail(request.email());
        String firstName = normalizeRequired(request.firstName(), "First name");
        String lastName = normalizeRequired(request.lastName(), "Last name");
        String password = request.password() == null ? "" : request.password().trim();
        String displayName = firstName + " " + lastName;
        String role = resolveRoleForEmail(email);

        if (password.length() < 6) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Password must be at least 6 characters");
        }

        Integer existing = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM app_user WHERE LOWER(email) = LOWER(?)",
                Integer.class,
                email
        );
        if (existing != null && existing > 0) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "Email already exists");
        }

        String hash = passwordEncoder.encode(password);
        long now = System.currentTimeMillis();
        jdbcTemplate.update(
                "INSERT INTO app_user (username, display_name, first_name, last_name, email, role, password_hash, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                email,
                displayName,
                firstName,
                lastName,
                email,
                role,
                hash,
                now
        );

        AuthModels.AuthUserResponse user = findByEmail(email);
        session.setAttribute(AuthModels.SESSION_USER_ID, user.userId());
        return user;
    }

    public AuthModels.AuthUserResponse login(AuthModels.LoginRequest request, HttpSession session) {
        String email = normalizeEmail(request.email());
        String password = request.password() == null ? "" : request.password().trim();

        UserRecord record = findRecordByEmail(email);
        if (!passwordEncoder.matches(password, record.passwordHash())) {
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "Invalid credentials");
        }

        session.setAttribute(AuthModels.SESSION_USER_ID, record.userId());
        return new AuthModels.AuthUserResponse(
                record.userId(),
                record.firstName(),
                record.lastName(),
                record.email(),
                record.role(),
                record.displayName()
        );
    }

    public AuthModels.AuthUserResponse getCurrentUser(HttpSession session) {
        Object userId = session.getAttribute(AuthModels.SESSION_USER_ID);
        if (!(userId instanceof Number number)) {
            return null;
        }
        long id = number.longValue();
        try {
            return jdbcTemplate.queryForObject(
                    "SELECT user_id, first_name, last_name, email, role, display_name FROM app_user WHERE user_id = ?",
                    (rs, rowNum) -> new AuthModels.AuthUserResponse(
                            rs.getLong("user_id"),
                            rs.getString("first_name"),
                            rs.getString("last_name"),
                            rs.getString("email"),
                            rs.getString("role"),
                            rs.getString("display_name")
                    ),
                    id
            );
        } catch (EmptyResultDataAccessException ex) {
            session.removeAttribute(AuthModels.SESSION_USER_ID);
            return null;
        }
    }

    public AuthModels.AuthUserResponse requireCurrentUser(HttpSession session) {
        AuthModels.AuthUserResponse user = getCurrentUser(session);
        if (user == null) {
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "Login required");
        }
        return user;
    }

    public AuthModels.AuthUserResponse requireAdmin(HttpSession session) {
        AuthModels.AuthUserResponse user = requireCurrentUser(session);
        if (!ROLE_ADMIN.equalsIgnoreCase(user.role())) {
            throw new ResponseStatusException(HttpStatus.FORBIDDEN, "Admin access required");
        }
        return user;
    }

    public void logout(HttpSession session) {
        session.invalidate();
    }

    public void resetPassword(AuthModels.ResetPasswordRequest request) {
        String email = normalizeEmail(request.email());
        String newPassword = request.newPassword() == null ? "" : request.newPassword().trim();
        if (newPassword.length() < 6) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Password must be at least 6 characters");
        }
        String hash = passwordEncoder.encode(newPassword);
        int updated = jdbcTemplate.update(
                "UPDATE app_user SET password_hash = ? WHERE LOWER(email) = LOWER(?)",
                hash,
                email
        );
        if (updated == 0) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "User not found");
        }
    }

    private AuthModels.AuthUserResponse findByEmail(String email) {
        return jdbcTemplate.queryForObject(
                "SELECT user_id, first_name, last_name, email, role, display_name FROM app_user WHERE LOWER(email) = LOWER(?)",
                (rs, rowNum) -> new AuthModels.AuthUserResponse(
                        rs.getLong("user_id"),
                        rs.getString("first_name"),
                        rs.getString("last_name"),
                        rs.getString("email"),
                        rs.getString("role"),
                        rs.getString("display_name")
                ),
                email
        );
    }

    private UserRecord findRecordByEmail(String email) {
        try {
            return jdbcTemplate.queryForObject(
                    "SELECT user_id, first_name, last_name, email, role, display_name, password_hash FROM app_user WHERE LOWER(email) = LOWER(?)",
                    (rs, rowNum) -> new UserRecord(
                            rs.getLong("user_id"),
                            rs.getString("first_name"),
                            rs.getString("last_name"),
                            rs.getString("email"),
                            rs.getString("role"),
                            rs.getString("display_name"),
                            rs.getString("password_hash")
                    ),
                    email
            );
        } catch (EmptyResultDataAccessException ex) {
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "Invalid credentials");
        }
    }

    private String normalizeRequired(String value, String fieldName) {
        String normalized = value == null ? "" : value.trim();
        if (normalized.isBlank()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, fieldName + " is required");
        }
        return normalized;
    }

    private String normalizeEmail(String email) {
        String value = email == null ? "" : email.trim().toLowerCase();
        if (value.isBlank()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Email is required");
        }
        if (!value.contains("@") || value.startsWith("@") || value.endsWith("@")) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Email is invalid");
        }
        return value;
    }

    private void ensureUserTable() {
        jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS app_user (
                    user_id BIGSERIAL PRIMARY KEY,
                    username TEXT NOT NULL UNIQUE,
                    display_name TEXT NOT NULL,
                    first_name TEXT NULL,
                    last_name TEXT NULL,
                    email TEXT NULL,
                    role TEXT NOT NULL DEFAULT 'USER',
                    password_hash TEXT NOT NULL,
                    created_at BIGINT NOT NULL
                )
                """);

        ensureColumnExists("app_user", "first_name", "TEXT");
        ensureColumnExists("app_user", "last_name", "TEXT");
        ensureColumnExists("app_user", "email", "TEXT");
        ensureColumnExists("app_user", "role", "TEXT");
        jdbcTemplate.execute("UPDATE app_user SET email = username WHERE email IS NULL OR btrim(email) = ''");
        jdbcTemplate.execute("UPDATE app_user SET email = LOWER(email) WHERE email IS NOT NULL");
        jdbcTemplate.execute("UPDATE app_user SET first_name = display_name WHERE first_name IS NULL OR btrim(first_name) = ''");
        jdbcTemplate.execute("UPDATE app_user SET last_name = '' WHERE last_name IS NULL");
        jdbcTemplate.update(
                "UPDATE app_user SET role = ? WHERE role IS NULL OR btrim(role) = ''",
                ROLE_USER
        );
        jdbcTemplate.update(
                "UPDATE app_user SET role = ? WHERE LOWER(email) = LOWER(?)",
                ROLE_ADMIN,
                PRIMARY_ADMIN_EMAIL
        );
        jdbcTemplate.execute("CREATE UNIQUE INDEX IF NOT EXISTS uk_app_user_email ON app_user(email)");
    }

    private String resolveRoleForEmail(String email) {
        return PRIMARY_ADMIN_EMAIL.equalsIgnoreCase(email) ? ROLE_ADMIN : ROLE_USER;
    }

    private void ensureColumnExists(String tableName, String columnName, String sqlType) {
        jdbcTemplate.execute("ALTER TABLE " + tableName + " ADD COLUMN IF NOT EXISTS " + columnName + " " + sqlType);
    }

    private record UserRecord(
            long userId,
            String firstName,
            String lastName,
            String email,
            String role,
            String displayName,
            String passwordHash
    ) {
    }
}
