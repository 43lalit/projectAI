package com.projectai.projectai.auth;

public final class AuthModels {

    private AuthModels() {
    }

    public static final String SESSION_USER_ID = "session_user_id";

    public record AuthUserResponse(
            long userId,
            String firstName,
            String lastName,
            String email,
            String displayName
    ) {
    }

    public record AuthStatusResponse(boolean authenticated, AuthUserResponse user) {
    }

    public record LoginRequest(String email, String password) {
    }

    public record RegisterRequest(String firstName, String lastName, String email, String password) {
    }

    public record ResetPasswordRequest(String email, String newPassword) {
    }

    public record CsrfTokenResponse(String token) {
    }
}
