package com.projectai.projectai.auth;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpSession;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.security.SecureRandom;
import java.util.Base64;

@Service
public class CsrfService {

    private static final String SESSION_CSRF_TOKEN = "session_csrf_token";

    private final SecureRandom secureRandom = new SecureRandom();

    public String getOrCreateToken(HttpSession session) {
        Object existing = session.getAttribute(SESSION_CSRF_TOKEN);
        if (existing instanceof String token && !token.isBlank()) {
            return token;
        }
        byte[] bytes = new byte[24];
        secureRandom.nextBytes(bytes);
        String token = Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
        session.setAttribute(SESSION_CSRF_TOKEN, token);
        return token;
    }

    public void requireValidToken(HttpServletRequest request, HttpSession session) {
        String expected = getOrCreateToken(session);
        String actual = request.getHeader("X-CSRF-Token");
        if (actual == null || actual.isBlank() || !expected.equals(actual)) {
            throw new ResponseStatusException(HttpStatus.FORBIDDEN, "Invalid CSRF token");
        }
    }
}
